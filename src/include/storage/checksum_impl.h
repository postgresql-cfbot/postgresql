/*-------------------------------------------------------------------------
 *
 * checksum_impl.h
 *	  Checksum implementation for data pages.
 *
 * This file exists for the benefit of external programs that may wish to
 * check Postgres page checksums.  They can #include this to get the code
 * referenced by storage/checksum.h.  (Note: you may need to redefine
 * Assert() as empty to compile this successfully externally.)
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum_impl.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * The algorithm used to checksum pages is chosen for very fast calculation.
 * Workloads where the database working set fits into OS file cache but not
 * into shared buffers can read in pages at a very fast pace and the checksum
 * algorithm itself can become the largest bottleneck.
 *
 * The checksum algorithm itself is based on the FNV-1a hash (FNV is shorthand
 * for Fowler/Noll/Vo).  The primitive of a plain FNV-1a hash folds in data 1
 * byte at a time according to the formula:
 *
 *	   hash = (hash ^ value) * FNV_PRIME
 *
 * FNV-1a algorithm is described at http://www.isthe.com/chongo/tech/comp/fnv/
 *
 * PostgreSQL doesn't use FNV-1a hash directly because it has bad mixing of
 * high bits - high order bits in input data only affect high order bits in
 * output data. To resolve this we xor in the value prior to multiplication
 * shifted right by 17 bits. The number 17 was chosen because it doesn't
 * have common denominator with set bit positions in FNV_PRIME and empirically
 * provides the fastest mixing for high order bits of final iterations quickly
 * avalanche into lower positions. For performance reasons we choose to combine
 * 4 bytes at a time. The actual hash formula used as the basis is:
 *
 *	   hash = (hash ^ value) * FNV_PRIME ^ ((hash ^ value) >> 17)
 *
 * The main bottleneck in this calculation is the multiplication latency. To
 * hide the latency and to make use of SIMD parallelism multiple hash values
 * are calculated in parallel. The page is treated as a 32 column two
 * dimensional array of 32 bit values. Each column is aggregated separately
 * into a partial checksum. Each partial checksum uses a different initial
 * value (offset basis in FNV terminology). The initial values actually used
 * were chosen randomly, as the values themselves don't matter as much as that
 * they are different and don't match anything in real data. After initializing
 * partial checksums each value in the column is aggregated according to the
 * above formula. Finally two more iterations of the formula are performed with
 * value 0 to mix the bits of the last value added.
 *
 * The partial checksums are then folded together using xor to form a single
 * 32-bit checksum. The caller can safely reduce the value to 16 bits
 * using modulo 2^16-1. That will cause a very slight bias towards lower
 * values but this is not significant for the performance of the
 * checksum.
 *
 * The algorithm choice was based on what instructions are available in SIMD
 * instruction sets. This meant that a fast and good algorithm needed to use
 * multiplication as the main mixing operator. The simplest multiplication
 * based checksum primitive is the one used by FNV. The prime used is chosen
 * for good dispersion of values. It has no known simple patterns that result
 * in collisions. Test of 5-bit differentials of the primitive over 64bit keys
 * reveals no differentials with 3 or more values out of 100000 random keys
 * colliding. Avalanche test shows that only high order bits of the last word
 * have a bias. Tests of 1-4 uncorrelated bit errors, stray 0 and 0xFF bytes,
 * overwriting page from random position to end with 0 bytes, and overwriting
 * random segments of page with 0x00, 0xFF and random data all show optimal
 * 2e-16 false positive rate within margin of error.
 *
 * Vectorization of the algorithm requires 32bit x 32bit -> 32bit integer
 * multiplication instruction. As of 2013 the corresponding instruction is
 * available on x86 SSE4.1 extensions (pmulld) and ARM NEON (vmul.i32).
 * Vectorization requires a compiler to do the vectorization for us. For recent
 * GCC versions the flags -msse4.1 -funroll-loops -ftree-vectorize are enough
 * to achieve vectorization.
 *
 * The optimal amount of parallelism to use depends on CPU specific instruction
 * latency, SIMD instruction width, throughput and the amount of registers
 * available to hold intermediate state. Generally, more parallelism is better
 * up to the point that state doesn't fit in registers and extra load-store
 * instructions are needed to swap values in/out. The number chosen is a fixed
 * part of the algorithm because changing the parallelism changes the checksum
 * result.
 *
 * The parallelism number 32 was chosen based on the fact that it is the
 * largest state that fits into architecturally visible x86 SSE registers while
 * leaving some free registers for intermediate values. For future processors
 * with 256bit vector registers this will leave some performance on the table.
 * When vectorization is not available it might be beneficial to restructure
 * the computation to calculate a subset of the columns at a time and perform
 * multiple passes to avoid register spilling. This optimization opportunity
 * is not used. Current coding also assumes that the compiler has the ability
 * to unroll the inner loop to avoid loop overhead and minimize register
 * spilling. For less sophisticated compilers it might be beneficial to
 * manually unroll the inner loop.
 */

#include "storage/bufpage.h"
#include "common/komihash.h"

/* number of checksums to calculate in parallel */
#define N_SUMS 32
/* prime multiplier of FNV-1a hash */
#define FNV_PRIME 16777619

/* Use a union so that this code is valid under strict aliasing */
typedef union
{
	PageHeaderData phdr;
	uint32		data[BLCKSZ / (sizeof(uint32) * N_SUMS)][N_SUMS];
} PGChecksummablePage;

/*
 * Base offsets to initialize each of the parallel FNV hashes into a
 * different initial state.
 */
static const uint32 checksumBaseOffsets[N_SUMS] = {
	0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A,
	0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
	0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA,
	0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
	0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE,
	0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
	0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
	0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756
};

/*
 * Calculate one round of the checksum.
 */
#define CHECKSUM_COMP(checksum, value) \
do { \
	uint32 __tmp = (checksum) ^ (value); \
	(checksum) = __tmp * FNV_PRIME ^ (__tmp >> 17); \
} while (0)

/*
 * Block checksum algorithm.  The page must be adequately aligned
 * (at least on 4-byte boundary).
 */
static uint32
pg_checksum_block(const PGChecksummablePage *page)
{
	uint32		sums[N_SUMS];
	uint32		result = 0;
	uint32		i,
				j;

	/* ensure that the size is compatible with the algorithm */
	Assert(sizeof(PGChecksummablePage) == BLCKSZ);

	/* initialize partial checksums to their corresponding offsets */
	memcpy(sums, checksumBaseOffsets, sizeof(checksumBaseOffsets));

	/* main checksum calculation */
	for (i = 0; i < (uint32) (BLCKSZ / (sizeof(uint32) * N_SUMS)); i++)
		for (j = 0; j < N_SUMS; j++)
			CHECKSUM_COMP(sums[j], page->data[i][j]);

	/* finally add in two rounds of zeroes for additional mixing */
	for (i = 0; i < 2; i++)
		for (j = 0; j < N_SUMS; j++)
			CHECKSUM_COMP(sums[j], 0);

	/* xor fold partial checksums together */
	for (i = 0; i < N_SUMS; i++)
		result ^= sums[i];

	return result;
}

/*
 * Compute the checksum for a Postgres page.
 *
 * The page must be adequately aligned (at least on a 4-byte boundary).
 * Beware also that the checksum field of the page is transiently zeroed.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 */
uint16
pg_checksum_page(char *page, BlockNumber blkno)
{
	PGChecksummablePage *cpage = (PGChecksummablePage *) page;
	uint16		save_checksum;
	uint32		checksum;

	/* We only calculate the checksum for properly-initialized pages */
	Assert(!PageIsNew((Page) page));

	/*
	 * Save pd_checksum and temporarily set it to zero, so that the checksum
	 * calculation isn't affected by the old checksum stored on the page.
	 * Restore it after, because actually updating the checksum is NOT part of
	 * the API of this function.
	 */
	save_checksum = cpage->phdr.pd_checksum;
	cpage->phdr.pd_checksum = 0;
	checksum = pg_checksum_block(cpage);
	cpage->phdr.pd_checksum = save_checksum;

	/* Mix in the block number to detect transposed pages */
	checksum ^= blkno;

	/*
	 * Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
	 * one. That avoids checksums of zero, which seems like a good idea.
	 */
	return (uint16) ((checksum % 65535) + 1);
}


/*
 * Compute and return a 32-bit checksum for a Postgres page.
 *
 * Beware that the 16-bit portion of the page that cksum points to is
 * transiently zeroed, as is the pd_checksums field.  The storage location for
 * this is determined by the PageFeatures in play for cluster, so we are
 * storing the
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 *
 * The high bits of this are stored in the overflow storage area of the page
 * pointed to by *cksum, leaving the pd_checksum field with the same checksum
 * you'd expect if running the pg_checksum_page function.
 */
uint32
pg_checksum32_page(char *page, BlockNumber blkno, char *cksum)
{
	PGChecksummablePage *cpage = (PGChecksummablePage *) page;
	uint16		save_pd,save_ext,*ptr;
	uint32		checksum;

	/* We only calculate the checksum for properly-initialized pages */
	Assert(!PageIsNew((Page) page));
	/* Ensure that the cksum pointer is in the page range on this page */
	Assert(cksum >= page && cksum <= (page + BLCKSZ - sizeof(uint16)));

	ptr = (uint16*)cksum;

	/*
	 * Save the existing checksum locations and temporarily set it to zero, so
	 * that the checksum calculation isn't affected by the old checksum stored
	 * on the page.  Restore it after, because actually updating the checksum
	 * is NOT part of the API of this function.
	 */

	save_ext = *ptr;
	save_pd = cpage->phdr.pd_checksum;
	*ptr = 0;
	cpage->phdr.pd_checksum = 0;

	checksum = pg_checksum_block(cpage);

	/* restore */
	*ptr = save_ext;
	cpage->phdr.pd_checksum = save_pd;

	/* Mix in the block number to detect transposed pages */
	checksum ^= blkno;

	/* ensure we have non-zero return value here; this does double-up on our
	 * coset for group 1 here, but it's a nice property to preserve */
	return (checksum == 0 ? 1 : checksum);
}

/*
 * 64-bit block checksum algorithm.  The page must be adequately aligned
 * (at least on 4-byte boundary).
 */

static uint64
pg_checksum64_block(const PGChecksummablePage *page)
{
	/* ensure that the size is compatible with the algorithm */
	Assert(sizeof(PGChecksummablePage) == BLCKSZ);

	return (uint64)komihash(page, BLCKSZ, 0);
}

/* temporary struct for ease of accessing memory */
typedef union {
	uint64 u64;
	uint8 bytes[8];
} Checksum56;

StaticAssertDecl(sizeof(Checksum56) == sizeof(uint64), "Can't make combined checksum56 struct");

/*
 * Compute and return a 64-bit checksum for a Postgres page.
 *
 * Beware that the 64-bit portion of the page that cksum points to is
 * transiently zeroed, though it is restored.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 */
uint64
pg_checksum64_page(char *page, BlockNumber blkno, uint64 *cksumloc)
{
	PGChecksummablePage *cpage = (PGChecksummablePage *) page;
	uint64      saved;
	uint64      checksum;

	/* We only calculate the checksum for properly-initialized pages */
	Assert(!PageIsNew((Page) page));
	/* Ensure that the cksum pointer is in the page range on this page */
	Assert((char*)cksumloc >= page && (char*)cksumloc <= (page + BLCKSZ - sizeof(uint64)));

	saved = *cksumloc;
	*cksumloc = 0;

	checksum = pg_checksum64_block(cpage);

	/* restore */
	*cksumloc = saved;

	/* Mix in the block number to detect transposed pages */
	checksum ^= blkno;

	/* ensure in the extremely unlikely case that we have non-zero return
	 * value here; this does double-up on our coset for group 1 here, but it's
	 * a nice property to preserve */
	return (checksum == 0 ? 1 : checksum);
}

/*
 * Compute and return a 56-bit checksum for a Postgres page.
 *
 * Beware that the 56-bit portion of the page that cksum points to is
 * transiently zeroed, though it is restored.  The low byte of the uint64 is
 * not part of this checksum, so is left on the page to be included as well.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 *
 */
uint64
pg_checksum56_page(char *page, BlockNumber blkno, uint64 *cksumloc)
{
	PGChecksummablePage *cpage = (PGChecksummablePage *) page;
	Checksum56      saved, checksum;

	/* We only calculate the checksum for properly-initialized pages */
	Assert(!PageIsNew((Page) page));
	/* Ensure that the cksum pointer is in the page range on this page */
	Assert((char*)cksumloc >= page && (char*)cksumloc <= (page + BLCKSZ - sizeof(uint64)));

	saved = *(Checksum56*)cksumloc;
	((Checksum56*)cksumloc)->u64 = 0;
	((Checksum56*)cksumloc)->bytes[7] = saved.bytes[7];

	checksum.u64 = pg_checksum64_block(cpage);
	checksum.bytes[7] = saved.bytes[7];
	/* restore */
	*cksumloc = saved.u64;

	/* Mix in the block number to detect transposed pages */
	checksum.u64 ^= blkno << 8;

	// checksum cannot be zero
	return checksum.u64;
}

/*
 * Set a 56-bit checksum onto a Postgres page.
 *
 * Given a uint64*, set the top 7 bytes to this checksum value, leaving the
 * original low-order byte in-place.
 */
void
pg_set_checksum56_page(char *page, uint64 checksum, uint64 *cksumloc)
{
	uint8 byte;
	/* Can only set the checksum for properly-initialized pages */
	Assert(!PageIsNew((Page) page));

	/* Ensure that the cksum pointer is in the page range on this page */
	Assert((char*)cksumloc >= page && (char*)cksumloc <= (page + BLCKSZ - sizeof(uint64)));

	// preserve the old byte field
	byte = ((Checksum56*)cksumloc)->bytes[7];
	((Checksum56*)cksumloc)->u64 = checksum;
	((Checksum56*)cksumloc)->bytes[7] = byte;
}

/*
 * Get the 56-bit checksum onto a Postgres page given the offset to the
 * containing uint64.
 */
uint64
pg_get_checksum56_page(char *page, uint64 *cksumloc)
{
	/* Can only set the checksum for properly-initialized pages */
	Assert(!PageIsNew((Page) page));

	/* Ensure that the cksum pointer is in the page range on this page */
	Assert((char*)cksumloc >= page && (char*)cksumloc <= (page + BLCKSZ - sizeof(uint64)));
	Assert(MAXALIGN((uint64)cksumloc) == (uint64)cksumloc);

	return *cksumloc;
}

