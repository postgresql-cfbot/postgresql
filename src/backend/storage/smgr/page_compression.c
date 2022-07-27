/*
 * page_compression.c
 *		Routines for page compression
 *
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/page_compression.c
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#include "storage/page_compression.h"
#include "storage/page_compression_impl.h"

static BlockNumber getsegno(const char *path_pca);

/*
 * Returns segment number for specified pca file.
 *
 */
static BlockNumber
getsegno(const char *path_pca)
{
	int			i;
	BlockNumber segno = (BlockNumber) 0;
	int			pathlen = strlen(path_pca);
	const char *dot_pos = NULL;

	for (i = pathlen - 5; i >= 0; i--)
	{
		if (path_pca[i] < '0' && path_pca[i] > '9')
			break;

		if (path_pca[i] == '.')
		{
			dot_pos = &path_pca[i];
			break;
		}
	}

	if (dot_pos)
		segno = atol(dot_pos);

	return segno;
}

/*
 * errcode_for_dynamic_shared_memory --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of mmap access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_dynamic_shared_memory(void)
{
	if (errno == EFBIG || errno == ENOMEM)
		return errcode(ERRCODE_OUT_OF_MEMORY);
	else
		return errcode_for_file_access();
}

/*
 * Check data consistency of pca file and if inconsistent try to repair it via
 * the pcd file.
 *
 */
void
check_and_repair_compress_address(PageCompressHeader *pcmap, uint16 chunk_size, uint8 algorithm,
								  const char *path, int fd_pcd, const char *path_pcd)
{
	int			i,
				unused_chunks;
	BlockNumber blocknum,
				segno;
	uint32		nblocks,
				allocated_chunks,
				real_blocks;
	BlockNumber *global_chunknos;
	char		last_recovery_start_time_buf[sizeof(TimestampTz)];
	char		start_time_buf[sizeof(TimestampTz)];
	struct stat fst;
	bool		need_check = false;
	int			total_allocated_chunks;
	PageCompressAddr *pcaddr;

	unused_chunks = 0;
	real_blocks = 0;

	/* if the relation had been checked in this startup, skip */
	memcpy(last_recovery_start_time_buf, &pcmap->last_recovery_start_time, sizeof(TimestampTz));
	memcpy(start_time_buf, &PgStartTime, sizeof(TimestampTz));
	for (i = 0; i < sizeof(TimestampTz); i++)
	{
		if (start_time_buf[i] != last_recovery_start_time_buf[i])
		{
			need_check = true;
			break;
		}
	}
	if (!need_check)
		return;

	/* read count of allocated chunks */
	if (stat(path_pcd, &fst) < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path_pcd)));
	}
	total_allocated_chunks = fst.st_size / chunk_size;

	/* check header of page compression address file */
	if (pcmap->chunk_size != chunk_size || pcmap->algorithm != algorithm)
	{
		/*
		 * reinitialize header of pca file if it is invalid and
		 * zero_damaged_pages is on
		 */
		if (zero_damaged_pages)
		{
			ereport(WARNING,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid chunk_size %u or algorithm %u in header of file \"%s\", and reinitialized it.",
							pcmap->chunk_size, pcmap->algorithm, path)));

			pcmap->algorithm = algorithm;
			pcmap->chunk_size = chunk_size;
			pg_atomic_write_u32(&pcmap->nblocks, 0);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid chunk_size %u or algorithm %u in header of file \"%s\"",
							pcmap->chunk_size, pcmap->algorithm, path)));
	}

	segno = getsegno(path);
	nblocks = pg_atomic_read_u32(&pcmap->nblocks);
	allocated_chunks = pg_atomic_read_u32(&pcmap->allocated_chunks);
	global_chunknos = palloc0(sizeof(BlockNumber) * MAX_PAGE_COMPRESS_CHUNK_NUMBER(chunk_size));

	/* check address data of every pages */
	for (blocknum = segno * ((BlockNumber) RELSEG_SIZE); blocknum < (segno + 1) * ((BlockNumber) RELSEG_SIZE); blocknum++)
	{
		pcaddr = GetPageCompressAddr(pcmap, chunk_size, blocknum);

		/* skip when found first zero filled block after nblocks */
		if (blocknum % RELSEG_SIZE >= (BlockNumber) nblocks && pcaddr->allocated_chunks == 0)
			break;

		/* check allocated_chunks for one page */
		if (pcaddr->allocated_chunks > MaxChunksPreCompressedPage(chunk_size))
		{
			if (zero_damaged_pages)
			{
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid allocated chunks %u of block %u in file \"%s\", and zero the block",
								pcaddr->allocated_chunks, blocknum, path)));

				MemSet(pcaddr, 0, SizeOfPageCompressAddr(chunk_size));
				continue;
			}
			else
			{
				pfree(global_chunknos);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid allocated chunks %u of block %u in file \"%s\"",
								pcaddr->allocated_chunks, blocknum, path)));
			}
		}

		/* check nchunks for one page */
		if (pcaddr->nchunks > pcaddr->allocated_chunks)
		{
			if (zero_damaged_pages)
			{
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("nchunks %u exceeds allocated_chunks %u of block %u in file \"%s\", and zero the block",
								pcaddr->nchunks, pcaddr->allocated_chunks, blocknum, path)));

				MemSet(pcaddr, 0, SizeOfPageCompressAddr(chunk_size));
				continue;
			}
			else
			{
				pfree(global_chunknos);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("nchunks %u exceeds allocated_chunks %u of block %u in file \"%s\"",
								pcaddr->nchunks, pcaddr->allocated_chunks, blocknum, path)));
			}
		}

		/* check chunknos for one page */
		for (i = 0; i < pcaddr->allocated_chunks; i++)
		{
			/* check for invalid chunkno */
			if (pcaddr->chunknos[i] == 0 || pcaddr->chunknos[i] > MAX_PAGE_COMPRESS_CHUNK_NUMBER(chunk_size))
			{
				if (zero_damaged_pages)
				{
					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid chunk number %u of block %u in file \"%s\", and zero the block",
									pcaddr->chunknos[i], blocknum, path)));

					MemSet(pcaddr, 0, SizeOfPageCompressAddr(chunk_size));
					break;
				}
				else
				{
					pfree(global_chunknos);
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid chunk number %u of block %u in file \"%s\"",
									pcaddr->chunknos[i], blocknum, path)));
				}
			}

			/* check for duplicate chunkno */
			if (global_chunknos[pcaddr->chunknos[i] - 1] != 0)
			{
				if (zero_damaged_pages)
				{
					int			j;
					PageCompressAddr *pcaddr_dup = GetPageCompressAddr(pcmap, chunk_size, global_chunknos[pcaddr->chunknos[i] - 1] - 1);

					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("chunk number %u of block %u duplicate with block %u in file \"%s\", and zero the two blocks",
									pcaddr->chunknos[i], blocknum, global_chunknos[pcaddr->chunknos[i] - 1] - 1, path)));

					/*
					 * clean all chunk allocation infomation in the two
					 * duplicated pages
					 */
					for (j = 0; j < pcaddr_dup->allocated_chunks; j++)
					{
						global_chunknos[pcaddr_dup->chunknos[j] - 1] = 0;
					}
					MemSet(pcaddr, 0, SizeOfPageCompressAddr(chunk_size));
					MemSet(pcaddr_dup, 0, SizeOfPageCompressAddr(chunk_size));
					continue;
				}
				else
				{
					pfree(global_chunknos);
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("chunk number %u of block %u duplicate with block %u in file \"%s\"",
									pcaddr->chunknos[i], blocknum, global_chunknos[pcaddr->chunknos[i] - 1] - 1, path)));
				}
			}

			/*
			 * check if chunkno exceeds size of the pcd file, exceeded chunkno
			 * will be clean up
			 */
			if (pcaddr->chunknos[i] > total_allocated_chunks)
			{
				pcaddr->allocated_chunks = i;
				break;
			}
		}

		/*
		 * clean chunknos beyond allocated_chunks for one page and this is a
		 * normal scenario
		 */
		for (i = pcaddr->allocated_chunks; i < MaxChunksPreCompressedPage(chunk_size); i++)
		{
			if (pcaddr->chunknos[i] != 0)
			{
				ereport(DEBUG1,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("clear chunk %u at position %d which beyond allocated_chunks %u of block %u in file \"%s\"",
								pcaddr->chunknos[i], i + 1, pcaddr->allocated_chunks, blocknum, path)));
				pcaddr->chunknos[i] = 0;
			}
		}

		for (i = 0; i < pcaddr->allocated_chunks; i++)
		{
			global_chunknos[pcaddr->chunknos[i] - 1] = blocknum + 1;
		}

		if (pcaddr->nchunks > 0)
			real_blocks = blocknum + 1;
	}

	/* clean the rest of address data in pca file */
	for (; blocknum < (segno + 1) * ((BlockNumber) RELSEG_SIZE); blocknum++)
	{
		char		buf[256],
				   *p;
		bool		need_clean = false;

		pcaddr = GetPageCompressAddr(pcmap, chunk_size, blocknum);
		if (pcaddr->allocated_chunks != 0 || pcaddr->nchunks != 0)
			need_clean = true;

		/* clean address data and output content of the address */
		MemSet(buf, 0, sizeof(buf));
		p = buf;

		for (i = 0; i < MaxChunksPreCompressedPage(chunk_size); i++)
		{
			if (pcaddr->chunknos[i])
			{
				need_clean = true;
				if (i == 0)
					snprintf(p, (sizeof(buf) - (p - buf)), "%u", pcaddr->chunknos[i]);
				else
					snprintf(p, (sizeof(buf) - (p - buf)), ",%u", pcaddr->chunknos[i]);
				p += strlen(p);
			}
		}

		if (need_clean)
		{
			ereport(WARNING,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("clean unused address data of block %u in file \"%s\", old allocated_chunks/nchunks/chunknos: %u/%u/{%s}",
							blocknum, path, pcaddr->allocated_chunks, pcaddr->nchunks, buf)));

			MemSet(pcaddr, 0, SizeOfPageCompressAddr(chunk_size));
		}
	}

	/* fix unused chunks via compression data file */
	for (i = 0; i < total_allocated_chunks; i++)
	{
		int			rc;
		char		buf[SizeOfPageCompressChunkHeaderData + SizeOfPageCompressDataHeaderData];
		int			nbytes = SizeOfPageCompressChunkHeaderData + SizeOfPageCompressDataHeaderData;
		PageCompressChunk *pcchunk = (PageCompressChunk *) buf;
		PageCompressData *pcdata = (PageCompressData *) (buf + SizeOfPageCompressChunkHeaderData);

		if (global_chunknos[i] == 0)
		{
			rc = pg_pread(fd_pcd, buf, nbytes, i * chunk_size);

			if (rc < 0)
			{
				pfree(global_chunknos);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path_pcd)));
			}

			if (rc >= 0 && rc != nbytes)
			{
				pfree(global_chunknos);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": read %d of %d",
								path_pcd, rc, nbytes)));
			}

			if (pcchunk->chunkseq > 0 && pcchunk->chunkseq <= MaxChunksPreCompressedPage(chunk_size))
			{
				pcaddr = GetPageCompressAddr(pcmap, chunk_size, pcchunk->blockno);
				if (pcaddr->chunknos[pcchunk->chunkseq - 1] == 0 &&
					pcaddr->allocated_chunks + 1 == pcchunk->chunkseq)
				{
					global_chunknos[i] = pcchunk->blockno + 1;
					pcaddr->chunknos[pcchunk->chunkseq - 1] = i + 1;
					pcaddr->allocated_chunks++;

					ereport(LOG,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("fix allocation of chunk for block %u in file \"%s\", seq: %u, chunkno: %u",
									pcchunk->blockno, path, pcchunk->chunkseq, i + 1)));

					/*
					 * After fixing page's nchunks, may cause nchunks to be
					 * larger than allocated_chunks. This will be fixed when
					 * replaying WAL record with FPI.
					 */
					if (pcchunk->withdata && pcchunk->chunkseq == 1)
					{
						int			nchunks = 0;

						nchunks = NeedPageCompressChunksToStoreData(chunk_size, SizeOfPageCompressDataHeaderData + pcdata->size);

						if (nchunks <= MaxChunksPreCompressedPage(chunk_size) && pcaddr->nchunks != nchunks)
						{
							ereport(LOG,
									(errcode(ERRCODE_DATA_CORRUPTED),
									 errmsg("fix nchunks of block %u in file \"%s\", old: %u, new: %u",
											pcchunk->blockno, path, pcaddr->nchunks, nchunks)));

							pcaddr->nchunks = nchunks;
							if ((pcchunk->blockno % RELSEG_SIZE) + 1 > real_blocks)
								real_blocks = (pcchunk->blockno % RELSEG_SIZE) + 1;
						}
					}
				}
			}
		}
	}

	/* check for holes in allocated chunks */
	for (i = 0; i < total_allocated_chunks; i++)
		if (global_chunknos[i] == 0)
			unused_chunks++;

	if (unused_chunks > 0)
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("there are %u chunks of total allocated chunks %u can not be use in file \"%s\"",
						unused_chunks, total_allocated_chunks, path),
				 errhint("You may need to run VACUMM FULL to optimize space allocation, or run REINDEX if it is an index.")));

	/*
	 * update nblocks in header of page compression address file. Because
	 * mdextend_pc delays space allocation for new pages, it is normal for the
	 * number of blocks recorded in the compression address file to be greater
	 * than the maximum block number in the compression data file. So scenes
	 * with "real_blocks < nblocks" do not need to be fixed.
	 */
	if (real_blocks > nblocks)
	{
		pg_atomic_write_u32(&pcmap->nblocks, real_blocks);
		pg_atomic_write_u32(&pcmap->last_synced_nblocks, real_blocks);

		ereport(LOG,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("fix nblocks in header of  file \"%s\", old: %u, new: %u",
						path, nblocks, real_blocks)));
	}

	/* update allocated_chunks in header of compression address file */
	if (allocated_chunks != total_allocated_chunks)
	{
		pg_atomic_write_u32(&pcmap->allocated_chunks, total_allocated_chunks);
		pg_atomic_write_u32(&pcmap->last_synced_allocated_chunks, total_allocated_chunks);

		ereport(LOG,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("fix allocated_chunks in header of file \"%s\", old: %u, new: %u",
						path, allocated_chunks, total_allocated_chunks)));
	}

	pfree(global_chunknos);

	if (pc_msync(pcmap) != 0)
		ereport(data_sync_elevel(ERROR),
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not msync file \"%s\": %m",
						path)));

	pcmap->last_recovery_start_time = PgStartTime;
}
