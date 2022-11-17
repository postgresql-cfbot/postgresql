/*-------------------------------------------------------------------------
 *
 * bufenc.c
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/crypto/bufenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "lib/stringinfo.h"

#include "access/gist.h"
#include "access/xlog.h"
#include "crypto/bufenc.h"
#include "storage/bufpage.h"
#include "storage/fd.h"

extern XLogRecPtr LSNForEncryption(bool use_wal_lsn);

/*
 * We use the page LSN, page number, and permanent-bit to indicate if a fake
 * LSN was used to create a nonce for each page.
 */
#define BUFENC_IV_SIZE		16

static unsigned char buf_encryption_iv[BUFENC_IV_SIZE];

PgCipherCtx *BufEncCtx = NULL;
PgCipherCtx *BufDecCtx = NULL;

static void set_buffer_encryption_iv(Page page, BlockNumber blkno,
									 bool relation_is_permanent);

void
InitializeBufferEncryption(void)
{
	const CryptoKey *key;

	if (!FileEncryptionEnabled)
		return;

	key = KmgrGetKey(KMGR_KEY_ID_REL);

	BufEncCtx = pg_cipher_ctx_create(PG_CIPHER_AES_CTR,
									 (unsigned char *) key->key,
									 (key->klen), true);
	if (!BufEncCtx)
		elog(ERROR, "cannot intialize encryption context");

	BufDecCtx = pg_cipher_ctx_create(PG_CIPHER_AES_CTR,
									 (unsigned char *) key->key,
									 (key->klen), false);
	if (!BufDecCtx)
		elog(ERROR, "cannot intialize decryption context");
}

/* Encrypt the given page with the relation key */
void
EncryptPage(Page page, bool relation_is_permanent, BlockNumber blkno)
{
	unsigned char *ptr = (unsigned char *) page + PageEncryptOffset;
	bool		is_gist_page_or_similar;

	int			enclen;

	Assert(BufEncCtx != NULL);

	/*
	 * Permanent pages have valid LSNs, and non-permanent pages usually have
	 * invalid (not set) LSNs.  (One exception are GiST fake LSNs, see below.)
	 * However, we need valid ones on all pages for encryption.  There are too
	 * many places that set the page LSN for permanent pages to do the same
	 * for non-permanent pages, so we just set it here.
	 *
	 * Also, while permanent relations get new LSNs every time the page is
	 * modified, for non-permanent relations do not, so we just update the LSN
	 * here before it is encrypted.
	 *
	 * GiST indexes uses LSNs, which are also stored in NSN fields, to detect
	 * page splits.  Therefore, we allow the GiST code to assign LSNs and we
	 * don't change them here.
	 */

	/* Permanent relations should already have valid LSNs. */
	Assert(!XLogRecPtrIsInvalid(PageGetLSN(page)) || !relation_is_permanent);

	/*
	 * Check if the page has a special size == GISTPageOpaqueData, a valid
	 * GIST_PAGE_ID, no invalid GiST flag bits are set, and a valid LSN.  This
	 * is true for all GiST pages, and perhaps a few pages that are not.  The
	 * only downside of guessing wrong is that we might not update the LSN for
	 * some non-permanent relation page changes, and therefore reuse the IV,
	 * which seems acceptable.
	 */
	is_gist_page_or_similar =
		(PageGetSpecialSize(page) == MAXALIGN(sizeof(GISTPageOpaqueData)) &&
		 GistPageGetOpaque(page)->gist_page_id == GIST_PAGE_ID &&
		 (GistPageGetOpaque(page)->flags & ~GIST_FLAG_BITMASK) == 0 &&
		 !XLogRecPtrIsInvalid(PageGetLSN(page)));

	if (!relation_is_permanent && !is_gist_page_or_similar)
		PageSetLSN(page, LSNForEncryption(relation_is_permanent));

	set_buffer_encryption_iv(page, blkno, relation_is_permanent);
	if (unlikely(!pg_cipher_encrypt(BufEncCtx, PG_CIPHER_AES_CTR,
									(const unsigned char *) ptr,	/* input  */
									SizeOfPageEncryption,
									ptr,	/* length */
									&enclen,	/* resulting length */
									buf_encryption_iv,	/* iv */
									BUFENC_IV_SIZE,
									NULL, 0)))
		elog(ERROR, "cannot encrypt page %u", blkno);

	Assert(enclen == SizeOfPageEncryption);
}

/* Decrypt the given page with the relation key */
void
DecryptPage(Page page, bool relation_is_permanent, BlockNumber blkno)
{
	unsigned char *ptr = (unsigned char *) page + PageEncryptOffset;
	int			enclen;

	Assert(BufDecCtx != NULL);

	set_buffer_encryption_iv(page, blkno, relation_is_permanent);
	if (unlikely(!pg_cipher_decrypt(BufDecCtx, PG_CIPHER_AES_CTR,
									(const unsigned char *) ptr,	/* input  */
									SizeOfPageEncryption,
									ptr,	/* output */
									&enclen,	/* resulting length */
									buf_encryption_iv,	/* iv */
									BUFENC_IV_SIZE,
									NULL, 0)))
		elog(ERROR, "cannot decrypt page %u", blkno);

	Assert(enclen == SizeOfPageEncryption);
}

/* Construct iv for the given page */
static void
set_buffer_encryption_iv(Page page, BlockNumber blkno,
						 bool relation_is_permanent)
{
	unsigned char *p = buf_encryption_iv;

	MemSet(buf_encryption_iv, 0, BUFENC_IV_SIZE);

	/* page lsn (8 byte) */
	memcpy(p, &((PageHeader) page)->pd_lsn, sizeof(PageXLogRecPtr));
	p += sizeof(PageXLogRecPtr);

	/* block number (4 byte) */
	memcpy(p, &blkno, sizeof(BlockNumber));
	p += sizeof(BlockNumber);

	/*
	 * Mark use of fake LSNs in IV so if the real and fake LSN counters
	 * overlap, the IV will remain unique.  XXX Is there a better value?
	 */
	if (!relation_is_permanent)
		*p++ = 0x80;

	/*
	 * The maximum required counter for AES-CTR is 2048, which fits in the
	 * last three bytes.
	 */
}
