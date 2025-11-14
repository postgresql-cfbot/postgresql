/*--------------------------------------------------------------------------
 *
 * test_dsm_registry.c
 *	  Test the dynamic shared memory registry.
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_dsm_registry/test_dsm_registry.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "fmgr.h"
#include "pgstat.h"
#include "storage/dsm_registry.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"

PG_MODULE_MAGIC;

/* Location of permanent storage file (valid on checkpoint) */
#define TDR_DUMP_FILE	PGSTAT_STAT_PERMANENT_DIRECTORY "/pg_stat_statements.stat"
/* Magic number identifying the stats file format */
static const uint32 TDR_FILE_HEADER = 0x20251114;

typedef struct TestDSMRegistryStruct
{
	int			val;
	LWLock		lck;
} TestDSMRegistryStruct;

typedef struct TestDSMRegistryHashEntry
{
	char		key[64];
	dsa_pointer val;
} TestDSMRegistryHashEntry;

static TestDSMRegistryStruct *tdr_dsm;
static dsa_area *tdr_dsa;
static dshash_table *tdr_hash;

static const dshash_parameters dsh_params = {
	offsetof(TestDSMRegistryHashEntry, val),
	sizeof(TestDSMRegistryHashEntry),
	dshash_strcmp,
	dshash_strhash,
	dshash_strcpy
};

static Checkpoint_hook_type	prev_Checkpoint_hook = NULL;

static void load_htab(void);
static void pgss_Checkpoint(XLogRecPtr checkPointRedo, int flags);

static void
init_tdr_dsm(void *ptr, void *arg)
{
	TestDSMRegistryStruct *dsm = (TestDSMRegistryStruct *) ptr;

	if ((int) (intptr_t) arg != 5432)
		elog(ERROR, "unexpected arg value %d", (int) (intptr_t) arg);

	LWLockInitialize(&dsm->lck, LWLockNewTrancheId("test_dsm_registry"));
	dsm->val = 0;
}

static void
tdr_attach_shmem(void)
{
	bool		found;

	tdr_dsm = GetNamedDSMSegment("test_dsm_registry_dsm",
								 sizeof(TestDSMRegistryStruct),
								 init_tdr_dsm,
								 &found, (void *) (intptr_t) 5432);

	if (tdr_dsa == NULL)
		tdr_dsa = GetNamedDSA("test_dsm_registry_dsa", &found);

	if (tdr_hash == NULL)
	{
		LWLockAcquire(&tdr_dsm->lck, LW_EXCLUSIVE);
		tdr_hash = GetNamedDSHash("test_dsm_registry_hash", &dsh_params, &found);
		if (!found)
			load_htab();

		LWLockRelease(&tdr_dsm->lck);
	}
}

PG_FUNCTION_INFO_V1(set_val_in_shmem);
Datum
set_val_in_shmem(PG_FUNCTION_ARGS)
{
	tdr_attach_shmem();

	LWLockAcquire(&tdr_dsm->lck, LW_EXCLUSIVE);
	tdr_dsm->val = PG_GETARG_INT32(0);
	LWLockRelease(&tdr_dsm->lck);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_val_in_shmem);
Datum
get_val_in_shmem(PG_FUNCTION_ARGS)
{
	int			ret;

	tdr_attach_shmem();

	LWLockAcquire(&tdr_dsm->lck, LW_SHARED);
	ret = tdr_dsm->val;
	LWLockRelease(&tdr_dsm->lck);

	PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(set_val_in_hash);
Datum
set_val_in_hash(PG_FUNCTION_ARGS)
{
	TestDSMRegistryHashEntry *entry;
	char	   *key = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	   *val = TextDatumGetCString(PG_GETARG_DATUM(1));
	bool		found;

	if (strlen(key) >= offsetof(TestDSMRegistryHashEntry, val))
		ereport(ERROR,
				(errmsg("key too long")));

	tdr_attach_shmem();

	entry = dshash_find_or_insert(tdr_hash, key, &found);
	if (found)
		dsa_free(tdr_dsa, entry->val);

	entry->val = dsa_allocate(tdr_dsa, strlen(val) + 1);
	strcpy(dsa_get_address(tdr_dsa, entry->val), val);

	dshash_release_lock(tdr_hash, entry);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_val_in_hash);
Datum
get_val_in_hash(PG_FUNCTION_ARGS)
{
	TestDSMRegistryHashEntry *entry;
	char	   *key = TextDatumGetCString(PG_GETARG_DATUM(0));
	text	   *val = NULL;

	tdr_attach_shmem();

	entry = dshash_find(tdr_hash, key, false);
	if (entry == NULL)
		PG_RETURN_NULL();

	val = cstring_to_text(dsa_get_address(tdr_dsa, entry->val));

	dshash_release_lock(tdr_hash, entry);

	PG_RETURN_TEXT_P(val);
}

/*
 * Load any pre-existing entries from file.
 */
static void
load_htab(void)
{
	bool	found;
	FILE   *file = NULL;
	uint32	header;
	char   *val = palloc(1);

	Assert(tdr_dsa != NULL && tdr_hash != NULL);

	/*
	 * Attempt to load old entries from the dump file.
	 */
	file = AllocateFile(TDR_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		/* No existing persisted file, so we're done */
		return;
	}

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != TDR_FILE_HEADER)
		goto read_error;

	while (!feof(file))
	{
		TestDSMRegistryHashEntry *entry;
		char	key[64];
		int		keylen = offsetof(TestDSMRegistryHashEntry, val);
		int32	vlen;

		if (fread(key, keylen, 1, file) != 1 ||
			fread(&vlen, sizeof(int32), 1, file) != 1)
			goto read_error;

		val = repalloc(val, vlen);
		if (fread(val, vlen, 1, file) != 1)
			goto read_error;

		Assert(val[vlen - 1] == '\0');

		entry = (TestDSMRegistryHashEntry *)
								dshash_find_or_insert(tdr_hash, key, &found);
		Assert(!found);

		entry->val = dsa_allocate(tdr_dsa, strlen(val) + 1);
		strcpy(dsa_get_address(tdr_dsa, entry->val), val);

		dshash_release_lock(tdr_hash, entry);
	}

	FreeFile(file);
	return;

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read from file \"%s\": %m", TDR_DUMP_FILE)));
	if (file)
		FreeFile(file);
	/* If possible, throw away the bogus file; ignore any error */
	unlink(TDR_DUMP_FILE);
}

/*
 * Dump hash table into file.
 *
 */
static void
pgss_Checkpoint(XLogRecPtr checkPointRedo, int flags)
{
	FILE					   *file;
	dshash_seq_status			hstat;
	TestDSMRegistryHashEntry   *entry;

	if (flags & CHECKPOINT_END_OF_RECOVERY)
		return;

	tdr_attach_shmem();

	file = AllocateFile(TDR_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;
	if (fwrite(&TDR_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;

	dshash_seq_init(&hstat, tdr_hash, false);
	while ((entry = dshash_seq_next(&hstat)) != NULL)
	{
		int		keylen = offsetof(TestDSMRegistryHashEntry, val);
		char   *val;
		int32	vlen;

		val = (char *) dsa_get_address(tdr_dsa, entry->val);
		vlen = strlen(val) + 1;
		if (fwrite(entry->key, keylen, 1, file) != 1 ||
			fwrite(&vlen, sizeof(int32), 1, file) != 1 ||
			fwrite(val, vlen, 1, file) != 1)
		{
			dshash_seq_term(&hstat);
			goto error;
		}
	}
	dshash_seq_term(&hstat);

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file into place, so we atomically replace any old one.
	 */
	(void) durable_rename(TDR_DUMP_FILE ".tmp", TDR_DUMP_FILE, LOG);
	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					TDR_DUMP_FILE ".tmp")));
	if (file)
		FreeFile(file);
	unlink(TDR_DUMP_FILE ".tmp");
}

/*
 * Entry point for this module.
 */
void
_PG_init(void)
{
	prev_Checkpoint_hook = Checkpoint_hook;
	Checkpoint_hook = pgss_Checkpoint;
}
