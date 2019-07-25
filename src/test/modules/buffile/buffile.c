#include <fcntl.h>

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "storage/buffile.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

PG_MODULE_MAGIC;

/*
 * To cope with files that span multiple segments w/o wasting resources, use
 * the smallest possible segment size. The test scripts need to set
 * buffile_max_filesize (GUC) accordingly.
 */
#define MAX_PHYSICAL_FILESIZE_TEST	(4 * BLCKSZ)

static BufFile *bf = NULL;
static TransientBufFile *bft = NULL;

static void check_file(void);

extern Datum buffile_create_transient(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_create);
Datum
buffile_create(PG_FUNCTION_ARGS)
{
	MemoryContext old_cxt;
	ResourceOwner old_ro;

	if (bf != NULL)
		elog(ERROR, "file already exists");

	old_cxt = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Make sure the file is not deleted across function calls.
	 */
	old_ro = CurrentResourceOwner;
	CurrentResourceOwner = TopTransactionResourceOwner;

	bf = BufFileCreateTemp(false);

	CurrentResourceOwner = old_ro;
	MemoryContextSwitchTo(old_cxt);

	PG_RETURN_VOID();
}

extern Datum buffile_close(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_close);
Datum
buffile_close(PG_FUNCTION_ARGS)
{
	if (bf == NULL)
		elog(ERROR, "there's no file to close");

	BufFileClose(bf);
	bf = NULL;

	PG_RETURN_VOID();
}

extern Datum buffile_write(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_write);
Datum
buffile_write(PG_FUNCTION_ARGS)
{
	Datum		d = PG_GETARG_DATUM(0);
	char	   *s = TextDatumGetCString(d);
	size_t		res;

	if (bf)
		res = BufFileWrite(bf, s, strlen(s));
	else if (bft)
		res = BufFileWriteTransient(bft, s, strlen(s));
	else
		elog(ERROR, "No file is open");

	PG_RETURN_INT64(res);
}

extern Datum buffile_read(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_read);
Datum
buffile_read(PG_FUNCTION_ARGS)
{
	int64		size = PG_GETARG_INT64(0);
	StringInfo	buf = makeStringInfo();
	size_t		res_size;
	bytea	   *result;

	enlargeStringInfo(buf, size);

	if (bf)
		res_size = BufFileRead(bf, buf->data, size);
	else if (bft)
		res_size = BufFileReadTransient(bft, buf->data, size);
	else
		elog(ERROR, "No file is open");

	buf->len = res_size;

	result = DatumGetByteaPP(DirectFunctionCall1(bytearecv,
												 PointerGetDatum(buf)));
	PG_RETURN_BYTEA_P(result);
}

extern Datum buffile_seek(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_seek);
Datum
buffile_seek(PG_FUNCTION_ARGS)
{
	int32		fileno = PG_GETARG_INT32(0);
	int64		offset = PG_GETARG_INT64(1);
	int32		res;

	check_file();
	res = BufFileSeek(bf, fileno, offset, SEEK_SET);

	PG_RETURN_INT32(res);
}

extern Datum buffile_assert_fileno(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_assert_fileno);
Datum
buffile_assert_fileno(PG_FUNCTION_ARGS)
{
	int32		fileno_expected = PG_GETARG_INT32(0);
	int32		fileno;
	off_t		offset;

	check_file();
	BufFileTell(bf, &fileno, &offset);

	if (fileno != fileno_expected)
	{
		/*
		 * Bring the backend down so that the following tests have no chance
		 * to create the 1GB files.
		 */
		elog(FATAL, "file number does not match");
	}

	PG_RETURN_VOID();
}

static void
check_file(void)
{
	if (bf == NULL)
		elog(ERROR, "the file is not opened");
}

/*
 * This test is especially important for shared encrypted files, see the
 * comments below.
 */
extern Datum buffile_test_shared(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_test_shared);
Datum
buffile_test_shared(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;
	SharedFileSet *fileset;
	BufFile    *bf_1,
			   *bf_2;
	char	   *data_1,
			   *data_2,
			   *data;
	Size		chunk_size_1,
				chunk_size_2;
	int			fileno,
				i;
	off_t		offset,
				res,
				total_size;

	/*
	 * The size is not important, we actually do not need the shared memory.
	 * The segment is only needed to initialize the fileset.
	 */
	seg = dsm_create(1024, 0);

	/*
	 * The fileset must survive error handling, so that dsm_detach works fine.
	 * (The typical use case is that the fileset is in shared memory.)
	 */
	fileset = (SharedFileSet *) MemoryContextAlloc(TopTransactionContext,
												   sizeof(SharedFileSet));
	SharedFileSetInit(fileset, seg);

	bf_1 = BufFileCreateShared(fileset, "file_1");

	/*
	 * Write more data than the buffer size, so that we can check that the
	 * number of "useful bytes" word is only appended at the end of the
	 * segment, not after each buffer.
	 */
	chunk_size_1 = BLCKSZ + 256;
	data_1 = (char *) palloc(chunk_size_1);
	memset(data_1, 1, chunk_size_1);
	if (BufFileWrite(bf_1, data_1, chunk_size_1) != chunk_size_1)
		elog(ERROR, "Failed to write data");
	pfree(data_1);

	/*
	 * Enforce buffer flush (The BufFileFlush() function is not exported).
	 * Thus the "useful bytes" metadata should appear at the current end the
	 * first file segment. The next write will have to seek back to overwrite
	 * the metadata.
	 */
	BufFileTell(bf_1, &fileno, &offset);
	if (BufFileSeek(bf_1, 0, 0, SEEK_SET) != 0)
		elog(ERROR, "seek failed");
	if (BufFileSeek(bf_1, fileno, offset, SEEK_SET) != 0)
		elog(ERROR, "seek failed");

	/*
	 * Write another chunk that does not fit into the first segment file. Thus
	 * the "useful bytes" metadata should appear at the end of both segments.
	 */
	chunk_size_2 = 3 * BLCKSZ;
	data_2 = (char *) palloc(chunk_size_2);
	memset(data_2, 1, chunk_size_2);
	if (BufFileWrite(bf_1, data_2, chunk_size_2) != chunk_size_2)
		elog(ERROR, "Failed to write data");
	pfree(data_2);
	BufFileClose(bf_1);

	/*
	 * The word indicating the number of "useful bytes" (i.e. the actual data
	 * w/o padding to buffer size) is stored at the end of each segment file.
	 * Check that this metadata is read correctly.
	 */
	bf_2 = BufFileOpenShared(fileset, "file_1");
	total_size = BufFileSize(bf_2);
	if (total_size != (chunk_size_1 + chunk_size_2))
		elog(ERROR, "Incorrect file size: %zu", total_size);

	data = (char *) palloc(total_size);
	res = BufFileRead(bf_2, data, total_size);
	if (res != total_size)
		elog(ERROR, "Incorrect chunk size read: %zu", res);
	for (i = 0; i < total_size; i++)
		if (data[i] != 1)
			elog(ERROR, "Unexpected data read from the file");
	pfree(data);
	BufFileClose(bf_2);

	dsm_detach(seg);

	PG_RETURN_VOID();
}


/*
 * Test BufFileAppend().
 */
extern Datum buffile_test_shared_append(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_test_shared_append);
Datum
buffile_test_shared_append(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;
	SharedFileSet *fileset;
	BufFile    *bf_1,
			   *bf_2,
			   *bf_3;
	char	   *data;
	Size		chunk_size;
	int			i;
	off_t		res,
				total_size;

	seg = dsm_create(1024, 0);

	fileset = (SharedFileSet *) MemoryContextAlloc(TopTransactionContext,
												   sizeof(SharedFileSet));
	SharedFileSetInit(fileset, seg);

	/*
	 * XXX Does the chunk size matter much?
	 */
	chunk_size = 8;
	data = (char *) palloc(chunk_size);
	memset(data, 1, chunk_size);

	bf_1 = BufFileCreateShared(fileset, "file_1");
	if (BufFileWrite(bf_1, data, chunk_size) != chunk_size)
		elog(ERROR, "Failed to write data");

	bf_2 = BufFileCreateShared(fileset, "file_2");
	if (BufFileWrite(bf_2, data, chunk_size) != chunk_size)
		elog(ERROR, "Failed to write data");

	/*
	 * Make sure it's read-only so that BufFileAppend() can accept it as
	 * source.
	 */
	BufFileClose(bf_2);
	bf_2 = BufFileOpenShared(fileset, "file_2");

	bf_3 = BufFileCreateShared(fileset, "file_3");
	if (BufFileWrite(bf_3, data, chunk_size) != chunk_size)
		elog(ERROR, "Failed to write data");
	BufFileClose(bf_3);
	bf_3 = BufFileOpenShared(fileset, "file_3");

	BufFileAppend(bf_1, bf_2);
	BufFileAppend(bf_1, bf_3);

	total_size = BufFileSize(bf_1);

	/*
	 * The result should contain complete segments of bf_1 and bf_2 and the
	 * valid part of bf_3.
	 */
	if (total_size != (2 * MAX_PHYSICAL_FILESIZE_TEST + chunk_size))
		elog(ERROR, "Incorrect total size of the appended data: %zu",
			 total_size);

	/*
	 * Check that data of the 2nd segment was decrypted correctly.
	 */
	if (BufFileSeek(bf_1, 1, 0, SEEK_SET) != 0)
		elog(ERROR, "seek failed");
	res = BufFileRead(bf_1, data, chunk_size);
	if (res != chunk_size)
		elog(ERROR, "Incorrect chunk size read: %zu", res);
	for (i = 0; i < chunk_size; i++)
		if (data[i] != 1)
			elog(ERROR, "Unexpected data read from the file");

	/*
	 * And the same for the 3rd segment.
	 *
	 * TODO Reuse the code above by putting it into a function.
	 */
	if (BufFileSeek(bf_1, 2, 0, SEEK_SET) != 0)
		elog(ERROR, "seek failed");
	res = BufFileRead(bf_1, data, chunk_size);
	if (res != chunk_size)
		elog(ERROR, "Incorrect chunk size read: %zu", res);
	for (i = 0; i < chunk_size; i++)
		if (data[i] != 1)
			elog(ERROR, "Unexpected data read from the file");

	BufFileClose(bf_1);
	dsm_detach(seg);
	PG_RETURN_VOID();
}

extern Datum buffile_open_transient(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_open_transient);
Datum
buffile_open_transient(PG_FUNCTION_ARGS)
{
	MemoryContext old_cxt;
	Datum		d = PG_GETARG_DATUM(0);
	char	   *path = TextDatumGetCString(d);
	bool		write_only = PG_GETARG_BOOL(1);
	bool		append = PG_GETARG_BOOL(2);
	int			flags = O_CREAT | PG_BINARY;

	if (bft != NULL)
		elog(ERROR, "file already exists");

	if (write_only)
		flags |= O_WRONLY;
	if (append)
		flags |= O_APPEND;

	old_cxt = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Make sure the file is not deleted across function calls.
	 */
	bft = BufFileOpenTransient(path, flags);

	MemoryContextSwitchTo(old_cxt);

	PG_RETURN_VOID();
}

extern Datum buffile_close_transient(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_close_transient);
Datum
buffile_close_transient(PG_FUNCTION_ARGS)
{
	if (bft == NULL)
		elog(ERROR, "there's no file to close");

	BufFileCloseTransient(bft);
	bft = NULL;

	PG_RETURN_VOID();
}

extern Datum buffile_delete_file(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(buffile_delete_file);
Datum
buffile_delete_file(PG_FUNCTION_ARGS)
{
	Datum		d = PG_GETARG_DATUM(0);
	char	   *path = TextDatumGetCString(d);

	if (bft != NULL)
		elog(ERROR, "the file is still open");

	PathNameDeleteTemporaryFile(path, true);

	PG_RETURN_VOID();
}
