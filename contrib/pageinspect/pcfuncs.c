/*-------------------------------------------------------------------------
 *
 * pcfuncs.c
 *	  Functions to investigate the content of the address file for compressed
 * relation.
 *
 *
 * Copyright (c) 2007-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pageinspect/pcfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pageinspect.h"
#include "storage/page_compression.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/varlena.h"


static PageCompressHeader *get_compress_address_contents_internal(text *relname, int segno);


/*
 * get_compress_address_header
 *
 * Allows inspection of header fields of the address file for a compressed
 * relation.
 */
PG_FUNCTION_INFO_V1(get_compress_address_header);

Datum
get_compress_address_header(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int32		segno = PG_GETARG_INT32(1);
	Datum		result;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	Datum		values[6];
	bool		nulls[6];
	PageCompressHeader *pcmap;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (segno < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid segment number")));

	pcmap = get_compress_address_contents_internal(relname, segno);
	if (pcmap == NULL)
		PG_RETURN_NULL();

	values[0] = UInt32GetDatum(pg_atomic_read_u32(&pcmap->nblocks));
	values[1] = UInt32GetDatum(pg_atomic_read_u32(&pcmap->allocated_chunks));
	values[2] = UInt32GetDatum(pcmap->chunk_size);
	values[3] = UInt32GetDatum(pcmap->algorithm);
	values[4] = UInt32GetDatum(pg_atomic_read_u32(&pcmap->last_synced_nblocks));
	values[5] = UInt32GetDatum(pg_atomic_read_u32(&pcmap->last_synced_allocated_chunks));

	pfree(pcmap);

	memset(nulls, 0, sizeof(nulls));

	/* Build and return the result tuple */
	tuple = heap_form_tuple(tupleDesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * get_compress_address_items
 *
 * Allows inspection of the address contents for a compressed relation.
 */
PG_FUNCTION_INFO_V1(get_compress_address_items);

typedef struct compress_address_items_state
{
	TupleDesc	tupd;
	uint32		blkno;
	PageCompressHeader *pcmap;
} compress_address_items_state;

Datum
get_compress_address_items(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int32		segno = PG_GETARG_INT32(1);
	compress_address_items_state *inter_call_data = NULL;
	FuncCallContext *fctx;
	PageCompressHeader *pcmap;
	PageCompressAddr *pcaddr;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	if (segno < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid segment number")));

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext mctx;
		uint32		blkno;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		inter_call_data = palloc(sizeof(compress_address_items_state));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		inter_call_data->tupd = tupdesc;
		inter_call_data->blkno = 0;

		pcmap = get_compress_address_contents_internal(relname, segno);

		inter_call_data->pcmap = pcmap;
		if (pcmap)
		{
			/* find the largest page with a non-empty address */
			fctx->max_calls = pg_atomic_read_u32(&pcmap->nblocks);
			for (blkno = fctx->max_calls; blkno < RELSEG_SIZE; blkno++)
			{
				pcaddr = GetPageCompressAddr(pcmap,
											 pcmap->chunk_size,
											 blkno);
				if (pcaddr->allocated_chunks != 0)
					fctx->max_calls = blkno + 1;
			}
		}
		else
			fctx->max_calls = 0;

		fctx->user_fctx = inter_call_data;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	inter_call_data = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
		Datum		result;
		HeapTuple	tuple;
		int			i,
					len,
					chunk_count;
		char	   *values[5];
		char		buf[256];
		char	   *p;

		pcmap = inter_call_data->pcmap;
		pcaddr = GetPageCompressAddr(pcmap,
									 pcmap->chunk_size,
									 inter_call_data->blkno);

		/* Extract information from the compress address */
		p = buf;
		snprintf(p, sizeof(buf) - (p - buf), "{");
		p++;

		/* skip invalid chunkno at tail */
		chunk_count = MaxChunksPreCompressedPage(pcmap->chunk_size);
		while (pcaddr->chunknos[chunk_count - 1] == 0)
			chunk_count--;

		for (i = 0; i < chunk_count; i++)
		{
			if (i == 0)
				len = snprintf(p, sizeof(buf) - (p - buf), "%d", pcaddr->chunknos[i]);
			else
				len = snprintf(p, sizeof(buf) - (p - buf), ",%d", pcaddr->chunknos[i]);
			p += len;
		}
		snprintf(p, sizeof(buf) - (p - buf), "}");

		values[0] = psprintf("%d", inter_call_data->blkno);
		values[1] = psprintf("%d", pcaddr->nchunks);
		values[2] = psprintf("%d", pcaddr->allocated_chunks);
		values[3] = psprintf("%s", buf);

		/* Build and return the result tuple. */
		tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(inter_call_data->tupd),
									   values);

		result = HeapTupleGetDatum(tuple);

		inter_call_data->blkno++;

		SRF_RETURN_NEXT(fctx, result);
	}
	else
		SRF_RETURN_DONE(fctx);
}

/*
 * get_compress_address_contents_internal
 *
 * Returns raw contents of compressed address file
 */
static PageCompressHeader *
get_compress_address_contents_internal(text *relname, int segno)
{
	RangeVar   *relrv;
	Relation	rel;
	PageCompressHeader *pcmap;
	PageCompressHeader *result = NULL;
	int			i;

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	/* Check that this relation support compression */
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get compressed address contents of relation \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_relkind_not_supported(rel->rd_rel->relkind)));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	if (rel->rd_locator.compressOpt.algorithm != PAGE_COMPRESSION_NONE)
	{
		/* Get file path */
		char	   *path,
				   *pca_path;
		int			file,
					chunk_size;

		path = relpathbackend(rel->rd_locator, rel->rd_backend, MAIN_FORKNUM);
		if (segno > 0)
			pca_path = psprintf("%s.%u_pca", path, segno);
		else
			pca_path = psprintf("%s_pca", path);
		pfree(path);

		file = open(pca_path, PG_BINARY | O_RDONLY, 0);

		if (file < 0)
		{
			if (errno == ENOENT)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("segment number %u is out of range for relation \"%s\"",
								segno, RelationGetRelationName(rel))));
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", pca_path)));
		}

		chunk_size = BLCKSZ / rel->rd_locator.compressOpt.chunks_pre_block;
		pcmap = pc_mmap(file, chunk_size, true);
		if (pcmap == MAP_FAILED)
			ereport(ERROR,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not mmap file \"%s\": %m",
							pca_path)));

		result = (PageCompressHeader *) palloc(SizeofPageCompressAddrFile(chunk_size));

		/*
		 * Make a member-by-member copy to ensure that each member is read
		 * atomically
		 */
		for (i = 0; i < SizeofPageCompressAddrFile(chunk_size) / sizeof(int); i++)
			((int *) result)[i] = ((int *) pcmap)[i];

		pc_munmap(pcmap);
		close(file);
		pfree(pca_path);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an compressed relation",
						RelationGetRelationName(rel))));
	}

	relation_close(rel, AccessShareLock);

	return result;
}


/*
 * page_compress
 *
 * compress one raw page
 */

PG_FUNCTION_INFO_V1(page_compress);

Datum
page_compress(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	char	   *algorithm_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int32		level = PG_GETARG_INT32(2);
	char	   *work_buffer;
	int			raw_page_size;
	int			work_buffer_size;
	int8		algorithm;
	bytea	   *result;
	int			nbytes;
	PageHeader	page;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	raw_page_size = VARSIZE(raw_page) - VARHDRSZ;

	/*
	 * Check that enough data was supplied, so that we don't try to access
	 * fields outside the supplied buffer.
	 */
	if (raw_page_size < BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page too small (%d bytes)", raw_page_size)));

	page = (PageHeader) VARDATA(raw_page);

	/* compress page */
	if (strcmp(algorithm_name, "pglz") == 0)
		algorithm = PAGE_COMPRESSION_PGLZ;
	else if (strcmp(algorithm_name, "lz4") == 0)
		algorithm = PAGE_COMPRESSION_LZ4;
	else if (strcmp(algorithm_name, "zstd") == 0)
		algorithm = PAGE_COMPRESSION_ZSTD;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unrecognized compression algorithm \"%s\"", algorithm_name)));

	work_buffer_size = compress_page_buffer_bound(algorithm);
	if (work_buffer_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unrecognized compression algorithm %d", algorithm)));
	work_buffer = palloc(work_buffer_size);

	nbytes = compress_page((char *) page, work_buffer, work_buffer_size, algorithm, level);

	if (nbytes < 0)
		elog(ERROR, "compression failed with algorithm %d", algorithm);

	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	memcpy(VARDATA(result), work_buffer, nbytes);

	pfree(work_buffer);

	PG_RETURN_BYTEA_P(result);
}
