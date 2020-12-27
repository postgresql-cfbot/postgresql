/*-------------------------------------------------------------------------
 *
 * copyfrom_internal.h
 *	  Internal definitions for COPY FROM command.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyfrom_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYFROM_INTERNAL_H
#define COPYFROM_INTERNAL_H

#include "access/parallel.h"
#include "commands/copy.h"
#include "commands/trigger.h"

#define IsHeaderLine()			(cstate->opts.header_line && cstate->cur_lineno == 1)

#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */

/*
 * The macros DATA_BLOCK_SIZE, RINGSIZE & MAX_BLOCKS_COUNT stores the records
 * read from the file that need to be inserted into the relation. These values
 * help in the handover of multiple records with the significant size of data to
 * be processed by each of the workers. This also ensures there is no context
 * switch and the work is fairly distributed among the workers. This number
 * showed best results in the performance tests.
 */
#define DATA_BLOCK_SIZE RAW_BUF_SIZE

/*
 * It can hold MAX_BLOCKS_COUNT blocks of RAW_BUF_SIZE data in DSM to be
 * processed by the worker.
 */
#define MAX_BLOCKS_COUNT 1024

/*
 * It can hold upto RINGSIZE record information for worker to process. RINGSIZE
 * should be a multiple of WORKER_CHUNK_COUNT, as wrap around cases is currently
 * not handled while selecting the WORKER_CHUNK_COUNT by the worker.
 */
#define RINGSIZE (10 * 1024)

/*
 * While accessing DSM, each worker will pick the WORKER_CHUNK_COUNT records
 * from the DSM data blocks at a time and store them in it's local memory. This
 * is to make workers not contend much while getting record information from the
 * DSM. Read RINGSIZE comments before changing this value.
 */
#define WORKER_CHUNK_COUNT 64

#define IsParallelCopy()		(cstate->is_parallel)
#define IsLeader()				(cstate->pcdata->is_leader)
#define IsWorker()				(IsParallelCopy() && !IsLeader())

/*
 * CHECK_FIELD_COUNT - Handles the error cases for field count
 * for binary format files.
 */
#define CHECK_FIELD_COUNT \
{\
	if (fld_count == -1) \
	{ \
		if (IsParallelCopy() && \
			!IsLeader()) \
			return true; \
		else if (IsParallelCopy() && \
			IsLeader()) \
		{ \
			if (cstate->pcdata->curr_data_block->data[cstate->raw_buf_index + sizeof(fld_count)] != 0) \
				ereport(ERROR, \
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT), \
						errmsg("received copy data after EOF marker"))); \
			return true; \
		} \
		else \
		{ \
			/* \
			 * Received EOF marker.  In a V3-protocol copy, wait for the \
			 * protocol-level EOF, and complain if it doesn't come \
			 * immediately.  This ensures that we correctly handle CopyFail, \
			 * if client chooses to send that now. \
			 * \
			 * Note that we MUST NOT try to read more data in an old-protocol \
			 * copy, since there is no protocol-level EOF marker then.  We \
			 * could go either way for copy from file, but choose to throw \
			 * error if there's data after the EOF marker, for consistency \
			 * with the new-protocol case. \
			 */ \
			char		dummy; \
			if (cstate->copy_src != COPY_OLD_FE && \
				CopyReadBinaryData(cstate, &dummy, 1) > 0) \
				ereport(ERROR, \
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT), \
						errmsg("received copy data after EOF marker"))); \
				return false; \
		} \
	} \
	if (fld_count != cstate->max_fields) \
		ereport(ERROR, \
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT), \
				errmsg("row field count is %d, expected %d", \
				(int) fld_count, cstate->max_fields))); \
}

/*
 * CHECK_FIELD_SIZE - Handles the error case for field size
 * for binary format files.
 */
#define CHECK_FIELD_SIZE(fld_size) \
{ \
	if (fld_size < -1) \
		ereport(ERROR, \
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT), \
				errmsg("invalid field size")));\
}

/*
 * EOF_ERROR - Error statement for EOF for binary format
 * files.
 */
#define EOF_ERROR \
{ \
	ereport(ERROR, \
		(errcode(ERRCODE_BAD_COPY_FILE_FORMAT), \
		errmsg("unexpected EOF in COPY data")));\
}

/*
 * GET_RAW_BUF_INDEX - Calculates the raw buf index for the cases
 * where the data spread is across multiple data blocks.
 */
#define GET_RAW_BUF_INDEX(raw_buf_index, fld_size, required_blks, curr_blk_bytes) \
{ \
	raw_buf_index = fld_size - (((required_blks - 1) * DATA_BLOCK_SIZE) + curr_blk_bytes); \
}

/*
 * GET_REQUIRED_BLOCKS - Calculates the number of data
 * blocks required for the cases where the data spread
 * is across multiple data blocks.
 */
#define GET_REQUIRED_BLOCKS(required_blks, fld_size, curr_blk_bytes) \
{ \
	/* \
	 * field size can spread across multiple data blocks, \
	 * calculate the number of required data blocks and try to get \
	 * those many data blocks. \
	 */ \
	required_blks = (int32)(fld_size - curr_blk_bytes)/(int32)DATA_BLOCK_SIZE; \
	/* \
	 * check if we need the data block for the field data \
	 * bytes that are not modulus of data block size. \
	 */ \
	if ((fld_size - curr_blk_bytes)%DATA_BLOCK_SIZE != 0) \
		required_blks++; \
}

/*
 * Represents the different source cases we need to worry about at
 * the bottom level
 */
typedef enum CopySource
{
	COPY_FILE,					/* from file (or a piped program) */
	COPY_OLD_FE,				/* from frontend (2.0 protocol) */
	COPY_NEW_FE,				/* from frontend (3.0 protocol) */
	COPY_CALLBACK				/* from callback function */
} CopySource;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

/*
 * State of the line.
 */
typedef enum ParallelCopyLineState
{
	LINE_INIT,					/* initial state of line */
	LINE_LEADER_POPULATING,		/* leader processing line */
	LINE_LEADER_POPULATED,		/* leader completed populating line */
	LINE_WORKER_PROCESSING,		/* worker processing line */
	LINE_WORKER_PROCESSED		/* worker completed processing line */
} ParallelCopyLineState;

/*
 * Represents the heap insert method to be used during COPY FROM.
 */
typedef enum CopyInsertMethod
{
	CIM_SINGLE,					/* use table_tuple_insert or fdw routine */
	CIM_MULTI,					/* always use table_multi_insert */
	CIM_MULTI_CONDITIONAL		/* use table_multi_insert only if valid */
} CopyInsertMethod;

/*
 * Copy data block information.
 *
 * These data blocks are created in DSM. Data read from file will be copied in
 * these DSM data blocks. The leader process identifies the records and the
 * record information will be shared to the workers. The workers will insert the
 * records into the table. There can be one or more number of records in each of
 * the data block based on the record size.
 */
typedef struct ParallelCopyDataBlock
{
	/* The number of unprocessed lines in the current block. */
	pg_atomic_uint32 unprocessed_line_parts;

	/*
	 * If the current line data is continued into another block,
	 * following_block will have the position where the remaining data need to
	 * be read.
	 */
	uint32		following_block;

	/*
	 * This flag will be set, when the leader finds out this block can be read
	 * safely by the worker. This helps the worker to start processing the
	 * line early where the line will be spread across many blocks and the
	 * worker need not wait for the complete line to be processed.
	 */
	bool		curr_blk_completed;

	/*
	 * Few bytes need to be skipped from this block, this will be set when a
	 * sequence of characters like \r\n is expected, but end of our block
	 * contained only \r. In this case we copy the data from \r into the new
	 * block as they have to be processed together to identify end of line.
	 * Worker will use skip_bytes to know that this data must be skipped from
	 * this data block.
	 */
	uint8		skip_bytes;
	char		data[DATA_BLOCK_SIZE];	/* data read from file */
} ParallelCopyDataBlock;

/*
 * Individual line information.
 *
 * ParallelCopyLineBoundary is common data structure between leader & worker.
 * Leader process will be populating data block, data block offset & the size of
 * the record in DSM for the workers to copy the data into the relation.
 * The leader & worker process access the shared line information by following
 * the below steps to avoid any data corruption or hang:
 *
 * Leader should operate in the following order:
 * 1) check if line_size is -1, if line_size is not -1 wait until line_size is
 * set to -1 by the worker. If line_size is -1 it means worker is still
 * processing.
 * 2) set line_state to LINE_LEADER_POPULATING, so that the worker knows that
 * leader is populating this line.
 * 3) update first_block, start_offset & cur_lineno in any order.
 * 4) update line_size.
 * 5) update line_state to LINE_LEADER_POPULATED.
 *
 * Worker should operate in the following order:
 * 1) check line_state is LINE_LEADER_POPULATED, if not it means leader is still
 * populating the data.
 * 2) read line_size to know the size of the data.
 * 3) only one worker should choose one line for processing, this is handled by
 *    using pg_atomic_compare_exchange_u32, worker will change the state to
 *    LINE_WORKER_PROCESSING if line_state is LINE_LEADER_POPULATED(complete
 *	  line is populated) or if line_state is LINE_LEADER_POPULATING(worker can
 *	  start processing the DSM & release it to the leader).
 * 4) read first_block, start_offset & cur_lineno in any order.
 * 5) process line_size data.
 * 6) update line_size to -1.
 */
typedef struct ParallelCopyLineBoundary
{
	/* Position of the first block in data_blocks array. */
	uint32		first_block;
	uint32		start_offset;	/* start offset of the line */

	/*
	 * Size of the current line -1 means line is yet to be filled completely,
	 * 0 means empty line, >0 means line filled with line size data.
	 */
	pg_atomic_uint32 line_size;
	pg_atomic_uint32 line_state;	/* line state */
	uint64		cur_lineno;		/* line number for error messages */
} ParallelCopyLineBoundary;

/*
 * Circular queue used to store the line information.
 */
typedef struct ParallelCopyLineBoundaries
{
	/* Position for the leader to populate a line. */
	uint32		pos;
	uint32		worker_pos;			/* Worker's last blocked Position. */
	slock_t		worker_pos_lock;	/* locks worker_pos shared variable. */

	/* Data read from the file/stdin by the leader process. */
	ParallelCopyLineBoundary ring[RINGSIZE];
} ParallelCopyLineBoundaries;

/*
 * Shared information among parallel copy workers. This will be allocated in the
 * DSM segment.
 */
typedef struct ParallelCopyShmInfo
{
	bool		is_read_in_progress;	/* file read status */

	/*
	 * Actual lines inserted by worker, will not be same as
	 * total_worker_processed if where condition is specified along with copy.
	 * This will be the actual records inserted into the relation.
	 */
	pg_atomic_uint64 processed;

	/*
	 * The number of records currently processed by the worker, this will also
	 * include the number of records that was filtered because of where
	 * clause.
	 */
	pg_atomic_uint64 total_worker_processed;
	uint64		populated;		/* lines populated by leader */
	uint32		cur_block_pos;	/* current data block */
	ParallelCopyDataBlock data_blocks[MAX_BLOCKS_COUNT];	/* data block array */
	ParallelCopyLineBoundaries line_boundaries; /* line array */
} ParallelCopyShmInfo;

/*
 * Parallel copy line buffer information.
 */
typedef struct ParallelCopyLineBuf
{
	StringInfoData line_buf;
	uint64		cur_lineno;		/* line number for error messages */
} ParallelCopyLineBuf;

/*
 * Represents the usage mode for CopyReadBinaryGetDataBlock.
 */
typedef enum FieldInfoType
{
	FIELD_NONE = 0,
	FIELD_COUNT,
	FIELD_SIZE,
	FIELD_DATA
}			FieldInfoType;

/*
 * This structure helps in storing the common List/Node from CopyStateData that
 * are required by the workers. This information will then be copied and stored
 * into the DSM for the worker to retrieve and copy it to CopyStateData.
 */
typedef struct SerializedListToStrCState
{
	char	   *whereClauseStr;
	char	   *rangeTableStr;
	char	   *attnameListStr;
	char	   *notnullListStr;
	char	   *nullListStr;
	char	   *convertListStr;
} SerializedListToStrCState;

/*
 * Parallel copy data information.
 */
typedef struct ParallelCopyData
{
	Oid			relid;			/* relation id of the table */
	ParallelCopyShmInfo *pcshared_info; /* common info in shared memory */
	bool		is_leader;

	/* line position which worker is processing */
	uint32		worker_processed_pos;

	WalUsage   *walusage;
	BufferUsage *bufferusage;

	/*
	 * Local line_buf array, workers will copy it here and release the lines
	 * for the leader to continue.
	 */
	ParallelCopyLineBuf worker_line_buf[WORKER_CHUNK_COUNT];
	uint32		worker_line_buf_count;	/* Number of lines */

	/* Current position in worker_line_buf */
	uint32		worker_line_buf_pos;

	/* For binary formatted files */
	ParallelCopyDataBlock *curr_data_block;
} ParallelCopyData;


/*
 * This struct contains all the state variables used throughout a COPY FROM
 * operation.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is true
 * when we have to do it the hard way.
 */
typedef struct CopyFromStateData
{
	/* low-level state data */
	CopySource	copy_src;		/* type of copy source */
	FILE	   *copy_file;		/* used if copy_src == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used if copy_src == COPY_NEW_FE */
	bool		reached_eof;	/* true if we read to end of copy data (not
								 * all copy_src types maintain this) */

	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb; /* function for reading data */

	CopyFormatOptions opts;
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	uint64		cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	AttrNumber	num_defaults;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;	/* is any of defexprs volatile? */
	List	   *range_table;
	ExprState  *qualexpr;

	TransitionCaptureState *transition_capture;

	/*
	 * These variables are used to reduce overhead in COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 *
	 * In binary COPY FROM, attribute_buf holds the binary data for the
	 * current field, but the usage is otherwise similar.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.  (In binary mode,
	 * line_buf is not used.)
	 */
	StringInfoData line_buf;
	bool		line_buf_converted; /* converted to server encoding? */
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).  In text mode, CopyReadLine parses this data
	 * sufficiently to locate line boundaries, then transfers the data to
	 * line_buf and converts it.  In binary mode, CopyReadBinaryData fetches
	 * appropriate amounts of data from this buffer.  In both modes, we
	 * guarantee that there is a \0 at raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
	bool		is_parallel;
	ParallelCopyData *pcdata;

	/* Shorthand for number of unconsumed bytes available in raw_buf */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)
} CopyFromStateData;

extern void ReceiveCopyBegin(CopyFromState cstate);
extern void ReceiveCopyBinaryHeader(CopyFromState cstate);
extern void PopulateCommonCStateInfo(CopyFromState cstate, TupleDesc tup_desc,
									 List *attnamelist);
extern void ConvertToServerEncoding(CopyFromState cstate);

extern void ParallelCopyMain(dsm_segment *seg, shm_toc *toc);
extern ParallelContext *BeginParallelCopy(CopyFromState cstate, List *attnamelist, Oid relid);
extern void ParallelCopyFrom(CopyFromState cstate);
extern void EndParallelCopy(ParallelContext *pcxt);
extern bool IsParallelCopyAllowed(CopyFromState cstate, Oid relid);
extern void ExecBeforeStmtTrigger(CopyFromState cstate);
extern void CheckTargetRelValidity(CopyFromState cstate);
extern void PopulateCStateCatalogInfo(CopyFromState cstate);
extern uint32 GetLinePosition(CopyFromState cstate, uint32 line_pos);
extern bool GetWorkerLine(CopyFromState cstate);
extern bool CopyReadLine(CopyFromState cstate);
extern uint32 WaitGetFreeCopyBlock(ParallelCopyShmInfo *pcshared_info);
extern void SetRawBufForLoad(CopyFromState cstate, uint32 line_size, uint32 copy_buf_len,
							 uint32 raw_buf_ptr, char **copy_raw_buf);
extern uint32 UpdateSharedLineInfo(CopyFromState cstate, uint32 blk_pos, uint32 offset,
								   uint32 line_size, uint32 line_state, uint32 blk_line_pos);
extern void EndLineParallelCopy(CopyFromState cstate, uint32 line_pos, uint32 line_size,
								uint32 raw_buf_ptr);

extern int	CopyGetData(CopyFromState cstate, void *databuf, int minread, int maxread);
extern int	CopyReadBinaryData(CopyFromState cstate, char *dest, int nbytes);
extern bool CopyReadBinaryTupleLeader(CopyFromState cstate);
extern bool CopyReadBinaryTupleWorker(CopyFromState cstate, Datum *values, bool *nulls);
extern void CopyReadBinaryFindTupleSize(CopyFromState cstate, uint32 *line_size);
extern Datum CopyReadBinaryAttributeWorker(CopyFromState cstate, FmgrInfo *flinfo,
										   Oid typioparam, int32 typmod, bool *isnull);
extern void CopyReadBinaryGetDataBlock(CopyFromState cstate, FieldInfoType field_info);

#endif							/* COPYFROM_INTERNAL_H */
