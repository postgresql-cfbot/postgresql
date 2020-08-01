/*-------------------------------------------------------------------------
 *
 * copy.c
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')

/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE,				/* to/from frontend (3.0 protocol) */
	COPY_CALLBACK				/* to/from callback function */
} CopyDest;

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
 * Represents the heap insert method to be used during COPY FROM.
 */
typedef enum CopyInsertMethod
{
	CIM_SINGLE,					/* use table_tuple_insert or fdw routine */
	CIM_MULTI,					/* always use table_multi_insert */
	CIM_MULTI_CONDITIONAL		/* use table_multi_insert only if valid */
} CopyInsertMethod;

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
}ParallelCopyLineState;

#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */

/*
 * The macros DATA_BLOCK_SIZE, RINGSIZE & MAX_BLOCKS_COUNT stores the records
 * read from the file that need to be inserted into the relation. These values
 * help in handover of multiple records with significant size of data to be
 * processed by each of the workers to make sure there is no context switch & the
 * work is fairly distributed among the workers. This number showed best
 * results in the performance tests.
 */
#define DATA_BLOCK_SIZE RAW_BUF_SIZE

/* It can hold upto 10000 record information for worker to process. */
#define RINGSIZE (10 * 1000)

/* It can hold 1000 blocks of 64K data in DSM to be processed by the worker. */
#define MAX_BLOCKS_COUNT 1000

/*
 * Each worker will be allocated WORKER_CHUNK_COUNT of records from DSM data
 * block to process to avoid lock contention. This value should be mode of
 * RINGSIZE, as wrap around cases is currently not handled while selecting the
 * WORKER_CHUNK_COUNT by the worker.
 */
#define WORKER_CHUNK_COUNT 50

#define	IsParallelCopy()		(cstate->is_parallel)
#define IsLeader()				(cstate->pcdata->is_leader)
#define IsWorker()				(IsParallelCopy() && !IsLeader())
#define IsHeaderLine()			(cstate->header_line && cstate->cur_lineno == 1)

/*
 * Copy data block information.
 * ParallelCopyDataBlock's will be created in DSM. Data read from file will be
 * copied in these DSM data blocks. The leader process identifies the records
 * and the record information will be shared to the workers. The workers will
 * insert the records into the table. There can be one or more number of records
 * in each of the data block based on the record size.
 */
typedef struct ParallelCopyDataBlock
{
	/* The number of unprocessed lines in the current block. */
	pg_atomic_uint32 unprocessed_line_parts;

	/*
	 * If the current line data is continued into another block, following_block
	 * will have the position where the remaining data need to be read.
	 */
	uint32	following_block;

	/*
	 * This flag will be set, when the leader finds out this block can be read
	 * safely by the worker. This helps the worker to start processing the line
	 * early where the line will be spread across many blocks and the worker
	 * need not wait for the complete line to be processed.
	 */
	bool   curr_blk_completed;
	char   data[DATA_BLOCK_SIZE]; /* data read from file */
	uint8  skip_bytes;
}ParallelCopyDataBlock;

/*
 * Individual line information.
 * ParallelCopyLineBoundary is common data structure between leader & worker,
 * Leader process will be populating data block, data block offset & the size of
 * the record in DSM for the workers to copy the data into the relation.
 * This is protected by the following sequence in the leader & worker. If they
 * don't follow this order the worker might process wrong line_size and leader
 * might populate the information which worker has not yet processed or in the
 * process of processing.
 * Leader should operate in the following order:
 * 1) check if line_size is -1, if not wait, it means worker is still
 * processing.
 * 2) set line_state to LINE_LEADER_POPULATING.
 * 3) update first_block, start_offset & cur_lineno in any order.
 * 4) update line_size.
 * 5) update line_state to LINE_LEADER_POPULATED.
 * Worker should operate in the following order:
 * 1) check line_state is LINE_LEADER_POPULATED, if not it means leader is still
 * populating the data.
 * 2) read line_size.
 * 3) only one worker should choose one line for processing, this is handled by
 *    using pg_atomic_compare_exchange_u32, worker will change the sate to
 *    LINE_WORKER_PROCESSING only if line_state is LINE_LEADER_POPULATED.
 * 4) read first_block, start_offset & cur_lineno in any order.
 * 5) process line_size data.
 * 6) update line_size to -1.
 */
typedef struct ParallelCopyLineBoundary
{
	/* Position of the first block in data_blocks array. */
	uint32				first_block;
	uint32				start_offset;   /* start offset of the line */

	/*
	 * Size of the current line -1 means line is yet to be filled completely,
	 * 0 means empty line, >0 means line filled with line size data.
	 */
	pg_atomic_uint32	line_size;
	pg_atomic_uint32	line_state;		/* line state */
	uint64				cur_lineno;		/* line number for error messages */
}ParallelCopyLineBoundary;

/*
 * Circular queue used to store the line information.
 */
typedef struct ParallelCopyLineBoundaries
{
	/* Position for the leader to populate a line. */
	uint32			pos;

	/* Data read from the file/stdin by the leader process. */
	ParallelCopyLineBoundary	ring[RINGSIZE];
}ParallelCopyLineBoundaries;

/*
 * Shared information among parallel copy workers. This will be allocated in the
 * DSM segment.
 */
typedef struct ParallelCopyShmInfo
{
	bool					is_read_in_progress; /* file read status */

	/*
	 * Actual lines inserted by worker, will not be same as
	 * total_worker_processed if where condition is specified along with copy.
	 * This will be the actual records inserted into the relation.
	 */
	pg_atomic_uint64			processed;

	/*
	 * The number of records currently processed by the worker, this will also
	 * include the number of records that was filtered because of where clause.
	 */
	pg_atomic_uint64			total_worker_processed;
	uint64						populated; /* lines populated by leader */
	uint32						cur_block_pos; /* current data block */
	ParallelCopyDataBlock		data_blocks[MAX_BLOCKS_COUNT]; /* data block array */
	FullTransactionId			full_transaction_id; /* xid for copy from statement */
	CommandId					mycid;	/* command id */
	ParallelCopyLineBoundaries	line_boundaries; /* line array */
} ParallelCopyShmInfo;

/*
 * Parallel copy line buffer information.
 */
typedef struct ParallelCopyLineBuf
{
	StringInfoData		line_buf;
	uint64				cur_lineno;	/* line number for error messages */
}ParallelCopyLineBuf;

/*
 * Parallel copy data information.
 */
typedef struct ParallelCopyData
{
	Oid					relid;				/* relation id of the table */
	ParallelCopyShmInfo	*pcshared_info;		/* common info in shared memory */
	bool				is_leader;

	/* line position which worker is processing */
	uint32				worker_processed_pos;

	/*
	 * Local line_buf array, workers will copy it here and release the lines
	 * for the leader to continue.
	 */
	ParallelCopyLineBuf	worker_line_buf[WORKER_CHUNK_COUNT];
	uint32				worker_line_buf_count; /* Number of lines */

	/* Current position in worker_line_buf */
	uint32				worker_line_buf_pos;
}ParallelCopyData;

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
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
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		is_copy_from;	/* COPY TO, or COPY FROM? */
	bool		reached_eof;	/* true if we read to end of copy data (not
								 * all copy_dest types maintain this) */
	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to or from */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb; /* function for reading data */
	bool		binary;			/* binary format? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	bool		header_line;	/* CSV header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select; /* list of column names (can be NIL) */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	uint64		cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
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
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
	int			nworkers;
	bool		is_parallel;
	ParallelCopyData *pcdata;
	/* Shorthand for number of unconsumed bytes available in raw_buf */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)
} CopyStateData;

/*
 * This structure helps in storing the common data from CopyStateData that are
 * required by the workers. This information will then be allocated and stored
 * into the DSM for the worker to retrieve and copy it to CopyStateData.
 */
typedef struct SerializedParallelCopyState
{
	/* low-level state data */
	CopyDest            copy_dest;		/* type of copy source/destination */
	int                 file_encoding;	/* file or remote side's character encoding */
	bool                need_transcoding;	/* file encoding diff from server? */
	bool                encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	bool                csv_mode;		/* Comma Separated Value format? */
	bool                header_line;	/* CSV header line? */
	int                 null_print_len; /* length of same */
	bool                force_quote_all;	/* FORCE_QUOTE *? */
	bool                convert_selectively;	/* do selective binary conversion? */

	/* Working state for COPY FROM */
	AttrNumber          num_defaults;
	Oid                 relid;
}SerializedParallelCopyState;

/* DestReceiver for COPY (query) TO */
typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	CopyState	cstate;			/* CopyStateData for the command */
	uint64		processed;		/* # of tuples processed */
} DR_copy;


/*
 * No more than this many tuples per CopyMultiInsertBuffer
 *
 * Caution: Don't make this too big, as we could end up with this many
 * CopyMultiInsertBuffer items stored in CopyMultiInsertInfo's
 * multiInsertBuffers list.  Increasing this can cause quadratic growth in
 * memory requirements during copies into partitioned tables with a large
 * number of partitions.
 */
#define MAX_BUFFERED_TUPLES		1000

/*
 * Flush buffers if there are >= this many bytes, as counted by the input
 * size, of tuples stored.
 */
#define MAX_BUFFERED_BYTES		65535

/* Trim the list of buffers back down to this number after flushing */
#define MAX_PARTITION_BUFFERS	32

/* DSM keys for parallel copy.  */
#define PARALLEL_COPY_KEY_SHARED_INFO		    1
#define PARALLEL_COPY_KEY_CSTATE				2
#define PARALLEL_COPY_KEY_NULL_PRINT			3
#define PARALLEL_COPY_KEY_DELIM					4
#define PARALLEL_COPY_KEY_QUOTE					5
#define PARALLEL_COPY_KEY_ESCAPE				6
#define PARALLEL_COPY_KEY_ATTNAME_LIST			7
#define PARALLEL_COPY_KEY_NOT_NULL_LIST		    8
#define PARALLEL_COPY_KEY_NULL_LIST			    9
#define PARALLEL_COPY_KEY_CONVERT_LIST		   10
#define PARALLEL_COPY_KEY_WHERE_CLAUSE_STR     11
#define PARALLEL_COPY_KEY_RANGE_TABLE		   12
#define PARALLEL_COPY_WAL_USAGE				   13
#define PARALLEL_COPY_BUFFER_USAGE			   14

/* Stores multi-insert data related to a single relation in CopyFrom. */
typedef struct CopyMultiInsertBuffer
{
	TupleTableSlot *slots[MAX_BUFFERED_TUPLES]; /* Array to store tuples */
	ResultRelInfo *resultRelInfo;	/* ResultRelInfo for 'relid' */
	BulkInsertState bistate;	/* BulkInsertState for this rel */
	int			nused;			/* number of 'slots' containing tuples */
	uint64		linenos[MAX_BUFFERED_TUPLES];	/* Line # of tuple in copy
												 * stream */
} CopyMultiInsertBuffer;

/*
 * Stores one or many CopyMultiInsertBuffers and details about the size and
 * number of tuples which are stored in them.  This allows multiple buffers to
 * exist at once when COPYing into a partitioned table.
 */
typedef struct CopyMultiInsertInfo
{
	List	   *multiInsertBuffers; /* List of tracked CopyMultiInsertBuffers */
	int			bufferedTuples; /* number of tuples buffered over all buffers */
	int			bufferedBytes;	/* number of bytes from all buffered tuples */
	CopyState	cstate;			/* Copy state for this CopyMultiInsertInfo */
	EState	   *estate;			/* Executor state used for COPY */
	CommandId	mycid;			/* Command Id used for COPY */
	int			ti_options;		/* table insert options */
} CopyMultiInsertInfo;


/*
 * These macros centralize code used to process line_buf and raw_buf buffers.
 * They are macros because they often do continue/break control and to avoid
 * function call overhead in tight COPY loops.
 *
 * We must use "if (1)" because the usual "do {...} while(0)" wrapper would
 * prevent the continue/break processing from working.  We end the "if (1)"
 * with "else ((void) 0)" to ensure the "if" does not unintentionally match
 * any "else" in the calling code, and to avoid any compiler warnings about
 * empty statements.  See http://www.cit.gu.edu.au/~anthony/info/C/C.macros.
 */

/*
 * This keeps the character read at the top of the loop in the buffer
 * even if there is more than one read-ahead.
 */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen) \
if (1) \
{ \
	if (raw_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) \
	{ \
		raw_buf_ptr = prev_raw_ptr; /* undo fetch */ \
		need_data = true; \
		continue; \
	} \
} else ((void) 0)

/* This consumes the remainder of the buffer and breaks */
#define IF_NEED_REFILL_AND_EOF_BREAK(extralen) \
if (1) \
{ \
	if (raw_buf_ptr + (extralen) >= copy_buf_len && hit_eof) \
	{ \
		if (extralen) \
			raw_buf_ptr = copy_buf_len; /* consume the partial character */ \
		/* backslash just before EOF, treat as data char */ \
		result = true; \
		break; \
	} \
} else ((void) 0)

/*
 * Transfer any approved data to line_buf; must do this to be sure
 * there is some room in raw_buf.
 */
#define REFILL_LINEBUF \
if (1) \
{ \
	if (raw_buf_ptr > cstate->raw_buf_index) \
	{ \
		if (!IsParallelCopy()) \
			appendBinaryStringInfo(&cstate->line_buf, \
								   cstate->raw_buf + cstate->raw_buf_index, \
								   raw_buf_ptr - cstate->raw_buf_index); \
		else \
			line_size +=  raw_buf_ptr - cstate->raw_buf_index; \
		\
		cstate->raw_buf_index = raw_buf_ptr; \
	} \
} else ((void) 0)

/* Undo any read-ahead and jump out of the block. */
#define NO_END_OF_COPY_GOTO \
if (1) \
{ \
	raw_buf_ptr = prev_raw_ptr + 1; \
	goto not_end_of_copy; \
} else ((void) 0)

/* Begin parallel copy Macros */
#define SET_NEWLINE_SIZE() \
{ \
	if (cstate->eol_type == EOL_NL || cstate->eol_type == EOL_CR) \
		new_line_size = 1; \
	else if (cstate->eol_type == EOL_CRNL) \
		new_line_size = 2; \
	else \
		new_line_size = 0; \
}

/*
 * COPY_WAIT_TO_PROCESS - Wait before continuing to process.
 */
#define COPY_WAIT_TO_PROCESS() \
{ \
	CHECK_FOR_INTERRUPTS(); \
	(void) WaitLatch(MyLatch, \
					 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, \
					 1L, WAIT_EVENT_PG_SLEEP); \
	ResetLatch(MyLatch); \
}

/*
 * CLEAR_EOL_LINE - Wrapper for clearing EOL.
 */
#define CLEAR_EOL_LINE() \
if (!result && !IsHeaderLine()) \
{ \
	if (IsParallelCopy()) \
		ClearEOLFromCopiedData(cstate, cstate->raw_buf, \
									raw_buf_ptr, &line_size); \
	else \
		ClearEOLFromCopiedData(cstate, cstate->line_buf.data, \
									cstate->line_buf.len, \
									&cstate->line_buf.len); \
} \

/*
 * INCREMENTPROCESSED - Increment the lines processed.
 */
#define INCREMENTPROCESSED(processed) \
{ \
	if (!IsParallelCopy()) \
		processed++; \
	else \
		pg_atomic_add_fetch_u64(&cstate->pcdata->pcshared_info->processed, 1); \
}

/*
 * GETPROCESSED - Get the lines processed.
 */
#define GETPROCESSED(processed) \
if (!IsParallelCopy()) \
	return processed; \
else \
	return pg_atomic_read_u64(&cstate->pcdata->pcshared_info->processed);

/* End parallel copy Macros */

static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/* non-export function prototypes */
static CopyState BeginCopy(ParseState *pstate, bool is_from, Relation rel,
						   RawStmt *raw_query, Oid queryRelId, List *attnamelist,
						   List *options);
static void EndCopy(CopyState cstate);
static void ClosePipeToProgram(CopyState cstate);
static CopyState BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *query,
							 Oid queryRelId, const char *filename, bool is_program,
							 List *attnamelist, List *options);
static void EndCopyTo(CopyState cstate);
static uint64 DoCopyTo(CopyState cstate);
static uint64 CopyTo(CopyState cstate);
static void CopyOneRowTo(CopyState cstate, TupleTableSlot *slot);
static bool CopyReadLine(CopyState cstate);
static bool CopyReadLineText(CopyState cstate);
static int	CopyReadAttributesText(CopyState cstate);
static int	CopyReadAttributesCSV(CopyState cstate);
static Datum CopyReadBinaryAttribute(CopyState cstate, FmgrInfo *flinfo,
									 Oid typioparam, int32 typmod,
									 bool *isnull);
static void CopyAttributeOutText(CopyState cstate, char *string);
static void CopyAttributeOutCSV(CopyState cstate, char *string,
								bool use_quote, bool single_attr);
static List *CopyGetAttnums(TupleDesc tupDesc, Relation rel,
							List *attnamelist);
static char *limit_printout_length(const char *str);

/* Low-level communications functions */
static void SendCopyBegin(CopyState cstate);
static void ReceiveCopyBegin(CopyState cstate);
static void SendCopyEnd(CopyState cstate);
static void CopySendData(CopyState cstate, const void *databuf, int datasize);
static void CopySendString(CopyState cstate, const char *str);
static void CopySendChar(CopyState cstate, char c);
static void CopySendEndOfRow(CopyState cstate);
static int	CopyGetData(CopyState cstate, void *databuf,
						int minread, int maxread);
static void CopySendInt32(CopyState cstate, int32 val);
static bool CopyGetInt32(CopyState cstate, int32 *val);
static void CopySendInt16(CopyState cstate, int16 val);
static bool CopyGetInt16(CopyState cstate, int16 *val);
static bool CopyLoadRawBuf(CopyState cstate);
static int	CopyReadBinaryData(CopyState cstate, char *dest, int nbytes);

static pg_attribute_always_inline void EndParallelCopy(ParallelContext *pcxt);
static void PopulateCommonCstateInfo(CopyState cstate, TupleDesc	tup_desc,
									 List *attnamelist);
static void ClearEOLFromCopiedData(CopyState cstate, char *copy_line_data,
								   int copy_line_pos, int *copy_line_size);
static void ConvertToServerEncoding(CopyState cstate);
static void ExecBeforeStmtTrigger(CopyState cstate);
static void CheckTargetRelValidity(CopyState cstate);
static void PopulateCstateCatalogInfo(CopyState cstate);
static pg_attribute_always_inline uint32 GetLinePosition(CopyState cstate);
/*
 * SerializeParallelCopyState - Copy shared_cstate using cstate information.
 */
static pg_attribute_always_inline void
SerializeParallelCopyState(CopyState cstate, SerializedParallelCopyState *shared_cstate)
{
	shared_cstate->copy_dest = cstate->copy_dest;
	shared_cstate->file_encoding = cstate->file_encoding;
	shared_cstate->need_transcoding = cstate->need_transcoding;
	shared_cstate->encoding_embeds_ascii = cstate->encoding_embeds_ascii;
	shared_cstate->csv_mode = cstate->csv_mode;
	shared_cstate->header_line = cstate->header_line;
	shared_cstate->null_print_len = cstate->null_print_len;
	shared_cstate->force_quote_all = cstate->force_quote_all;
	shared_cstate->convert_selectively = cstate->convert_selectively;
	shared_cstate->num_defaults = cstate->num_defaults;
	shared_cstate->relid = cstate->pcdata->relid;
}

/*
 * RestoreString - Retrieve the string from shared memory.
 */
static void
RestoreString(shm_toc *toc, int sharedkey, char **copystr)
{
	char *shared_str_val = (char *)shm_toc_lookup(toc, sharedkey, true);
	if (shared_str_val)
		*copystr = pstrdup(shared_str_val);
}

/*
 * EstimateLineKeysStr - Estimate the size required in shared memory for the
 * input string.
 */
static void
EstimateLineKeysStr(ParallelContext *pcxt, char *inputstr)
{
	if (inputstr)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, strlen(inputstr) + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
}

/*
 * SerializeString - Insert a string into shared memory.
 */
static void
SerializeString(ParallelContext *pcxt, int key, char *inputstr)
{
	if (inputstr)
	{
		char *shmptr = (char *)shm_toc_allocate(pcxt->toc,
												strlen(inputstr) + 1);
		strcpy(shmptr, inputstr);
		shm_toc_insert(pcxt->toc, key, shmptr);
	}
}

/*
 * PopulateParallelCopyShmInfo - set ParallelCopyShmInfo.
 */
static void
PopulateParallelCopyShmInfo(ParallelCopyShmInfo *shared_info_ptr,
							FullTransactionId full_transaction_id)
{
	uint32 count;

	MemSet(shared_info_ptr, 0, sizeof(ParallelCopyShmInfo));
	shared_info_ptr->is_read_in_progress = true;
	shared_info_ptr->cur_block_pos = -1;
	shared_info_ptr->full_transaction_id = full_transaction_id;
	shared_info_ptr->mycid = GetCurrentCommandId(true);
	for (count = 0; count < RINGSIZE; count++)
	{
		ParallelCopyLineBoundary *lineInfo = &shared_info_ptr->line_boundaries.ring[count];
		pg_atomic_init_u32(&(lineInfo->line_size), -1);
	}
}

/*
 * IsTriggerFunctionParallelSafe - Check if the trigger function is parallel
 * safe for the triggers. Return false if any one of the trigger has parallel
 * unsafe function.
 */
static pg_attribute_always_inline bool
IsTriggerFunctionParallelSafe(TriggerDesc *trigdesc)
{
	int	i;
	for (i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trigger = &trigdesc->triggers[i];
		int 		trigtype = RI_TRIGGER_NONE;

		if (func_parallel(trigger->tgfoid) != PROPARALLEL_SAFE)
			return false;

		/* If the trigger is parallel safe, also look for RI_TRIGGER. */
		trigtype = RI_FKey_trigger_type(trigger->tgfoid);
		if (trigtype == RI_TRIGGER_PK || trigtype == RI_TRIGGER_FK)
			return false;
	}

	return true;
}

/*
 * CheckExprParallelSafety - determine parallel safety of volatile expressions
 * in default clause of column definition or in where clause and return true if
 * they are parallel safe.
 */
static pg_attribute_always_inline bool
CheckExprParallelSafety(CopyState cstate)
{
	if (contain_volatile_functions(cstate->whereClause))
	{
		if (max_parallel_hazard((Query *)cstate->whereClause) != PROPARALLEL_SAFE)
			return false;
	}

	/*
	 * Check if any of the column has volatile default expression. if yes, and
	 * they are not parallel safe, then parallelism is not allowed. For
	 * instance, if there are any serial/bigserial columns for which nextval()
	 * default expression which is parallel unsafe is associated, parallelism
	 * should not be allowed. In non parallel copy volatile functions are not
	 * checked for nextval().
	 */
	if (cstate->defexprs != NULL && cstate->num_defaults != 0)
	{
		int     i;
		for (i = 0; i < cstate->num_defaults; i++)
		{
			bool volatile_expr = contain_volatile_functions((Node *)cstate->defexprs[i]->expr);
			if (volatile_expr &&
				(max_parallel_hazard((Query *)cstate->defexprs[i]->expr)) !=
				 PROPARALLEL_SAFE)
				return false;
		}
	}

	return true;
}

/*
 * FindInsertMethod - determine insert mode single, multi, or multi conditional.
 */
static pg_attribute_always_inline CopyInsertMethod
FindInsertMethod(CopyState cstate)
{
	if (cstate->rel->trigdesc != NULL &&
		(cstate->rel->trigdesc->trig_insert_before_row ||
		 cstate->rel->trigdesc->trig_insert_instead_row))
		return CIM_SINGLE;

	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
			cstate->rel->trigdesc != NULL &&
			cstate->rel->trigdesc->trig_insert_new_table)
		return CIM_SINGLE;

	if (cstate->rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		return CIM_SINGLE;

	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return CIM_MULTI_CONDITIONAL;

	return CIM_MULTI;
}

/*
 * IsParallelCopyAllowed - check for the cases where parallel copy is not
 * applicable.
 */
static pg_attribute_always_inline bool
IsParallelCopyAllowed(CopyState cstate)
{
	/* Parallel copy not allowed for frontend (2.0 protocol) & binary option. */
	if ((cstate->copy_dest == COPY_OLD_FE) || cstate->binary)
		return false;

	/* Check if copy is into foreign table or temporary table. */
	if (cstate->rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
		RelationUsesLocalBuffers(cstate->rel))
		return false;

	/* Check if trigger function is parallel safe. */
	if (cstate->rel->trigdesc != NULL &&
		!IsTriggerFunctionParallelSafe(cstate->rel->trigdesc))
		return false;

	/*
	 * Check if there is after statement or instead of trigger or transition
	 * table triggers.
	 */
	if (cstate->rel->trigdesc != NULL &&
		(cstate->rel->trigdesc->trig_insert_after_statement ||
		 cstate->rel->trigdesc->trig_insert_instead_row ||
		 cstate->rel->trigdesc->trig_insert_new_table))
		return false;

	/* Check if the volatile expressions are parallel safe, if present any. */
	if (!CheckExprParallelSafety(cstate))
		return false;

	/* Check if the insertion mode is single. */
	if (FindInsertMethod(cstate) == CIM_SINGLE)
		return false;

	return true;
}

/*
 * BeginParallelCopy - start parallel copy tasks.
 *
 * Get the number of workers required to perform the parallel copy. The data
 * structures that are required by the parallel workers will be initialized, the
 * size required in DSM will be calculated and the necessary keys will be loaded
 * in the DSM. The specified number of workers will then be launched.
 */
static ParallelContext*
BeginParallelCopy(int nworkers, CopyState cstate, List *attnamelist, Oid relid)
{
	ParallelContext *pcxt;
	ParallelCopyShmInfo *shared_info_ptr;
	SerializedParallelCopyState *shared_cstate;
	FullTransactionId full_transaction_id;
	Size est_cstateshared;
	char *whereClauseStr = NULL;
	char *rangeTableStr = NULL;
	char *attnameListStr = NULL;
	char *notnullListStr = NULL;
	char *nullListStr = NULL;
	char *convertListStr = NULL;
	int  parallel_workers = 0;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	ParallelCopyData *pcdata;
	MemoryContext oldcontext;

	CheckTargetRelValidity(cstate);
	parallel_workers = Min(nworkers, max_worker_processes);

	/* Can't perform copy in parallel */
	if (parallel_workers <= 0)
		return NULL;

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);
	pcdata = (ParallelCopyData *) palloc0(sizeof(ParallelCopyData));
	MemoryContextSwitchTo(oldcontext);
	cstate->pcdata = pcdata;

	/*
	 * User chosen parallel copy. Determine if the parallel copy is actually
	 * allowed. If not, go with the non-parallel mode.
	 */
	if (!IsParallelCopyAllowed(cstate))
		return NULL;

	full_transaction_id = GetCurrentFullTransactionId();

	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "ParallelCopyMain",
								 parallel_workers);
	Assert(pcxt->nworkers > 0);

	/*
	 * Estimate size for shared information for PARALLEL_COPY_KEY_SHARED_INFO
	 */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelCopyShmInfo));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate the size for shared information for PARALLEL_COPY_KEY_CSTATE */
	est_cstateshared = 	MAXALIGN(sizeof(SerializedParallelCopyState));
	shm_toc_estimate_chunk(&pcxt->estimator, est_cstateshared);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	EstimateLineKeysStr(pcxt, cstate->null_print);
	EstimateLineKeysStr(pcxt, cstate->null_print_client);
	EstimateLineKeysStr(pcxt, cstate->delim);
	EstimateLineKeysStr(pcxt, cstate->quote);
	EstimateLineKeysStr(pcxt, cstate->escape);

	if (cstate->whereClause != NULL)
	{
		whereClauseStr = nodeToString(cstate->whereClause);
		EstimateLineKeysStr(pcxt, whereClauseStr);
	}

	if (cstate->range_table != NULL)
	{
		rangeTableStr = nodeToString(cstate->range_table);
		EstimateLineKeysStr(pcxt, rangeTableStr);
	}

	/* Estimate the size for shared information for PARALLEL_COPY_KEY_XID. */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(FullTransactionId));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/*
	 * Estimate the size for shared information for
	 * PARALLEL_COPY_KEY_ATTNAME_LIST.
	 */
	if (attnamelist != NIL)
	{
		attnameListStr = nodeToString(attnamelist);
		EstimateLineKeysStr(pcxt, attnameListStr);
	}

	/*
	 * Estimate the size for shared information for
	 * PARALLEL_COPY_KEY_NOT_NULL_LIST.
	 */
	if (cstate->force_notnull != NIL)
	{
		notnullListStr = nodeToString(cstate->force_notnull);
		EstimateLineKeysStr(pcxt, notnullListStr);
	}

	/*
	 * Estimate the size for shared information for
	 * PARALLEL_COPY_KEY_NULL_LIST.
	 */
	if (cstate->force_null != NIL)
	{
		nullListStr = nodeToString(cstate->force_null);
		EstimateLineKeysStr(pcxt, nullListStr);
	}

	/*
	 * Estimate the size for shared information for
	 * PARALLEL_COPY_KEY_CONVERT_LIST.
	 */
	if (cstate->convert_select != NIL)
	{
		convertListStr = nodeToString(cstate->convert_select);
		EstimateLineKeysStr(pcxt, convertListStr);
	}

	/*
	 * Estimate space for WalUsage and BufferUsage -- PARALLEL_COPY_WAL_USAGE
	 * and PARALLEL_COPY_BUFFER_USAGE.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial copy) */
	if (pcxt->seg == NULL)
	{
		EndParallelCopy(pcxt);
		return NULL;
	}

	/* Allocate shared memory for PARALLEL_COPY_KEY_SHARED_INFO */
	shared_info_ptr = (ParallelCopyShmInfo *) shm_toc_allocate(pcxt->toc, sizeof(ParallelCopyShmInfo));
	PopulateParallelCopyShmInfo(shared_info_ptr, full_transaction_id);

	shm_toc_insert(pcxt->toc, PARALLEL_COPY_KEY_SHARED_INFO, shared_info_ptr);
	pcdata->pcshared_info = shared_info_ptr;
	pcdata->relid = relid;

	/* Store shared build state, for which we reserved space. */
	shared_cstate = (SerializedParallelCopyState *)shm_toc_allocate(pcxt->toc, est_cstateshared);

	/* copy cstate variables. */
	SerializeParallelCopyState(cstate, shared_cstate);
	shm_toc_insert(pcxt->toc, PARALLEL_COPY_KEY_CSTATE, shared_cstate);

	SerializeString(pcxt, PARALLEL_COPY_KEY_NULL_PRINT, cstate->null_print);
	SerializeString(pcxt, PARALLEL_COPY_KEY_DELIM, cstate->delim);
	SerializeString(pcxt, PARALLEL_COPY_KEY_QUOTE, cstate->quote);
	SerializeString(pcxt, PARALLEL_COPY_KEY_ESCAPE, cstate->escape);
	SerializeString(pcxt, PARALLEL_COPY_KEY_ATTNAME_LIST, attnameListStr);
	SerializeString(pcxt, PARALLEL_COPY_KEY_NOT_NULL_LIST, notnullListStr);
	SerializeString(pcxt, PARALLEL_COPY_KEY_NULL_LIST, nullListStr);
	SerializeString(pcxt, PARALLEL_COPY_KEY_CONVERT_LIST, convertListStr);
	SerializeString(pcxt, PARALLEL_COPY_KEY_WHERE_CLAUSE_STR, whereClauseStr);
	SerializeString(pcxt, PARALLEL_COPY_KEY_RANGE_TABLE, rangeTableStr);

	/*
	 * Allocate space for each worker's WalUsage and BufferUsage; no need to
	 * initialize.
	 */
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_COPY_WAL_USAGE, walusage);
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_COPY_BUFFER_USAGE, bufferusage);

	LaunchParallelWorkers(pcxt);
	if (pcxt->nworkers_launched == 0)
	{
		EndParallelCopy(pcxt);
		return NULL;
	}

	/*
	 * Caller needs to wait for all launched workers when we return.  Make sure
	 * that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);

	pcdata->is_leader = true;
	cstate->is_parallel = true;
	return pcxt;
}

/*
 * EndParallelCopy - end the parallel copy tasks.
 */
static pg_attribute_always_inline void
EndParallelCopy(ParallelContext *pcxt)
{
	Assert(!IsParallelWorker());

	DestroyParallelContext(pcxt);
	ExitParallelMode();
}

/*
 * InitializeParallelCopyInfo - Initialize parallel worker.
 */
static void
InitializeParallelCopyInfo(SerializedParallelCopyState *shared_cstate,
		CopyState cstate, List *attnamelist)
{
	uint32		count;
	ParallelCopyData *pcdata = cstate->pcdata;
	TupleDesc	tup_desc = RelationGetDescr(cstate->rel);

	cstate->copy_dest = shared_cstate->copy_dest;
	cstate->file_encoding = shared_cstate->file_encoding;
	cstate->need_transcoding = shared_cstate->need_transcoding;
	cstate->encoding_embeds_ascii = shared_cstate->encoding_embeds_ascii;
	cstate->csv_mode = shared_cstate->csv_mode;
	cstate->header_line = shared_cstate->header_line;
	cstate->null_print_len = shared_cstate->null_print_len;
	cstate->force_quote_all = shared_cstate->force_quote_all;
	cstate->convert_selectively = shared_cstate->convert_selectively;
	cstate->num_defaults = shared_cstate->num_defaults;
	pcdata->relid = shared_cstate->relid;

	PopulateCommonCstateInfo(cstate, tup_desc, attnamelist);

	/* Initialize state variables. */
	cstate->reached_eof = false;
	cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/* Set up variables to avoid per-attribute overhead. */
	initStringInfo(&cstate->attribute_buf);

	initStringInfo(&cstate->line_buf);
	for (count = 0; count < WORKER_CHUNK_COUNT; count++)
		initStringInfo(&pcdata->worker_line_buf[count].line_buf);

	cstate->line_buf_converted = false;
	cstate->raw_buf = NULL;
	cstate->raw_buf_index = cstate->raw_buf_len = 0;

	PopulateCstateCatalogInfo(cstate);

	/* Create workspace for CopyReadAttributes results. */
	if (!cstate->binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **)palloc(attr_count * sizeof(char *));
	}
}

/*
 * CacheLineInfo - Cache the line information to local memory.
 */
static bool
CacheLineInfo(CopyState cstate, uint32 buff_count)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	ParallelCopyData *pcdata = cstate->pcdata;
	uint32			write_pos;
	ParallelCopyDataBlock *data_blk_ptr;
	ParallelCopyLineBoundary *lineInfo;
	uint32 offset;
	int dataSize;
	int copiedSize = 0;

	resetStringInfo(&pcdata->worker_line_buf[buff_count].line_buf);
	write_pos = GetLinePosition(cstate);
	if (-1 == write_pos)
		return true;

	/* Get the current line information. */
	lineInfo = &pcshared_info->line_boundaries.ring[write_pos];
	if (pg_atomic_read_u32(&lineInfo->line_size) == 0)
		goto empty_data_line_update;

	/* Get the block information. */
	data_blk_ptr = &pcshared_info->data_blocks[lineInfo->first_block];

	/* Get the offset information from where the data must be copied. */
	offset = lineInfo->start_offset;
	pcdata->worker_line_buf[buff_count].cur_lineno = lineInfo->cur_lineno;

	elog(DEBUG1, "[Worker] Processing - line position:%d, block:%d, unprocessed lines:%d, offset:%d, line size:%d",
				 write_pos, lineInfo->first_block,
				 pg_atomic_read_u32(&data_blk_ptr->unprocessed_line_parts),
				 offset, pg_atomic_read_u32(&lineInfo->line_size));

	for (;;)
	{
		uint8 skip_bytes = data_blk_ptr->skip_bytes;
		/*
		 * There is a possibility that the above loop has come out because
		 * data_blk_ptr->curr_blk_completed is set, but dataSize read might
		 * be an old value, if data_blk_ptr->curr_blk_completed and the line is
		 * completed, line_size will be set. Read the line_size again to be
		 * sure if it is complete or partial block.
		 */
		dataSize = pg_atomic_read_u32(&lineInfo->line_size);
		if (dataSize)
		{
			int remainingSize = dataSize - copiedSize;
			if (!remainingSize)
				break;

			/* Whole line is in current block. */
			if (remainingSize + offset + skip_bytes < DATA_BLOCK_SIZE)
			{
				appendBinaryStringInfo(&pcdata->worker_line_buf[buff_count].line_buf,
									   &data_blk_ptr->data[offset],
									   remainingSize);
				pg_atomic_sub_fetch_u32(&data_blk_ptr->unprocessed_line_parts,
										1);
				break;
			}
			else
			{
				/* Line is spread across the blocks. */
				uint32 lineInCurrentBlock =  (DATA_BLOCK_SIZE - skip_bytes) - offset;
				appendBinaryStringInfoNT(&pcdata->worker_line_buf[buff_count].line_buf,
										 &data_blk_ptr->data[offset],
										 lineInCurrentBlock);
				pg_atomic_sub_fetch_u32(&data_blk_ptr->unprocessed_line_parts, 1);
				copiedSize += lineInCurrentBlock;
				while (copiedSize < dataSize)
				{
					uint32 currentBlockCopySize;
					ParallelCopyDataBlock *currBlkPtr = &pcshared_info->data_blocks[data_blk_ptr->following_block];
					skip_bytes = currBlkPtr->skip_bytes;

					/*
					 * If complete data is present in current block use
					 * dataSize - copiedSize, or copy the whole block from
					 * current block.
					 */
					currentBlockCopySize = Min(dataSize - copiedSize, DATA_BLOCK_SIZE - skip_bytes);
					appendBinaryStringInfoNT(&pcdata->worker_line_buf[buff_count].line_buf,
											 &currBlkPtr->data[0],
											 currentBlockCopySize);
					pg_atomic_sub_fetch_u32(&currBlkPtr->unprocessed_line_parts, 1);
					copiedSize += currentBlockCopySize;
					data_blk_ptr = currBlkPtr;
				}

				break;
			}
		}
		else
		{
			/* Copy this complete block from the current offset. */
			uint32 lineInCurrentBlock =  (DATA_BLOCK_SIZE - skip_bytes) - offset;
			appendBinaryStringInfoNT(&pcdata->worker_line_buf[buff_count].line_buf,
									 &data_blk_ptr->data[offset],
									 lineInCurrentBlock);
			pg_atomic_sub_fetch_u32(&data_blk_ptr->unprocessed_line_parts, 1);
			copiedSize += lineInCurrentBlock;

			/*
			 * Reset the offset. For the first copy, copy from the offset. For
			 * the subsequent copy the complete block.
			 */
			offset = 0;

			/* Set data_blk_ptr to the following block. */
			data_blk_ptr = &pcshared_info->data_blocks[data_blk_ptr->following_block];
		}

		for (;;)
		{
			/* Get the size of this line */
			dataSize = pg_atomic_read_u32(&lineInfo->line_size);

			/*
			 * If the data is present in current block lineInfo.line_size
			 * will be updated. If the data is spread across the blocks either
			 * of lineInfo.line_size or data_blk_ptr->curr_blk_completed can
			 * be updated. lineInfo.line_size will be updated if the complete
			 * read is finished. data_blk_ptr->curr_blk_completed will be
			 * updated if processing of current block is finished and data
			 * processing is not finished.
			 */
			if (data_blk_ptr->curr_blk_completed || (dataSize != -1))
				break;

			COPY_WAIT_TO_PROCESS()
		}
	}

empty_data_line_update:
	elog(DEBUG1, "[Worker] Completed processing line:%d", write_pos);
	pg_atomic_write_u32(&lineInfo->line_state, LINE_WORKER_PROCESSED);
	pg_atomic_write_u32(&lineInfo->line_size, -1);
	pg_atomic_add_fetch_u64(&pcshared_info->total_worker_processed, 1);
	return false;
}

/*
 * GetWorkerLine - Returns a line for worker to process.
 */
static bool
GetWorkerLine(CopyState cstate)
{
	uint32 buff_count;
	ParallelCopyData *pcdata = cstate->pcdata;

	/*
	 * Copy the line data to line_buf and release the line position so that the
	 * worker can continue loading data.
	 */
	if (pcdata->worker_line_buf_pos < pcdata->worker_line_buf_count)
		goto return_line;

	pcdata->worker_line_buf_pos = 0;
	pcdata->worker_line_buf_count = 0;

	for (buff_count = 0; buff_count < WORKER_CHUNK_COUNT; buff_count++)
	{
		bool result = CacheLineInfo(cstate, buff_count);
		if (result)
			break;

		pcdata->worker_line_buf_count++;
	}

	if (pcdata->worker_line_buf_count)
		goto return_line;
	else
		resetStringInfo(&cstate->line_buf);

	return true;

return_line:
	cstate->line_buf = pcdata->worker_line_buf[pcdata->worker_line_buf_pos].line_buf;
	cstate->cur_lineno = pcdata->worker_line_buf[pcdata->worker_line_buf_pos].cur_lineno;
	cstate->line_buf_valid = true;

	/* Mark that encoding conversion hasn't occurred yet. */
	cstate->line_buf_converted = false;
	ConvertToServerEncoding(cstate);
	pcdata->worker_line_buf_pos++;
	return false;
 }

/*
 * ParallelCopyMain - parallel copy worker's code.
 *
 * Where clause handling, convert tuple to columns, add default null values for
 * the missing columns that are not present in that record. Find the partition
 * if it is partitioned table, invoke before row insert Triggers, handle
 * constraints and insert the tuples.
 */
void
ParallelCopyMain(dsm_segment *seg, shm_toc *toc)
{
	CopyState cstate;
	ParallelCopyData *pcdata;
	ParallelCopyShmInfo *pcshared_info;
	SerializedParallelCopyState *shared_cstate;
	Relation	rel = NULL;
	MemoryContext oldcontext;
	List *attlist = NIL;
	char *whereClauseStr = NULL;
	char *rangeTableStr = NULL;
	char *attnameListStr = NULL;
	char *notnullListStr = NULL;
	char *nullListStr = NULL;
	char *convertListStr = NULL;
	WalUsage   *walusage;
	BufferUsage *bufferusage;

	/* Allocate workspace and zero all fields. */
	cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	pcdata = (ParallelCopyData *) palloc0(sizeof(ParallelCopyData));
	cstate->pcdata = pcdata;
	pcdata->is_leader = false;
	pcdata->worker_processed_pos = -1;
	cstate->is_parallel = true;
	pcshared_info = (ParallelCopyShmInfo *)shm_toc_lookup(toc, PARALLEL_COPY_KEY_SHARED_INFO, false);

	ereport(DEBUG1, (errmsg("Starting parallel copy worker")));

	pcdata->pcshared_info = pcshared_info;
	AssignFullTransactionIdForWorker(pcshared_info->full_transaction_id);
	AssignCommandIdForWorker(pcshared_info->mycid, true);

	shared_cstate = (SerializedParallelCopyState *)shm_toc_lookup(toc, PARALLEL_COPY_KEY_CSTATE, false);
	cstate->null_print = (char *)shm_toc_lookup(toc, PARALLEL_COPY_KEY_NULL_PRINT, true);

	RestoreString(toc, PARALLEL_COPY_KEY_DELIM, &cstate->delim);
	RestoreString(toc, PARALLEL_COPY_KEY_QUOTE, &cstate->quote);
	RestoreString(toc, PARALLEL_COPY_KEY_ESCAPE, &cstate->escape);
	RestoreString(toc, PARALLEL_COPY_KEY_ATTNAME_LIST, &attnameListStr);
	if (attnameListStr)
		attlist = (List *)stringToNode(attnameListStr);

	RestoreString(toc, PARALLEL_COPY_KEY_NOT_NULL_LIST, &notnullListStr);
	if (notnullListStr)
		cstate->force_notnull = (List *)stringToNode(notnullListStr);

	RestoreString(toc, PARALLEL_COPY_KEY_NULL_LIST, &nullListStr);
	if (nullListStr)
		cstate->force_null = (List *)stringToNode(nullListStr);

	RestoreString(toc, PARALLEL_COPY_KEY_CONVERT_LIST, &convertListStr);
	if (convertListStr)
		cstate->convert_select = (List *)stringToNode(convertListStr);

	RestoreString(toc, PARALLEL_COPY_KEY_WHERE_CLAUSE_STR, &whereClauseStr);
	RestoreString(toc, PARALLEL_COPY_KEY_RANGE_TABLE, &rangeTableStr);

	if (whereClauseStr)
	{
		Node *whereClauseCnvrtdFrmStr = (Node *) stringToNode(whereClauseStr);
		cstate->whereClause = whereClauseCnvrtdFrmStr;
	}

	if (rangeTableStr)
	{
		List *rangeTableCnvrtdFrmStr = (List *) stringToNode(rangeTableStr);
		cstate->range_table = rangeTableCnvrtdFrmStr;
	}

	/* Open and lock the relation, using the appropriate lock type. */
	rel = table_open(shared_cstate->relid, RowExclusiveLock);
	cstate->rel = rel;
	InitializeParallelCopyInfo(shared_cstate, cstate, attlist);

	CopyFrom(cstate);

	if (rel != NULL)
		table_close(rel, RowExclusiveLock);

	/* Report WAL/buffer usage during parallel execution */
	bufferusage = shm_toc_lookup(toc, PARALLEL_COPY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_COPY_WAL_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

	MemoryContextSwitchTo(oldcontext);
	pfree(cstate);
	return;
}

/*
 * UpdateBlockInLineInfo - Update the line information.
 */
static pg_attribute_always_inline int
UpdateBlockInLineInfo(CopyState cstate, uint32 blk_pos,
					   uint32 offset, uint32 line_size, uint32 line_state)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	ParallelCopyLineBoundaries *lineBoundaryPtr = &pcshared_info->line_boundaries;
	ParallelCopyLineBoundary *lineInfo;
	int line_pos = lineBoundaryPtr->pos;

	/* Update the line information for the worker to pick and process. */
	lineInfo = &lineBoundaryPtr->ring[line_pos];
	while (pg_atomic_read_u32(&lineInfo->line_size) != -1)
		COPY_WAIT_TO_PROCESS()

	lineInfo->first_block = blk_pos;
	lineInfo->start_offset = offset;
	lineInfo->cur_lineno = cstate->cur_lineno;
	pg_atomic_write_u32(&lineInfo->line_size, line_size);
	pg_atomic_write_u32(&lineInfo->line_state, line_state);
	lineBoundaryPtr->pos = (lineBoundaryPtr->pos + 1) % RINGSIZE;

	return line_pos;
}

/*
 * ParallelCopyFrom - parallel copy leader's functionality.
 *
 * Leader executes the before statement for before statement trigger, if before
 * statement trigger is present. It will read the table data from the file and
 * copy the contents to DSM data blocks. It will then read the input contents
 * from the DSM data block and identify the records based on line breaks. This
 * information is called line or a record that need to be inserted into a
 * relation. The line information will be stored in ParallelCopyLineBoundary DSM
 * data structure. Workers will then process this information and insert the
 * data in to table. It will repeat this process until the all data is read from
 * the file and all the DSM data blocks are processed. While processing if
 * leader identifies that DSM Data blocks or DSM ParallelCopyLineBoundary data
 * structures is full, leader will wait till the worker frees up some entries
 * and repeat the process. It will wait till all the lines populated are
 * processed by the workers and exits.
 */
static void
ParallelCopyFrom(CopyState cstate)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	ereport(DEBUG1, (errmsg("Running parallel copy leader")));

	/* Execute the before statement triggers from the leader */
	ExecBeforeStmtTrigger(cstate);

	/* On input just throw the header line away. */
	if (cstate->cur_lineno == 0 && cstate->header_line)
	{
		cstate->cur_lineno++;
		if (CopyReadLine(cstate))
		{
			pcshared_info->is_read_in_progress = false;
			return;			/* done */
		}
	}

	for (;;)
	{
		bool done;
		cstate->cur_lineno++;

		/* Actually read the line into memory here. */
		done = CopyReadLine(cstate);

		/*
		 * EOF at start of line means we're done.  If we see EOF after some
		 * characters, we act as though it was newline followed by EOF, ie,
		 * process the line and then exit loop on next iteration.
		 */
		if (done && cstate->line_buf.len == 0)
			break;
	}

	pcshared_info->is_read_in_progress = false;
	cstate->cur_lineno = 0;
 }

/*
 * GetLinePosition - return the line position that worker should process.
 */
static uint32
GetLinePosition(CopyState cstate)
{
	ParallelCopyData *pcdata = cstate->pcdata;
	ParallelCopyShmInfo *pcshared_info = pcdata->pcshared_info;
	uint32  previous_pos = pcdata->worker_processed_pos;
	uint32 write_pos = (previous_pos == -1) ? 0 : (previous_pos + 1) % RINGSIZE;
	for (;;)
	{
		int		dataSize;
		bool	is_read_in_progress = pcshared_info->is_read_in_progress;
		ParallelCopyLineBoundary *lineInfo;
		ParallelCopyDataBlock *data_blk_ptr;
		ParallelCopyLineState line_state = LINE_LEADER_POPULATED;
		ParallelCopyLineState curr_line_state;
		CHECK_FOR_INTERRUPTS();

		/* File read completed & no elements to process. */
		if (!is_read_in_progress &&
			(pcshared_info->populated ==
			 pg_atomic_read_u64(&pcshared_info->total_worker_processed)))
		{
			write_pos = -1;
			break;
		}

		/* Get the current line information. */
		lineInfo = &pcshared_info->line_boundaries.ring[write_pos];
		curr_line_state = pg_atomic_read_u32(&lineInfo->line_state);
		if ((write_pos % WORKER_CHUNK_COUNT == 0) &&
			(curr_line_state == LINE_WORKER_PROCESSED ||
			 curr_line_state == LINE_WORKER_PROCESSING))
		{
			pcdata->worker_processed_pos = write_pos;
			write_pos = (write_pos + WORKER_CHUNK_COUNT) %  RINGSIZE;
			continue;
		}

		/* Get the size of this line. */
		dataSize = pg_atomic_read_u32(&lineInfo->line_size);

		if (dataSize != 0) /* If not an empty line. */
		{
			/* Get the block information. */
			data_blk_ptr = &pcshared_info->data_blocks[lineInfo->first_block];

			if (!data_blk_ptr->curr_blk_completed && (dataSize == -1))
			{
				/* Wait till the current line or block is added. */
				COPY_WAIT_TO_PROCESS()
				continue;
			}
		}

		/* Make sure that no worker has consumed this element. */
		if (pg_atomic_compare_exchange_u32(&lineInfo->line_state,
										   &line_state, LINE_WORKER_PROCESSING))
			break;
	}

	pcdata->worker_processed_pos = write_pos;
	return write_pos;
}

/*
 * GetFreeCopyBlock - Get a free block for data to be copied.
 */
static pg_attribute_always_inline uint32
GetFreeCopyBlock(ParallelCopyShmInfo *pcshared_info)
{
	int count = 0;
	uint32 last_free_block = pcshared_info->cur_block_pos;
	uint32 block_pos = (last_free_block != -1) ? ((last_free_block + 1) % MAX_BLOCKS_COUNT): 0;

	/*
	 * Get a new block for copying data, don't check current block, current
	 * block will have some unprocessed data.
	 */
	while (count < (MAX_BLOCKS_COUNT - 1))
	{
		ParallelCopyDataBlock *dataBlkPtr = &pcshared_info->data_blocks[block_pos];
		uint32 unprocessed_line_parts = pg_atomic_read_u32(&dataBlkPtr->unprocessed_line_parts);
		if (unprocessed_line_parts == 0)
		{
			dataBlkPtr->curr_blk_completed = false;
			dataBlkPtr->skip_bytes = 0;
			pcshared_info->cur_block_pos = block_pos;
			return block_pos;
		}

		block_pos = (block_pos + 1) % MAX_BLOCKS_COUNT;
		count++;
	}

	return -1;
}

/*
 * WaitGetFreeCopyBlock - If there are no blocks available, wait and get a block
 * for copying data.
 */
static uint32
WaitGetFreeCopyBlock(ParallelCopyShmInfo *pcshared_info)
{
	uint32 new_free_pos = -1;
	for (;;)
	{
		new_free_pos = GetFreeCopyBlock(pcshared_info);
		if (new_free_pos != -1)	/* We have got one block, break now. */
			break;

		COPY_WAIT_TO_PROCESS()
	}

	return new_free_pos;
}

/*
 * SetRawBufForLoad - Set raw_buf to the shared memory where the file data must
 * be read.
 */
static void
SetRawBufForLoad(CopyState cstate, uint32 line_size, uint32 copy_buf_len,
				 uint32 raw_buf_ptr, char **copy_raw_buf)
{
	ParallelCopyShmInfo	*pcshared_info;
	uint32 cur_block_pos;
	uint32 next_block_pos;
	ParallelCopyDataBlock	*cur_data_blk_ptr = NULL;
	ParallelCopyDataBlock	*next_data_blk_ptr = NULL;

	if (!IsParallelCopy())
		return;

	pcshared_info = cstate->pcdata->pcshared_info;
	cur_block_pos = pcshared_info->cur_block_pos;
	cur_data_blk_ptr = (cstate->raw_buf) ? &pcshared_info->data_blocks[cur_block_pos] : NULL;
	next_block_pos = WaitGetFreeCopyBlock(pcshared_info);
	next_data_blk_ptr = &pcshared_info->data_blocks[next_block_pos];

	/* set raw_buf to the data block in shared memory */
	cstate->raw_buf = next_data_blk_ptr->data;
	*copy_raw_buf = cstate->raw_buf;
	if (cur_data_blk_ptr && line_size)
	{
		/*
		 * Mark the previous block as completed, worker can start copying this
		 * data.
		 */
		cur_data_blk_ptr->following_block = next_block_pos;
		pg_atomic_add_fetch_u32(&cur_data_blk_ptr->unprocessed_line_parts, 1);
		cur_data_blk_ptr->skip_bytes = copy_buf_len - raw_buf_ptr;
		cur_data_blk_ptr->curr_blk_completed = true;
	}
}

/*
 * EndLineParallelCopy - Update the line information in shared memory.
 */
static void
EndLineParallelCopy(CopyState cstate, uint32 line_pos, uint32 line_size,
					 uint32 raw_buf_ptr)
{
	uint8 new_line_size;
	if (!IsParallelCopy())
		return;

	if (!IsHeaderLine())
	{
		ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
		ParallelCopyLineBoundaries *lineBoundaryPtr = &pcshared_info->line_boundaries;
		SET_NEWLINE_SIZE()
		if (line_size)
		{
			ParallelCopyLineBoundary *lineInfo = &lineBoundaryPtr->ring[line_pos];
			/*
			 * If the new_line_size > raw_buf_ptr, then the new block has only
			 * new line char content. The unprocessed count should not be
			 * increased in this case.
			 */
			if (raw_buf_ptr > new_line_size)
			{
				uint32 cur_block_pos = pcshared_info->cur_block_pos;
				ParallelCopyDataBlock *curr_data_blk_ptr = &pcshared_info->data_blocks[cur_block_pos];
				pg_atomic_add_fetch_u32(&curr_data_blk_ptr->unprocessed_line_parts, 1);
			}

			/* Update line size. */
			pg_atomic_write_u32(&lineInfo->line_size, line_size);
			pg_atomic_write_u32(&lineInfo->line_state, LINE_LEADER_POPULATED);
			elog(DEBUG1, "[Leader] After adding - line position:%d, line_size:%d",
						 line_pos, line_size);
			pcshared_info->populated++;
		}
		else if (new_line_size)
		{
			/* This means only new line char, empty record should be inserted.*/
			ParallelCopyLineBoundary *lineInfo;
			line_pos = UpdateBlockInLineInfo(cstate, -1, -1, 0,
											   LINE_LEADER_POPULATED);
			lineInfo = &lineBoundaryPtr->ring[line_pos];
			elog(DEBUG1, "[Leader] Added empty line with offset:%d, line position:%d, line size:%d",
						 lineInfo->start_offset, line_pos,
						 pg_atomic_read_u32(&lineInfo->line_size));
			pcshared_info->populated++;
		}
	}
}

/*
 * ExecBeforeStmtTrigger - Execute the before statement trigger, this will be
 * executed for parallel copy by the leader process.
 */
static void
ExecBeforeStmtTrigger(CopyState cstate)
{
	EState	   *estate = CreateExecutorState();
	ResultRelInfo *resultRelInfo;

	Assert(IsLeader());

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo,
					  cstate->rel,
					  1,		/* must match rel's position in range_table */
					  NULL,
					  0);

	/* Verify the named relation is a valid target for INSERT */
	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	ExecInitRangeTable(estate, cstate->range_table);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	/* Close any trigger target relations */
	ExecCleanUpTriggerState(estate);

	FreeExecutorState(estate);
}

/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void
SendCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = list_length(cstate->attnumlist);
		int16		format = (cstate->binary ? 1 : 0);
		int			i;

		pq_beginmessage(&buf, 'H');
		pq_sendbyte(&buf, format);	/* overall format */
		pq_sendint16(&buf, natts);
		for (i = 0; i < natts; i++)
			pq_sendint16(&buf, format); /* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
	}
	else
	{
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('H');
		/* grottiness needed for old COPY OUT protocol */
		pq_startcopyout();
		cstate->copy_dest = COPY_OLD_FE;
	}
}

static void
ReceiveCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = list_length(cstate->attnumlist);
		int16		format = (cstate->binary ? 1 : 0);
		int			i;

		pq_beginmessage(&buf, 'G');
		pq_sendbyte(&buf, format);	/* overall format */
		pq_sendint16(&buf, natts);
		for (i = 0; i < natts; i++)
			pq_sendint16(&buf, format); /* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
		cstate->fe_msgbuf = makeStringInfo();
	}
	else
	{
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('G');
		/* any error in old protocol will make us lose sync */
		pq_startmsgread();
		cstate->copy_dest = COPY_OLD_FE;
	}
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();
}

static void
SendCopyEnd(CopyState cstate)
{
	if (cstate->copy_dest == COPY_NEW_FE)
	{
		/* Shouldn't have any unsent data */
		Assert(cstate->fe_msgbuf->len == 0);
		/* Send Copy Done message */
		pq_putemptymessage('c');
	}
	else
	{
		CopySendData(cstate, "\\.", 2);
		/* Need to flush out the trailer (this also appends a newline) */
		CopySendEndOfRow(cstate);
		pq_endcopyout(false);
	}
}

/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(CopyState cstate, const void *databuf, int datasize)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, databuf, datasize);
}

static void
CopySendString(CopyState cstate, const char *str)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

static void
CopySendChar(CopyState cstate, char c)
{
	appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

static void
CopySendEndOfRow(CopyState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			if (!cstate->binary)
			{
				/* Default line termination depends on platform */
#ifndef WIN32
				CopySendChar(cstate, '\n');
#else
				CopySendString(cstate, "\r\n");
#endif
			}

			if (fwrite(fe_msgbuf->data, fe_msgbuf->len, 1,
					   cstate->copy_file) != 1 ||
				ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					if (errno == EPIPE)
					{
						/*
						 * The pipe will be closed automatically on error at
						 * the end of transaction, but we might get a better
						 * error message from the subprocess' exit code than
						 * just "Broken Pipe"
						 */
						ClosePipeToProgram(cstate);

						/*
						 * If ClosePipeToProgram() didn't throw an error, the
						 * program terminated normally, but closed the pipe
						 * first. Restore errno, and throw an error.
						 */
						errno = EPIPE;
					}
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY program: %m")));
				}
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY file: %m")));
			}
			break;
		case COPY_OLD_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->binary)
				CopySendChar(cstate, '\n');

			if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
			{
				/* no hope of recovering connection sync, so FATAL */
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection lost during COPY to stdout")));
			}
			break;
		case COPY_NEW_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->binary)
				CopySendChar(cstate, '\n');

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_CALLBACK:
			Assert(false);		/* Not yet supported. */
			break;
	}

	resetStringInfo(fe_msgbuf);
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.  The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 */
static int
CopyGetData(CopyState cstate, void *databuf, int minread, int maxread)
{
	int			bytesread = 0;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			bytesread = fread(databuf, 1, maxread, cstate->copy_file);
			if (ferror(cstate->copy_file))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from COPY file: %m")));
			if (bytesread == 0)
				cstate->reached_eof = true;
			break;
		case COPY_OLD_FE:

			/*
			 * We cannot read more than minread bytes (which in practice is 1)
			 * because old protocol doesn't have any clear way of separating
			 * the COPY stream from following data.  This is slow, but not any
			 * slower than the code path was originally, and we don't care
			 * much anymore about the performance of old protocol.
			 */
			if (pq_getbytes((char *) databuf, minread))
			{
				/* Only a \. terminator is legal EOF in old protocol */
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection with an open transaction")));
			}
			bytesread = minread;
			break;
		case COPY_NEW_FE:
			while (maxread > 0 && bytesread < minread && !cstate->reached_eof)
			{
				int			avail;

				while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len)
				{
					/* Try to receive another message */
					int			mtype;

			readmessage:
					HOLD_CANCEL_INTERRUPTS();
					pq_startmsgread();
					mtype = pq_getbyte();
					if (mtype == EOF)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					if (pq_getmessage(cstate->fe_msgbuf, 0))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					RESUME_CANCEL_INTERRUPTS();
					switch (mtype)
					{
						case 'd':	/* CopyData */
							break;
						case 'c':	/* CopyDone */
							/* COPY IN correctly terminated by frontend */
							cstate->reached_eof = true;
							return bytesread;
						case 'f':	/* CopyFail */
							ereport(ERROR,
									(errcode(ERRCODE_QUERY_CANCELED),
									 errmsg("COPY from stdin failed: %s",
											pq_getmsgstring(cstate->fe_msgbuf))));
							break;
						case 'H':	/* Flush */
						case 'S':	/* Sync */

							/*
							 * Ignore Flush/Sync for the convenience of client
							 * libraries (such as libpq) that may send those
							 * without noticing that the command they just
							 * sent was COPY.
							 */
							goto readmessage;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("unexpected message type 0x%02X during COPY from stdin",
											mtype)));
							break;
					}
				}
				avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
				if (avail > maxread)
					avail = maxread;
				pq_copymsgbytes(cstate->fe_msgbuf, databuf, avail);
				databuf = (void *) ((char *) databuf + avail);
				maxread -= avail;
				bytesread += avail;
			}
			break;
		case COPY_CALLBACK:
			bytesread = cstate->data_source_cb(databuf, minread, maxread);
			break;
	}

	return bytesread;
}


/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static inline void
CopySendInt32(CopyState cstate, int32 val)
{
	uint32		buf;

	buf = pg_hton32((uint32) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
static inline bool
CopyGetInt32(CopyState cstate, int32 *val)
{
	uint32		buf;

	if (CopyReadBinaryData(cstate, (char *) &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int32) pg_ntoh32(buf);
	return true;
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static inline void
CopySendInt16(CopyState cstate, int16 val)
{
	uint16		buf;

	buf = pg_hton16((uint16) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
static inline bool
CopyGetInt16(CopyState cstate, int16 *val)
{
	uint16		buf;

	if (CopyReadBinaryData(cstate, (char *) &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int16) pg_ntoh16(buf);
	return true;
}


/*
 * CopyLoadRawBuf loads some more data into raw_buf
 *
 * Returns true if able to obtain at least one more byte, else false.
 *
 * If RAW_BUF_BYTES(cstate) > 0, the unprocessed bytes are moved to the start
 * of the buffer and then we load more data after that.  This case occurs only
 * when a multibyte character crosses a bufferload boundary.
 */
static bool
CopyLoadRawBuf(CopyState cstate)
{
	int			nbytes = RAW_BUF_BYTES(cstate);
	int			inbytes;
	int         minread = 1;

	/* Copy down the unprocessed data if any. */
	if (nbytes > 0)
		memmove(cstate->raw_buf, cstate->raw_buf + cstate->raw_buf_index,
				nbytes);

	if (cstate->copy_dest == COPY_NEW_FE)
		minread = RAW_BUF_SIZE - nbytes;

	inbytes = CopyGetData(cstate, cstate->raw_buf + nbytes,
						  minread, RAW_BUF_SIZE - nbytes);
	nbytes += inbytes;
	cstate->raw_buf[nbytes] = '\0';
	cstate->raw_buf_index = 0;
	cstate->raw_buf_len = nbytes;
	return (inbytes > 0);
}

/*
 * CopyReadBinaryData
 *
 * Reads up to 'nbytes' bytes from cstate->copy_file via cstate->raw_buf
 * and writes them to 'dest'.  Returns the number of bytes read (which
 * would be less than 'nbytes' only if we reach EOF).
 */
static int
CopyReadBinaryData(CopyState cstate, char *dest, int nbytes)
{
	int			copied_bytes = 0;

	if (RAW_BUF_BYTES(cstate) >= nbytes)
	{
		/* Enough bytes are present in the buffer. */
		memcpy(dest, cstate->raw_buf + cstate->raw_buf_index, nbytes);
		cstate->raw_buf_index += nbytes;
		copied_bytes = nbytes;
	}
	else
	{
		/*
		 * Not enough bytes in the buffer, so must read from the file.  Need
		 * to loop since 'nbytes' could be larger than the buffer size.
		 */
		do
		{
			int			copy_bytes;

			/* Load more data if buffer is empty. */
			if (RAW_BUF_BYTES(cstate) == 0)
			{
				if (!CopyLoadRawBuf(cstate))
					break;		/* EOF */
			}

			/* Transfer some bytes. */
			copy_bytes = Min(nbytes - copied_bytes, RAW_BUF_BYTES(cstate));
			memcpy(dest, cstate->raw_buf + cstate->raw_buf_index, copy_bytes);
			cstate->raw_buf_index += copy_bytes;
			dest += copy_bytes;
			copied_bytes += copy_bytes;
		} while (copied_bytes < nbytes);
	}

	return copied_bytes;
}


/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = true means we are inserting into the table.)  In the "TO" case
 * we also support copying the output of an arbitrary SELECT, INSERT, UPDATE
 * or DELETE query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.  Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Do not allow a Postgres user without the 'pg_read_server_files' or
 * 'pg_write_server_files' role to read from or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table or the specifically requested columns.
 */
void
DoCopy(ParseState *pstate, const CopyStmt *stmt,
	   int stmt_location, int stmt_len,
	   uint64 *processed)
{
	CopyState	cstate;
	bool		is_from = stmt->is_from;
	bool		pipe = (stmt->filename == NULL);
	Relation	rel;
	Oid			relid;
	RawStmt    *query = NULL;
	Node	   *whereClause = NULL;

	/*
	 * Disallow COPY to/from file or program except to users with the
	 * appropriate role.
	 */
	if (!pipe)
	{
		if (stmt->is_program)
		{
			if (!is_member_of_role(GetUserId(), DEFAULT_ROLE_EXECUTE_SERVER_PROGRAM))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser or a member of the pg_execute_server_program role to COPY to or from an external program"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			if (is_from && !is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_SERVER_FILES))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser or a member of the pg_read_server_files role to COPY from a file"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));

			if (!is_from && !is_member_of_role(GetUserId(), DEFAULT_ROLE_WRITE_SERVER_FILES))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser or a member of the pg_write_server_files role to COPY to a file"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
		}
	}

	if (stmt->relation)
	{
		LOCKMODE	lockmode = is_from ? RowExclusiveLock : AccessShareLock;
		ParseNamespaceItem *nsitem;
		RangeTblEntry *rte;
		TupleDesc	tupDesc;
		List	   *attnums;
		ListCell   *cur;

		Assert(!stmt->query);

		/* Open and lock the relation, using the appropriate lock type. */
		rel = table_openrv(stmt->relation, lockmode);

		relid = RelationGetRelid(rel);

		nsitem = addRangeTableEntryForRelation(pstate, rel, lockmode,
											   NULL, false, false);
		rte = nsitem->p_rte;
		rte->requiredPerms = (is_from ? ACL_INSERT : ACL_SELECT);

		if (stmt->whereClause)
		{
			/* add nsitem to query namespace */
			addNSItemToQuery(pstate, nsitem, false, true, true);

			/* Transform the raw expression tree */
			whereClause = transformExpr(pstate, stmt->whereClause, EXPR_KIND_COPY_WHERE);

			/* Make sure it yields a boolean result. */
			whereClause = coerce_to_boolean(pstate, whereClause, "WHERE");

			/* we have to fix its collations too */
			assign_expr_collations(pstate, whereClause);

			whereClause = eval_const_expressions(NULL, whereClause);

			whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
			whereClause = (Node *) make_ands_implicit((Expr *) whereClause);
		}

		tupDesc = RelationGetDescr(rel);
		attnums = CopyGetAttnums(tupDesc, rel, stmt->attlist);
		foreach(cur, attnums)
		{
			int			attno = lfirst_int(cur) -
			FirstLowInvalidHeapAttributeNumber;

			if (is_from)
				rte->insertedCols = bms_add_member(rte->insertedCols, attno);
			else
				rte->selectedCols = bms_add_member(rte->selectedCols, attno);
		}
		ExecCheckRTPerms(pstate->p_rtable, true);

		/*
		 * Permission check for row security policies.
		 *
		 * check_enable_rls will ereport(ERROR) if the user has requested
		 * something invalid and will otherwise indicate if we should enable
		 * RLS (returns RLS_ENABLED) or not for this COPY statement.
		 *
		 * If the relation has a row security policy and we are to apply it
		 * then perform a "query" copy and allow the normal query processing
		 * to handle the policies.
		 *
		 * If RLS is not enabled for this, then just fall through to the
		 * normal non-filtering relation handling.
		 */
		if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED)
		{
			SelectStmt *select;
			ColumnRef  *cr;
			ResTarget  *target;
			RangeVar   *from;
			List	   *targetList = NIL;

			if (is_from)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));

			/*
			 * Build target list
			 *
			 * If no columns are specified in the attribute list of the COPY
			 * command, then the target list is 'all' columns. Therefore, '*'
			 * should be used as the target list for the resulting SELECT
			 * statement.
			 *
			 * In the case that columns are specified in the attribute list,
			 * create a ColumnRef and ResTarget for each column and add them
			 * to the target list for the resulting SELECT statement.
			 */
			if (!stmt->attlist)
			{
				cr = makeNode(ColumnRef);
				cr->fields = list_make1(makeNode(A_Star));
				cr->location = -1;

				target = makeNode(ResTarget);
				target->name = NULL;
				target->indirection = NIL;
				target->val = (Node *) cr;
				target->location = -1;

				targetList = list_make1(target);
			}
			else
			{
				ListCell   *lc;

				foreach(lc, stmt->attlist)
				{
					/*
					 * Build the ColumnRef for each column.  The ColumnRef
					 * 'fields' property is a String 'Value' node (see
					 * nodes/value.h) that corresponds to the column name
					 * respectively.
					 */
					cr = makeNode(ColumnRef);
					cr->fields = list_make1(lfirst(lc));
					cr->location = -1;

					/* Build the ResTarget and add the ColumnRef to it. */
					target = makeNode(ResTarget);
					target->name = NULL;
					target->indirection = NIL;
					target->val = (Node *) cr;
					target->location = -1;

					/* Add each column to the SELECT statement's target list */
					targetList = lappend(targetList, target);
				}
			}

			/*
			 * Build RangeVar for from clause, fully qualified based on the
			 * relation which we have opened and locked.
			 */
			from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								pstrdup(RelationGetRelationName(rel)),
								-1);

			/* Build query */
			select = makeNode(SelectStmt);
			select->targetList = targetList;
			select->fromClause = list_make1(from);

			query = makeNode(RawStmt);
			query->stmt = (Node *) select;
			query->stmt_location = stmt_location;
			query->stmt_len = stmt_len;

			/*
			 * Close the relation for now, but keep the lock on it to prevent
			 * changes between now and when we start the query-based COPY.
			 *
			 * We'll reopen it later as part of the query-based COPY.
			 */
			table_close(rel, NoLock);
			rel = NULL;
		}
	}
	else
	{
		Assert(stmt->query);

		query = makeNode(RawStmt);
		query->stmt = stmt->query;
		query->stmt_location = stmt_location;
		query->stmt_len = stmt_len;

		relid = InvalidOid;
		rel = NULL;
	}

	if (is_from)
	{
		ParallelContext *pcxt = NULL;
		Assert(rel);

		/* check read-only transaction and parallel mode */
		if (XactReadOnly && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("COPY FROM");

		cstate = BeginCopyFrom(pstate, rel, stmt->filename, stmt->is_program,
							   NULL, stmt->attlist, stmt->options);
		cstate->whereClause = whereClause;
		cstate->is_parallel = false;

		if (cstate->nworkers > 0)
			pcxt = BeginParallelCopy(cstate->nworkers, cstate, stmt->attlist,
									 relid);

		if (pcxt)
		{
			ParallelCopyFrom(cstate);

			/* Wait for all copy workers to finish */
			WaitForParallelWorkersToFinish(pcxt);
			*processed = pg_atomic_read_u64(&cstate->pcdata->pcshared_info->processed);
			EndParallelCopy(pcxt);
		}
		else
			*processed = CopyFrom(cstate); /* copy from file to database */

		EndCopyFrom(cstate);
	}
	else
	{
		cstate = BeginCopyTo(pstate, rel, query, relid,
							 stmt->filename, stmt->is_program,
							 stmt->attlist, stmt->options);
		*processed = DoCopyTo(cstate);	/* copy from database to file */
		EndCopyTo(cstate);
	}

	if (rel != NULL)
		table_close(rel, NoLock);
}

/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into cstate, applying appropriate error checking.
 *
 * cstate is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, cstate should be passed as NULL
 * (since external users don't know sizeof(CopyStateData)) and the collected
 * data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void
ProcessCopyOptions(ParseState *pstate,
				   CopyState cstate,
				   bool is_from,
				   List *options)
{
	bool		format_specified = false;
	ListCell   *option;

	/* Support external use for option sanity checking */
	if (cstate == NULL)
		cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	cstate->is_copy_from = is_from;

	cstate->file_encoding = -1;
	cstate->nworkers = -1;

	/* Extract options from the statement node tree */
	foreach(option, options)
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "format") == 0)
		{
			char	   *fmt = defGetString(defel);

			if (format_specified)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			format_specified = true;
			if (strcmp(fmt, "text") == 0)
				 /* default format */ ;
			else if (strcmp(fmt, "csv") == 0)
				cstate->csv_mode = true;
			else if (strcmp(fmt, "binary") == 0)
				cstate->binary = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("COPY format \"%s\" not recognized", fmt),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "freeze") == 0)
		{
			if (cstate->freeze)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->freeze = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			if (cstate->delim)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->delim = defGetString(defel);
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			if (cstate->null_print)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->null_print = defGetString(defel);
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			if (cstate->header_line)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->header_line = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			if (cstate->quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->quote = defGetString(defel);
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (cstate->escape)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->escape = defGetString(defel);
		}
		else if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (cstate->force_quote || cstate->force_quote_all)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, A_Star))
				cstate->force_quote_all = true;
			else if (defel->arg && IsA(defel->arg, List))
				cstate->force_quote = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_not_null") == 0)
		{
			if (cstate->force_notnull)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_notnull = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_null") == 0)
		{
			if (cstate->force_null)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_null = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "convert_selectively") == 0)
		{
			/*
			 * Undocumented, not-accessible-from-SQL option: convert only the
			 * named columns to binary form, storing the rest as NULLs. It's
			 * allowed for the column list to be NIL.
			 */
			if (cstate->convert_selectively)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->convert_selectively = true;
			if (defel->arg == NULL || IsA(defel->arg, List))
				cstate->convert_select = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "encoding") == 0)
		{
			if (cstate->file_encoding >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->file_encoding = pg_char_to_encoding(defGetString(defel));
			if (cstate->file_encoding < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a valid encoding name",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "parallel") == 0)
		{
			char *endptr, *str;
			long val;

			if (!cstate->is_copy_from)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("parallel option supported only for copy from"),
						 parser_errposition(pstate, defel->location)));
			if (cstate->nworkers >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));

			str = defGetString(defel);

			errno = 0; /* To distinguish success/failure after call */
			val = strtol(str, &endptr, 10);

			/* Check for various possible errors */
			if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
				|| (errno != 0 && val == 0) ||
				*endptr)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("improper use of argument to option \"%s\"",
								defel->defname),
						 parser_errposition(pstate, defel->location)));

			if (endptr == str)
			   ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("no digits were found in argument to option \"%s\"",
								defel->defname),
						 parser_errposition(pstate, defel->location)));

			cstate->nworkers = (int) val;

			if (cstate->nworkers <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a positive integer greater than zero",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
	}

	/*
	 * Check for incompatible options (must do these two before inserting
	 * defaults)
	 */
	if (cstate->binary && cstate->delim)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot specify DELIMITER in BINARY mode")));

	if (cstate->binary && cstate->null_print)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot specify NULL in BINARY mode")));

	/* Set defaults for omitted options */
	if (!cstate->delim)
		cstate->delim = cstate->csv_mode ? "," : "\t";

	if (!cstate->null_print)
		cstate->null_print = cstate->csv_mode ? "" : "\\N";
	cstate->null_print_len = strlen(cstate->null_print);

	if (cstate->csv_mode)
	{
		if (!cstate->quote)
			cstate->quote = "\"";
		if (!cstate->escape)
			cstate->escape = cstate->quote;
	}

	/* Only single-byte delimiter strings are supported. */
	if (strlen(cstate->delim) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must be a single one-byte character")));

	/* Disallow end-of-line characters */
	if (strchr(cstate->delim, '\r') != NULL ||
		strchr(cstate->delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be newline or carriage return")));

	if (strchr(cstate->null_print, '\r') != NULL ||
		strchr(cstate->null_print, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY null representation cannot use newline or carriage return")));

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
	 * more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (!cstate->csv_mode &&
		strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789",
			   cstate->delim[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be \"%s\"", cstate->delim)));

	/* Check header */
	if (!cstate->csv_mode && cstate->header_line)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY HEADER available only in CSV mode")));

	/* Check quote */
	if (!cstate->csv_mode && cstate->quote != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote available only in CSV mode")));

	if (cstate->csv_mode && strlen(cstate->quote) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote must be a single one-byte character")));

	if (cstate->csv_mode && cstate->delim[0] == cstate->quote[0])
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter and quote must be different")));

	/* Check escape */
	if (!cstate->csv_mode && cstate->escape != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY escape available only in CSV mode")));

	if (cstate->csv_mode && strlen(cstate->escape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY escape must be a single one-byte character")));

	/* Check force_quote */
	if (!cstate->csv_mode && (cstate->force_quote || cstate->force_quote_all))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote available only in CSV mode")));
	if ((cstate->force_quote || cstate->force_quote_all) && is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote only available using COPY TO")));

	/* Check force_notnull */
	if (!cstate->csv_mode && cstate->force_notnull != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null available only in CSV mode")));
	if (cstate->force_notnull != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null only available using COPY FROM")));

	/* Check force_null */
	if (!cstate->csv_mode && cstate->force_null != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null available only in CSV mode")));

	if (cstate->force_null != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null only available using COPY FROM")));

	/* Don't allow the delimiter to appear in the null string. */
	if (strchr(cstate->null_print, cstate->delim[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must not appear in the NULL specification")));

	/* Don't allow the CSV quote char to appear in the null string. */
	if (cstate->csv_mode &&
		strchr(cstate->null_print, cstate->quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CSV quote character must not appear in the NULL specification")));
}

/*
 * Common setup routines used by BeginCopyFrom and BeginCopyTo.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
static CopyState
BeginCopy(ParseState *pstate,
		  bool is_from,
		  Relation rel,
		  RawStmt *raw_query,
		  Oid queryRelId,
		  List *attnamelist,
		  List *options)
{
	CopyState	cstate;
	TupleDesc	tupDesc;
	MemoryContext oldcontext;

	/* Allocate workspace and zero all fields */
	cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, cstate, is_from, options);

	/* Process the source/target relation or query */
	if (rel)
	{
		Assert(!raw_query);

		cstate->rel = rel;

		tupDesc = RelationGetDescr(cstate->rel);
	}
	else
	{
		List	   *rewritten;
		Query	   *query;
		PlannedStmt *plan;
		DestReceiver *dest;

		Assert(!is_from);
		cstate->rel = NULL;

		/*
		 * Run parse analysis and rewrite.  Note this also acquires sufficient
		 * locks on the source table(s).
		 *
		 * Because the parser and planner tend to scribble on their input, we
		 * make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that the COPY is in a portal or plpgsql
		 * function and is executed repeatedly.  (See also the same hack in
		 * DECLARE CURSOR and PREPARE.)  XXX FIXME someday.
		 */
		rewritten = pg_analyze_and_rewrite(copyObject(raw_query),
										   pstate->p_sourcetext, NULL, 0,
										   NULL);

		/* check that we got back something we can work with */
		if (rewritten == NIL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DO INSTEAD NOTHING rules are not supported for COPY")));
		}
		else if (list_length(rewritten) > 1)
		{
			ListCell   *lc;

			/* examine queries to determine which error message to issue */
			foreach(lc, rewritten)
			{
				Query	   *q = lfirst_node(Query, lc);

				if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("conditional DO INSTEAD rules are not supported for COPY")));
				if (q->querySource == QSRC_NON_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DO ALSO rules are not supported for the COPY")));
			}

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multi-statement DO INSTEAD rules are not supported for COPY")));
		}

		query = linitial_node(Query, rewritten);

		/* The grammar allows SELECT INTO, but we don't support that */
		if (query->utilityStmt != NULL &&
			IsA(query->utilityStmt, CreateTableAsStmt))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (SELECT INTO) is not supported")));

		Assert(query->utilityStmt == NULL);

		/*
		 * Similarly the grammar doesn't enforce the presence of a RETURNING
		 * clause, but this is required here.
		 */
		if (query->commandType != CMD_SELECT &&
			query->returningList == NIL)
		{
			Assert(query->commandType == CMD_INSERT ||
				   query->commandType == CMD_UPDATE ||
				   query->commandType == CMD_DELETE);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY query must have a RETURNING clause")));
		}

		/* plan the query */
		plan = pg_plan_query(query, pstate->p_sourcetext,
							 CURSOR_OPT_PARALLEL_OK, NULL);

		/*
		 * With row level security and a user using "COPY relation TO", we
		 * have to convert the "COPY relation TO" to a query-based COPY (eg:
		 * "COPY (SELECT * FROM relation) TO"), to allow the rewriter to add
		 * in any RLS clauses.
		 *
		 * When this happens, we are passed in the relid of the originally
		 * found relation (which we have locked).  As the planner will look up
		 * the relation again, we double-check here to make sure it found the
		 * same one that we have locked.
		 */
		if (queryRelId != InvalidOid)
		{
			/*
			 * Note that with RLS involved there may be multiple relations,
			 * and while the one we need is almost certainly first, we don't
			 * make any guarantees of that in the planner, so check the whole
			 * list and make sure we find the original relation.
			 */
			if (!list_member_oid(plan->relationOids, queryRelId))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("relation referenced by COPY statement has changed")));
		}

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create dest receiver for COPY OUT */
		dest = CreateDestReceiver(DestCopyOut);
		((DR_copy *) dest)->cstate = cstate;

		/* Create a QueryDesc requesting no output */
		cstate->queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
											GetActiveSnapshot(),
											InvalidSnapshot,
											dest, NULL, NULL, 0);

		/*
		 * Call ExecutorStart to prepare the plan for execution.
		 *
		 * ExecutorStart computes a result tupdesc for us
		 */
		ExecutorStart(cstate->queryDesc, 0);

		tupDesc = cstate->queryDesc->tupDesc;
	}

	PopulateCommonCstateInfo(cstate, tupDesc, attnamelist);
	cstate->copy_dest = COPY_FILE;	/* default */

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * PopulateCommonCstateInfo - Populates the common variables required for copy
 * from operation. This is a helper function for BeginCopy &
 * InitializeParallelCopyInfo function.
 */
static void
PopulateCommonCstateInfo(CopyState cstate, TupleDesc	tupDesc,
						 List *attnamelist)
{
	int			num_phys_attrs;

	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_QUOTE name list to per-column flags, check validity */
	cstate->force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_quote_all)
	{
		int			i;

		for (i = 0; i < num_phys_attrs; i++)
			cstate->force_quote_flags[i] = true;
	}
	else if (cstate->force_quote)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_quote);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_QUOTE column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_quote_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->force_null_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_null)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_null);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->convert_selectively)
	{
		List	   *attnums;
		ListCell   *cur;

		cstate->convert_select_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->convert_select);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg_internal("selected column \"%s\" not referenced by COPY",
										 NameStr(attr->attname))));
			cstate->convert_select_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();

	/*
	 * Set up encoding conversion info.  Even if the file and server encodings
	 * are the same, we must apply pg_any_to_server() to validate data in
	 * multibyte encodings.
	 */
	cstate->need_transcoding =
		(cstate->file_encoding != GetDatabaseEncoding() ||
		 pg_database_encoding_max_length() > 1);
	/* See Multibyte encoding comment above */
	cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
}

/*
 * Closes the pipe to an external program, checking the pclose() return code.
 */
static void
ClosePipeToProgram(CopyState cstate)
{
	int			pclose_rc;

	Assert(cstate->is_program);

	pclose_rc = ClosePipeStream(cstate->copy_file);
	if (pclose_rc == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	else if (pclose_rc != 0)
	{
		/*
		 * If we ended a COPY FROM PROGRAM before reaching EOF, then it's
		 * expectable for the called program to fail with SIGPIPE, and we
		 * should not report that as an error.  Otherwise, SIGPIPE indicates a
		 * problem.
		 */
		if (cstate->is_copy_from && !cstate->reached_eof &&
			wait_result_is_signal(pclose_rc, SIGPIPE))
			return;

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("program \"%s\" failed",
						cstate->filename),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
	}
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void
EndCopy(CopyState cstate)
{
	if (cstate->is_program)
	{
		ClosePipeToProgram(cstate);
	}
	else
	{
		if (cstate->filename != NULL && FreeFile(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							cstate->filename)));
	}

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Setup CopyState to read tuples from a table or a query for COPY TO.
 */
static CopyState
BeginCopyTo(ParseState *pstate,
			Relation rel,
			RawStmt *query,
			Oid queryRelId,
			const char *filename,
			bool is_program,
			List *attnamelist,
			List *options)
{
	CopyState	cstate;
	bool		pipe = (filename == NULL);
	MemoryContext oldcontext;

	if (rel != NULL && rel->rd_rel->relkind != RELKIND_RELATION)
	{
		if (rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from materialized view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from foreign table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from sequence \"%s\"",
							RelationGetRelationName(rel))));
		else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from partitioned table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from non-table relation \"%s\"",
							RelationGetRelationName(rel))));
	}

	cstate = BeginCopy(pstate, false, rel, query, queryRelId, attnamelist,
					   options);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	if (pipe)
	{
		Assert(!is_program);	/* the grammar does not allow this */
		if (whereToSendOutput != DestRemote)
			cstate->copy_file = stdout;
	}
	else
	{
		cstate->filename = pstrdup(filename);
		cstate->is_program = is_program;

		if (is_program)
		{
			cstate->copy_file = OpenPipeStream(cstate->filename, PG_BINARY_W);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			mode_t		oumask; /* Pre-existing umask value */
			struct stat st;

			/*
			 * Prevent write to relative path ... too easy to shoot oneself in
			 * the foot by overwriting a database file ...
			 */
			if (!is_absolute_path(filename))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("relative path not allowed for COPY to file")));

			oumask = umask(S_IWGRP | S_IWOTH);
			PG_TRY();
			{
				cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
			}
			PG_FINALLY();
			{
				umask(oumask);
			}
			PG_END_TRY();
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for writing: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY TO instructs the PostgreSQL server process to write a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * This intermediate routine exists mainly to localize the effects of setjmp
 * so we don't need to plaster a lot of variables with "volatile".
 */
static uint64
DoCopyTo(CopyState cstate)
{
	bool		pipe = (cstate->filename == NULL);
	bool		fe_copy = (pipe && whereToSendOutput == DestRemote);
	uint64		processed;

	PG_TRY();
	{
		if (fe_copy)
			SendCopyBegin(cstate);

		processed = CopyTo(cstate);

		if (fe_copy)
			SendCopyEnd(cstate);
	}
	PG_CATCH();
	{
		/*
		 * Make sure we turn off old-style COPY OUT mode upon error. It is
		 * okay to do this in all cases, since it does nothing if the mode is
		 * not on.
		 */
		pq_endcopyout(true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return processed;
}

/*
 * Clean up storage and release resources for COPY TO.
 */
static void
EndCopyTo(CopyState cstate)
{
	if (cstate->queryDesc != NULL)
	{
		/* Close down the query and free resources. */
		ExecutorFinish(cstate->queryDesc);
		ExecutorEnd(cstate->queryDesc);
		FreeQueryDesc(cstate->queryDesc);
		PopActiveSnapshot();
	}

	/* Clean up storage */
	EndCopy(cstate);
}

/*
 * Copy from relation or query TO file.
 */
static uint64
CopyTo(CopyState cstate)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	ListCell   *cur;
	uint64		processed;

	if (cstate->rel)
		tupDesc = RelationGetDescr(cstate->rel);
	else
		tupDesc = cstate->queryDesc->tupDesc;
	num_phys_attrs = tupDesc->natts;
	cstate->null_print_client = cstate->null_print; /* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		if (cstate->binary)
			getTypeBinaryOutputInfo(attr->atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr->atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_SIZES);

	if (cstate->binary)
	{
		/* Generate header for a binary copy */
		int32		tmp;

		/* Signature */
		CopySendData(cstate, BinarySignature, 11);
		/* Flags field */
		tmp = 0;
		CopySendInt32(cstate, tmp);
		/* No header extension */
		tmp = 0;
		CopySendInt32(cstate, tmp);
	}
	else
	{
		/*
		 * For non-binary copy, we need to convert null_print to file
		 * encoding, because it will be sent directly with CopySendString.
		 */
		if (cstate->need_transcoding)
			cstate->null_print_client = pg_server_to_any(cstate->null_print,
														 cstate->null_print_len,
														 cstate->file_encoding);

		/* if a header has been requested send the line */
		if (cstate->header_line)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->delim[0]);
				hdr_delim = true;

				colname = NameStr(TupleDescAttr(tupDesc, attnum - 1)->attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			CopySendEndOfRow(cstate);
		}
	}

	if (cstate->rel)
	{
		TupleTableSlot *slot;
		TableScanDesc scandesc;

		scandesc = table_beginscan(cstate->rel, GetActiveSnapshot(), 0, NULL);
		slot = table_slot_create(cstate->rel, NULL);

		processed = 0;
		while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
		{
			CHECK_FOR_INTERRUPTS();

			/* Deconstruct the tuple ... */
			slot_getallattrs(slot);

			/* Format and send the data */
			CopyOneRowTo(cstate, slot);
			processed++;
		}

		ExecDropSingleTupleTableSlot(slot);
		table_endscan(scandesc);
	}
	else
	{
		/* run the plan --- the dest receiver will send tuples */
		ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L, true);
		processed = ((DR_copy *) cstate->queryDesc->dest)->processed;
	}

	if (cstate->binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);
		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	MemoryContextDelete(cstate->rowcontext);

	return processed;
}

/*
 * Emit one row during CopyTo().
 */
static void
CopyOneRowTo(CopyState cstate, TupleTableSlot *slot)
{
	bool		need_delim = false;
	FmgrInfo   *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	char	   *string;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	if (cstate->binary)
	{
		/* Binary per-tuple header */
		CopySendInt16(cstate, list_length(cstate->attnumlist));
	}

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = slot->tts_values[attnum - 1];
		bool		isnull = slot->tts_isnull[attnum - 1];

		if (!cstate->binary)
		{
			if (need_delim)
				CopySendChar(cstate, cstate->delim[0]);
			need_delim = true;
		}

		if (isnull)
		{
			if (!cstate->binary)
				CopySendString(cstate, cstate->null_print_client);
			else
				CopySendInt32(cstate, -1);
		}
		else
		{
			if (!cstate->binary)
			{
				string = OutputFunctionCall(&out_functions[attnum - 1],
											value);
				if (cstate->csv_mode)
					CopyAttributeOutCSV(cstate, string,
										cstate->force_quote_flags[attnum - 1],
										list_length(cstate->attnumlist) == 1);
				else
					CopyAttributeOutText(cstate, string);
			}
			else
			{
				bytea	   *outputbytes;

				outputbytes = SendFunctionCall(&out_functions[attnum - 1],
											   value);
				CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(cstate, VARDATA(outputbytes),
							 VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
	}

	CopySendEndOfRow(cstate);

	MemoryContextSwitchTo(oldcontext);
}


/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyState.
 */
void
CopyFromErrorCallback(void *arg)
{
	CopyState	cstate = (CopyState) arg;
	char		curlineno_str[32];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	if (cstate->binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("COPY %s, line %s, column %s",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		else
			errcontext("COPY %s, line %s",
					   cstate->cur_relname, curlineno_str);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char	   *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("COPY %s, line %s, column %s: \"%s\"",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname, attval);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("COPY %s, line %s, column %s: null input",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, and it's already
			 * transcoded, print it. If it's still in a foreign encoding, it's
			 * quite likely that the error is precisely a failure to do
			 * encoding conversion (ie, bad data). We dare not try to convert
			 * it, and at present there's no way to regurgitate it without
			 * conversion. So we have to punt and just report the line number.
			 */
			if (cstate->line_buf_valid &&
				(cstate->line_buf_converted || !cstate->need_transcoding))
			{
				char	   *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("COPY %s, line %s: \"%s\"",
						   cstate->cur_relname, curlineno_str, lineval);
				pfree(lineval);
			}
			else
			{
				errcontext("COPY %s, line %s",
						   cstate->cur_relname, curlineno_str);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * Returns a pstrdup'd copy of the input.
 */
static char *
limit_printout_length(const char *str)
{
#define MAX_COPY_DATA_DISPLAY 100

	int			slen = strlen(str);
	int			len;
	char	   *res;

	/* Fast path if definitely okay */
	if (slen <= MAX_COPY_DATA_DISPLAY)
		return pstrdup(str);

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

	/*
	 * Truncate, and add "..." to show we truncated the input.
	 */
	res = (char *) palloc(len + 4);
	memcpy(res, str, len);
	strcpy(res + len, "...");

	return res;
}

/*
 * Allocate memory and initialize a new CopyMultiInsertBuffer for this
 * ResultRelInfo.
 */
static CopyMultiInsertBuffer *
CopyMultiInsertBufferInit(ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = (CopyMultiInsertBuffer *) palloc(sizeof(CopyMultiInsertBuffer));
	memset(buffer->slots, 0, sizeof(TupleTableSlot *) * MAX_BUFFERED_TUPLES);
	buffer->resultRelInfo = rri;
	buffer->bistate = GetBulkInsertState();
	buffer->nused = 0;

	return buffer;
}

/*
 * Make a new buffer for this ResultRelInfo.
 */
static inline void
CopyMultiInsertInfoSetupBuffer(CopyMultiInsertInfo *miinfo,
							   ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = CopyMultiInsertBufferInit(rri);

	/* Setup back-link so we can easily find this buffer again */
	rri->ri_CopyMultiInsertBuffer = buffer;
	/* Record that we're tracking this buffer */
	miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
}

/*
 * Initialize an already allocated CopyMultiInsertInfo.
 *
 * If rri is a non-partitioned table then a CopyMultiInsertBuffer is set up
 * for that table.
 */
static void
CopyMultiInsertInfoInit(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						CopyState cstate, EState *estate, CommandId mycid,
						int ti_options)
{
	miinfo->multiInsertBuffers = NIL;
	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;
	miinfo->cstate = cstate;
	miinfo->estate = estate;
	miinfo->mycid = mycid;
	miinfo->ti_options = ti_options;

	/*
	 * Only setup the buffer when not dealing with a partitioned table.
	 * Buffers for partitioned tables will just be setup when we need to send
	 * tuples their way for the first time.
	 */
	if (rri->ri_RelationDesc->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		CopyMultiInsertInfoSetupBuffer(miinfo, rri);
}

/*
 * Returns true if the buffers are full
 */
static inline bool
CopyMultiInsertInfoIsFull(CopyMultiInsertInfo *miinfo)
{
	if (miinfo->bufferedTuples >= MAX_BUFFERED_TUPLES ||
		miinfo->bufferedBytes >= MAX_BUFFERED_BYTES)
		return true;
	return false;
}

/*
 * Returns true if we have no buffered tuples
 */
static inline bool
CopyMultiInsertInfoIsEmpty(CopyMultiInsertInfo *miinfo)
{
	return miinfo->bufferedTuples == 0;
}

/*
 * Write the tuples stored in 'buffer' out to the table.
 */
static inline void
CopyMultiInsertBufferFlush(CopyMultiInsertInfo *miinfo,
						   CopyMultiInsertBuffer *buffer)
{
	MemoryContext oldcontext;
	int			i;
	uint64		save_cur_lineno;
	CopyState	cstate = miinfo->cstate;
	EState	   *estate = miinfo->estate;
	CommandId	mycid = miinfo->mycid;
	int			ti_options = miinfo->ti_options;
	bool		line_buf_valid = cstate->line_buf_valid;
	int			nused = buffer->nused;
	ResultRelInfo *resultRelInfo = buffer->resultRelInfo;
	TupleTableSlot **slots = buffer->slots;

	/* Set es_result_relation_info to the ResultRelInfo we're flushing. */
	estate->es_result_relation_info = resultRelInfo;

	/*
	 * Print error context information correctly, if one of the operations
	 * below fail.
	 */
	cstate->line_buf_valid = false;
	save_cur_lineno = cstate->cur_lineno;

	/*
	 * table_multi_insert may leak memory, so switch to short-lived memory
	 * context before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	table_multi_insert(resultRelInfo->ri_RelationDesc,
					   slots,
					   nused,
					   mycid,
					   ti_options,
					   buffer->bistate);
	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < nused; i++)
	{
		/*
		 * If there are any indexes, update them for all the inserted tuples,
		 * and run AFTER ROW INSERT triggers.
		 */
		if (resultRelInfo->ri_NumIndices > 0)
		{
			List	   *recheckIndexes;

			cstate->cur_lineno = buffer->linenos[i];
			recheckIndexes =
				ExecInsertIndexTuples(buffer->slots[i], estate, false, NULL,
									  NIL);
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], recheckIndexes,
								 cstate->transition_capture);
			list_free(recheckIndexes);
		}

		/*
		 * There's no indexes, but see if we need to run AFTER ROW INSERT
		 * triggers anyway.
		 */
		else if (resultRelInfo->ri_TrigDesc != NULL &&
				 (resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
				  resultRelInfo->ri_TrigDesc->trig_insert_new_table))
		{
			cstate->cur_lineno = buffer->linenos[i];
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], NIL, cstate->transition_capture);
		}

		ExecClearTuple(slots[i]);
	}

	/* Mark that all slots are free */
	buffer->nused = 0;

	/* reset cur_lineno and line_buf_valid to what they were */
	cstate->line_buf_valid = line_buf_valid;
	cstate->cur_lineno = save_cur_lineno;
}

/*
 * Drop used slots and free member for this buffer.
 *
 * The buffer must be flushed before cleanup.
 */
static inline void
CopyMultiInsertBufferCleanup(CopyMultiInsertInfo *miinfo,
							 CopyMultiInsertBuffer *buffer)
{
	int			i;

	/* Ensure buffer was flushed */
	Assert(buffer->nused == 0);

	/* Remove back-link to ourself */
	buffer->resultRelInfo->ri_CopyMultiInsertBuffer = NULL;

	FreeBulkInsertState(buffer->bistate);

	/* Since we only create slots on demand, just drop the non-null ones. */
	for (i = 0; i < MAX_BUFFERED_TUPLES && buffer->slots[i] != NULL; i++)
		ExecDropSingleTupleTableSlot(buffer->slots[i]);

	table_finish_bulk_insert(buffer->resultRelInfo->ri_RelationDesc,
							 miinfo->ti_options);

	pfree(buffer);
}

/*
 * Write out all stored tuples in all buffers out to the tables.
 *
 * Once flushed we also trim the tracked buffers list down to size by removing
 * the buffers created earliest first.
 *
 * Callers should pass 'curr_rri' is the ResultRelInfo that's currently being
 * used.  When cleaning up old buffers we'll never remove the one for
 * 'curr_rri'.
 */
static inline void
CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo, ResultRelInfo *curr_rri)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
	{
		CopyMultiInsertBuffer *buffer = (CopyMultiInsertBuffer *) lfirst(lc);

		CopyMultiInsertBufferFlush(miinfo, buffer);
	}

	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;

	/*
	 * Trim the list of tracked buffers down if it exceeds the limit.  Here we
	 * remove buffers starting with the ones we created first.  It seems more
	 * likely that these older ones are less likely to be needed than ones
	 * that were just created.
	 */
	while (list_length(miinfo->multiInsertBuffers) > MAX_PARTITION_BUFFERS)
	{
		CopyMultiInsertBuffer *buffer;

		buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);

		/*
		 * We never want to remove the buffer that's currently being used, so
		 * if we happen to find that then move it to the end of the list.
		 */
		if (buffer->resultRelInfo == curr_rri)
		{
			miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
			miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
			buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);
		}

		CopyMultiInsertBufferCleanup(miinfo, buffer);
		miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
	}
}

/*
 * Cleanup allocated buffers and free memory
 */
static inline void
CopyMultiInsertInfoCleanup(CopyMultiInsertInfo *miinfo)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
		CopyMultiInsertBufferCleanup(miinfo, lfirst(lc));

	list_free(miinfo->multiInsertBuffers);
}

/*
 * Get the next TupleTableSlot that the next tuple should be stored in.
 *
 * Callers must ensure that the buffer is not full.
 *
 * Note: 'miinfo' is unused but has been included for consistency with the
 * other functions in this area.
 */
static inline TupleTableSlot *
CopyMultiInsertInfoNextFreeSlot(CopyMultiInsertInfo *miinfo,
								ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;
	int			nused = buffer->nused;

	Assert(buffer != NULL);
	Assert(nused < MAX_BUFFERED_TUPLES);

	if (buffer->slots[nused] == NULL)
		buffer->slots[nused] = table_slot_create(rri->ri_RelationDesc, NULL);
	return buffer->slots[nused];
}

/*
 * Record the previously reserved TupleTableSlot that was reserved by
 * CopyMultiInsertInfoNextFreeSlot as being consumed.
 */
static inline void
CopyMultiInsertInfoStore(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						 TupleTableSlot *slot, int tuplen, uint64 lineno)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;

	Assert(buffer != NULL);
	Assert(slot == buffer->slots[buffer->nused]);

	/* Store the line number so we can properly report any errors later */
	buffer->linenos[buffer->nused] = lineno;

	/* Record this slot as being used */
	buffer->nused++;

	/* Update how many tuples are stored and their size */
	miinfo->bufferedTuples++;
	miinfo->bufferedBytes += tuplen;
}

/*
 * Check if the relation specified in copy from is valid.
 */
static void
CheckTargetRelValidity(CopyState cstate)
{
	Assert(cstate->rel);

	/*
	 * The target must be a plain, foreign, or partitioned relation, or have
	 * an INSTEAD OF INSERT row trigger.  (Currently, such triggers are only
	 * allowed on views, so we only hint about them in the view case.)
	 */
	if (cstate->rel->rd_rel->relkind != RELKIND_RELATION &&
		cstate->rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		!(cstate->rel->trigdesc &&
		  cstate->rel->trigdesc->trig_insert_instead_row))
	{
		if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(cstate->rel)),
					 errhint("To enable copying to a view, provide an INSTEAD OF INSERT trigger.")));
		else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(cstate->rel))));
	}

	if (cstate->freeze)
	{
		/*
		 * We currently disallow COPY FREEZE on partitioned tables.  The
		 * reason for this is that we've simply not yet opened the partitions
		 * to determine if the optimization can be applied to them.  We could
		 * go and open them all here, but doing so may be quite a costly
		 * overhead for small copies.  In any case, we may just end up routing
		 * tuples to a small number of partitions.  It seems better just to
		 * raise an ERROR for partitioned tables.
		 */
		if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot perform COPY FREEZE on a partitioned table")));
		}

		/*
		 * Tolerate one registration for the benefit of FirstXactSnapshot.
		 * Scan-bearing queries generally create at least two registrations,
		 * though relying on that is fragile, as is ignoring ActiveSnapshot.
		 * Clear CatalogSnapshot to avoid counting its registration.  We'll
		 * still detect ongoing catalog scans, each of which separately
		 * registers the snapshot it uses.
		 */
		InvalidateCatalogSnapshot();
		if (!ThereAreNoPriorRegisteredSnapshots() || !ThereAreNoReadyPortals())
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot perform COPY FREEZE because of prior transaction activity")));

		if (cstate->rel->rd_createSubid != GetCurrentSubTransactionId() &&
			cstate->rel->rd_newRelfilenodeSubid != GetCurrentSubTransactionId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction")));
	}
}

/*
 * Copy FROM file to relation.
 */
uint64
CopyFrom(CopyState cstate)
{
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *target_resultRelInfo;
	ResultRelInfo *prevResultRelInfo = NULL;
	EState	   *estate = CreateExecutorState(); /* for ExecConstraints() */
	ModifyTableState *mtstate;
	ExprContext *econtext;
	TupleTableSlot *singleslot = NULL;
	MemoryContext oldcontext = CurrentMemoryContext;

	PartitionTupleRouting *proute = NULL;
	ErrorContextCallback errcallback;
	CommandId	mycid = IsParallelCopy() ? GetCurrentCommandId(false) :
										   GetCurrentCommandId(true);
	int			ti_options = 0; /* start with default options for insert */
	BulkInsertState bistate = NULL;
	CopyInsertMethod insertMethod;
	CopyMultiInsertInfo multiInsertInfo = {0};	/* pacify compiler */
	uint64		processed = 0;
	bool		has_before_insert_row_trig;
	bool		has_instead_insert_row_trig;
	bool		leafpart_use_multi_insert = false;

	/*
	 * Perform this check if it is not parallel copy. In case of parallel
	 * copy, this check is done by the leader, so that if any invalid case
	 * exist the copy from command will error out from the leader itself,
	 * avoiding launching workers, just to throw error.
	 */
	if (!IsParallelCopy())
		CheckTargetRelValidity(cstate);

	/*
	 * If the target file is new-in-transaction, we assume that checking FSM
	 * for free space is a waste of time.  This could possibly be wrong, but
	 * it's unlikely.
	 */
	if (RELKIND_HAS_STORAGE(cstate->rel->rd_rel->relkind) &&
		(cstate->rel->rd_createSubid != InvalidSubTransactionId ||
		 cstate->rel->rd_firstRelfilenodeSubid != InvalidSubTransactionId))
		ti_options |= TABLE_INSERT_SKIP_FSM;

	/*
	 * Optimize if new relfilenode was created in this subxact or one of its
	 * committed children and we won't see those rows later as part of an
	 * earlier scan or command. The subxact test ensures that if this subxact
	 * aborts then the frozen rows won't be visible after xact cleanup.  Note
	 * that the stronger test of exactly which subtransaction created it is
	 * crucial for correctness of this optimization. The test for an earlier
	 * scan or command tolerates false negatives. FREEZE causes other sessions
	 * to see rows they would not see under MVCC, and a false negative merely
	 * spreads that anomaly to the current session.
	 */
	if (cstate->freeze)
		ti_options |= TABLE_INSERT_FROZEN;

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo,
					  cstate->rel,
					  1,		/* must match rel's position in range_table */
					  NULL,
					  0);
	target_resultRelInfo = resultRelInfo;

	/* Verify the named relation is a valid target for INSERT */
	if (!IsParallelCopy())
		CheckValidResultRel(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	ExecInitRangeTable(estate, cstate->range_table);

	/*
	 * Set up a ModifyTableState so we can let FDW(s) init themselves for
	 * foreign-table result relation(s).
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->resultRelInfo = estate->es_result_relations;

	if (resultRelInfo->ri_FdwRoutine != NULL &&
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert != NULL)
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert(mtstate,
														 resultRelInfo);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 *
	 * Because partition tuple routing would like to know about whether
	 * transition capture is active, we also set it in mtstate, which is
	 * passed to ExecFindPartition() below.
	 */
	cstate->transition_capture = mtstate->mt_transition_capture =
		MakeTransitionCaptureState(cstate->rel->trigdesc,
								   RelationGetRelid(cstate->rel),
								   CMD_INSERT);

	/*
	 * If the named relation is a partitioned table, initialize state for
	 * CopyFrom tuple routing.
	 */
	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		proute = ExecSetupPartitionTupleRouting(estate, NULL, cstate->rel);

	if (cstate->whereClause)
		cstate->qualexpr = ExecInitQual(castNode(List, cstate->whereClause),
										&mtstate->ps);

	/*
	 * It's generally more efficient to prepare a bunch of tuples for
	 * insertion, and insert them in one table_multi_insert() call, than call
	 * table_tuple_insert() separately for every tuple. However, there are a
	 * number of reasons why we might not be able to do this.  These are
	 * explained below.
	 */
	if (resultRelInfo->ri_TrigDesc != NULL &&
		(resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		 resultRelInfo->ri_TrigDesc->trig_insert_instead_row))
	{
		/*
		 * Can't support multi-inserts when there are any BEFORE/INSTEAD OF
		 * triggers on the table. Such triggers might query the table we're
		 * inserting into and act differently if the tuples that have already
		 * been processed and prepared for insertion are not there.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (proute != NULL && resultRelInfo->ri_TrigDesc != NULL &&
			 resultRelInfo->ri_TrigDesc->trig_insert_new_table)
	{
		/*
		 * For partitioned tables we can't support multi-inserts when there
		 * are any statement level insert triggers. It might be possible to
		 * allow partitioned tables with such triggers in the future, but for
		 * now, CopyMultiInsertInfoFlush expects that any before row insert
		 * and statement level insert triggers are on the same relation.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (resultRelInfo->ri_FdwRoutine != NULL ||
			 cstate->volatile_defexprs)
	{
		/*
		 * Can't support multi-inserts to foreign tables or if there are any
		 * volatile default expressions in the table.  Similarly to the
		 * trigger case above, such expressions may query the table we're
		 * inserting into.
		 *
		 * Note: It does not matter if any partitions have any volatile
		 * default expressions as we use the defaults from the target of the
		 * COPY command.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (contain_volatile_functions(cstate->whereClause))
	{
		/*
		 * Can't support multi-inserts if there are any volatile function
		 * expressions in WHERE clause.  Similarly to the trigger case above,
		 * such expressions may query the table we're inserting into.
		 */
		insertMethod = CIM_SINGLE;
	}
	else
	{
		/*
		 * For partitioned tables, we may still be able to perform bulk
		 * inserts.  However, the possibility of this depends on which types
		 * of triggers exist on the partition.  We must disable bulk inserts
		 * if the partition is a foreign table or it has any before row insert
		 * or insert instead triggers (same as we checked above for the parent
		 * table).  Since the partition's resultRelInfos are initialized only
		 * when we actually need to insert the first tuple into them, we must
		 * have the intermediate insert method of CIM_MULTI_CONDITIONAL to
		 * flag that we must later determine if we can use bulk-inserts for
		 * the partition being inserted into.
		 */
		if (proute)
			insertMethod = CIM_MULTI_CONDITIONAL;
		else
			insertMethod = CIM_MULTI;

		CopyMultiInsertInfoInit(&multiInsertInfo, resultRelInfo, cstate,
								estate, mycid, ti_options);
	}

	/*
	 * If not using batch mode (which allocates slots as needed) set up a
	 * tuple slot too. When inserting into a partitioned table, we also need
	 * one, even if we might batch insert, to read the tuple in the root
	 * partition's form.
	 */
	if (insertMethod == CIM_SINGLE || insertMethod == CIM_MULTI_CONDITIONAL)
	{
		singleslot = table_slot_create(resultRelInfo->ri_RelationDesc,
									   &estate->es_tupleTable);
		bistate = GetBulkInsertState();
	}

	has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

	has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

	if (!IsParallelCopy())
	{
		/*
		 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
		 * should do this for COPY, since it's not really an "INSERT" statement as
		 * such. However, executing these triggers maintains consistency with the
		 * EACH ROW triggers that we already fire on COPY.
		 */
		ExecBSInsertTriggers(estate, resultRelInfo);
	}

	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	for (;;)
	{
		TupleTableSlot *myslot;
		bool		skip_tuple;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Reset the per-tuple exprcontext. We do this after every tuple, to
		 * clean-up after expression evaluations etc.
		 */
		ResetPerTupleExprContext(estate);

		/* select slot to (initially) load row into */
		if (insertMethod == CIM_SINGLE || proute)
		{
			myslot = singleslot;
			Assert(myslot != NULL);
		}
		else
		{
			Assert(resultRelInfo == target_resultRelInfo);
			Assert(insertMethod == CIM_MULTI);

			myslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
													 resultRelInfo);
		}

		/*
		 * Switch to per-tuple context before calling NextCopyFrom, which does
		 * evaluate default expressions etc. and requires per-tuple context.
		 */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		ExecClearTuple(myslot);

		/* Directly store the values/nulls array in the slot */
		if (!NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
			break;

		ExecStoreVirtualTuple(myslot);

		/*
		 * Constraints and where clause might reference the tableoid column,
		 * so (re-)initialize tts_tableOid before evaluating them.
		 */
		myslot->tts_tableOid = RelationGetRelid(target_resultRelInfo->ri_RelationDesc);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		if (cstate->whereClause)
		{
			econtext->ecxt_scantuple = myslot;
			/* Skip items that don't match COPY's WHERE clause */
			if (!ExecQual(cstate->qualexpr, econtext))
				continue;
		}

		/* Determine the partition to insert the tuple into */
		if (proute)
		{
			TupleConversionMap *map;

			/*
			 * Attempt to find a partition suitable for this tuple.
			 * ExecFindPartition() will raise an error if none can be found or
			 * if the found partition is not suitable for INSERTs.
			 */
			resultRelInfo = ExecFindPartition(mtstate, target_resultRelInfo,
											  proute, myslot, estate);

			if (prevResultRelInfo != resultRelInfo)
			{
				/* Determine which triggers exist on this partition */
				has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

				has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

				/*
				 * Disable multi-inserts when the partition has BEFORE/INSTEAD
				 * OF triggers, or if the partition is a foreign partition.
				 */
				leafpart_use_multi_insert = insertMethod == CIM_MULTI_CONDITIONAL &&
					!has_before_insert_row_trig &&
					!has_instead_insert_row_trig &&
					resultRelInfo->ri_FdwRoutine == NULL;

				/*
				 * If the table has any partitions that are either foreign or
				 * has BEFORE/INSTEAD OF triggers, we can't perform copy
				 * operations with parallel workers.
				 */
				if (!leafpart_use_multi_insert && IsParallelWorker())
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform PARALLEL COPY if partition has BEFORE/INSTEAD OF triggers, or if the partition is foreign partition"),
							errhint("Try COPY without PARALLEL option")));

				/* Set the multi-insert buffer to use for this partition. */
				if (leafpart_use_multi_insert)
				{
					if (resultRelInfo->ri_CopyMultiInsertBuffer == NULL)
						CopyMultiInsertInfoSetupBuffer(&multiInsertInfo,
													   resultRelInfo);
				}
				else if (insertMethod == CIM_MULTI_CONDITIONAL &&
						 !CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
				{
					/*
					 * Flush pending inserts if this partition can't use
					 * batching, so rows are visible to triggers etc.
					 */
					CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}

				if (bistate != NULL)
					ReleaseBulkInsertStatePin(bistate);
				prevResultRelInfo = resultRelInfo;
			}

			/*
			 * For ExecInsertIndexTuples() to work on the partition's indexes
			 */
			estate->es_result_relation_info = resultRelInfo;

			/*
			 * If we're capturing transition tuples, we might need to convert
			 * from the partition rowtype to root rowtype.
			 */
			if (cstate->transition_capture != NULL)
			{
				if (has_before_insert_row_trig)
				{
					/*
					 * If there are any BEFORE triggers on the partition,
					 * we'll have to be ready to convert their result back to
					 * tuplestore format.
					 */
					cstate->transition_capture->tcs_original_insert_tuple = NULL;
					cstate->transition_capture->tcs_map =
						resultRelInfo->ri_PartitionInfo->pi_PartitionToRootMap;
				}
				else
				{
					/*
					 * Otherwise, just remember the original unconverted
					 * tuple, to avoid a needless round trip conversion.
					 */
					cstate->transition_capture->tcs_original_insert_tuple = myslot;
					cstate->transition_capture->tcs_map = NULL;
				}
			}

			/*
			 * We might need to convert from the root rowtype to the partition
			 * rowtype.
			 */
			map = resultRelInfo->ri_PartitionInfo->pi_RootToPartitionMap;
			if (insertMethod == CIM_SINGLE || !leafpart_use_multi_insert)
			{
				/* non batch insert */
				if (map != NULL)
				{
					TupleTableSlot *new_slot;

					new_slot = resultRelInfo->ri_PartitionInfo->pi_PartitionTupleSlot;
					myslot = execute_attr_map_slot(map->attrMap, myslot, new_slot);
				}
			}
			else
			{
				/*
				 * Prepare to queue up tuple for later batch insert into
				 * current partition.
				 */
				TupleTableSlot *batchslot;

				/* no other path available for partitioned table */
				Assert(insertMethod == CIM_MULTI_CONDITIONAL);

				batchslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
															resultRelInfo);

				if (map != NULL)
					myslot = execute_attr_map_slot(map->attrMap, myslot,
												   batchslot);
				else
				{
					/*
					 * This looks more expensive than it is (Believe me, I
					 * optimized it away. Twice.). The input is in virtual
					 * form, and we'll materialize the slot below - for most
					 * slot types the copy performs the work materialization
					 * would later require anyway.
					 */
					ExecCopySlot(batchslot, myslot);
					myslot = batchslot;
				}
			}

			/* ensure that triggers etc see the right relation  */
			myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		}

		skip_tuple = false;

		/* BEFORE ROW INSERT Triggers */
		if (has_before_insert_row_trig)
		{
			if (!ExecBRInsertTriggers(estate, resultRelInfo, myslot))
				skip_tuple = true;	/* "do nothing" */
		}

		if (!skip_tuple)
		{
			/*
			 * If there is an INSTEAD OF INSERT ROW trigger, let it handle the
			 * tuple.  Otherwise, proceed with inserting the tuple into the
			 * table or foreign table.
			 */
			if (has_instead_insert_row_trig)
			{
				ExecIRInsertTriggers(estate, resultRelInfo, myslot);
			}
			else
			{
				/* Compute stored generated columns */
				if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
					resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored)
					ExecComputeStoredGenerated(estate, myslot, CMD_INSERT);

				/*
				 * If the target is a plain table, check the constraints of
				 * the tuple.
				 */
				if (resultRelInfo->ri_FdwRoutine == NULL &&
					resultRelInfo->ri_RelationDesc->rd_att->constr)
					ExecConstraints(resultRelInfo, myslot, estate);

				/*
				 * Also check the tuple against the partition constraint, if
				 * there is one; except that if we got here via tuple-routing,
				 * we don't need to if there's no BR trigger defined on the
				 * partition.
				 */
				if (resultRelInfo->ri_PartitionCheck &&
					(proute == NULL || has_before_insert_row_trig))
					ExecPartitionCheck(resultRelInfo, myslot, estate, true);

				/* Store the slot in the multi-insert buffer, when enabled. */
				if (insertMethod == CIM_MULTI || leafpart_use_multi_insert)
				{
					/*
					 * The slot previously might point into the per-tuple
					 * context. For batching it needs to be longer lived.
					 */
					ExecMaterializeSlot(myslot);

					/* Add this tuple to the tuple buffer */
					CopyMultiInsertInfoStore(&multiInsertInfo,
											 resultRelInfo, myslot,
											 cstate->line_buf.len,
											 cstate->cur_lineno);

					/*
					 * If enough inserts have queued up, then flush all
					 * buffers out to their tables.
					 */
					if (CopyMultiInsertInfoIsFull(&multiInsertInfo))
						CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}
				else
				{
					List	   *recheckIndexes = NIL;

					/* OK, store the tuple */
					if (resultRelInfo->ri_FdwRoutine != NULL)
					{
						myslot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
																				 resultRelInfo,
																				 myslot,
																				 NULL);

						if (myslot == NULL) /* "do nothing" */
							continue;	/* next tuple please */

						/*
						 * AFTER ROW Triggers might reference the tableoid
						 * column, so (re-)initialize tts_tableOid before
						 * evaluating them.
						 */
						myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
					}
					else
					{
						/* OK, store the tuple and create index entries for it */
						table_tuple_insert(resultRelInfo->ri_RelationDesc,
										   myslot, mycid, ti_options, bistate);

						if (resultRelInfo->ri_NumIndices > 0)
							recheckIndexes = ExecInsertIndexTuples(myslot,
																   estate,
																   false,
																   NULL,
																   NIL);
					}

					/* AFTER ROW INSERT Triggers */
					ExecARInsertTriggers(estate, resultRelInfo, myslot,
										 recheckIndexes, cstate->transition_capture);

					list_free(recheckIndexes);
				}
			}

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger
			 * or FDW; this is the same definition used by nodeModifyTable.c
			 * for counting tuples inserted by an INSERT command.
			 */
			INCREMENTPROCESSED(processed)
		}
	}

	/* Flush any remaining buffered tuples */
	if (insertMethod != CIM_SINGLE)
	{
		if (!CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
			CopyMultiInsertInfoFlush(&multiInsertInfo, NULL);
	}

	/* Done, clean up */
	error_context_stack = errcallback.previous;

	if (bistate != NULL)
		FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * In the old protocol, tell pqcomm that we can process normal protocol
	 * messages again.
	 */
	if (cstate->copy_dest == COPY_OLD_FE)
		pq_endmsgread();

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, target_resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Allow the FDW to shut down */
	if (target_resultRelInfo->ri_FdwRoutine != NULL &&
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert != NULL)
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert(estate,
															  target_resultRelInfo);

	/* Tear down the multi-insert buffer data */
	if (insertMethod != CIM_SINGLE)
		CopyMultiInsertInfoCleanup(&multiInsertInfo);

	ExecCloseIndices(target_resultRelInfo);

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (proute)
		ExecCleanupTupleRouting(mtstate, proute);

	/* Close any trigger target relations */
	ExecCleanUpTriggerState(estate);

	FreeExecutorState(estate);

	GETPROCESSED(processed)
}

/*
 * PopulateCstateCatalogInfo - populate the catalog information.
 */
static void
PopulateCstateCatalogInfo(CopyState cstate)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				num_defaults;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	int		   *defmap;
	ExprState **defexprs;
	bool		volatile_defexprs;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	num_defaults = 0;
	volatile_defexprs = false;

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		if (cstate->binary)
			getTypeBinaryInputInfo(att->atttypid,
								   &in_func_oid, &typioparams[attnum - 1]);
		else
			getTypeInputInfo(att->atttypid,
							 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum) && !att->attgenerated)
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Expr	   *defexpr = (Expr *) build_column_default(cstate->rel,
																attnum);

			if (defexpr != NULL)
			{
				/* Run the expression through planner */
				defexpr = expression_planner(defexpr);

				/* Initialize executable expression in copycontext */
				defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;

				/*
				 * If a default expression looks at the table being loaded,
				 * then it could give the wrong answer when using
				 * multi-insert. Since database access can be dynamic this is
				 * hard to test for exactly, so we use the much wider test of
				 * whether the default expression is volatile. We allow for
				 * the special case of when the default expression is the
				 * nextval() of a sequence which in this specific case is
				 * known to be safe for use with the multi-insert
				 * optimization. Hence we use this special case function
				 * checker rather than the standard check for
				 * contain_volatile_functions().
				 */
				if (!volatile_defexprs)
					volatile_defexprs = contain_volatile_functions_not_nextval((Node *) defexpr);
			}
		}
	}

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->defmap = defmap;
	cstate->defexprs = defexprs;
	cstate->volatile_defexprs = volatile_defexprs;
	cstate->num_defaults = num_defaults;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
CopyState
BeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  const char *filename,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  List *attnamelist,
			  List *options)
{
	CopyState	cstate;
	bool		pipe = (filename == NULL);
	MemoryContext oldcontext;

	cstate = BeginCopy(pstate, true, rel, NULL, InvalidOid, attnamelist, options);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	cstate->reached_eof = false;
	cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/*
	 * Set up variables to avoid per-attribute overhead.  attribute_buf is
	 * used in both text and binary modes, but we use line_buf and raw_buf
	 * only in text mode.
	 */
	initStringInfo(&cstate->attribute_buf);
	cstate->raw_buf = (IsParallelCopy()) ? NULL : (char *) palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;
	if (!cstate->binary)
	{
		initStringInfo(&cstate->line_buf);
		cstate->line_buf_converted = false;
	}

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	PopulateCstateCatalogInfo(cstate);
	cstate->is_program = is_program;

	if (data_source_cb)
	{
		cstate->copy_dest = COPY_CALLBACK;
		cstate->data_source_cb = data_source_cb;
	}
	else if (pipe)
	{
		Assert(!is_program);	/* the grammar does not allow this */
		if (whereToSendOutput == DestRemote)
			ReceiveCopyBegin(cstate);
		else
			cstate->copy_file = stdin;
	}
	else
	{
		cstate->filename = pstrdup(filename);

		if (cstate->is_program)
		{
			cstate->copy_file = OpenPipeStream(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			struct stat st;

			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for reading: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY FROM instructs the PostgreSQL server process to read a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));
		}
	}

	if (cstate->binary)
	{
		/* Read and verify binary header */
		char		readSig[11];
		int32		tmp;

		/* Signature */
		if (CopyReadBinaryData(cstate, readSig, 11) != 11 ||
			memcmp(readSig, BinarySignature, 11) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("COPY file signature not recognized")));
		/* Flags field */
		if (!CopyGetInt32(cstate, &tmp))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (missing flags)")));
		if ((tmp & (1 << 16)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (WITH OIDS)")));
		tmp &= ~(1 << 16);
		if ((tmp >> 16) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unrecognized critical flags in COPY file header")));
		/* Header extension length */
		if (!CopyGetInt32(cstate, &tmp) ||
			tmp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (missing length)")));
		/* Skip extension header, if present */
		while (tmp-- > 0)
		{
			if (CopyReadBinaryData(cstate, readSig, 1) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid COPY file header (wrong length)")));
		}
	}

	/* create workspace for CopyReadAttributes results */
	if (!cstate->binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Read raw fields in the next line for COPY FROM in text or csv mode.
 * Return false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 */
bool
NextCopyFromRawFields(CopyState cstate, char ***fields, int *nfields)
{
	int			fldct;
	bool		done;

	/* only available for text or csv input */
	Assert(!cstate->binary);

	if (IsParallelCopy())
	{
		done = GetWorkerLine(cstate);
		if (done && cstate->line_buf.len == 0)
			return false;
	}
	else
	{
		/* on input just throw the header line away */
		if (cstate->cur_lineno == 0 && cstate->header_line)
		{
			cstate->cur_lineno++;
			if (CopyReadLine(cstate))
				return false;		/* done */
		}

		cstate->cur_lineno++;

		/* Actually read the line into memory here */
		done = CopyReadLine(cstate);

		/*
		 * EOF at start of line means we're done.  If we see EOF after some
		 * characters, we act as though it was newline followed by EOF, ie,
		 * process the line and then exit loop on next iteration.
		 */
		if (done && cstate->line_buf.len == 0)
			return false;
	}

	/* Parse the line into de-escaped field values */
	if (cstate->csv_mode)
		fldct = CopyReadAttributesCSV(cstate);
	else
		fldct = CopyReadAttributesText(cstate);

	*fields = cstate->raw_fields;
	*nfields = fldct;
	return true;
}

/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 * Oid of the tuple is returned with 'tupleOid' separately.
 */
bool
NextCopyFrom(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->binary)
	{
		char	  **field_strings;
		ListCell   *cur;
		int			fldct;
		int			fieldno;
		char	   *string;

		/* read raw fields in the next line */
		if (!NextCopyFromRawFields(cstate, &field_strings, &fldct))
			return false;

		/* check for overflowing fields */
		if (attr_count > 0 && fldct > attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));

		fieldno = 0;

		/* Loop to read the user attributes on the line. */
		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			if (fieldno >= fldct)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for column \"%s\"",
								NameStr(att->attname))));
			string = field_strings[fieldno++];

			if (cstate->convert_select_flags &&
				!cstate->convert_select_flags[m])
			{
				/* ignore input field, leaving column as NULL */
				continue;
			}

			if (cstate->csv_mode)
			{
				if (string == NULL &&
					cstate->force_notnull_flags[m])
				{
					/*
					 * FORCE_NOT_NULL option is set and column is NULL -
					 * convert it to the NULL string.
					 */
					string = cstate->null_print;
				}
				else if (string != NULL && cstate->force_null_flags[m]
						 && strcmp(string, cstate->null_print) == 0)
				{
					/*
					 * FORCE_NULL option is set and column matches the NULL
					 * string. It must have been quoted, or otherwise the
					 * string would already have been set to NULL. Convert it
					 * to NULL as specified.
					 */
					string = NULL;
				}
			}

			cstate->cur_attname = NameStr(att->attname);
			cstate->cur_attval = string;
			values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  att->atttypmod);
			if (string != NULL)
				nulls[m] = false;
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		Assert(fieldno == attr_count);
	}
	else
	{
		/* binary */
		int16		fld_count;
		ListCell   *cur;

		cstate->cur_lineno++;

		if (!CopyGetInt16(cstate, &fld_count))
		{
			/* EOF detected (end of file, or protocol-level EOF) */
			return false;
		}

		if (fld_count == -1)
		{
			/*
			 * Received EOF marker.  In a V3-protocol copy, wait for the
			 * protocol-level EOF, and complain if it doesn't come
			 * immediately.  This ensures that we correctly handle CopyFail,
			 * if client chooses to send that now.
			 *
			 * Note that we MUST NOT try to read more data in an old-protocol
			 * copy, since there is no protocol-level EOF marker then.  We
			 * could go either way for copy from file, but choose to throw
			 * error if there's data after the EOF marker, for consistency
			 * with the new-protocol case.
			 */
			char		dummy;

			if (cstate->copy_dest != COPY_OLD_FE &&
				CopyReadBinaryData(cstate, &dummy, 1) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("received copy data after EOF marker")));
			return false;
		}

		if (fld_count != attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("row field count is %d, expected %d",
							(int) fld_count, attr_count)));

		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			cstate->cur_attname = NameStr(att->attname);
			values[m] = CopyReadBinaryAttribute(cstate,
												&in_functions[m],
												typioparams[m],
												att->atttypmod,
												&nulls[m]);
			cstate->cur_attname = NULL;
		}
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.  Anything not processed here or above will
	 * remain NULL.
	 */
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]]);
	}

	return true;
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
void
EndCopyFrom(CopyState cstate)
{
	/* No COPY FROM related resources except memory. */

	EndCopy(cstate);
}

/*
 * Read the next input line and stash it in line_buf, with conversion to
 * server encoding.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.  The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
static bool
CopyReadLine(CopyState cstate)
{
	bool		result;

	resetStringInfo(&cstate->line_buf);
	cstate->line_buf_valid = true;

	/* Mark that encoding conversion hasn't occurred yet */
	cstate->line_buf_converted = false;

	/* Parse data and transfer into line_buf */
	result = CopyReadLineText(cstate);

	if (result)
	{
		/*
		 * Reached EOF.  In protocol version 3, we should ignore anything
		 * after \. up to the protocol end of copy data.  (XXX maybe better
		 * not to treat \. as special?)
		 */
		if (cstate->copy_dest == COPY_NEW_FE)
		{
			bool bIsFirst = true;
			do
			{
				if (!IsParallelCopy())
					cstate->raw_buf_index = cstate->raw_buf_len;
				else
				{
					if (cstate->raw_buf_index == RAW_BUF_SIZE)
					{
						/* Get a new block if it is the first time, From the
						 * subsequent time, reset the index and re-use the same
						 * block.
						 */
						if (bIsFirst)
						{
							ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
							uint32 block_pos = WaitGetFreeCopyBlock(pcshared_info);
							cstate->raw_buf = pcshared_info->data_blocks[block_pos].data;
							bIsFirst = false;
						}

						cstate->raw_buf_index = cstate->raw_buf_len = 0;
					}
				}

			} while (CopyLoadRawBuf(cstate));
		}
	}

	ConvertToServerEncoding(cstate);
	return result;
}

/*
 * ClearEOLFromCopiedData - Clear EOL from the copied data.
 */
static void
ClearEOLFromCopiedData(CopyState cstate, char *copy_line_data,
					   int copy_line_pos, int *copy_line_size)
{
	/*
	 * If we didn't hit EOF, then we must have transferred the EOL marker
	 * to line_buf along with the data.  Get rid of it.
	 */
   switch (cstate->eol_type)
   {
	   case EOL_NL:
		   Assert(*copy_line_size >= 1);
		   Assert(copy_line_data[copy_line_pos - 1] == '\n');
		   copy_line_data[copy_line_pos - 1] = '\0';
		   (*copy_line_size)--;
		   break;
	   case EOL_CR:
		   Assert(*copy_line_size >= 1);
		   Assert(copy_line_data[copy_line_pos - 1] == '\r');
		   copy_line_data[copy_line_pos - 1] = '\0';
		   (*copy_line_size)--;
		   break;
	   case EOL_CRNL:
		   Assert(*copy_line_size >= 2);
		   Assert(copy_line_data[copy_line_pos - 2] == '\r');
		   Assert(copy_line_data[copy_line_pos - 1] == '\n');
		   copy_line_data[copy_line_pos - 2] = '\0';
		   *copy_line_size -= 2;
		   break;
	   case EOL_UNKNOWN:
		   /* shouldn't get here */
		   Assert(false);
		   break;
   }
}

/*
 * ConvertToServerEncoding - convert contents to server encoding.
 */
static void
ConvertToServerEncoding(CopyState cstate)
{
	/* Done reading the line.  Convert it to server encoding. */
	if (cstate->need_transcoding && (!IsParallelCopy() || IsWorker()))
	{
		char	   *cvt;
		cvt = pg_any_to_server(cstate->line_buf.data,
							   cstate->line_buf.len,
							   cstate->file_encoding);
		if (cvt != cstate->line_buf.data)
		{
			/* transfer converted data back to line_buf */
			resetStringInfo(&cstate->line_buf);
			appendBinaryStringInfo(&cstate->line_buf, cvt, strlen(cvt));
			pfree(cvt);
		}
	}
	/* Now it's safe to use the buffer in error messages */
	cstate->line_buf_converted = true;
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
static bool
CopyReadLineText(CopyState cstate)
{
	char	   *copy_raw_buf;
	int			raw_buf_ptr;
	int			copy_buf_len;
	bool		need_data = false;
	bool		hit_eof = false;
	bool		result = false;
	char		mblen_str[2];

	/* CSV variables */
	bool		first_char_in_line = true;
	bool		in_quote = false,
				last_was_esc = false;
	char		quotec = '\0';
	char		escapec = '\0';

	/* For parallel copy */
	int			line_size = 0;
	int			line_pos = 0;

	cstate->eol_type = EOL_UNKNOWN;
	if (cstate->csv_mode)
	{
		quotec = cstate->quote[0];
		escapec = cstate->escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	mblen_str[1] = '\0';

	/*
	 * The objective of this loop is to transfer the entire next input line
	 * into line_buf.  Hence, we only care for detecting newlines (\r and/or
	 * \n) and the end-of-copy marker (\.).
	 *
	 * In CSV mode, \r and \n inside a quoted field are just part of the data
	 * value and are put in line_buf.  We keep just enough state to know if we
	 * are currently in a quoted field or not.
	 *
	 * These four characters, and the CSV escape and quote characters, are
	 * assumed the same in frontend and backend encodings.
	 *
	 * For speed, we try to move data from raw_buf to line_buf in chunks
	 * rather than one character at a time.  raw_buf_ptr points to the next
	 * character to examine; any characters from raw_buf_index to raw_buf_ptr
	 * have been determined to be part of the line, but not yet transferred to
	 * line_buf.
	 *
	 * For a little extra speed within the loop, we copy raw_buf and
	 * raw_buf_len into local variables.
	 */
	copy_raw_buf = cstate->raw_buf;
	raw_buf_ptr = cstate->raw_buf_index;
	copy_buf_len = cstate->raw_buf_len;

	for (;;)
	{
		int			prev_raw_ptr;
		char		c;

		/*
		 * Load more data if needed.  Ideally we would just force four bytes
		 * of read-ahead and avoid the many calls to
		 * IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(), but the COPY_OLD_FE protocol
		 * does not allow us to read too far ahead or we might read into the
		 * next data, so we read-ahead only as far we know we can.  One
		 * optimization would be to read-ahead four byte here if
		 * cstate->copy_dest != COPY_OLD_FE, but it hardly seems worth it,
		 * considering the size of the buffer.
		 */
		if (raw_buf_ptr >= copy_buf_len || need_data)
		{
			REFILL_LINEBUF;
			SetRawBufForLoad(cstate, line_size, copy_buf_len, raw_buf_ptr,
							 &copy_raw_buf);

			/*
			 * Try to read some more data.  This will certainly reset
			 * raw_buf_index to zero, and raw_buf_ptr must go with it.
			 */
			if (!CopyLoadRawBuf(cstate))
				hit_eof = true;
			raw_buf_ptr = 0;
			copy_buf_len = cstate->raw_buf_len;

			/*
			 * If we are completely out of data, break out of the loop,
			 * reporting EOF.
			 */
			if (copy_buf_len <= 0)
			{
				result = true;
				break;
			}
			need_data = false;
		}

		/* OK to fetch a character */
		prev_raw_ptr = raw_buf_ptr;
		c = copy_raw_buf[raw_buf_ptr++];

		if (cstate->csv_mode)
		{
			/*
			 * If character is '\\' or '\r', we may need to look ahead below.
			 * Force fetch of the next character if we don't already have it.
			 * We need to do this before changing CSV state, in case one of
			 * these characters is also the quote or escape character.
			 *
			 * Note: old-protocol does not like forced prefetch, but it's OK
			 * here since we cannot validly be at EOF.
			 */
			if (c == '\\' || c == '\r')
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			}

			/*
			 * Dealing with quotes and escapes here is mildly tricky. If the
			 * quote char is also the escape char, there's no problem - we
			 * just use the char as a toggle. If they are different, we need
			 * to ensure that we only take account of an escape inside a
			 * quoted field and immediately preceding a quote char, and not
			 * the second in an escape-escape sequence.
			 */
			if (in_quote && c == escapec)
				last_was_esc = !last_was_esc;
			if (c == quotec && !last_was_esc)
				in_quote = !in_quote;
			if (c != escapec)
				last_was_esc = false;

			/*
			 * Updating the line count for embedded CR and/or LF chars is
			 * necessarily a little fragile - this test is probably about the
			 * best we can do.  (XXX it's arguable whether we should do this
			 * at all --- is cur_lineno a physical or logical count?)
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		/* Process \r */
		if (c == '\r' && (!cstate->csv_mode || !in_quote))
		{
			/* Check for \r\n on first line, _and_ handle \r\n. */
			if (cstate->eol_type == EOL_UNKNOWN ||
				cstate->eol_type == EOL_CRNL)
			{
				/*
				 * If need more data, go back to loop top to load it.
				 *
				 * Note that if we are at EOF, c will wind up as '\0' because
				 * of the guaranteed pad of raw_buf.
				 */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

				/* get next char */
				c = copy_raw_buf[raw_buf_ptr];

				if (c == '\n')
				{
					raw_buf_ptr++;	/* eat newline */
					cstate->eol_type = EOL_CRNL;	/* in case not set yet */
				}
				else
				{
					/* found \r, but no \n */
					if (cstate->eol_type == EOL_CRNL)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 !cstate->csv_mode ?
								 errmsg("literal carriage return found in data") :
								 errmsg("unquoted carriage return found in data"),
								 !cstate->csv_mode ?
								 errhint("Use \"\\r\" to represent carriage return.") :
								 errhint("Use quoted CSV field to represent carriage return.")));

					/*
					 * if we got here, it is the first line and we didn't find
					 * \n, so don't consume the peeked character
					 */
					cstate->eol_type = EOL_CR;
				}
			}
			else if (cstate->eol_type == EOL_NL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ?
						 errmsg("literal carriage return found in data") :
						 errmsg("unquoted carriage return found in data"),
						 !cstate->csv_mode ?
						 errhint("Use \"\\r\" to represent carriage return.") :
						 errhint("Use quoted CSV field to represent carriage return.")));
			/* If reach here, we have found the line terminator */
			break;
		}

		/* Process \n */
		if (c == '\n' && (!cstate->csv_mode || !in_quote))
		{
			if (cstate->eol_type == EOL_CR || cstate->eol_type == EOL_CRNL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ?
						 errmsg("literal newline found in data") :
						 errmsg("unquoted newline found in data"),
						 !cstate->csv_mode ?
						 errhint("Use \"\\n\" to represent newline.") :
						 errhint("Use quoted CSV field to represent newline.")));
			cstate->eol_type = EOL_NL;	/* in case not set yet */
			/* If reach here, we have found the line terminator */
			break;
		}

		/*
		 * In CSV mode, we only recognize \. alone on a line.  This is because
		 * \. is a valid CSV data value.
		 */
		if (c == '\\' && (!cstate->csv_mode || first_char_in_line))
		{
			char		c2;

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			IF_NEED_REFILL_AND_EOF_BREAK(0);

			/* -----
			 * get next character
			 * Note: we do not change c so if it isn't \., we can fall
			 * through and continue processing for file encoding.
			 * -----
			 */
			c2 = copy_raw_buf[raw_buf_ptr];

			if (c2 == '.')
			{
				raw_buf_ptr++;	/* consume the '.' */

				/*
				 * Note: if we loop back for more data here, it does not
				 * matter that the CSV state change checks are re-executed; we
				 * will come back here with no important state changed.
				 */
				if (cstate->eol_type == EOL_CRNL)
				{
					/* Get the next character */
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					/* if hit_eof, c2 will become '\0' */
					c2 = copy_raw_buf[raw_buf_ptr++];

					if (c2 == '\n')
					{
						if (!cstate->csv_mode)
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker does not match previous newline style")));
						else
							NO_END_OF_COPY_GOTO;
					}
					else if (c2 != '\r')
					{
						if (!cstate->csv_mode)
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker corrupt")));
						else
							NO_END_OF_COPY_GOTO;
					}
				}

				/* Get the next character */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
				/* if hit_eof, c2 will become '\0' */
				c2 = copy_raw_buf[raw_buf_ptr++];

				if (c2 != '\r' && c2 != '\n')
				{
					if (!cstate->csv_mode)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker corrupt")));
					else
						NO_END_OF_COPY_GOTO;
				}

				if ((cstate->eol_type == EOL_NL && c2 != '\n') ||
					(cstate->eol_type == EOL_CRNL && c2 != '\n') ||
					(cstate->eol_type == EOL_CR && c2 != '\r'))
				{
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker does not match previous newline style")));
				}

				/*
				 * Transfer only the data before the \. into line_buf, then
				 * discard the data and the \. sequence.
				 */
				if (prev_raw_ptr > cstate->raw_buf_index)
				{
					if (!IsParallelCopy())
						appendBinaryStringInfo(&cstate->line_buf,
										   cstate->raw_buf + cstate->raw_buf_index,
										   prev_raw_ptr - cstate->raw_buf_index);
					else
						line_size += prev_raw_ptr - cstate->raw_buf_index;
				}

				cstate->raw_buf_index = raw_buf_ptr;
				result = true;	/* report EOF */
				break;
			}
			else if (!cstate->csv_mode)

				/*
				 * If we are here, it means we found a backslash followed by
				 * something other than a period.  In non-CSV mode, anything
				 * after a backslash is special, so we skip over that second
				 * character too.  If we didn't do that \\. would be
				 * considered an eof-of copy, while in non-CSV mode it is a
				 * literal backslash followed by a period.  In CSV mode,
				 * backslashes are not special, so we want to process the
				 * character after the backslash just like a normal character,
				 * so we don't increment in those cases.
				 */
				raw_buf_ptr++;
		}

		/*
		 * This label is for CSV cases where \. appears at the start of a
		 * line, but there is more text after it, meaning it was a data value.
		 * We are more strict for \. in CSV mode because \. could be a data
		 * value, while in non-CSV mode, \. cannot be a data value.
		 */
not_end_of_copy:

		/*
		 * Process all bytes of a multi-byte character as a group.
		 *
		 * We only support multi-byte sequences where the first byte has the
		 * high-bit set, so as an optimization we can avoid this block
		 * entirely if it is not set.
		 */
		if (cstate->encoding_embeds_ascii && IS_HIGHBIT_SET(c))
		{
			int			mblen;

			/*
			 * It is enough to look at the first byte in all our encodings, to
			 * get the length.  (GB18030 is a bit special, but still works for
			 * our purposes; see comment in pg_gb18030_mblen())
			 */
			mblen_str[0] = c;
			mblen = pg_encoding_mblen(cstate->file_encoding, mblen_str);

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(mblen - 1);
			IF_NEED_REFILL_AND_EOF_BREAK(mblen - 1);
			raw_buf_ptr += mblen - 1;
		}

		/*
		 * Skip the header line. Update the line here, this cannot be done at
		 * the beginning, as there is a possibility that file contains empty
		 * lines.
		 */
		if (IsParallelCopy() && first_char_in_line && !IsHeaderLine())
		{
			ParallelCopyShmInfo	*pcshared_info = cstate->pcdata->pcshared_info;
			ParallelCopyLineBoundary *lineInfo;
			uint32			 line_first_block = pcshared_info->cur_block_pos;
			line_pos = UpdateBlockInLineInfo(cstate,
											   line_first_block,
											   cstate->raw_buf_index, -1,
											   LINE_LEADER_POPULATING);
			lineInfo = &pcshared_info->line_boundaries.ring[line_pos];
			elog(DEBUG1, "[Leader] Adding - block:%d, offset:%d, line position:%d",
						 line_first_block,	lineInfo->start_offset, line_pos);
		}

		first_char_in_line = false;
	}							/* end of outer loop */

	/*
	 * Transfer any still-uncopied data to line_buf.
	 */
	REFILL_LINEBUF;
	CLEAR_EOL_LINE();
	EndLineParallelCopy(cstate, line_pos, line_size, raw_buf_ptr);

	return result;
}

/*
 *	Return decimal value for a hexadecimal digit
 */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf.  We use attribute_buf to hold the result
 * strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
static int
CopyReadAttributesText(CopyState cstate)
{
	char		delimc = cstate->delim[0];
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;
		bool		saw_non_ascii = false;

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char		c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (c == delimc)
			{
				found_delim = true;
				break;
			}
			if (c == '\\')
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							/* handle \013 */
							int			val;

							val = OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
									if (cur_ptr < line_end_ptr)
									{
										c = *cur_ptr;
										if (ISOCTAL(c))
										{
											cur_ptr++;
											val = (val << 3) + OCTVALUE(c);
										}
									}
								}
							}
							c = val & 0377;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char		hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int			val = GetDecimalFromHex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;

						/*
						 * in all other cases, take the char after '\'
						 * literally
						 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char	   *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
static int
CopyReadAttributesCSV(CopyState cstate)
{
	char		delimc = cstate->delim[0];
	char		quotec = cstate->quote[0];
	char		escapec = cstate->escape[0];
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		bool		saw_quote = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field,
		 *
		 * The loop starts in "not quote" mode and then toggles between that
		 * and "in quote" mode. The loop exits normally if it is in "not
		 * quote" mode and a delimiter or line end is seen.
		 */
		for (;;)
		{
			char		c;

			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (c == delimc)
				{
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}

			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unterminated CSV quoted field")));

				c = *cur_ptr++;

				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char		nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}

				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;

				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
endfield:

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}


/*
 * Read a binary attribute
 */
static Datum
CopyReadBinaryAttribute(CopyState cstate, FmgrInfo *flinfo,
						Oid typioparam, int32 typmod,
						bool *isnull)
{
	int32		fld_size;
	Datum		result;

	if (!CopyGetInt32(cstate, &fld_size))
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (fld_size == -1)
	{
		*isnull = true;
		return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
	}
	if (fld_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid field size")));

	/* reset attribute_buf to empty, and load raw data in it */
	resetStringInfo(&cstate->attribute_buf);

	enlargeStringInfo(&cstate->attribute_buf, fld_size);
	if (CopyReadBinaryData(cstate, cstate->attribute_buf.data,
						   fld_size) != fld_size)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	cstate->attribute_buf.len = fld_size;
	cstate->attribute_buf.data[fld_size] = '\0';

	/* Call the column type's binary input converter */
	result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf,
								 typioparam, typmod);

	/* Trouble if it didn't eat the whole buffer */
	if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("incorrect binary data format")));

	*isnull = false;
	return result;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

static void
CopyAttributeOutText(CopyState cstate, char *string)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				start = ptr++;	/* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, '\\');
				start = ptr++;	/* we include char in next run */
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(CopyState cstate, char *string,
					bool use_quote, bool single_attr)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		quotec = cstate->quote[0];
	char		escapec = cstate->escape[0];

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			char	   *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		CopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr;	/* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
		DUMPSOFAR();

		CopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		CopySendString(cstate, ptr);
	}
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 */
static List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			if (TupleDescAttr(tupDesc, i)->attgenerated)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					if (att->attgenerated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
								 errmsg("column \"%s\" is a generated column",
										name),
								 errdetail("Generated columns cannot be used in COPY.")));
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}


/*
 * copy_dest_startup --- executor startup
 */
static void
copy_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	/* no-op */
}

/*
 * copy_dest_receive --- receive one tuple
 */
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyState	cstate = myState->cstate;

	/* Send the data */
	CopyOneRowTo(cstate, slot);
	myState->processed++;

	return true;
}

/*
 * copy_dest_shutdown --- executor end
 */
static void
copy_dest_shutdown(DestReceiver *self)
{
	/* no-op */
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void
copy_dest_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver *
CreateCopyDestReceiver(void)
{
	DR_copy    *self = (DR_copy *) palloc(sizeof(DR_copy));

	self->pub.receiveSlot = copy_dest_receive;
	self->pub.rStartup = copy_dest_startup;
	self->pub.rShutdown = copy_dest_shutdown;
	self->pub.rDestroy = copy_dest_destroy;
	self->pub.mydest = DestCopyOut;

	self->cstate = NULL;		/* will be set later */
	self->processed = 0;

	return (DestReceiver *) self;
}
