/*-------------------------------------------------------------------------
 *
 * copyapi.h
 *	  API for COPY TO/FROM handlers
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYAPI_H
#define COPYAPI_H

#include "commands/trigger.h"
#include "executor/execdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

/*
 * Represents whether a header line should be present, and whether it must
 * match the actual names (which implies "true").
 */
typedef enum CopyHeaderChoice
{
	COPY_HEADER_FALSE = 0,
	COPY_HEADER_TRUE,
	COPY_HEADER_MATCH,
} CopyHeaderChoice;

/*
 * Represents where to save input processing errors.  More values to be added
 * in the future.
 */
typedef enum CopyOnErrorChoice
{
	COPY_ON_ERROR_STOP = 0,		/* immediately throw errors, default */
	COPY_ON_ERROR_IGNORE,		/* ignore errors */
} CopyOnErrorChoice;

/*
 * Represents verbosity of logged messages by COPY command.
 */
typedef enum CopyLogVerbosityChoice
{
	COPY_LOG_VERBOSITY_DEFAULT = 0, /* logs no additional messages, default */
	COPY_LOG_VERBOSITY_VERBOSE, /* logs additional messages */
} CopyLogVerbosityChoice;

/*
 * A struct to hold COPY options, in a parsed form. All of these are related
 * to formatting, except for 'freeze', which doesn't really belong here, but
 * it's expedient to parse it along with all the other options.
 */
typedef struct CopyFormatOptions
{
	/* parameters from the COPY command */
	int			file_encoding;	/* file or remote side's character encoding,
								 * -1 if not specified */
	bool		binary;			/* binary format? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	CopyHeaderChoice header_line;	/* header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *default_print;	/* DEFAULT marker string */
	int			default_print_len;	/* length of same */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool		force_notnull_all;	/* FORCE_NOT_NULL *? */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool		force_null_all; /* FORCE_NULL *? */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	CopyOnErrorChoice on_error; /* what to do when error happened */
	CopyLogVerbosityChoice log_verbosity;	/* verbosity of logged messages */
	List	   *convert_select; /* list of column names (can be NIL) */
	Node	   *routine;		/* CopyToRoutine or CopyFromRoutine (can be
								 * NULL) */
} CopyFormatOptions;

typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread);

typedef struct CopyFromStateData *CopyFromState;

/*
 * API structure for a COPY FROM format implementation.  Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
typedef struct CopyFromRoutine
{
	NodeTag		type;

	/*
	 * Called when COPY FROM is started to set up the input functions
	 * associated with the relation's attributes writing to.  `finfo` can be
	 * optionally filled to provide the catalog information of the input
	 * function.  `typioparam` can be optionally filled to define the OID of
	 * the type to pass to the input function.  `atttypid` is the OID of data
	 * type used by the relation's attribute.
	 */
	void		(*CopyFromInFunc) (CopyFromState cstate, Oid atttypid,
								   FmgrInfo *finfo, Oid *typioparam);

	/*
	 * Called when COPY FROM is started.
	 *
	 * `tupDesc` is the tuple descriptor of the relation where the data needs
	 * to be copied.  This can be used for any initialization steps required
	 * by a format.
	 */
	void		(*CopyFromStart) (CopyFromState cstate, TupleDesc tupDesc);

	/*
	 * Copy one row to a set of `values` and `nulls` of size tupDesc->natts.
	 *
	 * 'econtext' is used to evaluate default expression for each column that
	 * is either not read from the file or is using the DEFAULT option of COPY
	 * FROM.  It is NULL if no default values are used.
	 *
	 * Returns false if there are no more tuples to copy.
	 */
	bool		(*CopyFromOneRow) (CopyFromState cstate, ExprContext *econtext,
								   Datum *values, bool *nulls);

	/* Called when COPY FROM has ended. */
	void		(*CopyFromEnd) (CopyFromState cstate);
} CopyFromRoutine;

/*
 * Represents the different source cases we need to worry about at
 * the bottom level
 */
typedef enum CopySource
{
	COPY_SOURCE_FILE,			/* from file (or a piped program) */
	COPY_SOURCE_FRONTEND,		/* from frontend */
	COPY_SOURCE_CALLBACK,		/* from callback function */
} CopySource;

/*
 * Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL,
} EolType;

/*
 * Represents the insert method to be used during COPY FROM.
 */
typedef enum CopyInsertMethod
{
	CIM_SINGLE,					/* use table_tuple_insert or ExecForeignInsert */
	CIM_MULTI,					/* always use table_multi_insert or
								 * ExecForeignBatchInsert */
	CIM_MULTI_CONDITIONAL,		/* use table_multi_insert or
								 * ExecForeignBatchInsert only if valid */
} CopyInsertMethod;

/*
 * This struct contains all the state variables used throughout a COPY FROM
 * operation.
 */
typedef struct CopyFromStateData
{
	/* format routine */
	const CopyFromRoutine *routine;

	/* low-level state data */
	CopySource	copy_src;		/* type of copy source */
	FILE	   *copy_file;		/* used if copy_src == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used if copy_src == COPY_FRONTEND */

	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	Oid			conversion_proc;	/* encoding conversion function */

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
	bool		relname_only;	/* don't output line number, att, etc. */

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	AttrNumber	num_defaults;	/* count of att that are missing and have
								 * default value */
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	ErrorSaveContext *escontext;	/* soft error trapper during in_functions
									 * execution */
	uint64		num_errors;		/* total number of rows which contained soft
								 * errors */
	int		   *defmap;			/* array of default att numbers related to
								 * missing att */
	ExprState **defexprs;		/* array of default att expressions for all
								 * att */
	bool	   *defaults;		/* if DEFAULT marker was found for
								 * corresponding att */
	bool		volatile_defexprs;	/* is any of defexprs volatile? */
	List	   *range_table;	/* single element list of RangeTblEntry */
	List	   *rteperminfos;	/* single element list of RTEPermissionInfo */
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
	 * input cycle is first to read the whole line into line_buf, and then
	 * extract the individual attribute fields into attribute_buf.  line_buf
	 * is preserved unmodified so that we can display it in error messages if
	 * appropriate.  (In binary mode, line_buf is not used.)
	 */
	StringInfoData line_buf;
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * input_buf holds input data, already converted to database encoding.
	 *
	 * In text mode, CopyReadLine parses this data sufficiently to locate line
	 * boundaries, then transfers the data to line_buf. We guarantee that
	 * there is a \0 at input_buf[input_buf_len] at all times.  (In binary
	 * mode, input_buf is not used.)
	 *
	 * If encoding conversion is not required, input_buf is not a separate
	 * buffer but points directly to raw_buf.  In that case, input_buf_len
	 * tracks the number of bytes that have been verified as valid in the
	 * database encoding, and raw_buf_len is the total number of bytes stored
	 * in the buffer.
	 */
#define INPUT_BUF_SIZE 65536	/* we palloc INPUT_BUF_SIZE+1 bytes */
	char	   *input_buf;
	int			input_buf_index;	/* next byte to process */
	int			input_buf_len;	/* total # of bytes stored */
	bool		input_reached_eof;	/* true if we reached EOF */
	bool		input_reached_error;	/* true if a conversion error happened */
	/* Shorthand for number of unconsumed bytes available in input_buf */
#define INPUT_BUF_BYTES(cstate) ((cstate)->input_buf_len - (cstate)->input_buf_index)

	/*
	 * raw_buf holds raw input data read from the data source (file or client
	 * connection), not yet converted to the database encoding.  Like with
	 * 'input_buf', we guarantee that there is a \0 at raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
	bool		raw_reached_eof;	/* true if we reached EOF */

	/* Shorthand for number of unconsumed bytes available in raw_buf */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)

	uint64		bytes_processed;	/* number of bytes processed so far */

	/* For custom format implementation */
	void	   *opaque;			/* private space */
} CopyFromStateData;

extern int	CopyFromStateRead(CopyFromState cstate, char *dest, int nbytes);


typedef struct CopyToStateData *CopyToState;

/*
 * API structure for a COPY TO format implementation.   Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
typedef struct CopyToRoutine
{
	NodeTag		type;

	/*
	 * Called when COPY TO is started to set up the output functions
	 * associated with the relation's attributes reading from.  `finfo` can be
	 * optionally filled to provide the catalog information of the output
	 * function.  `atttypid` is the OID of data type used by the relation's
	 * attribute.
	 */
	void		(*CopyToOutFunc) (CopyToState cstate, Oid atttypid,
								  FmgrInfo *finfo);

	/*
	 * Called when COPY TO is started.
	 *
	 * `tupDesc` is the tuple descriptor of the relation from where the data
	 * is read.
	 */
	void		(*CopyToStart) (CopyToState cstate, TupleDesc tupDesc);

	/*
	 * Copy one row for COPY TO.
	 *
	 * `slot` is the tuple slot where the data is emitted.
	 */
	void		(*CopyToOneRow) (CopyToState cstate, TupleTableSlot *slot);

	/* Called when COPY TO has ended */
	void		(*CopyToEnd) (CopyToState cstate);
} CopyToRoutine;

/*
 * Represents the different dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_DEST_FILE,				/* to file (or a piped program) */
	COPY_DEST_FRONTEND,			/* to frontend */
	COPY_DEST_CALLBACK,			/* to callback function */
} CopyDest;

typedef void (*copy_data_dest_cb) (void *data, int len);

/*
 * This struct contains all the state variables used throughout a COPY TO
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
typedef struct CopyToStateData
{
	/* format routine */
	const CopyToRoutine *routine;

	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO */

	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_dest_cb data_dest_cb; /* function for writing data */

	CopyFormatOptions opts;
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/*
	 * Working state
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */
	uint64		bytes_processed;	/* number of bytes processed so far */

	/* For custom format implementation */
	void	   *opaque;			/* private space */
} CopyToStateData;

extern void CopyToStateFlush(CopyToState cstate);

#endif							/* COPYAPI_H */
