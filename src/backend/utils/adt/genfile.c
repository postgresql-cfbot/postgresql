/*-------------------------------------------------------------------------
 *
 * genfile.c
 *		Functions for direct access to files
 *
 *
 * Copyright (c) 2004-2021, PostgreSQL Global Development Group
 *
 * Author: Andreas Pflug <pgadmin@pse-consulting.de>
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/genfile.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "access/htup_details.h"
#include "access/xlog_internal.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

static char get_file_type(mode_t mode, const char *path);
static void values_from_stat(struct stat *fst, const char *path, Datum *values,
		bool *nulls);
static Datum pg_ls_dir_files(FunctionCallInfo fcinfo, const char *dir, int flags);
void pg_ls_dir_files_internal(const char *dirname, DIR *dirdesc,
		Tuplestorestate *tupstore, TupleDesc tupdesc, int flags);

#define	LS_DIR_TYPE					(1<<0) /* Show column: type */
#define	LS_DIR_METADATA				(1<<1) /* Show columns: mtime, size */
#define	LS_DIR_MISSING_OK			(1<<2) /* Ignore ENOENT if the toplevel dir is missing */
#define	LS_DIR_SKIP_DOT_DIRS		(1<<3) /* Do not show . or .. */
#define	LS_DIR_SKIP_HIDDEN			(1<<4) /* Do not show anything begining with . */
#define	LS_DIR_SKIP_DIRS			(1<<5) /* Do not show directories */
#define	LS_DIR_SKIP_SPECIAL			(1<<6) /* Do not show special file types */
#define	LS_DIR_RECURSE				(1<<7) /* Recurse into subdirs */

/* Shortcut for common behavior */
#define LS_DIR_COMMON				(LS_DIR_SKIP_HIDDEN | LS_DIR_METADATA)

/*
 * Convert a "text" filename argument to C string, and check it's allowable.
 *
 * Filename may be absolute or relative to the DataDir, but we only allow
 * absolute paths that match DataDir or Log_directory.
 *
 * This does a privilege check against the 'pg_read_server_files' role, so
 * this function is really only appropriate for callers who are only checking
 * 'read' access.  Do not use this function if you are looking for a check
 * for 'write' or 'program' access without updating it to access the type
 * of check as an argument and checking the appropriate role membership.
 */
static char *
convert_and_check_filename(text *arg)
{
	char	   *filename;

	filename = text_to_cstring(arg);
	canonicalize_path(filename);	/* filename can change length here */

	/*
	 * Members of the 'pg_read_server_files' role are allowed to access any
	 * files on the server as the PG user, so no need to do any further checks
	 * here.
	 */
	if (is_member_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
		return filename;

	/*
	 * User isn't a member of the pg_read_server_files role, so check if it's
	 * allowable
	 */
	if (is_absolute_path(filename))
	{
		/* Disallow '/a/b/data/..' */
		if (path_contains_parent_reference(filename))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("reference to parent directory (\"..\") not allowed")));

		/*
		 * Allow absolute paths if within DataDir or Log_directory, even
		 * though Log_directory might be outside DataDir.
		 */
		if (!path_is_prefix_of_path(DataDir, filename) &&
			(!is_absolute_path(Log_directory) ||
			 !path_is_prefix_of_path(Log_directory, filename)))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("absolute path not allowed")));
	}
	else if (!path_is_relative_and_below_cwd(filename))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("path must be in or below the current directory")));

	return filename;
}


/*
 * Read a section of a file, returning it as bytea
 *
 * Caller is responsible for all permissions checking.
 *
 * We read the whole of the file when bytes_to_read is negative.
 */
static bytea *
read_binary_file(const char *filename, int64 seek_offset, int64 bytes_to_read,
				 bool missing_ok)
{
	bytea	   *buf;
	size_t		nbytes = 0;
	FILE	   *file;

	/* clamp request size to what we can actually deliver */
	if (bytes_to_read > (int64) (MaxAllocSize - VARHDRSZ))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("requested length too large")));

	if ((file = AllocateFile(filename, PG_BINARY_R)) == NULL)
	{
		if (missing_ok && errno == ENOENT)
			return NULL;
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" for reading: %m",
							filename)));
	}

	if (fseeko(file, (off_t) seek_offset,
			   (seek_offset >= 0) ? SEEK_SET : SEEK_END) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek in file \"%s\": %m", filename)));

	if (bytes_to_read >= 0)
	{
		/* If passed explicit read size just do it */
		buf = (bytea *) palloc((Size) bytes_to_read + VARHDRSZ);

		nbytes = fread(VARDATA(buf), 1, (size_t) bytes_to_read, file);
	}
	else
	{
		/* Negative read size, read rest of file */
		StringInfoData sbuf;

		initStringInfo(&sbuf);
		/* Leave room in the buffer for the varlena length word */
		sbuf.len += VARHDRSZ;
		Assert(sbuf.len < sbuf.maxlen);

		while (!(feof(file) || ferror(file)))
		{
			size_t		rbytes;

			/* Minimum amount to read at a time */
#define MIN_READ_SIZE 4096

			/*
			 * If not at end of file, and sbuf.len is equal to MaxAllocSize -
			 * 1, then either the file is too large, or there is nothing left
			 * to read. Attempt to read one more byte to see if the end of
			 * file has been reached. If not, the file is too large; we'd
			 * rather give the error message for that ourselves.
			 */
			if (sbuf.len == MaxAllocSize - 1)
			{
				char		rbuf[1];

				if (fread(rbuf, 1, 1, file) != 0 || !feof(file))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("file length too large")));
				else
					break;
			}

			/* OK, ensure that we can read at least MIN_READ_SIZE */
			enlargeStringInfo(&sbuf, MIN_READ_SIZE);

			/*
			 * stringinfo.c likes to allocate in powers of 2, so it's likely
			 * that much more space is available than we asked for.  Use all
			 * of it, rather than making more fread calls than necessary.
			 */
			rbytes = fread(sbuf.data + sbuf.len, 1,
						   (size_t) (sbuf.maxlen - sbuf.len - 1), file);
			sbuf.len += rbytes;
			nbytes += rbytes;
		}

		/* Now we can commandeer the stringinfo's buffer as the result */
		buf = (bytea *) sbuf.data;
	}

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));

	SET_VARSIZE(buf, nbytes + VARHDRSZ);

	FreeFile(file);

	return buf;
}

/*
 * Similar to read_binary_file, but we verify that the contents are valid
 * in the database encoding.
 */
static text *
read_text_file(const char *filename, int64 seek_offset, int64 bytes_to_read,
			   bool missing_ok)
{
	bytea	   *buf;

	buf = read_binary_file(filename, seek_offset, bytes_to_read, missing_ok);

	if (buf != NULL)
	{
		/* Make sure the input is valid */
		pg_verifymbstr(VARDATA(buf), VARSIZE(buf) - VARHDRSZ, false);

		/* OK, we can cast it to text safely */
		return (text *) buf;
	}
	else
		return NULL;
}

/*
 * Read a section of a file, returning it as text
 *
 * This function is kept to support adminpack 1.0.
 */
Datum
pg_read_file(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_PP(0);
	int64		seek_offset = 0;
	int64		bytes_to_read = -1;
	bool		missing_ok = false;
	char	   *filename;
	text	   *result;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to read files with adminpack 1.0"),
		/* translator: %s is a SQL function name */
				 errhint("Consider using %s, which is part of core, instead.",
						 "pg_read_file()")));

	/* handle optional arguments */
	if (PG_NARGS() >= 3)
	{
		seek_offset = PG_GETARG_INT64(1);
		bytes_to_read = PG_GETARG_INT64(2);

		if (bytes_to_read < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested length cannot be negative")));
	}
	if (PG_NARGS() >= 4)
		missing_ok = PG_GETARG_BOOL(3);

	filename = convert_and_check_filename(filename_t);

	result = read_text_file(filename, seek_offset, bytes_to_read, missing_ok);
	if (result)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

/*
 * Read a section of a file, returning it as text
 *
 * No superuser check done here- instead privileges are handled by the
 * GRANT system.
 */
Datum
pg_read_file_v2(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_PP(0);
	int64		seek_offset = 0;
	int64		bytes_to_read = -1;
	bool		missing_ok = false;
	char	   *filename;
	text	   *result;

	/* handle optional arguments */
	if (PG_NARGS() >= 3)
	{
		seek_offset = PG_GETARG_INT64(1);
		bytes_to_read = PG_GETARG_INT64(2);

		if (bytes_to_read < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested length cannot be negative")));
	}
	if (PG_NARGS() >= 4)
		missing_ok = PG_GETARG_BOOL(3);

	filename = convert_and_check_filename(filename_t);

	result = read_text_file(filename, seek_offset, bytes_to_read, missing_ok);
	if (result)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

/*
 * Read a section of a file, returning it as bytea
 */
Datum
pg_read_binary_file(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_PP(0);
	int64		seek_offset = 0;
	int64		bytes_to_read = -1;
	bool		missing_ok = false;
	char	   *filename;
	bytea	   *result;

	/* handle optional arguments */
	if (PG_NARGS() >= 3)
	{
		seek_offset = PG_GETARG_INT64(1);
		bytes_to_read = PG_GETARG_INT64(2);

		if (bytes_to_read < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested length cannot be negative")));
	}
	if (PG_NARGS() >= 4)
		missing_ok = PG_GETARG_BOOL(3);

	filename = convert_and_check_filename(filename_t);

	result = read_binary_file(filename, seek_offset,
							  bytes_to_read, missing_ok);
	if (result)
		PG_RETURN_BYTEA_P(result);
	else
		PG_RETURN_NULL();
}


/*
 * Wrapper functions for the 1 and 3 argument variants of pg_read_file_v2()
 * and pg_read_binary_file().
 *
 * These are necessary to pass the sanity check in opr_sanity, which checks
 * that all built-in functions that share the implementing C function take
 * the same number of arguments.
 */
Datum
pg_read_file_off_len(PG_FUNCTION_ARGS)
{
	return pg_read_file_v2(fcinfo);
}

Datum
pg_read_file_all(PG_FUNCTION_ARGS)
{
	return pg_read_file_v2(fcinfo);
}

Datum
pg_read_binary_file_off_len(PG_FUNCTION_ARGS)
{
	return pg_read_binary_file(fcinfo);
}

Datum
pg_read_binary_file_all(PG_FUNCTION_ARGS)
{
	return pg_read_binary_file(fcinfo);
}

/* Return a character indicating the type of file, or '?' if unknown type */
static char
get_file_type(mode_t mode, const char *path)
{
	if (S_ISREG(mode))
		return '-';

	if (S_ISDIR(mode))
		return 'd';
#ifndef WIN32
	if (S_ISLNK(mode))
		return 'l';
#else
	if (pgwin32_is_junction(path))
		return 'l';
#endif

#ifdef S_ISCHR
	if (S_ISCHR(mode))
		return 'c';
#endif
#ifdef S_ISBLK
	if (S_ISBLK(mode))
		return 'b';
#endif
#ifdef S_ISFIFO
	if (S_ISFIFO(mode))
		return 'p';
#endif
#ifdef S_ISSOCK
	if (S_ISSOCK(mode))
		return 's';
#endif

	return '?';
}

/*
 * Populate values and nulls from fst and path.
 * Used for pg_stat_file() and pg_ls_dir_files()
 * nulls is assumed to have been zerod.
 */
static void
values_from_stat(struct stat *fst, const char *path, Datum *values, bool *nulls)
{
	values[0] = Int64GetDatum((int64) fst->st_size);
	values[1] = TimestampTzGetDatum(time_t_to_timestamptz(fst->st_atime));
	values[2] = TimestampTzGetDatum(time_t_to_timestamptz(fst->st_mtime));
	/* Unix has file status change time, while Win32 has creation time */
#if !defined(WIN32) && !defined(__CYGWIN__)
	values[3] = TimestampTzGetDatum(time_t_to_timestamptz(fst->st_ctime));
	nulls[4] = true;
#else
	nulls[3] = true;
	values[4] = TimestampTzGetDatum(time_t_to_timestamptz(fst->st_ctime));
#endif
	values[5] = CharGetDatum(get_file_type(fst->st_mode, path));
}

/*
 * stat a file
 */
Datum
pg_stat_file(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_PP(0);
	char	   *filename;
	struct stat fst;
	Datum		values[7];
	bool		nulls[7];
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	bool		missing_ok = false;
	char		type;

	/* check the optional argument */
	if (PG_NARGS() == 2)
		missing_ok = PG_GETARG_BOOL(1);

	filename = convert_and_check_filename(filename_t);

	if (lstat(filename, &fst) < 0)
	{
		if (missing_ok && errno == ENOENT)
			PG_RETURN_NULL();
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", filename)));
	}

	/*
	 * This record type had better match the output parameters declared for me
	 * in pg_proc.h.
	 */
	tupdesc = CreateTemplateTupleDesc(7);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1,
					   "size", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2,
					   "access", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3,
					   "modification", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4,
					   "change", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5,
					   "creation", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6,
					   "isdir", BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7,
					   "type", CHAROID, -1, 0);
	BlessTupleDesc(tupdesc);

	memset(nulls, false, sizeof(nulls));
	values_from_stat(&fst, filename, values, nulls);

	/* For pg_stat_file, keep isdir column for backward compatibility */
	type = DatumGetChar(values[5]);
	values[5] = BoolGetDatum(type == 'd'); /* isdir */
	values[6] = CharGetDatum(type); /* file type */

	tuple = heap_form_tuple(tupdesc, values, nulls);

	pfree(filename);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * stat a file (1 argument version)
 *
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments
 */
Datum
pg_stat_file_1arg(PG_FUNCTION_ARGS)
{
	return pg_stat_file(fcinfo);
}

/*
 * List a directory (returns the filenames only)
 */
Datum
pg_ls_dir(PG_FUNCTION_ARGS)
{
	text	*filename_t = PG_GETARG_TEXT_PP(0);
	char	*filename = convert_and_check_filename(filename_t);
	return pg_ls_dir_files(fcinfo, filename, LS_DIR_SKIP_DOT_DIRS);
}

/*
 * List a directory (1 argument version)
 *
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments.
 */
Datum
pg_ls_dir_1arg(PG_FUNCTION_ARGS)
{
	text	*filename_t = PG_GETARG_TEXT_PP(0);
	char	*filename = convert_and_check_filename(filename_t);
	return pg_ls_dir_files(fcinfo, filename, LS_DIR_SKIP_DOT_DIRS);
}

/*
 * Generic function to return a directory listing of files (and optionally dirs).
 *
 * If the directory isn't there, silently return an empty set if MISSING_OK.
 * Other unreadable-directory cases throw an error.
 */
static Datum
pg_ls_dir_files(FunctionCallInfo fcinfo, const char *dir, int flags)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		randomAccess;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	DIR		   *dirdesc;
	MemoryContext oldcontext;
	TypeFuncClass	tuptype ;

	/* type depends on metadata */
	Assert(!(flags&LS_DIR_TYPE) || (flags&LS_DIR_METADATA));
	/* Unreasonable to show type and skip dirs XXX */
	Assert(!(flags&LS_DIR_TYPE) || !(flags&LS_DIR_SKIP_DIRS));

	/* check the optional arguments */
	if (PG_NARGS() > 1 &&
		!PG_ARGISNULL(1))
		{
			if (PG_GETARG_BOOL(1))
				flags |= LS_DIR_MISSING_OK;
			else
				flags &= ~LS_DIR_MISSING_OK;
		}

	if (PG_NARGS() > 2 &&
		!PG_ARGISNULL(2))
		{
			if (PG_GETARG_BOOL(2))
				flags &= ~LS_DIR_SKIP_DOT_DIRS;
			else
				flags |= LS_DIR_SKIP_DOT_DIRS;
		}

	if (PG_NARGS() > 3 &&
		!PG_ARGISNULL(3))
		{
			if (PG_GETARG_BOOL(3))
				flags |= LS_DIR_RECURSE;
			else
				flags &= ~LS_DIR_RECURSE;
		}

	if ((flags & LS_DIR_RECURSE) != 0 &&
			(flags & LS_DIR_SKIP_DOT_DIRS) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_RECURSION), // ??
				 errmsg("recursion requires skipping dot dirs")));


	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tuptype = get_call_result_type(fcinfo, NULL, &tupdesc);
	if (flags & LS_DIR_METADATA)
	{
		if (tuptype != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
	}
	else
	{
		/* pg_ls_dir returns a simple scalar */
		if (tuptype != TYPEFUNC_SCALAR)
			elog(ERROR, "return type must be a scalar type");
		tupdesc = CreateTemplateTupleDesc(1);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "column", TEXTOID, -1, 0);
	}

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Now walk the directory.  Note that we must do this within a single SRF
	 * call, not leave the directory open across multiple calls, since we
	 * can't count on the SRF being run to completion.
	 */
	dirdesc = AllocateDir(dir);
	if (!dirdesc)
	{
		/* Return empty tuplestore if appropriate */
		if (flags & LS_DIR_MISSING_OK && errno == ENOENT)
			return (Datum) 0;
		/* Otherwise, we can let ReadDir() throw the error */
	}

	pg_ls_dir_files_internal(dir, dirdesc, tupstore, tupdesc, flags);
	FreeDir(dirdesc);
	return (Datum) 0;
}

void pg_ls_dir_files_internal(const char *dirname, DIR *dirdesc,
		Tuplestorestate *tupstore, TupleDesc tupdesc, int flags)
{
	struct dirent *de;

	while ((de = ReadDir(dirdesc, dirname)) != NULL)
	{
		Datum		values[8];
		bool		nulls[8];
		char		path[MAXPGPATH * 2];
		struct stat attrib;

		/* Skip dot dirs? */
		if (flags & LS_DIR_SKIP_DOT_DIRS &&
			(strcmp(de->d_name, ".") == 0 ||
			 strcmp(de->d_name, "..") == 0))
			continue;

		/* Skip hidden files? */
		if (flags & LS_DIR_SKIP_HIDDEN &&
			de->d_name[0] == '.')
			continue;

		/* Get the file info */
		if (strcmp(dirname, ".") != 0)
			snprintf(path, sizeof(path), "%s/%s", dirname, de->d_name);
		else
			snprintf(path, sizeof(path), "%s", de->d_name);

		if (lstat(path, &attrib) < 0)
		{
			/* Ignore concurrently-deleted files, else complain */
			if (errno == ENOENT)
				continue;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", path)));
		}

		/* Skip dirs or special files? */
		if (S_ISDIR(attrib.st_mode))
		{
			if (flags & LS_DIR_SKIP_DIRS)
				continue;
		}
		else if (!S_ISREG(attrib.st_mode))
		{
			if (flags & LS_DIR_SKIP_SPECIAL)
				continue;
		}

		memset(nulls, false, sizeof(nulls));
		values[0] = CStringGetTextDatum(de->d_name);
		if ((flags & (LS_DIR_RECURSE|LS_DIR_METADATA)) != 0)
		{
			values_from_stat(&attrib, path, 1+values, 1+nulls);

			/*
			 * path is only really useful for recursion, but this function
			 * can't return different fields when recursing
			 * XXX: return dirname (which is nice since it's the original,
			 * unprocessed input to this recursion) or path (which is nice
			 * since it's a "cooked" value without leading/duplicate slashes)
			 */
			values[7] = CStringGetTextDatum(path);
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* Recurse? */
		if ((flags & LS_DIR_RECURSE) != 0 &&
			S_ISDIR(attrib.st_mode))
		{
			DIR *newdir = AllocateDir(path);
			/* Failure handled by ReadDir */
			pg_ls_dir_files_internal(path, newdir, tupstore, tupdesc, flags);
			Assert(newdir != NULL);
			FreeDir(newdir);
		}
	}
}

/* Function to return the list of files in the log directory */
Datum
pg_ls_logdir(PG_FUNCTION_ARGS)
{
	return pg_ls_dir_files(fcinfo, Log_directory, LS_DIR_COMMON | LS_DIR_MISSING_OK);
}

/* Function to return the list of files in the WAL directory */
Datum
pg_ls_waldir(PG_FUNCTION_ARGS)
{
	return pg_ls_dir_files(fcinfo, XLOGDIR, LS_DIR_COMMON);
}

/*
 * Generic function to return the list of files in pgsql_tmp
 */
static Datum
pg_ls_tmpdir(FunctionCallInfo fcinfo, Oid tblspc)
{
	char		path[MAXPGPATH];

	if (!SearchSysCacheExists1(TABLESPACEOID, ObjectIdGetDatum(tblspc)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist",
						tblspc)));

	TempTablespacePath(path, tblspc);
	return pg_ls_dir_files(fcinfo, path,
			LS_DIR_COMMON | LS_DIR_MISSING_OK);
}

/*
 * Function to return the list of temporary files in the pg_default tablespace's
 * pgsql_tmp directory
 */
Datum
pg_ls_tmpdir_noargs(PG_FUNCTION_ARGS)
{
	return pg_ls_tmpdir(fcinfo, DEFAULTTABLESPACE_OID);
}

/*
 * Function to return the list of temporary files in the specified tablespace's
 * pgsql_tmp directory
 */
Datum
pg_ls_tmpdir_1arg(PG_FUNCTION_ARGS)
{
	return pg_ls_tmpdir(fcinfo, PG_GETARG_OID(0));
}

/*
 * Function to return the list of files in the WAL archive status directory.
 */
Datum
pg_ls_archive_statusdir(PG_FUNCTION_ARGS)
{
	return pg_ls_dir_files(fcinfo, XLOGDIR "/archive_status",
			LS_DIR_COMMON | LS_DIR_MISSING_OK);
}

/*
 * Return the list of files and metadata in an arbitrary directory.
 */
Datum
pg_ls_dir_metadata(PG_FUNCTION_ARGS)
{
	char	*dirname = convert_and_check_filename(PG_GETARG_TEXT_PP(0));

	return pg_ls_dir_files(fcinfo, dirname,
			LS_DIR_METADATA | LS_DIR_TYPE);
}

/*
 * Return the list of files and metadata in an arbitrary directory.
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments.
 */
Datum
pg_ls_dir_metadata_1arg(PG_FUNCTION_ARGS)
{
	char	*dirname = convert_and_check_filename(PG_GETARG_TEXT_PP(0));

	return pg_ls_dir_files(fcinfo, dirname,
			LS_DIR_METADATA | LS_DIR_TYPE);
}
