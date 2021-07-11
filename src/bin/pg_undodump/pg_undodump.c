/*-------------------------------------------------------------------------
 *
 * pg_undodump.c - decode and display UNDO log
 *
 * Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_undodump/pg_undodump.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/undolog.h"
#include "access/undopage.h"
#include "access/undorecordset.h"
#include "common/logging.h"
#include "common/fe_memutils.h"
#include "common/undo_parser.h"
#include "rmgrdesc.h"

#include "getopt_long.h"

static const char *progname;

static bool show_chunks = false;
static bool show_metadata = false;
static bool show_records = false;

/*
 * Open the file in the valid target directory.
 *
 * return a read only fd
 */
static int
open_file(UndoSegFile *seg)
{
	int			fd = -1;

	fd = open(seg->path, O_RDONLY | PG_BINARY, 0);

	if (fd < 0)
		pg_log_error("could not open file \"%s\": %s",
					 seg->path, strerror(errno));
	return fd;
}

static void
usage(void)
{
	printf(_("%s decodes and displays PostgreSQL undo logs for debugging.\n\n"),
		   progname);
	printf(_("Usage:\n  %s [OPTION]... DATADIR\n\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -c, --chunks           show chunk headers\n"));
	/* TODO Rename to -s / --slots ? */
	printf(_("  -m, --metadata         show metadata of the undo log files\n"));
	printf(_("  -r, --records          show undo log records\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nReport bugs to <pgsql-bugs@lists.postgresql.org>.\n"));
}

/*
 * Each log consists of multiple segments, which need to be processed by the
 * offset. To ensure that the logs are sorted separately, sort first by logno,
 * then by offset.
 */
static int
undo_seg_compare(const void *s1, const void *s2)
{
	UndoSegFile *seg1 = (UndoSegFile *) s1;
	UndoSegFile *seg2 = (UndoSegFile *) s2;

	if (seg1->logno != seg2->logno)
		return seg1->logno > seg2->logno ? 1 : -1;

	if (seg1->offset != seg2->offset)
		return seg1->offset > seg2->offset ? 1 : -1;

	return 0;
}

static void
print_chunk_info(UndoLogChunkInfo * chunk, UndoRecPtr location)
{
	UndoRecordSetChunkHeader *hdr = &chunk->hdr;
	UndoLogNumber logno = UndoRecPtrGetLogNo(location);

	printf("logno: %.6X, start: " UndoRecPtrFormat ", prev: ", logno, location);
	if (hdr->previous_chunk != InvalidUndoRecPtr)
	{
		UndoLogNumber logno_prev = UndoRecPtrGetLogNo(hdr->previous_chunk);
		UndoLogOffset off_prev = UndoRecPtrGetOffset(hdr->previous_chunk);

		printf("%X.%010zX, ", logno_prev, off_prev);
	}
	else
		printf("<invalid>, ");
	printf("size: %zu, ", hdr->size);

	printf("discarded: %s, ", hdr->discarded ? "true" : "false");

	if (chunk->type == URST_TRANSACTION)
	{
		XactUndoRecordSetHeader *hdr;
		TransactionId xid;

		hdr = &chunk->type_header.xact;
		xid = XidFromFullTransactionId(hdr->fxid);

		/*
		 * Print out t/f rather than true/false as if it was the bool SQL
		 * type. (Other monitoring code does use bool.)
		 */
		printf("type: transaction (xid=%u, applied=%s)",
			   xid, hdr->applied ? "t" : "f");
	}
	else if (chunk->type == URST_FOO)
	{
		printf("type: foo (%s)", chunk->type_header.foo);
	}
	else if (chunk->type == URST_INVALID)
	{
		/* Non-first chunk of the URS. */
	}
	else
	{
		pg_log_error("unrecognized URS type %d", chunk->type);
		return;
	}

	printf("\n");
}

static void
print_record_info(UndoNode *node, UndoRecPtr location)
{
	const RmgrDescData *desc;
	StringInfoData buf;
	WrittenUndoNode wnode;

	wnode.n = *node;
	wnode.location = location;

	initStringInfo(&buf);

	desc = &RmgrDescTable[wnode.n.rmid];
	resetStringInfo(&buf);
	desc->undo_desc(&buf, &wnode);
	printf("rmgr: %-11s len: %7zu, urp: %06X.%010zX, %s\n",
		   desc->rm_name,
		   wnode.n.length,
		   (UndoLogNumber) UndoRecPtrGetLogNo(location),
		   UndoRecPtrGetOffset(location),
		   buf.data);

	pfree(buf.data);
}

/*
 * Parse a single segment of undo log file.
 *
 * 'segbuf' points to memory containing the segment contents
 * 'seg' points to information about the segment.
 */
static void
process_log_segment(UndoLogParserState * s, char *segbuf, UndoSegFile *seg)
{
	char	   *p = segbuf;
	int			i;

	for (i = 0; i < UndoLogSegmentSize / BLCKSZ; i++)
	{
		int			j;

		parse_undo_page(s, p, i, seg, show_records);

		for (j = 0; j < s->nitems; j++)
		{
			UndoPageItem *item = &s->items[j];

			if (show_chunks)
				print_chunk_info(&item->u.chunk, item->location);
			else if (show_records)
				print_record_info(&item->u.record, item->location);
		}

		/*
		 * Do not continue if chunk ended in the previous page (the rest of
		 * the log should have been empty at that moment.)
		 */
		if (s->gap)
			return;

		p += BLCKSZ;
	}
}

/*
 * Process segments of a single log file. Return prematurely if any error is
 * encountered.
 *
 * prev_chunk is in/out argument that helps to maintain the pointer to the
 * previous chunk across calls.
 */
static void
process_log(UndoSegFile *first, int count)
{
	int			i;
	UndoSegFile *seg = first;
	UndoLogOffset off_expected = 0;
	char		buf[UndoLogSegmentSize];
	UndoLogParserState state;

	/* This is very unlikely, but easy to check. */
	if (count > (UndoLogMaxSize / UndoLogSegmentSize))
	{
		pg_log_error("log %d has too many segments", first->logno);
		return;
	}

	initialize_undo_parser(&state);

	for (i = 0; i < count; i++)
	{
		int			seg_file;
		int			nread;

		/*
		 * Do not continue if chunk ended in the previous segment (the rest of
		 * the log should have been empty at that moment.)
		 */
		if (state.gap)
			return;

		/*
		 * Since the UNDO log is a continuous stream of changes, any hole
		 * terminates processing.
		 */
		if (seg->offset != off_expected)
		{
			pg_log_error("segment %010zX missing in log %d", off_expected,
						 seg->logno);
			return;
		}

		/* Open the segment file and read it. */
		seg_file = open_file(seg);
		if (seg_file < 0)
			return;

		nread = read(seg_file, buf, UndoLogSegmentSize);
		if (nread != UndoLogSegmentSize)
		{
			if (nread <= 0)
				pg_log_error("could not read from log file %s: %s",
							 seg->path, strerror(errno));
			else
				pg_log_error("could only read %d bytes out of %zu from log file %s: %s",
							 nread, UndoLogSegmentSize, seg->path,
							 strerror(errno));
			return;
		}

		/* Process pages of the segment. */
		process_log_segment(&state, buf, seg);

		close(seg_file);
		off_expected += UndoLogSegmentSize;
		seg++;
	}

	finalize_undo_parser(&state, show_records);
}

static void
process_logs(const char *dir_path)
{
	DIR		   *dir;
	struct dirent *de;
	UndoSegFile *segments,
			   *seg,
			   *log_start;
	int			nseg,
				nseg_max,
				i;

	dir = opendir(dir_path);
	if (dir == NULL)
	{
		pg_log_error("could not open directory \"%s\": %m", dir_path);
		exit(1);
	}

	nseg_max = 8;
	nseg = 0;
	segments = (UndoSegFile *) palloc(nseg_max * sizeof(UndoSegFile));

	/* First, collect information on all segments. */
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		UndoLogNumber logno;
		UndoLogOffset offset;
		int			offset_high;
		int			offset_low;

		if ((strcmp(de->d_name, ".") == 0) ||
			(strcmp(de->d_name, "..") == 0))
			continue;

		if (strlen(de->d_name) != 17 ||
			sscanf(de->d_name, "%06X.%02X%08X",
				   &logno, &offset_high, &offset_low) != 3)
		{
			pg_log_info("unexpected file \"%s\" in \"%s\"", de->d_name, dir_path);
			continue;
		}

		offset = ((UndoLogOffset) offset_high << 32) | offset_low;

		if (nseg >= nseg_max)
		{
			nseg_max *= 2;
			segments = (UndoSegFile *) repalloc(segments,
												nseg_max * sizeof(UndoSegFile));
		}
		seg = &segments[nseg++];
		snprintf(seg->path, sizeof(seg->path), "%s/%s", dir_path, de->d_name);
		seg->logno = logno;
		seg->offset = offset;
	}

	if (errno)
	{
		pg_log_error("could not read directory \"%s\": %m", dir_path);
		goto cleanup;
	}

	if (closedir(dir))
	{
		pg_log_error("could not close directory \"%s\": %m", dir_path);
		goto cleanup;
	}

	/*
	 * The segments need to be processed in the offset order, so sort them.
	 */
	qsort((void *) segments, nseg, sizeof(UndoSegFile), undo_seg_compare);

	/* Process the per-log sequences. */
	seg = log_start = segments;
	for (i = 0; i < nseg; i++)
	{
		/* Reached the end or a new log? */
		if (i == nseg || seg->logno != log_start->logno)
		{
			process_log(log_start, seg - log_start);
			log_start = seg;
		}

		seg++;
	}
	if (seg > log_start)
		process_log(log_start, seg - log_start);

cleanup:
	pfree(segments);
}

static void
process_metadata(const char *dir_path)
{
	DIR		   *dir;
	struct dirent *de;
	XLogRecPtr	last = InvalidXLogRecPtr;
	int			fd = -1;
	char		fpath[MAXPGPATH];
	UndoLogNumber next_logno,
				num_logs;
	int			i,
				nread;
	pg_crc32c	crc;

	dir = opendir(dir_path);
	if (dir == NULL)
	{
		pg_log_error("could not open directory \"%s\": %m", dir_path);
		exit(1);
	}

	/* First, select the latest checkpoint */
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		XLogRecPtr	current;

		if ((strcmp(de->d_name, ".") == 0) ||
			(strcmp(de->d_name, "..") == 0))
			continue;

		if (strlen(de->d_name) != 16 ||
			sscanf(de->d_name, "%016" INT64_MODIFIER "X", &current) != 1)
		{
			pg_log_info("unexpected file \"%s\" in \"%s\"", de->d_name, dir_path);
			continue;
		}

		if (last == InvalidXLogRecPtr || current > last)
			last = current;
	}

	if (last == InvalidXLogRecPtr)
	{
		pg_log_error("No slot metadata found in \"%s\"", dir_path);
		exit(1);
	}

	snprintf(fpath, MAXPGPATH, "pg_undo/%016" INT64_MODIFIER "X", last);

	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0)
	{
		pg_log_error("could not open file \"%s\": %s", fpath,
					 strerror(errno));
		exit(1);
	}

	nread = read(fd, &next_logno, sizeof(next_logno));
	if (nread != sizeof(next_logno))
	{
		pg_log_error("could not read next_logno");
		exit(1);
	}

	nread = read(fd, &num_logs, sizeof(num_logs));
	if (nread != sizeof(num_logs))
	{
		pg_log_error("could not read num_logs");
		exit(1);
	}

	for (i = 0; i < num_logs; i++)
	{
		UndoLogMetaData slot;

		nread = read(fd, &slot, sizeof(UndoLogMetaData));
		if (nread == 0)
			break;
		if (nread != sizeof(UndoLogMetaData))
		{
			if (nread <= 0)
				pg_log_error("could not read from file %s: %s", fpath,
							 strerror(errno));
			else
				pg_log_error("could only read %d bytes out of %zu from file %s: %s",
							 nread, sizeof(UndoLogMetaData), fpath,
							 strerror(errno));
			exit(1);
		}

		/* TODO Print out all the fields. */
		printf("logno: %d, discard: " UndoRecPtrFormat
			   ", insert: " UndoRecPtrFormat ", size: %lu \n",
			   slot.logno,
			   MakeUndoRecPtr(slot.logno, slot.discard),
			   MakeUndoRecPtr(slot.logno, slot.insert),
			   slot.size);
	}

	nread = read(fd, &crc, sizeof(pg_crc32c));
	if (nread == sizeof(pg_crc32c))
	{
		/* TODO Verify CRC. */
	}
	else
	{
		if (nread < 0)
			pg_log_error("could not read from file %s: %s", fpath,
						 strerror(errno));
		else
			pg_log_error("could only read %d bytes out of %zu from file %s: %s",
						 nread, sizeof(pg_crc32c), fpath,
						 strerror(errno));

		pg_log_warning("CRC not found");
	}

	close(fd);
}

int
main(int argc, char **argv)
{
	char	   *DataDir = NULL;

	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"chunks", no_argument, NULL, 'c'},
		{"metadata", no_argument, NULL, 'm'},
		{"records", no_argument, NULL, 'r'},
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_undodump"));
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_und_dump (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((option = getopt_long(argc, argv, "D:cmr",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'D':
				DataDir = optarg;
				break;

			case 'c':
				show_chunks = true;
				break;

			case 'm':
				show_metadata = true;
				break;

			case 'r':
				show_records = true;
				break;

			default:
				goto bad_argument;
		}
	}

	if ((optind + 1) < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind + 1]);
		goto bad_argument;
	}

	if (DataDir == NULL && optind < argc)
		DataDir = argv[optind++];

	if (DataDir == NULL)
	{
		pg_log_error("no data directory specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	/*
	 * Not all the options can be used at the same time.
	 */
	if (show_metadata)
	{
		if (show_chunks)
		{
			pg_log_error("the '-c' and '-m' options are mutually exclusive");
			exit(1);
		}

		if (show_records)
		{
			pg_log_error("the '-r' and '-m' options are mutually exclusive");
			exit(1);
		}
	}
	if (show_chunks && show_records)
	{
		pg_log_error("the '-c' and '-r' options are mutually exclusive");
		exit(1);
	}

	/* List of records is the default output. */
	if (!(show_chunks || show_records || show_metadata))
		show_records = true;

	if (chdir(DataDir) < 0)
	{
		pg_log_error("could not change directory to \"%s\": %m",
					 DataDir);
		exit(1);
	}

	if (show_chunks || show_records)
		/* TODO Process tablespaces too. */
		process_logs("base/undo");
	else
		process_metadata("pg_undo");

	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}
