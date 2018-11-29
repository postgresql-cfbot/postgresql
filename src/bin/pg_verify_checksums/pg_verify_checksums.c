/*
 * pg_verify_checksums
 *
 * Verifies page level checksums in a cluster
 *
 *	Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 *	src/bin/pg_verify_checksums/pg_verify_checksums.c
 */
#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/relpath.h"
#include "getopt_long.h"
#include "pg_getopt.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"


static int64 files = 0;
static int64 blocks = 0;
static int64 badblocks = 0;
static int64 skippedblocks = 0;
static ControlFileData *ControlFile;
static XLogRecPtr checkpointLSN;

static char *only_relfilenode = NULL;
static bool verbose = false;
static bool online = false;

static const char *progname;

static void
usage(void)
{
	printf(_("%s verifies data checksums in a PostgreSQL database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -r RELFILENODE         check only relation with specified relfilenode\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}

/*
 * isRelFileName
 *
 * Check if the given file name is authorized for checksum verification.
 */
static bool
isRelFileName(const char *fn)
{
	int			pos;

	/*----------
	 * Only files including data checksums are authorized for verification.
	 * This is guessed based on the file name by reverse-engineering
	 * GetRelationPath() so make sure to update both code paths if any
	 * updates are done.  The following file name formats are allowed:
	 * <digits>
	 * <digits>.<segment>
	 * <digits>_<forkname>
	 * <digits>_<forkname>.<segment>
	 *
	 * Note that temporary files, beginning with 't', are also skipped.
	 *
	 *----------
	 */

	/* A non-empty string of digits should follow */
	for (pos = 0; isdigit((unsigned char) fn[pos]); ++pos)
		;
	/* leave if no digits */
	if (pos == 0)
		return false;
	/* good to go if only digits */
	if (fn[pos] == '\0')
		return true;

	/* Authorized fork files can be scanned */
	if (fn[pos] == '_')
	{
		int			forkchar = forkname_chars(&fn[pos + 1], NULL);

		if (forkchar <= 0)
			return false;

		pos += forkchar + 1;
	}

	/* Check for an optional segment number */
	if (fn[pos] == '.')
	{
		int			segchar;

		for (segchar = 1; isdigit((unsigned char) fn[pos + segchar]); ++segchar)
			;

		if (segchar <= 1)
			return false;
		pos += segchar;
	}

	/* Now this should be the end */
	if (fn[pos] != '\0')
		return false;
	return true;
}

static void
scan_file(const char *fn, BlockNumber segmentno)
{
	PGAlignedBlock buf;
	PageHeader	header = (PageHeader) buf.data;
	int			f;
	BlockNumber blockno;
	bool		block_retry = false;

	f = open(fn, O_RDONLY | PG_BINARY, 0);
	if (f < 0)
	{
		if (online && errno == ENOENT)
		{
			/* File was removed in the meantime */
			return;
		}

		fprintf(stderr, _("%s: could not open file \"%s\": %s\n"),
				progname, fn, strerror(errno));
		exit(1);
	}

	files++;

	for (blockno = 0;; blockno++)
	{
		uint16		csum;
		int			r = read(f, buf.data, BLCKSZ);

		if (r == 0)
			break;
		if (r < 0)
		{
			fprintf(stderr, _("%s: could not read block %u in file \"%s\": %s\n"),
					progname, blockno, fn, strerror(errno));
			return;
		}
		if (r != BLCKSZ)
		{
			if (block_retry)
			{
				/* We already tried once to reread the block, bail out */
				fprintf(stderr, _("%s: could not read block %u in file \"%s\": read %d of %d\n"),
						progname, blockno, fn, r, BLCKSZ);
				exit(1);
			}

			/*
			 * Retry the block. It's possible that we read the block while it
			 * was extended or shrinked, so it it ends up looking torn to us.
			 */

			/*
			 * Seek back by the amount of bytes we read to the beginning of
			 * the failed block.
			 */
			if (lseek(f, -r, SEEK_CUR) == -1)
			{
				fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
						progname, fn);
				exit(1);
			}

			/* Set flag so we know a retry was attempted */
			block_retry = true;

			/* Reset loop to validate the block again */
			blockno--;

			continue;
		}

		/* New pages have no checksum yet */
		if (PageIsNew(header))
		{
			skippedblocks++;
			continue;
		}

		blocks++;

		csum = pg_checksum_page(buf.data, blockno + segmentno * RELSEG_SIZE);
		if (csum != header->pd_checksum)
		{
			/*
			 * Retry the block on the first failure.  If the verification is
			 * done while the instance is online, it is possible that we read
			 * the first 4K page of the block just before postgres updated the
			 * entire block so it ends up looking torn to us.  We only need to
			 * retry once because the LSN should be updated to something we can
			 * ignore on the next pass.  If the error happens again then it is
			 * a true validation failure.
			 */
			if (!block_retry)
			{
				/* Seek to the beginning of the failed block */
				if (lseek(f, -BLCKSZ, SEEK_CUR) == -1)
				{
					fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
							progname, fn);
					exit(1);
				}

				/* Set flag so we know a retry was attempted */
				block_retry = true;

				/* Reset loop to validate the block again */
				blockno--;

				continue;
			}

			/*
			 * The checksum verification failed on retry as well.  Check if the
			 * page has been modified since the checkpoint and skip it in this
			 * case.
			 */
			if (PageGetLSN(buf.data) > checkpointLSN)
			{
				block_retry = false;
				blocks--;
				skippedblocks++;
				continue;
			}

			if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION)
				fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %u: calculated checksum %X but block contains %X\n"),
						progname, fn, blockno, csum, header->pd_checksum);
			badblocks++;
		}
		block_retry = false;
	}

	if (verbose)
		fprintf(stderr,
				_("%s: checksums verified in file \"%s\"\n"), progname, fn);

	close(f);
}

static void
scan_directory(const char *basedir, const char *subdir)
{
	char		path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(path, sizeof(path), "%s/%s", basedir, subdir);
	dir = opendir(path);
	if (!dir)
	{
		fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"),
				progname, path, strerror(errno));
		exit(1);
	}
	while ((de = readdir(dir)) != NULL)
	{
		char		fn[MAXPGPATH];
		struct stat st;

		if (!isRelFileName(de->d_name))
			continue;

		snprintf(fn, sizeof(fn), "%s/%s", path, de->d_name);
		if (lstat(fn, &st) < 0)
		{
			if (online && errno == ENOENT)
			{
				/* File was removed in the meantime */
				continue;
			}

			fprintf(stderr, _("%s: could not stat file \"%s\": %s\n"),
					progname, fn, strerror(errno));
			exit(1);
		}
		if (S_ISREG(st.st_mode))
		{
			char		fnonly[MAXPGPATH];
			char	   *forkpath,
					   *segmentpath;
			BlockNumber segmentno = 0;

			/*
			 * Cut off at the segment boundary (".") to get the segment number
			 * in order to mix it into the checksum. Then also cut off at the
			 * fork boundary, to get the relfilenode the file belongs to for
			 * filtering.
			 */
			strlcpy(fnonly, de->d_name, sizeof(fnonly));
			segmentpath = strchr(fnonly, '.');
			if (segmentpath != NULL)
			{
				*segmentpath++ = '\0';
				segmentno = atoi(segmentpath);
				if (segmentno == 0)
				{
					fprintf(stderr, _("%s: invalid segment number %d in file name \"%s\"\n"),
							progname, segmentno, fn);
					exit(1);
				}
			}

			forkpath = strchr(fnonly, '_');
			if (forkpath != NULL)
				*forkpath++ = '\0';

			if (only_relfilenode && strcmp(only_relfilenode, fnonly) != 0)
				/* Relfilenode not to be included */
				continue;

			scan_file(fn, segmentno);
		}
#ifndef WIN32
		else if (S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode))
#else
		else if (S_ISDIR(st.st_mode) || pgwin32_is_junction(fn))
#endif
			scan_directory(path, de->d_name);
	}
	closedir(dir);
}

int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	char	   *DataDir = NULL;
	int			c;
	int			option_index;
	bool		crc_ok;

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_verify_checksums"));

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
			puts("pg_verify_checksums (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:r:v", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'v':
				verbose = true;
				break;
			case 'D':
				DataDir = optarg;
				break;
			case 'r':
				if (atoi(optarg) == 0)
				{
					fprintf(stderr, _("%s: invalid relfilenode specification, must be numeric: %s\n"), progname, optarg);
					exit(1);
				}
				only_relfilenode = pstrdup(optarg);
				break;
			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	if (DataDir == NULL)
	{
		if (optind < argc)
			DataDir = argv[optind++];
		else
			DataDir = getenv("PGDATA");

		/* If no DataDir was specified, and none could be found, error out */
		if (DataDir == NULL)
		{
			fprintf(stderr, _("%s: no data directory specified\n"), progname);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Check if checksums are enabled */
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
	if (!crc_ok)
	{
		fprintf(stderr, _("%s: pg_control CRC value is incorrect\n"), progname);
		exit(1);
	}

	/* Check if cluster is running */
	if (ControlFile->state != DB_SHUTDOWNED &&
		ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY)
		online = true;

	if (ControlFile->data_checksum_version == 0)
	{
		fprintf(stderr, _("%s: data checksums are not enabled in cluster\n"), progname);
		exit(1);
	}

	/* Get checkpoint LSN */
	checkpointLSN = ControlFile->checkPoint;

	/* Scan all files */
	scan_directory(DataDir, "global");
	scan_directory(DataDir, "base");
	scan_directory(DataDir, "pg_tblspc");

	printf(_("Checksum scan completed\n"));
	printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
	printf(_("Files scanned:  %s\n"), psprintf(INT64_FORMAT, files));
	printf(_("Blocks scanned: %s\n"), psprintf(INT64_FORMAT, blocks));
	if (skippedblocks > 0)
		printf(_("Blocks skipped: %s\n"), psprintf(INT64_FORMAT, skippedblocks));
	printf(_("Bad checksums:  %s\n"), psprintf(INT64_FORMAT, badblocks));

	if (badblocks > 0)
		return 1;

	return 0;
}
