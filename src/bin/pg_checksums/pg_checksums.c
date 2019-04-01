/*-------------------------------------------------------------------------
 *
 * pg_checksums.c
 *	  Checks, enables or disables page level checksums for a cluster
 *
 * Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/bin/pg_checksums/pg_checksums.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "getopt_long.h"
#include "pg_getopt.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"


static int64 files = 0;
static int64 skippedfiles = 0;
static int64 blocks = 0;
static int64 badblocks = 0;
static int64 skippedblocks = 0;
static ControlFileData *ControlFile;
static XLogRecPtr checkpointLSN;
static int64 blocks_last_lsn_update = 0;

static char *only_relfilenode = NULL;
static bool do_sync = true;
static bool verbose = false;
static bool online = false;

static char *DataDir = NULL;

typedef enum
{
	PG_MODE_CHECK,
	PG_MODE_DISABLE,
	PG_MODE_ENABLE
} PgChecksumMode;

/*
 * Filename components.
 *
 * XXX: fd.h is not declared here as frontend side code is not able to
 * interact with the backend-side definitions for the various fsync
 * wrappers.
 */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

static PgChecksumMode mode = PG_MODE_CHECK;

static const char *progname;

static void
usage(void)
{
	printf(_("%s enables, disables or verifies data checksums in a PostgreSQL database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -c, --check            check data checksums (default)\n"));
	printf(_("  -d, --disable          disable data checksums\n"));
	printf(_("  -e, --enable           enable data checksums\n"));
	printf(_("  -N, --no-sync          do not wait for changes to be written safely to disk\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -r RELFILENODE         check only relation with specified relfilenode\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to <pgsql-bugs@lists.postgresql.org>.\n"));
}

/*
 * List of files excluded from checksum validation.
 *
 * Note: this list should be kept in sync with what basebackup.c includes.
 */
static const char *const skip[] = {
	"pg_control",
	"pg_filenode.map",
	"pg_internal.init",
	"PG_VERSION",
#ifdef EXEC_BACKEND
	"config_exec_params",
	"config_exec_params.new",
#endif
	NULL,
};

static void
update_checkpoint_lsn(void)
{
	bool		crc_ok;

	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
	if (!crc_ok)
	{
		fprintf(stderr, _("%s: pg_control CRC value is incorrect\n"), progname);
		exit(1);
	}

	/* Update checkpointLSN and blocks_last_lsn_update with the current value */
	checkpointLSN = ControlFile->checkPoint;
	blocks_last_lsn_update = blocks;
}

static bool
skipfile(const char *fn)
{
	const char *const *f;

	for (f = skip; *f; f++)
		if (strcmp(*f, fn) == 0)
			return true;

	return false;
}

static void
scan_file(const char *fn, BlockNumber segmentno)
{
	PGAlignedBlock buf;
	PageHeader	header = (PageHeader) buf.data;
	int			f;
	BlockNumber blockno;
	int			block_loc = 0;
	bool		block_retry = false;
	bool		all_zeroes;
	size_t	   *pagebytes;
	int			flags;

	Assert(mode == PG_MODE_ENABLE ||
		   mode == PG_MODE_CHECK);

	flags = (mode == PG_MODE_ENABLE) ? O_RDWR : O_RDONLY;
	f = open(fn, PG_BINARY | flags, 0);

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
		int			r = read(f, buf.data + block_loc, BLCKSZ - block_loc);

		if (r == 0)
		{
			/*
			 * We had a short read and got an EOF before we could read the
			 * whole block, so count this as a skipped block.
			 */
			if (online && block_loc != 0)
				skippedblocks++;
			break;
		}
		if (r < 0)
		{
			skippedfiles++;
			fprintf(stderr, _("%s: could not read block %u in file \"%s\": %s\n"),
					progname, blockno, fn, strerror(errno));
			return;
		}
		if (r < (BLCKSZ - block_loc))
		{
			if (online)
			{
				/*
				 * We read only parts of the block, possibly due to a
				 * concurrent extension of the relation file.  Increment
				 * block_loc by the amount that we read and try again.
				 */
				block_loc += r;
				continue;
			}
			else
			{
				skippedfiles++;
				fprintf(stderr, _("%s: could not read block %u in file \"%s\": read %d of %d\n"),
						progname, blockno, fn, r, BLCKSZ);
				return;
			}
		}

		blocks++;

		/* New pages have no checksum yet */
		if (PageIsNew(header))
		{
			/* Check for an all-zeroes page */
			all_zeroes = true;
			pagebytes = (size_t *) buf.data;
			for (int i = 0; i < (BLCKSZ / sizeof(size_t)); i++)
			{
				if (pagebytes[i] != 0)
				{
					all_zeroes = false;
					break;
				}
			}
			if (!all_zeroes)
			{
				fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %u: pd_upper is zero but block is not all-zero\n"),
						progname, fn, blockno);
				badblocks++;
			}
			else
			{
				skippedblocks++;
			}
			continue;
		}

		csum = pg_checksum_page(buf.data, blockno + segmentno * RELSEG_SIZE);
		if (mode == PG_MODE_CHECK)
		{
			if (csum != header->pd_checksum)
			{
				if (online)
				{
					/*
					 * Retry the block on the first failure if online.  If the
					 * verification is done while the instance is online, it is
					 * possible that we read the first 4K page of the block just
					 * before postgres updated the entire block so it ends up
					 * looking torn to us.  We only need to retry once because the
					 * LSN should be updated to something we can ignore on the next
					 * pass.  If the error happens again then it is a true
					 * validation failure.
					 */
					if (!block_retry)
					{
						/* Seek to the beginning of the failed block */
						if (lseek(f, -BLCKSZ, SEEK_CUR) == -1)
						{
							skippedfiles++;
							fprintf(stderr, _("%s: could not lseek in file \"%s\": %m\n"),
									progname, fn);
							return;
						}

						/* Set flag so we know a retry was attempted */
						block_retry = true;

						/* Reset loop to validate the block again */
						blockno--;
						blocks--;

						continue;
					}

					/*
					 * The checksum verification failed on retry as well.
					 * Check if the page has been modified since the last
					 * checkpoint and skip it in this case.  As a sanity check,
					 * demand that the upper 32 bits of the LSN are identical
					 * in order to skip as a guard against a corrupted LSN in
					 * the pageheader.
					 */
					update_checkpoint_lsn();
					if ((PageGetLSN(buf.data) > checkpointLSN) &&
					   (PageGetLSN(buf.data) >> 32 == checkpointLSN >> 32))
					{
						block_retry = false;
						skippedblocks++;
						continue;
					}
				}

				if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION)
					fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %u: calculated checksum %X but block contains %X\n"),
							progname, fn, blockno, csum, header->pd_checksum);
				badblocks++;
			}
		}
		else if (mode == PG_MODE_ENABLE)
		{
			/* Set checksum in page header */
			header->pd_checksum = csum;

			/* Seek back to beginning of block */
			if (lseek(f, -BLCKSZ, SEEK_CUR) < 0)
			{
				fprintf(stderr, _("%s: seek failed for block %d in file \"%s\": %s\n"), progname, blockno, fn, strerror(errno));
				exit(1);
			}

			/* Write block with checksum */
			if (write(f, buf.data, BLCKSZ) != BLCKSZ)
			{
				fprintf(stderr, _("%s: could not update checksum of block %d in file \"%s\": %s\n"),
						progname, blockno, fn, strerror(errno));
				exit(1);
			}
		}

		block_retry = false;
		block_loc = 0;
	}

	if (verbose)
	{
		if (mode == PG_MODE_CHECK)
			fprintf(stderr,
					_("%s: checksums verified in file \"%s\"\n"), progname, fn);
		if (mode == PG_MODE_ENABLE)
			fprintf(stderr,
					_("%s: checksums enabled in file \"%s\"\n"), progname, fn);
	}

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

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		/* Skip temporary files */
		if (strncmp(de->d_name,
					PG_TEMP_FILE_PREFIX,
					strlen(PG_TEMP_FILE_PREFIX)) == 0)
			continue;

		/* Skip temporary folders */
		if (strncmp(de->d_name,
					PG_TEMP_FILES_DIR,
					strlen(PG_TEMP_FILES_DIR)) == 0)
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

			if (skipfile(de->d_name))
				continue;

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
		{"check", no_argument, NULL, 'c'},
		{"pgdata", required_argument, NULL, 'D'},
		{"disable", no_argument, NULL, 'd'},
		{"enable", no_argument, NULL, 'e'},
		{"no-sync", no_argument, NULL, 'N'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			c;
	int			option_index;
	bool		crc_ok;

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_checksums"));

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
			puts("pg_checksums (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "cD:deNr:v", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'c':
				mode = PG_MODE_CHECK;
				break;
			case 'd':
				mode = PG_MODE_DISABLE;
				break;
			case 'e':
				mode = PG_MODE_ENABLE;
				break;
			case 'N':
				do_sync = false;
				break;
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

	/* Relfilenode checking only works in --check mode */
	if (mode != PG_MODE_CHECK && only_relfilenode)
	{
		fprintf(stderr, _("%s: relfilenode option only possible with --check\n"), progname);
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

	if (ControlFile->pg_control_version != PG_CONTROL_VERSION)
	{
		fprintf(stderr, _("%s: cluster is not compatible with this version of pg_checksums\n"),
				progname);
		exit(1);
	}

	if (ControlFile->blcksz != BLCKSZ)
	{
		fprintf(stderr, _("%s: database cluster is not compatible.\n"),
				progname);
		fprintf(stderr, _("The database cluster was initialized with block size %u, but pg_checksums was compiled with block size %u.\n"),
				ControlFile->blcksz, BLCKSZ);
		exit(1);
	}

	/* Check if cluster is running */
	if (ControlFile->state != DB_SHUTDOWNED &&
		ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY)
		online = true;

	if (ControlFile->data_checksum_version == 0 &&
		mode == PG_MODE_CHECK)
	{
		fprintf(stderr, _("%s: data checksums are not enabled in cluster\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == 0 &&
		mode == PG_MODE_DISABLE)
	{
		fprintf(stderr, _("%s: data checksums are already disabled in cluster\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version > 0 &&
		mode == PG_MODE_ENABLE)
	{
		fprintf(stderr, _("%s: data checksums are already enabled in cluster\n"), progname);
		exit(1);
	}

	/* Get checkpoint LSN */
	checkpointLSN = ControlFile->checkPoint;

	/* Operate on all files if checking or enabling checksums */
	if (mode == PG_MODE_CHECK || mode == PG_MODE_ENABLE)
	{
		scan_directory(DataDir, "global");
		scan_directory(DataDir, "base");
		scan_directory(DataDir, "pg_tblspc");

		printf(_("Checksum operation completed\n"));
		printf(_("Files scanned:  %s\n"), psprintf(INT64_FORMAT, files));
		if (skippedfiles > 0)
			printf(_("Files skipped: %s\n"), psprintf(INT64_FORMAT, skippedfiles));
		printf(_("Blocks scanned: %s\n"), psprintf(INT64_FORMAT, blocks));
		if (skippedblocks > 0)
			printf(_("Blocks skipped: %s\n"), psprintf(INT64_FORMAT, skippedblocks));
		if (mode == PG_MODE_CHECK)
		{
			printf(_("Bad checksums:  %s\n"), psprintf(INT64_FORMAT, badblocks));
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);

			if (badblocks > 0)
			{
				if (online)
				{
					/*
					 * Issue a warning about possible false positives due to
					 * repeatedly read torn pages.
					 */
					printf(_("%s ran against an online cluster and found some bad checksums.\n"), progname);
					printf(_("It could be that those are false positives due concurrently updated blocks,\n"));
					printf(_("checking the offending files again with the -r option is advised.\n"));
				}
				exit(1);
			}
		}
	}

	/*
	 * Finally make the data durable on disk if enabling or disabling
	 * checksums.  Flush first the data directory for safety, and then update
	 * the control file to keep the switch consistent.
	 */
	if (mode == PG_MODE_ENABLE || mode == PG_MODE_DISABLE)
	{
		ControlFile->data_checksum_version =
			(mode == PG_MODE_ENABLE) ? PG_DATA_CHECKSUM_VERSION : 0;

		if (do_sync)
		{
			printf(_("Syncing data directory\n"));
			fsync_pgdata(DataDir, progname, PG_VERSION_NUM);
		}

		printf(_("Updating control file\n"));
		update_controlfile(DataDir, progname, ControlFile, do_sync);

		if (verbose)
			printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
		if (mode == PG_MODE_ENABLE)
			printf(_("Checksums enabled in cluster\n"));
		else
			printf(_("Checksums disabled in cluster\n"));
	}

	/* skipped blocks or files are considered an error if offline */
	if (!online)
		if (skippedblocks > 0 || skippedfiles > 0)
			return 1;

	return 0;
}
