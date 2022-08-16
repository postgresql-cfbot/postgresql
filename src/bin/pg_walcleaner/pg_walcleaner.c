/*-------------------------------------------------------------------------
 *
 * pg_walcleaner.c
 *		  Tool for deleting (optionally archiving before deleting) unneeded
 *        PostgreSQL Write-Ahead Log (WAL) files to free up disk space in
 *        server down/crash because of "no space left on device"
 *        situations.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_walcleaner/pg_walcleaner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include "access/xlog_internal.h"
#include "common/controldata_utils.h"
#include "common/logging.h"
#include "common/string.h"
#include "getopt_long.h"
#include "pg_getopt.h"
#include "repl_slot.h"

#define PG_WAL_DIR "pg_wal"
#define PG_REPLSLOT_DIR "pg_replslot"

const char *progname;

/* options and defaults */
char	*archive_command = NULL;
bool	dryrun = false;
char	*data_dir = NULL;
bool	ignore_replication_slots = false;
bool	verbose = false;

/*
 * Oldest WAL file to retain. All other files before this can safely be
 * removed.
 */
char	cutoff_wal_file[MAXFNAMELEN];
ControlFileData *ControlFile = NULL;
int	WalSegSz;

static void usage(void);
static DIR *get_destination_dir(char *dest);
static void close_destination_dir(DIR *dest_dir, char *dest);
static bool ArchiveWALFile(const char *file, const char *path);
static void RemoveWALFiles(void);
static XLogRecPtr process_replslots(void);
static bool read_repl_slot(const char *name, ReplicationSlotOnDisk *s_info);

static void
usage(void)
{
	printf(_("%s deletes unneeded PostgreSQL Write-Ahead Log (WAL) files.\n\n"), progname);
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -a, --archive-command=COMMAND   archive command to execute and send WAL files before deletion\n"));
	printf(_("  -D, --pgdata=DATADIR            data directory\n"));
	printf(_("  -d, --dry-run                   dry run, show the names of the files that would be removed\n"));
	printf(_("  -i, --ignore-replication-slots  ignore replication slots to calculate oldest WAL file\n"
			 "                                  i.e. WAL files required by replication slots may be deleted\n"));
	printf(_("  -v, --verbose                   output verbose messages\n"));
	printf(_("  -V, --version                   output version information, then exit\n"));
	printf(_("  -?, --help                      show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}

/*
 * Get destination directory.
 */
static DIR *
get_destination_dir(char *dest)
{
	DIR		   *dir;

	Assert(dest != NULL);
	dir = opendir(dest);
	if (dir == NULL)
		pg_fatal("could not open directory \"%s\": %m", dest);

	return dir;
}

/*
 * Close existing directory.
 */
static void
close_destination_dir(DIR *dest_dir, char *dest)
{
	Assert(dest_dir != NULL && dest != NULL);
	if (closedir(dest_dir))
		pg_fatal("could not close directory \"%s\": %m", dest);
}

/*
 * Archives a given WAL file.
 */
static bool
ArchiveWALFile(const char *file, const char *path)
{
	char		xlogarchcmd[MAXPGPATH];
	char	   *dp;
	char	   *endp;
	const char *sp;
	int			rc;

	/* construct the command to be executed */
	dp = xlogarchcmd;
	endp = xlogarchcmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = archive_command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					/* %p: relative path of source file */
					sp++;
					strlcpy(dp, path, endp - dp);
					make_native_path(dp);
					dp += strlen(dp);
					break;
				case 'f':
					/* %f: filename of source file */
					sp++;
					strlcpy(dp, file, endp - dp);
					dp += strlen(dp);
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	if (verbose)
		pg_log_info("executing archive command \"%s\"", xlogarchcmd);

	rc = system(xlogarchcmd);

	if (rc != 0)
	{
		bool is_fatal = false;

		/*
		 * If either the shell itself, or a called command, died on a signal,
		 * abort the archiver.  We do this because system() ignores SIGINT and
		 * SIGQUIT while waiting; so a signal is very likely something that
		 * should have interrupted us too.  Also die if the shell got a hard
		 * "command not found" type of error.  If we overreact it's no big
		 * deal, the postmaster will just start the archiver again.
		 */
		if (wait_result_is_any_signal(rc, true))
			is_fatal = true;

		if (WIFEXITED(rc))
		{
			if (is_fatal)
				pg_fatal("archive command \"%s\" failed with exit code %d",
						 xlogarchcmd, WEXITSTATUS(rc));
			else
				pg_log_error("archive command \"%s\" failed with exit code %d",
							 xlogarchcmd, WEXITSTATUS(rc));
		}
		else if (WIFSIGNALED(rc))
		{
#if defined(WIN32)
			if (is_fatal)
			{
				pg_log_error("archive command \"%s\" was terminated by exception 0x%X",
							 xlogarchcmd, WTERMSIG(rc));
				pg_log_error("See C include file \"ntstatus.h\" for a description of the hexadecimal value.");
				exit(EXIT_FAILURE);
			}
			else
			{
				pg_log_error("archive command \"%s\" was terminated by exception 0x%X",
							 xlogarchcmd, WTERMSIG(rc));
				pg_log_error("See C include file \"ntstatus.h\" for a description of the hexadecimal value.");
			}
#else
			if (is_fatal)
				pg_fatal("archive command \"%s\" was terminated by signal %d: %s",
						 xlogarchcmd, WTERMSIG(rc), pg_strsignal(WTERMSIG(rc)));
			else
				pg_log_error("archive command \"%s\" was terminated by signal %d: %s",
							 xlogarchcmd, WTERMSIG(rc), pg_strsignal(WTERMSIG(rc)));
#endif
		}
		else
		{
			if (is_fatal)
				pg_fatal("archive command \"%s\" exited with unrecognized status %d",
						 xlogarchcmd, rc);
			else
				pg_log_error("archive command \"%s\" exited with unrecognized status %d",
							 xlogarchcmd, rc);
		}

		return false;
	}

	if (verbose)
		pg_log_info("archived write-ahead log file \"%s\"", file);

	return true;
}

/*
 * Removes all WAL file(s) older than the cut off WAL file name.
 */
static void
RemoveWALFiles(void)
{
	int	rc;
	DIR	*xldir;
	struct dirent *xlde;
	int cnt = 0;

	xldir = get_destination_dir(PG_WAL_DIR);

	while (errno = 0, (xlde = readdir(xldir)) != NULL)
	{
		/* ignore files that are not XLOG segments */
		if (!IsXLogFileName(xlde->d_name) &&
			!IsPartialXLogFileName(xlde->d_name))
			continue;

		/*
		 * We ignore the timeline part of the XLOG segment identifiers in
		 * deciding whether a segment is still needed. This ensures that we
		 * won't prematurely remove a segment from a parent timeline. We could
		 * probably be a little more proactive about removing segments of
		 * non-parent timelines, but that would be a whole lot more
		 * complicated.
		 *
		 * We use the alphanumeric sorting property of the filenames to decide
		 * which ones are earlier than the cutoff_wal_file file. Note that this
		 * means files are not removed in the order they were originally
		 * written, in case this worries you.
		 */
		if (strcmp(xlde->d_name + 8, cutoff_wal_file + 8) < 0)
		{
			char	path[MAXPGPATH];

			snprintf(path, sizeof(path), "%s/%s/%s", data_dir,
					 PG_WAL_DIR, xlde->d_name);

			if (archive_command != NULL && !dryrun)
			{
				/*
				 * If archive_command is specified, just archive the WAL file
				 * irrespective of whether the PostgreSQL server has archived
				 * it or not (don't look at the .done file).
				 *
				 * This might cause the WAL file archived multiple times if
				 * PostgreSQL server and pg_walcleaner uses the same archive
				 * location.
				 */
				if (ArchiveWALFile(xlde->d_name, path))
				{
					if (verbose)
						pg_log_info("successfully archived WAL file %s",
									xlde->d_name);
				}
				else
				{
					/*
					 * Let's just not remove the WAL file if ArchiveWALFile()
					 * couldn't archive it. Note that the ArchiveWALFile()
					 * would have already logged an error message.
					 */
					continue;
				}
			}

			if (archive_command == NULL)
			{
				char	archiveStatusPath[MAXPGPATH];
				struct stat stat_buf;

				/*
				 * Check for .ready file --- this means the PostgreSQL server
				 * has not yet archived the WAL file.
				 */
				StatusFilePath(archiveStatusPath, xlde->d_name, ".ready");
				if (stat(archiveStatusPath, &stat_buf) == 0)
				{
					if (verbose)
						pg_log_info("WAL file \"%s\" is yet to be archived by PostgreSQL server, hence not removing it",
									path);

					continue;
				}
			}

			cnt++;

			if (dryrun)
			{
				/*
				 * Prints the name of the file to be removed and skips the
				 * actual removal. The regular printout is so that the user can
				 * pipe the output into some other program.
				 */
				printf("%s\n", path);

				if (verbose)
					pg_log_info("file \"%s\" would be removed", path);

				continue;
			}

			if (verbose)
				pg_log_info("removing file \"%s\"", path);

			rc = unlink(path);

			if (rc != 0)
				pg_fatal("could not remove file \"%s\": %m", path);
		}
	}

	if (errno)
		pg_fatal("could not read directory \"%s\": %m", PG_WAL_DIR);

	close_destination_dir(xldir, PG_WAL_DIR);

	if (cnt == 0)
	{
		if (dryrun)
			pg_log_info("no WAL files would be removed");
		else
			pg_log_info("no WAL files were removed");
	}
	else
	{
		if (dryrun)
			pg_log_info("%d WAL file(s) would be removed", cnt);
		else
			pg_log_info("%d WAL file(s) were removed", cnt);
	}
}

/*
 * Loops over all the existing replication slots and return the oldest
 * restart_lsn.
 */
static XLogRecPtr
process_replslots(void)
{
	DIR	*rsdir;
	struct dirent *rsde;
	uint32	cnt = 0;
	XLogRecPtr oldest_restart_lsn;

	rsdir = get_destination_dir(PG_REPLSLOT_DIR);

	oldest_restart_lsn = InvalidXLogRecPtr;

	while (errno = 0, (rsde = readdir(rsdir)) != NULL)
	{
		struct stat statbuf;
		char		path[MAXPGPATH];
		ReplicationSlotOnDisk s_info;
		bool	s_read_success;

		if (strcmp(rsde->d_name, ".") == 0 ||
			strcmp(rsde->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path),
				 PG_REPLSLOT_DIR
				 "/%s",
				 rsde->d_name);

		/* we only care about directories here, skip if it's not one */
		if (lstat(path, &statbuf) == 0 && !S_ISDIR(statbuf.st_mode))
			continue;

		/* we crashed while a slot was being setup or deleted, clean up */
		if (pg_str_endswith(rsde->d_name, ".tmp"))
		{
			if (verbose)
				pg_log_info("server was crashed while the slot \"%s\" was being setup or deleted",
							rsde->d_name);

			continue;
		}

		/* looks like a slot in a normal state, decode its information */
		s_read_success = read_repl_slot(rsde->d_name, &s_info);

		if (!s_read_success)
			continue;

		cnt++;

		if (cnt == 1)
		{
			/* first time */
			oldest_restart_lsn = s_info.slotdata.restart_lsn;
		}
		else if (!XLogRecPtrIsInvalid(s_info.slotdata.restart_lsn) &&
				 s_info.slotdata.restart_lsn < oldest_restart_lsn)
		{
			oldest_restart_lsn = s_info.slotdata.restart_lsn;
		}
	}

	if (errno)
		pg_fatal("could not read directory \"%s\": %m", PG_REPLSLOT_DIR);

	if (cnt == 0 && verbose)
		pg_log_info("no replication slots were found");
	else if (cnt > 0 && verbose)
		pg_log_info("oldest restart_lsn found is %X/%X",
					LSN_FORMAT_ARGS(oldest_restart_lsn));

	close_destination_dir(rsdir, PG_REPLSLOT_DIR);

	return oldest_restart_lsn;
}

/*
 * Reads given replication slot information from its disk file and return the
 * contents.
 */
static bool
read_repl_slot(const char *name, ReplicationSlotOnDisk *s_info)
{
	ReplicationSlotOnDisk cp;
	char	slotdir[MAXPGPATH];
	char	path[MAXPGPATH];
	int		fd;
	int		readBytes;
	pg_crc32c	checksum;

	/* if temp file exists, just inform and continue further */
	sprintf(slotdir, PG_REPLSLOT_DIR"/%s", name);
	sprintf(path, "%s/state.tmp", slotdir);

	fd = open(path, O_RDONLY | PG_BINARY, 0);

	if (fd > 0)
	{
		if (verbose)
			pg_log_info("found temporary state file \"%s\": %m", path);

		if (close(fd) != 0)
			pg_fatal("could not close file \"%s\": %m", path);

		return false;
	}

	sprintf(path, "%s/state", slotdir);

	if (verbose)
		pg_log_info("reading replication slot from \"%s\"", path);

	fd = open(path, O_RDONLY | PG_BINARY, 0);

	/*
	 * We do not need to handle this as we are rename()ing the directory into
	 * place only after we fsync()ed the state file.
	 */
	if (fd < 0)
		pg_fatal("could not open file \"%s\": %m", path);

	if (verbose)
		pg_log_info("reading version independent replication slot state file");

	/* read part of statefile that's guaranteed to be version independent */
	readBytes = read(fd, &cp, ReplicationSlotOnDiskConstantSize);

	if (readBytes != ReplicationSlotOnDiskConstantSize)
	{
		if (readBytes < 0)
			pg_fatal("could not read file \"%s\": %m", path);
		else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 path, readBytes,
					 (Size) ReplicationSlotOnDiskConstantSize);
	}

	/* verify magic */
	if (cp.magic != SLOT_MAGIC)
		pg_fatal("replication slot file \"%s\" has wrong magic number: %u instead of %u",
				 path, cp.magic, SLOT_MAGIC);

	/* verify version */
	if (cp.version != SLOT_VERSION)
		pg_fatal("replication slot file \"%s\" has unsupported version %u",
				 path, cp.version);

	/* boundary check on length */
	if (cp.length != ReplicationSlotOnDiskV2Size)
		pg_fatal("replication slot file \"%s\" has corrupted length %u",
				 path, cp.length);

	if (verbose)
		pg_log_info("reading the entire replication slot state file");

	/* now that we know the size, read the entire file */
	readBytes = read(fd,
					 (char *) &cp + ReplicationSlotOnDiskConstantSize,
					 cp.length);

	if (readBytes != cp.length)
	{
		if (readBytes < 0)
			pg_fatal("could not read file \"%s\": %m", path);
		else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 path, readBytes, (Size) cp.length);
	}

	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", path);

	/* now verify the CRC */
	INIT_CRC32C(checksum);
	COMP_CRC32C(checksum,
				(char *) &cp + ReplicationSlotOnDiskNotChecksummedSize,
				ReplicationSlotOnDiskChecksummedSize);
	FIN_CRC32C(checksum);

	if (!EQ_CRC32C(checksum, cp.checksum))
		pg_fatal("checksum mismatch for replication slot file \"%s\": is %u, should be %u",
				 path, checksum, cp.checksum);

	MemSet(s_info, 0, sizeof(ReplicationSlotOnDisk));
	memcpy(s_info, &cp, sizeof(ReplicationSlotOnDisk));

	return true;
}

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"archive-command", required_argument, NULL, 'a'},
		{"dry-run", no_argument, NULL, 'd'},
		{"pgdata", required_argument, NULL, 'D'},
		{"ignore-replication-slots", no_argument, NULL, 'i'},
		{"verbose", no_argument, NULL, 'v'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int	option;
	int	optindex = 0;
	DIR	*dir;
	bool	crc_ok;
	XLogSegNo	segno;
	XLogRecPtr	oldest_restart_lsn;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_walcleaner"));
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(EXIT_SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_walcleaner (PostgreSQL) " PG_VERSION);
			exit(EXIT_SUCCESS);
		}
	}

	while ((option = getopt_long(argc, argv, "a:dD:v",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'a':
				archive_command = pstrdup(optarg);
				break;
			case 'd':
				dryrun = true;
				break;
			case 'D':
				data_dir = pstrdup(optarg);
				break;
			case 'i':
				ignore_replication_slots = true;
				break;
			case 'v':
				verbose = true;
				break;
			default:
				goto bad_argument;
		}
	}

	/* any non-option arguments? */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		goto bad_argument;
	}

	if (data_dir == NULL)
	{
		data_dir = getenv("PGDATA");

		/* If no data_dir was specified, and none could be found, error out */
		if (data_dir == NULL)
		{
			pg_log_error("no data directory specified");
			goto bad_argument;
		}
	}

	if (verbose)
		pg_log_info("data directory is \"%s\"", data_dir);

	/* check existence of data directory */
	dir = get_destination_dir(data_dir);
	close_destination_dir(dir, data_dir);

	if (chdir(data_dir) < 0)
		pg_fatal("could not change directory to \"%s\": %m", data_dir);

	/*
	 * XXX: should we check if the server isn't running by looking at
	 * postmaster.pid file? Because we don't want this tool to be running while
	 * the server is up as it might interfere when checkpointer removing the
	 * old WAL files. But, just the presence of postmaster.pid file doesn't
	 * guarantee that the server is up as server can leave the postmaster.pid
	 * file in case of crashes (for instance, kill -9 <<postmaster_pid>>).
	 *
	 * For now, let's leave the responsibility of running this tool only when
	 * the server isn't up to the user.
	 */

	/* get a copy of the control file */
	ControlFile = get_controlfile(data_dir, &crc_ok);
	if (!crc_ok)
	{
		pg_log_error("pg_control file contains invalid checksum\n");
		pg_log_error("Calculated CRC checksum does not match value stored in file.\n"
					 "Either the file is corrupt, or it has a different layout than this program\n"
					 "is expecting.\n\n");

		exit(EXIT_FAILURE);
	}

	/* set WAL segment size */
	WalSegSz = ControlFile->xlog_seg_size;

	if (!IsValidWalSegSize(WalSegSz))
	{
		pg_log_error("invalid WAL segment size found in pg_control file\n");
		pg_log_error("The WAL segment size stored in the file, %d byte(s), is not a power of two\n"
					 "between 1 MB and 1 GB.  The file is corrupt.\n\n",
					 WalSegSz);

		exit(EXIT_FAILURE);
	}

	/*
	 * Calculate name of the WAL file containing the latest checkpoint's REDO
	 * start point.
	 */
	XLByteToSeg(ControlFile->checkPointCopy.redo, segno, WalSegSz);
	XLogFileName(cutoff_wal_file, ControlFile->checkPointCopy.ThisTimeLineID,
				 segno, WalSegSz);

	if (verbose)
		pg_log_info("checkpoint's REDO WAL file name is \"%s\"",
					cutoff_wal_file);

	/*
	 * By default, keep WAL segments required by all the replication slots
	 * that were present at the time of crash.
	 *
	 * The replication slots must be dropped and recreated after the server
	 * is up when users choose to not keep WAL segments for them i.e.
	 * ignore_replication_slots is set to true.
	 */
	if (!ignore_replication_slots)
	{
		oldest_restart_lsn = process_replslots();

		/*
		 * If oldest restart_lsn is older than checkpoint's REDO start point,
		 * then re-compute the cutoff_wal_file.
		 */
		if (!XLogRecPtrIsInvalid(oldest_restart_lsn) &&
			oldest_restart_lsn < ControlFile->checkPointCopy.redo)
		{
			XLByteToSeg(oldest_restart_lsn, segno, WalSegSz);

			/*
			 * XXX: will any of replication slots clients stream from a
			 * timeline other than the server's insert timeline? How do we find
			 * the server's insert timeline id just before the crash?
			 *
			 * Using ThisTimeLineID from checkpoint may not be correct here,
			 * but we anyways don't use timeline id while deleting the WAL
			 * files, see comments in RemoveWALFiles().
			 */
			XLogFileName(cutoff_wal_file,
						 ControlFile->checkPointCopy.ThisTimeLineID,
						 segno, WalSegSz);

			if (verbose)
				pg_log_info("oldest WAL file required by all replication slots is \"%s\"",
							cutoff_wal_file);
		}
	}

	/*
	 * XXX: should we consider the WAL files on historical timelines as well?
	 * PrevTimeLineID, timeline history files etc.?
	 */

	/*
	 * XXX: should we consider deleting the partial WAL files, backup history
	 * files as well on standbys or PITR/restored server?
	 */

	pg_log_info("keeping WAL file \"%s\" and later", cutoff_wal_file);

	/* remove WAL files older than cutoff_wal_file */
	RemoveWALFiles();

	return EXIT_SUCCESS;

bad_argument:
	pg_log_error_hint("Try \"%s --help\" for more information.", progname);
	return EXIT_FAILURE;
}
