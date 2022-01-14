/*-------------------------------------------------------------------------
 *
 * pg_replslotdata.c - provides information about the replication slots
 * from $PGDATA/pg_replslot/<slot_name>.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_replslotdata/pg_replslotdata.c
 *-------------------------------------------------------------------------
 */
/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.  Hence this ugly hack.
 */
#define FRONTEND 1

#include "postgres.h"

#include <sys/stat.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "common/logging.h"
#include "common/string.h"
#include "getopt_long.h"
#include "pg_getopt.h"
#include "replication/slot_common.h"

static bool verbose = false;

static void process_replslots(void);
static void read_and_display_repl_slot(const char *name);

static void
usage(const char *progname)
{
	printf(_("%s displays information about the replication slots from $PGDATA/pg_replslot/<slot_name>.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION] [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -v, --verbose          write a lot of output\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}

static void
process_replslots(void)
{
	DIR		   *rsdir;
	struct dirent *rsde;
	uint32		cnt = 0;

	rsdir = opendir("pg_replslot");
	if (rsdir == NULL)
	{
		pg_log_error("could not open directory \"%s\": %m", "pg_replslot");
		exit(1);
	}

	/* XXX: comment here about the format spefiiers */
	printf("%-64s %9s %10s %11s %10s %12s %21s %21s %21s %21s %10s %20s\n"
		   "%-64s %9s %10s %11s %10s %12s %21s %21s %21s %21s %10s %20s\n",
		   "slot_name", "slot_type", "datoid", "persistency", "xmin", "catalog_xmin", "restart_lsn", "invalidated_at", "confirmed_flush", "two_phase_at", "two_phase", "plugin",
		   "---------", "---------", "------", "-----------", "----", "------------", "-----------", "--------------", "---------------", "------------", "---------", "------");

	while (errno = 0, (rsde = readdir(rsdir)) != NULL)
	{
		struct stat statbuf;
		char		path[MAXPGPATH];

		if (strcmp(rsde->d_name, ".") == 0 ||
			strcmp(rsde->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "pg_replslot/%s", rsde->d_name);

		/* we're only creating directories here, skip if it's not our's */
		if (lstat(path, &statbuf) == 0 && !S_ISDIR(statbuf.st_mode))
			continue;

		/* we crashed while a slot was being setup or deleted, clean up */
		if (pg_str_endswith(rsde->d_name, ".tmp"))
		{
			pg_log_warning("server was crashed while the slot \"%s\" was being setup or deleted",
						   rsde->d_name);
			continue;
		}

		/* looks like a slot in a normal state, restore */
		read_and_display_repl_slot(rsde->d_name);
		cnt++;
	}

	if (errno)
	{
		pg_log_error("could not read directory \"%s\": %m", "pg_replslot");
		exit(1);
	}

	if (cnt == 0)
	{
		pg_log_info("no replication slots were found");
		exit(0);
	}

	if (closedir(rsdir))
	{
		pg_log_error("could not close directory \"%s\": %m", "pg_replslot");
		exit(1);
	}
}

static void
read_and_display_repl_slot(const char *name)
{
	ReplicationSlotOnDisk cp;
	char		slotdir[MAXPGPATH];
	char		path[MAXPGPATH];
	char		restart_lsn[NAMEDATALEN];
	char		invalidated_at[NAMEDATALEN];
	char		confirmed_flush[NAMEDATALEN];
	char		two_phase_at[NAMEDATALEN];
	char		persistency[NAMEDATALEN];
	int			fd;
	int			readBytes;
	pg_crc32c	checksum;

	/* delete temp file if it exists */
	sprintf(slotdir, "pg_replslot/%s", name);
	sprintf(path, "%s/state.tmp", slotdir);

	fd = open(path, O_RDONLY | PG_BINARY, 0);

	if (fd > 0)
	{
		pg_log_error("found temporary state file \"%s\": %m", path);
		exit(1);
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
	{
		pg_log_error("could not open file \"%s\": %m", path);
		exit(1);
	}

	if (verbose)
		pg_log_info("reading version independent replication slot state file");

	/* read part of statefile that's guaranteed to be version independent */
	readBytes = read(fd, &cp, ReplicationSlotOnDiskConstantSize);
	if (readBytes != ReplicationSlotOnDiskConstantSize)
	{
		if (readBytes < 0)
		{
			pg_log_error("could not read file \"%s\": %m", path);
			exit(1);
		}
		else
		{
			pg_log_error("could not read file \"%s\": read %d of %zu",
						 path, readBytes,
						 (Size) ReplicationSlotOnDiskConstantSize);
			exit(1);
		}
	}

	/* verify magic */
	if (cp.magic != SLOT_MAGIC)
	{
		pg_log_error("replication slot file \"%s\" has wrong magic number: %u instead of %u",
					 path, cp.magic, SLOT_MAGIC);
		exit(1);
	}

	/* verify version */
	if (cp.version != SLOT_VERSION)
	{
		pg_log_error("replication slot file \"%s\" has unsupported version %u",
					 path, cp.version);
		exit(1);
	}

	/* boundary check on length */
	if (cp.length != ReplicationSlotOnDiskV2Size)
	{
		pg_log_error("replication slot file \"%s\" has corrupted length %u",
					 path, cp.length);
		exit(1);
	}

	if (verbose)
		pg_log_info("reading the entire replication slot state file");

	/* now that we know the size, read the entire file */
	readBytes = read(fd,
					 (char *) &cp + ReplicationSlotOnDiskConstantSize,
					 cp.length);
	if (readBytes != cp.length)
	{
		if (readBytes < 0)
		{
			pg_log_error("could not read file \"%s\": %m", path);
			exit(1);
		}
		else
		{
			pg_log_error("could not read file \"%s\": read %d of %zu",
						 path, readBytes, (Size) cp.length);
			exit(1);
		}
	}

	if (close(fd) != 0)
	{
		pg_log_error("could not close file \"%s\": %m", path);
		exit(1);
	}

	/* now verify the CRC */
	INIT_CRC32C(checksum);
	COMP_CRC32C(checksum,
				(char *) &cp + ReplicationSlotOnDiskNotChecksummedSize,
				ReplicationSlotOnDiskChecksummedSize);
	FIN_CRC32C(checksum);

	if (!EQ_CRC32C(checksum, cp.checksum))
	{
		pg_log_error("checksum mismatch for replication slot file \"%s\": is %u, should be %u",
					 path, checksum, cp.checksum);
		exit(1);
	}

	sprintf(restart_lsn, "%X/%X", LSN_FORMAT_ARGS(cp.slotdata.restart_lsn));
	sprintf(invalidated_at, "%X/%X", LSN_FORMAT_ARGS(cp.slotdata.invalidated_at));
	sprintf(confirmed_flush, "%X/%X", LSN_FORMAT_ARGS(cp.slotdata.confirmed_flush));
	sprintf(two_phase_at, "%X/%X", LSN_FORMAT_ARGS(cp.slotdata.two_phase_at));

	if (cp.slotdata.persistency == RS_PERSISTENT)
		sprintf(persistency, "persistent");
	else if (cp.slotdata.persistency == RS_EPHEMERAL)
		sprintf(persistency, "ephemeral");
	else if (cp.slotdata.persistency == RS_TEMPORARY)
		sprintf(persistency, "temporary");

	/* display the slot information */
	printf("%-64s %9s %10u %11s %10u %12u %21s %21s %21s %21s %10d %20s\n",
		   NameStr(cp.slotdata.name),
		   cp.slotdata.database == InvalidOid ? "physical" : "logical",
		   cp.slotdata.database,
		   persistency,
		   cp.slotdata.xmin,
		   cp.slotdata.catalog_xmin,
		   restart_lsn,
		   invalidated_at,
		   confirmed_flush,
		   two_phase_at,
		   cp.slotdata.two_phase,
		   NameStr(cp.slotdata.plugin));
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
	const char *progname;
	int			c;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_replslotdata"));
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_replslotdata (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:v", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'D':
				DataDir = optarg;
				break;
			case 'v':
				verbose = true;
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
	}

	/* complain if any arguments remain */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (DataDir == NULL)
	{
		pg_log_error("no data directory specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (verbose)
		pg_log_info("data directory: \"%s\"", DataDir);

	if (chdir(DataDir) < 0)
	{
		pg_log_error("could not change directory to \"%s\": %m",
					 DataDir);
		exit(1);
	}

	process_replslots();

	return 0;
}
