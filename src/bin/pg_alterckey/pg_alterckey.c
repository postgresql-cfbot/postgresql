/*-------------------------------------------------------------------------
 *
 * pg_alterckey.c
 *    A utility to change the cluster key (key encryption key, KEK)
 *    used for cluster file encryption.  The KEK wrap data encryption
 *    keys (DEK).
 *
 * The theory of operation is fairly simple:
 *    1. Create lock file
 *    2. Retrieve current and new cluster key using the supplied
 *       commands.
 *    3. Revert any failed alter operation.
 *    4. Create a "new" directory
 *    5. Unwrap each DEK in "live" using the old KEK
 *    6. Wrap each DEK using the new KEK and write it to "new"
 *    7. Rename "live" to "old"
 *    8. Rename "new" to "live"
 *    9. Remove "old"
 *   10. Remove lock file
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_alterckey/pg_alterckey.c
 *
 *-------------------------------------------------------------------------
 */


#define FRONTEND 1

#include "postgres_fe.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/restricted_token.h"
#include "common/logging.h"
#include "crypto/kmgr.h"
#include "getopt_long.h"
#include "pg_getopt.h"

typedef enum
{
	SUCCESS_EXIT = 0,
	ERROR_EXIT,
	RMDIR_EXIT,
	REPAIR_EXIT
} exit_action;

#define MAX_WRAPPED_KEY_LENGTH 64

static int	lock_fd = -1;
static bool pass_terminal_fd = false;
int			terminal_fd = -1;
static bool repair_mode = false;
static char *old_cluster_key_cmd = NULL,
		   *new_cluster_key_cmd = NULL;
static char old_cluster_key[KMGR_CLUSTER_KEY_MAX_LEN],
	new_cluster_key[KMGR_CLUSTER_KEY_MAX_LEN];
static CryptoKey data_key;
unsigned char in_key[MAX_WRAPPED_KEY_LENGTH],
			out_key[MAX_WRAPPED_KEY_LENGTH];
int			in_klen,
			out_klen;
static char top_path[MAXPGPATH],
			pid_path[MAXPGPATH],
			live_path[MAXPGPATH],
			new_path[MAXPGPATH],
			old_path[MAXPGPATH];

static char *DataDir = NULL;
static const char *progname;
static ControlFileData *ControlFile;

static void create_lockfile(void);
static void recover_failure(void);
static void retrieve_cluster_keys(void);
static void bzero_keys_and_exit(exit_action action);
static void reencrypt_data_keys(void);
static void install_new_keys(void);

static uint64 hex_decode(const char *src, size_t len, char *dst);


static void
usage(const char *progname)
{
	printf(_("%s changes the cluster key of a PostgreSQL database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION] old_cluster_key_command new_cluster_key_command [DATADIR]\n"), progname);
	printf(_("  %s [repair_option] [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -R, --authprompt       prompt for a passphrase or PIN\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nRepair options:\n"));
	printf(_("  -r, --repair           repair previous failure\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}


int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"authprompt", required_argument, NULL, 'R'},
		{"repair", required_argument, NULL, 'r'},
		{"pgdata", required_argument, NULL, 'D'},
		{NULL, 0, NULL, 0}
	};

	int			c;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_alterckey"));
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
			puts("pg_alterckey (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	/* check for -r/-R */
	while ((c = getopt_long(argc, argv, "D:rR", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'D':
				DataDir = optarg;
				break;
			case 'r':
				repair_mode = true;
				break;
			case 'R':
				pass_terminal_fd = true;
				break;
			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	if (!repair_mode)
	{
		/* get cluster key commands */
		if (optind < argc)
			old_cluster_key_cmd = argv[optind++];
		else
		{
			pg_log_error("missing old_cluster_key_command");
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit(1);
		}

		if (optind < argc)
			new_cluster_key_cmd = argv[optind++];
		else
		{
			pg_log_error("missing new_cluster_key_command");
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
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
			pg_log_error("no data directory specified");
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Disallow running as root because we create directories in PGDATA
	 */
#ifndef WIN32
	if (geteuid() == 0)
	{
		pg_log_error("%s: cannot be run as root\n"
					 "Please log in (using, e.g., \"su\") as the "
					 "(unprivileged) user that will\n"
					 "own the server process.\n",
					 progname);
		exit(1);
	}
#endif

	ControlFile = get_controlfile(DataDir, NULL);
	
	get_restricted_token();

	/* Set mask based on PGDATA permissions */
	if (!GetDataDirectoryCreatePerm(DataDir))
	{
		pg_log_error("could not read permissions of directory \"%s\": %m",
					 DataDir);
		exit(1);
	}

	umask(pg_mode_mask);

	snprintf(top_path, sizeof(top_path), "%s/%s", DataDir, KMGR_DIR);
	snprintf(pid_path, sizeof(pid_path), "%s/%s", DataDir, KMGR_DIR_PID);
	snprintf(live_path, sizeof(live_path), "%s/%s", DataDir, LIVE_KMGR_DIR);
	snprintf(new_path, sizeof(new_path), "%s/%s", DataDir, NEW_KMGR_DIR);
	snprintf(old_path, sizeof(old_path), "%s/%s", DataDir, OLD_KMGR_DIR);

	/* Complain if any arguments remain */
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

	create_lockfile();

	recover_failure();

	if (!repair_mode)
	{
		retrieve_cluster_keys();
		reencrypt_data_keys();
		install_new_keys();
	}

#ifndef WIN32
	/* remove file system reference to file */
	if (unlink(pid_path) < 0)
	{
		pg_log_error("could not delete lock file \"%s\": %m", KMGR_DIR_PID);
		exit(1);
	}
#endif

	close(lock_fd);

	bzero_keys_and_exit(SUCCESS_EXIT);
}

/* Create a lock file;  this prevents almost all cases of concurrent access */
void
create_lockfile(void)
{
	struct stat buffer;
	char		lock_pid_str[20];

	if (stat(top_path, &buffer) != 0 || !S_ISDIR(buffer.st_mode))
	{
		pg_log_error("cluster file encryption directory \"%s\" is missing;  is it enabled?", KMGR_DIR_PID);
		fprintf(stderr, _("Exiting with no changes made.\n"));
		exit(1);
	}

	/* Does a lockfile exist? */
	if ((lock_fd = open(pid_path, O_RDONLY, 0)) != -1)
	{
		int			lock_pid;
		int			len;

		/* read the PID */
		if ((len = read(lock_fd, lock_pid_str, sizeof(lock_pid_str) - 1)) == 0)
		{
			pg_log_error("cannot read pid from lock file \"%s\": %m", KMGR_DIR_PID);
			fprintf(stderr, _("Exiting with no changes made.\n"));
			exit(1);
		}
		lock_pid_str[len] = '\0';

		if ((lock_pid = atoi(lock_pid_str)) == 0)
		{
			pg_log_error("invalid pid in lock file \"%s\": %m", KMGR_DIR_PID);
			fprintf(stderr, _("Exiting with no changes made.\n"));
			exit(1);
		}

		/* Is the PID running? */
		if (kill(lock_pid, 0) == 0)
		{
			pg_log_error("active process %d currently holds a lock on this operation, recorded in \"%s\"",
						 lock_pid, KMGR_DIR_PID);
			fprintf(stderr, _("Exiting with no changes made.\n"));
			exit(1);
		}

		close(lock_fd);

		if (repair_mode)
			printf("old lock file removed\n");

		/* ----------
		 * pid is no longer running, so remove the lock file.
		 * This is not 100% safe from concurrent access, e.g.:
		 *
		 *    process 1 exits and leaves stale lock file
		 *    process 2 checks stale lock file of process 1
		 *    process 3 checks stale lock file of process 1
		 *    process 2 remove the lock file of process 1
		 *    process 4 creates a lock file
		 *    process 3 remove the lock file of process 4
		 *    process 5 creates a lock file
		 *
		 * The sleep(2) helps with this since it reduces the likelihood
		 * a process that did an unlock will interfere with another unlock
		 * process.  We could ask users to remove the lock, but that seems
		 * even more error-prone, especially since this might happen
		 * on server start.  Many PG tools seem to have problems with
		 * concurrent access.
		 * ----------
		 */
		unlink(pid_path);

		/* Sleep to reduce the likelihood of concurrent unlink */
		pg_usleep(2000000L);	/* 2 seconds */
	}

	/* Create our own lockfile? */
#ifndef WIN32
	lock_fd = open(pid_path, O_RDWR | O_CREAT | O_EXCL, pg_file_create_mode);
#else
	/* delete on close */
	lock_fd = open(pid_path, O_RDWR | O_CREAT | O_EXCL | O_TEMPORARY,
				   pg_file_create_mode);
#endif

	if (lock_fd == -1)
	{
		if (errno == EEXIST)
			pg_log_error("an active process currently holds a lock on this operation, recorded in \"%s\"",
						 KMGR_DIR_PID);
		else
			pg_log_error("unable to create lock file \"%s\": %m", KMGR_DIR_PID);
		fprintf(stderr, _("Exiting with no changes made.\n"));
		exit(1);
	}

	snprintf(lock_pid_str, sizeof(lock_pid_str), "%d\n", getpid());
	if (write(lock_fd, lock_pid_str, strlen(lock_pid_str)) != strlen(lock_pid_str))
	{
		pg_log_error("could not write pid to lock file \"%s\": %m", KMGR_DIR_PID);
		fprintf(stderr, _("Exiting with no changes made.\n"));
		exit(1);
	}
}

/*
 * ----------
 *	recover_failure
 *
 * A previous pg_alterckey might have failed, so it might need recovery.
 * The normal operation is:
 * 1.     reencrypt  LIVE_KMGR_DIR -> NEW_KMGR_DIR
 * 2.     rename     KMGR_DIR      -> OLD_KMGR_DIR
 * 3.     rename     NEW_KMGR_DIR  -> LIVE_KMGR_DIR
 *        remove     OLD_KMGR_DIR
 *
 * There are eight possible directory configurations:
 *
 *                            LIVE_KMGR_DIR   NEW_KMGR_DIR    OLD_KMGR_DIR
 *
 * Normal:
 * 0. normal                       X
 * 1. remove new                   X               X
 * 2. install new                                  X               X
 * 3. remove old                   X                               X
 *
 * Abnormal:
 *    fatal
 *    restore old                                                  X
 *    install new                                  X
 *    remove old and new           X               X               X
 *
 * We don't handle the abnormal cases, just report an error.
 * ----------
 */
static void
recover_failure(void)
{
	struct stat buffer;
	bool		is_live,
				is_new,
				is_old;

	is_live = !stat(live_path, &buffer);
	is_new = !stat(new_path, &buffer);
	is_old = !stat(old_path, &buffer);

	/* normal #0 */
	if (is_live && !is_new && !is_old)
	{
		if (repair_mode)
			printf("repair unnecessary\n");
		return;
	}
	/* remove new #1 */
	else if (is_live && is_new && !is_old)
	{
		if (!rmtree(new_path, true))
		{
			pg_log_error("unable to remove new directory \"%s\": %m", NEW_KMGR_DIR);
			fprintf(stderr, _("Exiting with no changes made.\n"));
			exit(1);
		}
		printf(_("removed files created during previously aborted alter operation\n"));
		return;
	}
	/* install new #2 */
	else if (!is_live && is_new && is_old)
	{
		if (rename(new_path, live_path) != 0)
		{
			pg_log_error("unable to rename directory \"%s\" to \"%s\": %m",
						 NEW_KMGR_DIR, LIVE_KMGR_DIR);
			fprintf(stderr, _("Exiting with no changes made.\n"));
			exit(1);
		}
		printf(_("Installed new cluster password supplied in previous alter operation\n"));
		return;
	}
	/* remove old #3 */
	else if (is_live && !is_new && is_old)
	{
		if (!rmtree(old_path, true))
		{
			pg_log_error("unable to remove old directory \"%s\": %m", OLD_KMGR_DIR);
			fprintf(stderr, _("Exiting with no changes made.\n"));
			exit(1);
		}
		printf(_("Removed old files invalidated during previous alter operation\n"));
		return;
	}
	else
	{
		pg_log_error("cluster file encryption directory \"%s\" is in an abnormal state and cannot be processed",
					 KMGR_DIR);
		fprintf(stderr, _("Exiting with no changes made.\n"));
		exit(1);
	}
}

/* Retrieve old and new cluster keys */
void
retrieve_cluster_keys(void)
{
	int			cluster_key_len;
	char		cluster_key_hex[ALLOC_KMGR_CLUSTER_KEY_MAX_LEN];

	/*
	 * If we have been asked to pass an open file descriptor to the user
	 * terminal to the commands, set one up.
	 */
	if (pass_terminal_fd)
	{
#ifndef WIN32
		terminal_fd = open("/dev/tty", O_RDWR, 0);
#else
		terminal_fd = open("CONOUT$", O_RDWR, 0);
#endif
		if (terminal_fd < 0)
		{
			pg_log_error(_("%s: could not open terminal: %s\n"),
						 progname, strerror(errno));
			exit(1);
		}
	}

	/* Get old key encryption key from the cluster key command */
	cluster_key_len = kmgr_run_cluster_key_command(old_cluster_key_cmd,
												   (char *) cluster_key_hex,
												   ALLOC_KMGR_CLUSTER_KEY_MAX_LEN,
												   live_path, terminal_fd);
	if (hex_decode(cluster_key_hex, cluster_key_len,
					  (char *) old_cluster_key) !=
		KMGR_CLUSTER_KEY_LEN(ControlFile->file_encryption_method))
	{
		pg_log_error("cluster key must be at %d hex bytes", KMGR_CLUSTER_KEY_LEN(ControlFile->file_encryption_method) * 2);
		bzero_keys_and_exit(ERROR_EXIT);
	}

	/*
	 * Create new key directory here in case the new cluster key command needs
	 * it to exist.
	 */
	if (mkdir(new_path, pg_dir_create_mode) != 0)
	{
		pg_log_error("unable to create new cluster key directory \"%s\": %m", NEW_KMGR_DIR);
		bzero_keys_and_exit(ERROR_EXIT);
	}

	/* Get new key */
	cluster_key_len = kmgr_run_cluster_key_command(new_cluster_key_cmd,
												   (char *) cluster_key_hex,
												   ALLOC_KMGR_CLUSTER_KEY_MAX_LEN,
												   new_path, terminal_fd);
	if (hex_decode(cluster_key_hex, cluster_key_len,
					  (char *) new_cluster_key) !=
		KMGR_CLUSTER_KEY_LEN(ControlFile->file_encryption_method))
	{
		pg_log_error("cluster key must be at %d hex bytes", KMGR_CLUSTER_KEY_LEN(ControlFile->file_encryption_method) * 2);
		bzero_keys_and_exit(ERROR_EXIT);
	}

	if (pass_terminal_fd)
		close(terminal_fd);

	/* output newline */
	puts("");

	if (memcmp(old_cluster_key, new_cluster_key, KMGR_CLUSTER_KEY_LEN(ControlFile->file_encryption_method)) == 0)
	{
		pg_log_error("cluster keys are identical, exiting\n");
		bzero_keys_and_exit(RMDIR_EXIT);
	}
}

/* Decrypt old keys encrypted with old pass phrase and reencrypt with new one */
void
reencrypt_data_keys(void)
{
	PgCipherCtx *old_ctx,
			   *new_ctx;

	old_ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP,
								   (unsigned char *) old_cluster_key,
								   KMGR_KEK_KEY_LEN, false);
	if (!old_ctx)
		pg_log_error("could not initialize encryption context");

	new_ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP,
								   (unsigned char *) new_cluster_key,
								   KMGR_KEK_KEY_LEN, true);
	if (!new_ctx)
		pg_log_error("could not initialize encryption context");

	for (int id = 0; id < KMGR_NUM_DATA_KEYS; id++)
	{
		char		src_path[MAXPGPATH],
					dst_path[MAXPGPATH];
		int			src_fd,
					dst_fd;
		int			len;
		struct stat st;

		CryptoKeyFilePath(src_path, live_path, id);
		CryptoKeyFilePath(dst_path, new_path, id);

		if ((src_fd = open(src_path, O_RDONLY | PG_BINARY, 0)) < 0)
		{
			pg_log_error("could not open file \"%s\": %m", src_path);
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		if ((dst_fd = open(dst_path, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY,
						   pg_file_create_mode)) < 0)
		{
			pg_log_error("could not open file \"%s\": %m", dst_path);
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		if (fstat(src_fd, &st))
		{
			pg_log_error("could not stat file \"%s\": %m", src_path);
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		in_klen = st.st_size;

		if (in_klen > MAX_WRAPPED_KEY_LENGTH)
		{
			pg_log_error("invalid wrapped key length (%d) for file \"%s\"", in_klen, src_path);
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		/* Read the source key */
		len = read(src_fd, in_key, in_klen);
		if (len != in_klen)
		{
			if (len < 0)
				pg_log_error("could read file \"%s\": %m", src_path);
			else
				pg_log_error("could read file \"%s\": read %d of %u",
							 src_path, len, in_klen);
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		/* decrypt with old key */
		if (!kmgr_unwrap_data_key(old_ctx, in_key, in_klen, &data_key))
		{
			pg_log_error("incorrect old key specified");
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		if (KMGR_MAX_KEY_LEN_BYTES + pg_cipher_blocksize(new_ctx) > MAX_WRAPPED_KEY_LENGTH)
		{
			pg_log_error("invalid max wrapped key length");
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		/* encrypt with new key */
		if (!kmgr_wrap_data_key(new_ctx, &data_key, out_key, &out_klen))
		{
			pg_log_error("could not encrypt new key");
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		/* Write to the dest key */
		len = write(dst_fd, out_key, out_klen);
		if (len != out_klen)
		{
			pg_log_error("could not write fie \"%s\"", dst_path);
			bzero_keys_and_exit(RMDIR_EXIT);
		}

		close(src_fd);
		close(dst_fd);
	}

	/* The cluster key is correct, free the cipher context */
	pg_cipher_ctx_free(old_ctx);
	pg_cipher_ctx_free(new_ctx);
}

/* Install new keys */
void
install_new_keys(void)
{
	/*
	 * Issue fsync's so key rotation is less likely to be left in an
	 * inconsistent state in case of a crash during this operation.
	 */
	
	if (rename(live_path, old_path) != 0)
	{
		pg_log_error("unable to rename directory \"%s\" to \"%s\": %m",
					 LIVE_KMGR_DIR, OLD_KMGR_DIR);
		bzero_keys_and_exit(RMDIR_EXIT);
	}
	sync_dir_recurse(top_path, DATA_DIR_SYNC_METHOD_FSYNC);

	if (rename(new_path, live_path) != 0)
	{
		pg_log_error("unable to rename directory \"%s\" to \"%s\": %m",
					 NEW_KMGR_DIR, LIVE_KMGR_DIR);
		bzero_keys_and_exit(REPAIR_EXIT);
	}
	sync_dir_recurse(top_path, DATA_DIR_SYNC_METHOD_FSYNC);

	if (!rmtree(old_path, true))
	{
		pg_log_error("unable to remove old directory \"%s\": %m", OLD_KMGR_DIR);
		bzero_keys_and_exit(REPAIR_EXIT);
	}
	sync_dir_recurse(top_path, DATA_DIR_SYNC_METHOD_FSYNC);
}

/* Erase memory and exit */
void
bzero_keys_and_exit(exit_action action)
{
	explicit_bzero(old_cluster_key, sizeof(old_cluster_key));
	explicit_bzero(new_cluster_key, sizeof(new_cluster_key));

	explicit_bzero(in_key, sizeof(in_key));
	explicit_bzero(&data_key, sizeof(data_key));
	explicit_bzero(out_key, sizeof(out_key));

	if (action == RMDIR_EXIT)
	{
		if (!rmtree(new_path, true))
			pg_log_error("unable to remove new directory \"%s\": %m", NEW_KMGR_DIR);
		printf("Re-running pg_alterckey to repair might be needed before the next server start\n");
		exit(1);
	}
	else if (action == REPAIR_EXIT)
	{
		unlink(pid_path);
		printf("Re-running pg_alterckey to repair might be needed before the next server start\n");
	}

	/* return 0 or 1 */
	exit(action != SUCCESS_EXIT);
}

/*
 * HEX
 */

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static inline char
get_hex(const char *cp)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = hexlookup[c];

	if (res < 0)
		pg_fatal("invalid hexadecimal digit: \"%s\"",cp);

	return (char) res;
}

static uint64
hex_decode(const char *src, size_t len, char *dst)
{
	const char *s,
			   *srcend;
	char		v1,
				v2,
			   *p;

	srcend = src + len;
	s = src;
	p = dst;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		v1 = get_hex(s) << 4;
		s++;
		if (s >= srcend)
			pg_fatal("invalid hexadecimal data: odd number of digits");

		v2 = get_hex(s);
		s++;
		*p++ = v1 | v2;
	}

	return p - dst;
}
