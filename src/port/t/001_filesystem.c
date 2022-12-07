/*
 * Tests for our partial implementations of POSIX-style filesystem APIs, for
 * Windows.
 *
 * Currently, have_posix_unlink_semantics is expected to be true on all Unix
 * systems and all Windows 10-based operatings using NTFS, and false on other
 * Windows ReFS and SMB filesystems.
 */

#include "c.h"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "lib/pg_tap.h"
#include "port/pg_iovec.h"

/*
 * Make a path under the tmp_check directory.  TESTDATADIR is expected to
 * contain an absolute path.
 */
static void
make_path(char *buffer, const char *name)
{
	const char *directory;

	directory = getenv("TESTDATADIR");
	PG_REQUIRE(directory);

	snprintf(buffer, MAXPGPATH, "%s/%s", directory, name);
}

/*
 * Check if a directory contains a given entry, according to readdir().
 */
static bool
directory_contains(const char *directory_path, const char *name)
{
	char		directory_full_path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	bool		saw_name = false;

	make_path(directory_full_path, directory_path);
	PG_REQUIRE_SYS((dir = opendir(directory_full_path)));
	errno = 0;
	while ((de = readdir(dir)))
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;
		if (strcmp(de->d_name, name) == 0)
			saw_name = true;
	}
	PG_REQUIRE_SYS(errno == 0);
	PG_REQUIRE_SYS(closedir(dir) == 0);

	return saw_name;
}

#ifdef WIN32
/*
 * Make all slashes lean one way, to normalize paths for comparisons on Windows.
 */
static void
normalize_slashes(char *path)
{
	while (*path)
	{
		if (*path == '/')
			*path = '\\';
		path++;
	}
}

typedef struct apc_trampoline_data
{
	HANDLE		timer;
	void		(*function) (void *);
	void	   *data;
}			apc_trampoline_data;

static void CALLBACK
apc_trampoline(void *data, DWORD low, DWORD high)
{
	apc_trampoline_data *x = data;

	x->function(x->data);
	CloseHandle(x->timer);
	free(data);
}

/*
 * Schedule a Windows APC callback.  The pg_usleep() call inside the
 * sleep/retry loops in the wrappers under test is interruptible by APCs,
 * causing it to run the procedure and return early.
 */
static void
run_async_procedure_after_delay(void (*p) (void *), void *data, int milliseconds)
{
	LARGE_INTEGER delay_time = {.LowPart = -10 * milliseconds};
	apc_trampoline_data *x;
	HANDLE		timer;

	timer = CreateWaitableTimer(NULL, false, NULL);
	PG_REQUIRE(timer);

	x = malloc(sizeof(*x));
	PG_REQUIRE_SYS(x);
	x->timer = timer;
	x->function = p;
	x->data = data;

	PG_REQUIRE(SetWaitableTimer(timer, &delay_time, 0, apc_trampoline, x, false));
}

static void
close_fd(void *data)
{
	int		   *fd = data;

	PG_REQUIRE_SYS(close(*fd) == 0);
}

static void
close_handle(void *data)
{
	HANDLE		handle = data;

	PG_REQUIRE(CloseHandle(handle));
}
#endif

/*
 * Tests of directory entry manipulation.
 */
static void
filesystem_metadata_tests(void)
{
	int			fd;
	int			fd2;
	char		path[MAXPGPATH];
	char		path2[MAXPGPATH];
	char		path3[MAXPGPATH];
	struct stat statbuf;
	ssize_t		size;
	bool		have_posix_unlink_semantics;
#ifdef WIN32
	HANDLE		handle;
#endif

	/*
	 * Current versions of Windows 10 have "POSIX semantics" when unlinking
	 * files, meaning that unlink() and rename() remove the directory entry
	 * synchronously, just like Unix.  At least some server OSes still seem to
	 * have the traditional Windows behavior, where directory entries remain
	 * in STATUS_DELETE_PENDING state, visible but unopenable, until all file
	 * handles are closed.  We have a lot of code paths to deal with the older
	 * asynchronous behavior, and tests for those here.  Before we go further,
	 * determine which behavior to expect.  Behavior may also vary on non-NTFS
	 * filesystems.
	 */
	make_path(path, "test.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_REQUIRE_SYS(fd >= 0);
	PG_REQUIRE_SYS(unlink(path) == 0);
	fd2 = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_REQUIRE_SYS(fd2 >= 0 || errno == EEXIST);
	if (fd2 >= 0)
	{
		/* Expected behavior on Unix and some Windows 10 releases. */
		PG_EXPECT_SYS(unlink(path) == 0, "POSIX unlink semantics detected: cleaning up");
		have_posix_unlink_semantics = true;
		PG_REQUIRE_SYS(close(fd2) == 0);
	}
	else
	{
		/* Our open wrapper fails with EEXIST for O_EXCL in this case. */
		PG_EXPECT_SYS(errno == EEXIST,
					  "traditional Windows unlink semantics detected: O_EXCL -> EEXIST");
		have_posix_unlink_semantics = false;
	}
	PG_REQUIRE_SYS(close(fd) == 0);

	/* Set up test directory structure. */

	make_path(path, "dir1");
	PG_REQUIRE_SYS(mkdir(path, 0777) == 0);
	make_path(path, "dir1/my_subdir");
	PG_REQUIRE_SYS(mkdir(path, 0777) == 0);
	make_path(path, "dir1/test.txt");
	fd = open(path, O_CREAT | O_RDWR | PG_BINARY, 0777);
	PG_REQUIRE_SYS(fd >= 0);
	PG_REQUIRE_SYS(write(fd, "hello world\n", 12) == 12);
	PG_REQUIRE_SYS(close(fd) == 0);

	/* Tests for symlink()/readlink(). */

	make_path(path, "dir999/my_symlink");	/* name of symlink to create */
	make_path(path2, "dir1/my_subdir"); /* name of directory it will point to */
	PG_EXPECT(symlink(path2, path) == -1, "symlink fails on missing parent");
	PG_EXPECT_EQ(errno, ENOENT);

	make_path(path, "dir1/my_symlink"); /* name of symlink to create */
	make_path(path2, "dir1/my_subdir"); /* name of directory it will point to */
	PG_EXPECT_SYS(symlink(path2, path) == 0, "create symlink");

	size = readlink(path, path3, sizeof(path3));
	PG_EXPECT_SYS(size != -1, "readlink succeeds");
	PG_EXPECT_EQ(size, strlen(path2), "readlink reports expected size");
	path3[Max(size, 0)] = '\0';
#ifdef WIN32
	normalize_slashes(path2);
	normalize_slashes(path3);
#endif
	PG_EXPECT_EQ_STR(path2, path3, "readlink reports expected target");

	PG_EXPECT(readlink("does-not-exist", path3, sizeof(path3)) == -1, "readlink fails on missing path");
	PG_EXPECT_EQ(errno, ENOENT);

	/*
	 * Checks that we don't corrupt non-drive-absolute paths when peforming
	 * internal conversions.
	 */

	/*
	 * Typical case: Windows drive absolute.  This should also be accepted on
	 * POSIX systems, because they are required not to validate the target
	 * string as a path.
	 */
	make_path(path2, "my_symlink");
	PG_EXPECT_SYS(symlink("c:\\foo", path2) == 0);
	size = readlink(path2, path3, sizeof(path3));
	PG_EXPECT_SYS(size != -1, "readlink succeeds");
	PG_EXPECT_EQ(size, 6);
	path3[Max(size, 0)] = '\0';
	PG_EXPECT_EQ_STR(path3, "c:\\foo");
	PG_EXPECT_SYS(unlink(path2) == 0);

	/*
	 * Drive absolute given in full NT format will be stripped on round-trip
	 * through our Windows emulations.
	 */
	make_path(path2, "my_symlink");
	PG_EXPECT_SYS(symlink("\\??\\c:\\foo", path2) == 0);
	size = readlink(path2, path3, sizeof(path3));
	PG_EXPECT_SYS(size != -1, "readlink succeeds");
	path3[Max(size, 0)] = '\0';
#ifdef WIN32
	PG_EXPECT_EQ(size, 6);
	PG_EXPECT_EQ_STR(path3, "c:\\foo");
#else
	PG_EXPECT_EQ(size, 10);
	PG_EXPECT_EQ_STR(path3, "\\??\\c:\\foo");
#endif
	PG_EXPECT_SYS(unlink(path2) == 0);

	/*
	 * Anything that doesn't look like the NT pattern that symlink() creates
	 * will be returned verbatim.  This will allow our stat() to handle paths
	 * that were not created by symlink().
	 */
	make_path(path2, "my_symlink");
	PG_EXPECT_SYS(symlink("\\??\\Volume1234", path2) == 0);
	size = readlink(path2, path3, sizeof(path3));
	PG_EXPECT_SYS(size != -1, "readlink succeeds");
	PG_EXPECT_EQ(size, 14);
	path3[Max(size, 0)] = '\0';
	PG_EXPECT_EQ_STR(path3, "\\??\\Volume1234");
	PG_EXPECT_SYS(unlink(path2) == 0);

	/* Tests for fstat(). */

	make_path(path, "dir1/test.txt");
	fd = open(path, O_RDWR, 0777);
	PG_REQUIRE_SYS(fd >= 0);
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(fstat(fd, &statbuf) == 0, "fstat regular file");
	PG_EXPECT(S_ISREG(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, 12);
	PG_EXPECT_EQ(statbuf.st_nlink, 1);
	PG_REQUIRE_SYS(close(fd) == 0);

	/* Tests for stat(). */

	PG_EXPECT(stat("does-not-exist.txt", &statbuf) == -1, "stat missing file fails");
	PG_EXPECT_EQ(errno, ENOENT);

	make_path(path, "dir1/test.txt");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(stat(path, &statbuf) == 0, "stat regular file");
	PG_EXPECT(S_ISREG(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, 12);
	PG_EXPECT_EQ(statbuf.st_nlink, 1);

	make_path(path, "dir1/my_subdir");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(stat(path, &statbuf) == 0, "stat directory");
	PG_EXPECT(S_ISDIR(statbuf.st_mode));

	make_path(path, "dir1/my_symlink");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(stat(path, &statbuf) == 0, "stat symlink");
	PG_EXPECT(S_ISDIR(statbuf.st_mode));

	/* Recursive symlinks. */
	make_path(path, "dir1");
	make_path(path2, "sym001");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym001 -> dir1");
	make_path(path, "sym001");
	make_path(path2, "sym002");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym002 -> sym001");
	make_path(path, "sym002");
	make_path(path2, "sym003");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym003 -> sym002");
	make_path(path, "sym003");
	make_path(path2, "sym004");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym004 -> sym003");
	make_path(path, "sym004");
	make_path(path2, "sym005");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym005 -> sym004");
	make_path(path, "sym005");
	make_path(path2, "sym006");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym006 -> sym005");
	make_path(path, "sym006");
	make_path(path2, "sym007");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym007 -> sym006");
	make_path(path, "sym007");
	make_path(path2, "sym008");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym008 -> sym007");
	make_path(path, "sym008");
	make_path(path2, "sym009");
	PG_EXPECT_SYS(symlink(path, path2) == 0, "sym009 -> sym008");

	/* POSIX says SYMLOOP_MAX should be at least 8. */
	make_path(path, "sym008");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT_SYS(stat(path, &statbuf) == 0, "stat sym008");
	PG_EXPECT(S_ISDIR(statbuf.st_mode));

#ifdef WIN32

	/*
	 * Test ELOOP failure in our Windows implementation of stat(), because we
	 * know it gives up after 8.
	 */
	make_path(path, "sym009");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(stat(path, &statbuf) == -1, "Windows: stat sym009 fails");
	PG_EXPECT_EQ(errno, ELOOP);
#endif

	/* If we break the chain we get ENOENT. */
	make_path(path, "sym003");
	PG_EXPECT_SYS(unlink(path) == 0);
	make_path(path, "sym008");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(stat(path, &statbuf) == -1, "stat broken symlink chain fails");
	PG_EXPECT_EQ(errno, ENOENT);

	/* Tests for lstat(). */

	PG_EXPECT(stat("does-not-exist.txt", &statbuf) == -1, "lstat missing file fails");
	PG_EXPECT_EQ(errno, ENOENT);

	make_path(path, "dir1/test.txt");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(lstat(path, &statbuf) == 0, "lstat regular file");
	PG_EXPECT(S_ISREG(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, 12);
	PG_EXPECT_EQ(statbuf.st_nlink, 1);

	make_path(path2, "dir1/my_subdir");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(lstat(path2, &statbuf) == 0, "lstat directory");
	PG_EXPECT(S_ISDIR(statbuf.st_mode));

	make_path(path, "dir1/my_symlink");
	memset(&statbuf, 0, sizeof(statbuf));
	PG_EXPECT(lstat(path, &statbuf) == 0, "lstat symlink");
	PG_EXPECT(S_ISLNK(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, strlen(path2), "got expected symlink size");

	make_path(path, "broken-symlink");
	make_path(path2, "does-not-exist");
	PG_EXPECT_SYS(symlink(path2, path) == 0, "make a broken symlink");
	PG_EXPECT_SYS(lstat(path, &statbuf) == 0, "lstat broken symlink");
	PG_EXPECT(S_ISLNK(statbuf.st_mode));
	PG_EXPECT_SYS(unlink(path) == 0);

	/* Tests for link() and unlink(). */

	make_path(path, "does-not-exist-1");
	make_path(path2, "does-not-exist-2");
	PG_EXPECT(link(path, path2) == -1, "link missing file fails");
	PG_EXPECT_EQ(errno, ENOENT);

	make_path(path, "dir1/test.txt");
	make_path(path2, "dir1/test2.txt");
	PG_EXPECT_SYS(link(path, path2) == 0, "link succeeds");

	PG_EXPECT(lstat(path, &statbuf) == 0, "lstat link 1 succeeds");
	PG_EXPECT(S_ISREG(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, 12);
	PG_EXPECT_EQ(statbuf.st_nlink, 2);

	PG_EXPECT(lstat(path, &statbuf) == 0, "lstat link 2 succeeds");
	PG_EXPECT(S_ISREG(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, 12);
	PG_EXPECT_EQ(statbuf.st_nlink, 2);

	PG_EXPECT_SYS(unlink(path2) == 0, "unlink succeeds");

	PG_EXPECT(lstat(path, &statbuf) == 0, "lstat link 1 succeeds");
	PG_EXPECT(S_ISREG(statbuf.st_mode));
	PG_EXPECT_EQ(statbuf.st_size, 12);
	PG_EXPECT_EQ(statbuf.st_nlink, 1);

	PG_EXPECT_SYS(lstat(path2, &statbuf) == -1, "lstat link 2 fails");
	PG_EXPECT_EQ(errno, ENOENT);

	/*
	 * On Windows we have a special code-path to make unlink() work on
	 * junction points created by our symlink() wrapper, to match POSIX
	 * behavior.
	 */
	make_path(path, "unlink_me_symlink");
	make_path(path2, "dir1");
	PG_EXPECT_SYS(symlink(path2, path) == 0, "create symlink");
	PG_EXPECT_SYS(unlink(path) == 0, "unlink symlink");

#ifdef WIN32

	/*
	 * But make sure that it doesn't work on plain old directories.
	 *
	 * POSIX doesn't specify whether unlink() works on a directory, so we
	 * don't check that on non-Windows.  In practice almost all known systems
	 * fail with EPERM (POSIX systems) or EISDIR (non-standard error on
	 * Linux), though AIX/JFS1 is rumored to succeed.  However, our Windows
	 * emulation doesn't allow it, because we want to avoid surprises by
	 * behaving like nearly all Unix systems.  So we check this on Windows
	 * only, where our wrapper fails with EPERM.
	 */
	PG_EXPECT_SYS(unlink(path2) == -1, "Windows: can't unlink() a directory");
	PG_EXPECT_EQ(errno, EPERM);
#endif

#ifdef WIN32

	/*
	 * Test that we automatically retry for a while if we get a sharing
	 * violation because external software does not use FILE_SHARE_* when
	 * opening our files.  See similar open() test for explanation.
	 */

	make_path(path, "name1.txt");

	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(close(fd) == 0);

	handle = CreateFile(path, GENERIC_WRITE | GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	PG_REQUIRE(handle);

	pgwin32_dirmod_loops = 2;	/* minimize looping to fail fast in testing */
	PG_EXPECT(unlink(path) == -1, "Windows: can't unlink file while non-shared handle exists");
	PG_EXPECT_EQ(errno, EACCES);

	pgwin32_dirmod_loops = 1800;	/* loop for up to 180s to make sure our
									 * 100ms callback is run */
	run_async_procedure_after_delay(close_handle, handle, 100); /* close handle after
																 * 100ms */
	PG_EXPECT_SYS(unlink(path) == 0, "Windows: can rename file after non-shared handle asynchronously closed");
#endif

	/*
	 * Our Windows unlink() wrapper blocks in a retry loop if you try to
	 * unlink a file in STATUS_DELETE_PENDING (ie that has already been
	 * unlinked but is still open), until it times out with EACCES or reaches
	 * ENOENT.  That may be useful for waiting for files to be asynchronously
	 * unlinked while performing a recursive unlink on
	 * !have_posix_unlink_semantics systems, so that rmdir(parent) works.
	 */
	make_path(path, "dir2");
	PG_EXPECT_SYS(mkdir(path, 0777) == 0);
	make_path(path, "dir2/test-file");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open dir2/test-file");
	PG_EXPECT_SYS(unlink(path) == 0, "unlink file while it's open, once");
#ifdef WIN32
	pgwin32_dirmod_loops = 2;	/* minimize looping to fail fast in testing */
#endif
	PG_EXPECT(unlink(path) == -1, "can't unlink again");
	if (have_posix_unlink_semantics)
	{
		PG_EXPECT_EQ(errno, ENOENT, "POSIX: we expect ENOENT");
	}
	else
	{
		PG_EXPECT_EQ(errno, EACCES, "Windows non-POSIX: we expect EACCES (delete already pending)");
#ifdef WIN32
		pgwin32_dirmod_loops = 1800;	/* loop for up to 180s to make sure
										 * our 100ms callback is run */
		run_async_procedure_after_delay(close_fd, &fd, 100);	/* close fd after 100ms */
#endif
		PG_EXPECT(unlink(path) == -1, "Windows non-POSIX: trying again fails");
		PG_EXPECT_EQ(errno, ENOENT, "Windows non-POSIX: ... but blocked until ENOENT was reached due to asynchronous close");
	}
	make_path(path, "dir2");
	PG_EXPECT_SYS(rmdir(path) == 0, "now we can remove the directory");

	/* Tests for rename(). */

	make_path(path, "name1.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	make_path(path2, "name2.txt");
	PG_EXPECT_SYS(rename(path, path2) == 0, "rename name1.txt -> name2.txt");

	PG_EXPECT_EQ(open(path, O_RDWR | PG_BINARY, 0777), -1, "can't open name1.txt anymore");
	PG_EXPECT_EQ(errno, ENOENT);

	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	make_path(path2, "name2.txt");
	PG_EXPECT_SYS(rename(path, path2) == 0, "rename name1.txt -> name2.txt, replacing it");

	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	fd = open(path2, O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open name2.txt");
	make_path(path2, "name2.txt");

	if (!have_posix_unlink_semantics)
	{
#ifdef WIN32
		pgwin32_dirmod_loops = 2;	/* minimize looping to fail fast in testing */
#endif
		PG_EXPECT_SYS(rename(path, path2) == -1,
					  "Windows non-POSIX: can't rename name1.txt -> name2.txt while name2.txt is open");
		PG_EXPECT_EQ(errno, EACCES);
		PG_EXPECT_SYS(unlink(path) == 0, "unlink name1.txt");
	}
	else
	{
		PG_EXPECT_SYS(rename(path, path2) == 0,
					  "POSIX: can rename name1.txt -> name2.txt while name2.txt is open");
	}
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	make_path(path, "name1.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	/* leave open */
	make_path(path2, "name2.txt");
	PG_EXPECT_SYS(rename(path, path2) == 0,
				  "can rename name1.txt -> name2.txt while name1.txt is open");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	make_path(path, "name1.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	fd = open(path2, O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open name2.txt");
	PG_EXPECT_SYS(unlink(path2) == 0, "unlink name2.txt while it is still open");

	if (have_posix_unlink_semantics)
	{
		PG_EXPECT_SYS(rename(path, path2) == 0,
					  "POSIX: rename name1.txt -> name2.txt while unlinked file is still open");
	}
	else
	{
		PG_EXPECT_SYS(rename(path, path2) == -1,
					  "Windows non-POSIX: cannot rename name1.txt -> name2.txt while unlinked file is still open");
		PG_EXPECT_EQ(errno, EACCES);
		PG_EXPECT_SYS(unlink(path) == 0);
	}
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

#ifdef WIN32

	/*
	 * Test that we automatically retry for a while if we get a sharing
	 * violation because external software does not use FILE_SHARE_* when
	 * opening our files.  See similar open() test for explanation.
	 */

	make_path(path, "name1.txt");
	make_path(path2, "name2.txt");

	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(close(fd) == 0);

	handle = CreateFile(path, GENERIC_WRITE | GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	PG_REQUIRE(handle);

	pgwin32_dirmod_loops = 2;	/* minimize looping to fail fast in testing */
	PG_EXPECT(rename(path, path2) == -1, "Windows: can't rename file while non-shared handle exists for name1");
	PG_EXPECT_EQ(errno, EACCES);

	pgwin32_dirmod_loops = 1800;	/* loop for up to 180s to make sure our
									 * 100ms callback is run */
	run_async_procedure_after_delay(close_handle, handle, 100); /* close handle after
																 * 100ms */
	PG_EXPECT_SYS(rename(path, path2) == 0, "Windows: can rename file after non-shared handle asynchronously closed");

	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "touch name1.txt");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	handle = CreateFile(path2, GENERIC_WRITE | GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	PG_REQUIRE(handle);

	pgwin32_dirmod_loops = 2;	/* minimize looping to fail fast in testing */
	PG_EXPECT(rename(path, path2) == -1, "Windows: can't rename file while non-shared handle exists for name2");
	PG_EXPECT_EQ(errno, EACCES);

	pgwin32_dirmod_loops = 1800;	/* loop for up to 180s to make sure our
									 * 100ms callback is run */
	run_async_procedure_after_delay(close_handle, handle, 100); /* close handle after
																 * 100ms */
	PG_EXPECT_SYS(rename(path, path2) == 0, "Windows: can rename file after non-shared handle asynchronously closed");

	PG_REQUIRE_SYS(unlink(path2) == 0);
#endif

	/* Tests for open(). */

	make_path(path, "test.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open(O_CREAT | O_EXCL) succeeds");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_EQ(fd, -1, "open(O_CREAT | O_EXCL) again fails");
	PG_EXPECT_EQ(errno, EEXIST);

	fd = open(path, O_CREAT | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open(O_CREAT) succeeds");

	/*
	 * We can unlink a file that we still have an open handle for on Windows,
	 * because our open() wrapper used FILE_SHARE_DELETE.
	 */
	PG_EXPECT_SYS(unlink(path) == 0);

	/*
	 * On older Windows, our open wrapper on Windows will see
	 * STATUS_DELETE_PENDING and pretend it can't see the file.  On newer
	 * Windows, it won't see the file.  Either way matches expected POSIX
	 * behavior.
	 */
	PG_EXPECT(open(path, O_RDWR | PG_BINARY, 0777) == -1);
	PG_EXPECT(errno == ENOENT);

	/*
	 * Things are trickier if you asked to create a file.  ENOENT doesn't make
	 * sense as a error number to a program expecting POSIX behavior then, so
	 * our wrpaper uses EEXIST for lack of anything more appropriate.
	 */
	fd2 = open(path, O_RDWR | PG_BINARY | O_CREAT, 0777);
	if (have_posix_unlink_semantics)
	{
		PG_EXPECT_SYS(fd2 >= 0, "POSIX: create file with same name, while unlinked file is still open");
		PG_EXPECT_SYS(close(fd2) == 0);
	}
	else
	{
		PG_EXPECT_SYS(fd2 == -1,
					  "Windows non-POSIX: cannot create file with same name, while unlinked file is still open");
		PG_EXPECT_EQ(errno, EEXIST);
	}

	/* After closing the handle, Windows non-POSIX can now create a new file. */
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);
	fd = open(path, O_RDWR | PG_BINARY | O_CREAT, 0777);
	PG_EXPECT_SYS(fd >= 0,
				  "can create a file with recently unlinked name after closing");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

#ifdef WIN32

	/*
	 * Even on systems new enough to have POSIX unlink behavior, the problem
	 * of sharing violations still applies.
	 *
	 * Our own open() wrapper is careful to use the FILE_SHARE_* flags to
	 * avoid the dreaded ERROR_SHARING_VIOLATION, but it's possible that
	 * another program might open one of our files without them.  In that
	 * case, our open() wrapper will sleep and retry for a long time, in the
	 * hope that other program goes away.  So let's open a handle with
	 * antisocial flags. We'll adjust the loop count via a side-hatch so we
	 * don't waste time in this test, though.
	 */
	handle = CreateFile(path, GENERIC_WRITE | GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	PG_REQUIRE(handle);

	pgwin32_open_handle_loops = 2;	/* minimize retries so test is fast */
	PG_EXPECT(open(path, O_RDWR | PG_BINARY, 0777) == -1, "Windows: can't open file while non-shared handle exists");
	PG_EXPECT_EQ(errno, EACCES);

	pgwin32_open_handle_loops = 18000;	/* 180s must be long enough for our
										 * async callback to run */
	run_async_procedure_after_delay(close_handle, handle, 100); /* close handle after
																 * 100ms */
	fd = open(path, O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "Windows: can open file after non-shared handle asynchronously closed");
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);
#endif

	PG_EXPECT_SYS(unlink(path) == 0);

	/* Tests for opendir(), readdir(), closedir(). */
	{
		DIR		   *dir;
		struct dirent *de;
		int			dot = -1;
		int			dotdot = -1;
		int			my_subdir = -1;
		int			my_symlink = -1;
		int			test_txt = -1;

		make_path(path, "does-not-exist");
		PG_EXPECT(opendir(path) == NULL, "open missing directory fails");
		PG_EXPECT_EQ(errno, ENOENT);

		make_path(path, "dir1");
		PG_EXPECT_SYS((dir = opendir(path)), "open directory");

#ifdef DT_REG
/*
 * Linux, *BSD, macOS and our Windows wrappers have BSD d_type.  On a few rare
 * file systems, it may be reported as DT_UNKNOWN so we have to tolerate that too.
 */
#define LOAD(name, variable) if (strcmp(de->d_name, name) == 0) variable = de->d_type
#define CHECK(name, variable, type) \
	PG_EXPECT(variable != -1, name " was found"); \
	PG_EXPECT(variable == DT_UNKNOWN || variable == type, name " has type DT_UNKNOWN or " #type)
#else
/*
 * Solaris, AIX and Cygwin do not have it (it's not in POSIX).  Just check that
 * we saw the file and ignore the type.
 */
#define LOAD(name, variable) if (strcmp(de->d_name, name) == 0) variable = 0
#define CHECK(name, variable, type) \
	PG_EXPECT(variable != -1, name " was found")
#endif

		/* Load and check in two phases because the order is unknown. */
		errno = 0;
		while ((de = readdir(dir)))
		{
			LOAD(".", dot);
			LOAD("..", dotdot);
			LOAD("my_subdir", my_subdir);
			LOAD("my_symlink", my_symlink);
			LOAD("test.txt", test_txt);
		}
		PG_EXPECT_SYS(errno == 0, "ran out of dirents without error");

		CHECK(".", dot, DT_DIR);
		CHECK("..", dotdot, DT_DIR);
		CHECK("my_subdir", my_subdir, DT_DIR);
		CHECK("my_symlink", my_symlink, DT_LNK);
		CHECK("test.txt", test_txt, DT_REG);

#undef LOAD
#undef CHECK
	}

	/*
	 * On older Windows, readdir() sees entries that are in
	 * STATUS_DELETE_PENDING state, and they prevent the directory itself from
	 * being unlinked.  Demonstrate that difference here.
	 */
	make_path(path, "dir2");
	PG_REQUIRE_SYS(mkdir(path, 0777) == 0);
	make_path(path, "dir2/test.txt");
	fd = open(path, O_CREAT | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open file");
	PG_EXPECT_SYS(unlink(path) == 0, "unlink file while still open");
	if (have_posix_unlink_semantics)
	{
		PG_EXPECT(!directory_contains("dir2", "test.txt"), "POSIX: readdir doesn't see it before closing");
	}
	else
	{
		/*
		 * This test fails on Windows SMB filesystems (AKA network drives):
		 * test.txt is not visible to our readdir() wrapper, because SMB seems
		 * to hide STATUS_DELETE_PENDING files.
		 */
		PG_EXPECT(directory_contains("dir2", "test.txt"), "Windows non-POSIX: readdir still sees it before closing");
	}
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);
	PG_EXPECT(!directory_contains("dir2", "test.txt"), "readdir doesn't see it after closing");
}

/*
 * Tests for pread, pwrite, and vector variants.
 */
static void
filesystem_io_tests(void)
{
	int			fd;
	char		path[MAXPGPATH];
	char		buffer[80];
	char		buffer2[80];
	struct iovec iov[8];

	/*
	 * These are our own wrappers on Windows.  On Unix, they're just macros
	 * for system APIs, except on Solaris which shares the pg_{read,write}v
	 * replacements used on Windows.
	 */

	make_path(path, "io_test_file.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "create a new file");
	PG_REQUIRE(fd >= 0);

	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 0, "initial file position is zero");

	PG_EXPECT_EQ(pg_pwrite(fd, "llo", 3, 2), 3);
	PG_EXPECT_EQ(pg_pwrite(fd, "he", 2, 0), 2);

	/*
	 * On Windows, the current position moves, which is why we have the pg_
	 * prefixes as a warning to users.  Demonstrate that difference here.
	 */
#ifdef WIN32
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 2, "Windows: file position moved");
#else
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 0, "POSIX: file position didn't move");
#endif

	memset(buffer, 0, sizeof(buffer));
	PG_EXPECT_EQ(pg_pread(fd, buffer, 2, 3), 2);
	PG_EXPECT(memcmp(buffer, "lo", 2) == 0);
	PG_EXPECT_EQ(pg_pread(fd, buffer, 3, 0), 3);
	PG_EXPECT(memcmp(buffer, "hel", 3) == 0);

#ifdef WIN32
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 3, "Windows: file position moved");
#else
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 0, "POSIX: file position didn't move");
#endif

	PG_EXPECT_EQ(pg_pread(fd, buffer, 80, 0), 5, "pg_pread short read");
	PG_EXPECT_EQ(pg_pread(fd, buffer, 80, 5), 0, "pg_pread EOF");

	iov[0].iov_base = "wo";
	iov[0].iov_len = 2;
	iov[1].iov_base = "rld";
	iov[1].iov_len = 3;
	PG_EXPECT_EQ(pg_pwritev(fd, iov, 2, 5), 5);

#ifdef WIN32
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 10, "Windows: file position moved");
#else
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 0, "POSIX: file position didn't move");
#endif

	memset(buffer, 0, sizeof(buffer));
	memset(buffer2, 0, sizeof(buffer2));
	iov[0].iov_base = buffer;
	iov[0].iov_len = 4;
	iov[1].iov_base = buffer2;
	iov[1].iov_len = 4;
	PG_EXPECT_EQ(pg_preadv(fd, iov, 2, 1), 8);

#ifdef WIN32
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 9, "Windows: file position moved");
#else
	PG_EXPECT_EQ(lseek(fd, 0, SEEK_CUR), 0, "POSIX: file position didn't move");
#endif

	PG_EXPECT(memcmp(buffer, "ello", 4) == 0);
	PG_EXPECT(memcmp(buffer2, "worl", 4) == 0);

	memset(buffer, 0, sizeof(buffer));
	memset(buffer2, 0, sizeof(buffer2));
	iov[0].iov_base = buffer;
	iov[0].iov_len = 1;
	iov[1].iov_base = buffer2;
	iov[1].iov_len = 80;
	PG_EXPECT_EQ(pg_preadv(fd, iov, 2, 8), 2);
	PG_EXPECT_EQ(pg_preadv(fd, iov, 2, 9), 1);
	PG_EXPECT_EQ(pg_preadv(fd, iov, 2, 10), 0);
	PG_EXPECT_EQ(pg_preadv(fd, iov, 2, 11), 0);

	memset(buffer, 0, sizeof(buffer));
	PG_EXPECT_EQ(pg_pread(fd, buffer, 10, 0), 10);
	PG_EXPECT_EQ_STR(buffer, "helloworld");

	PG_REQUIRE_SYS(close(fd) == 0);

	/* Demonstrate the effects of "text" mode (no PG_BINARY flag). */

	/* Write out a message in text mode. */
	fd = open(path, O_RDWR, 0777);
	PG_EXPECT_SYS(fd >= 0, "open file in text mode");
	PG_REQUIRE(fd >= 0);
	PG_EXPECT_EQ(write(fd, "hello world\n", 12), 12);
	PG_REQUIRE_SYS(fd < 0 || close(fd) == 0);

	/* Read it back in binary mode, to reveal the translation. */
	fd = open(path, O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open file in binary mode");
	PG_REQUIRE(fd >= 0);
#ifdef WIN32
	PG_EXPECT_EQ(read(fd, buffer, sizeof(buffer)), 13,
				 "Windows: \\n was translated");
	PG_EXPECT(memcmp(buffer, "hello world\r\n", 13) == 0,
			  "Windows: \\n was translated to \\r\\n");
#else
	PG_EXPECT_EQ(read(fd, buffer, sizeof(buffer)), 12,
				 "POSIX: \\n was not translated");
	PG_EXPECT(memcmp(buffer, "hello world\n", 12) == 0,
			  "POSIX: \\n is \\n");
#endif
	PG_REQUIRE_SYS(close(fd) == 0);

	/* The opposite translation happens in text mode, hiding it. */
	fd = open(path, O_RDWR, 0777);
	PG_EXPECT_SYS(fd >= 0, "open file in text mode");
	PG_REQUIRE(fd >= 0);
	PG_EXPECT_EQ(read(fd, buffer, sizeof(buffer)), 12);
	PG_EXPECT(memcmp(buffer, "hello world\n", 12) == 0);
	PG_REQUIRE_SYS(close(fd) == 0);

	PG_REQUIRE_SYS(unlink(path) == 0);

	/*
	 * Write out a message in text mode, this time with pg_pwrite(), which
	 * does not suffer from newline translation.
	 */
	fd = open(path, O_RDWR | O_CREAT, 0777);
	PG_EXPECT_SYS(fd >= 0, "open file in text mode");
	PG_REQUIRE(fd >= 0);
	PG_EXPECT_EQ(pg_pwrite(fd, "hello world\n", 12, 0), 12);
	PG_REQUIRE_SYS(close(fd) == 0);

	/* Read it back in binary mode to verify that. */
	fd = open(path, O_RDWR | PG_BINARY, 0777);
	PG_EXPECT_SYS(fd >= 0, "open file in binary mode");
	PG_REQUIRE(fd >= 0);
	PG_EXPECT_EQ(pg_pread(fd, buffer, sizeof(buffer), 0), 12,
				 "\\n was not translated by pg_pread()");
	PG_REQUIRE_SYS(close(fd) == 0);

	PG_REQUIRE_SYS(unlink(path) == 0);
}

/*
 * Exercise fsync and fdatasync.
 */
static void
filesystem_sync_tests(void)
{
	int			fd;
	char		path[MAXPGPATH];

	make_path(path, "sync_me.txt");
	fd = open(path, O_CREAT | O_EXCL | O_RDWR | PG_BINARY, 0777);
	PG_REQUIRE_SYS(fd >= 0);

	PG_REQUIRE_SYS(write(fd, "x", 1) == 1);
	PG_EXPECT_SYS(fsync(fd) == 0);

	PG_REQUIRE_SYS(write(fd, "x", 1) == 1);
	PG_EXPECT_SYS(fdatasync(fd) == 0);

	PG_REQUIRE_SYS(unlink(path) == 0);
}

int
main()
{
	PG_BEGIN_TESTS();

	filesystem_metadata_tests();
	filesystem_io_tests();
	filesystem_sync_tests();

	PG_END_TESTS();
}
