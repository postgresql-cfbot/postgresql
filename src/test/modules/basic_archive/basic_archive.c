/*-------------------------------------------------------------------------
 *
 * basic_archive.c
 *
 * This file demonstrates a basic archive library implementation that is
 * roughly equivalent to the following shell command:
 *
 * 		test ! -f /path/to/dest && cp /path/to/src /path/to/dest
 *
 * One notable difference between this module and the shell command above
 * is that this module first copies the file to a temporary destination,
 * syncs it to disk, and then durably moves it to the final destination.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/basic_archive/basic_archive.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "postmaster/pgarch.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_archive_module_init(ArchiveModuleCallbacks *cb);

static char *archive_directory = NULL;

static bool basic_archive_configured(void);
static bool basic_archive_file(const char *file, const char *path);
static bool check_archive_directory(char **newval, void **extra, GucSource source);

/*
 * _PG_init
 *
 * Defines the module's GUC.
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("basic_archive.archive_directory",
							   gettext_noop("Archive file destination directory."),
							   NULL,
							   &archive_directory,
							   "",
							   PGC_SIGHUP,
							   0,
							   check_archive_directory, NULL, NULL);

	EmitWarningsOnPlaceholders("basic_archive");
}

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
void
_PG_archive_module_init(ArchiveModuleCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_archive_module_init, ArchiveModuleInit);

	cb->check_configured_cb = basic_archive_configured;
	cb->archive_file_cb = basic_archive_file;
}

/*
 * check_archive_directory
 *
 * Checks that the provided archive directory exists.
 */
static bool
check_archive_directory(char **newval, void **extra, GucSource source)
{
	struct stat st;

	/*
	 * The default value is an empty string, so we have to accept that value.
	 * Our check_configured callback also checks for this and prevents archiving
	 * from proceeding if it is still empty.
	 */
	if (*newval == NULL || *newval[0] == '\0')
		return true;

	/*
	 * Make sure the file paths won't be too long.  The docs indicate that the
	 * file names to be archived can be up to 64 characters long.
	 */
	if (strlen(*newval) + 64 + 2 >= MAXPGPATH)
	{
		GUC_check_errdetail("archive directory too long");
		return false;
	}

	/*
	 * Do a basic sanity check that the specified archive directory exists.  It
	 * could be removed at some point in the future, so we still need to be
	 * prepared for it not to exist in the actual archiving logic.
	 */
	if (stat(*newval, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		GUC_check_errdetail("specified archive directory does not exist");
		return false;
	}

	return true;
}

/*
 * basic_archive_configured
 *
 * Checks that archive_directory is not blank.
 */
static bool
basic_archive_configured(void)
{
	return archive_directory != NULL && archive_directory[0] != '\0';
}

/*
 * basic_archive_file
 *
 * Archives one file.
 */
static bool
basic_archive_file(const char *file, const char *path)
{
	char destination[MAXPGPATH];
	char temp[MAXPGPATH];
	struct stat st;

	ereport(DEBUG3,
			(errmsg("archiving \"%s\" via basic_archive", file)));

	snprintf(destination, MAXPGPATH, "%s/%s", archive_directory, file);
	snprintf(temp, MAXPGPATH, "%s/%s", archive_directory, "archtemp");

	/*
	 * First, check if the file has already been archived.  If the archive file
	 * already exists, something might be wrong, so we just fail.
	 */
	if (stat(destination, &st) == 0)
	{
		ereport(WARNING,
				(errmsg("archive file \"%s\" already exists", destination)));
		return false;
	}
	else if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", destination)));

	/*
	 * Remove pre-existing temporary file, if one exists.
	 */
	if (unlink(temp) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not unlink file \"%s\": %m", temp)));

	/*
	 * Copy the file to its temporary destination.
	 */
	copy_file(unconstify(char *, path), temp);

	/*
	 * Sync the temporary file to disk and move it to its final destination.
	 */
	(void) durable_rename_excl(temp, destination, ERROR);

	ereport(DEBUG1,
			(errmsg("archived \"%s\" via basic_archive", file)));

	return true;
}
