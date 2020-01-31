/*-------------------------------------------------------------------------
 *
 * archive.c
 *	  Common WAL archive routines
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/archive.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/archive.h"

/*
 * Constructs restore_command from template with %p, %f and %r aliases.
 * Returns 0 if restore_command was successfuly built.
 *
 * If any of the required arguments is NULL, but corresponding alias is
 * met, then -1 code will be returned.
 */
int
ConstructRestoreCommand(const char *restoreCommand,
						const char *xlogpath,
						const char *xlogfname,
						const char *lastRestartPointFname,
						char *result)
{
	char	   *dp,
			   *endp;
	const char *sp;

	/*
	 * Construct the command to be executed.
	 */
	dp = result;
	endp = result + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = restoreCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					/* %p: relative path of target file */
					if (xlogpath == NULL)
						return -1;
					sp++;
					StrNCpy(dp, xlogpath, endp - dp);
					make_native_path(dp);
					dp += strlen(dp);
					break;
				case 'f':
					/* %f: filename of desired file */
					if (xlogfname == NULL)
						return -1;
					sp++;
					StrNCpy(dp, xlogfname, endp - dp);
					dp += strlen(dp);
					break;
				case 'r':
					/* %r: filename of last restartpoint */
					if (lastRestartPointFname == NULL)
						return -1;
					sp++;
					StrNCpy(dp, lastRestartPointFname, endp - dp);
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

	return 0;
}
