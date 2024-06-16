/*-------------------------------------------------------------------------
 *
 * be-secure-common.c
 *
 * common implementation-independent SSL support code
 *
 * While be-secure.c contains the interfaces that the rest of the
 * communications code calls, this file contains support routines that are
 * used by the library-specific implementations such as be-secure-openssl.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/libpq/be-secure-common.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "common/percentrepl.h"
#include "common/string.h"
#include "libpq/hba.h"
#include "libpq/libpq.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"

static HostsLine *parse_hosts_line(TokenizedAuthLine *tok_line, int elevel);

/*
 * Run ssl_passphrase_command
 *
 * prompt will be substituted for %p.  is_server_start determines the loglevel
 * of error messages.
 *
 * The result will be put in buffer buf, which is of size size.  The return
 * value is the length of the actual result.
 */
int
run_ssl_passphrase_command(const char *prompt, bool is_server_start, char *buf, int size)
{
	int			loglevel = is_server_start ? ERROR : LOG;
	char	   *command;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(prompt);
	Assert(size > 0);
	buf[0] = '\0';

	command = replace_percent_placeholders(ssl_passphrase_command, "ssl_passphrase_command", "p", prompt);

	fh = OpenPipeStream(command, "r");
	if (fh == NULL)
	{
		ereport(loglevel,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command)));
		goto error;
	}

	if (!fgets(buf, size, fh))
	{
		if (ferror(fh))
		{
			explicit_bzero(buf, size);
			ereport(loglevel,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command)));
			goto error;
		}
	}

	pclose_rc = ClosePipeStream(fh);
	if (pclose_rc == -1)
	{
		explicit_bzero(buf, size);
		ereport(loglevel,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
		goto error;
	}
	else if (pclose_rc != 0)
	{
		char	   *reason;

		explicit_bzero(buf, size);
		reason = wait_result_to_str(pclose_rc);
		ereport(loglevel,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command),
				 errdetail_internal("%s", reason)));
		pfree(reason);
		goto error;
	}

	/* strip trailing newline and carriage return */
	len = pg_strip_crlf(buf);

error:
	pfree(command);
	return len;
}


/*
 * Check permissions for SSL key files.
 */
bool
check_ssl_key_file_permissions(const char *ssl_key_file, bool isServerStart)
{
	int			loglevel = isServerStart ? FATAL : LOG;
	struct stat buf;

	if (stat(ssl_key_file, &buf) != 0)
	{
		ereport(loglevel,
				(errcode_for_file_access(),
				 errmsg("could not access private key file \"%s\": %m",
						ssl_key_file)));
		return false;
	}

	/* Key file must be a regular file */
	if (!S_ISREG(buf.st_mode))
	{
		ereport(loglevel,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("private key file \"%s\" is not a regular file",
						ssl_key_file)));
		return false;
	}

	/*
	 * Refuse to load key files owned by users other than us or root, and
	 * require no public access to the key file.  If the file is owned by us,
	 * require mode 0600 or less.  If owned by root, require 0640 or less to
	 * allow read access through either our gid or a supplementary gid that
	 * allows us to read system-wide certificates.
	 *
	 * Note that roughly similar checks are performed in
	 * src/interfaces/libpq/fe-secure-openssl.c so any changes here may need
	 * to be made there as well.  The environment is different though; this
	 * code can assume that we're not running as root.
	 *
	 * Ideally we would do similar permissions checks on Windows, but it is
	 * not clear how that would work since Unix-style permissions may not be
	 * available.
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	if (buf.st_uid != geteuid() && buf.st_uid != 0)
	{
		ereport(loglevel,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("private key file \"%s\" must be owned by the database user or root",
						ssl_key_file)));
		return false;
	}

	if ((buf.st_uid == geteuid() && buf.st_mode & (S_IRWXG | S_IRWXO)) ||
		(buf.st_uid == 0 && buf.st_mode & (S_IWGRP | S_IXGRP | S_IRWXO)))
	{
		ereport(loglevel,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("private key file \"%s\" has group or world access",
						ssl_key_file),
				 errdetail("File must have permissions u=rw (0600) or less if owned by the database user, or permissions u=rw,g=r (0640) or less if owned by root.")));
		return false;
	}
#endif

	return true;
}

/*
 * parse_hosts_line
 *
 * Parses a loaded line from the pg_hosts.conf configuration and pulls out the
 * hostname, certificate, key and CA parts in order to build an SNI config in
 * the TLS backend. Validation of the parsed values is left for the TLS backend
 * to implement.
 */
static HostsLine *
parse_hosts_line(TokenizedAuthLine *tok_line, int elevel)
{
	HostsLine  *parsedline;
	List	   *tokens;
	ListCell   *field;
	AuthToken  *token;

	parsedline = palloc0(sizeof(HostsLine));
	parsedline->sourcefile = pstrdup(tok_line->file_name);
	parsedline->linenumber = tok_line->line_num;
	parsedline->rawline = pstrdup(tok_line->raw_line);

	/* Hostname */
	field = list_head(tok_line->fields);
	tokens = lfirst(field);
	token = linitial(tokens);
	parsedline->hostname = pstrdup(token->string);

	/* SSL Certificate */
	field = lnext(tok_line->fields, field);
	if (!field)
	{
		ereport(elevel,
				errcode(ERRCODE_CONFIG_FILE_ERROR),
				errmsg("missing entry at end of line"),
				errcontext("line %d of configuration file \"%s\"",
						   tok_line->line_num, tok_line->file_name));
		return NULL;
	}
	tokens = lfirst(field);
	token = linitial(tokens);
	parsedline->ssl_cert = pstrdup(token->string);

	/* SSL key */
	field = lnext(tok_line->fields, field);
	if (!field)
	{
		ereport(elevel,
				errcode(ERRCODE_CONFIG_FILE_ERROR),
				errmsg("missing entry at end of line"),
				errcontext("line %d of configuration file \"%s\"",
						   tok_line->line_num, tok_line->file_name));
		return NULL;
	}
	tokens = lfirst(field);
	token = linitial(tokens);
	parsedline->ssl_key = pstrdup(token->string);

	/* SSL CA */
	field = lnext(tok_line->fields, field);
	if (!field)
	{
		ereport(elevel,
				errcode(ERRCODE_CONFIG_FILE_ERROR),
				errmsg("missing entry at end of line"),
				errcontext("line %d of configuration file \"%s\"",
						   tok_line->line_num, tok_line->file_name));
		return NULL;
	}
	tokens = lfirst(field);
	token = linitial(tokens);
	parsedline->ssl_ca = pstrdup(token->string);

	return parsedline;
}

/*
 * load_hosts
 *
 * Reads pg_hosts.conf and passes back a List of parsed lines, or NIL in case
 * of errors.
 */
List *
load_hosts(void)
{
	FILE	   *file;
	ListCell   *line;
	List	   *hosts_lines = NIL;
	List	   *parsed_lines = NIL;
	HostsLine  *newline;
	bool		ok = true;
	MemoryContext oldcxt;
	MemoryContext hostcxt;

	file = open_auth_file(HostsFileName, LOG, 0, NULL);
	if (file == NULL)
	{
		/* An error has already been logged so no need to add one here. */
		return NIL;
	}

	tokenize_auth_file(HostsFileName, file, &hosts_lines, LOG, 0);

	hostcxt = AllocSetContextCreate(PostmasterContext,
									"hosts file parser context",
									ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(hostcxt);

	foreach(line, hosts_lines)
	{
		TokenizedAuthLine *tok_line = (TokenizedAuthLine *) lfirst(line);

		if (tok_line->err_msg != NULL)
		{
			ok = false;
			continue;
		}

		if ((newline = parse_hosts_line(tok_line, LOG)) == NULL)
		{
			ok = false;
			continue;
		}

		parsed_lines = lappend(parsed_lines, newline);
	}

	free_auth_file(file, 0);
	MemoryContextSwitchTo(oldcxt);

	/*
	 * If we didn't find any SNI configuration then that's not an error since
	 * the pg_hosts file is additive to the default SSL configuration.
	 */
	if (ok && parsed_lines == NIL)
	{
		ereport(DEBUG1,
				errmsg("no SNI configuration added from configuration file  \"%s\"",
					   HostsFileName));
		MemoryContextDelete(hostcxt);
		return NIL;
	}

	if (!ok)
	{
		MemoryContextDelete(hostcxt);
		return NIL;
	}

	return parsed_lines;
}
