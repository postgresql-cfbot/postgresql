/* -*-pgsql-c-*- */
/*
 * Configuration files parser for usage in frontend.
 *
 * Copyright (c) 2000-2018, PostgreSQL Global Development Group
 *
 * src/bin/pg_rewind/guc-file-fe.l
 * Modified version of src/backend/utils/misc/guc-file.l
 */

%{

#include "postgres_fe.h"

#include <ctype.h>
#include <dirent.h>
#include <unistd.h>
#include <setjmp.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "guc-file-fe.h"
#include "logging.h"

#include "common/fe_memutils.h"

/*
 * flex emits a yy_fatal_error() function that it calls in response to
 * critical errors like malloc failure, file I/O errors, and detection of
 * internal inconsistency.  That function prints a message and calls exit().
 * Mutate it to instead call our handler, which jumps out of the parser.
 */
#undef fprintf
#define fprintf(file, fmt, msg) GUC_flex_fatal(msg)

enum
{
	GUC_ID = 1,
	GUC_STRING = 2,
	GUC_INTEGER = 3,
	GUC_REAL = 4,
	GUC_EQUALS = 5,
	GUC_UNQUOTED_STRING = 6,
	GUC_QUALIFIED_ID = 7,
	GUC_EOL = 99,
	GUC_ERROR = 100
};

static unsigned int ConfigFileLineno;
static const char *GUC_flex_fatal_errmsg;
static sigjmp_buf *GUC_flex_fatal_jmp;

static void FreeConfigVariable(ConfigVariable *item);

static void record_config_file_error(const char *errmsg,
						 const char *config_file,
						 int lineno,
						 ConfigVariable **head_p,
						 ConfigVariable **tail_p);

static int	GUC_flex_fatal(const char *msg);
static char *GUC_scanstr(const char *s);
static bool ParseConfigDirectory(const char *includedir,
					 const char *calling_file, int calling_lineno,
					 int depth, int elevel,
					 ConfigVariable **head_p,
					 ConfigVariable **tail_p);

/* LCOV_EXCL_START */

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="GUC_yy"


SIGN			("-"|"+")
DIGIT			[0-9]
HEXDIGIT		[0-9a-fA-F]

UNIT_LETTER		[a-zA-Z]

INTEGER			{SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*

EXPONENT		[Ee]{SIGN}?{DIGIT}+
REAL			{SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?

LETTER			[A-Za-z_\200-\377]
LETTER_OR_DIGIT [A-Za-z_0-9\200-\377]

ID				{LETTER}{LETTER_OR_DIGIT}*
QUALIFIED_ID	{ID}"."{ID}

UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
STRING			\'([^'\\\n]|\\.|\'\')*\'

%%

\n				ConfigFileLineno++; return GUC_EOL;
[ \t\r]+		/* eat whitespace */
#.*				/* eat comment (.* matches anything until newline) */

{ID}			return GUC_ID;
{QUALIFIED_ID}	return GUC_QUALIFIED_ID;
{STRING}		return GUC_STRING;
{UNQUOTED_STRING} return GUC_UNQUOTED_STRING;
{INTEGER}		return GUC_INTEGER;
{REAL}			return GUC_REAL;
=				return GUC_EQUALS;

.				return GUC_ERROR;

%%

/* LCOV_EXCL_STOP */

/*
 * the bare comparison function for GUC names
 */
static int
guc_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * array ordering has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}

/*
 * Given a configuration file or directory location that may be a relative
 * path, return an absolute one.  We consider the location to be relative to
 * the directory holding the calling file.
 */
static char *
AbsoluteConfigLocation(const char *location, const char *calling_file)
{
	char abs_path[MAXPGPATH];

	if (is_absolute_path(location))
		return pstrdup(location);
	else
	{
		strlcpy(abs_path, calling_file, sizeof(abs_path));
		get_parent_directory(abs_path);
		join_path_components(abs_path, abs_path, location);
		canonicalize_path(abs_path);

		return pstrdup(abs_path);
	}
}

/*
 * Read and parse a single configuration file.  This function recurses
 * to handle "include" directives.
 *
 * If "strict" is true, treat failure to open the config file as an error,
 * otherwise just skip the file.
 *
 * calling_file/calling_lineno identify the source of the request.
 * Pass NULL/0 if not recursing from an inclusion request.
 *
 * See ParseConfigFp for further details.  This one merely adds opening the
 * config file rather than working from a caller-supplied file descriptor,
 * and absolute-ifying the path name if necessary.
 */
bool
ParseConfigFile(const char *config_file, bool strict,
				const char *calling_file, int calling_lineno,
				int depth, int elevel,
				ConfigVariable **head_p,
				ConfigVariable **tail_p)
{
	char	   *abs_path;
	bool		OK = true;
	FILE	   *fp;

	/*
	 * Reject too-deep include nesting depth.  This is just a safety check to
	 * avoid dumping core due to stack overflow if an include file loops back
	 * to itself.  The maximum nesting depth is pretty arbitrary.
	 */
	if (depth > 10)
	{
		pg_log(PG_WARNING, 
				"could not open configuration file \"%s\": maximum nesting depth exceeded\n",
				config_file);
		record_config_file_error("nesting depth exceeded",
								 calling_file, calling_lineno,
								 head_p, tail_p);
		return false;
	}

	abs_path = AbsoluteConfigLocation(config_file, calling_file);
	fp = fopen(abs_path, "r");
	if (!fp)
	{
		if (strict)
		{
			pg_log(PG_WARNING, 
				"could not open configuration file \"%s\"\n",
				abs_path);
			record_config_file_error(psprintf("could not open file \"%s\"",
											  abs_path),
									 calling_file, calling_lineno,
									 head_p, tail_p);
			OK = false;
		}
		else
		{
			pg_log(PG_WARNING, 
				"skipping missing configuration file \"%s\"\n",
				abs_path);
		}
		goto cleanup;
	}

	OK = ParseConfigFp(fp, abs_path, depth, elevel, head_p, tail_p);

cleanup:
	if (fp)
		fclose(fp);
	pfree(abs_path);

	return OK;
}

/*
 * Capture an error message in the ConfigVariable list returned by
 * config file parsing.
 */
static void
record_config_file_error(const char *errmsg,
						 const char *config_file,
						 int lineno,
						 ConfigVariable **head_p,
						 ConfigVariable **tail_p)
{
	ConfigVariable *item;

	item = palloc(sizeof *item);
	item->name = NULL;
	item->value = NULL;
	item->errmsg = pstrdup(errmsg);
	item->filename = config_file ? pstrdup(config_file) : NULL;
	item->sourceline = lineno;
	item->ignore = true;
	item->applied = false;
	item->next = NULL;
	if (*head_p == NULL)
		*head_p = item;
	else
		(*tail_p)->next = item;
	*tail_p = item;
}

/*
 * Flex fatal errors bring us here.  Stash the error message and jump back to
 * ParseConfigFp().  Assume all msg arguments point to string constants; this
 * holds for flex 2.5.31 (earliest we support) and flex 2.5.35 (latest as of
 * this writing).  Otherwise, we would need to copy the message.
 *
 * We return "int" since this takes the place of calls to fprintf().
*/
static int
GUC_flex_fatal(const char *msg)
{
	GUC_flex_fatal_errmsg = msg;
	siglongjmp(*GUC_flex_fatal_jmp, 1);
	return 0;					/* keep compiler quiet */
}

/*
 * Read and parse a single configuration file.  This function recurses
 * to handle "include" directives.
 *
 * Input parameters:
 *	fp: file pointer from AllocateFile for the configuration file to parse
 *	config_file: absolute or relative path name of the configuration file
 *	depth: recursion depth (should be 0 in the outermost call)
 *	elevel: error logging level to use
 * Input/Output parameters:
 *	head_p, tail_p: head and tail of linked list of name/value pairs
 *
 * *head_p and *tail_p must be initialized, either to NULL or valid pointers
 * to a ConfigVariable list, before calling the outer recursion level.  Any
 * name-value pairs read from the input file(s) will be appended to the list.
 * Error reports will also be appended to the list, if elevel < ERROR.
 *
 * Returns TRUE if successful, FALSE if an error occurred.
 *
 * Note: this function is used to parse not only postgresql.conf, but
 * various other configuration files that use the same "name = value"
 * syntax.  Hence, do not do anything here or in the subsidiary routines
 * ParseConfigFile/ParseConfigDirectory that assumes we are processing
 * GUCs specifically.
 */
bool
ParseConfigFp(FILE *fp, const char *config_file, int depth, int elevel,
			  ConfigVariable **head_p, ConfigVariable **tail_p)
{
	volatile bool OK = true;
	unsigned int save_ConfigFileLineno = ConfigFileLineno;
	sigjmp_buf *save_GUC_flex_fatal_jmp = GUC_flex_fatal_jmp;
	sigjmp_buf	flex_fatal_jmp;
	volatile YY_BUFFER_STATE lex_buffer = NULL;
	int			errorcount;
	int			token;

	if (sigsetjmp(flex_fatal_jmp, 1) == 0)
		GUC_flex_fatal_jmp = &flex_fatal_jmp;
	else
	{
		/*
		 * Regain control after a fatal, internal flex error.  It may have
		 * corrupted parser state.  Consequently, abandon the file, but trust
		 * that the state remains sane enough for yy_delete_buffer().
		 */
		pg_log(PG_WARNING, "%s at file \"%s\" line %u\n", GUC_flex_fatal_errmsg,
				config_file, ConfigFileLineno);
		record_config_file_error(GUC_flex_fatal_errmsg,
								 config_file, ConfigFileLineno,
								 head_p, tail_p);
		OK = false;
		goto cleanup;
	}

	/*
	 * Parse
	 */
	ConfigFileLineno = 1;
	errorcount = 0;

	lex_buffer = yy_create_buffer(fp, YY_BUF_SIZE);
	yy_switch_to_buffer(lex_buffer);

	/* This loop iterates once per logical line */
	while ((token = yylex()))
	{
		char	   *opt_name = NULL;
		char	   *opt_value = NULL;
		ConfigVariable *item;

		if (token == GUC_EOL)	/* empty or comment line */
			continue;

		/* first token on line is option name */
		if (token != GUC_ID && token != GUC_QUALIFIED_ID)
			goto parse_error;
		opt_name = pstrdup(yytext);

		/* next we have an optional equal sign; discard if present */
		token = yylex();
		if (token == GUC_EQUALS)
			token = yylex();

		/* now we must have the option value */
		if (token != GUC_ID &&
			token != GUC_STRING &&
			token != GUC_INTEGER &&
			token != GUC_REAL &&
			token != GUC_UNQUOTED_STRING)
			goto parse_error;
		if (token == GUC_STRING)	/* strip quotes and escapes */
			opt_value = GUC_scanstr(yytext);
		else
			opt_value = pstrdup(yytext);

		/* now we'd like an end of line, or possibly EOF */
		token = yylex();
		if (token != GUC_EOL)
		{
			if (token != 0)
				goto parse_error;
			/* treat EOF like \n for line numbering purposes, cf bug 4752 */
			ConfigFileLineno++;
		}

		/* OK, process the option name and value */
		if (guc_name_compare(opt_name, "include_dir") == 0)
		{
			/*
			 * An include_dir directive isn't a variable and should be
			 * processed immediately.
			 */
			if (!ParseConfigDirectory(opt_value,
									  config_file, ConfigFileLineno - 1,
									  depth + 1, elevel,
									  head_p, tail_p))
				OK = false;
			yy_switch_to_buffer(lex_buffer);
			pfree(opt_name);
			pfree(opt_value);
		}
		else if (guc_name_compare(opt_name, "include_if_exists") == 0)
		{
			/*
			 * An include_if_exists directive isn't a variable and should be
			 * processed immediately.
			 */
			if (!ParseConfigFile(opt_value, false,
								 config_file, ConfigFileLineno - 1,
								 depth + 1, elevel,
								 head_p, tail_p))
				OK = false;
			yy_switch_to_buffer(lex_buffer);
			pfree(opt_name);
			pfree(opt_value);
		}
		else if (guc_name_compare(opt_name, "include") == 0)
		{
			/*
			 * An include directive isn't a variable and should be processed
			 * immediately.
			 */
			if (!ParseConfigFile(opt_value, true,
								 config_file, ConfigFileLineno - 1,
								 depth + 1, elevel,
								 head_p, tail_p))
				OK = false;
			yy_switch_to_buffer(lex_buffer);
			pfree(opt_name);
			pfree(opt_value);
		}
		else
		{
			/* ordinary variable, append to list */
			item = palloc(sizeof *item);
			item->name = opt_name;
			item->value = opt_value;
			item->errmsg = NULL;
			item->filename = pstrdup(config_file);
			item->sourceline = ConfigFileLineno - 1;
			item->ignore = false;
			item->applied = false;
			item->next = NULL;
			if (*head_p == NULL)
				*head_p = item;
			else
				(*tail_p)->next = item;
			*tail_p = item;
		}

		/* break out of loop if read EOF, else loop for next line */
		if (token == 0)
			break;
		continue;

parse_error:
		/* release storage if we allocated any on this line */
		if (opt_name)
			pfree(opt_name);
		if (opt_value)
			pfree(opt_value);

		/* report the error */
		if (token == GUC_EOL || token == 0)
		{
			pg_log(PG_WARNING, "syntax error in file \"%s\" line %u, near end of line\n",
					config_file, ConfigFileLineno - 1);
			record_config_file_error("syntax error",
									 config_file, ConfigFileLineno - 1,
									 head_p, tail_p);
		}
		else
		{
			pg_log(PG_WARNING, "syntax error in file \"%s\" line %u, near token \"%s\"\n",
				config_file, ConfigFileLineno, yytext);
			record_config_file_error("syntax error",
									 config_file, ConfigFileLineno,
									 head_p, tail_p);
		}
		OK = false;
		errorcount++;

		/*
		 * To avoid producing too much noise when fed a totally bogus file,
		 * give up after 100 syntax errors per file (an arbitrary number).
		 */
		if (errorcount >= 100)
		{
			pg_log(PG_WARNING, "too many syntax errors found, abandoning file \"%s\"\n",
					config_file);
			break;
		}

		/* resync to next end-of-line or EOF */
		while (token != GUC_EOL && token != 0)
			token = yylex();
		/* break out of loop on EOF */
		if (token == 0)
			break;
	}

cleanup:
	yy_delete_buffer(lex_buffer);
	/* Each recursion level must save and restore these static variables. */
	ConfigFileLineno = save_ConfigFileLineno;
	GUC_flex_fatal_jmp = save_GUC_flex_fatal_jmp;
	return OK;
}

/*
 * Read and parse all config files in a subdirectory in alphabetical order
 *
 * includedir is the absolute or relative path to the subdirectory to scan.
 *
 * calling_file/calling_lineno identify the source of the request.
 * Pass NULL/0 if not recursing from an inclusion request.
 *
 * See ParseConfigFp for further details.
 */
bool
ParseConfigDirectory(const char *includedir,
					 const char *calling_file, int calling_lineno,
					 int depth, int elevel,
					 ConfigVariable **head_p,
					 ConfigVariable **tail_p)
{
	char	   *directory;
	DIR		   *d;
	struct dirent *de;
	char	  **filenames;
	int			num_filenames;
	int			size_filenames;
	bool		status;

	directory = AbsoluteConfigLocation(includedir, calling_file);
	d = opendir(directory);
	if (d == NULL)
	{
		pg_log(PG_WARNING, "could not open configuration directory \"%s\"\n",
			directory);
		record_config_file_error(psprintf("could not open directory \"%s\"",
										  directory),
								 calling_file, calling_lineno,
								 head_p, tail_p);
		status = false;
		goto cleanup;
	}

	/*
	 * Read the directory and put the filenames in an array, so we can sort
	 * them prior to processing the contents.
	 */
	size_filenames = 32;
	filenames = (char **) palloc(size_filenames * sizeof(char *));
	num_filenames = 0;

	while ((de = readdir(d)) != NULL)
	{
		struct stat st;
		char		filename[MAXPGPATH];

		/*
		 * Only parse files with names ending in ".conf".  Explicitly reject
		 * files starting with ".".  This excludes things like "." and "..",
		 * as well as typical hidden files, backup files, and editor debris.
		 */
		if (strlen(de->d_name) < 6)
			continue;
		if (de->d_name[0] == '.')
			continue;
		if (strcmp(de->d_name + strlen(de->d_name) - 5, ".conf") != 0)
			continue;

		join_path_components(filename, directory, de->d_name);
		canonicalize_path(filename);
		if (stat(filename, &st) == 0)
		{
			if (!S_ISDIR(st.st_mode))
			{
				/* Add file to array, increasing its size in blocks of 32 */
				if (num_filenames >= size_filenames)
				{
					size_filenames += 32;
					filenames = (char **) repalloc(filenames,
											size_filenames * sizeof(char *));
				}
				filenames[num_filenames] = pstrdup(filename);
				num_filenames++;
			}
		}
		else
		{
			/*
			 * stat does not care about permissions, so the most likely reason
			 * a file can't be accessed now is if it was removed between the
			 * directory listing and now.
			 */
			pg_log(PG_WARNING, "could not stat file \"%s\"\n", filename);
			record_config_file_error(psprintf("could not stat file \"%s\"",
											  filename),
									 calling_file, calling_lineno,
									 head_p, tail_p);
			status = false;
			goto cleanup;
		}
	}

	if (num_filenames > 0)
	{
		int			i;

		qsort(filenames, num_filenames, sizeof(char *), pg_qsort_strcmp);
		for (i = 0; i < num_filenames; i++)
		{
			if (!ParseConfigFile(filenames[i], true,
								 calling_file, calling_lineno,
								 depth, elevel,
								 head_p, tail_p))
			{
				status = false;
				goto cleanup;
			}
		}
	}
	status = true;

cleanup:
	if (d)
		closedir(d);
	pfree(directory);
	return status;
}

/*
 * Free a list of ConfigVariables, including the names and the values
 */
void
FreeConfigVariables(ConfigVariable *list)
{
	ConfigVariable *item;

	item = list;
	while (item)
	{
		ConfigVariable *next = item->next;

		FreeConfigVariable(item);
		item = next;
	}
}

/*
 * Free a single ConfigVariable
 */
static void
FreeConfigVariable(ConfigVariable *item)
{
	if (item->name)
		pfree(item->name);
	if (item->value)
		pfree(item->value);
	if (item->errmsg)
		pfree(item->errmsg);
	if (item->filename)
		pfree(item->filename);
	pfree(item);
}


/*
 *		scanstr
 *
 * Strip the quotes surrounding the given string, and collapse any embedded
 * '' sequences and backslash escapes.
 *
 * the string returned is palloc'd and should eventually be pfree'd by the
 * caller.
 */
static char *
GUC_scanstr(const char *s)
{
	char	   *newStr;
	int			len,
				i,
				j;

	Assert(s != NULL && s[0] == '\'');
	len = strlen(s);
	Assert(len >= 2);
	Assert(s[len - 1] == '\'');

	/* Skip the leading quote; we'll handle the trailing quote below */
	s++, len--;

	/* Since len still includes trailing quote, this is enough space */
	newStr = palloc(len);

	for (i = 0, j = 0; i < len; i++)
	{
		if (s[i] == '\\')
		{
			i++;
			switch (s[i])
			{
				case 'b':
					newStr[j] = '\b';
					break;
				case 'f':
					newStr[j] = '\f';
					break;
				case 'n':
					newStr[j] = '\n';
					break;
				case 'r':
					newStr[j] = '\r';
					break;
				case 't':
					newStr[j] = '\t';
					break;
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
					{
						int			k;
						long		octVal = 0;

						for (k = 0;
							 s[i + k] >= '0' && s[i + k] <= '7' && k < 3;
							 k++)
							octVal = (octVal << 3) + (s[i + k] - '0');
						i += k - 1;
						newStr[j] = ((char) octVal);
					}
					break;
				default:
					newStr[j] = s[i];
					break;
			}					/* switch */
		}
		else if (s[i] == '\'' && s[i + 1] == '\'')
		{
			/* doubled quote becomes just one quote */
			newStr[j] = s[++i];
		}
		else
			newStr[j] = s[i];
		j++;
	}

	/* We copied the ending quote to newStr, so replace with \0 */
	Assert(j > 0 && j <= len);
	newStr[--j] = '\0';

	return newStr;
}
