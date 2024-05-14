/*-------------------------------------------------------------------------
 *
 * win32setlocale.c
 *		Wrapper to work around bugs in Windows setlocale() implementation
 *
 * Copyright (c) 2011-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/win32setlocale.c
 *
 *
 * The setlocale() function in Windows is broken in two ways. First, it
 * has a problem with locale names that have a dot in the country name. For
 * example:
 *
 * "Chinese (Traditional)_Hong Kong S.A.R..950"
 *
 * For some reason, setlocale() doesn't accept that as argument, even though
 * setlocale(LC_ALL, NULL) returns exactly that. Fortunately, it accepts
 * various alternative names for such countries, so to work around the broken
 * setlocale() function, we map the troublemaking locale names to accepted
 * aliases, before calling setlocale().
 *
 * The second problem is that the locale name for "Norwegian (Bokm&aring;l)"
 * contains a non-ASCII character. That's problematic, because it's not clear
 * what encoding the locale name itself is supposed to be in, when you
 * haven't yet set a locale. Also, it causes problems when the cluster
 * contains databases with different encodings, as the locale name is stored
 * in the pg_database system catalog. To work around that, when setlocale()
 * returns that locale name, map it to a pure-ASCII alias for the same
 * locale.
 *-------------------------------------------------------------------------
 */

#include "c.h"

#undef setlocale

#ifndef FRONTEND
#include "pg_config_paths.h"
#endif

/*
 * The path of a text file that can be created under PGDATA to override these
 * rules.  It allows locale names to be overridden on input to setlocale, and
 * on return from setlocale when querying the default.  This is intended as an
 * option of last resort for users whose system becomes unstartable due to an
 * operating system update that changes a locale name.
 *
 * # comments begin a hash sign
 * call pattern=replacement
 * return pattern=replacemnt
 *
 * The pattern syntax supports ? for any character (really byte), and * for
 * any sequence, though only one star can be used in the whole pattern.
 *
 * The encoding of the file is effectively undefined; setlocale() works with
 * the current Windows ACP, and PostgreSQL thinks the strings should be ASCII
 * or some undefined superset.  This could lead to some confusion if databases
 * have different encodings, so it's likely that replacements should use BCP47
 * tags if possible.
 */

typedef struct Win32LocaleTableEntry
{
	const char *pattern;
	const char *replacement;
} Win32LocaleTableEntry;

static const Win32LocaleTableEntry default_call_mapping_table[] = {
	/*
	 * "HKG" is listed here:
	 * http://msdn.microsoft.com/en-us/library/cdax410z%28v=vs.71%29.aspx
	 * (Country/Region Strings).
	 *
	 * "ARE" is the ISO-3166 three-letter code for U.A.E. It is not on the
	 * above list, but seems to work anyway.
	 */
	{"Hong Kong S.A.R.", "HKG"},
	{"U.A.E.", "ARE"},

	/*
	 * The ISO-3166 country code for Macau S.A.R. is MAC, but Windows doesn't
	 * seem to recognize that. And Macau isn't listed in the table of accepted
	 * abbreviations linked above. Fortunately, "ZHM" seems to be accepted as
	 * an alias for "Chinese (Traditional)_Macau S.A.R..950". I'm not sure
	 * where "ZHM" comes from, must be some legacy naming scheme. But hey, it
	 * works.
	 *
	 * Note that unlike HKG and ARE, ZHM is an alias for the *whole* locale
	 * name, not just the country part.
	 *
	 * Some versions of Windows spell it "Macau", others "Macao".
	 */
	{"Chinese (Traditional)_Maca? S.A.R..950", "ZHM"},
	{"Chinese_Maca? S.A.R..950", "ZHM"},

	{NULL}
};

static const Win32LocaleTableEntry return_mapping_table[] = {
	/*
	 * "Norwegian (Bokm&aring;l)" locale name contains the a-ring character.
	 * Map it to a pure-ASCII alias.
	 *
	 * It's not clear what encoding setlocale() uses when it returns the
	 * locale name, so to play it safe, we search for "Norwegian (Bok*l)".
	 *
	 * Just to make life even more complicated, some versions of Windows spell
	 * the locale name without parentheses.  Translate that too.
	 */
	{"Norwegian (Bokm*l)_Norway", "Norwegian_Norway"},
	{"Norwegian Bokm*l_Norway", "Norwegian_Norway"},

	{NULL}
};

static const Win32LocaleTableEntry *call_mapping_table;

/*
 * Parse a line of the mapping file.  Returns 0 on success. Squawks to stderr
 * on failure, but also sets the errno for setlocale() to fail with and
 * returns -1 in that case.
 */
static int
parse_line(const char *path,
		   char *line,
		   int line_number,
		   char **pattern,
		   char **replacement)
{
	const char *delimiter;
	size_t		len;

	/* Strip line endings. */
	while ((len = strlen(line)) > 0 &&
		   (line[len - 1] == '\r' || line[len - 1] == '\n'))
		line[len - 1] = '\0';

	/* Skip empty lines and shell-style comments. */
	if (line[0] == '\0' || line[0] == '#')
		return 0;

	/* Look for the equal sign followed by something. */
	delimiter = strchr(line, '=');
	if (!delimiter || delimiter[1] == '\0')
	{
		fprintf(stderr,
				"syntax error on line %d of \"%s\", expected pattern=replacement'\n",
				line_number, path);
		errno = EINVAL;
		return -1;
	}

	/* Copy the pattern. */
	len = delimiter - line;
	*pattern = malloc(len + 1);
	if (!*pattern)
	{
		errno = ENOMEM;
		return -1;
	}
	memcpy(*pattern, line, len);
	(*pattern)[len] = '\0';

	/* Copy the replacement. */
	*replacement = strdup(delimiter + 1);
	if (!*replacement)
	{
		free(*pattern);
		errno = ENOMEM;
		return -1;
	}

	return 0;
}

/*
 * Free a mapping table.  Only used for cleanup on failure, because otherwise
 * the mapping table is built once and sticks around until process exit.
 */
static void
free_call_mapping_table(Win32LocaleTableEntry *table)
{
	while (table->pattern)
	{
		free(unconstify(char *, table->pattern));
		free(unconstify(char *, table->replacement));
		table++;
	}
	free(table);
}

/*
 * Initialize call_mapping_table and call_mapping_table_size.  On failure,
 * errno is set and an error message is written to stderr.
 */
static int
initialize_call_mapping_table(void)
{
	FILE	   *file;
	const char *path;
	char		line[128];
	int			line_number;
	Win32LocaleTableEntry *table;
	size_t		table_size;
	size_t		table_capacity;

	/* Has the user specified a map file in the environment? */
	path = getenv("PG_SETLOCALE_MAP");

	/*
	 * In backends only, fall back to trying to find a file installed in the
	 * share directory.
	 */
	if (!path)
	{
#ifndef FRONTEND
		path = PGSHAREDIR "/setlocale.map";
#else
		/* Use defaults in frontend. */
		call_mapping_table = default_call_mapping_table;
		return 0;
#endif
	}

	/* If there is no mapping file at that path, use defaults. */
	file = fopen(path, "r");
	if (!file)
	{
		call_mapping_table = default_call_mapping_table;
		return 0;
	}

	/* Initial guess at required space. */
	table_size = 0;
	table_capacity = 16;
	table = malloc(sizeof(Win32LocaleTableEntry) * table_capacity);
	if (table == NULL)
	{
		errno = ENOMEM;
		return -1;
	}
	table->pattern = NULL;

	/* Read the file line-by-line. */
	line_number = 0;
	while (fgets(line, sizeof(line), file))
	{
		char	   *pattern = NULL;
		char	   *replacement;

		if (parse_line(path,
					   line,
					   line_number++,
					   &pattern,
					   &replacement) != 0)
		{
			int			save_errno = errno;

			free_call_mapping_table(table);
			fclose(file);
			errno = save_errno;
			return -1;
		}

		/* Skip blank/comments. */
		if (pattern == NULL)
			continue;

		/*
		 * Grow by doubling on demand.  Allow an extra entry because we want a
		 * null terminator.
		 */
		if (table_size + 1 == table_capacity)
		{
			Win32LocaleTableEntry *new_table;

			new_table = malloc(sizeof(*new_table) * table_capacity * 2);
			if (new_table == NULL)
			{
				free_call_mapping_table(table);
				fclose(file);
				errno = ENOMEM;
				return -1;
			}
			memcpy(new_table, table, sizeof(*table) * table_size);
			free(table);
			table = new_table;
			table_capacity *= 2;
		}

		/* Fill in new entry. */
		table[table_size].pattern = pattern;
		table[table_size].replacement = replacement;
		table_size++;

		table[table_size].pattern = NULL;
	}

	fclose(file);

	/* Mapping table established for this process. */
	call_mapping_table = table;

	return 0;
}

/*
 * Checks if n bytes of pattern and name match.  '?' is treated as a wildcard
 * in the pattern, but all other bytes must be identical to match.
 */
static bool
subpattern_matches(const char *pattern, const char *name, size_t n)
{
	while (n > 0)
	{
		/* Have we hit the end of the pattern or name? */
		if (*pattern == '\0')
			return *name == '\0';
		else if (*name == '\0')
			return false;

		/* Otherwise matches wildcard or exact character. */
		if (*pattern != '?' && *pattern != *name)
			return false;

		/* Next. */
		n--;
		pattern++;
		name++;
	}
	return true;
}

/*
 * Checks if a name matches a pattern, with an extremely simple pattern logic.
 * The pattern may contain any number of '?' characters to match any character,
 * and zero or one '*' characters to match any sequence of characters.
 */
static bool
pattern_matches(const char *pattern, const char *name)
{
	const char *star;

	if ((star = strchr(pattern, '*')))
	{
		size_t		len_pattern_before_star;
		size_t		len_pattern_after_star;
		size_t		len_name;

		/* Does the name match the part before the star? */
		len_pattern_before_star = star - pattern;
		if (!subpattern_matches(pattern, name, len_pattern_before_star))
			return false;

		/* Step over the star in the pattern. */
		pattern += len_pattern_before_star;
		pattern++;
		len_pattern_after_star = strlen(pattern);

		/* Step over the star in the name. */
		name += len_pattern_before_star;
		len_name = strlen(name);
		if (len_name < len_pattern_after_star)
			return false;
		name += len_name - len_pattern_after_star;
	}

	return subpattern_matches(pattern, name, SIZE_MAX);
}

/*
 * Convert a locale name using the given table, if it contains a matching
 * pattern.
 */
static const char *
map_locale(const Win32LocaleTableEntry *table, const char *name)
{
	while (table->pattern)
	{
		if (pattern_matches(table->pattern, name))
			return table->replacement;
		table++;
	}
	return name;
}

/*
 * This implementation sets errno and in some cases writes messages to stderr
 * for catastrophic internal failures, though the POSIX function defines no
 * errors so callers don't actually check errno.
 */
char *
pgwin32_setlocale(int category, const char *locale)
{
	const char *argument;
	char	   *result;

	if (!call_mapping_table)
	{
		if (initialize_call_mapping_table() < 0)
			return NULL;
	}

	/*
	 * XXX Call value transformation is relevant as long as we think there are
	 * existing systems that were initdb'd with unstable and non-ASCII locale
	 * names.
	 */
	if (locale == NULL)
		argument = NULL;
	else
		argument = map_locale(call_mapping_table, locale);

	/* Call the real setlocale() function */
	result = setlocale(category, argument);

	/*
	 * setlocale() is specified to return a "char *" that the caller is
	 * forbidden to modify, so casting away the "const" is innocuous.
	 *
	 * XXX Return value transformation is only relevant as long as we continue
	 * to use setlocale("") as a way to query the default locale names, which
	 * is the source of the unstable and non-ASCII locale names.
	 */
	if (result)
		result = unconstify(char *, map_locale(return_mapping_table, result));

	return result;
}
