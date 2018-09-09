/*-------------------------------------------------------------------------
 *
 * chklocale.c
 *		Functions for handling locale-related info
 *
 *
 * Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/port/chklocale.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif

#ifdef USE_ICU
#include <unicode/ucol.h>
#endif

#include "catalog/pg_collation.h"
#include "common/pg_collation_fn_common.h"
#include "mb/pg_wchar.h"

/*
 * In backend, we will use palloc/pfree.  In frontend, use malloc/free.
 */
#ifndef FRONTEND
#define STRDUP(s) pstrdup(s)
#define ALLOC(size) palloc(size)
#define FREE(s) pfree(s)
#else
#define STRDUP(s) strdup(s)
#define ALLOC(size) malloc(size)
#define FREE(s) free(s)
#endif

/*
 * This table needs to recognize all the CODESET spellings for supported
 * backend encodings, as well as frontend-only encodings where possible
 * (the latter case is currently only needed for initdb to recognize
 * error situations).  On Windows, we rely on entries for codepage
 * numbers (CPnnn).
 *
 * Note that we search the table with pg_strcasecmp(), so variant
 * capitalizations don't need their own entries.
 */
struct encoding_match
{
	enum pg_enc pg_enc_code;
	const char *system_enc_name;
};

static const struct encoding_match encoding_match_list[] = {
	{PG_EUC_JP, "EUC-JP"},
	{PG_EUC_JP, "eucJP"},
	{PG_EUC_JP, "IBM-eucJP"},
	{PG_EUC_JP, "sdeckanji"},
	{PG_EUC_JP, "CP20932"},

	{PG_EUC_CN, "EUC-CN"},
	{PG_EUC_CN, "eucCN"},
	{PG_EUC_CN, "IBM-eucCN"},
	{PG_EUC_CN, "GB2312"},
	{PG_EUC_CN, "dechanzi"},
	{PG_EUC_CN, "CP20936"},

	{PG_EUC_KR, "EUC-KR"},
	{PG_EUC_KR, "eucKR"},
	{PG_EUC_KR, "IBM-eucKR"},
	{PG_EUC_KR, "deckorean"},
	{PG_EUC_KR, "5601"},
	{PG_EUC_KR, "CP51949"},

	{PG_EUC_TW, "EUC-TW"},
	{PG_EUC_TW, "eucTW"},
	{PG_EUC_TW, "IBM-eucTW"},
	{PG_EUC_TW, "cns11643"},
	/* No codepage for EUC-TW ? */

	{PG_UTF8, "UTF-8"},
	{PG_UTF8, "utf8"},
	{PG_UTF8, "CP65001"},

	{PG_LATIN1, "ISO-8859-1"},
	{PG_LATIN1, "ISO8859-1"},
	{PG_LATIN1, "iso88591"},
	{PG_LATIN1, "CP28591"},

	{PG_LATIN2, "ISO-8859-2"},
	{PG_LATIN2, "ISO8859-2"},
	{PG_LATIN2, "iso88592"},
	{PG_LATIN2, "CP28592"},

	{PG_LATIN3, "ISO-8859-3"},
	{PG_LATIN3, "ISO8859-3"},
	{PG_LATIN3, "iso88593"},
	{PG_LATIN3, "CP28593"},

	{PG_LATIN4, "ISO-8859-4"},
	{PG_LATIN4, "ISO8859-4"},
	{PG_LATIN4, "iso88594"},
	{PG_LATIN4, "CP28594"},

	{PG_LATIN5, "ISO-8859-9"},
	{PG_LATIN5, "ISO8859-9"},
	{PG_LATIN5, "iso88599"},
	{PG_LATIN5, "CP28599"},

	{PG_LATIN6, "ISO-8859-10"},
	{PG_LATIN6, "ISO8859-10"},
	{PG_LATIN6, "iso885910"},

	{PG_LATIN7, "ISO-8859-13"},
	{PG_LATIN7, "ISO8859-13"},
	{PG_LATIN7, "iso885913"},

	{PG_LATIN8, "ISO-8859-14"},
	{PG_LATIN8, "ISO8859-14"},
	{PG_LATIN8, "iso885914"},

	{PG_LATIN9, "ISO-8859-15"},
	{PG_LATIN9, "ISO8859-15"},
	{PG_LATIN9, "iso885915"},
	{PG_LATIN9, "CP28605"},

	{PG_LATIN10, "ISO-8859-16"},
	{PG_LATIN10, "ISO8859-16"},
	{PG_LATIN10, "iso885916"},

	{PG_KOI8R, "KOI8-R"},
	{PG_KOI8R, "CP20866"},

	{PG_KOI8U, "KOI8-U"},
	{PG_KOI8U, "CP21866"},

	{PG_WIN866, "CP866"},
	{PG_WIN874, "CP874"},
	{PG_WIN1250, "CP1250"},
	{PG_WIN1251, "CP1251"},
	{PG_WIN1251, "ansi-1251"},
	{PG_WIN1252, "CP1252"},
	{PG_WIN1253, "CP1253"},
	{PG_WIN1254, "CP1254"},
	{PG_WIN1255, "CP1255"},
	{PG_WIN1256, "CP1256"},
	{PG_WIN1257, "CP1257"},
	{PG_WIN1258, "CP1258"},

	{PG_ISO_8859_5, "ISO-8859-5"},
	{PG_ISO_8859_5, "ISO8859-5"},
	{PG_ISO_8859_5, "iso88595"},
	{PG_ISO_8859_5, "CP28595"},

	{PG_ISO_8859_6, "ISO-8859-6"},
	{PG_ISO_8859_6, "ISO8859-6"},
	{PG_ISO_8859_6, "iso88596"},
	{PG_ISO_8859_6, "CP28596"},

	{PG_ISO_8859_7, "ISO-8859-7"},
	{PG_ISO_8859_7, "ISO8859-7"},
	{PG_ISO_8859_7, "iso88597"},
	{PG_ISO_8859_7, "CP28597"},

	{PG_ISO_8859_8, "ISO-8859-8"},
	{PG_ISO_8859_8, "ISO8859-8"},
	{PG_ISO_8859_8, "iso88598"},
	{PG_ISO_8859_8, "CP28598"},

	{PG_SJIS, "SJIS"},
	{PG_SJIS, "PCK"},
	{PG_SJIS, "CP932"},
	{PG_SJIS, "SHIFT_JIS"},

	{PG_BIG5, "BIG5"},
	{PG_BIG5, "BIG5HKSCS"},
	{PG_BIG5, "Big5-HKSCS"},
	{PG_BIG5, "CP950"},

	{PG_GBK, "GBK"},
	{PG_GBK, "CP936"},

	{PG_UHC, "UHC"},
	{PG_UHC, "CP949"},

	{PG_JOHAB, "JOHAB"},
	{PG_JOHAB, "CP1361"},

	{PG_GB18030, "GB18030"},
	{PG_GB18030, "CP54936"},

	{PG_SHIFT_JIS_2004, "SJIS_2004"},

	{PG_SQL_ASCII, "US-ASCII"},

	{PG_SQL_ASCII, NULL}		/* end marker */
};

#ifdef WIN32
/*
 * On Windows, use CP<code page number> instead of the nl_langinfo() result
 *
 * Visual Studio 2012 expanded the set of valid LC_CTYPE values, so have its
 * locale machinery determine the code page.  See comments at IsoLocaleName().
 * For other compilers, follow the locale's predictable format.
 *
 * Visual Studio 2015 should still be able to do the same, but the declaration
 * of lc_codepage is missing in _locale_t, causing this code compilation to
 * fail, hence this falls back instead on GetLocaleInfoEx. VS 2015 may be an
 * exception and post-VS2015 versions should be able to handle properly the
 * codepage number using _create_locale(). So, instead of the same logic as
 * VS 2012 and VS 2013, this routine uses GetLocaleInfoEx to parse short
 * locale names like "de-DE", "fr-FR", etc. If those cannot be parsed correctly
 * process falls back to the pre-VS-2010 manual parsing done with
 * using <Language>_<Country>.<CodePage> as a base.
 *
 * Returns a malloc()'d string for the caller to free.
 */
static char *
win32_langinfo(const char *ctype)
{
	char	   *r = NULL;

#if (_MSC_VER >= 1700) && (_MSC_VER < 1900)
	_locale_t	loct = NULL;

	loct = _create_locale(LC_CTYPE, ctype);
	if (loct != NULL)
	{
		r = malloc(16);			/* excess */
		if (r != NULL)
			sprintf(r, "CP%u", loct->locinfo->lc_codepage);
		_free_locale(loct);
	}
#else
	char	   *codepage;

#if (_MSC_VER >= 1900)
	uint32		cp;
	WCHAR		wctype[LOCALE_NAME_MAX_LENGTH];

	memset(wctype, 0, sizeof(wctype));
	MultiByteToWideChar(CP_ACP, 0, ctype, -1, wctype, LOCALE_NAME_MAX_LENGTH);

	if (GetLocaleInfoEx(wctype,
						LOCALE_IDEFAULTANSICODEPAGE | LOCALE_RETURN_NUMBER,
						(LPWSTR) &cp, sizeof(cp) / sizeof(WCHAR)) > 0)
	{
		r = malloc(16);			/* excess */
		if (r != NULL)
			sprintf(r, "CP%u", cp);
	}
	else
#endif
	{
		/*
		 * Locale format on Win32 is <Language>_<Country>.<CodePage> . For
		 * example, English_United States.1252.
		 */
		codepage = strrchr(ctype, '.');
		if (codepage != NULL)
		{
			int			ln;

			codepage++;
			ln = strlen(codepage);
			r = malloc(ln + 3);
			if (r != NULL)
				sprintf(r, "CP%s", codepage);
		}

	}
#endif

	return r;
}

#ifndef FRONTEND
/*
 * Given a Windows code page identifier, find the corresponding PostgreSQL
 * encoding.  Issue a warning and return -1 if none found.
 */
int
pg_codepage_to_encoding(UINT cp)
{
	char		sys[16];
	int			i;

	sprintf(sys, "CP%u", cp);

	/* Check the table */
	for (i = 0; encoding_match_list[i].system_enc_name; i++)
		if (pg_strcasecmp(sys, encoding_match_list[i].system_enc_name) == 0)
			return encoding_match_list[i].pg_enc_code;

	ereport(WARNING,
			(errmsg("could not determine encoding for codeset \"%s\"", sys)));

	return -1;
}
#endif
#endif							/* WIN32 */

#if (defined(HAVE_LANGINFO_H) && defined(CODESET)) || defined(WIN32)

/*
 * Given a setting for LC_CTYPE, return the Postgres ID of the associated
 * encoding, if we can determine it.  Return -1 if we can't determine it.
 *
 * Pass in NULL to get the encoding for the current locale setting.
 * Pass "" to get the encoding selected by the server's environment.
 *
 * If the result is PG_SQL_ASCII, callers should treat it as being compatible
 * with any desired encoding.
 *
 * If running in the backend and write_message is false, this function must
 * cope with the possibility that elog() and palloc() are not yet usable.
 */
int
pg_get_encoding_from_locale(const char *ctype, bool write_message)
{
	char	   *sys;
	int			i;

	/* Get the CODESET property, and also LC_CTYPE if not passed in */
	if (ctype)
	{
		char	   *save;
		char	   *name;

		/* If locale is C or POSIX, we can allow all encodings */
		if (pg_strcasecmp(ctype, "C") == 0 ||
			pg_strcasecmp(ctype, "POSIX") == 0)
			return PG_SQL_ASCII;

		save = setlocale(LC_CTYPE, NULL);
		if (!save)
			return -1;			/* setlocale() broken? */
		/* must copy result, or it might change after setlocale */
		save = strdup(save);
		if (!save)
			return -1;			/* out of memory; unlikely */

		name = setlocale(LC_CTYPE, ctype);
		if (!name)
		{
			free(save);
			return -1;			/* bogus ctype passed in? */
		}

#ifndef WIN32
		sys = nl_langinfo(CODESET);
		if (sys)
			sys = strdup(sys);
#else
		sys = win32_langinfo(name);
#endif

		setlocale(LC_CTYPE, save);
		free(save);
	}
	else
	{
		/* much easier... */
		ctype = setlocale(LC_CTYPE, NULL);
		if (!ctype)
			return -1;			/* setlocale() broken? */

		/* If locale is C or POSIX, we can allow all encodings */
		if (pg_strcasecmp(ctype, "C") == 0 ||
			pg_strcasecmp(ctype, "POSIX") == 0)
			return PG_SQL_ASCII;

#ifndef WIN32
		sys = nl_langinfo(CODESET);
		if (sys)
			sys = strdup(sys);
#else
		sys = win32_langinfo(ctype);
#endif
	}

	if (!sys)
		return -1;				/* out of memory; unlikely */

	/* Check the table */
	for (i = 0; encoding_match_list[i].system_enc_name; i++)
	{
		if (pg_strcasecmp(sys, encoding_match_list[i].system_enc_name) == 0)
		{
			free(sys);
			return encoding_match_list[i].pg_enc_code;
		}
	}

	/* Special-case kluges for particular platforms go here */

#ifdef __darwin__

	/*
	 * Current macOS has many locales that report an empty string for CODESET,
	 * but they all seem to actually use UTF-8.
	 */
	if (strlen(sys) == 0)
	{
		free(sys);
		return PG_UTF8;
	}
#endif

	/*
	 * We print a warning if we got a CODESET string but couldn't recognize
	 * it.  This means we need another entry in the table.
	 */
	if (write_message)
	{
#ifdef FRONTEND
		fprintf(stderr, _("could not determine encoding for locale \"%s\": codeset is \"%s\""),
				ctype, sys);
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(WARNING,
				(errmsg("could not determine encoding for locale \"%s\": codeset is \"%s\"",
						ctype, sys)));
#endif
	}

	free(sys);
	return -1;
}
#else							/* (HAVE_LANGINFO_H && CODESET) || WIN32 */

/*
 * stub if no multi-language platform support
 *
 * Note: we could return -1 here, but that would have the effect of
 * forcing users to specify an encoding to initdb on such platforms.
 * It seems better to silently default to SQL_ASCII.
 */
int
pg_get_encoding_from_locale(const char *ctype, bool write_message)
{
	return PG_SQL_ASCII;
}

#endif							/* (HAVE_LANGINFO_H && CODESET) || WIN32 */

/* do not make libpq with icu */
#ifndef LIBPQ_MAKE

/*
 * Check if the locale contains the modifier of the collation provider.
 *
 * Set up the collation provider according to the appropriate modifier or '\0'.
 * Set up the collation version to NULL if we don't find it after the collation
 * provider modifier.
 *
 * The malloc'd copy of the locale's canonical name without the modifier of the
 * collation provider and the collation version is stored in the canonname if
 * locale is not NULL. The canoname can have the zero length.
 */
void
check_locale_collprovider(const char *locale, char **canonname,
						  char *collprovider, char **collversion)
{
	const char *modifier_sign,
			   *dot_sign,
			   *cur_collprovider_end;
	char		cur_collprovider_name[NAMEDATALEN];
	int			cur_collprovider_len;
	char		cur_collprovider;

	/* in case of failure or if we don't find them in the locale name */
	if (canonname)
		*canonname = NULL;
	if (collprovider)
		*collprovider = '\0';
	if (collversion)
		*collversion = NULL;

	if (!locale)
		return;

	/* find the last occurrence of the modifier sign '@' in the locale */
	modifier_sign = strrchr(locale, '@');

	if (!modifier_sign)
	{
		/* just copy all the name */
		if (canonname)
			*canonname = STRDUP(locale);
		 return;
	}

	/* check if there's a version after the collation provider modifier */
	if ((dot_sign = strchr(modifier_sign, '.')) == NULL)
		cur_collprovider_end = &locale[strlen(locale)];
	else
		cur_collprovider_end = dot_sign;

	cur_collprovider_len = cur_collprovider_end - modifier_sign - 1;
	if (cur_collprovider_len + 1 > NAMEDATALEN)
	{
#ifdef FRONTEND
		fprintf(stderr, _("collation provider name is too long: %s"), locale);
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else							/* not FRONTEND */
		ereport(ERROR,
				(errmsg("collation provider name is too long: %s", locale)));
#endif							/* not FRONTEND */
		return;
	}

	strncpy(cur_collprovider_name, modifier_sign + 1, cur_collprovider_len);
	cur_collprovider_name[cur_collprovider_len] = '\0';

	/* check if this is a valid collprovider name */
	cur_collprovider = get_collprovider(cur_collprovider_name);
	if (is_valid_nondefault_collprovider(cur_collprovider))
	{
		if (collprovider)
			*collprovider = cur_collprovider;

		if (canonname)
		{
			int			canonname_len = modifier_sign - locale;

			*canonname = ALLOC((canonname_len + 1) * sizeof(char));
			if (*canonname)
			{
				strncpy(*canonname, locale, canonname_len);
				(*canonname)[canonname_len] = '\0';
			}
			else
			{
#ifdef FRONTEND
				fprintf(stderr, _("out of memory"));
				/*
				 * keep newline separate so there's only one translatable string
				 */
				fputc('\n', stderr);
#else							/* not FRONTEND */
				ereport(ERROR, (errmsg("out of memory")));
#endif							/* not FRONTEND */
			}
		}

		if (dot_sign && collversion)
			*collversion = STRDUP(dot_sign + 1);
	}
	else
	{
		/* just copy all the name */
		if (canonname)
			*canonname = STRDUP(locale);
	}
}

/*
 * Return true if locale is "C" or "POSIX";
 */
bool
locale_is_c(const char *locale)
{
	return locale && (strcmp(locale, "C") == 0 || strcmp(locale, "POSIX") == 0);
}

/*
 * Return locale ended with collation provider modifier and collation version.
 *
 * Return NULL if locale is NULL.
 */
char *
get_full_collation_name(const char *locale, char collprovider,
						const char *collversion)
{
	char	   *new_locale;
	int			old_len,
				len_with_provider,
				new_len;
	const char *collprovider_name;

	if (!locale)
		return NULL;

	collprovider_name = get_collprovider_name(collprovider);
	Assert(collprovider_name);

	old_len = strlen(locale);
	new_len = len_with_provider = old_len + 1 + strlen(collprovider_name);
	if (collversion && *collversion)
		new_len += 1 + strlen(collversion);

	new_locale = ALLOC((new_len + 1) * sizeof(char));
	if (!new_locale)
	{
#ifdef FRONTEND
		fprintf(stderr, _("out of memory"));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else							/* not FRONTEND */
		ereport(ERROR, (errmsg("out of memory")));
#endif							/* not FRONTEND */

		return NULL;
	}

	/* add the collation provider modifier */
	strcpy(new_locale, locale);
	new_locale[old_len] = '@';
	strcpy(&new_locale[old_len + 1], collprovider_name);

	/* add the collation version if needed */
	if (collversion && *collversion)
	{
		new_locale[len_with_provider] = '.';
		strcpy(&new_locale[len_with_provider + 1], collversion);
	}

	new_locale[new_len] = '\0';

	return new_locale;
}

/*
 * Get provider-specific collation version string for the given collation from
 * the operating system/library.
 *
 * A particular provider must always either return a non-NULL string or return
 * NULL (if it doesn't support versions).  It must not return NULL for some
 * collcollate and not NULL for others.
 */
#ifdef FRONTEND
void
get_collation_actual_version(char collprovider, const char *collcollate,
							 char **collversion, bool *failure)
{
	if (failure)
		*failure = false;

#ifdef USE_ICU
	if (collprovider == COLLPROVIDER_ICU)
	{
		UCollator  *collator = open_collator(collcollate);
		UVersionInfo versioninfo;
		char		buf[U_MAX_VERSION_STRING_LENGTH];

		if (collator)
		{
			ucol_getVersion(collator, versioninfo);
			ucol_close(collator);

			u_versionToString(versioninfo, buf);
			if (collversion)
				*collversion = STRDUP(buf);
		}
		else
		{
			if (collversion)
				*collversion = NULL;
			if (failure)
				*failure = true;
		}
	}
	else
#endif
	{
		if (collversion)
			*collversion = NULL;
	}
}
#else							/* not FRONTEND */
char *
get_collation_actual_version(char collprovider, const char *collcollate)
{
	char	   *collversion;

#ifdef USE_ICU
	if (collprovider == COLLPROVIDER_ICU)
	{
		UCollator  *collator = open_collator(collcollate);
		UVersionInfo versioninfo;
		char		buf[U_MAX_VERSION_STRING_LENGTH];

		ucol_getVersion(collator, versioninfo);
		ucol_close(collator);

		u_versionToString(versioninfo, buf);
		collversion = STRDUP(buf);
	}
	else
#endif
		collversion = NULL;

	return collversion;
}
#endif							/* not FRONTEND */

#ifdef USE_ICU
/*
 * Open the collator for this icu locale. Return NULL in case of failure.
 */
UCollator *
open_collator(const char *collate)
{
	UCollator  *collator;
	UErrorCode	status;
	const char *save = uloc_getDefault();
	char	   *save_dup;

	if (!save)
	{
#ifdef FRONTEND
		fprintf(stderr, _("ICU error: uloc_getDefault() failed"));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(ERROR, (errmsg("ICU error: uloc_getDefault() failed")));
#endif
		return NULL;
	}

	/* save may be pointing at a modifiable scratch variable, so copy it. */
	save_dup = STRDUP(save);

	/* set the default locale to root */
	status = U_ZERO_ERROR;
	uloc_setDefault(ICU_ROOT_LOCALE, &status);
	if (U_FAILURE(status))
	{
#ifdef FRONTEND
		fprintf(stderr, _("ICU error: failed to set the default locale to \"%s\": %s"),
				ICU_ROOT_LOCALE, u_errorName(status));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(ERROR,
				(errmsg("ICU error: failed to set the default locale to \"%s\": %s",
						ICU_ROOT_LOCALE, u_errorName(status))));
#endif
		return NULL;
	}

	/* get a collator for this collate */
	status = U_ZERO_ERROR;
	collator = ucol_open(collate, &status);
	if (U_FAILURE(status))
	{
#ifdef FRONTEND
		fprintf(stderr, _("ICU error: could not open collator for locale \"%s\": %s"),
				collate, u_errorName(status));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(ERROR,
				(errmsg("ICU error: could not open collator for locale \"%s\": %s",
						collate, u_errorName(status))));
#endif
		collator = NULL;
	}

	/* restore old value of the default locale. */
	status = U_ZERO_ERROR;
	uloc_setDefault(save_dup, &status);
	if (U_FAILURE(status))
	{
#ifdef FRONTEND
		fprintf(stderr, _("ICU error: failed to restore old locale \"%s\": %s"),
				save_dup, u_errorName(status));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(ERROR,
				(errmsg("ICU error: failed to restore old locale \"%s\": %s",
						save_dup, u_errorName(status))));
#endif
		return NULL;
	}
	FREE(save_dup);

	return collator;
}

/*
 * Get the ICU language tag for a locale name.
 * The result is a palloc'd string.
 * Return NULL in case of failure or if localename is NULL.
 */
char *
get_icu_language_tag(const char *localename)
{
	char		buf[ULOC_FULLNAME_CAPACITY];
	UErrorCode	status = U_ZERO_ERROR;

	if (!localename)
		return NULL;

	uloc_toLanguageTag(localename, buf, sizeof(buf), TRUE, &status);
	if (U_FAILURE(status))
	{
#ifdef FRONTEND
		fprintf(stderr,
				_("ICU error: could not convert locale name \"%s\" to language tag: %s"),
				localename, u_errorName(status));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(ERROR,
				(errmsg("ICU error: could not convert locale name \"%s\" to language tag: %s",
						localename, u_errorName(status))));
#endif
		return NULL;
	}
	return STRDUP(buf);
}

/*
 * Get the icu collation name.
 */
const char *
get_icu_collate(const char *locale, const char *langtag)
{
	return U_ICU_VERSION_MAJOR_NUM >= 54 ? langtag : locale;
}

#ifdef WIN32
/*
 * Get the Language Code Identifier (LCID) for the Windows locale.
 *
 * Return zero in case of failure.
 */
static uint32
get_lcid(const wchar_t *winlocale)
{
	/*
	 * The second argument to the LocaleNameToLCID function is:
	 * - Prior to Windows 7: reserved; should always be 0.
	 * - Beginning in Windows 7: use LOCALE_ALLOW_NEUTRAL_NAMES to allow the
	 *   return of lcids of locales without regions.
	 */
#if (NTDDI_VERSION >= NTDDI_WIN7)
	return LocaleNameToLCID(winlocale, LOCALE_ALLOW_NEUTRAL_NAMES);
#else
	return LocaleNameToLCID(winlocale, 0);
#endif
}

/*
 * char2wchar_ascii --- convert multibyte characters to wide characters
 *
 * This is a simplified version of the char2wchar() function from backend.
 */
static size_t
char2wchar_ascii(wchar_t *to, size_t tolen, const char *from, size_t fromlen)
{
	size_t		result;

	if (tolen == 0)
		return 0;

	/* Win32 API does not work for zero-length input */
	if (fromlen == 0)
		result = 0;
	else
	{
		result = MultiByteToWideChar(CP_ACP, 0, from, fromlen, to, tolen - 1);
		/* A zero return is failure */
		if (result == 0)
			result = -1;
	}

	if (result != -1)
	{
		Assert(result < tolen);
		/* Append trailing null wchar (MultiByteToWideChar() does not) */
		to[result] = 0;
	}

	return result;
}

/*
 * Get the canonical ICU name for the Windows locale.
 *
 * Return a malloc'd string or NULL in case of failure.
 */
char *
check_icu_winlocale(const char *winlocale)
{
	uint32		lcid;
	char		canonname_buf[ULOC_FULLNAME_CAPACITY];
	UErrorCode	status = U_ZERO_ERROR;
#if (_MSC_VER >= 1400)			/* VC8.0 or later */
	_locale_t	loct = NULL;
#endif

	if (winlocale == NULL)
		return NULL;

	/* Get the Language Code Identifier (LCID). */

#if (_MSC_VER >= 1400)			/* VC8.0 or later */
	loct = _create_locale(LC_COLLATE, winlocale);

	if (loct != NULL)
	{
#if (_MSC_VER >= 1700)			/* Visual Studio 2012 or later */
		if ((lcid = get_lcid(loct->locinfo->locale_name[LC_COLLATE])) == 0)
		{
			/* there's an error */
#ifdef FRONTEND
			fprintf(stderr,
					_("failed to get the Language Code Identifier (LCID) for locale \"%s\""),
					winlocale);
			/* keep newline separate so there's only one translatable string */
			fputc('\n', stderr);
#else							/* not FRONTEND */
			ereport(ERROR,
					(errmsg("failed to get the Language Code Identifier (LCID) for locale \"%s\"",
							winlocale)));
#endif							/* not FRONTEND */
			_free_locale(loct);
			return NULL;
		}
#else							/* _MSC_VER >= 1400 && _MSC_VER < 1700 */
		if ((lcid = loct->locinfo->lc_handle[LC_COLLATE]) == 0)
		{
			/* there's an error */
#ifdef FRONTEND
			fprintf(stderr,
					_("failed to get the Language Code Identifier (LCID) for locale \"%s\""),
					winlocale);
			/* keep newline separate so there's only one translatable string */
			fputc('\n', stderr);
#else							/* not FRONTEND */
			ereport(ERROR,
					(errmsg("failed to get the Language Code Identifier (LCID) for locale \"%s\"",
							winlocale)));
#endif							/* not FRONTEND */
			_free_locale(loct);
			return NULL;
		}
#endif							/* _MSC_VER >= 1400 && _MSC_VER < 1700 */
		_free_locale(loct);
	}
	else
#endif							/* VC8.0 or later */
	{
		if (strlen(winlocale) == 0)
		{
			lcid = LOCALE_USER_DEFAULT;
		}
		else
		{
			size_t		locale_len = strlen(winlocale);
			wchar_t	   *wlocale = (wchar_t*) ALLOC(
				(locale_len + 1) * sizeof(wchar_t));
			/* Locale names use only ASCII */
			size_t		locale_wlen = char2wchar_ascii(wlocale, locale_len + 1,
													   winlocale, locale_len);
			if (locale_wlen == -1)
			{
				/* there's an error */
#ifdef FRONTEND
				fprintf(stderr,
						_("failed to convert locale \"%s\" to wide characters"),
						winlocale);
				/* keep newline separate so there's only one translatable string */
				fputc('\n', stderr);
#else
				ereport(ERROR,
						(errmsg("failed to convert locale \"%s\" to wide characters",
								winlocale)));
#endif
				FREE(wlocale);
				return NULL;
			}

			if ((lcid = get_lcid(wlocale)) == 0)
			{
				/* there's an error */
#ifdef FRONTEND
				fprintf(stderr,
						_("failed to get the Language Code Identifier (LCID) for locale \"%s\""),
						winlocale);
				/* keep newline separate so there's only one translatable string */
				fputc('\n', stderr);
#else
				ereport(ERROR,
						(errmsg("failed to get the Language Code Identifier (LCID) for locale \"%s\"",
								winlocale)));
#endif
				FREE(wlocale);
				return NULL;
			}

			FREE(wlocale);
		}
	}

	/* Get the ICU canoname. */

	uloc_getLocaleForLCID(lcid, canonname_buf, sizeof(canonname_buf), &status);
	if (U_FAILURE(status))
	{
#ifdef FRONTEND
		fprintf(stderr,
				_("ICU error: failed to get the locale name for LCID 0x%04x: %s"),
				lcid, u_errorName(status));
		/* keep newline separate so there's only one translatable string */
		fputc('\n', stderr);
#else
		ereport(ERROR,
				(errmsg("ICU error: failed to get the locale name for LCID 0x%04x: %s",
						lcid, u_errorName(status))));
#endif
		return NULL;
	}

	return STRDUP(canonname_buf);
}
#endif							/* WIN32 */
#endif							/* USE_ICU */

#endif							/* not LIBPQ_MAKE */
