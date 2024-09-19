/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities for libc
 *
 * Portions Copyright (c) 2002-2024, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/pg_locale_libc.c
 *
 *-----------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <wctype.h>

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/resowner.h"
#include "utils/syscache.h"

/*
 * This should be large enough that most strings will fit, but small enough
 * that we feel comfortable putting it on the stack
 */
#define		TEXTBUFLEN			1024

extern pg_locale_t libc_dat_create_locale(HeapTuple dattuple);
extern pg_locale_t libc_coll_create_locale(MemoryContext context,
										   ResourceOwner resowner,
										   HeapTuple colltuple);

static int strncoll_libc(const char *arg1, ssize_t len1,
						 const char *arg2, ssize_t len2,
						 pg_locale_t locale);
static size_t strnxfrm_libc(char *dest, size_t destsize,
							const char *src, ssize_t srclen,
							pg_locale_t locale);

static int char_props_libc(pg_wchar wc, int mask, pg_locale_t locale);
static pg_wchar toupper_libc(pg_wchar wc, pg_locale_t locale);
static pg_wchar tolower_libc(pg_wchar wc, pg_locale_t locale);

static void ResourceOwnerRememberLocaleT(ResourceOwner resowner,
										 locale_t locale);
static void ResOwnerReleaseLocaleT(Datum val);

static locale_t make_libc_collator(const char *collate, const char *ctype);

static void report_newlocale_failure(const char *localename);

static const ResourceOwnerDesc LocaleTResourceKind =
{
	.name = "locale_t reference",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_LAST,
	.ReleaseResource = ResOwnerReleaseLocaleT,
	.DebugPrint = NULL                      /* the default message is fine */
};

struct collate_methods libc_collate_methods = {
	.strncoll = strncoll_libc,
	.strnxfrm = strnxfrm_libc,
	.strnxfrm_prefix = NULL,
};

struct ctype_methods libc_ctype_methods = {
	.char_props = char_props_libc,
	.wc_toupper = toupper_libc,
	.wc_tolower = tolower_libc,
};

pg_locale_t
libc_dat_create_locale(HeapTuple dattuple)
{
	Form_pg_database dbform;
	Datum		datum;
	const char *datcollate;
	const char *datctype;
	pg_locale_t	result;

	dbform = (Form_pg_database) GETSTRUCT(dattuple);
	
	Assert(dbform->datlocprovider == COLLPROVIDER_LIBC);

	datum = SysCacheGetAttrNotNull(DATABASEOID, dattuple, Anum_pg_database_datcollate);
	datcollate = TextDatumGetCString(datum);
	datum = SysCacheGetAttrNotNull(DATABASEOID, dattuple, Anum_pg_database_datctype);
	datctype = TextDatumGetCString(datum);

	result = MemoryContextAllocZero(TopMemoryContext,
									sizeof(struct pg_locale_struct));

	result->provider = dbform->datlocprovider;
	result->deterministic = true;
	result->collate_is_c = (strcmp(datcollate, "C") == 0) ||
		(strcmp(datcollate, "POSIX") == 0);
	result->ctype_is_c = (strcmp(datctype, "C") == 0) ||
		(strcmp(datctype, "POSIX") == 0);

	if (!result->collate_is_c)
		result->collate = &libc_collate_methods;
	if (!result->ctype_is_c)
		result->ctype = &libc_ctype_methods;
	result->info.lt = make_libc_collator(datcollate, datctype);

	return result;
}

pg_locale_t
libc_coll_create_locale(MemoryContext context, ResourceOwner resowner,
						HeapTuple colltuple)
{
	Form_pg_collation collform;
	Datum		datum;
	const char *collcollate;
	const char *collctype;
	locale_t	locale;
	pg_locale_t result;

	collform = (Form_pg_collation) GETSTRUCT(colltuple);

	datum = SysCacheGetAttrNotNull(COLLOID, colltuple, Anum_pg_collation_collcollate);
	collcollate = TextDatumGetCString(datum);
	datum = SysCacheGetAttrNotNull(COLLOID, colltuple, Anum_pg_collation_collctype);
	collctype = TextDatumGetCString(datum);

	ResourceOwnerEnlarge(resowner);
	locale = make_libc_collator(collcollate, collctype);
	if (locale)
		ResourceOwnerRememberLocaleT(resowner, locale);

	result = MemoryContextAllocZero(context,
									sizeof(struct pg_locale_struct));

	result->provider = collform->collprovider;
	result->deterministic = collform->collisdeterministic;
	result->collate_is_c = (strcmp(collcollate, "C") == 0) ||
		(strcmp(collcollate, "POSIX") == 0);
	result->ctype_is_c = (strcmp(collctype, "C") == 0) ||
		(strcmp(collctype, "POSIX") == 0);
	if (!result->collate_is_c)
		result->collate = &libc_collate_methods;
	if (!result->ctype_is_c)
		result->ctype = &libc_ctype_methods;
	result->info.lt = locale;

	return result;
}

static void
ResourceOwnerRememberLocaleT(ResourceOwner resowner, locale_t locale)
{
	ResourceOwnerRemember(resowner, PointerGetDatum(locale),
						  &LocaleTResourceKind);
}

static void
ResOwnerReleaseLocaleT(Datum val)
{
	locale_t locale = (locale_t) DatumGetPointer(val);
#ifndef WIN32
	freelocale(locale);
#else
	_free_locale(locale);
#endif
}

static int
char_props_libc(pg_wchar wc, int mask, pg_locale_t locale)
{
	int result = 0;

	Assert(!locale->ctype_is_c);

	if (mask & PG_ISDIGIT)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswdigit_l((wint_t) wc, locale->info.lt))
				result |= PG_ISDIGIT;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				isdigit_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISDIGIT;
		}
	}
	if (mask & PG_ISALPHA)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswalpha_l((wint_t) wc, locale->info.lt))
				result |= PG_ISALPHA;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				isalpha_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISALPHA;
		}
	}
	if (mask & PG_ISUPPER)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswupper_l((wint_t) wc, locale->info.lt))
				result |= PG_ISUPPER;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				isupper_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISUPPER;
		}
	}
	if (mask & PG_ISLOWER)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswlower_l((wint_t) wc, locale->info.lt))
				result |= PG_ISLOWER;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				islower_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISLOWER;
		}
	}
	if (mask & PG_ISGRAPH)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswgraph_l((wint_t) wc, locale->info.lt))
				result |= PG_ISGRAPH;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				isgraph_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISGRAPH;
		}
	}
	if (mask & PG_ISPRINT)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswprint_l((wint_t) wc, locale->info.lt))
				result |= PG_ISPRINT;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				isprint_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISPRINT;
		}
	}
	if (mask & PG_ISPUNCT)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswpunct_l((wint_t) wc, locale->info.lt))
				result |= PG_ISPUNCT;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				ispunct_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISPUNCT;
		}
	}
	if (mask & PG_ISSPACE)
	{
		if (GetDatabaseEncoding() == PG_UTF8 &&
			(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		{
			if (iswspace_l((wint_t) wc, locale->info.lt))
				result |= PG_ISSPACE;
		}
		else
		{
			if (wc <= (pg_wchar) UCHAR_MAX &&
				isspace_l((unsigned char) wc, locale->info.lt))
				result |= PG_ISSPACE;
		}
	}

	return result;
}

static pg_wchar
toupper_libc(pg_wchar wc, pg_locale_t locale)
{
	if (GetDatabaseEncoding() == PG_UTF8 &&
		(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		return towupper_l((wint_t) wc, locale->info.lt);
	else if (wc <= (pg_wchar) UCHAR_MAX)
		return toupper_l((unsigned char) wc, locale->info.lt);
	else
		return wc;
}

static pg_wchar
tolower_libc(pg_wchar wc, pg_locale_t locale)
{
	if (GetDatabaseEncoding() == PG_UTF8 &&
		(sizeof(wchar_t) >= 4 || wc <= (pg_wchar) 0xFFFF))
		return towlower_l((wint_t) wc, locale->info.lt);
	else if (wc <= (pg_wchar) UCHAR_MAX)
		return tolower_l((unsigned char) wc, locale->info.lt);
	else
		return wc;
}

/*
 * Create a locale_t with the given collation and ctype.
 *
 * The "C" and "POSIX" locales are not actually handled by libc, so return
 * NULL.
 *
 * Ensure that no path leaks a locale_t.
 */
static locale_t
make_libc_collator(const char *collate, const char *ctype)
{
	locale_t	loc = 0;

	if (strcmp(collate, ctype) == 0)
	{
		if (strcmp(ctype, "C") != 0 && strcmp(ctype, "POSIX") != 0)
		{
			/* Normal case where they're the same */
			errno = 0;
#ifndef WIN32
			loc = newlocale(LC_COLLATE_MASK | LC_CTYPE_MASK, collate,
							NULL);
#else
			loc = _create_locale(LC_ALL, collate);
#endif
			if (!loc)
				report_newlocale_failure(collate);
		}
	}
	else
	{
#ifndef WIN32
		/* We need two newlocale() steps */
		locale_t	loc1 = 0;

		if (strcmp(collate, "C") != 0 && strcmp(collate, "POSIX") != 0)
		{
			errno = 0;
			loc1 = newlocale(LC_COLLATE_MASK, collate, NULL);
			if (!loc1)
				report_newlocale_failure(collate);
		}

		if (strcmp(ctype, "C") != 0 && strcmp(ctype, "POSIX") != 0)
		{
			errno = 0;
			loc = newlocale(LC_CTYPE_MASK, ctype, loc1);
			if (!loc)
			{
				if (loc1)
					freelocale(loc1);
				report_newlocale_failure(ctype);
			}
		}
		else
			loc = loc1;
#else

		/*
		 * XXX The _create_locale() API doesn't appear to support this. Could
		 * perhaps be worked around by changing pg_locale_t to contain two
		 * separate fields.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("collations with different collate and ctype values are not supported on this platform")));
#endif
	}

	return loc;
}

/*
 * strncoll_libc_win32_utf8
 *
 * Win32 does not have UTF-8. Convert UTF8 arguments to wide characters and
 * invoke wcscoll_l().
 *
 * An input string length of -1 means that it's NUL-terminated.
 */
#ifdef WIN32
static int
strncoll_libc_win32_utf8(const char *arg1, ssize_t len1, const char *arg2,
						 ssize_t len2, pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	char	   *a1p,
			   *a2p;
	int			a1len;
	int			a2len;
	int			r;
	int			result;

	Assert(locale->provider == COLLPROVIDER_LIBC);
	Assert(GetDatabaseEncoding() == PG_UTF8);
#ifndef WIN32
	Assert(false);
#endif

	if (len1 == -1)
		len1 = strlen(arg1);
	if (len2 == -1)
		len2 = strlen(arg2);

	a1len = len1 * 2 + 2;
	a2len = len2 * 2 + 2;

	if (a1len + a2len > TEXTBUFLEN)
		buf = palloc(a1len + a2len);

	a1p = buf;
	a2p = buf + a1len;

	/* API does not work for zero-length input */
	if (len1 == 0)
		r = 0;
	else
	{
		r = MultiByteToWideChar(CP_UTF8, 0, arg1, len1,
								(LPWSTR) a1p, a1len / 2);
		if (!r)
			ereport(ERROR,
					(errmsg("could not convert string to UTF-16: error code %lu",
							GetLastError())));
	}
	((LPWSTR) a1p)[r] = 0;

	if (len2 == 0)
		r = 0;
	else
	{
		r = MultiByteToWideChar(CP_UTF8, 0, arg2, len2,
								(LPWSTR) a2p, a2len / 2);
		if (!r)
			ereport(ERROR,
					(errmsg("could not convert string to UTF-16: error code %lu",
							GetLastError())));
	}
	((LPWSTR) a2p)[r] = 0;

	errno = 0;
	result = wcscoll_l((LPWSTR) a1p, (LPWSTR) a2p, locale->info.lt);
	if (result == 2147483647)	/* _NLSCMPERROR; missing from mingw headers */
		ereport(ERROR,
				(errmsg("could not compare Unicode strings: %m")));

	if (buf != sbuf)
		pfree(buf);

	return result;
}
#endif							/* WIN32 */

/*
 * strncoll_libc
 *
 * An input string length of -1 means that it's NUL-terminated.
 */
static int
strncoll_libc(const char *arg1, ssize_t len1, const char *arg2, ssize_t len2,
			  pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	size_t		bufsize1 = (len1 == -1) ? 0 : len1 + 1;
	size_t		bufsize2 = (len2 == -1) ? 0 : len2 + 1;
	const char *arg1n;
	const char *arg2n;
	int			result;

	Assert(locale->provider == COLLPROVIDER_LIBC);

#ifdef WIN32
	/* check for this case before doing the work for nul-termination */
	if (GetDatabaseEncoding() == PG_UTF8)
		return strncoll_libc_win32_utf8(arg1, len1, arg2, len2, locale);
#endif							/* WIN32 */

	if (bufsize1 + bufsize2 > TEXTBUFLEN)
		buf = palloc(bufsize1 + bufsize2);

	/* nul-terminate arguments if necessary */
	if (len1 == -1)
	{
		arg1n = arg1;
	}
	else
	{
		char *buf1 = buf;
		memcpy(buf1, arg1, len1);
		buf1[len1] = '\0';
		arg1n = buf1;
	}

	if (len2 == -1)
	{
		arg2n = arg2;
	}
	else
	{
		char *buf2 = buf + bufsize1;
		memcpy(buf2, arg2, len2);
		buf2[len2] = '\0';
		arg2n = buf2;
	}

	result = strcoll_l(arg1n, arg2n, locale->info.lt);

	if (buf != sbuf)
		pfree(buf);

	return result;
}

static size_t
strnxfrm_libc(char *dest, size_t destsize, const char *src, ssize_t srclen,
			  pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	size_t		bufsize = srclen + 1;
	size_t		result;

	Assert(locale->provider == COLLPROVIDER_LIBC);

	if (srclen == -1)
		return strxfrm_l(dest, src, destsize, locale->info.lt);

	if (bufsize > TEXTBUFLEN)
		buf = palloc(bufsize);

	/* nul-terminate argument */
	memcpy(buf, src, srclen);
	buf[srclen] = '\0';

	result = strxfrm_l(dest, buf, destsize, locale->info.lt);

	if (buf != sbuf)
		pfree(buf);

	/* if dest is defined, it should be nul-terminated */
	Assert(result >= destsize || dest[result] == '\0');

	return result;
}

/* simple subroutine for reporting errors from newlocale() */
static void
report_newlocale_failure(const char *localename)
{
	int			save_errno;

	/*
	 * Windows doesn't provide any useful error indication from
	 * _create_locale(), and BSD-derived platforms don't seem to feel they
	 * need to set errno either (even though POSIX is pretty clear that
	 * newlocale should do so).  So, if errno hasn't been set, assume ENOENT
	 * is what to report.
	 */
	if (errno == 0)
		errno = ENOENT;

	/*
	 * ENOENT means "no such locale", not "no such file", so clarify that
	 * errno with an errdetail message.
	 */
	save_errno = errno;			/* auxiliary funcs might change errno */
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("could not create locale \"%s\": %m",
					localename),
			 (save_errno == ENOENT ?
			  errdetail("The operating system could not find any locale data for the locale name \"%s\".",
						localename) : 0)));
}

