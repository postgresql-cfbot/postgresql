/*-------------------------------------------------------------------------
 * case_test.c
 *		Program to test Unicode case mapping functions.
 *
 * Portions Copyright (c) 2017-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/unicode/case_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wctype.h>

#ifdef USE_ICU
#include <unicode/uchar.h>
#endif
#include "common/unicode_case.h"
#include "common/unicode_category.h"
#include "common/unicode_version.h"

/*
 * We expect that C.UTF-8 has the same CTYPE behavior as the simple unicode
 * mappings, but that's not guaranteed. If there are failures in the libc
 * test, that's useful information, but does not necessarily indicate a
 * problem.
 */
#define LIBC_LOCALE "C.UTF-8"

#ifdef USE_ICU

/* use root locale for test */
#define ICU_LOCALE "und"

static void
icu_test_simple(pg_wchar code)
{
	pg_wchar	lower = unicode_lowercase_simple(code);
	pg_wchar	title = unicode_titlecase_simple(code);
	pg_wchar	upper = unicode_uppercase_simple(code);
	pg_wchar	iculower = u_tolower(code);
	pg_wchar	icutitle = u_totitle(code);
	pg_wchar	icuupper = u_toupper(code);

	if (lower != iculower || title != icutitle || upper != icuupper)
	{
		printf("case_test: FAILURE for codepoint 0x%06x\n", code);
		printf("case_test: Postgres lower/title/upper:	0x%06x/0x%06x/0x%06x\n",
			   lower, title, upper);
		printf("case_test: ICU lower/title/upper:		0x%06x/0x%06x/0x%06x\n",
			   iculower, icutitle, icuupper);
		printf("\n");
		exit(1);
	}
}
#endif

static void
libc_test_simple(pg_wchar code)
{
	pg_wchar	lower = unicode_lowercase_simple(code);
	pg_wchar	upper = unicode_uppercase_simple(code);
	wchar_t		libclower = towlower(code);
	wchar_t		libcupper = towupper(code);

	if (lower != libclower || upper != libcupper)
	{
		printf("case_test: FAILURE for codepoint 0x%06x\n", code);
		printf("case_test: Postgres lower/upper:	0x%06x/0x%06x\n",
			   lower, upper);
		printf("case_test: libc lower/upper:		0x%06x/0x%06x\n",
			   libclower, libcupper);
		printf("\n");
		exit(1);
	}
}

/*
 * Exhaustively compare case mappings with the results from libc and ICU.
 */
int
main(int argc, char **argv)
{
	int		 libc_successful = 0;
#ifdef USE_ICU
	int		 icu_successful	 = 0;
#endif
	char	*libc_locale;

	libc_locale = setlocale(LC_CTYPE, LIBC_LOCALE);

	printf("case_test: Postgres Unicode version:\t%s\n", PG_UNICODE_VERSION);
#ifdef USE_ICU
	printf("case_test: ICU Unicode version:\t\t%s\n", U_UNICODE_VERSION);
#else
	printf("case_test: ICU not available; skipping\n");
#endif

	if (libc_locale)
	{
		printf("case_test: comparing with libc locale \"%s\"\n", libc_locale);
		for (pg_wchar code = 0; code <= 0x10ffff; code++)
		{
			pg_unicode_category category = unicode_category(code);

			if (category != PG_U_UNASSIGNED && category != PG_U_SURROGATE)
			{
				libc_test_simple(code);
				libc_successful++;
			}
		}
		printf("case_test: libc simple mapping test: %d codepoints successful\n",
			   libc_successful);
	}
	else
		printf("case_test: libc locale \"%s\" not available; skipping\n",
			   LIBC_LOCALE);

#ifdef USE_ICU
	for (pg_wchar code = 0; code <= 0x10ffff; code++)
	{
		pg_unicode_category category = unicode_category(code);

		if (category != PG_U_UNASSIGNED && category != PG_U_SURROGATE)
		{
			icu_test_simple(code);
			icu_successful++;
		}
	}
	printf("case_test: ICU simple mapping test: %d codepoints successful\n",
		   icu_successful);
#endif

	exit(0);
}
