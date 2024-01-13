/*-------------------------------------------------------------------------
 * category_test.c
 *		Program to test Unicode general category and character class
 *		functions.
 *
 * Portions Copyright (c) 2017-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/unicode/category_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef USE_ICU
#include <unicode/uchar.h>
#endif
#include <wctype.h>

#include "common/unicode_version.h"
#include "common/unicode_category.h"

#define LIBC_LOCALE "C.UTF-8"

static int	pg_unicode_version = 0;
#ifdef USE_ICU
static int	icu_unicode_version = 0;
#endif

/*
 * Parse version into integer for easy comparison.
 */
static int
parse_unicode_version(const char *version)
{
	int			n PG_USED_FOR_ASSERTS_ONLY;
	int			major;
	int			minor;

	n = sscanf(version, "%d.%d", &major, &minor);

	Assert(n == 2);
	Assert(minor < 100);

	return major * 100 + minor;
}

#ifdef USE_ICU
/*
 * Test Postgres Unicode tables by comparing with ICU. Test the General
 * Category, as well as the properties Alphabetic, Lowercase, Uppercase,
 * White_Space, and Hex_Digit.
 */
static void
icu_test()
{
	int			successful = 0;
	int			pg_skipped_codepoints = 0;
	int			icu_skipped_codepoints = 0;

	for (pg_wchar code = 0; code <= 0x10ffff; code++)
	{
		uint8_t		pg_category = unicode_category(code);
		uint8_t		icu_category = u_charType(code);

		/* Property tests */
		bool		prop_alphabetic = pg_u_prop_alphabetic(code);
		bool		prop_lowercase = pg_u_prop_lowercase(code);
		bool		prop_uppercase = pg_u_prop_uppercase(code);
		bool		prop_white_space = pg_u_prop_white_space(code);
		bool		prop_hex_digit = pg_u_prop_hex_digit(code);
		bool		prop_join_control = pg_u_prop_join_control(code);

		bool		icu_prop_alphabetic = u_hasBinaryProperty(
			code, UCHAR_ALPHABETIC);
		bool		icu_prop_lowercase =  u_hasBinaryProperty(
			code, UCHAR_LOWERCASE);
		bool		icu_prop_uppercase =  u_hasBinaryProperty(
			code, UCHAR_UPPERCASE);
		bool		icu_prop_white_space =  u_hasBinaryProperty(
			code, UCHAR_WHITE_SPACE);
		bool		icu_prop_hex_digit =  u_hasBinaryProperty(
			code, UCHAR_HEX_DIGIT);
		bool		icu_prop_join_control =  u_hasBinaryProperty(
			code, UCHAR_JOIN_CONTROL);

		/*
		 * Compare with ICU for TR #18 character classes using:
		 *
		 * https://unicode-org.github.io/icu-docs/apidoc/dev/icu4c/uchar_8h.html#details
		 *
		 * which describes how to use ICU to test for membership in regex
		 * character classes ("Standard", not "POSIX Compatible").
		 *
		 * NB: the document suggests testing for some properties such as
		 * UCHAR_POSIX_ALNUM, but that doesn't mean that we're testing for the
		 * "POSIX Compatible" character classes.
		 */
		bool		isalpha = pg_u_isalpha(code);
		bool		islower = pg_u_islower(code);
		bool		isupper = pg_u_isupper(code);
		bool		ispunct = pg_u_ispunct(code, false);
		bool		isdigit = pg_u_isdigit(code, false);
		bool		isxdigit = pg_u_isxdigit(code, false);
		bool		isalnum = pg_u_isalnum(code, false);
		bool		isspace = pg_u_isspace(code);
		bool		isblank = pg_u_isblank(code);
		bool		iscntrl = pg_u_iscntrl(code);
		bool		isgraph = pg_u_isgraph(code);
		bool		isprint = pg_u_isprint(code);

		bool		icu_isalpha = u_isUAlphabetic(code);
		bool		icu_islower = u_isULowercase(code);
		bool		icu_isupper = u_isUUppercase(code);
		bool		icu_ispunct = u_ispunct(code);
		bool		icu_isdigit = u_isdigit(code);
		bool		icu_isxdigit = u_hasBinaryProperty(code,
													   UCHAR_POSIX_XDIGIT);
		bool		icu_isalnum = u_hasBinaryProperty(code,
													  UCHAR_POSIX_ALNUM);
		bool		icu_isspace = u_isUWhiteSpace(code);
		bool		icu_isblank = u_isblank(code);
		bool		icu_iscntrl = icu_category == PG_U_CONTROL;
		bool		icu_isgraph = u_hasBinaryProperty(code,
													  UCHAR_POSIX_GRAPH);
		bool		icu_isprint = u_hasBinaryProperty(code,
													  UCHAR_POSIX_PRINT);

		/*
		 * A version mismatch means that some assigned codepoints in the newer
		 * version may be unassigned in the older version. That's OK, though
		 * the test will not cover those codepoints marked unassigned in the
		 * older version (that is, it will no longer be an exhaustive test).
		 */
		if (pg_category == PG_U_UNASSIGNED &&
			icu_category != PG_U_UNASSIGNED &&
			pg_unicode_version < icu_unicode_version)
		{
			pg_skipped_codepoints++;
			continue;
		}

		if (icu_category == PG_U_UNASSIGNED &&
			pg_category != PG_U_UNASSIGNED &&
			icu_unicode_version < pg_unicode_version)
		{
			icu_skipped_codepoints++;
			continue;
		}

		if (pg_category != icu_category)
		{
			printf("category_test: FAILURE for codepoint 0x%06x\n", code);
			printf("category_test: Postgres category:	%02d %s %s\n", pg_category,
				   unicode_category_abbrev(pg_category),
				   unicode_category_string(pg_category));
			printf("category_test: ICU category:		%02d %s %s\n", icu_category,
				   unicode_category_abbrev(icu_category),
				   unicode_category_string(icu_category));
			printf("\n");
			exit(1);
		}

		if (prop_alphabetic != icu_prop_alphabetic ||
			prop_lowercase != icu_prop_lowercase ||
			prop_uppercase != icu_prop_uppercase ||
			prop_white_space != icu_prop_white_space ||
			prop_hex_digit != icu_prop_hex_digit ||
			prop_join_control != icu_prop_join_control)
		{
			printf("category_test: FAILURE for codepoint 0x%06x\n", code);
			printf("category_test: Postgres	property	alphabetic/lowercase/uppercase/white_space/hex_digit/join_control: %d/%d/%d/%d/%d/%d\n",
				   prop_alphabetic, prop_lowercase, prop_uppercase,
				   prop_white_space, prop_hex_digit, prop_join_control);
			printf("category_test: ICU	property	alphabetic/lowercase/uppercase/white_space/hex_digit/join_control: %d/%d/%d/%d/%d/%d\n",
				   icu_prop_alphabetic, icu_prop_lowercase, icu_prop_uppercase,
				   icu_prop_white_space, icu_prop_hex_digit, icu_prop_join_control);
			printf("\n");
			exit(1);
		}

		if (isalpha != icu_isalpha ||
			islower != icu_islower ||
			isupper != icu_isupper ||
			ispunct != icu_ispunct ||
			isdigit != icu_isdigit ||
			isxdigit != icu_isxdigit ||
			isalnum != icu_isalnum ||
			isspace != icu_isspace ||
			isblank != icu_isblank ||
			iscntrl != icu_iscntrl ||
			isgraph != icu_isgraph ||
			isprint != icu_isprint)
		{
			printf("category_test: FAILURE for codepoint 0x%06x\n", code);
			printf("category_test: Postgres	class	alpha/lower/upper/punct/digit/xdigit/alnum/space/blank/cntrl/graph/print: %d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d\n",
				   isalpha, islower, isupper, ispunct, isdigit, isxdigit, isalnum, isspace, isblank, iscntrl, isgraph, isprint);
			printf("category_test: ICU class	alpha/lower/upper/punct/digit/xdigit/alnum/space/blank/cntrl/graph/print: %d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d\n",
				   icu_isalpha, icu_islower, icu_isupper, icu_ispunct, icu_isdigit, icu_isxdigit, icu_isalnum, icu_isspace, icu_isblank, icu_iscntrl, icu_isgraph, icu_isprint);
			printf("\n");
			exit(1);
		}

		if (pg_category != PG_U_UNASSIGNED)
			successful++;
	}

	if (pg_skipped_codepoints > 0)
		printf("category_test: skipped %d codepoints unassigned in Postgres due to Unicode version mismatch\n",
			   pg_skipped_codepoints);
	if (icu_skipped_codepoints > 0)
		printf("category_test: skipped %d codepoints unassigned in ICU due to Unicode version mismatch\n",
			   icu_skipped_codepoints);

	printf("category_test: ICU test: %d codepoints successful\n", successful);
}
#endif

/*
 * For libc, test only some characters for membership in the punctuation
 * class. We have no guarantee that all characters will obey the same rules as
 * pg_u_ispunct_posix(), though some coverage is still useful.
 */
static const unsigned char test_punct[] = {
	',', '$', '"', 0x85, 0x00, 'b', '&', 'Z', ' ', '\t', '\n'
};

/*
 * Test what we can for libc, which is limited but still useful to cover the
 * _posix-variant functions.
 */
static void
libc_test()
{
	char * libc_locale = setlocale(LC_CTYPE, LIBC_LOCALE);

	if (!libc_locale)
	{
		printf("category_test: libc locale \"%s\" not available; skipping\n", LIBC_LOCALE);
		return;
	}

	/* non-exhaustive test of pg_u_ispunct_posix() */
	for (int i = 0; i < sizeof(test_punct)/sizeof(test_punct[0]); i++)
	{
		pg_wchar code = (pg_wchar) test_punct[i];
		bool ispunct = pg_u_ispunct(code, true);
		bool libc_ispunct = iswpunct(code);

		if (ispunct != libc_ispunct)
		{
			printf("category_test: FAILURE for codepoint 0x%06x\n", code);
			printf("category_test: Postgres	ispunct_posix:	%d\n", ispunct);
			printf("category_test: libc iswpunct:		%d\n", libc_ispunct);
			printf("\n");
			exit(1);
		}
	}

	for (pg_wchar code = 0; code <= 0x10ffff; code++)
	{
		uint8_t		pg_category = unicode_category(code);

		bool		isalpha = pg_u_isalpha(code);
		bool		isdigit = pg_u_isdigit(code, true);
		bool		isxdigit = pg_u_isxdigit(code, true);
		bool		isalnum = pg_u_isalnum(code, true);

		bool		libc_isdigit = iswdigit(code);
		bool		libc_isxdigit = iswxdigit(code);

		if (pg_category == PG_U_UNASSIGNED)
			continue;

		/* check that alnum is the same as isdigit OR isalpha */
		if (((isdigit || isalpha) && !isalnum) ||
			(!(isdigit || isalpha) && isalnum))
		{
			printf("category_test: FAILURE for codepoint 0x%06x\n", code);
			printf("category_test: isalnum inconsistent: isalpha/isdigit/isalnum: %d/%d/%d\n",
				   isalpha, isdigit, isalnum);
			exit(1);
		}

		if (isdigit != libc_isdigit ||
			isxdigit != libc_isxdigit)
		{
			printf("category_test: FAILURE for codepoint 0x%06x\n", code);
			printf("category_test: Postgres	class	digit/xdigit: %d/%d\n",
				   isdigit, isxdigit);
			printf("category_test: libc class	digit/xdigit: %d/%d\n",
				   libc_isdigit, libc_isxdigit);
			printf("\n");
			exit(1);
		}
	}
}

int
main(int argc, char **argv)
{
	pg_unicode_version = parse_unicode_version(PG_UNICODE_VERSION);
	printf("category_test: Postgres Unicode version:\t%s\n", PG_UNICODE_VERSION);

	libc_test();

#ifdef USE_ICU
	icu_unicode_version = parse_unicode_version(U_UNICODE_VERSION);
	printf("category_test: ICU Unicode version:\t\t%s\n", U_UNICODE_VERSION);

	icu_test();
#else
	printf("category_test: ICU not available; skipping\n");
#endif
}
