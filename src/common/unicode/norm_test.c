/*-------------------------------------------------------------------------
 * norm_test.c
 *		Program to test Unicode normalization functions.
 *
 * Portions Copyright (c) 2017-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/unicode/norm_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/unicode_norm.h"

#include "norm_test_table.h"

static char *
print_wchar_str(const char32_t *s)
{
#define BUF_DIGITS 50
	static char buf[BUF_DIGITS * 11 + 1];
	int			i;
	char	   *p;

	i = 0;
	p = buf;
	while (*s && i < BUF_DIGITS)
	{
		p += sprintf(p, "U+%04X ", *s);
		i++;
		s++;
	}
	*p = '\0';

	return buf;
}

static int
pg_wcscmp(const char32_t *s1, const char32_t *s2)
{
	for (;;)
	{
		if (*s1 < *s2)
			return -1;
		if (*s1 > *s2)
			return 1;
		if (*s1 == 0)
			return 0;
		s1++;
		s2++;
	}
}

int
main(int argc, char **argv)
{
	const		pg_unicode_test *test;
	static const char32_t blocked_then_starter_input[] =
	{0x0078, 0x0301, 0x0061, 0x0301, 0};
	static const char32_t blocked_then_starter_nfc[] =
	{0x0078, 0x0301, 0x00E1, 0};
	static const struct
	{
		const char *name;
		UnicodeNormalizationForm form;
		const char32_t *input;
		const char32_t *output;
	}			extra_tests[] =
	{
		{
			"blocked mark before new starter",
			UNICODE_NFC,
			blocked_then_starter_input,
			blocked_then_starter_nfc
		},
		{0}
	};

	for (test = UnicodeNormalizationTests; test->input[0] != 0; test++)
	{
		for (int form = 0; form < 4; form++)
		{
			char32_t   *result;

			result = unicode_normalize(form, test->input);

			if (pg_wcscmp(test->output[form], result) != 0)
			{
				printf("FAILURE (NormalizationTest.txt line %d form %d):\n", test->linenum, form);
				printf("input:    %s\n", print_wchar_str(test->input));
				printf("expected: %s\n", print_wchar_str(test->output[form]));
				printf("got:      %s\n", print_wchar_str(result));
				printf("\n");
				exit(1);
			}
		}
	}

	for (int extra = 0; extra_tests[extra].name != NULL; extra++)
	{
		char32_t   *result;

		result = unicode_normalize(extra_tests[extra].form,
								   extra_tests[extra].input);

		if (result == NULL || pg_wcscmp(extra_tests[extra].output, result) != 0)
		{
			printf("FAILURE (extra test \"%s\" form %d):\n",
				   extra_tests[extra].name, extra_tests[extra].form);
			printf("input:    %s\n", print_wchar_str(extra_tests[extra].input));
			printf("expected: %s\n", print_wchar_str(extra_tests[extra].output));
			printf("got:      %s\n", result ? print_wchar_str(result) : "<null>");
			printf("\n");
			exit(1);
		}
	}

	printf("norm_test: All tests successful!\n");
	exit(0);
}
