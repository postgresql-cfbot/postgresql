/*-------------------------------------------------------------------------
 *
 * test_json_parser_incremental.c
 *    Test program for incremental JSON parser
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/test/modules/test_json_parser/test_json_parser_incremental.c
 *
 * This progam tests incremental parsing of json. The input is fed into
 * the parser in very small chunks. In practice you would normally use
 * much larger chunks, but doing this makes it more likely that the
 * full range of incement handling, especially in the lexer, is exercised.
 *
 * The argument specifies the file containing the JSON input.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "common/jsonapi.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include <stdio.h>

int
main(int argc, char **argv)
{
	/* max delicious line length is less than this */
	char		buff[6001];
	FILE	   *json_file;
	JsonParseErrorType result;
	JsonLexContext lex;
	StringInfoData json;
	int			n_read;

	makeJsonLexContextIncremental(&lex, PG_UTF8, false);
	initStringInfo(&json);

	json_file = fopen(argv[1], "r");
	while ((n_read = fread(buff, 1, 60, json_file)) > 0)
	{
		appendBinaryStringInfo(&json, buff, n_read);
		if (!feof(json_file))
		{
			result = pg_parse_json_incremental(&lex, &nullSemAction,
											   json.data, json.len,
											   false);
			if (result != JSON_INCOMPLETE)
			{
				fprintf(stderr,
						"unexpected result %d (expecting %d) on token %s\n",
						result, JSON_INCOMPLETE, lex.token_start);
				exit(1);
			}
			resetStringInfo(&json);
		}
		else
		{
			result = pg_parse_json_incremental(&lex, &nullSemAction,
											   json.data, json.len,
											   true);
			if (result != JSON_SUCCESS)
			{
				fprintf(stderr,
						"unexpected result %d (expecting %d) on final chunk\n",
						result, JSON_SUCCESS);
				exit(1);
			}
			printf("SUCCESS!\n");
			break;
		}
	}
	fclose(json_file);
	exit(0);
}
