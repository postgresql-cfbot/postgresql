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
 * If the "-c SIZE" option is provided, that chunk size is used instead.
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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

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
	size_t		chunk_size = 60;
	struct stat statbuf;
	off_t		bytes_left;

	if (strcmp(argv[1], "-c") == 0)
	{
		sscanf(argv[2], "%zu", &chunk_size);
		argv += 2;
	}

	makeJsonLexContextIncremental(&lex, PG_UTF8, false);
	initStringInfo(&json);

	json_file = fopen(argv[1], "r");
	fstat(fileno(json_file), &statbuf);
	bytes_left = statbuf.st_size;

	for (;;)
	{
		n_read = fread(buff, 1, chunk_size, json_file);
		appendBinaryStringInfo(&json, buff, n_read);
		appendStringInfoString(&json, "1+23 trailing junk");
		bytes_left -= n_read;
		if (bytes_left > 0)
		{
			result = pg_parse_json_incremental(&lex, &nullSemAction,
											   json.data, n_read,
											   false);
			if (result != JSON_INCOMPLETE)
			{
				fprintf(stderr, "%s\n", json_errdetail(result, &lex));
				exit(1);
			}
			resetStringInfo(&json);
		}
		else
		{
			result = pg_parse_json_incremental(&lex, &nullSemAction,
											   json.data, n_read,
											   true);
			if (result != JSON_SUCCESS)
			{
				fprintf(stderr, "%s\n", json_errdetail(result, &lex));
				exit(1);
			}
			printf("SUCCESS!\n");
			break;
		}
	}
	fclose(json_file);
	exit(0);
}
