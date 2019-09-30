/*-------------------------------------------------------------------------
 *
 * option.c
 *	  argument parsing helpers for frontend code
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/fe_utils/option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "fe_utils/option.h"

pg_strtoint_status
pg_strtoint64_range(const char *str, int64 *result,
					int64 min, int64 max, char **error)
{
	int64		temp;
	pg_strtoint_status s = pg_strtoint64(str, &temp);

	if (s == PG_STRTOINT_OK && (temp < min || temp > max))
		s = PG_STRTOINT_RANGE_ERROR;

	switch (s)
	{
		case PG_STRTOINT_OK:
			*result = temp;
			break;
		case PG_STRTOINT_SYNTAX_ERROR:
			*error = psprintf("could not parse '%s' as integer", str);
			break;
		case PG_STRTOINT_RANGE_ERROR:
			*error = psprintf("%s is outside range "
							  INT64_FORMAT ".." INT64_FORMAT,
							  str, min, max);
			break;
		default:
			pg_unreachable();
	}
	return s;
}
