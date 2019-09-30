/*
 *	option.h
 *		argument parsing helpers for frontend code
 *
 *	Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *	src/include/fe_utils/option.h
 */
#ifndef FE_OPTION_H
#define FE_OPTION_H

#include "common/string.h"

/*
 * Parses string as int64 like pg_strtoint64, but fails
 * with PG_STRTOINT_RANGE_ERROR if the result is outside
 * the range min .. max inclusive.
 *
 * On failure, creates user-friendly error message with
 * psprintf, and assigns it to the error output parameter.
 */
pg_strtoint_status
pg_strtoint64_range(const char *str, int64 *result,
					int64 min, int64 max, char **error);

#endif							/* FE_OPTION_H */
