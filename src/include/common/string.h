/*
 *	string.h
 *		string handling helpers
 *
 *	Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/include/common/string.h
 */
#ifndef COMMON_STRING_H
#define COMMON_STRING_H

extern bool pg_str_endswith(const char *str, const char *end);
extern int	strtoint(const char *pg_restrict str, char **pg_restrict endptr,
					 int base);

/*
 * Set of routines for conversion from string to various integer types.
 */
typedef enum pg_strtoint_status
{
	PG_STRTOINT_OK,
	PG_STRTOINT_SYNTAX_ERROR,
	PG_STRTOINT_RANGE_ERROR
} pg_strtoint_status;

extern pg_strtoint_status pg_strtoint16(const char *str, int16 *result);
extern pg_strtoint_status pg_strtoint32(const char *str, int32 *result);
extern pg_strtoint_status pg_strtoint64(const char *str, int64 *result);
extern pg_strtoint_status pg_strtouint16(const char *str, uint16 *result);
extern pg_strtoint_status pg_strtouint32(const char *str, uint32 *result);
extern pg_strtoint_status pg_strtouint64(const char *str, uint64 *result);

extern void pg_clean_ascii(char *str);
extern int	pg_strip_crlf(char *str);

#endif							/* COMMON_STRING_H */
