/*-------------------------------------------------------------------------
 *
 * string.c
 *		string handling helpers
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/string.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/int.h"
#include "common/string.h"


/*
 * Returns whether the string `str' has the postfix `end'.
 */
bool
pg_str_endswith(const char *str, const char *end)
{
	size_t		slen = strlen(str);
	size_t		elen = strlen(end);

	/* can't be a postfix if longer */
	if (elen > slen)
		return false;

	/* compare the end of the strings */
	str += slen - elen;
	return strcmp(str, end) == 0;
}


/*
 * strtoint --- just like strtol, but returns int not long
 */
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
	long		val;

	val = strtol(str, endptr, base);
	if (val != (int) val)
		errno = ERANGE;
	return (int) val;
}


/*
 * pg_strtoint16
 *
 * Convert input string to a signed 16-bit integer.  Allows any number of
 * leading or trailing whitespace characters.
 *
 * NB: Accumulate input as a negative number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * positive number.
 *
 * The function returns immediately if the conversion failed with a status
 * value to let the caller handle the error.  On success, the result is
 * stored in "*result".
 */
pg_strtoint_status
pg_strtoint16(const char *s, int16 *result)
{
	const char *ptr = s;
	int16		tmp = 0;
	bool		neg = false;

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* process digits, we know that we have one ahead per the last check */
	do
	{
		int8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_s16_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_sub_s16_overflow(tmp, digit, &tmp)))
			return PG_STRTOINT_RANGE_ERROR;
	}
	while (isdigit((unsigned char) *ptr));

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		return PG_STRTOINT_SYNTAX_ERROR;

	if (!neg)
	{
		/* could fail if input is most negative number */
		if (unlikely(tmp == PG_INT16_MIN))
			return PG_STRTOINT_RANGE_ERROR;
		tmp = -tmp;
	}

	*result = tmp;
	return PG_STRTOINT_OK;
}

/*
 * pg_strtoint32
 *
 * Same as previously, for 32-bit signed integer.
 */
pg_strtoint_status
pg_strtoint32(const char *s, int32 *result)
{
	const char *ptr = s;
	int32		tmp = 0;
	bool		neg = false;

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* process digits, we know that we have one ahead per the last check */
	do
	{
		int8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_s32_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_sub_s32_overflow(tmp, digit, &tmp)))
			return PG_STRTOINT_RANGE_ERROR;
	}
	while (isdigit((unsigned char) *ptr));

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		return PG_STRTOINT_SYNTAX_ERROR;

	if (!neg)
	{
		/* could fail if input is most negative number */
		if (unlikely(tmp == PG_INT32_MIN))
			return PG_STRTOINT_RANGE_ERROR;
		tmp = -tmp;
	}

	*result = tmp;
	return PG_STRTOINT_OK;
}

/*
 * pg_strtoint64
 *
 * Same as previously, for 64-bit signed integer.
 */
pg_strtoint_status
pg_strtoint64(const char *str, int64 *result)
{
	const char *ptr = str;
	int64		tmp = 0;
	bool		neg = false;

	/*
	 * Do our own scan, rather than relying on sscanf which might be broken
	 * for long long.
	 *
	 * As INT64_MIN can't be stored as a positive 64 bit integer, accumulate
	 * value as a negative number.
	 */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* process digits, we know that we have one ahead per the last check */
	do
	{
		int64		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_s64_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_sub_s64_overflow(tmp, digit, &tmp)))
			return PG_STRTOINT_RANGE_ERROR;
	}
	while (isdigit((unsigned char) *ptr));

	/* allow trailing whitespace, but not other trailing chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		return PG_STRTOINT_SYNTAX_ERROR;

	if (!neg)
	{
		if (unlikely(tmp == PG_INT64_MIN))
			return PG_STRTOINT_RANGE_ERROR;
		tmp = -tmp;
	}

	*result = tmp;
	return PG_STRTOINT_OK;
}

/*
 * pg_strtouint16
 *
 * Convert input string to an unsigned 16-bit integer.  Allows any number of
 * leading or trailing whitespace characters.
 *
 * The function returns immediately if the conversion failed with a status
 * value to let the caller handle the error.  On success, the result is
 * stored in "*result".
 */
pg_strtoint_status
pg_strtouint16(const char *str, uint16 *result)
{
	const char *ptr = str;
	uint16		tmp = 0;

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '+')
		ptr++;
	else if (unlikely(*ptr == '-'))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* process digits, we know that we have one ahead per the last check */
	do
	{
		uint8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_u16_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_add_u16_overflow(tmp, digit, &tmp)))
			return PG_STRTOINT_RANGE_ERROR;
	}
	while (isdigit((unsigned char) *ptr));

	/* allow trailing whitespace */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* but not other trailing chars */
	if (unlikely(*ptr != '\0'))
		return PG_STRTOINT_SYNTAX_ERROR;

	*result = tmp;
	return PG_STRTOINT_OK;
}

/*
 * pg_strtouint32
 *
 * Same as previously, for 32-bit unsigned integer.
 */
pg_strtoint_status
pg_strtouint32(const char *str, uint32 *result)
{
	const char *ptr = str;
	uint32		tmp = 0;

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '+')
		ptr++;
	else if (unlikely(*ptr == '-'))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* process digits, we know that we have one ahead per the last check */
	do
	{
		uint8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_u32_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_add_u32_overflow(tmp, digit, &tmp)))
			return PG_STRTOINT_RANGE_ERROR;
	}
	while (isdigit((unsigned char) *ptr));

	/* allow trailing whitespace */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* but not other trailing chars */
	if (unlikely(*ptr != '\0'))
		return PG_STRTOINT_SYNTAX_ERROR;

	*result = tmp;
	return PG_STRTOINT_OK;
}

/*
 * pg_strtouint64
 *
 * Same as previously, for 64-bit unsigned integer.
 */
pg_strtoint_status
pg_strtouint64(const char *str, uint64 *result)
{
	const char *ptr = str;
	uint64		tmp = 0;

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '+')
		ptr++;
	else if (unlikely(*ptr == '-'))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		return PG_STRTOINT_SYNTAX_ERROR;

	/* process digits, we know that we have one ahead per the last check */
	do
	{
		uint64		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_u64_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_add_u64_overflow(tmp, digit, &tmp)))
			return PG_STRTOINT_RANGE_ERROR;
	}
	while (isdigit((unsigned char) *ptr));

	/* allow trailing whitespace */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* but not other trailing chars */
	if (unlikely(*ptr != '\0'))
		return PG_STRTOINT_SYNTAX_ERROR;

	*result = tmp;
	return PG_STRTOINT_OK;
}

/*
 * pg_clean_ascii -- Replace any non-ASCII chars with a '?' char
 *
 * Modifies the string passed in which must be '\0'-terminated.
 *
 * This function exists specifically to deal with filtering out
 * non-ASCII characters in a few places where the client can provide an almost
 * arbitrary string (and it isn't checked to ensure it's a valid username or
 * database name or similar) and we don't want to have control characters or other
 * things ending up in the log file where server admins might end up with a
 * messed up terminal when looking at them.
 *
 * In general, this function should NOT be used- instead, consider how to handle
 * the string without needing to filter out the non-ASCII characters.
 *
 * Ultimately, we'd like to improve the situation to not require stripping out
 * all non-ASCII but perform more intelligent filtering which would allow UTF or
 * similar, but it's unclear exactly what we should allow, so stick to ASCII only
 * for now.
 */
void
pg_clean_ascii(char *str)
{
	/* Only allow clean ASCII chars in the string */
	char	   *p;

	for (p = str; *p != '\0'; p++)
	{
		if (*p < 32 || *p > 126)
			*p = '?';
	}
}


/*
 * pg_strip_crlf -- Remove any trailing newline and carriage return
 *
 * Removes any trailing newline and carriage return characters (\r on
 * Windows) in the input string, zero-terminating it.
 *
 * The passed in string must be zero-terminated.  This function returns
 * the new length of the string.
 */
int
pg_strip_crlf(char *str)
{
	int			len = strlen(str);

	while (len > 0 && (str[len - 1] == '\n' ||
					   str[len - 1] == '\r'))
		str[--len] = '\0';

	return len;
}
