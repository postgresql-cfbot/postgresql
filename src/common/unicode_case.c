/*-------------------------------------------------------------------------
 * unicode_case.c
 *		Conversion to upper or lower case.
 *
 * Portions Copyright (c) 2017-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/unicode_case.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/unicode_case.h"
#include "common/unicode_case_table.h"

/* find entry in simple case map, if any */
static inline const pg_simple_case_map *
find_case_map(pg_wchar ucs)
{
	int			min = 0;
	int			mid;
	int			max = lengthof(simple_case_map) - 1;

	/* all chars <= 0x80 are stored in array for fast lookup */
	Assert(max >= 0x7f);
	if (ucs < 0x80)
	{
		const pg_simple_case_map *map = &simple_case_map[ucs];
		Assert(map->codepoint == ucs);
		return map;
	}

	/* otherwise, binary search */
	while (max >= min)
	{
		mid = (min + max) / 2;
		if (ucs > simple_case_map[mid].codepoint)
			min = mid + 1;
		else if (ucs < simple_case_map[mid].codepoint)
			max = mid - 1;
		else
			return &simple_case_map[mid];
	}

	return NULL;
}

/*
 * Returns simple lowercase mapping for the given character, or the original
 * character if none. Sets *special to the special case mapping, if any.
 */
pg_wchar
unicode_lowercase_simple(pg_wchar ucs)
{
	const		pg_simple_case_map *map = find_case_map(ucs);

	return map ? map->simple_lowercase : ucs;
}

/*
 * Returns simple titlecase mapping for the given character, or the original
 * character if none. Sets *special to the special case mapping, if any.
 */
pg_wchar
unicode_titlecase_simple(pg_wchar ucs)
{
	const		pg_simple_case_map *map = find_case_map(ucs);

	return map ? map->simple_titlecase : ucs;
}

/*
 * Returns simple uppercase mapping for the given character, or the original
 * character if none. Sets *special to the special case mapping, if any.
 */
pg_wchar
unicode_uppercase_simple(pg_wchar ucs)
{
	const		pg_simple_case_map *map = find_case_map(ucs);

	return map ? map->simple_uppercase : ucs;
}
