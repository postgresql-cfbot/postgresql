/*-------------------------------------------------------------------------
 *
 * pg_collation_fn_common.c
 *	  commmon routines to support manipulation of the pg_collation relation
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/pg_collation_fn_common.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "catalog/pg_collation.h"
#include "common/pg_collation_fn_common.h"


/*
 * Note that we search the table with pg_strcasecmp(), so variant
 * capitalizations don't need their own entries.
 */
typedef struct collprovider_name
{
	char		collprovider;
	const char *name;
} collprovider_name;

static const collprovider_name collprovider_name_tbl[] =
{
	{COLLPROVIDER_DEFAULT, "default"},
	{COLLPROVIDER_LIBC, "libc"},
	{COLLPROVIDER_ICU, "icu"},
	{'\0', NULL}				/* end marker */
};

/*
 * Get the collation provider from the given collation provider name.
 *
 * Return '\0' if we can't determine it.
 */
char
get_collprovider(const char *name)
{
	int			i;

	if (!name)
		return '\0';

	/* Check the table */
	for (i = 0; collprovider_name_tbl[i].name; ++i)
		if (pg_strcasecmp(name, collprovider_name_tbl[i].name) == 0)
			return collprovider_name_tbl[i].collprovider;

	return '\0';
}

/*
 * Get the name of the given collation provider.
 *
 * Return NULL if we can't determine it.
 */
const char *
get_collprovider_name(char collprovider)
{
	int			i;

	/* Check the table */
	for (i = 0; collprovider_name_tbl[i].collprovider; ++i)
		if (collprovider_name_tbl[i].collprovider == collprovider)
			return collprovider_name_tbl[i].name;

	return NULL;
}

/*
 * Return true if collation provider is nondefault and valid, and false otherwise.
 */
bool
is_valid_nondefault_collprovider(char collprovider)
{
	return (collprovider == COLLPROVIDER_LIBC ||
			collprovider == COLLPROVIDER_ICU);
}
