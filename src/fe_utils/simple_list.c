/*-------------------------------------------------------------------------
 *
 * Simple list facilities for frontend code
 *
 * Data structures for simple lists of OIDs and strings.  The support for
 * these is very primitive compared to the backend's List facilities, but
 * it's all we need in, eg, pg_dump.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/fe_utils/simple_list.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "fe_utils/simple_list.h"


/*
 * Append an OID to the list.
 */
void
simple_oid_list_append(SimpleOidList *list, Oid val)
{
	simple_oid_list_append_data(list, val, NULL);
}

/*
 * Is OID present in the list?
 */
bool
simple_oid_list_member(SimpleOidList *list, Oid val)
{
	return simple_oid_list_find_data(list, val, NULL);
}

/*
 * Append an OID to the list, along with extra pointer-sized data.
 */
void
simple_oid_list_append_data(SimpleOidList *list, Oid val, void *extra_data)
{
	SimpleOidListCell *cell;

	cell = (SimpleOidListCell *) pg_malloc(sizeof(SimpleOidListCell));
	cell->next = NULL;
	cell->val = val;
	cell->extra_data = extra_data;

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}

/*
 * Is OID present in the list?
 * If so, return true, and provide pointer-sized data by setting result of extra_data parameter.
 * If not, return false.
 */
bool
simple_oid_list_find_data(SimpleOidList *list, Oid val, void **extra_data)
{
	SimpleOidListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (cell->val == val) {
			if (extra_data)
				*extra_data = cell->extra_data;
			return true;
		}
	}
	return false;
}

/*
 * Append a string to the list.
 *
 * The given string is copied, so it need not survive past the call.
 */
void
simple_string_list_append(SimpleStringList *list, const char *val)
{
	SimpleStringListCell *cell;

	cell = (SimpleStringListCell *)
		pg_malloc(offsetof(SimpleStringListCell, val) + strlen(val) + 1);

	cell->next = NULL;
	cell->touched = false;
	strcpy(cell->val, val);

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}

/*
 * Is string present in the list?
 *
 * If found, the "touched" field of the first match is set true.
 */
bool
simple_string_list_member(SimpleStringList *list, const char *val)
{
	SimpleStringListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (strcmp(cell->val, val) == 0)
		{
			cell->touched = true;
			return true;
		}
	}
	return false;
}

/*
 * Find first not-touched list entry, if there is one.
 */
const char *
simple_string_list_not_touched(SimpleStringList *list)
{
	SimpleStringListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (!cell->touched)
			return cell->val;
	}
	return NULL;
}
