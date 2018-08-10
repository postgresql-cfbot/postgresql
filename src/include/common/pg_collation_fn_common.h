/*-------------------------------------------------------------------------
 *
 * pg_collation_fn_common.h
 *	 prototypes for functions in common/pg_collation_fn_common.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/pg_collation_fn_common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_COLLATION_FN_COMMON_H
#define PG_COLLATION_FN_COMMON_H

extern char get_collprovider(const char *name);
extern const char *get_collprovider_name(char collprovider);
extern bool is_valid_nondefault_collprovider(char collprovider);

#endif							/* PG_COLLATION_FN_COMMON_H */
