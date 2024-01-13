/*-------------------------------------------------------------------------
 *
 * unicode_case.h
 *	  Routines for converting character case.
 *
 * These definitions can be used by both frontend and backend code.
 *
 * Copyright (c) 2017-2023, PostgreSQL Global Development Group
 *
 * src/include/common/unicode_case.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNICODE_CASE_H
#define UNICODE_CASE_H

#include "mb/pg_wchar.h"

pg_wchar	unicode_lowercase_simple(pg_wchar ucs);
pg_wchar	unicode_titlecase_simple(pg_wchar ucs);
pg_wchar	unicode_uppercase_simple(pg_wchar ucs);

#endif							/* UNICODE_CASE_H */
