/*-------------------------------------------------------------------------
 *
 * scansup.h
 *	  scanner support routines used by the core lexer
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/scansup.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SCANSUP_H
#define SCANSUP_H

#include "mb/pg_wchar.h"

extern char *downcase_truncate_identifier(const char *ident, int len,
										  bool warn);

extern char *downcase_identifier(const char *ident, int len,
								 bool warn, bool truncate);

extern void truncate_identifier(char *ident, int len, bool warn);

extern bool scanner_isspace(char ch);

extern unsigned int hexval(unsigned char c);

extern void check_unicode_value(pg_wchar c);

#endif							/* SCANSUP_H */
