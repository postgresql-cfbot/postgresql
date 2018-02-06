/*-------------------------------------------------------------------------
 *
 * xpath_parser.h
 *	  Declarations for XML XPath transformation.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/xml.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef XPATH_PARSER_H
#define XPATH_PARSER_H

#include "postgres.h"
#include "lib/stringinfo.h"

void transformXPath(StringInfo str, const char *xpath, char *def_namespace_name);

#endif   /* XPATH_PARSER_H */
