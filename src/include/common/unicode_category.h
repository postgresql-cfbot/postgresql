/*-------------------------------------------------------------------------
 *
 * unicode_category.h
 *	  Routines for determining the category of Unicode characters.
 *
 * These definitions can be used by both frontend and backend code.
 *
 * Copyright (c) 2017-2023, PostgreSQL Global Development Group
 *
 * src/include/common/unicode_category.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNICODE_CATEGORY_H
#define UNICODE_CATEGORY_H

#include "mb/pg_wchar.h"

/* matches corresponding numeric values of UCharCategory, defined by ICU */
typedef enum pg_unicode_category {
	PG_U_UNASSIGNED = 0,
	PG_U_UPPERCASE_LETTER = 1,
	PG_U_LOWERCASE_LETTER = 2,
	PG_U_TITLECASE_LETTER = 3,
	PG_U_MODIFIER_LETTER = 4,
	PG_U_OTHER_LETTER = 5,
	PG_U_NON_SPACING_MARK = 6,
	PG_U_ENCLOSING_MARK = 7,
	PG_U_COMBINING_SPACING_MARK = 8,
	PG_U_DECIMAL_DIGIT_NUMBER = 9,
	PG_U_LETTER_NUMBER = 10,
	PG_U_OTHER_NUMBER = 11,
	PG_U_SPACE_SEPARATOR = 12,
	PG_U_LINE_SEPARATOR = 13,
	PG_U_PARAGRAPH_SEPARATOR = 14,
	PG_U_CONTROL_CHAR = 15,
	PG_U_FORMAT_CHAR = 16,
	PG_U_PRIVATE_USE_CHAR = 17,
	PG_U_SURROGATE = 18,
	PG_U_DASH_PUNCTUATION = 19,
	PG_U_START_PUNCTUATION = 20,
	PG_U_END_PUNCTUATION = 21,
	PG_U_CONNECTOR_PUNCTUATION = 22,
	PG_U_OTHER_PUNCTUATION = 23,
	PG_U_MATH_SYMBOL = 24,
	PG_U_CURRENCY_SYMBOL = 25,
	PG_U_MODIFIER_SYMBOL = 26,
	PG_U_OTHER_SYMBOL = 27,
	PG_U_INITIAL_PUNCTUATION = 28,
	PG_U_FINAL_PUNCTUATION = 29
} pg_unicode_category;

extern pg_unicode_category unicode_category(pg_wchar ucs);
const char *unicode_category_string(pg_unicode_category category);
const char *unicode_category_short(pg_unicode_category category);

#endif							/* UNICODE_CATEGORY_H */
