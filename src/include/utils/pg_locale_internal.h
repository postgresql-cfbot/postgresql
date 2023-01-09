/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities
 *
 * src/include/utils/pg_locale_internal.h
 *
 * Copyright (c) 2002-2022, PostgreSQL Global Development Group
 *
 *-----------------------------------------------------------------------
 */


#ifndef _PG_LOCALE_INTERNAL_
#define _PG_LOCALE_INTERNAL_

#ifdef USE_ICU
#include <unicode/ubrk.h>
#include <unicode/ucnv.h>
#include <unicode/ucol.h>
#endif

#ifdef USE_ICU
/*
 * ucol_strcollUTF8() was introduced in ICU 50, but it is buggy before ICU 53.
 * (see
 * <https://www.postgresql.org/message-id/flat/f1438ec6-22aa-4029-9a3b-26f79d330e72%40manitou-mail.org>)
 */
#if U_ICU_VERSION_MAJOR_NUM >= 53
#define HAVE_UCOL_STRCOLLUTF8 1
#else
#undef HAVE_UCOL_STRCOLLUTF8
#endif
#endif

#ifdef USE_ICU
/*
 * An ICU library version that we're either linked against or have loaded at
 * runtime.
 */
typedef struct pg_icu_library
{
	int			major_version;
	int			minor_version;
	void		(*getICUVersion) (UVersionInfo info);
	void		(*getUnicodeVersion) (UVersionInfo into);
	void		(*getCLDRVersion) (UVersionInfo info, UErrorCode *status);
	UCollator  *(*openCollator) (const char *loc, UErrorCode *status);
	void		(*closeCollator) (UCollator *coll);
	void		(*getCollatorVersion) (const UCollator *coll, UVersionInfo info);
	void		(*getUCAVersion) (const UCollator *coll, UVersionInfo info);
	void		(*versionToString) (const UVersionInfo versionArray,
									char *versionString);
	UCollationResult (*strcoll) (const UCollator *coll,
								 const UChar *source,
								 int32_t sourceLength,
								 const UChar *target,
								 int32_t targetLength);
	UCollationResult (*strcollUTF8) (const UCollator *coll,
									 const char *source,
									 int32_t sourceLength,
									 const char *target,
									 int32_t targetLength,
									 UErrorCode *status);
	int32_t		(*getSortKey) (const UCollator *coll,
							   const UChar *source,
							   int32_t sourceLength,
							   uint8_t *result,
							   int32_t resultLength);
	int32_t		(*nextSortKeyPart) (const UCollator *coll,
									UCharIterator *iter,
									uint32_t state[2],
									uint8_t *dest,
									int32_t count,
									UErrorCode *status);
	void		(*setUTF8) (UCharIterator *iter,
							const char *s,
							int32_t length);
	const char *(*errorName) (UErrorCode code);
	int32_t		(*strToUpper) (UChar *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   const char *locale,
							   UErrorCode *pErrorCode);
	int32_t		(*strToLower) (UChar *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   const char *locale,
							   UErrorCode *pErrorCode);
	int32_t		(*strToTitle) (UChar *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   UBreakIterator *titleIter,
							   const char *locale,
							   UErrorCode *pErrorCode);
	void		(*setAttribute) (UCollator *coll,
								 UColAttribute attr,
								 UColAttributeValue value,
								 UErrorCode *status);
	UConverter *(*openConverter) (const char *converterName,
								  UErrorCode *  	err);
	void		(*closeConverter) (UConverter *converter);
	int32_t		(*fromUChars) (UConverter *cnv,
							   char *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   UErrorCode *pErrorCode);
	int32_t		(*toUChars) (UConverter *cnv,
							 UChar *dest,
							 int32_t destCapacity,
							 const char *src,
							 int32_t srcLength,
							 UErrorCode *pErrorCode);
	int32_t		(*toLanguageTag) (const char *localeID,
								  char *langtag,
								  int32_t langtagCapacity,
								  UBool strict,
								  UErrorCode *err);
	int32_t		(*getDisplayName) (const char *localeID,
								   const char *inLocaleID,
								   UChar *result,
								   int32_t maxResultSize,
								   UErrorCode *err);
	int32_t		(*countAvailable) (void);
	const char *(*getAvailable) (int32_t n);
} pg_icu_library;

#endif

/*
 * We define our own wrapper around locale_t so we can keep the same
 * function signatures for all builds, while not having to create a
 * fake version of the standard type locale_t in the global namespace.
 * pg_locale_t is occasionally checked for truth, so make it a pointer.
 */
struct pg_locale_struct
{
	char		provider;
	bool		deterministic;
	char	   *collate;
	char	   *ctype;
	union
	{
#ifdef HAVE_LOCALE_T
		struct
		{
			locale_t	lt;
		}			libc;
#endif
#ifdef USE_ICU
		struct
		{
			UCollator		*ucol;
			pg_icu_library	*lib;
		}			icu;
#endif
		int			dummy;		/* in case we have neither LOCALE_T nor ICU */
	}			info;
};

#ifdef USE_ICU

typedef bool (*get_icu_library_hook_type)(
	pg_icu_library *lib, const char *collate, const char *ctype,
	const char *version);

extern PGDLLIMPORT get_icu_library_hook_type get_icu_library_hook;

#define PG_ICU_LIB(x) ((x)->info.icu.lib)
#define PG_ICU_COL(x) ((x)->info.icu.ucol)

extern pg_icu_library *get_icu_library(const char *collate, const char *ctype,
									   const char *version);
extern int32_t icu_to_uchar(pg_icu_library *lib, UChar **buff_uchar,
							const char *buff, size_t nbytes);
extern int32_t icu_from_uchar(pg_icu_library *lib, char **result,
							  const UChar *buff_uchar, int32_t len_uchar);
#endif

#endif							/* _PG_LOCALE_INTERNAL_ */
