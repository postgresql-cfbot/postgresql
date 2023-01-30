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
			UCollator	*ucol;
		}			icu;
#endif
		int			dummy;		/* in case we have neither LOCALE_T nor ICU */
	}			info;
};

#ifdef USE_ICU
extern int32_t icu_to_uchar(UChar **buff_uchar, const char *buff, size_t nbytes);
extern int32_t icu_from_uchar(char **result, const UChar *buff_uchar, int32_t len_uchar);
#endif

#endif							/* _PG_LOCALE_INTERNAL_ */
