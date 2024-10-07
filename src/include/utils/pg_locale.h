/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities
 *
 * src/include/utils/pg_locale.h
 *
 * Copyright (c) 2002-2024, PostgreSQL Global Development Group
 *
 *-----------------------------------------------------------------------
 */

#ifndef _PG_LOCALE_
#define _PG_LOCALE_

#include "mb/pg_wchar.h"

#ifdef USE_ICU
#include <unicode/ucol.h>
#endif

/*
 * Character properties for regular expressions.
 */
#define PG_ISDIGIT     0x01
#define PG_ISALPHA     0x02
#define PG_ISALNUM     (PG_ISDIGIT | PG_ISALPHA)
#define PG_ISUPPER     0x04
#define PG_ISLOWER     0x08
#define PG_ISGRAPH     0x10
#define PG_ISPRINT     0x20
#define PG_ISPUNCT     0x40
#define PG_ISSPACE     0x80

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

/* use for libc locale names */
#define LOCALE_NAME_BUFLEN 128

/* GUC settings */
extern PGDLLIMPORT char *locale_messages;
extern PGDLLIMPORT char *locale_monetary;
extern PGDLLIMPORT char *locale_numeric;
extern PGDLLIMPORT char *locale_time;
extern PGDLLIMPORT int icu_validation_level;

/* lc_time localization cache */
extern PGDLLIMPORT char *localized_abbrev_days[];
extern PGDLLIMPORT char *localized_full_days[];
extern PGDLLIMPORT char *localized_abbrev_months[];
extern PGDLLIMPORT char *localized_full_months[];

/* is the databases's LC_CTYPE the C locale? */
extern PGDLLIMPORT bool database_ctype_is_c;

extern bool check_locale(int category, const char *locale, char **canonname);
extern char *pg_perm_setlocale(int category, const char *locale);

/*
 * Return the POSIX lconv struct (contains number/money formatting
 * information) with locale information for all categories.
 */
extern struct lconv *PGLC_localeconv(void);

extern void cache_locale_time(void);


struct pg_locale_struct;
typedef struct pg_locale_struct *pg_locale_t;

/* methods that define collation behavior */
struct collate_methods
{
	/* required */
	int			(*strncoll) (const char *arg1, ssize_t len1,
							 const char *arg2, ssize_t len2,
							 pg_locale_t locale);

	/* required */
	size_t		(*strnxfrm) (char *dest, size_t destsize,
							 const char *src, ssize_t srclen,
							 pg_locale_t locale);

	/* optional */
	size_t		(*strnxfrm_prefix) (char *dest, size_t destsize,
									const char *src, ssize_t srclen,
									pg_locale_t locale);

	/*
	 * If the strnxfrm method is not trusted to return the correct results,
	 * set strxfrm_is_safe to false. It set to false, the method will not be
	 * used in most cases, but the planner still expects it to be there for
	 * estimation purposes (where incorrect results are acceptable).
	 */
	bool		strxfrm_is_safe;
};

/* methods that define string case mapping behavior */
struct casemap_methods
{
	size_t		(*strlower) (char *dest, size_t destsize,
							 const char *src, ssize_t srclen,
							 pg_locale_t locale);
	size_t		(*strtitle) (char *dest, size_t destsize,
							 const char *src, ssize_t srclen,
							 pg_locale_t locale);
	size_t		(*strupper) (char *dest, size_t destsize,
							 const char *src, ssize_t srclen,
							 pg_locale_t locale);
};

struct ctype_methods
{
	/* required */
	int			(*char_properties) (pg_wchar wc, int mask, pg_locale_t locale);

	/* required */
	bool		(*char_is_cased) (char ch, pg_locale_t locale);

	/*
	 * Optional. If defined, will only be called for single-byte encodings. If
	 * not defined, or if the encoding is multibyte, will fall back to
	 * pg_strlower().
	 */
	char		(*char_tolower) (unsigned char ch, pg_locale_t locale);

	/* required */
	pg_wchar	(*wc_toupper) (pg_wchar wc, pg_locale_t locale);
	pg_wchar	(*wc_tolower) (pg_wchar wc, pg_locale_t locale);

	/*
	 * For regex and pattern matching efficiency, the maximum char value
	 * supported by the above methods. If zero, limit is set by regex code.
	 */
	pg_wchar	max_chr;
};

/*
 * We use a discriminated union to hold either a locale_t or an ICU collator.
 * pg_locale_t is occasionally checked for truth, so make it a pointer.
 *
 * Also, hold two flags: whether the collation's LC_COLLATE or LC_CTYPE is C
 * (or POSIX), so we can optimize a few code paths in various places.  For the
 * built-in C and POSIX collations, we can know that without even doing a
 * cache lookup, but we want to support aliases for C/POSIX too.  For the
 * "default" collation, there are separate static cache variables, since
 * consulting the pg_collation catalog doesn't tell us what we need.
 *
 * Note that some code relies on the flags not reporting false negatives
 * (that is, saying it's not C when it is).  For example, char2wchar()
 * could fail if the locale is C, so str_tolower() shouldn't call it
 * in that case.
 */
struct pg_locale_struct
{
	bool		deterministic;
	bool		collate_is_c;
	bool		ctype_is_c;

	const struct collate_methods *collate;	/* NULL if collate_is_c */
	const struct casemap_methods *casemap;	/* NULL if ctype_is_c */
	const struct ctype_methods *ctype;	/* NULL if ctype_is_c */

	void	   *provider_data;
};

typedef struct pg_locale_struct *pg_locale_t;

extern void init_database_collation(void);
extern pg_locale_t pg_newlocale_from_collation(Oid collid);

extern char *get_collation_actual_version(char collprovider, const char *collcollate);
extern int	char_properties(pg_wchar wc, int mask, pg_locale_t locale);
extern bool char_is_cased(char ch, pg_locale_t locale);
extern bool char_tolower_enabled(pg_locale_t locale);
extern char char_tolower(unsigned char ch, pg_locale_t locale);
extern size_t pg_strlower(char *dest, size_t destsize,
						  const char *src, ssize_t srclen,
						  pg_locale_t locale);
extern size_t pg_strtitle(char *dest, size_t destsize,
						  const char *src, ssize_t srclen,
						  pg_locale_t locale);
extern size_t pg_strupper(char *dest, size_t destsize,
						  const char *src, ssize_t srclen,
						  pg_locale_t locale);
extern int	pg_strcoll(const char *arg1, const char *arg2, pg_locale_t locale);
extern int	pg_strncoll(const char *arg1, ssize_t len1,
						const char *arg2, ssize_t len2, pg_locale_t locale);
extern bool pg_strxfrm_enabled(pg_locale_t locale);
extern size_t pg_strxfrm(char *dest, const char *src, size_t destsize,
						 pg_locale_t locale);
extern size_t pg_strnxfrm(char *dest, size_t destsize, const char *src,
						  ssize_t srclen, pg_locale_t locale);
extern bool pg_strxfrm_prefix_enabled(pg_locale_t locale);
extern size_t pg_strxfrm_prefix(char *dest, const char *src, size_t destsize,
								pg_locale_t locale);
extern size_t pg_strnxfrm_prefix(char *dest, size_t destsize, const char *src,
								 ssize_t srclen, pg_locale_t locale);

extern int	builtin_locale_encoding(const char *locale);
extern const char *builtin_validate_locale(int encoding, const char *locale);
extern void icu_validate_locale(const char *loc_str);
extern char *icu_language_tag(const char *loc_str, int elevel);

/* These functions convert from/to libc's wchar_t, *not* pg_wchar_t */
extern size_t wchar2char(char *to, const wchar_t *from, size_t tolen,
						 pg_locale_t locale);
extern size_t char2wchar(wchar_t *to, size_t tolen,
						 const char *from, size_t fromlen, pg_locale_t locale);

#endif							/* _PG_LOCALE_ */
