/*-------------------------------------------------------------------------
 *
 * ts_public.h
 *	  Public interface to various tsearch modules, such as
 *	  parsers and dictionaries.
 *
 * Copyright (c) 1998-2018, PostgreSQL Global Development Group
 *
 * src/include/tsearch/ts_public.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PG_TS_PUBLIC_H_
#define _PG_TS_PUBLIC_H_

#include "nodes/pg_list.h"
#include "storage/itemptr.h"
#include "tsearch/ts_type.h"

/*
 * Parser's framework
 */

/*
 * returning type for prslextype method of parser
 */
typedef struct
{
	int			lexid;
	char	   *alias;
	char	   *descr;
} LexDescr;

/*
 * Interface to headline generator
 */
typedef struct
{
	uint32		selected:1,
				in:1,
				replace:1,
				repeated:1,
				skip:1,
				unused:3,
				type:8,
				len:16;
	WordEntryPos pos;
	char	   *word;
	QueryOperand *item;
} HeadlineWordEntry;

typedef struct
{
	HeadlineWordEntry *words;
	int32		lenwords;
	int32		curwords;
	int32		vectorpos;		/* positions a-la tsvector */
	char	   *startsel;
	char	   *stopsel;
	char	   *fragdelim;
	int16		startsellen;
	int16		stopsellen;
	int16		fragdelimlen;
} HeadlineParsedText;

/*
 * Common useful things for tsearch subsystem
 */
extern char *get_tsearch_config_filename(const char *basename,
							const char *extension);

/*
 * Often useful stopword list management
 */
typedef struct
{
	int			len;
	char	  **stop;
} StopList;

extern void readstoplist(const char *fname, StopList *s,
			 char *(*wordop) (const char *));
extern bool searchstoplist(StopList *s, char *key);

/*
 * API for text search dictionaries.
 *
 * API functions to handle a text search dictionary are defined by a text search
 * template.  Currently an existing template cannot be altered in order to
 * define another functions.  API consists of the following functions:
 *	- init function - optional function which initializes internal structures of
 *	  the dictionary.  It accepts DictInitData structure as an argument and must
 *	  return a custom palloc'd structure which stores content of the processed
 *	  dictionary and is used in lexize function.
 *	- lexize function - normalizes a single word (token) using specific
 *	  dictionary.  It must return a palloc'd array of TSLexeme the last entry of
 *	  which is the terminating entry and accepts the following arguments:
 *	  - dictData - pointer to a custom structure returned by init function or
 *		NULL if init function wasn't defined by the template.
 *	  - token - string which represents a token to normalize, isn't
 *		null-terminated.
 *	  - length - length of token.
 *	  - dictState - pointer to a DictSubState structure which stores current
 *		state of a set of tokens processing and allows to normalize phrases.
 */

/*
 * A preprocessed dictionary can be stored in shared memory using DSM.  Does
 * the dictionary want it decides init function.  A DSM segment is released if
 * the dictionary was altered or droppped.  But still there is a situation when
 * we haven't a way to prevent a segment leaking.  It may happen if the
 * dictionary was dropped, some backend used the dictionary before dropping, the
 * backend will hold its DSM segment till disconnecting or calling
 * lookup_ts_dictionary_cache(), where invalid segment is unpinned.
 *
 * DictPointerData is a structure to search a dictionary's DSM segment.  We
 * need xmin, xmax and tid to be sure that the content in the DSM segment still
 * valid.
 */
typedef struct
{
	Oid			id;				/* OID of dictionary which is processed */
	TransactionId xmin;			/* XMIN of the dictionary's tuple */
	TransactionId xmax;			/* XMAX of the dictionary's tuple */
	ItemPointerData tid;		/* TID of the dictionary's tuple */
} DictPointerData;

/*
 * API structure for a dictionary initialization.  It is passed as an argument
 * to a template's init function.
 */
typedef struct
{
	/*
	 * A dictionary option list for a template's init method. Should go first
	 * for backward compatibility.
	 */
	List	   *dict_options;
	/*
	 * A dictionary information used to allocate, search and release its DSM
	 * segment.
	 */
	DictPointerData dict;
} DictInitData;

/*
 * Return struct for any lexize function.  They are combined into an array, the
 * last entry is the terminating entry.
 */
typedef struct
{
	/*----------
	 * Number of current variant of split word.  For example the Norwegian
	 * word 'fotballklubber' has two variants to split: ( fotball, klubb )
	 * and ( fot, ball, klubb ). So, dictionary should return:
	 *
	 * nvariant    lexeme
	 *	   1	   fotball
	 *	   1	   klubb
	 *	   2	   fot
	 *	   2	   ball
	 *	   2	   klubb
	 *
	 * In general, a TSLexeme will be considered to belong to the same split
	 * variant as the previous one if they have the same nvariant value.
	 * The exact values don't matter, only changes from one lexeme to next.
	 *----------
	 */
	uint16		nvariant;

	uint16		flags;			/* See flag bits below */

	char	   *lexeme;			/* C string or NULL if it is a terminating
								 * entry */
} TSLexeme;

/* Flag bits that can appear in TSLexeme.flags */
#define TSL_ADDPOS		0x01
#define TSL_PREFIX		0x02
#define TSL_FILTER		0x04

/*
 * Struct for supporting complex dictionaries like thesaurus.
 * 4th argument for dictlexize method is a pointer to this
 */
typedef struct
{
	bool		isend;			/* in: marks for lexize_info about text end is
								 * reached */
	bool		getnext;		/* out: dict wants next lexeme */
	void	   *private_state;	/* internal dict state between calls with
								 * getnext == true */
} DictSubState;

#endif							/* _PG_TS_PUBLIC_H_ */
