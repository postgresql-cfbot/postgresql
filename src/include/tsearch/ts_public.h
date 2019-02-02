/*-------------------------------------------------------------------------
 *
 * ts_public.h
 *	  Public interface to various tsearch modules, such as
 *	  parsers and dictionaries.
 *
 * Copyright (c) 1998-2019, PostgreSQL Global Development Group
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
 * API functions to manage a text search dictionary are defined by a text search
 * template.  Currently an existing template cannot be altered in order to use
 * different functions.  API consists of the following functions:
 *
 * init function
 * -------------
 * - optional function which initializes internal structures of the dictionary
 * - accepts DictInitData structure as an argument and must return a custom
 *   palloc'd structure which stores content of the processed dictionary and
 *   is used by lexize function
 *
 * lexize function
 * ---------------
 * - normalizes a single word (token) using specific dictionary
 * - returns a palloc'd array of TSLexeme, with a terminating NULL entry
 * - accepts the following arguments:
 *
 *	  - dictData - pointer to a structure returned by init function or NULL if
 *	 	init function wasn't defined by the template
 *	  - token - string to normalize (not null-terminated)
 *	  - length - length of the token
 *	  - dictState - pointer to a DictSubState structure storing current
 *		state of a set of tokens processing and allows to normalize phrases
 */

/*
 * A preprocessed dictionary can be stored in shared memory using DSM - this is
 * decided in the init function.  A DSM segment is released after altering or
 * dropping the dictionary.  The segment may still leak, when a backend uses the
 * dictionary right before dropping - in that case the backend will hold the DSM
 * untill it disconnects or calls lookup_ts_dictionary_cache().
 *
 * DictEntryData represents DSM segment with a preprocessed dictionary.  We need
 * to ensure the content of the DSM segment is still valid, which is what xmin,
 * xmax and tid are for.
 */
typedef struct
{
	Oid				id;			/* OID of the dictionary */
	TransactionId	xmin;		/* XMIN of the dictionary's tuple */
	TransactionId	xmax;		/* XMAX of the dictionary's tuple */
	ItemPointerData	tid;		/* TID of the dictionary's tuple */
} DictEntryData;

/*
 * API structure for a dictionary initialization.  It is passed as an argument
 * to a template's init function.
 */
typedef struct
{
	/* List of options for a template's init method */
	List	   *dict_options;

	/* Data used to allocate, search and release the DSM segment */
	DictEntryData dict;
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

	char	   *lexeme;			/* C string (NULL for terminating entry) */
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
