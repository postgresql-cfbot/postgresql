/*-------------------------------------------------------------------------
 *
 * dict_ispell.c
 *		Ispell dictionary interface
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * By default all Ispell dictionaries are stored in DSM. But if number of
 * loaded dictionaries reached maximum allowed value then it will be
 * allocated within its memory context (dictCtx).
 *
 * All necessary data are built within dispell_build() function. But
 * structures for regular expressions are compiled on first demand and
 * stored using AffixReg array. It is because regex_t and Regis cannot be
 * stored in shared memory.
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/dict_ispell.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "storage/dsm.h"
#include "tsearch/dicts/spell.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_shared.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"


typedef struct
{
	StopList	stoplist;
	IspellDict	obj;
} DictISpell;

static void parse_dictoptions(List *dictoptions,
							  char **dictfile, char **afffile, char **stopfile,
							  bool *isshared);
static void *dispell_build(List *dictoptions, Size *size);

Datum
dispell_init(PG_FUNCTION_ARGS)
{
	List	   *dictoptions = (List *) PG_GETARG_POINTER(0);
	Oid			dictid = PG_GETARG_OID(1);
	DictISpell *d;
	void	   *dict_location;
	char	   *stopfile;
	bool		isshared;

	d = (DictISpell *) palloc0(sizeof(DictISpell));

	parse_dictoptions(dictoptions, NULL, NULL, &stopfile, &isshared);

	/* Make stop word list */
	if (stopfile)
		readstoplist(stopfile, &(d->stoplist), lowerstr);

	/* Make or get from shared memory dictionary itself */
	if (isshared)
		dict_location = ts_dict_shmem_location(dictid, dictoptions, dispell_build);
	else
		dict_location = dispell_build(dictoptions, NULL);

	Assert(dict_location);

	d->obj.dict = (IspellDictData *) dict_location;
	d->obj.reg = (AffixReg *) palloc0(d->obj.dict->nAffix *
									  sizeof(AffixReg));
	/* Current memory context is dictionary's private memory context */
	d->obj.dictCtx = CurrentMemoryContext;

	PG_RETURN_POINTER(d);
}

Datum
dispell_lexize(PG_FUNCTION_ARGS)
{
	DictISpell *d = (DictISpell *) PG_GETARG_POINTER(0);
	char	   *in = (char *) PG_GETARG_POINTER(1);
	int32		len = PG_GETARG_INT32(2);
	char	   *txt;
	TSLexeme   *res;
	TSLexeme   *ptr,
			   *cptr;

	if (len <= 0)
		PG_RETURN_POINTER(NULL);

	txt = lowerstr_with_len(in, len);
	res = NINormalizeWord(&(d->obj), txt);

	if (res == NULL)
		PG_RETURN_POINTER(NULL);

	cptr = res;
	for (ptr = cptr; ptr->lexeme; ptr++)
	{
		if (searchstoplist(&(d->stoplist), ptr->lexeme))
		{
			pfree(ptr->lexeme);
			ptr->lexeme = NULL;
		}
		else
		{
			if (cptr != ptr)
				memcpy(cptr, ptr, sizeof(TSLexeme));
			cptr++;
		}
	}
	cptr->lexeme = NULL;

	PG_RETURN_POINTER(res);
}

static void
parse_dictoptions(List *dictoptions, char **dictfile, char **afffile,
				  char **stopfile, bool *isshared)
{
	ListCell   *l;
	bool		isshared_defined = false;

	if (dictfile)
		*dictfile = NULL;
	if (afffile)
		*afffile = NULL;
	if (stopfile)
		*stopfile = NULL;
	if (isshared)
		*isshared = true;

	foreach(l, dictoptions)
	{
		DefElem    *defel = (DefElem *) lfirst(l);

		if (pg_strcasecmp(defel->defname, "DictFile") == 0)
		{
			if (!dictfile)
				continue;

			if (*dictfile)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("multiple DictFile parameters")));
			*dictfile = get_tsearch_config_filename(defGetString(defel), "dict");
		}
		else if (pg_strcasecmp(defel->defname, "AffFile") == 0)
		{
			if (!afffile)
				continue;

			if (*afffile)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("multiple AffFile parameters")));
			*afffile = get_tsearch_config_filename(defGetString(defel), "affix");
		}
		else if (pg_strcasecmp(defel->defname, "StopWords") == 0)
		{
			if (!stopfile)
				continue;

			if (*stopfile)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("multiple StopWords parameters")));
			*stopfile = defGetString(defel);
		}
		else if (pg_strcasecmp(defel->defname, "Shareable") == 0)
		{
			if (!isshared)
				continue;

			if (isshared_defined)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("multiple Shareable parameters")));

			*isshared = defGetBoolean(defel);
			isshared_defined = true;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized Ispell parameter: \"%s\"",
							defel->defname)));
		}
	}
}

/*
 * Build the dictionary.
 *
 * Result is palloc'ed.
 */
static void *
dispell_build(List *dictoptions, Size *size)
{
	IspellDictBuild build;
	char	   *dictfile,
			   *afffile;

	parse_dictoptions(dictoptions, &dictfile, &afffile, NULL, NULL);

	if (!afffile)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing AffFile parameter")));
	}
	else if (!dictfile)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing DictFile parameter")));
	}

	MemSet(&build, 0, sizeof(build));
	NIStartBuild(&build);

	/* Read files */
	NIImportDictionary(&build, dictfile);
	NIImportAffixes(&build, afffile);

	/* Build persistent data to use by backends */
	NISortDictionary(&build);
	NISortAffixes(&build);

	NICopyData(&build);

	/* Release temporary data */
	NIFinishBuild(&build);

	/* Return the buffer and its size */
	if (size)
		*size = build.dict_size;
	return build.dict;
}
