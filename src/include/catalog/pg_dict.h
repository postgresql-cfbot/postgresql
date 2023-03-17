/*-------------------------------------------------------------------------
 *
 * pg_dict.h
 *	  definition of the "dict" system catalog (pg_dict)
 *
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_dict.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DICT_H
#define PG_DICT_H

#include "catalog/genbki.h"
#include "catalog/pg_dict_d.h"

#include "nodes/pg_list.h"

/* ----------------
 *		pg_dict definition.  cpp turns this into
 *		typedef struct FormData_pg_dict
 * ----------------
 */
CATALOG(pg_dict,9861,DictRelationId)
{
	Oid			oid;			/* oid */
	Oid			dicttypid BKI_LOOKUP(pg_type);	/* OID of owning dict type */
    /* AALEKSEEV TODO refactor to bytea */
	NameData	dictentry;		/* text representation of the dictionary entry */
} FormData_pg_dict;

/* ----------------
 *		Form_pg_dict corresponds to a pointer to a tuple with
 *		the format of pg_dict relation.
 * ----------------
 */
typedef FormData_pg_dict *Form_pg_dict;

DECLARE_UNIQUE_INDEX_PKEY(pg_dict_oid_index, 9862, DictOidIndexId, on pg_dict using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_dict_typid_entry_index, 9863, DictTypIdEntryIndexId, on pg_dict using btree(dicttypid oid_ops, dictentry name_ops));


/*
 * DictEntry type represents one entry in the given dictionary.
 */
typedef struct
{
	Oid         oid;    /* entry id */
	uint32      length; /* entry length */
	uint8*      data;   /* entry data */
} DictEntry;

/*
 * Dictionary type represents all the entries in the given dictionary.
 */
typedef struct
{
	uint32      nentries; /* number of entries */
	DictEntry  *entries; /* array of entries */
} DictionaryData;

typedef DictionaryData *Dictionary;

/*
 * prototypes for functions in pg_dict.c
 */
extern void DictEntriesCreate(Oid dictTypeOid, List *vals);
extern void DictEntriesFree(Dictionary dict);
extern Dictionary DictEntriesRead(Oid dictTypeOid);
extern void DictEntriesDelete(Oid dictTypeOid);

#endif							/* PG_DICT_H */
