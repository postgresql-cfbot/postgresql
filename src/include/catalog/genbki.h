/*-------------------------------------------------------------------------
 *
 * genbki.h
 *	  Required include file for all POSTGRES catalog header files
 *
 * genbki.h defines CATALOG(), BKI_BOOTSTRAP and related macros
 * so that the catalog header files can be read by the C compiler.
 * (These same words are recognized by genbki.pl to build the BKI
 * bootstrap file from these header files.)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/genbki.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENBKI_H
#define GENBKI_H

/* Introduces a catalog's structure definition */
#define CATALOG(name,oid)	typedef struct CppConcat(FormData_,name)

/* Options that may appear after CATALOG (on the same line) */
#define BKI_BOOTSTRAP
#define BKI_SHARED_RELATION
#define BKI_WITHOUT_OIDS
#define BKI_ROWTYPE_OID(oid)
#define BKI_SCHEMA_MACRO
#define BKI_FORCE_NULL
#define BKI_FORCE_NOT_NULL

/* Specifies a default value for a catalog field */
#define BKI_DEFAULT(value)

/* Specifies an abbreviated label for a column name */
#define BKI_ABBREV(abbrev)

/* ----------------
 *	Some columns of type Oid have human-readable entries that are
 *	resolved when creating postgres.bki.
 * ----------------
 */
#define regam Oid
#define regoper Oid
#define regopf Oid
#define regtype Oid

/*
 * This is never defined; it's here only for documentation.
 *
 * Variable-length catalog fields (except possibly the first not nullable one)
 * should not be visible in C structures, so they are made invisible by #ifdefs
 * of an undefined symbol.  See also MARKNOTNULL in bootstrap.c for how this is
 * handled.
 */
#undef CATALOG_VARLEN

/*
 * Statements the bootstrap parser will turn into BootstrapToastTable
 * commands. Each line specifies the system catalog that needs a toast
 * table, the OID to assign to the toast table, and the OID to assign to
 * the toast table's index.  The reason we hard-wire these OIDs is that we
 * need stable OIDs for shared relations, and that includes toast tables of
 * shared relations.
 */
#define DECLARE_TOAST(name,toastoid,indexoid)

/* Statements the bootstrap parser will turn into DefineIndex calls.
 * The keyword is DECLARE_INDEX or DECLARE_UNIQUE_INDEX.  The first two
 * arguments are the index name and OID, the rest is much like a standard
 * 'create index' SQL command.
 *
 * For each index, we also provide a #define for its OID.  References to
 * the index in the C code should always use these #defines, not the actual
 * index name (much less the numeric OID).
 */
#define DECLARE_INDEX(name,oid,decl)
#define DECLARE_UNIQUE_INDEX(name,oid,decl)


#endif							/* GENBKI_H */
