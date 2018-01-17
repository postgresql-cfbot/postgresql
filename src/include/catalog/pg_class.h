/*-------------------------------------------------------------------------
 *
 * pg_class.h
 *	  definition of the system "relation" relation (pg_class)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_class.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLASS_H
#define PG_CLASS_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_class definition.  cpp turns this into
 *		typedef struct FormData_pg_class
 * ----------------
 */
#define RelationRelationId	1259
#define RelationRelation_Rowtype_Id  83

CATALOG(pg_class,1259) BKI_BOOTSTRAP BKI_ROWTYPE_OID(83) BKI_SCHEMA_MACRO
{
	/* class name */
	NameData	relname;

	/* OID of namespace containing this class */
	Oid			relnamespace BKI_DEFAULT(PGNSP);

	/* OID of entry in pg_type for table's implicit row type */
	Oid			reltype;

	/* OID of entry in pg_type for underlying composite type */
	Oid			reloftype BKI_DEFAULT(0);

	/* class owner */
	Oid			relowner BKI_DEFAULT(PGUID);

	/* index access method; 0 if not an index */
	Oid			relam BKI_DEFAULT(0);

	/* identifier of physical storage file */
	Oid			relfilenode BKI_DEFAULT(0);

	/* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */

	/* identifier of table space for relation */
	Oid			reltablespace BKI_DEFAULT(0);

	/* # of blocks (not always up-to-date) */
	int32		relpages BKI_DEFAULT(0);

	/* # of tuples (not always up-to-date) */
	float4		reltuples BKI_DEFAULT(0);

	/* # of all-visible blocks (not always up-to-date) */
	int32		relallvisible BKI_DEFAULT(0);

	/* OID of toast table; 0 if none */
	Oid			reltoastrelid BKI_DEFAULT(0);

	/* T if has (or has had) any indexes */
	bool		relhasindex BKI_DEFAULT(f);

	/* T if shared across databases */
	bool		relisshared BKI_DEFAULT(f);

	/* see RELPERSISTENCE_xxx constants below */
	char		relpersistence BKI_DEFAULT(p);

	/* see RELKIND_xxx constants below */
	char		relkind BKI_DEFAULT(r);

	/* number of user attributes */
	int16		relnatts;

	/*
	 * Class pg_attribute must contain exactly "relnatts" user attributes
	 * (with attnums ranging from 1 to relnatts) for this class.  It may also
	 * contain entries with negative attnums for system attributes.
	 */

	/* # of CHECK constraints for class */
	int16		relchecks BKI_DEFAULT(0);

	/* T if we generate OIDs for rows of rel */
	bool		relhasoids;

	/* has (or has had) PRIMARY KEY index */
	bool		relhaspkey BKI_DEFAULT(f);

	/* has (or has had) any rules */
	bool		relhasrules BKI_DEFAULT(f);

	/* has (or has had) any TRIGGERs */
	bool		relhastriggers BKI_DEFAULT(f);

	/* has (or has had) derived classes */
	bool		relhassubclass BKI_DEFAULT(f);

	/* row security is enabled or not */
	bool		relrowsecurity BKI_DEFAULT(f);

	/* row security forced for owners or not */
	bool		relforcerowsecurity BKI_DEFAULT(f);

	/* matview currently holds query results */
	bool		relispopulated BKI_DEFAULT(t);

	/* see REPLICA_IDENTITY_xxx constants  */
	char		relreplident BKI_DEFAULT(n);

	/* is relation a partition? */
	bool		relispartition BKI_DEFAULT(f);

	/* all Xids < this are frozen in this rel */
	/* Note: "3" stands for FirstNormalTransactionId */
	TransactionId relfrozenxid BKI_DEFAULT(3);

	/* all multixacts in this rel are >= this. This is really a MultiXactId */
	/* Note: "1" stands for FirstMultiXactId */
	TransactionId relminmxid BKI_DEFAULT(1);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* NOTE: These fields are not present in a relcache entry's rd_rel field. */

	/* access permissions */
	aclitem		relacl[1] BKI_DEFAULT(_null_);

	/* access-method-specific options */
	text		reloptions[1] BKI_DEFAULT(_null_);

	/* partition bound node tree */
	pg_node_tree relpartbound BKI_DEFAULT(_null_);
#endif
} FormData_pg_class;

/* Size of fixed part of pg_class tuples, not counting var-length fields */
#define CLASS_TUPLE_SIZE \
	 (offsetof(FormData_pg_class,relminmxid) + sizeof(TransactionId))

DECLARE_UNIQUE_INDEX(pg_class_oid_index, 2662, on pg_class using btree(oid oid_ops));
#define ClassOidIndexId  2662
DECLARE_UNIQUE_INDEX(pg_class_relname_nsp_index, 2663, on pg_class using btree(relname name_ops, relnamespace oid_ops));
#define ClassNameNspIndexId  2663
DECLARE_INDEX(pg_class_tblspc_relfilenode_index, 3455, on pg_class using btree(reltablespace oid_ops, relfilenode oid_ops));
#define ClassTblspcRelfilenodeIndexId  3455

/* ----------------
 *		Form_pg_class corresponds to a pointer to a tuple with
 *		the format of pg_class relation.
 * ----------------
 */
typedef FormData_pg_class *Form_pg_class;

/* ----------------
 *		compiler constants for pg_class
 * ----------------
 */

#define Natts_pg_class						33
#define Anum_pg_class_relname				1
#define Anum_pg_class_relnamespace			2
#define Anum_pg_class_reltype				3
#define Anum_pg_class_reloftype				4
#define Anum_pg_class_relowner				5
#define Anum_pg_class_relam					6
#define Anum_pg_class_relfilenode			7
#define Anum_pg_class_reltablespace			8
#define Anum_pg_class_relpages				9
#define Anum_pg_class_reltuples				10
#define Anum_pg_class_relallvisible			11
#define Anum_pg_class_reltoastrelid			12
#define Anum_pg_class_relhasindex			13
#define Anum_pg_class_relisshared			14
#define Anum_pg_class_relpersistence		15
#define Anum_pg_class_relkind				16
#define Anum_pg_class_relnatts				17
#define Anum_pg_class_relchecks				18
#define Anum_pg_class_relhasoids			19
#define Anum_pg_class_relhaspkey			20
#define Anum_pg_class_relhasrules			21
#define Anum_pg_class_relhastriggers		22
#define Anum_pg_class_relhassubclass		23
#define Anum_pg_class_relrowsecurity		24
#define Anum_pg_class_relforcerowsecurity	25
#define Anum_pg_class_relispopulated		26
#define Anum_pg_class_relreplident			27
#define Anum_pg_class_relispartition		28
#define Anum_pg_class_relfrozenxid			29
#define Anum_pg_class_relminmxid			30
#define Anum_pg_class_relacl				31
#define Anum_pg_class_reloptions			32
#define Anum_pg_class_relpartbound			33


#define		  RELKIND_RELATION		  'r'	/* ordinary table */
#define		  RELKIND_INDEX			  'i'	/* secondary index */
#define		  RELKIND_SEQUENCE		  'S'	/* sequence object */
#define		  RELKIND_TOASTVALUE	  't'	/* for out-of-line values */
#define		  RELKIND_VIEW			  'v'	/* view */
#define		  RELKIND_MATVIEW		  'm'	/* materialized view */
#define		  RELKIND_COMPOSITE_TYPE  'c'	/* composite type */
#define		  RELKIND_FOREIGN_TABLE   'f'	/* foreign table */
#define		  RELKIND_PARTITIONED_TABLE 'p' /* partitioned table */

#define		  RELPERSISTENCE_PERMANENT	'p' /* regular table */
#define		  RELPERSISTENCE_UNLOGGED	'u' /* unlogged permanent table */
#define		  RELPERSISTENCE_TEMP		't' /* temporary table */

/* default selection for replica identity (primary key or nothing) */
#define		  REPLICA_IDENTITY_DEFAULT	'd'
/* no replica identity is logged for this relation */
#define		  REPLICA_IDENTITY_NOTHING	'n'
/* all columns are logged as replica identity */
#define		  REPLICA_IDENTITY_FULL		'f'
/*
 * an explicitly chosen candidate key's columns are used as replica identity.
 * Note this will still be set if the index has been dropped; in that case it
 * has the same meaning as 'd'.
 */
#define		  REPLICA_IDENTITY_INDEX	'i'

#endif							/* PG_CLASS_H */
