/*-------------------------------------------------------------------------
 *
 * pg_amop.h
 *	  definition of the system "amop" relation (pg_amop)
 *	  along with the relation's initial contents.
 *
 * The amop table identifies the operators associated with each index operator
 * family and operator class (classes are subsets of families).  An associated
 * operator can be either a search operator or an ordering operator, as
 * identified by amoppurpose.
 *
 * The primary key for this table is <amopfamily, amoplefttype, amoprighttype,
 * amopstrategy>.  amoplefttype and amoprighttype are just copies of the
 * operator's oprleft/oprright, ie its declared input data types.  The
 * "default" operators for a particular opclass within the family are those
 * with amoplefttype = amoprighttype = opclass's opcintype.  An opfamily may
 * also contain other operators, typically cross-data-type operators.  All the
 * operators within a family are supposed to be compatible, in a way that is
 * defined by each individual index AM.
 *
 * We also keep a unique index on <amopopr, amoppurpose, amopfamily>, so that
 * we can use a syscache to quickly answer questions of the form "is this
 * operator in this opfamily, and if so what are its semantics with respect to
 * the family?"  This implies that the same operator cannot be listed for
 * multiple strategy numbers within a single opfamily, with the exception that
 * it's possible to list it for both search and ordering purposes (with
 * different strategy numbers for the two purposes).
 *
 * amopmethod is a copy of the owning opfamily's opfmethod field.  This is an
 * intentional denormalization of the catalogs to buy lookup speed.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_amop.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AMOP_H
#define PG_AMOP_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_amop definition.  cpp turns this into
 *		typedef struct FormData_pg_amop
 * ----------------
 */
#define AccessMethodOperatorRelationId	2602

CATALOG(pg_amop,2602)
{
	/* the index opfamily this entry is for */
	regopf		amopfamily BKI_ABBREV(opf);

	/* operator's left input data type */
	regtype		amoplefttype BKI_ABBREV(lt);

	/* operator's right input data type */
	regtype		amoprighttype BKI_ABBREV(rt);

	/* operator strategy number */
	int16		amopstrategy BKI_ABBREV(str);

	/* is operator for 's'earch or 'o'rdering? */
	char		amoppurpose BKI_ABBREV(pur) BKI_DEFAULT(s);

	/* the operator's pg_operator OID */
	regoper		amopopr BKI_ABBREV(oper);

	/* the index access method this entry is for */
	regam		amopmethod BKI_ABBREV(am);

	/* ordering opfamily OID, or 0 if search op */
	regopf		amopsortfamily BKI_DEFAULT(0);
} FormData_pg_amop;

DECLARE_UNIQUE_INDEX(pg_amop_fam_strat_index, 2653, on pg_amop using btree(amopfamily oid_ops, amoplefttype oid_ops, amoprighttype oid_ops, amopstrategy int2_ops));
#define AccessMethodStrategyIndexId  2653
DECLARE_UNIQUE_INDEX(pg_amop_opr_fam_index, 2654, on pg_amop using btree(amopopr oid_ops, amoppurpose char_ops, amopfamily oid_ops));
#define AccessMethodOperatorIndexId  2654
DECLARE_UNIQUE_INDEX(pg_amop_oid_index, 2756, on pg_amop using btree(oid oid_ops));
#define AccessMethodOperatorOidIndexId	2756

/* allowed values of amoppurpose: */
#define AMOP_SEARCH		's'		/* operator is for search */
#define AMOP_ORDER		'o'		/* operator is for ordering */

/* ----------------
 *		Form_pg_amop corresponds to a pointer to a tuple with
 *		the format of pg_amop relation.
 * ----------------
 */
typedef FormData_pg_amop *Form_pg_amop;

/* ----------------
 *		compiler constants for pg_amop
 * ----------------
 */
#define Natts_pg_amop					8
#define Anum_pg_amop_amopfamily			1
#define Anum_pg_amop_amoplefttype		2
#define Anum_pg_amop_amoprighttype		3
#define Anum_pg_amop_amopstrategy		4
#define Anum_pg_amop_amoppurpose		5
#define Anum_pg_amop_amopopr			6
#define Anum_pg_amop_amopmethod			7
#define Anum_pg_amop_amopsortfamily		8

#endif							/* PG_AMOP_H */
