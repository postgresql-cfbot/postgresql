/*-------------------------------------------------------------------------
 *
 * pg_operator.h
 *	  definition of the system "operator" relation (pg_operator)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_operator.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_OPERATOR_H
#define PG_OPERATOR_H

#include "catalog/genbki.h"
#include "catalog/oid_symbols.h"

/* ----------------
 *		pg_operator definition.  cpp turns this into
 *		typedef struct FormData_pg_operator
 * ----------------
 */
#define OperatorRelationId	2617

CATALOG(pg_operator,2617)
{

	/* name of operator */
	NameData	oprname;

	/* OID of namespace containing this oper */
	Oid			oprnamespace BKI_DEFAULT(PGNSP);

	/* operator owner */
	Oid			oprowner BKI_DEFAULT(PGUID);

	/* 'l', 'r', or 'b' */
	char		oprkind BKI_DEFAULT(b);

	/* can be used in merge join? */
	bool		oprcanmerge BKI_DEFAULT(f);

	/* can be used in hash join? */
	bool		oprcanhash BKI_DEFAULT(f);

	/* left arg type, or 0 if 'l' oprkind */
	regtype		oprleft;

	/* right arg type, or 0 if 'r' oprkind */
	regtype		oprright;

	/* result datatype */
	regtype		oprresult;

	/* OID of commutator oper, or 0 if none */
	regoper		oprcom BKI_DEFAULT(0);

	/* OID of negator oper, or 0 if none */
	regoper		oprnegate BKI_DEFAULT(0);

	/* OID of underlying function */
	regproc		oprcode;

	/* OID of restriction estimator, or 0 */
	regproc		oprrest BKI_DEFAULT(-);

	/* OID of join estimator, or 0 */
	regproc		oprjoin BKI_DEFAULT(-);
} FormData_pg_operator;

DECLARE_UNIQUE_INDEX(pg_operator_oid_index, 2688, on pg_operator using btree(oid oid_ops));
#define OperatorOidIndexId	2688
DECLARE_UNIQUE_INDEX(pg_operator_oprname_l_r_n_index, 2689, on pg_operator using btree(oprname name_ops, oprleft oid_ops, oprright oid_ops, oprnamespace oid_ops));
#define OperatorNameNspIndexId	2689

/* ----------------
 *		Form_pg_operator corresponds to a pointer to a tuple with
 *		the format of pg_operator relation.
 * ----------------
 */
typedef FormData_pg_operator *Form_pg_operator;

/* ----------------
 *		compiler constants for pg_operator
 * ----------------
 */

#define Natts_pg_operator				14
#define Anum_pg_operator_oprname		1
#define Anum_pg_operator_oprnamespace	2
#define Anum_pg_operator_oprowner		3
#define Anum_pg_operator_oprkind		4
#define Anum_pg_operator_oprcanmerge	5
#define Anum_pg_operator_oprcanhash		6
#define Anum_pg_operator_oprleft		7
#define Anum_pg_operator_oprright		8
#define Anum_pg_operator_oprresult		9
#define Anum_pg_operator_oprcom			10
#define Anum_pg_operator_oprnegate		11
#define Anum_pg_operator_oprcode		12
#define Anum_pg_operator_oprrest		13
#define Anum_pg_operator_oprjoin		14

#endif							/* PG_OPERATOR_H */
