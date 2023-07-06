/*-------------------------------------------------------------------------
 *
 * pg_variable.h
 *	  definition of session variables system catalog (pg_variables)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_variable.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_VARIABLE_H
#define PG_VARIABLE_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_variable_d.h"
#include "utils/acl.h"

/* ----------------
 *		pg_variable definition.  cpp turns this into
 *		typedef struct FormData_pg_variable
 * ----------------
 */
CATALOG(pg_variable,9222,VariableRelationId)
{
	Oid			oid;			/* oid */

	/* OID of entry in pg_type for variable's type */
	Oid			vartype BKI_LOOKUP(pg_type);

	/*
	 * Used for identity check [oid, create_lsn].
	 *
	 * This column of the 8-byte XlogRecPtr type should be at an address that
	 * is divisible by 8, but before any column of type NameData.
	 */
	XLogRecPtr	varcreate_lsn;

	/* variable name */
	NameData	varname;

	/* OID of namespace containing variable class */
	Oid			varnamespace BKI_LOOKUP(pg_namespace);

	/* variable owner */
	Oid			varowner BKI_LOOKUP(pg_authid);

	/* typmod for variable's type */
	int32		vartypmod BKI_DEFAULT(-1);

	/* variable collation */
	Oid			varcollation BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_collation);


#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/* access permissions */
	aclitem		varacl[1] BKI_DEFAULT(_null_);

#endif
} FormData_pg_variable;

/* ----------------
 *		Form_pg_variable corresponds to a pointer to a tuple with
 *		the format of the pg_variable relation.
 * ----------------
 */
typedef FormData_pg_variable *Form_pg_variable;

DECLARE_TOAST(pg_variable, 9223, 9224);

DECLARE_UNIQUE_INDEX_PKEY(pg_variable_oid_index, 9225, VariableOidIndexId, pg_variable, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_variable_varname_nsp_index, 9226, VariableNameNspIndexId, pg_variable, btree(varname name_ops, varnamespace oid_ops));

extern ObjectAddress CreateVariable(ParseState *pstate,
									CreateSessionVarStmt *stmt);
extern void DropVariableById(Oid varid);

MAKE_SYSCACHE(VARIABLENAMENSP, pg_variable_varname_nsp_index, 8);
MAKE_SYSCACHE(VARIABLEOID, pg_variable_oid_index, 8);

#endif							/* PG_VARIABLE_H */
