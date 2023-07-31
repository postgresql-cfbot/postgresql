/*-------------------------------------------------------------------------
 *
 * pg_variable.h
 *	  definition of session variables system catalog (pg_variables)
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
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
 *
 * The column varcreate_lsn of XlogRecPtr type (8-byte) should be on position
 * divisible by 8 unconditionally and before varname column of NameData type.
 * see sanity_check:check_columns
 * ----------------
 */
CATALOG(pg_variable,9222,VariableRelationId)
{
	Oid			oid;			/* oid */

	/* OID of entry in pg_type for variable's type */
	Oid			vartype BKI_LOOKUP(pg_type);

	/* used for identity check [oid, create_lsn] */
	XLogRecPtr	varcreate_lsn;

	/* variable name */
	NameData	varname;

	/* OID of namespace containing variable class */
	Oid			varnamespace BKI_LOOKUP(pg_namespace);

	/* variable owner */
	Oid			varowner BKI_LOOKUP(pg_authid);

	/* typmod for variable's type */
	int32		vartypmod;

	/* don't allow NULL */
	bool		varisnotnull;

	/* don't allow changes */
	bool		varisimmutable;

	/* action on transaction end */
	char		vareoxaction;

	/* variable collation */
	Oid			varcollation BKI_LOOKUP_OPT(pg_collation);


#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/* access permissions */
	aclitem		varacl[1] BKI_DEFAULT(_null_);

	/* list of expression trees for variable default (NULL if none) */
	pg_node_tree vardefexpr BKI_DEFAULT(_null_);

#endif
} FormData_pg_variable;

typedef enum VariableEOXAction
{
	VARIABLE_EOX_NOOP = 'n',	/* NOOP */
	VARIABLE_EOX_DROP = 'd',	/* ON COMMIT DROP */
	VARIABLE_EOX_RESET = 'r',	/* ON TRANSACTION END RESET */
}			VariableEOXAction;

/* ----------------
 *		Form_pg_variable corresponds to a pointer to a tuple with
 *		the format of pg_variable relation.
 * ----------------
 */
typedef FormData_pg_variable *Form_pg_variable;

DECLARE_TOAST(pg_variable, 9223, 9224);

DECLARE_UNIQUE_INDEX_PKEY(pg_variable_oid_index, 9225, VariableOidIndexId, on pg_variable using btree(oid oid_ops));
#define VariableObjectIndexId 9225

DECLARE_UNIQUE_INDEX(pg_variable_varname_nsp_index, 9226, VariableNameNspIndexId, on pg_variable using btree(varname name_ops, varnamespace oid_ops));
#define VariableNameNspIndexId  9226

extern ObjectAddress CreateVariable(ParseState *pstate,
									CreateSessionVarStmt *stmt);
extern void DropVariable(Oid varid);

#endif							/* PG_VARIABLE_H */
