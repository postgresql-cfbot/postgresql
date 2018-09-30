/*-------------------------------------------------------------------------
 *
 * pg_variable.h
 *	  definition of schema variables system catalog (pg_variables)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
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
CATALOG(pg_variable,4287,VariableRelationId)
{
	NameData	varname;		/* variable name */
	Oid			varnamespace;	/* OID of namespace containing variable class */
	Oid			vartype;		/* OID of entry in pg_type for variable's type */
	int32		vartypmod;			/* typmode for variable's type */
	Oid			varowner;		/* class owner */
	Oid			varcollation;	/* variable collation */
	bool		varisnotnull;	/* Don't allow NULL */
	bool		varistransact;	/* Variable is transactional */
	char		vareoxaction;	/* action on transaction end */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/* list of expression trees for variable default (NULL if none) */
	pg_node_tree vardefexpr BKI_DEFAULT(_null_);

	aclitem		varacl[1] BKI_DEFAULT(_null_);		/* access permissions */

#endif
} FormData_pg_variable;

typedef enum VariableEOXActionCodes
{
	VARIABLE_EOX_CODE_NOOP = 'n',			/* NOOP */
	VARIABLE_EOX_CODE_DROP = 'd',			/* ON COMMIT DROP */
	VARIABLE_EOX_CODE_RESET = 'r',			/* ON COMMIT RESET */
} VariableEOXActionCodes;

/* ----------------
 *		Form_pg_variable corresponds to a pointer to a tuple with
 *		the format of pg_variable relation.
 * ----------------
 */
typedef FormData_pg_variable *Form_pg_variable;

typedef struct Variable
{
	Oid		oid;
	char   *name;
	Oid		namespace;
	Oid		typid;
	int32	typmod;
	Oid		owner;
	Oid		collation;
	bool	is_not_null;
	bool	is_transact;
	VariableEOXAction eoxaction;
	bool	has_defexpr;
	Node   *defexpr;
	Acl	   *acl;
} Variable;

/* returns fields from pg_variable table */
extern char *get_schema_variable_name(Oid varid);
extern void get_schema_variable_type_typmod_collid(Oid varid,
												   Oid *typid,
												   int32 *typmod,
												   Oid *collid);

/* returns name of variable based on current search path */
extern char *schema_variable_get_name(Oid varid);

extern void initVariable(Variable *var,
								Oid varid,
								bool missing_ok,
								bool fast_only);
extern ObjectAddress VariableCreate(const char *varName,
									   Oid varNamespace,
									   Oid varType,
									   int32 varTypmod,
									   Oid varOwner,
									   Oid varCollation,
									   Node *varDefexpr,
									   VariableEOXAction eoxaction,
									   bool is_not_null,
									   bool is_transact,
									   bool if_not_exists);

#endif							/* PG_VARIABLE_H */
