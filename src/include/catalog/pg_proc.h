/*-------------------------------------------------------------------------
 *
 * pg_proc.h
 *	  definition of the system "procedure" relation (pg_proc)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_proc.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROC_H
#define PG_PROC_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_proc definition.  cpp turns this into
 *		typedef struct FormData_pg_proc
 * ----------------
 */
#define ProcedureRelationId  1255
#define ProcedureRelation_Rowtype_Id  81

CATALOG(pg_proc,1255) BKI_BOOTSTRAP BKI_ROWTYPE_OID(81) BKI_SCHEMA_MACRO
{
	/* procedure name */
	NameData	proname BKI_ABBREV(n);

	/* OID of namespace containing this proc */
	Oid			pronamespace BKI_DEFAULT(PGNSP);

	/* procedure owner */
	Oid			proowner BKI_DEFAULT(PGUID);

	/* OID of pg_language entry */
	Oid			prolang BKI_DEFAULT(12);

	/* estimated execution cost */
	float4		procost BKI_DEFAULT(1);

	/* estimated # of rows out (if proretset) */
	float4		prorows BKI_DEFAULT(0);

	/* element type of variadic array, or 0 */
	Oid			provariadic BKI_DEFAULT(0);

	/* transforms calls to it during planning */
	regproc		protransform BKI_DEFAULT(0);

	/* is it an aggregate? */
	bool		proisagg BKI_DEFAULT(f);

	/* is it a window function? */
	bool		proiswindow BKI_DEFAULT(f);

	/* security definer */
	bool		prosecdef BKI_DEFAULT(f);

	/* is it a leak-proof function? */
	bool		proleakproof BKI_ABBREV(lp) BKI_DEFAULT(f);

	/* strict with respect to NULLs? */
	bool		proisstrict BKI_ABBREV(is) BKI_DEFAULT(f);

	/* returns a set? */
	bool		proretset BKI_DEFAULT(f);

	/* see PROVOLATILE_ categories below */
	char		provolatile BKI_ABBREV(v) BKI_DEFAULT(v);

	/* see PROPARALLEL_ categories below */
	char		proparallel BKI_ABBREV(p) BKI_DEFAULT(u);

	/* number of arguments */
	int16		pronargs;

	/* number of arguments with defaults */
	int16		pronargdefaults BKI_DEFAULT(0);

	/* OID of result type */
	Oid			prorettype BKI_ABBREV(rt);

	/*
	 * variable-length fields start here, but we allow direct access to
	 * proargtypes
	 */

	/* parameter types (excludes OUT params) */
	oidvector	proargtypes BKI_ABBREV(at);

#ifdef CATALOG_VARLEN

	/* all param types (NULL if IN only) */
	Oid			proallargtypes[1] BKI_DEFAULT(_null_);

	/* parameter modes (NULL if IN only) */
	char		proargmodes[1] BKI_DEFAULT(_null_);

	/* parameter names (NULL if no names) */
	text		proargnames[1] BKI_DEFAULT(_null_);

	/* list of expression trees for argument defaults (NULL if none) */
	pg_node_tree proargdefaults BKI_DEFAULT(_null_);

	/* types for which to apply transforms */
	Oid			protrftypes[1] BKI_DEFAULT(_null_);

	/* procedure source text */
	text		prosrc BKI_ABBREV(s) BKI_FORCE_NOT_NULL;

	/* secondary procedure info (can be NULL) */
	text		probin BKI_DEFAULT(_null_);

	/* procedure-local GUC settings */
	text		proconfig[1] BKI_DEFAULT(_null_);

	/* access permissions */
	aclitem		proacl[1] BKI_DEFAULT(_null_);
#endif
} FormData_pg_proc;

DECLARE_TOAST(pg_proc, 2836, 2837);
DECLARE_UNIQUE_INDEX(pg_proc_oid_index, 2690, on pg_proc using btree(oid oid_ops));
#define ProcedureOidIndexId  2690
DECLARE_UNIQUE_INDEX(pg_proc_proname_args_nsp_index, 2691, on pg_proc using btree(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops));
#define ProcedureNameArgsNspIndexId  2691

/* ----------------
 *		Form_pg_proc corresponds to a pointer to a tuple with
 *		the format of pg_proc relation.
 * ----------------
 */
typedef FormData_pg_proc *Form_pg_proc;

/* ----------------
 *		compiler constants for pg_proc
 * ----------------
 */
#define Natts_pg_proc					29
#define Anum_pg_proc_proname			1
#define Anum_pg_proc_pronamespace		2
#define Anum_pg_proc_proowner			3
#define Anum_pg_proc_prolang			4
#define Anum_pg_proc_procost			5
#define Anum_pg_proc_prorows			6
#define Anum_pg_proc_provariadic		7
#define Anum_pg_proc_protransform		8
#define Anum_pg_proc_proisagg			9
#define Anum_pg_proc_proiswindow		10
#define Anum_pg_proc_prosecdef			11
#define Anum_pg_proc_proleakproof		12
#define Anum_pg_proc_proisstrict		13
#define Anum_pg_proc_proretset			14
#define Anum_pg_proc_provolatile		15
#define Anum_pg_proc_proparallel		16
#define Anum_pg_proc_pronargs			17
#define Anum_pg_proc_pronargdefaults	18
#define Anum_pg_proc_prorettype			19
#define Anum_pg_proc_proargtypes		20
#define Anum_pg_proc_proallargtypes		21
#define Anum_pg_proc_proargmodes		22
#define Anum_pg_proc_proargnames		23
#define Anum_pg_proc_proargdefaults		24
#define Anum_pg_proc_protrftypes		25
#define Anum_pg_proc_prosrc				26
#define Anum_pg_proc_probin				27
#define Anum_pg_proc_proconfig			28
#define Anum_pg_proc_proacl				29

/*
 * Symbolic values for provolatile column: these indicate whether the result
 * of a function is dependent *only* on the values of its explicit arguments,
 * or can change due to outside factors (such as parameter variables or
 * table contents).  NOTE: functions having side-effects, such as setval(),
 * must be labeled volatile to ensure they will not get optimized away,
 * even if the actual return value is not changeable.
 */
#define PROVOLATILE_IMMUTABLE	'i' /* never changes for given input */
#define PROVOLATILE_STABLE		's' /* does not change within a scan */
#define PROVOLATILE_VOLATILE	'v' /* can change even within a scan */

/*
 * Symbolic values for proparallel column: these indicate whether a function
 * can be safely be run in a parallel backend, during parallelism but
 * necessarily in the master, or only in non-parallel mode.
 */
#define PROPARALLEL_SAFE		's' /* can run in worker or master */
#define PROPARALLEL_RESTRICTED	'r' /* can run in parallel master only */
#define PROPARALLEL_UNSAFE		'u' /* banned while in parallel mode */

/*
 * Symbolic values for proargmodes column.  Note that these must agree with
 * the FunctionParameterMode enum in parsenodes.h; we declare them here to
 * be accessible from either header.
 */
#define PROARGMODE_IN		'i'
#define PROARGMODE_OUT		'o'
#define PROARGMODE_INOUT	'b'
#define PROARGMODE_VARIADIC 'v'
#define PROARGMODE_TABLE	't'

#endif							/* PG_PROC_H */
