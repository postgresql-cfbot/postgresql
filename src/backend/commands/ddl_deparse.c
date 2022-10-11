/*-------------------------------------------------------------------------
 *
 * ddl_deparse.c
 *	  Functions to convert utility commands to machine-parseable
 *	  representation
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *
 * This is intended to provide JSON blobs representing DDL commands, which can
 * later be re-processed into plain strings by well-defined sprintf-like
 * expansion.  These JSON objects are intended to allow for machine-editing of
 * the commands, by replacing certain nodes within the objects.
 *
 * Much of the information in the output blob actually comes from system
 * catalogs, not from the command parse node, as it is impossible to reliably
 * construct a fully-specified command (i.e. one not dependent on search_path
 * etc) looking only at the parse node.
 *
 * IDENTIFICATION
 *	  src/backend/commands/ddl_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "tcop/ddl_deparse.h"
#include "access/amapi.h"
#include "access/table.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/optimizer.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

/* Estimated length of the generated jsonb string */
#define JSONB_ESTIMATED_LEN 128

/*
 * Before they are turned into JSONB representation, each command is
 * represented as an object tree, using the structs below.
 */
typedef enum
{
	ObjTypeNull,
	ObjTypeBool,
	ObjTypeString,
	ObjTypeArray,
	ObjTypeInteger,
	ObjTypeFloat,
	ObjTypeObject
} ObjType;

typedef struct ObjTree
{
	slist_head	params;
	int			numParams;
	StringInfo	fmtinfo;
	bool		present;
} ObjTree;

typedef struct ObjElem
{
	char	   *name;
	ObjType		objtype;

	union
	{
		bool		boolean;
		char	   *string;
		int64		integer;
		float8		flt;
		ObjTree	   *object;
		List	   *array;
	} value;
	slist_node	node;
} ObjElem;

bool verbose = true;

static void append_array_object(ObjTree *tree, char *name, List *array);
static void append_bool_object(ObjTree *tree, char *name, bool value);
static void append_float_object(ObjTree *tree, char *name, float8 value);
static void append_null_object(ObjTree *tree, char *name);
static void append_object_object(ObjTree *tree, char *name, ObjTree *value);
static char *append_object_to_format_string(ObjTree *tree, char *sub_fmt);
static void append_premade_object(ObjTree *tree, ObjElem *elem);
static void append_string_object(ObjTree *tree, char *name, char *value);
static void format_type_detailed(Oid type_oid, int32 typemod,
								 Oid *nspid, char **typname, char **typemodstr,
								 bool *typarray);
static List *FunctionGetDefaults(text *proargdefaults);
static ObjElem *new_object(ObjType type, char *name);
static ObjTree *new_objtree_for_qualname_id(Oid classId, Oid objectId);
static ObjTree *new_objtree_for_rolespec(RoleSpec *spec);
static ObjElem *new_object_object(ObjTree *value);
static ObjTree *new_objtree_VA(char *fmt, int numobjs,...);
static ObjElem *new_string_object(char *value);
static JsonbValue *objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state);
static void pg_get_indexdef_detailed(Oid indexrelid,
									 char **index_am,
									 char **definition,
									 char **reloptions,
									 char **tablespace,
									 char **whereClause);
static char *RelationGetColumnDefault(Relation rel, AttrNumber attno,
									  List *dpcontext, List **exprs);
static const char *stringify_objtype(ObjectType objtype);

static ObjTree *deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
								  ColumnDef *coldef, bool is_alter, List **exprs);
static ObjTree *deparse_ColumnIdentity(Oid seqrelid, char identity, bool alter_table);
static ObjTree *deparse_ColumnSetOptions(AlterTableCmd *subcmd);
static ObjTree *deparse_DefineStmt_Collation(Oid objectId, DefineStmt *define,
											 ObjectAddress fromCollid);
static ObjTree *deparse_DefineStmt_Operator(Oid objectId, DefineStmt *define);
static ObjTree *deparse_DefineStmt_Type(Oid objectId, DefineStmt *define);
static ObjTree *deparse_DefineStmt_TSConfig(Oid objectId, DefineStmt *define, ObjectAddress copied);
static ObjTree *deparse_DefineStmt_TSParser(Oid objectId, DefineStmt *define);
static ObjTree *deparse_DefineStmt_TSDictionary(Oid objectId, DefineStmt *define);
static ObjTree *deparse_DefineStmt_TSTemplate(Oid objectId, DefineStmt *define);

static ObjTree *deparse_DefElem(DefElem *elem, bool is_reset);
static ObjTree *deparse_FunctionSet(VariableSetKind kind, char *name, char *value);
static ObjTree *deparse_OnCommitClause(OnCommitAction option);
static ObjTree *deparse_RelSetOptions(AlterTableCmd *subcmd);

static inline ObjElem *deparse_Seq_Cache(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Cycle(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_IncrementBy(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Minvalue(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Maxvalue(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table);
static ObjElem *deparse_Seq_OwnedBy(ObjTree *parent, Oid sequenceId, bool alter_table);
static inline ObjElem *deparse_Seq_Restart(ObjTree *parent, Form_pg_sequence_data seqdata);
static inline ObjElem *deparse_Seq_Startwith(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Type_Storage(ObjTree *parent, Form_pg_type typForm);
static inline ObjElem *deparse_Type_Receive(ObjTree *parent, Form_pg_type typForm);
static inline ObjElem *deparse_Type_Send(ObjTree *parent, Form_pg_type typForm);
static inline ObjElem *deparse_Type_Typmod_In(ObjTree *parent, Form_pg_type typForm);
static inline ObjElem *deparse_Type_Typmod_Out(ObjTree *parent, Form_pg_type typForm);
static inline ObjElem *deparse_Type_Analyze(ObjTree *parent, Form_pg_type typForm);
static inline ObjElem *deparse_Type_Subscript(ObjTree *parent, Form_pg_type typForm);

static List *deparse_InhRelations(Oid objectId);
static List *deparse_TableElements(Relation relation, List *tableElements, List *dpcontext,
								   bool typed, bool composite);

/*
 * Add common clauses to CreatePolicy or AlterPolicy deparse objects
 */
static void
add_policy_clauses(ObjTree *policyStmt, Oid policyOid, List *roles,
				   bool do_qual, bool do_with_check)
{
	Relation	polRel = table_open(PolicyRelationId, AccessShareLock);
	HeapTuple	polTup = get_catalog_object_by_oid(polRel, Anum_pg_policy_oid, policyOid);
	Form_pg_policy polForm;

	if (!HeapTupleIsValid(polTup))
		elog(ERROR, "cache lookup failed for policy %u", policyOid);

	polForm = (Form_pg_policy) GETSTRUCT(polTup);

	/* add the "ON table" clause */
	append_object_object(policyStmt, "ON %{table}D",
						 new_objtree_for_qualname_id(RelationRelationId,
													 polForm->polrelid));

	/*
	 * Add the "TO role" clause, if any.  In the CREATE case, it always
	 * contains at least PUBLIC, but in the ALTER case it might be empty.
	 */
	if (roles)
	{
		List   *list = NIL;
		ListCell *cell;

		foreach (cell, roles)
		{
			RoleSpec   *spec = (RoleSpec *) lfirst(cell);

			list = lappend(list,
						   new_object_object(new_objtree_for_rolespec(spec)));
		}
		append_array_object(policyStmt,"TO %{role:, }R", list);
	}
	else
	{
		append_bool_object(policyStmt, "present", false);
	}

	/* add the USING clause, if any */
	if (do_qual)
	{
		Datum	deparsed;
		Datum	storedexpr;
		bool	isnull;

		storedexpr = heap_getattr(polTup, Anum_pg_policy_polqual,
								  RelationGetDescr(polRel), &isnull);
		if (isnull)
			elog(ERROR, "invalid NULL polqual expression in policy %u", policyOid);
		deparsed = DirectFunctionCall2(pg_get_expr, storedexpr, polForm->polrelid);
		append_string_object(policyStmt, "USING (%{expression}s)",
							 TextDatumGetCString(deparsed));
	}
	else
		append_bool_object(policyStmt, "present", false);

	/* add the WITH CHECK clause, if any */
	if (do_with_check)
	{
		Datum	deparsed;
		Datum	storedexpr;
		bool	isnull;

		storedexpr = heap_getattr(polTup, Anum_pg_policy_polwithcheck,
								  RelationGetDescr(polRel), &isnull);
		if (isnull)
			elog(ERROR, "invalid NULL polwithcheck expression in policy %u", policyOid);
		deparsed = DirectFunctionCall2(pg_get_expr, storedexpr, polForm->polrelid);
		append_string_object(policyStmt, "WITH CHECK (%{expression}s)",
							 TextDatumGetCString(deparsed));
	}
	else
		append_bool_object(policyStmt, "present", false);

	relation_close(polRel, AccessShareLock);
}

/* Append an array parameter to a tree */
static void
append_array_object(ObjTree *tree, char *sub_fmt, List *array)
{
	ObjElem	*param;
	char *object_name;

	Assert(sub_fmt);

	if (list_length(array) == 0)
		return;

	if (!verbose)
	{
		ListCell *lc;

		/* Extract the ObjElems whose present flag is true */
		foreach(lc, array)
		{
			ObjElem *elem = (ObjElem *) lfirst(lc);

			Assert(elem->objtype == ObjTypeObject ||
				   elem->objtype == ObjTypeString);

			if (!elem->value.object->present &&
				elem->objtype == ObjTypeObject)
				array = foreach_delete_current(array, lc);
		}

	}

	object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeArray, object_name);
	param->value.array = array;
	append_premade_object(tree, param);
}

/* Append a boolean parameter to a tree */
static void
append_bool_object(ObjTree *tree, char *sub_fmt, bool value)
{
	ObjElem  *param;
	char	 *object_name = sub_fmt;
	bool	  is_present_flag = false;

	Assert(sub_fmt);

	/* Check if the present is part of the format string and store the boolean value*/
	if (strcmp(sub_fmt, "present") == 0)
	{
		is_present_flag = true;
		tree->present = value;
	}

	if (!verbose && !tree->present)
		return;

	if (!is_present_flag)
		object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeBool, object_name);
	param->value.boolean = value;
	append_premade_object(tree, param);
}

/*
 * Append a float8 parameter to a tree.
 */
static void
append_float_object(ObjTree *tree, char *sub_fmt, float8 value)
{
	ObjElem	   *param;
	char	   *object_name;

	Assert(sub_fmt);

	object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeFloat, object_name);
	param->value.flt = value;
	append_premade_object(tree, param);
}

/*
 * Append the input format string to the ObjTree.
 */
static void
append_format_string(ObjTree *tree, char *sub_fmt)
{
	int				len;
	char		   *fmt;

	if (tree->fmtinfo == NULL)
		return;

	fmt = tree->fmtinfo->data;
	len = tree->fmtinfo->len;

	/* Add a separator if necessary */
	if (len > 0 && fmt[len - 1] != ' ')
		appendStringInfoSpaces(tree->fmtinfo, 1);

	appendStringInfoString(tree->fmtinfo, sub_fmt);
}

/* Append a NULL object to a tree */
static void
append_null_object(ObjTree *tree, char *sub_fmt)
{
	char *object_name;

	Assert(sub_fmt);

	if (!verbose)
		return;

	object_name = append_object_to_format_string(tree, sub_fmt);

	append_premade_object(tree, new_object(ObjTypeNull, object_name));
}

/* Append an object parameter to a tree */
static void
append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value)
{
	ObjElem	*param;
	char   *object_name;

	Assert(sub_fmt);

	if (!verbose && !value->present)
		return;

	object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeObject, object_name);
	param->value.object = value;
	append_premade_object(tree, param);
}

/*
 * Return the object name which is extracted from the input "*%{name[:.]}*"
 * style string. And append the input format string to the ObjTree.
 */
static char *
append_object_to_format_string(ObjTree *tree, char *sub_fmt)
{
	StringInfoData	object_name;
	const char	   *end_ptr;
	const char	   *cp;
	bool			start_copy = false;

	if (sub_fmt == NULL || tree->fmtinfo == NULL)
		return sub_fmt;

	initStringInfo(&object_name);
	end_ptr = sub_fmt + strlen(sub_fmt);

	for (cp = sub_fmt; cp < end_ptr; cp++)
	{
		if (*cp == '{')
		{
			start_copy = true;
			continue;
		}

		if (!start_copy)
			continue;

		if (*cp == ':' || *cp == '}')
			break;

		appendStringInfoCharMacro(&object_name, *cp);
	}

	if (object_name.len == 0)
		elog(ERROR, "object name not found");

	append_format_string(tree, sub_fmt);

	return object_name.data;
}

/* Append a preallocated parameter to a tree */
static inline void
append_premade_object(ObjTree *tree, ObjElem *elem)
{
	slist_push_head(&tree->params, &elem->node);
	tree->numParams++;
}

/* Append a string parameter to a tree */
static void
append_string_object(ObjTree *tree, char *sub_fmt, char *value)
{
	ObjElem	   *param;
	char	   *object_name;

	Assert(sub_fmt);

	if (!verbose && (value == NULL || value[0] == '\0'))
		return;

	object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeString, object_name);
	param->value.string = value;
	append_premade_object(tree, param);
}

/*
 * Similar to format_type_extended, except we return each bit of information
 * separately:
 *
 * - nspid is the schema OID.  For certain SQL-standard types which have weird
 *   typmod rules, we return InvalidOid; caller is expected to not schema-
 *   qualify the name nor add quotes to the type name in this case.
 *
 * - typname is set to the type name, without quotes
 *
 * - typemodstr is set to the typemod, if any, as a string with parens
 *
 * - typarray indicates whether []s must be added
 *
 * We don't try to decode type names to their standard-mandated names, except
 * in the cases of types with unusual typmod rules.
 */
static void
format_type_detailed(Oid type_oid, int32 typemod,
					 Oid *nspid, char **typname, char **typemodstr,
					 bool *typarray)
{
	HeapTuple		tuple;
	Form_pg_type	typeform;
	Oid				array_base_type;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", type_oid);

	typeform = (Form_pg_type) GETSTRUCT(tuple);

	/*
	 * Check if it's a regular (variable length) array type.  As above,
	 * fixed-length array types such as "name" shouldn't get deconstructed.
	 */
	array_base_type = typeform->typelem;

	*typarray = (IsTrueArrayType(typeform) && typeform->typstorage != TYPSTORAGE_PLAIN);

	if (*typarray)
	{
		/* Switch our attention to the array element type */
		ReleaseSysCache(tuple);
		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", type_oid);

		typeform = (Form_pg_type) GETSTRUCT(tuple);
		type_oid = array_base_type;
	}

	/*
	 * Special-case crock for types with strange typmod rules where we put
	 * typmod at the middle of name(e.g. TIME(6) with time zone ). We cannot
	 * schema-qualify nor add quotes to the type name in these cases.
	 */
	if (type_oid == INTERVALOID ||
		type_oid == TIMESTAMPOID ||
		type_oid == TIMESTAMPTZOID ||
		type_oid == TIMEOID ||
		type_oid == TIMETZOID)
	{
		switch (type_oid)
		{
			case INTERVALOID:
				*typname = pstrdup("INTERVAL");
				break;
			case TIMESTAMPTZOID:
				if (typemod < 0)
					*typname = pstrdup("TIMESTAMP WITH TIME ZONE");
				else
					/* otherwise, WITH TZ is added by typmod. */
					*typname = pstrdup("TIMESTAMP");
				break;
			case TIMESTAMPOID:
				*typname = pstrdup("TIMESTAMP");
				break;
			case TIMETZOID:
				if (typemod < 0)
					*typname = pstrdup("TIME WITH TIME ZONE");
				else
					/* otherwise, WITH TZ is added by typmode. */
					*typname = pstrdup("TIME");
				break;
			case TIMEOID:
				*typname = pstrdup("TIME");
				break;
		}
		*nspid = InvalidOid;
	}
	else
	{
		/*
		 * No additional processing is required for other types, so get the type
		 * name and schema directly from the catalog.
		 */
		*nspid = typeform->typnamespace;
		*typname = pstrdup(NameStr(typeform->typname));
	}

	if (typemod >= 0)
		*typemodstr = printTypmod("", typemod, typeform->typmodout);
	else
		*typemodstr = pstrdup("");

	ReleaseSysCache(tuple);
}

/*
 * Return the defaults values of arguments to a function, as a list of
 * deparsed expressions.
 */
static List *
FunctionGetDefaults(text *proargdefaults)
{
	List   *nodedefs;
	List   *strdefs = NIL;
	ListCell *cell;

	nodedefs = (List *) stringToNode(text_to_cstring(proargdefaults));
	if (!IsA(nodedefs, List))
		elog(ERROR, "proargdefaults is not a list");

	foreach(cell, nodedefs)
	{
		Node   *onedef = lfirst(cell);

		strdefs = lappend(strdefs, deparse_expression(onedef, NIL, false, false));
	}

	return strdefs;
}

/*
 * Return the string representation of the given RELPERSISTENCE value.
 */
static char *
get_persistence_str(char persistence)
{
	switch (persistence)
	{
		case RELPERSISTENCE_TEMP:
			return "TEMPORARY";
		case RELPERSISTENCE_UNLOGGED:
			return "UNLOGGED";
		case RELPERSISTENCE_PERMANENT:
			return "";
		default:
			elog(ERROR, "unexpected persistence marking %c", persistence);
			return "";		/* make compiler happy */
	}
}

/* Allocate a new parameter */
static ObjElem *
new_object(ObjType type, char *name)
{
	ObjElem	*param;

	param = palloc0(sizeof(ObjElem));
	param->name = name;
	param->objtype = type;

	return param;
}

/* Allocate a new object parameter */
static ObjElem *
new_object_object(ObjTree *value)
{
	ObjElem	*param;

	param = new_object(ObjTypeObject, NULL);
	param->value.object = value;

	return param;
}

/*
 * Allocate a new object tree to store parameter values.
 */
static ObjTree *
new_objtree(char *fmt)
{
	ObjTree	*params;

	params = palloc0(sizeof(ObjTree));
	params->present = true;
	slist_init(&params->params);

	if (fmt)
	{
		params->fmtinfo = makeStringInfo();
		appendStringInfoString(params->fmtinfo, fmt);
	}

	return params;
}

/*
 * A helper routine to setup %{}D and %{}O elements.
 *
 * Elements "schemaname" and "objname" are set.  If the namespace OID
 * corresponds to a temp schema, that's set to "pg_temp".
 *
 * The difference between those two element types is whether the objname will
 * be quoted as an identifier or not, which is not something that this routine
 * concerns itself with; that will be up to the expand function.
 */
static ObjTree *
new_objtree_for_qualname(Oid nspid, char *name)
{
	ObjTree	*qualified;
	char	   *namespace;

	if (isAnyTempNamespace(nspid))
		namespace = pstrdup("pg_temp");
	else
		namespace = get_namespace_name(nspid);

	qualified = new_objtree_VA(NULL, 2,
							  "schemaname", ObjTypeString, namespace,
							  "objname", ObjTypeString, pstrdup(name));

	return qualified;
}

/*
 * A helper routine to setup %{}D and %{}O elements, with the object specified
 * by classId/objId.
 *
 */
static ObjTree *
new_objtree_for_qualname_id(Oid classId, Oid objectId)
{
	ObjTree	*qualified;
	Relation	catalog;
	HeapTuple	catobj;
	Datum		objnsp;
	Datum		objname;
	AttrNumber	Anum_name;
	AttrNumber	Anum_namespace;
	AttrNumber	Anum_oid = get_object_attnum_oid(classId);
	bool		isnull;

	catalog = table_open(classId, AccessShareLock);

	catobj = get_catalog_object_by_oid(catalog, Anum_oid, objectId);
	if (!catobj)
		elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
			 objectId, RelationGetRelationName(catalog));
	Anum_name = get_object_attnum_name(classId);
	Anum_namespace = get_object_attnum_namespace(classId);

	objnsp = heap_getattr(catobj, Anum_namespace, RelationGetDescr(catalog),
						  &isnull);
	if (isnull)
		elog(ERROR, "unexpected NULL namespace");

	objname = heap_getattr(catobj, Anum_name, RelationGetDescr(catalog),
						   &isnull);
	if (isnull)
		elog(ERROR, "unexpected NULL name");

	qualified = new_objtree_for_qualname(DatumGetObjectId(objnsp),
										 NameStr(*DatumGetName(objname)));
	table_close(catalog, AccessShareLock);

	return qualified;
}

/*
 * Helper routine for %{}R objects, with role specified by RoleSpec node.
 * Special values such as ROLESPEC_CURRENT_USER are expanded to their final
 * names.
 */
static ObjTree *
new_objtree_for_rolespec(RoleSpec *spec)
{
	ObjTree	   *role;
	char	   *roletype;

	if (spec->roletype != ROLESPEC_PUBLIC)
		roletype = get_rolespec_name(spec);
	else
		roletype = pstrdup("");

	role = new_objtree_VA(NULL,2,
						  "is_public", ObjTypeBool, spec->roletype == ROLESPEC_PUBLIC,
						  "rolename", ObjTypeString, roletype);
	return role;
}

/*
 * Helper routine for %{}R objects, with role specified by OID.  (ACL_ID_PUBLIC
 * means to use "public").
 */
static ObjTree *
new_objtree_for_role_id(Oid roleoid)
{
	ObjTree    *role;

	role = new_objtree("");

	if (roleoid != ACL_ID_PUBLIC)
	{
		HeapTuple	roltup;
		char	   *rolename;

		roltup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleoid));
		if (!HeapTupleIsValid(roltup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("role with OID %u does not exist", roleoid)));

		rolename = NameStr(((Form_pg_authid) GETSTRUCT(roltup))->rolname);
		append_string_object(role, "%{rolename}I", pstrdup(rolename));

		ReleaseSysCache(roltup);
	}
	else
		append_string_object(role, "%{rolename}I", "public");

	return role;
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree *
new_objtree_for_type(Oid typeId, int32 typmod)
{
	ObjTree	*typeParam;
	Oid			typnspid;
	char	   *typnsp;
	char	   *typename = NULL;
	char	   *typmodstr;
	bool		typarray;

	format_type_detailed(typeId, typmod,
						 &typnspid, &typename, &typmodstr, &typarray);

	if (OidIsValid(typnspid))
		typnsp = get_namespace_name_or_temp(typnspid);
	else
		typnsp = pstrdup("");

	typeParam = new_objtree_VA(NULL, 4,
							   "schemaname", ObjTypeString, typnsp,
							   "typename", ObjTypeString, typename,
							   "typmod", ObjTypeString, typmodstr,
							   "typarray", ObjTypeBool, typarray);

	return typeParam;
}

/*
 * Allocate a new object tree to store parameter values -- varargs version.
 *
 * The "fmt" argument is used to append as a "fmt" element in the output blob.
 * numobjs indicates the number of extra elements to append; for each one, a
 * name (string), type (from the ObjType enum) and value must be supplied.  The
 * value must match the type given; for instance, ObjTypeInteger requires an
 * int64, ObjTypeString requires a char *, ObjTypeArray requires a list (of
 * ObjElem), ObjTypeObject requires an ObjTree, and so on.  Each element type *
 * must match the conversion specifier given in the format string, as described
 * in ddl_deparse_expand_command, q.v.
 *
 * Note we don't have the luxury of sprintf-like compiler warnings for
 * malformed argument lists.
 */
static ObjTree *
new_objtree_VA(char *fmt, int numobjs,...)
{
	ObjTree	   *tree;
	va_list		args;
	int			i;

	/* Set up the toplevel object and its "fmt" */
	tree = new_objtree(fmt);

	/* And process the given varargs */
	va_start(args, numobjs);
	for (i = 0; i < numobjs; i++)
	{
		char	   *name;
		ObjType		type;
		ObjElem	   *elem;

		name = va_arg(args, char *);
		type = va_arg(args, ObjType);
		elem = new_object(type, NULL);

		/*
		 * For all other param types there must be a value in the varargs.
		 * Fetch it and add the fully formed subobject into the main object.
		 */
		switch (type)
		{
			case ObjTypeNull:
				/* Null params don't have a value (obviously) */
				break;
			case ObjTypeBool:
				elem->value.boolean = va_arg(args, int);
				break;
			case ObjTypeString:
				elem->value.string = va_arg(args, char *);
				break;
			case ObjTypeArray:
				elem->value.array = va_arg(args, List *);
				break;
			case ObjTypeInteger:
				elem->value.integer = va_arg(args, int64);
				break;
			case ObjTypeFloat:
				elem->value.flt = va_arg(args, double);
				break;
			case ObjTypeObject:
				elem->value.object = va_arg(args, ObjTree *);
				break;
			default:
				elog(ERROR, "invalid ObjTree element type %d", type);
		}

		elem->name = name;
		append_premade_object(tree, elem);
	}

	va_end(args);
	return tree;
}

/* Allocate a new string object */
static ObjElem *
new_string_object(char *value)
{
	ObjElem    *param;

	Assert(value);

	param = new_object(ObjTypeString, NULL);
	param->value.string = value;

	return param;
}

/*
 * Process the pre-built format string from the ObjTree into the output parse
 * state.
 */
static void
objtree_fmt_to_jsonb_element(JsonbParseState *state, ObjTree *tree)
{
	JsonbValue	key;
	JsonbValue	val;

	if (tree->fmtinfo == NULL)
		return;

	/* Push the key first */
	key.type = jbvString;
	key.val.string.val = "fmt";
	key.val.string.len = strlen(key.val.string.val);
	pushJsonbValue(&state, WJB_KEY, &key);

	/* Then process the pre-built format string */
	val.type = jbvString;
	val.val.string.len = tree->fmtinfo->len;
	val.val.string.val = tree->fmtinfo->data;
	pushJsonbValue(&state, WJB_VALUE, &val);
}

/*
 * Create a JSONB representation from an ObjTree.
 */
static Jsonb *
objtree_to_jsonb(ObjTree *tree)
{
	JsonbValue *value;

	value = objtree_to_jsonb_rec(tree, NULL);
	return JsonbValueToJsonb(value);
}

/*
 * Helper for objtree_to_jsonb: process an individual element from an object or
 * an array into the output parse state.
 */
static void
objtree_to_jsonb_element(JsonbParseState *state, ObjElem *object,
						 JsonbIteratorToken elem_token)
{
	ListCell   *cell;
	JsonbValue	val;

	switch (object->objtype)
	{
		case ObjTypeNull:
			val.type = jbvNull;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeString:
			val.type = jbvString;
			val.val.string.len = strlen(object->value.string);
			val.val.string.val = object->value.string;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeInteger:
			val.type = jbvNumeric;
			val.val.numeric = (Numeric)
				DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													object->value.integer));
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeFloat:
			val.type = jbvNumeric;
			val.val.numeric = (Numeric)
				DatumGetNumeric(DirectFunctionCall1(float8_numeric,
													object->value.integer));
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeBool:
			val.type = jbvBool;
			val.val.boolean = object->value.boolean;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeObject:
			/* Recursively add the object into the existing parse state */
			objtree_to_jsonb_rec(object->value.object, state);
			break;

		case ObjTypeArray:
			pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
			foreach(cell, object->value.array)
			{
				ObjElem   *elem = lfirst(cell);

				objtree_to_jsonb_element(state, elem, WJB_ELEM);
			}
			pushJsonbValue(&state, WJB_END_ARRAY, NULL);
			break;

		default:
			elog(ERROR, "unrecognized object type %d", object->objtype);
			break;
	}
}

/*
 * Recursive helper for objtree_to_jsonb.
 */
static JsonbValue *
objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state)
{
	slist_iter	iter;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	objtree_fmt_to_jsonb_element(state, tree);

	slist_foreach(iter, &tree->params)
	{
		ObjElem	*object = slist_container(ObjElem, node, iter.cur);
		JsonbValue	key;

		/* Push the key first */
		key.type = jbvString;
		key.val.string.len = strlen(object->name);
		key.val.string.val = object->name;
		pushJsonbValue(&state, WJB_KEY, &key);

		/* Then process the value according to its type */
		objtree_to_jsonb_element(state, object, WJB_VALUE);
	}

	return pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Subroutine for CREATE TABLE/CREATE DOMAIN deparsing.
 *
 * Given a table OID or domain OID, obtain its constraints and append them to
 * the given elements list.  The updated list is returned.
 *
 * This works for typed tables, regular tables, and domains.
 *
 * Note that CONSTRAINT_FOREIGN constraints are always ignored.
 */
static List *
obtainConstraints(List *elements, Oid relationId, Oid domainId)
{
	Relation	conRel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	ObjTree	*constr;

	/* Only one may be valid */
	Assert(OidIsValid(relationId) ^ OidIsValid(domainId));

	/*
	 * Scan pg_constraint to fetch all constraints linked to the given
	 * relation.
	 */
	conRel = table_open(ConstraintRelationId, AccessShareLock);
	if (OidIsValid(relationId))
	{
		ScanKeyInit(&key,
					Anum_pg_constraint_conrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relationId));
		scan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId,
								  true, NULL, 1, &key);
	}
	else
	{
		Assert(OidIsValid(domainId));
		ScanKeyInit(&key,
					Anum_pg_constraint_contypid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(domainId));
		scan = systable_beginscan(conRel, ConstraintTypidIndexId,
								  true, NULL, 1, &key);
	}

	/*
	 * For each constraint, add a node to the list of table elements.  In
	 * these nodes we include not only the printable information ("fmt"), but
	 * also separate attributes to indicate the type of constraint, for
	 * automatic processing.
	 */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint constrForm;
		char	   *contype;

		constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		switch (constrForm->contype)
		{
			case CONSTRAINT_CHECK:
				contype = "check";
				break;
			case CONSTRAINT_FOREIGN:
				continue;	/* not here */
			case CONSTRAINT_PRIMARY:
				contype = "primary key";
				break;
			case CONSTRAINT_UNIQUE:
				contype = "unique";
				break;
			case CONSTRAINT_TRIGGER:
				contype = "trigger";
				break;
			case CONSTRAINT_EXCLUSION:
				contype = "exclusion";
				break;
			default:
				elog(ERROR, "unrecognized constraint type");
		}

		/*
		 * "type" and "contype" are not part of the printable output, but are
		 * useful to programmatically distinguish these from columns and among
		 * different constraint types.
		 *
		 * XXX it might be useful to also list the column names in a PK, etc.
		 */
		constr = new_objtree_VA("CONSTRAINT %{name}I %{definition}s",
								4,
								"type", ObjTypeString, "constraint",
								"contype", ObjTypeString, contype,
								"name", ObjTypeString, NameStr(constrForm->conname),
								"definition", ObjTypeString,
								 pg_get_constraintdef_command_simple(constrForm->oid));
		elements = lappend(elements, new_object_object(constr));
	}

	systable_endscan(scan);
	table_close(conRel, AccessShareLock);

	return elements;
}

/*
 * Return an index definition, split in several pieces.
 *
 * A large amount of code is duplicated from  pg_get_indexdef_worker, but
 * control flow is different enough that it doesn't seem worth keeping them
 * together.
 */
static void
pg_get_indexdef_detailed(Oid indexrelid,
						 char **index_am,
						 char **definition,
						 char **reloptions,
						 char **tablespace,
						 char **whereClause)
{
	HeapTuple	ht_idx;
	HeapTuple	ht_idxrel;
	HeapTuple	ht_am;
	Form_pg_index idxrec;
	Form_pg_class idxrelrec;
	Form_pg_am	amrec;
	IndexAmRoutine *amroutine;
	List	   *indexprs;
	ListCell   *indexpr_item;
	List	   *context;
	Oid			indrelid;
	int			keyno;
	Datum		indcollDatum;
	Datum		indclassDatum;
	Datum		indoptionDatum;
	bool		isnull;
	oidvector  *indcollation;
	oidvector  *indclass;
	int2vector *indoption;
	StringInfoData definitionBuf;

	/*
	 * Fetch the pg_index tuple by the Oid of the index
	 */
	ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(ht_idx))
		elog(ERROR, "cache lookup failed for index %u", indexrelid);
	idxrec = (Form_pg_index) GETSTRUCT(ht_idx);

	indrelid = idxrec->indrelid;
	Assert(indexrelid == idxrec->indexrelid);

	/* Must get indcollation, indclass, and indoption the hard way */
	indcollDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
								   Anum_pg_index_indcollation, &isnull);
	Assert(!isnull);
	indcollation = (oidvector *) DatumGetPointer(indcollDatum);

	indclassDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(indclassDatum);

	indoptionDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									 Anum_pg_index_indoption, &isnull);
	Assert(!isnull);
	indoption = (int2vector *) DatumGetPointer(indoptionDatum);

	/*
	 * Fetch the pg_class tuple of the index relation
	 */
	ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(ht_idxrel))
		elog(ERROR, "cache lookup failed for relation %u", indexrelid);
	idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

	/*
	 * Fetch the pg_am tuple of the index' access method
	 */
	ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
	if (!HeapTupleIsValid(ht_am))
		elog(ERROR, "cache lookup failed for access method %u",
			 idxrelrec->relam);
	amrec = (Form_pg_am) GETSTRUCT(ht_am);

	/*
	 * Get the index expressions, if any.  (NOTE: we do not use the relcache
	 * versions of the expressions and predicate, because we want to display
	 * non-const-folded expressions.)
	 */
	if (!heap_attisnull(ht_idx, Anum_pg_index_indexprs, NULL))
	{
		Datum		exprsDatum;
		char	   *exprsString;

		exprsDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									 Anum_pg_index_indexprs, &isnull);
		Assert(!isnull);
		exprsString = TextDatumGetCString(exprsDatum);
		indexprs = (List *) stringToNode(exprsString);
		pfree(exprsString);
	}
	else
		indexprs = NIL;

	indexpr_item = list_head(indexprs);

	context = deparse_context_for(get_rel_name(indrelid), indrelid);

	initStringInfo(&definitionBuf);

	/* Output index AM */
	*index_am = pstrdup(quote_identifier(NameStr(amrec->amname)));

	/* Fetch the index AM's API struct */
	amroutine = GetIndexAmRoutine(amrec->amhandler);

	/*
	 * Output index definition.  Note the outer parens must be supplied by
	 * caller.
	 */
	for (keyno = 0; keyno < idxrec->indnatts; keyno++)
	{
		AttrNumber	attnum = idxrec->indkey.values[keyno];
		int16		opt = indoption->values[keyno];
		Oid			keycoltype;
		Oid			keycolcollation;
		Oid			indcoll;

		appendStringInfoString(&definitionBuf, keyno == 0 ? "" : ", ");

		if (attnum != 0)
		{
			/* Simple index column */
			char	   *attname;
			int32		keycoltypmod;

			attname = get_attname(indrelid, attnum, false);
			appendStringInfoString(&definitionBuf, quote_identifier(attname));
			get_atttypetypmodcoll(indrelid, attnum,
								  &keycoltype, &keycoltypmod,
								  &keycolcollation);
		}
		else
		{
			/* Expressional index */
			Node	   *indexkey;
			char	   *str;

			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexprs, indexpr_item);
			/* Deparse */
			str = deparse_expression(indexkey, context, false, false);

			/* Need parens if it's not a bare function call */
			if (indexkey && IsA(indexkey, FuncExpr) &&
				((FuncExpr *) indexkey)->funcformat == COERCE_EXPLICIT_CALL)
				appendStringInfoString(&definitionBuf, str);
			else
				appendStringInfo(&definitionBuf, "(%s)", str);

			keycoltype = exprType(indexkey);
			keycolcollation = exprCollation(indexkey);
		}

		/* Add collation, even if default */
		indcoll = indcollation->values[keyno];
		if (OidIsValid(indcoll))
			appendStringInfo(&definitionBuf, " COLLATE %s",
							 generate_collation_name((indcoll)));

		/* Add the operator class name, even if default */
		get_opclass_name(indclass->values[keyno], InvalidOid, &definitionBuf);

		/* Add options if relevant */
		if (amroutine->amcanorder)
		{
			/* If it supports sort ordering, report DESC and NULLS opts */
			if (opt & INDOPTION_DESC)
			{
				appendStringInfoString(&definitionBuf, " DESC");
				/* NULLS FIRST is the default in this case */
				if (!(opt & INDOPTION_NULLS_FIRST))
					appendStringInfoString(&definitionBuf, " NULLS LAST");
			}
			else
			{
				if (opt & INDOPTION_NULLS_FIRST)
					appendStringInfoString(&definitionBuf, " NULLS FIRST");
			}
		}

		/* XXX excludeOps thingy was here; do we need anything? */
	}
	*definition = definitionBuf.data;

	/* Output reloptions */
	*reloptions = flatten_reloptions(indexrelid);

	/* Output tablespace */
	{
		Oid			tblspc;

		tblspc = get_rel_tablespace(indexrelid);
		if (OidIsValid(tblspc))
			*tablespace = pstrdup(quote_identifier(get_tablespace_name(tblspc)));
		else
			*tablespace = NULL;
	}

	/* Report index predicate, if any */
	if (!heap_attisnull(ht_idx, Anum_pg_index_indpred, NULL))
	{
		Node	   *node;
		Datum		predDatum;
		char	   *predString;

		/* Convert text string to node tree */
		predDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
									Anum_pg_index_indpred, &isnull);
		Assert(!isnull);
		predString = TextDatumGetCString(predDatum);
		node = (Node *) stringToNode(predString);
		pfree(predString);

		/* Deparse */
		*whereClause =
			deparse_expression(node, context, false, false);
	}
	else
		*whereClause = NULL;

	/* Clean up */
	ReleaseSysCache(ht_idx);
	ReleaseSysCache(ht_idxrel);
	ReleaseSysCache(ht_am);
}

/*
 * Obtain the deparsed default value for the given column of the given table.
 *
 * Caller must have set a correct deparse context.
 */
static char *
RelationGetColumnDefault(Relation rel, AttrNumber attno, List *dpcontext,
						 List **exprs)
{
	Node *defval;
	char *defstr;

	defval = build_column_default(rel, attno);
	defstr = deparse_expression(defval, dpcontext, false, false);

	/* Collect the expression for later replication safety checks */
	if (exprs)
		*exprs = lappend(*exprs, defval);

	return defstr;
}

/*
 * Obtain the deparsed partition bound expression for the given table.
 */
static char *
RelationGetPartitionBound(Oid relid)
{
	Datum		deparsed;
	Datum		boundDatum;
	bool		isnull;
	HeapTuple	tuple;

	tuple = SearchSysCache1(RELOID, relid);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 relid);

	boundDatum = SysCacheGetAttr(RELOID, tuple,
								 Anum_pg_class_relpartbound,
								 &isnull);

	deparsed = DirectFunctionCall2(pg_get_expr,
								   CStringGetTextDatum(TextDatumGetCString(boundDatum)),
								   relid);

	ReleaseSysCache(tuple);

	return TextDatumGetCString(deparsed);
}

/*
 * Return the given object type as a string.
 */
static const char *
stringify_objtype(ObjectType objtype)
{
	switch (objtype)
	{
		case OBJECT_AGGREGATE:
			return "AGGREGATE";
		case OBJECT_CAST:
			return "CAST";
		case OBJECT_COLUMN:
			return "COLUMN";
		case OBJECT_COLLATION:
			return "COLLATION";
		case OBJECT_CONVERSION:
			return "CONVERSION";
		case OBJECT_DATABASE:
			return "DATABASE";
		case OBJECT_DOMAIN:
			return "DOMAIN";
		case OBJECT_EVENT_TRIGGER:
			return "EVENT TRIGGER";
		case OBJECT_EXTENSION:
			return "EXTENSION";
		case OBJECT_FDW:
			return "FOREIGN DATA WRAPPER";
		case OBJECT_FOREIGN_SERVER:
			return "SERVER";
		case OBJECT_FOREIGN_TABLE:
			return "FOREIGN TABLE";
		case OBJECT_FUNCTION:
			return "FUNCTION";
		case OBJECT_INDEX:
			return "INDEX";
		case OBJECT_LANGUAGE:
			return "LANGUAGE";
		case OBJECT_LARGEOBJECT:
			return "LARGE OBJECT";
		case OBJECT_MATVIEW:
			return "MATERIALIZED VIEW";
		case OBJECT_OPCLASS:
			return "OPERATOR CLASS";
		case OBJECT_OPERATOR:
			return "OPERATOR";
		case OBJECT_OPFAMILY:
			return "OPERATOR FAMILY";
		case OBJECT_POLICY:
			return "POLICY";
		case OBJECT_ROLE:
			return "ROLE";
		case OBJECT_RULE:
			return "RULE";
		case OBJECT_SCHEMA:
			return "SCHEMA";
		case OBJECT_SEQUENCE:
			return "SEQUENCE";
		case OBJECT_TABLE:
			return "TABLE";
		case OBJECT_TABLESPACE:
			return "TABLESPACE";
		case OBJECT_TRIGGER:
			return "TRIGGER";
		case OBJECT_TSCONFIGURATION:
			return "TEXT SEARCH CONFIGURATION";
		/*
		case OBJECT_TSCONFIG_MAPPING:
			return "TEXT SEARCH CONFIGURATION MAPPING";
		*/
		case OBJECT_TSDICTIONARY:
			return "TEXT SEARCH DICTIONARY";
		case OBJECT_TSPARSER:
			return "TEXT SEARCH PARSER";
		case OBJECT_TSTEMPLATE:
			return "TEXT SEARCH TEMPLATE";
		case OBJECT_TYPE:
			return "TYPE";
		case OBJECT_USER_MAPPING:
			return "USER MAPPING";
		case OBJECT_VIEW:
			return "VIEW";

		default:
			elog(ERROR, "unsupported object type %d", objtype);
	}
}

/*
 * Given a CollectedCommand, return a JSON representation of it.
 *
 * The command is expanded fully, so that there are no ambiguities even in the
 * face of search_path changes.
 */
Datum
ddl_deparse_to_json(PG_FUNCTION_ARGS)
{
	CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);
	char		   *command;

	command = deparse_utility_command(cmd, true);

	if (command)
		PG_RETURN_TEXT_P(cstring_to_text(command));

	PG_RETURN_NULL();
}

/*
 * Deparse an AlterCollationStmt (ALTER COLLATION)
 *
 * Given a collation OID and the parsetree that created it, return the JSON
 * blob representing the alter command.
 */
static ObjTree *
deparse_AlterCollation(Oid objectId, Node *parsetree)
{
	ObjTree    *stmt;
	HeapTuple	colTup;
	Form_pg_collation colForm;

	colTup = SearchSysCache1(COLLOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(colTup))
		elog(ERROR, "cache lookup failed for collation with OID %u", objectId);
	colForm = (Form_pg_collation) GETSTRUCT(colTup);

	stmt = new_objtree_VA("ALTER COLLATION %{identity}O REFRESH VERSION", 1,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname(colForm->collnamespace,
												   NameStr(colForm->collname)));

	ReleaseSysCache(colTup);

	return stmt;
}

/*
 * Deparse an AlterFunctionStmt (ALTER FUNCTION)
 *
 * Given a function OID and the parsetree that created it, return the JSON
 * blob representing the alter command.
 */
static ObjTree *
deparse_AlterFunction(Oid objectId, Node *parsetree)
{
	AlterFunctionStmt *node = (AlterFunctionStmt *) parsetree;
	ObjTree	   *alterFunc;
	ObjTree	   *sign;
	HeapTuple	procTup;
	Form_pg_proc procForm;
	List	   *params = NIL;
	List	   *elems = NIL;
	ListCell   *cell;
	int			i;

	/* Get the pg_proc tuple */
	procTup = SearchSysCache1(PROCOID, objectId);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failure for function with OID %u",
			 objectId);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	/*
	 * Verbose syntax
	 *
	 * ALTER FUNCTION %{signature}s %{definition: }s
	 */
	alterFunc = new_objtree_VA("ALTER FUNCTION", 0);

	/*
	 * ALTER FUNCTION does not change signature so we can use catalog
	 * to get input type Oids.
	 */
	for (i = 0; i < procForm->pronargs; i++)
	{
		ObjTree	   *tmpobj = new_objtree_VA("%{type}T", 0);

		append_object_object(tmpobj, "type",
							 new_objtree_for_type(procForm->proargtypes.values[i], -1));
		params = lappend(params, new_object_object(tmpobj));
	}

	sign = new_objtree("");

	append_object_object(sign, "%{identity}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 objectId));
	append_array_object(sign, "(%{arguments:, }s)", params);

	append_object_object(alterFunc, "%{signature}s", sign);

	foreach(cell, node->actions)
	{
		DefElem	*defel = (DefElem *) lfirst(cell);
		ObjTree	   *tmpobj = NULL;

		if (strcmp(defel->defname, "volatility") == 0)
		{
			tmpobj = new_objtree_VA(strVal(defel->arg), 0);
		}
		else if (strcmp(defel->defname, "strict") == 0)
		{
			tmpobj = new_objtree_VA(intVal(defel->arg) ?
								 "RETURNS NULL ON NULL INPUT" :
								 "CALLED ON NULL INPUT", 0);
		}
		else if (strcmp(defel->defname, "security") == 0)
		{
			tmpobj = new_objtree_VA(intVal(defel->arg) ?
								 "SECURITY DEFINER" : "SECURITY INVOKER", 0);
		}
		else if (strcmp(defel->defname, "leakproof") == 0)
		{
			tmpobj = new_objtree_VA(intVal(defel->arg) ?
								 "LEAKPROOF" : "NOT LEAKPROOF", 0);
		}
		else if (strcmp(defel->defname, "cost") == 0)
		{
			tmpobj = new_objtree_VA("COST %{cost}n", 1,
								 "cost", ObjTypeFloat,
								 defGetNumeric(defel));
		}
		else if (strcmp(defel->defname, "rows") == 0)
		{
			tmpobj = new_objtree_VA("ROWS", 0);
			if (defGetNumeric(defel) == 0)
				append_bool_object(tmpobj, "present", false);
			else
				append_float_object(tmpobj, "%{rows}n",
									defGetNumeric(defel));
		}
		else if (strcmp(defel->defname, "set") == 0)
		{
			VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
			char *value = ExtractSetVariableArgs(sstmt);

			tmpobj = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
		}
		else if (strcmp(defel->defname, "support") == 0)
		{
			Oid			argtypes[1];

			tmpobj = new_objtree("SUPPORT");

			Assert(procForm->prosupport);

			/*
			 * We should qualify the support function's name if it wouldn't be
			 * resolved by lookup in the current search path.
			 */
			argtypes[0] = INTERNALOID;
			append_string_object(tmpobj, "%{name}s",
								 generate_function_name(procForm->prosupport, 1,
														NIL, argtypes,
														false, NULL,
														EXPR_KIND_NONE));
		}
		else if (strcmp(defel->defname, "parallel") == 0)
		{
			char *fmt = psprintf("PARALLEL %s", strVal(defel->arg));
			tmpobj = new_objtree(fmt);
		}

		elems = lappend(elems, new_object_object(tmpobj));
	}

	append_array_object(alterFunc, "%{definition: }s", elems);

	ReleaseSysCache(procTup);

	return alterFunc;
}

/*
 * deparse an ALTER ... SET SCHEMA command.
 */
static ObjTree *
deparse_AlterObjectSchemaStmt(ObjectAddress address, Node *parsetree,
							  ObjectAddress oldschema)
{
	AlterObjectSchemaStmt *node = (AlterObjectSchemaStmt *) parsetree;
	ObjTree	   *alterStmt;
	char	   *fmt;
	char	   *identity;
	char	   *newschema;
	char	   *oldschname;
	char	   *ident;

	newschema = node->newschema;

	/*
	 * Verbose syntax
	 *
	 * ALTER %s %{identity}s SET SCHEMA %{newschema}I
	 */
	fmt = psprintf("ALTER %s", stringify_objtype(node->objectType));
	alterStmt = new_objtree(fmt);

	/*
	 * Since the command has already taken place from the point of view of
	 * catalogs, getObjectIdentity returns the object name with the already
	 * changed schema.  The output of our deparsing must return the original
	 * schema name however, so we chop the schema name off the identity string
	 * and then prepend the quoted schema name.
	 *
	 * XXX This is pretty clunky. Can we do better?
	 */
	identity = getObjectIdentity(&address, false);
	oldschname = get_namespace_name(oldschema.objectId);
	if (!oldschname)
		elog(ERROR, "cache lookup failed for schema with OID %u",
			 oldschema.objectId);

	ident = psprintf("%s%s", quote_identifier(oldschname),
					 identity + strlen(quote_identifier(newschema)));
	append_string_object(alterStmt, "%{identity}s", ident);

	append_string_object(alterStmt, "SET SCHEMA %{newschema}I", newschema);

	return alterStmt;
}

/*
 * Deparse a GRANT/REVOKE command.
 */
static ObjTree *
deparse_GrantStmt(CollectedCommand *cmd)
{
	InternalGrant *istmt;
	ObjTree	   *grantStmt;
	char	   *fmt;
	char	   *objtype;
	List	   *list;
	ListCell   *cell;
	Oid			classId;
	ObjTree	   *tmp;

	istmt = cmd->d.grant.istmt;

	/*
	 * If there are no objects from "ALL ... IN SCHEMA" to be granted, then
	 * we need not do anything.
	 */
	if (istmt->objects == NIL)
		return NULL;

	switch (istmt->objtype)
	{
		case OBJECT_COLUMN:
		case OBJECT_TABLE:
			objtype = "TABLE";
			classId = RelationRelationId;
			break;
		case OBJECT_SEQUENCE:
			objtype = "SEQUENCE";
			classId = RelationRelationId;
			break;
		case OBJECT_DOMAIN:
			objtype = "DOMAIN";
			classId = TypeRelationId;
			break;
		case OBJECT_FDW:
			objtype = "FOREIGN DATA WRAPPER";
			classId = ForeignDataWrapperRelationId;
			break;
		case OBJECT_FOREIGN_SERVER:
			objtype = "FOREIGN SERVER";
			classId = ForeignServerRelationId;
			break;
		case OBJECT_FUNCTION:
			objtype = "FUNCTION";
			classId = ProcedureRelationId;
			break;
		case OBJECT_PROCEDURE:
			objtype = "PROCEDURE";
			classId = ProcedureRelationId;
			break;
		case OBJECT_ROUTINE:
			objtype = "ROUTINE";
			classId = ProcedureRelationId;
			break;
		case OBJECT_LANGUAGE:
			objtype = "LANGUAGE";
			classId = LanguageRelationId;
			break;
		case OBJECT_LARGEOBJECT:
			objtype = "LARGE OBJECT";
			classId = LargeObjectRelationId;
			break;
		case OBJECT_SCHEMA:
			objtype = "SCHEMA";
			classId = NamespaceRelationId;
			break;
		case OBJECT_TYPE:
			objtype = "TYPE";
			classId = TypeRelationId;
			break;
		case OBJECT_DATABASE:
		case OBJECT_TABLESPACE:
			objtype = "";
			classId = InvalidOid;
			elog(ERROR, "global objects not supported");
			break;
		default:
			elog(ERROR, "invalid OBJECT value %d", istmt->objtype);
	}

	/* GRANT TO or REVOKE FROM */
	if (istmt->is_grant)
		fmt = psprintf("GRANT");
	else
		fmt = psprintf("REVOKE");

	grantStmt = new_objtree_VA(fmt, 0);

	/* build list of privileges to grant/revoke */
	if (istmt->all_privs)
	{
		tmp = new_objtree_VA("ALL PRIVILEGES", 0);
		list = list_make1(new_object_object(tmp));
	}
	else
	{
		list = NIL;

		if (istmt->privileges & ACL_INSERT)
			list = lappend(list, new_string_object("INSERT"));
		if (istmt->privileges & ACL_SELECT)
			list = lappend(list, new_string_object("SELECT"));
		if (istmt->privileges & ACL_UPDATE)
			list = lappend(list, new_string_object("UPDATE"));
		if (istmt->privileges & ACL_DELETE)
			list = lappend(list, new_string_object("DELETE"));
		if (istmt->privileges & ACL_TRUNCATE)
			list = lappend(list, new_string_object("TRUNCATE"));
		if (istmt->privileges & ACL_REFERENCES)
			list = lappend(list, new_string_object("REFERENCES"));
		if (istmt->privileges & ACL_TRIGGER)
			list = lappend(list, new_string_object("TRIGGER"));
		if (istmt->privileges & ACL_EXECUTE)
			list = lappend(list, new_string_object("EXECUTE"));
		if (istmt->privileges & ACL_USAGE)
			list = lappend(list, new_string_object("USAGE"));
		if (istmt->privileges & ACL_CREATE)
			list = lappend(list, new_string_object("CREATE"));
		if (istmt->privileges & ACL_CREATE_TEMP)
			list = lappend(list, new_string_object("TEMPORARY"));
		if (istmt->privileges & ACL_CONNECT)
			list = lappend(list, new_string_object("CONNECT"));

		if (!istmt->is_grant && istmt->grant_option)
			append_string_object(grantStmt, "%{grant_option}s",
								 istmt->grant_option ?  "GRANT OPTION FOR" : "");

		if (istmt->col_privs != NIL)
		{
			ListCell   *ocell;

			foreach(ocell, istmt->col_privs)
			{
				AccessPriv *priv = lfirst(ocell);
				List   *cols = NIL;

				tmp = new_objtree("");
				foreach(cell, priv->cols)
				{
					String *colname = lfirst_node(String, cell);

					cols = lappend(cols,
								   new_string_object(strVal(colname)));
				}
				append_array_object(tmp, "(%{cols:, }s)", cols);

				if (priv->priv_name == NULL)
					append_string_object(grantStmt, "%{priv}s", "ALL PRIVILEGES");
				else
					append_string_object(grantStmt, "%{priv}s", priv->priv_name);

				list = lappend(list, new_object_object(tmp));
			}
		}
	}
	append_array_object(grantStmt, "%{privileges:, }s ON", list);

	append_string_object(grantStmt, "%{objtype}s", objtype);

	/* target objects.  We use object identities here */
	list = NIL;
	foreach(cell, istmt->objects)
	{
		Oid		objid = lfirst_oid(cell);
		ObjectAddress addr;

		addr.classId = classId;
		addr.objectId = objid;
		addr.objectSubId = 0;

		tmp = new_objtree_VA("%{identity}s", 1,
							   "identity", ObjTypeString,
							   getObjectIdentity(&addr, false));

		list = lappend(list, new_object_object(tmp));
	}
	append_array_object(grantStmt, "%{privtarget:, }s", list);

	if (istmt->is_grant)
		append_format_string(grantStmt, "TO");
	else
		append_format_string(grantStmt, "FROM");

	/* list of grantees */
	list = NIL;
	foreach(cell, istmt->grantees)
	{
		Oid		grantee = lfirst_oid(cell);

		tmp = new_objtree_for_role_id(grantee);
		list = lappend(list, new_object_object(tmp));
	}

	append_array_object(grantStmt, "%{grantees:, }s", list);

	/* the wording of the grant option is variable ... */
	if (istmt->is_grant)
		append_string_object(grantStmt, "%{grant_option}s",
							 istmt->grant_option ?  "WITH GRANT OPTION" : "");

	if (istmt->grantor_uid)
	{
		HeapTuple	roltup;
		char	   *rolename;

		roltup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(istmt->grantor_uid));
		if (!HeapTupleIsValid(roltup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("role with OID %u does not exist", istmt->grantor_uid)));

		rolename = NameStr(((Form_pg_authid) GETSTRUCT(roltup))->rolname);
		append_string_object(grantStmt, "GRANTED BY %{rolename}s", rolename);
		ReleaseSysCache(roltup);
	}

	if (!istmt->is_grant)
	{
		if (istmt->behavior == DROP_CASCADE)
			append_string_object(grantStmt, "%{cascade}s", "CASCADE");
		else
			append_string_object(grantStmt, "%{cascade}s", "");
	}

	return grantStmt;
}

/*
 * Deparse an AlterOperatorStmt (ALTER OPERATOR ... SET ...).
 */
static ObjTree *
deparse_AlterOperatorStmt(Oid objectId, Node *parsetree)
{
	HeapTuple   	oprTup;
	AlterOperatorStmt *node = (AlterOperatorStmt *) parsetree;
	ObjTree		   *alterop;
	Form_pg_operator oprForm;
	ListCell	   *cell;
	List		   *list = NIL;

	oprTup = SearchSysCache1(OPEROID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(oprTup))
		elog(ERROR, "cache lookup failed for operator with OID %u", objectId);
	oprForm = (Form_pg_operator) GETSTRUCT(oprTup);

	/*
	 * Verbose syntax
	 *
	 * ALTER OPERATOR %{identity}O (%{left_type}T, %{right_type}T) SET
	 * (%{elems:, }s)
	 */
	alterop = new_objtree_VA("ALTER OPERATOR %{identity}O", 1,
							 "identity", ObjTypeObject,
							 new_objtree_for_qualname(oprForm->oprnamespace,
													  NameStr(oprForm->oprname)));

	/* LEFTARG */
	if (OidIsValid(oprForm->oprleft))
		append_object_object(alterop, "(%{left_type}T",
							 new_objtree_for_type(oprForm->oprleft, -1));
	else
		append_string_object(alterop, "(%{left_type}s", "NONE");

	/* RIGHTARG */
	Assert(OidIsValid(oprForm->oprleft));
	append_object_object(alterop, ", %{right_type}T)",
						 new_objtree_for_type(oprForm->oprright, -1));

	foreach(cell, node->options)
	{
		DefElem *elem = (DefElem *) lfirst(cell);
		ObjTree *tmpobj = NULL;

		if (strcmp(elem->defname, "restrict") == 0)
		{
			tmpobj = new_objtree_VA("RESTRICT=", 1,
								 "clause", ObjTypeString, "restrict");
			if (OidIsValid(oprForm->oprrest))
				append_object_object(tmpobj, "%{procedure}D",
									 new_objtree_for_qualname_id(ProcedureRelationId,
																 oprForm->oprrest));
			else
				append_string_object(tmpobj, "%{procedure}s", "NONE");
		}
		else if (strcmp(elem->defname, "join") == 0)
		{
			tmpobj = new_objtree_VA("JOIN=", 1,
								 "clause", ObjTypeString, "join");
			if (OidIsValid(oprForm->oprjoin))
				append_object_object(tmpobj, "%{procedure}D",
									 new_objtree_for_qualname_id(ProcedureRelationId,
																 oprForm->oprjoin));
			else
				append_string_object(tmpobj, "%{procedure}s", "NONE");
		}

		Assert(tmpobj);
		list = lappend(list, new_object_object(tmpobj));
	}

	append_array_object(alterop, "SET (%{elems:, }s)", list);

	ReleaseSysCache(oprTup);

	return alterop;
}

/*
 * Deparse an ALTER OPERATOR FAMILY ADD/DROP command.
 */
static ObjTree *
deparse_AlterOpFamily(CollectedCommand *cmd)
{
	ObjTree	   *alterOpFam;
	AlterOpFamilyStmt *stmt = (AlterOpFamilyStmt *) cmd->parsetree;
	HeapTuple	ftp;
	Form_pg_opfamily opfForm;
	List	   *list;
	ListCell   *cell;

	ftp = SearchSysCache1(OPFAMILYOID,
						  ObjectIdGetDatum(cmd->d.opfam.address.objectId));
	if (!HeapTupleIsValid(ftp))
		elog(ERROR, "cache lookup failed for operator family %u",
			 cmd->d.opfam.address.objectId);
	opfForm = (Form_pg_opfamily) GETSTRUCT(ftp);

	/*
	 * Verbose syntax
	 *
	 * ALTER OPERATOR FAMILY %{identity}D USING %{amname}I ADD/DROP
	 * %{items:, }s
	 */
	alterOpFam = new_objtree_VA("ALTER OPERATOR FAMILY %{identity}D "
								"USING %{amname}I", 2,
								"identity", ObjTypeObject,
								new_objtree_for_qualname(opfForm->opfnamespace,
														 NameStr(opfForm->opfname)),
								"amname", ObjTypeString, stmt->amname);

	list = NIL;
	foreach(cell, cmd->d.opfam.operators)
	{
		OpFamilyMember *oper = lfirst(cell);
		ObjTree	   *tmpobj;

		/*
		 * Verbose syntax
		 *
		 * OPERATOR %{num}n %{operator}O(%{ltype}T, %{rtype}T) %{purpose}s
		 */
		tmpobj = new_objtree_VA("OPERATOR %{num}n", 1,
							 "num", ObjTypeInteger, oper->number);

		/* Add the operator name; the DROP case doesn't have this */
		if (!stmt->isDrop)
		{
			append_object_object(tmpobj, "%{operator}O",
								 new_objtree_for_qualname_id(OperatorRelationId,
															 oper->object));
		}

		/* Add the types */
		append_object_object(tmpobj, "(%{ltype}T,",
							 new_objtree_for_type(oper->lefttype, -1));
		append_object_object(tmpobj, "%{rtype}T)",
							 new_objtree_for_type(oper->righttype, -1));

		/* Add the FOR SEARCH / FOR ORDER BY clause; not in the DROP case */
		if (!stmt->isDrop)
		{
			if (oper->sortfamily == InvalidOid)
				append_string_object(tmpobj, "%{purpose}s", "FOR SEARCH");
			else
			{
				ObjTree	   *tmpobj2;

				tmpobj2 = new_objtree_VA("FOR ORDER BY", 0);
				append_object_object(tmpobj2, "%{opfamily}D",
									 new_objtree_for_qualname_id(OperatorFamilyRelationId,
																 oper->sortfamily));
				append_object_object(tmpobj, "%{purpose}s", tmpobj2);
			}
		}

		list = lappend(list, new_object_object(tmpobj));
	}

	foreach(cell, cmd->d.opfam.procedures)
	{
		OpFamilyMember *proc = lfirst(cell);
		ObjTree	   *tmpobj;

		tmpobj = new_objtree_VA("FUNCTION %{num}n (%{ltype}T, %{rtype}T)", 3,
							 "num", ObjTypeInteger, proc->number,
							 "ltype", ObjTypeObject,
							 new_objtree_for_type(proc->lefttype, -1),
							 "rtype", ObjTypeObject,
							 new_objtree_for_type(proc->righttype, -1));

		/* Add the function name and arg types; the DROP case doesn't have this */
		if (!stmt->isDrop)
		{
			HeapTuple	procTup;
			Form_pg_proc procForm;
			Oid		   *proargtypes;
			List	   *arglist = NIL;
			int			i;

			procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc->object));
			if (!HeapTupleIsValid(procTup))
				elog(ERROR, "cache lookup failed for procedure %u", proc->object);
			procForm = (Form_pg_proc) GETSTRUCT(procTup);

			proargtypes = procForm->proargtypes.values;
			for (i = 0; i < procForm->pronargs; i++)
			{
				ObjTree	   *arg;

				arg = new_objtree_for_type(proargtypes[i], -1);
				arglist = lappend(arglist, new_object_object(arg));
			}

			append_object_object(tmpobj, "%{function}D",
								 new_objtree_for_qualname(procForm->pronamespace,
														  NameStr(procForm->proname)));

			append_format_string(tmpobj, "(");
			append_array_object(tmpobj, "%{argtypes:, }T", arglist);
			append_format_string(tmpobj, ")");

			ReleaseSysCache(procTup);
		}

		list = lappend(list, new_object_object(tmpobj));
	}

	if (stmt->isDrop)
		append_format_string(alterOpFam, "DROP");
	else
		append_format_string(alterOpFam, "ADD");

	append_array_object(alterOpFam, "%{items:, }s", list);

	ReleaseSysCache(ftp);

	return alterOpFam;
}

/*
 * Deparse a AlterOwnerStmt (ALTER ... OWNER TO ...).
 */
static ObjTree *
deparse_AlterOwnerStmt(ObjectAddress address, Node *parsetree)
{
	AlterOwnerStmt *node = (AlterOwnerStmt *) parsetree;
	ObjTree		   *ownerStmt;
	char		   *fmt;

	fmt = psprintf("ALTER %s %%{identity}s OWNER TO %%{newowner}I",
				   stringify_objtype(node->objectType));

	ownerStmt = new_objtree_VA(fmt, 2,
							   "identity", ObjTypeString,
							   getObjectIdentity(&address, false),
							   "newowner", ObjTypeString,
							   get_rolespec_name(node->newowner));

	return ownerStmt;
}

/*
 * Deparse an AlterSeqStmt.
 *
 * Given a sequence OID and a parsetree that modified it, return an ObjTree
 * representing the alter command.
 */
static ObjTree *
deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree		*alterSeq;
	ObjTree		*tmpobj;
	Relation	 relation;
	Form_pg_sequence_data seqdata;
	List	   *elems = NIL;
	ListCell   *cell;
	Form_pg_sequence seqform;
	Relation	rel;
	HeapTuple	seqtuple;
	AlterSeqStmt *alterSeqStmt = (AlterSeqStmt *) parsetree;

	/*
	 * Sequence for IDENTITY COLUMN output separately(via CREATE TABLE or
	 * ALTER TABLE); return empty here.
	 */
	if (alterSeqStmt->for_identity)
		return NULL;

	seqdata = get_sequence_values(objectId);

	relation = relation_open(objectId, AccessShareLock);
	rel = table_open(SequenceRelationId, RowExclusiveLock);
	seqtuple = SearchSysCacheCopy1(SEQRELID,
								   ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(seqtuple))
		elog(ERROR, "cache lookup failed for sequence %u",
			 objectId);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

	alterSeq = new_objtree("ALTER SEQUENCE");

	tmpobj = new_objtree_for_qualname(relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(alterSeq, "%{identity}D", tmpobj);

	foreach(cell, ((AlterSeqStmt *) parsetree)->options)
	{
		DefElem *elem = (DefElem *) lfirst(cell);
		ObjElem *newelm;

		if (strcmp(elem->defname, "cache") == 0)
			newelm = deparse_Seq_Cache(alterSeq, seqform, false);
		else if (strcmp(elem->defname, "cycle") == 0)
			newelm = deparse_Seq_Cycle(alterSeq, seqform, false);
		else if (strcmp(elem->defname, "increment") == 0)
			newelm = deparse_Seq_IncrementBy(alterSeq, seqform, false);
		else if (strcmp(elem->defname, "minvalue") == 0)
			newelm = deparse_Seq_Minvalue(alterSeq, seqform, false);
		else if (strcmp(elem->defname, "maxvalue") == 0)
			newelm = deparse_Seq_Maxvalue(alterSeq, seqform, false);
		else if (strcmp(elem->defname, "start") == 0)
			newelm = deparse_Seq_Startwith(alterSeq, seqform, false);
		else if (strcmp(elem->defname, "restart") == 0)
			newelm = deparse_Seq_Restart(alterSeq, seqdata);
		else if (strcmp(elem->defname, "owned_by") == 0)
			newelm = deparse_Seq_OwnedBy(alterSeq, objectId, false);
		else
			elog(ERROR, "invalid sequence option %s", elem->defname);

		elems = lappend(elems, newelm);
	}

	append_array_object(alterSeq, "%{definition: }s", elems);

	table_close(rel, RowExclusiveLock);
	relation_close(relation, AccessShareLock);

	return alterSeq;
}

/*
 * Deparse an AlterTypeStmt.
 *
 * Given a type OID and a parsetree that modified it, return an ObjTree
 * representing the alter type.
 */
static ObjTree *
deparse_AlterTypeSetStmt(Oid objectId, Node *cmd)
{
	AlterTypeStmt *alterTypeSetStmt = (AlterTypeStmt *)cmd;
	ObjTree		*alterType;
	ListCell   *pl;
	List	   *elems = NIL;
	HeapTuple   typTup;
	Form_pg_type typForm;

	typTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for type with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	alterType = new_objtree("ALTER TYPE");
	append_object_object(alterType, "%{identity}D SET",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));

	foreach(pl, alterTypeSetStmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		ObjElem *newelm;

		if (strcmp(defel->defname, "storage") == 0)
			newelm = deparse_Type_Storage(alterType, typForm);
		if (strcmp(defel->defname, "receive") == 0)
			newelm = deparse_Type_Receive(alterType, typForm);
		if (strcmp(defel->defname, "send") == 0)
			newelm = deparse_Type_Send(alterType, typForm);
		if (strcmp(defel->defname, "typmod_in") == 0)
			newelm = deparse_Type_Typmod_In(alterType, typForm);
		if (strcmp(defel->defname, "typmod_out") == 0)
			newelm = deparse_Type_Typmod_Out(alterType, typForm);
		if (strcmp(defel->defname, "analyze") == 0)
			newelm = deparse_Type_Analyze(alterType, typForm);
		if (strcmp(defel->defname, "subscript") == 0)
			newelm = deparse_Type_Subscript(alterType, typForm);
		else
			elog(ERROR, "invalid type option %s", defel->defname);

		elems = lappend(elems, newelm);
	}

	append_array_object(alterType, "(%{definition: }s)", elems);

	ReleaseSysCache(typTup);

	return alterType;
}

/*
 * Deparse a CompositeTypeStmt (CREATE TYPE AS)
 *
 * Given a type OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CompositeTypeStmt(Oid objectId, Node *parsetree)
{
	CompositeTypeStmt *node = (CompositeTypeStmt *) parsetree;
	ObjTree	   *composite;
	HeapTuple	typtup;
	Form_pg_type typform;
	Relation	typerel;
	List	   *dpcontext;
	List	   *tableelts = NIL;

	/* Find the pg_type entry and open the corresponding relation */
	typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", objectId);
	typform = (Form_pg_type) GETSTRUCT(typtup);
	typerel = relation_open(typform->typrelid, AccessShareLock);

	dpcontext = deparse_context_for(RelationGetRelationName(typerel),
									RelationGetRelid(typerel));

	composite = new_objtree_VA("CREATE TYPE", 0);
	append_object_object(composite, "%{identity}D",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));

	tableelts = deparse_TableElements(typerel, node->coldeflist, dpcontext,
									  false,		/* not typed */
									  true);		/* composite type */

	append_array_object(composite, "AS (%{columns:, }s)", tableelts);

	table_close(typerel, AccessShareLock);
	ReleaseSysCache(typtup);

	return composite;
}

static ObjTree *
deparse_CreateConversion(Oid objectId, Node *parsetree)
{
	HeapTuple   conTup;
	Relation	convrel;
	Form_pg_conversion conForm;
	ObjTree	   *ccStmt, *tmpObj;

	convrel = table_open(ConversionRelationId, AccessShareLock);
	conTup = get_catalog_object_by_oid(convrel, Anum_pg_conversion_oid, objectId);
	if (!HeapTupleIsValid(conTup))
		elog(ERROR, "cache lookup failed for conversion with OID %u", objectId);
	conForm = (Form_pg_conversion) GETSTRUCT(conTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE %{default}s CONVERSION %{identity}D FOR %{source}L TO %{dest}L
	 * FROM %{function}D
	 */
	ccStmt = new_objtree("CREATE");


	/* Add the DEFAULT clause */
	append_string_object(ccStmt, "default",
						 conForm->condefault ? "DEFAULT" : "");

	tmpObj = new_objtree_for_qualname(conForm->connamespace, NameStr(conForm->conname));
	append_object_object(ccStmt, "CONVERSION %{identity}D", tmpObj);
	append_string_object(ccStmt, "FOR %{source}L", (char *)
						 pg_encoding_to_char(conForm->conforencoding));
	append_string_object(ccStmt, "TO %{dest}L", (char *)
						 pg_encoding_to_char(conForm->contoencoding));
	append_object_object(ccStmt, "FROM %{function}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 conForm->conproc));

	table_close(convrel, AccessShareLock);

	return ccStmt;
}

/*
 * Deparse a CreateEnumStmt (CREATE TYPE AS ENUM)
 *
 * Given a type OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateEnumStmt(Oid objectId, Node *parsetree)
{
	CreateEnumStmt *node = (CreateEnumStmt *) parsetree;
	ObjTree	   *enumtype;
	List	   *values;
	ListCell   *cell;

	enumtype = new_objtree("CREATE TYPE");
	append_object_object(enumtype, "%{identity}D",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));

	values = NIL;
	foreach(cell, node->vals)
	{
		String   *val = lfirst_node(String, cell);

		values = lappend(values, new_string_object(strVal(val)));
	}

	append_array_object(enumtype, "AS ENUM (%{values:, }L)", values);
	return enumtype;
}

/*
 * deparse_CreateExtensionStmt
 *		deparse a CreateExtensionStmt
 *
 * Given an extension OID and the parsetree that created it, return the JSON
 * blob representing the creation command.
 *
 */
static ObjTree *
deparse_CreateExtensionStmt(Oid objectId, Node *parsetree)
{
	CreateExtensionStmt *node = (CreateExtensionStmt *) parsetree;
	Relation    pg_extension;
	HeapTuple   extTup;
	Form_pg_extension extForm;
	ObjTree	   *extStmt;
	ObjTree	   *tmp;
	List	   *list;
	ListCell   *cell;

	/*
	 * Verbose syntax
	 *
	 * CREATE EXTENSION %{if_not_exists}s %{identity}I %{options: }s
	 */

	pg_extension = table_open(ExtensionRelationId, AccessShareLock);
	extTup = get_catalog_object_by_oid(pg_extension, Anum_pg_extension_oid, objectId);
	if (!HeapTupleIsValid(extTup))
		elog(ERROR, "cache lookup failed for extension with OID %u",
			 objectId);
	extForm = (Form_pg_extension) GETSTRUCT(extTup);

	extStmt = new_objtree("CREATE EXTENSION");

	append_string_object(extStmt, "%{if_not_exists}s",
						 node->if_not_exists ? "IF NOT EXISTS" : "");

	append_string_object(extStmt, "%{name}I", node->extname);

	/* List of options */
	list = NIL;
	foreach(cell, node->options)
	{
		DefElem *opt = (DefElem *) lfirst(cell);

		if (strcmp(opt->defname, "schema") == 0)
		{
			/* skip this one; we add one unconditionally below */
			continue;
		}
		else if (strcmp(opt->defname, "new_version") == 0)
		{
			tmp = new_objtree_VA("VERSION %{version}L", 2,
								 "type", ObjTypeString, "version",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else if (strcmp(opt->defname, "old_version") == 0)
		{
			tmp = new_objtree_VA("FROM %{version}L", 2,
								 "type", ObjTypeString, "from",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else
			elog(ERROR, "unsupported option %s", opt->defname);
	}

	/* Add the SCHEMA option */
	tmp = new_objtree_VA("SCHEMA %{schema}I",
						 2, "type", ObjTypeString, "schema",
						 "schema", ObjTypeString,
						 get_namespace_name(extForm->extnamespace));
	list = lappend(list, new_object_object(tmp));

	/* done */
	append_array_object(extStmt, "%{options: }s", list);

	table_close(pg_extension, AccessShareLock);

	return extStmt;
}

/*
 * If a column name is specified, add an element for it; otherwise it's a
 * table-level option.
 */
static ObjTree *
deparse_FdwOptions(List *options, char *colname)
{
	ObjTree	   *tmp;

	if (colname)
		tmp = new_objtree_VA("ALTER COLUMN %{column}I",
							 1, "column", ObjTypeString, colname);
	else
		tmp = new_objtree_VA("OPTIONS", 0);

	if (options != NIL)
	{
		List	   *optout = NIL;
		ListCell   *cell;

		foreach(cell, options)
		{
			DefElem	   *elem;
			ObjTree	   *opt;

			elem = (DefElem *) lfirst(cell);

			switch (elem->defaction)
			{
				case DEFELEM_UNSPEC:
					opt = new_objtree_VA("%{label}I %{value}L", 2,
										 "label", ObjTypeString, elem->defname,
										 "value", ObjTypeString,
										 elem->arg ? defGetString(elem) :
										 defGetBoolean(elem) ? "TRUE" : "FALSE");
					break;
				case DEFELEM_SET:
					opt = new_objtree_VA("SET %{label}I %{value}L", 2,
										 "label", ObjTypeString, elem->defname,
										 "value", ObjTypeString,
										 elem->arg ? defGetString(elem) :
										 defGetBoolean(elem) ? "TRUE" : "FALSE");
					break;
				case DEFELEM_ADD:
					opt = new_objtree_VA("ADD %{label}I %{value}L", 2,
										 "label", ObjTypeString, elem->defname,
										 "value", ObjTypeString,
										 elem->arg ? defGetString(elem) :
										 defGetBoolean(elem) ? "TRUE" : "FALSE");
					break;
				case DEFELEM_DROP:
					opt = new_objtree_VA("DROP %{label}I", 1,
										 "label", ObjTypeString, elem->defname);
					break;
				default:
					elog(ERROR, "invalid def action %d", elem->defaction);
					opt = NULL;
			}

			optout = lappend(optout, new_object_object(opt));
		}

		append_array_object(tmp, "(%{option: ,}s)", optout);
	}
	else
		append_bool_object(tmp, "present", false);

	return tmp;
}

/*
 * deparse_CreateFdwStmt
 *   Deparse a CreateFdwStmt (CREATE FOREIGN DATA WRAPPER)
 *
 * Given a trigger OID and the parsetree that created it,
 * return an ObjTree representing the creation command.
 */
static ObjTree *
deparse_CreateFdwStmt(Oid objectId, Node *parsetree)
{
	CreateFdwStmt *node = (CreateFdwStmt *) parsetree;
	HeapTuple		fdwTup;
	Form_pg_foreign_data_wrapper fdwForm;
	Relation	rel;

	ObjTree	   *createStmt;
	ObjTree	   *tmp;

	rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

	fdwTup = SearchSysCache1(FOREIGNDATAWRAPPEROID,
							 ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(fdwTup))
		elog(ERROR, "cache lookup failed for foreign-data wrapper %u", objectId);

	fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(fdwTup);

	createStmt = new_objtree_VA("CREATE FOREIGN DATA WRAPPER %{identity}I", 1,
								"identity", ObjTypeString, NameStr(fdwForm->fdwname));

	/* add HANDLER clause */
	if (fdwForm->fdwhandler == InvalidOid)
		tmp = new_objtree_VA("NO HANDLER", 0);
	else
	{
		tmp = new_objtree_VA("HANDLER %{procedure}D", 1,
							 "handler", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 fdwForm->fdwhandler));
	}
	append_object_object(createStmt, "%{handler}s", tmp);

	/* add VALIDATOR clause */
	if (fdwForm->fdwvalidator == InvalidOid)
		tmp = new_objtree_VA("NO VALIDATOR", 0);
	else
	{
		tmp = new_objtree_VA("VALIDATOR %{procedure}D", 1,
							 "validator", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 fdwForm->fdwvalidator));
	}
	append_object_object(createStmt, "%{validator}s", tmp);

	/* add an OPTIONS clause, if any */
	append_object_object(createStmt, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(fdwTup);
	table_close(rel, RowExclusiveLock);

	return createStmt;
}

/*
 * deparse_AlterFdwStmt
 *  Deparse an AlterFdwStmt (ALTER FOREIGN DATA WRAPPER)
 *
 * Given a function OID and the parsetree that create it, return the
 * JSON blob representing the alter command.
 */
static ObjTree *
deparse_AlterFdwStmt(Oid objectId, Node *parsetree)
{
	AlterFdwStmt *node = (AlterFdwStmt *) parsetree;
	HeapTuple		fdwTup;
	Form_pg_foreign_data_wrapper fdwForm;
	Relation	rel;
	ObjTree	   *alterStmt;
	List	   *fdw_options = NIL;
	ListCell   *cell;

	rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

	fdwTup = SearchSysCache1(FOREIGNDATAWRAPPEROID,
							 ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(fdwTup))
		elog(ERROR, "cache lookup failed for foreign-data wrapper %u", objectId);

	fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(fdwTup);

	alterStmt = new_objtree_VA("ALTER FOREIGN DATA WRAPPER %{identity}I", 1,
							   "identity", ObjTypeString, NameStr(fdwForm->fdwname));

	/*
	 * Iterate through options, to see what changed, but use catalog as basis
	 * for new values.
	 */
	foreach(cell, node->func_options)
	{
		DefElem	   *elem;
		ObjTree	   *tmp;

		elem = lfirst(cell);

		if (pg_strcasecmp(elem->defname, "handler") == 0)
		{
			/* add HANDLER clause */
			if (fdwForm->fdwhandler == InvalidOid)
				tmp = new_objtree_VA("NO HANDLER", 0);
			else
			{
				tmp = new_objtree_VA("HANDLER %{procedure}D", 1,
									 "procedure",
									 new_objtree_for_qualname_id(ProcedureRelationId,
																 fdwForm->fdwhandler));
			}
			fdw_options = lappend(fdw_options, new_object_object(tmp));
		}
		else if (pg_strcasecmp(elem->defname, "validator") == 0)
		{
			/* add VALIDATOR clause */
			if (fdwForm->fdwvalidator == InvalidOid)
				tmp = new_objtree_VA("NO VALIDATOR", 0);
			else
			{
				tmp = new_objtree_VA("VALIDATOR %{procedure}D", 1,
									 "procedure",
									 new_objtree_for_qualname_id(ProcedureRelationId,
																 fdwForm->fdwvalidator));
			}
			fdw_options = lappend(fdw_options, new_object_object(tmp));
		}
	}

	/* Add HANDLER/VALIDATOR if specified */
	append_array_object(alterStmt, "%{fdw_options: }s", fdw_options);


	/* add an OPTIONS clause, if any */
	append_object_object(alterStmt, "%{generic_options}D",
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(fdwTup);
	table_close(rel, RowExclusiveLock);

	return alterStmt;
}

/*
 * Deparse a CREATE TYPE AS RANGE statement
 *
 * Given a type OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateRangeStmt(Oid objectId, Node *parsetree)
{
	ObjTree	   *range;
	ObjTree	   *tmp;
	List	   *definition = NIL;
	Relation	pg_range;
	HeapTuple	rangeTup;
	Form_pg_range rangeForm;
	ScanKeyData key[1];
	SysScanDesc scan;

	pg_range = table_open(RangeRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_range_rngtypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(pg_range, RangeTypidIndexId, true,
							  NULL, 1, key);

	rangeTup = systable_getnext(scan);
	if (!HeapTupleIsValid(rangeTup))
		elog(ERROR, "cache lookup failed for range with type oid %u",
			 objectId);

	rangeForm = (Form_pg_range) GETSTRUCT(rangeTup);

	range = new_objtree_VA("CREATE TYPE", 0);
	tmp = new_objtree_for_qualname_id(TypeRelationId, objectId);
	append_object_object(range, "%{identity}D AS RANGE", tmp);

	/* SUBTYPE */
	tmp = new_objtree_for_qualname_id(TypeRelationId,
									  rangeForm->rngsubtype);
	tmp = new_objtree_VA("SUBTYPE = %{type}D",
						 2,
						 "clause", ObjTypeString, "subtype",
						 "type", ObjTypeObject, tmp);
	definition = lappend(definition, new_object_object(tmp));

	/* SUBTYPE_OPCLASS */
	if (OidIsValid(rangeForm->rngsubopc))
	{
		tmp = new_objtree_for_qualname_id(OperatorClassRelationId,
										  rangeForm->rngsubopc);
		tmp = new_objtree_VA("SUBTYPE_OPCLASS = %{opclass}D",
							 2,
							 "clause", ObjTypeString, "opclass",
							 "opclass", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	/* COLLATION */
	if (OidIsValid(rangeForm->rngcollation))
	{
		tmp = new_objtree_for_qualname_id(CollationRelationId,
										  rangeForm->rngcollation);
		tmp = new_objtree_VA("COLLATION = %{collation}D",
							 2,
							 "clause", ObjTypeString, "collation",
							 "collation", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	/* CANONICAL */
	if (OidIsValid(rangeForm->rngcanonical))
	{
		tmp = new_objtree_for_qualname_id(ProcedureRelationId,
										  rangeForm->rngcanonical);
		tmp = new_objtree_VA("CANONICAL = %{canonical}D",
							 2,
							 "clause", ObjTypeString, "canonical",
							 "canonical", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	/* SUBTYPE_DIFF */
	if (OidIsValid(rangeForm->rngsubdiff))
	{
		tmp = new_objtree_for_qualname_id(ProcedureRelationId,
										  rangeForm->rngsubdiff);
		tmp = new_objtree_VA("SUBTYPE_DIFF = %{subtype_diff}D",
							 2,
							 "clause", ObjTypeString, "subtype_diff",
							 "subtype_diff", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	append_array_object(range, "(%{definition:, }s)", definition);

	systable_endscan(scan);
	table_close(pg_range, RowExclusiveLock);

	return range;
}

/*
 * Deparse an AlterEnumStmt.
 *
 * Given a type OID and a parsetree that modified it, return an ObjTree
 * representing the alter type.
 */
static ObjTree *
deparse_AlterEnumStmt(Oid objectId, Node *parsetree)
{
	AlterEnumStmt *node = (AlterEnumStmt *) parsetree;
	ObjTree	   *alterEnum;

	alterEnum =	new_objtree_VA("ALTER TYPE", 0);
	append_object_object(alterEnum, "%{identity}D",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));

	if (!node->oldVal)
	{
		append_format_string(alterEnum, "ADD VALUE");
		append_string_object(alterEnum, "%{if_not_exists}s",
							node->skipIfNewValExists ? "IF NOT EXISTS" : "");

		append_string_object(alterEnum, "%{value}L", node->newVal);

		if (node->newValNeighbor)
		{
			append_string_object(alterEnum, "%{after_or_before}s",
								node->newValIsAfter ? "AFTER" : "BEFORE");
			append_string_object(alterEnum, "%{neighbour}L", node->newValNeighbor);
		}
	}
	else
	{
		append_string_object(alterEnum, "RENAME VALUE %{value}L TO",
							 node->oldVal);
		append_string_object(alterEnum, "%{newvalue}L",
							 node->newVal);
	}

	return alterEnum;
}

/*
 * Deparse ALTER EXTENSION .. UPDATE TO VERSION
 */
static ObjTree *
deparse_AlterExtensionStmt(Oid objectId, Node *parsetree)
{
	AlterExtensionStmt *node = (AlterExtensionStmt *) parsetree;
	Relation    pg_extension;
	HeapTuple   extTup;
	Form_pg_extension extForm;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list = NIL;
	ListCell   *cell;

	pg_extension = table_open(ExtensionRelationId, AccessShareLock);
	extTup = get_catalog_object_by_oid(pg_extension, Anum_pg_extension_oid,  objectId);
	if (!HeapTupleIsValid(extTup))
		elog(ERROR, "cache lookup failed for extension with OID %u",
			 objectId);
	extForm = (Form_pg_extension) GETSTRUCT(extTup);

	stmt = new_objtree_VA("ALTER EXTENSION %{identity}D UPDATE", 1,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname(extForm->extnamespace,
												   NameStr(extForm->extname)));

	foreach(cell, node->options)
	{
		DefElem *opt = (DefElem *) lfirst(cell);

		if (strcmp(opt->defname, "new_version") == 0)
		{
			tmp = new_objtree_VA("TO %{version}L", 2,
								 "type", ObjTypeString, "version",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else
			elog(ERROR, "unsupported option %s", opt->defname);
	}

	append_array_object(stmt, "%{options: }s", list);

	table_close(pg_extension, AccessShareLock);

	return stmt;
}

/*
 * ALTER EXTENSION ext ADD/DROP object
 */
static ObjTree *
deparse_AlterExtensionContentsStmt(Oid objectId, Node *parsetree,
								   ObjectAddress objectAddress)
{
	AlterExtensionContentsStmt *node = (AlterExtensionContentsStmt *) parsetree;
	ObjTree	   *stmt;
	char	   *fmt;

	Assert(node->action == +1 || node->action == -1);

	fmt = psprintf("ALTER EXTENSION %%{extidentity}I %s %s %%{objidentity}s",
				   node->action == +1 ? "ADD" : "DROP",
				   stringify_objtype(node->objtype));

	stmt = new_objtree_VA(fmt, 2, "extidentity", ObjTypeString,
						  node->extname,
						  "objidentity", ObjTypeString,
						  getObjectIdentity(&objectAddress, false));

	return stmt;
}
/*
 * Deparse an CreateCastStmt.
 *
 * Given a sequence OID and a parsetree that modified it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateCastStmt(Oid objectId, Node *parsetree)
{
	CreateCastStmt *node = (CreateCastStmt *) parsetree;
	Relation	castrel;
	HeapTuple	castTup;
	Form_pg_cast castForm;
	ObjTree	   *createCast;
	char	   *context;

	castrel = table_open(CastRelationId, AccessShareLock);
	castTup = get_catalog_object_by_oid(castrel, Anum_pg_cast_oid, objectId);
	if (!HeapTupleIsValid(castTup))
		elog(ERROR, "cache lookup failed for cast with OID %u", objectId);
	castForm = (Form_pg_cast) GETSTRUCT(castTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE CAST (%{sourcetype}T AS %{targettype}T) %{mechanism}s %{context}s
	 */
	createCast = new_objtree_VA("CREATE CAST (%{sourcetype}T AS %{targettype}T)",
								2, "sourcetype", ObjTypeObject,
								new_objtree_for_type(castForm->castsource, -1),
								"targettype", ObjTypeObject,
								new_objtree_for_type(castForm->casttarget, -1));

	if (node->inout)
		append_string_object(createCast, "%{mechanism}s", "WITH INOUT");
	else if (node->func == NULL)
		append_string_object(createCast, "%{mechanism}s", "WITHOUT FUNCTION");
	else
	{
		ObjTree	   *tmpobj;
		StringInfoData func;
		HeapTuple	funcTup;
		Form_pg_proc funcForm;
		int			i;

		funcTup = SearchSysCache1(PROCOID, castForm->castfunc);
		funcForm = (Form_pg_proc) GETSTRUCT(funcTup);

		initStringInfo(&func);
		appendStringInfo(&func, "%s(",
						quote_qualified_identifier(get_namespace_name(funcForm->pronamespace),
												   NameStr(funcForm->proname)));
		for (i = 0; i < funcForm->pronargs; i++)
			appendStringInfoString(&func,
								   format_type_be_qualified(funcForm->proargtypes.values[i]));
		appendStringInfoChar(&func, ')');

		tmpobj = new_objtree_VA("WITH FUNCTION %{castfunction}s", 1,
							 "castfunction", ObjTypeString, func.data);
		append_object_object(createCast, "%{mechanism}s", tmpobj);

		ReleaseSysCache(funcTup);
	}

	switch (node->context)
	{
		case COERCION_IMPLICIT:
			context = "AS IMPLICIT";
			break;
		case COERCION_ASSIGNMENT:
			context = "AS ASSIGNMENT";
			break;
		case COERCION_EXPLICIT:
			context = "";
			break;
		default:
			elog(ERROR, "invalid coercion code %c", node->context);
			return NULL;	/* keep compiler quiet */
	}
	append_string_object(createCast, "%{context}s", context);

	table_close(castrel, AccessShareLock);

	return createCast;
}

/*
 * Deparse all the collected subcommands and return an ObjTree representing the
 * alter command.
 */
static ObjTree *
deparse_AlterTableStmt(CollectedCommand *cmd)
{
	ObjTree	   *alterTableStmt;
	ObjTree	   *tmpobj;
	ObjTree	   *tmpobj2;
	List	   *dpcontext;
	Relation	rel;
	List	   *subcmds = NIL;
	ListCell   *cell;
	char	   *fmtstr;
	const char *reltype;
	bool		istype = false;
	List	   *exprs = NIL;

	Assert(cmd->type == SCT_AlterTable);

	rel = relation_open(cmd->d.alterTable.objectId, AccessShareLock);
	dpcontext = deparse_context_for(RelationGetRelationName(rel),
									cmd->d.alterTable.objectId);

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_PARTITIONED_TABLE:
			reltype = "TABLE";
			break;
		case RELKIND_INDEX:
		case RELKIND_PARTITIONED_INDEX:
			reltype = "INDEX";
			break;
		case RELKIND_VIEW:
			reltype = "VIEW";
			break;
		case RELKIND_COMPOSITE_TYPE:
			reltype = "TYPE";
			istype = true;
			break;
		case RELKIND_FOREIGN_TABLE:
			reltype = "FOREIGN TABLE";
			break;

		/* TODO support for partitioned table */

		default:
			elog(ERROR, "unexpected relkind %d", rel->rd_rel->relkind);
	}

	/*
	 * Verbose syntax
	 *
	 * ALTER reltype %{identity}D %{subcmds:, }s
	 */
	fmtstr = psprintf("ALTER %s", reltype);
	alterTableStmt = new_objtree(fmtstr);

	tmpobj = new_objtree_for_qualname(rel->rd_rel->relnamespace,
								   RelationGetRelationName(rel));
	append_object_object(alterTableStmt, "%{identity}D", tmpobj);

	foreach(cell, cmd->d.alterTable.subcmds)
	{
		CollectedATSubcmd *sub = (CollectedATSubcmd *) lfirst(cell);
		AlterTableCmd	*subcmd = (AlterTableCmd *) sub->parsetree;
		ObjTree	   *tree;

		Assert(IsA(subcmd, AlterTableCmd));

		switch (subcmd->subtype)
		{
			case AT_AddColumn:
			case AT_AddColumnRecurse:
				/* XXX need to set the "recurse" bit somewhere? */
				Assert(IsA(subcmd->def, ColumnDef));
				tree = deparse_ColumnDef(rel, dpcontext, false,
										 (ColumnDef *) subcmd->def, true, &exprs);
				fmtstr = psprintf("ADD %s %%{definition}s",
								  istype ? "ATTRIBUTE" : "COLUMN");
				tmpobj = new_objtree_VA(fmtstr, 2,
									 "type", ObjTypeString, "add column",
									 "definition", ObjTypeObject, tree);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_AddIndexConstraint:
				{
					IndexStmt  *istmt;
					Relation	idx;
					Oid			constrOid = sub->address.objectId;

					Assert(IsA(subcmd->def, IndexStmt));
					istmt = (IndexStmt *) subcmd->def;

					Assert(istmt->isconstraint && istmt->unique);

					idx = relation_open(istmt->indexOid, AccessShareLock);

					/*
					 * Verbose syntax
					 *
					 * ADD CONSTRAINT %{name}I %{constraint_type}s USING INDEX
					 * %index_name}I %{deferrable}s %{init_deferred}s
					 */
					tmpobj = new_objtree_VA("ADD CONSTRAINT %{name}I", 2,
										 "type", ObjTypeString, "add constraint using index",
										 "name", ObjTypeString, get_constraint_name(constrOid));

					append_string_object(tmpobj, "%{constraint_type}s", istmt->deferrable ?
										 "DEFERRABLE" : "NOT DEFERRABLE");
					append_string_object(tmpobj, "USING INDEX %{index_name}I",
										 RelationGetRelationName(idx));
					append_string_object(tmpobj, "%{deferrable}s", istmt->deferrable ?
										 "DEFERRABLE" : "NOT DEFERRABLE");
					append_string_object(tmpobj, "%{init_deferred}s", istmt->initdeferred ?
										 "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE");

					subcmds = lappend(subcmds, new_object_object(tmpobj));

					relation_close(idx, AccessShareLock);
				}
				break;

			case AT_ReAddIndex:
			case AT_ReAddConstraint:
			case AT_ReAddComment:
			case AT_ReplaceRelOptions:
				/* Subtypes used for internal operations; nothing to do here */
				break;

			case AT_AddColumnToView:
				/* CREATE OR REPLACE VIEW -- nothing to do here */
				break;

			case AT_ColumnDefault:
				if (subcmd->def == NULL)
				{
					tmpobj = new_objtree_VA("ALTER COLUMN %{column}I DROP DEFAULT", 2,
										 "type", ObjTypeString, "drop default",
										 "column", ObjTypeString, subcmd->name);
				}
				else
				{
					List	   *dpcontext_rel;
					HeapTuple	attrtup;
					AttrNumber	attno;

					tmpobj = new_objtree_VA("ALTER COLUMN %{column}I SET DEFAULT", 2,
										 "type", ObjTypeString, "set default",
										 "column", ObjTypeString, subcmd->name);

					dpcontext_rel = deparse_context_for(RelationGetRelationName(rel),
													RelationGetRelid(rel));
					attrtup = SearchSysCacheAttName(RelationGetRelid(rel), subcmd->name);
					attno = ((Form_pg_attribute) GETSTRUCT(attrtup))->attnum;
					append_string_object(tmpobj, "%{definition}s",
										 RelationGetColumnDefault(rel, attno,
																  dpcontext_rel,
																  NULL));
					ReleaseSysCache(attrtup);
				}

				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropNotNull:
				tmpobj = new_objtree_VA("ALTER COLUMN %{column}I DROP NOT NULL", 2,
									 "type", ObjTypeString, "drop not null",
									 "column", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_SetNotNull:
				tmpobj = new_objtree_VA("ALTER COLUMN %{column}I SET NOT NULL", 2,
									 "type", ObjTypeString, "set not null",
									 "column", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropExpression:
				tmpobj = new_objtree_VA("ALTER COLUMN %{column}I DROP EXPRESSION", 2,
									 "type", ObjTypeString, "drop expression",
									 "column", ObjTypeString, subcmd->name);
				append_string_object(tmpobj, "%{if_not_exists}s",
									 subcmd->missing_ok ? "IF EXISTS": "");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_SetStatistics:
				{
					Assert(IsA(subcmd->def, Integer));
					if (subcmd->name)
						tmpobj = new_objtree_VA("ALTER COLUMN %{column}I SET STATISTICS %{statistics}n",
											 3,
											 "type", ObjTypeString, "set statistics",
											 "column", ObjTypeString, subcmd->name,
											 "statistics", ObjTypeInteger,
											 intVal((Integer *) subcmd->def));
					else
						tmpobj = new_objtree_VA("ALTER COLUMN %{column}n SET STATISTICS %{statistics}n",
											 3,
											 "type", ObjTypeString, "set statistics",
											 "column", ObjTypeInteger, subcmd->num,
											 "statistics", ObjTypeInteger,
											 intVal((Integer *) subcmd->def));
					subcmds = lappend(subcmds, new_object_object(tmpobj));
				}
				break;

			case AT_SetOptions:
			case AT_ResetOptions:
				subcmds = lappend(subcmds, new_object_object(
									  deparse_ColumnSetOptions(subcmd)));
				break;

			case AT_SetStorage:
				Assert(IsA(subcmd->def, String));
				tmpobj = new_objtree_VA("ALTER COLUMN %{column}I SET STORAGE %{storage}s", 3,
									 "type", ObjTypeString, "set storage",
									 "column", ObjTypeString, subcmd->name,
									 "storage", ObjTypeString,
									 strVal((String *) subcmd->def));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_SetCompression:
				Assert(IsA(subcmd->def, String));
				tmpobj = new_objtree_VA("ALTER COLUMN %{column}I SET COMPRESSION %{compression_method}s",
									 3,
									 "type", ObjTypeString, "set compression",
									 "column", ObjTypeString, subcmd->name,
									 "compression_method", ObjTypeString,
									 strVal((String *) subcmd->def));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropColumnRecurse:
			case AT_DropColumn:
				fmtstr = psprintf("DROP %s %%{column}I",
								  istype ? "ATTRIBUTE" : "COLUMN");
				tmpobj = new_objtree_VA(fmtstr, 2,
									 "type", ObjTypeString, "drop column",
									 "column", ObjTypeString, subcmd->name);
				tmpobj2 = new_objtree_VA("CASCADE", 1,
									  "present", ObjTypeBool, subcmd->behavior);
				append_object_object(tmpobj, "%{cascade}s", tmpobj2);

				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_AddIndex:
				{
					Oid			idxOid = sub->address.objectId;
					IndexStmt  *istmt;
					Relation	idx;
					const char *idxname;
					Oid			constrOid;

					Assert(IsA(subcmd->def, IndexStmt));
					istmt = (IndexStmt *) subcmd->def;

					if (!istmt->isconstraint)
						break;

					idx = relation_open(idxOid, AccessShareLock);
					idxname = RelationGetRelationName(idx);

					constrOid = get_relation_constraint_oid(
						cmd->d.alterTable.objectId, idxname, false);

					tmpobj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
										 "type", ObjTypeString, "add constraint",
										 "name", ObjTypeString, idxname,
										 "definition", ObjTypeString,
										 pg_get_constraintdef_command_simple(constrOid));
					subcmds = lappend(subcmds, new_object_object(tmpobj));

					relation_close(idx, AccessShareLock);
				}
				break;

			case AT_AddConstraint:
			case AT_AddConstraintRecurse:
				{
					/* XXX need to set the "recurse" bit somewhere? */
					Oid			constrOid = sub->address.objectId;
					bool		isnull;
					HeapTuple	tup;
					Datum		val;
					Constraint *constr;

					Assert(IsA(subcmd->def, Constraint));
					constr =  castNode(Constraint, subcmd->def);

					if (!constr->skip_validation)
					{
						tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constrOid));

						if (HeapTupleIsValid(tup))
						{
							char	   *conbin;

							/* Fetch constraint expression in parsetree form */
							val = SysCacheGetAttr(CONSTROID, tup,
												  Anum_pg_constraint_conbin, &isnull);

							if (!isnull)
							{
								conbin = TextDatumGetCString(val);
								exprs = lappend(exprs, stringToNode(conbin));
							}

							ReleaseSysCache(tup);
						}
					}

					tmpobj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
										 "type", ObjTypeString, "add constraint",
										 "name", ObjTypeString, get_constraint_name(constrOid),
										 "definition", ObjTypeString,
										 pg_get_constraintdef_command_simple(constrOid));
					subcmds = lappend(subcmds, new_object_object(tmpobj));
				}
				break;

			case AT_AlterConstraint:
				{
					Oid		constrOid = sub->address.objectId;
					Constraint *c = (Constraint *) subcmd->def;

					/* If no constraint was altered, silently skip it */
					if (!OidIsValid(constrOid))
						break;

					Assert(IsA(c, Constraint));
					tmpobj = new_objtree_VA("ALTER CONSTRAINT %{name}I %{deferrable}s %{init_deferred}s",
										 4,
										 "type", ObjTypeString, "alter constraint",
										 "name", ObjTypeString, get_constraint_name(constrOid),
										 "deferrable", ObjTypeString,
										 c->deferrable ? "DEFERRABLE" : "NOT DEFERRABLE",
										 "init_deferred", ObjTypeString,
										 c->initdeferred ? "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE");

					subcmds = lappend(subcmds, new_object_object(tmpobj));
				}
				break;

			case AT_ValidateConstraintRecurse:
			case AT_ValidateConstraint:
				tmpobj = new_objtree_VA("VALIDATE CONSTRAINT %{constraint}I", 2,
									 "type", ObjTypeString, "validate constraint",
									 "constraint", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropConstraintRecurse:
			case AT_DropConstraint:
				tmpobj = new_objtree_VA("DROP CONSTRAINT %{constraint}I", 2,
									 "type", ObjTypeString, "drop constraint",
									 "constraint", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_AlterColumnType:
				{
					TupleDesc tupdesc = RelationGetDescr(rel);
					Form_pg_attribute att;
					ColumnDef	   *def;

					att = &(tupdesc->attrs[sub->address.objectSubId - 1]);
					def = (ColumnDef *) subcmd->def;
					Assert(IsA(def, ColumnDef));

					/*
					 * Verbose syntax
					 *
					 * Composite types: ALTER reltype %{column}I SET DATA TYPE
					 * %{datatype}T %{collation}s ATTRIBUTE %{cascade}s
					 *
					 * Normal types: ALTER reltype %{column}I SET DATA TYPE
					 * %{datatype}T %{collation}s COLUMN %{using}s
					 */
					fmtstr = psprintf("ALTER %s %%{column}I SET DATA TYPE",
									  istype ? "ATTRIBUTE" : "COLUMN");

					tmpobj = new_objtree_VA(fmtstr, 2,
										 "type", ObjTypeString, "alter column type",
										 "column", ObjTypeString, subcmd->name);
					/* Add the TYPE clause */
					append_object_object(tmpobj, "%{datatype}T",
										 new_objtree_for_type(att->atttypid,
															  att->atttypmod));

					/* Add a COLLATE clause, if needed */
					tmpobj2 = new_objtree("COLLATE");
					if (OidIsValid(att->attcollation))
					{
						ObjTree *collname;

						collname = new_objtree_for_qualname_id(CollationRelationId,
															   att->attcollation);
						append_object_object(tmpobj2, "%{name}D", collname);
					}
					else
						append_bool_object(tmpobj2, "present", false);
					append_object_object(tmpobj, "%{collation}s", tmpobj2);

					/* If not a composite type, add the USING clause */
					if (!istype)
					{
						/*
						 * If there's a USING clause, transformAlterTableStmt
						 * ran it through transformExpr and stored the
						 * resulting node in cooked_default, which we can use
						 * here.
						 */
						tmpobj2 = new_objtree("USING");
						if (def->raw_default)
						{
							Datum	deparsed;
							char   *defexpr;

							exprs = lappend(exprs, def->cooked_default);
							defexpr = nodeToString(def->cooked_default);
							deparsed = DirectFunctionCall2(pg_get_expr,
														   CStringGetTextDatum(defexpr),
														   RelationGetRelid(rel));
							append_string_object(tmpobj2, "%{expression}s",
												 TextDatumGetCString(deparsed));
						}
						else
							append_bool_object(tmpobj2, "present", false);
						append_object_object(tmpobj, "%{using}s", tmpobj2);
					}

					/* If it's a composite type, add the CASCADE clause */
					if (istype)
					{
						tmpobj2 = new_objtree("CASCADE");
						if (subcmd->behavior != DROP_CASCADE)
							append_bool_object(tmpobj2, "present", false);
						append_object_object(tmpobj, "%{cascade}s", tmpobj2);
					}

					subcmds = lappend(subcmds, new_object_object(tmpobj));
				}
				break;

#ifdef TODOLIST
			case AT_AlterColumnGenericOptions:
				tmpobj = deparse_FdwOptions((List *) subcmd->def,
										 subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
#endif
			case AT_ChangeOwner:
				tmpobj = new_objtree_VA("OWNER TO %{owner}I", 2,
									 "type", ObjTypeString, "change owner",
									 "owner",  ObjTypeString,
									 get_rolespec_name(subcmd->newowner));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_ClusterOn:
				tmpobj = new_objtree_VA("CLUSTER ON %{index}I", 2,
									 "type", ObjTypeString, "cluster on",
									 "index", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropCluster:
				tmpobj = new_objtree_VA("SET WITHOUT CLUSTER", 1,
									 "type", ObjTypeString, "set without cluster");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_SetLogged:
				tmpobj = new_objtree_VA("SET LOGGED", 1,
									 "type", ObjTypeString, "set logged");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_SetUnLogged:
				tmpobj = new_objtree_VA("SET UNLOGGED", 1,
									 "type", ObjTypeString, "set unlogged");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropOids:
				tmpobj = new_objtree_VA("SET WITHOUT OIDS", 1,
									 "type", ObjTypeString, "set without oids");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
			case AT_SetAccessMethod:
				tmpobj = new_objtree_VA("SET ACCESS METHOD %{access_method}I", 2,
									 "type", ObjTypeString, "set access method",
									 "access_method", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
			case AT_SetTableSpace:
				tmpobj = new_objtree_VA("SET TABLESPACE %{tablespace}I", 2,
									 "type", ObjTypeString, "set tablespace",
									 "tablespace", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_SetRelOptions:
			case AT_ResetRelOptions:
				subcmds = lappend(subcmds, new_object_object(
									  deparse_RelSetOptions(subcmd)));
				break;

			case AT_EnableTrig:
				tmpobj = new_objtree_VA("ENABLE TRIGGER %{trigger}I", 2,
									 "type", ObjTypeString, "enable trigger",
									 "trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableAlwaysTrig:
				tmpobj = new_objtree_VA("ENABLE ALWAYS TRIGGER %{trigger}I", 2,
									 "type", ObjTypeString, "enable always trigger",
									 "trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableReplicaTrig:
				tmpobj = new_objtree_VA("ENABLE REPLICA TRIGGER %{trigger}I", 2,
									 "type", ObjTypeString, "enable replica trigger",
									 "trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DisableTrig:
				tmpobj = new_objtree_VA("DISABLE TRIGGER %{trigger}I", 2,
									 "type", ObjTypeString, "disable trigger",
									 "trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableTrigAll:
				tmpobj = new_objtree_VA("ENABLE TRIGGER ALL", 1,
									 "type", ObjTypeString, "enable trigger all");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DisableTrigAll:
				tmpobj = new_objtree_VA("DISABLE TRIGGER ALL", 1,
									 "type", ObjTypeString, "disable trigger all");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableTrigUser:
				tmpobj = new_objtree_VA("ENABLE TRIGGER USER", 1,
									 "type", ObjTypeString, "enable trigger user");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DisableTrigUser:
				tmpobj = new_objtree_VA("DISABLE TRIGGER USER", 1,
									 "type", ObjTypeString, "disable trigger user");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableRule:
				tmpobj = new_objtree_VA("ENABLE RULE %{rule}I", 2,
									 "type", ObjTypeString, "enable rule",
									 "rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableAlwaysRule:
				tmpobj = new_objtree_VA("ENABLE ALWAYS RULE %{rule}I", 2,
									 "type", ObjTypeString, "enable always rule",
									 "rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableReplicaRule:
				tmpobj = new_objtree_VA("ENABLE REPLICA RULE %{rule}I", 2,
									 "type", ObjTypeString, "enable replica rule",
									 "rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DisableRule:
				tmpobj = new_objtree_VA("DISABLE RULE %{rule}I", 2,
									 "type", ObjTypeString, "disable rule",
									 "rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_AddInherit:
				tmpobj = new_objtree_VA("INHERIT %{parent}D", 2,
									 "type", ObjTypeString, "inherit",
									 "parent", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropInherit:
				tmpobj = new_objtree_VA("NO INHERIT %{parent}D", 2,
									 "type", ObjTypeString, "drop inherit",
									 "parent", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_AddOf:
				tmpobj = new_objtree_VA("OF %{type_of}T", 2,
									 "type", ObjTypeString, "add of",
									 "type_of", ObjTypeObject,
									 new_objtree_for_type(sub->address.objectId, -1));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DropOf:
				tmpobj = new_objtree_VA("NOT OF", 1,
									 "type", ObjTypeString, "not of");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_ReplicaIdentity:
				tmpobj = new_objtree_VA("REPLICA IDENTITY", 1,
									 "type", ObjTypeString, "replica identity");
				switch (((ReplicaIdentityStmt *) subcmd->def)->identity_type)
				{
					case REPLICA_IDENTITY_DEFAULT:
						append_string_object(tmpobj, "%{ident}s", "DEFAULT");
						break;
					case REPLICA_IDENTITY_FULL:
						append_string_object(tmpobj, "%{ident}s", "FULL");
						break;
					case REPLICA_IDENTITY_NOTHING:
						append_string_object(tmpobj, "%{ident}s", "NOTHING");
						break;
					case REPLICA_IDENTITY_INDEX:
						tmpobj2 = new_objtree_VA("USING INDEX %{index}I", 1,
											  "index", ObjTypeString,
											  ((ReplicaIdentityStmt *) subcmd->def)->name);
						append_object_object(tmpobj, "%{ident}s", tmpobj2);
						break;
				}
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_EnableRowSecurity:
				tmpobj = new_objtree_VA("ENABLE ROW LEVEL SECURITY", 1,
									 "type", ObjTypeString, "enable row security");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;

			case AT_DisableRowSecurity:
				tmpobj = new_objtree_VA("DISABLE ROW LEVEL SECURITY", 1,
									 "type", ObjTypeString, "disable row security");
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
#ifdef TODOLIST
			case AT_GenericOptions:
				tmpobj = deparse_FdwOptions((List *) subcmd->def, NULL);
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
#endif
			case AT_AttachPartition:
				tmpobj = new_objtree_VA("ATTACH PARTITION %{partition_identity}D", 2,
									 "type", ObjTypeString, "attach partition",
									 "partition_identity", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 sub->address.objectId));

				if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
					append_string_object(tmpobj, "%{partition_bound}s",
										 RelationGetPartitionBound(sub->address.objectId));

				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
			case AT_DetachPartition:
				tmpobj = new_objtree_VA("DETACH PARTITION %{partition_identity}D", 2,
									 "type", ObjTypeString, "detach partition",
									 "partition_identity", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
			case AT_DetachPartitionFinalize:
				tmpobj = new_objtree_VA("DETACH PARTITION %{partition_identity}D FINALIZE", 2,
									 "type", ObjTypeString, "detach partition finalize",
									 "partition_identity", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
			case AT_AddIdentity:
				{
					AttrNumber	attnum;
					Oid			seq_relid;
					ObjTree	*seqdef;
					ColumnDef  *coldef = (ColumnDef *) subcmd->def;

					tmpobj = new_objtree_VA("ALTER COLUMN %{column}I ADD %{identity_column}s", 2,
										 "type", ObjTypeString, "add identity",
										 "column", ObjTypeString, subcmd->name);

					attnum = get_attnum(RelationGetRelid(rel), subcmd->name);
					seq_relid = getIdentitySequence(RelationGetRelid(rel), attnum, true);
					seqdef = deparse_ColumnIdentity(seq_relid, coldef->identity, false);

					append_object_object(tmpobj, "identity_column", seqdef);

					subcmds = lappend(subcmds, new_object_object(tmpobj));
				}
				break;
			case AT_SetIdentity:
				{
					DefElem	*defel;
					char		identity = 0;
					ObjTree	   *seqdef;
					AttrNumber	attnum;
					Oid			seq_relid;


					tmpobj = new_objtree_VA("ALTER COLUMN %{column}I", 2,
										 "type", ObjTypeString, "set identity",
										 "column", ObjTypeString, subcmd->name);

					if (subcmd->def)
					{
						List	   *def = (List *) subcmd->def;

						Assert(IsA(subcmd->def, List));

						defel = linitial_node(DefElem, def);
						identity = defGetInt32(defel);
					}

					attnum = get_attnum(RelationGetRelid(rel), subcmd->name);
					seq_relid = getIdentitySequence(RelationGetRelid(rel), attnum, true);
					seqdef = deparse_ColumnIdentity(seq_relid, identity, true);

					append_object_object(tmpobj, "%{definition}s", seqdef);

					subcmds = lappend(subcmds, new_object_object(tmpobj));
					break;
				}
			case AT_DropIdentity:
				tmpobj = new_objtree_VA("ALTER COLUMN %{column}I DROP IDENTITY", 2,
									 "type", ObjTypeString, "drop identity",
									 "column", ObjTypeString, subcmd->name);

				append_string_object(tmpobj, "%{if_not_exists}s",
									 subcmd->missing_ok ? "IF EXISTS" : "");

				subcmds = lappend(subcmds, new_object_object(tmpobj));
				break;
			default:
				elog(WARNING, "unsupported alter table subtype %d",
					 subcmd->subtype);
				break;
		}

		/*
		 * We don't support replicating ALTER TABLE which contains volatile
		 * functions because It's possible the functions contain DDL/DML in
		 * which case these opertions will be executed twice and cause
		 * duplicate data. In addition, we don't know whether the tables being
		 * accessed by these DDL/DML are published or not. So blindly allowing
		 * such functions can allow unintended clauses like the tables accessed
		 * in those functions may not even exist on the subscriber-side.
		 */
		if (contain_volatile_functions((Node *) exprs))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ALTER TABLE command using volatile function cannot be replicated")));

		/*
		 * Clean the list as we already confirmed there is no volatile
		 * function.
		 */
		list_free(exprs);
		exprs = NIL;
	}

	table_close(rel, AccessShareLock);

	if (list_length(subcmds) == 0)
		return NULL;

	append_array_object(alterTableStmt, "%{subcmds:, }s", subcmds);

	return alterTableStmt;
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Deparse a ColumnDef node within a regular (non typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway.).
 */
static ObjTree *
deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
				  ColumnDef *coldef, bool is_alter, List **exprs)
{
	ObjTree	   *column;
	ObjTree	   *tmpobj;
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;

	/*
	 * Inherited columns without local definitions must not be emitted.
	 *
	 * XXX maybe it is useful to have them with "present = false" or some such?
	 */
	if (!coldef->is_local)
		return NULL;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/*
	 * Verbose syntax
	 *
	 * %{name}I %{coltype}T %{compression}s %{default}s %{not_null}s
	 * %{collation}s
	 */
	column = new_objtree_VA("%{name}I %{coltype}T", 3,
							"type", ObjTypeString, "column",
							"name", ObjTypeString, coldef->colname,
							"coltype", ObjTypeObject,
							new_objtree_for_type(typid, typmod));

	/* USING clause */
	tmpobj = new_objtree("COMPRESSION");
	if (coldef->compression)
		append_string_object(tmpobj, "%{compression_method}I", coldef->compression);
	else
	{
		append_null_object(tmpobj, "%{compression_method}I");
		append_bool_object(tmpobj, "present", false);
	}
	append_object_object(column, "%{compression}s", tmpobj);

	tmpobj = new_objtree("COLLATE");
	if (OidIsValid(typcollation))
	{
		ObjTree *collname;

		collname = new_objtree_for_qualname_id(CollationRelationId,
											   typcollation);
		append_object_object(tmpobj, "%{name}D", collname);
	}
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(column, "%{collation}s", tmpobj);

	if (!composite)
	{
		Oid	seqrelid = InvalidOid;

		/*
		 * Emit a NOT NULL declaration if necessary.  Note that we cannot trust
		 * pg_attribute.attnotnull here, because that bit is also set when
		 * primary keys are specified; and we must not emit a NOT NULL
		 * constraint in that case, unless explicitely specified.  Therefore,
		 * we scan the list of constraints attached to this column to determine
		 * whether we need to emit anything.
		 * (Fortunately, NOT NULL constraints cannot be table constraints.)
		 *
		 * In the ALTER TABLE cases, we also add a NOT NULL if the colDef is
		 * marked is_not_null.
		 */
		saw_notnull = false;
		foreach(cell, coldef->constraints)
		{
			Constraint *constr = (Constraint *) lfirst(cell);

			if (constr->contype == CONSTR_NOTNULL)
				saw_notnull = true;
		}
		if (is_alter && coldef->is_not_null)
			saw_notnull = true;

		append_string_object(column, "%{not_null}s",
							 saw_notnull ? "NOT NULL" : "");

		tmpobj = new_objtree("DEFAULT");
		if (attrForm->atthasdef)
		{
			char *defstr;

			defstr = RelationGetColumnDefault(relation, attrForm->attnum,
											  dpcontext, exprs);

			append_string_object(tmpobj, "%{default}s", defstr);
		}
		else
			append_bool_object(tmpobj, "present", false);
		append_object_object(column, "%{default}s", tmpobj);

		/* IDENTITY COLUMN */
		if (coldef->identity)
		{
			Oid	attno = get_attnum(relid, coldef->colname);
			seqrelid = getIdentitySequence(relid, attno, false);
		}

		tmpobj = deparse_ColumnIdentity(seqrelid, coldef->identity, is_alter);
		append_object_object(column, "%{identity_column}s", tmpobj);

		/* GENERATED COLUMN EXPRESSION */
		tmpobj = new_objtree("GENERATED ALWAYS AS");
		if (coldef->generated == ATTRIBUTE_GENERATED_STORED)
		{
			char *defstr;

			defstr = RelationGetColumnDefault(relation, attrForm->attnum,
											  dpcontext, exprs);
			append_string_object(tmpobj, "%{generation_expr}s STORED", defstr);
		}
		else
			append_bool_object(tmpobj, "present", false);

		append_object_object(column, "%{generated_column}s", tmpobj);
	}

	ReleaseSysCache(attrTup);

	return column;
}

/*
 * Subroutine for CREATE TABLE OF deparsing.
 *
 * Deparse a ColumnDef node within a typed table creation.	This is simpler
 * than the regular case, because we don't have to emit the type declaration,
 * collation, or default.  Here we only return something if the column is being
 * declared NOT NULL.
 *
 * As in deparse_ColumnDef, any other constraint is processed elsewhere.
 */
static ObjTree *
deparse_ColumnDef_typed(Relation relation, List *dpcontext, ColumnDef *coldef)
{
	ObjTree	*column = NULL;
	ObjTree	*tmpobj;
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/*
	 * Search for a NOT NULL declaration.  As in deparse_ColumnDef, we rely on
	 * finding a constraint on the column rather than coldef->is_not_null.
	 * (This routine is never used for ALTER cases.)
	 */
	saw_notnull = false;
	foreach(cell, coldef->constraints)
	{
		Constraint *constr = (Constraint *) lfirst(cell);

		if (constr->contype == CONSTR_NOTNULL)
		{
			saw_notnull = true;
			break;
		}
	}

	if (!saw_notnull && !attrForm->atthasdef)
	{
		ReleaseSysCache(attrTup);
		return NULL;
	}

	/*
	 * Verbose syntax
	 *
	 * %{name}I WITH OPTIONS %{default}s %{not_null}s.
	 */
	column = new_objtree_VA("%{name}I WITH OPTIONS", 2,
							"type", ObjTypeString, "column",
							"name", ObjTypeString, coldef->colname);

	append_string_object(column, "%{not_null}s",
						 saw_notnull ? "NOT NULL" : "");

	tmpobj = new_objtree("DEFAULT");
	if (attrForm->atthasdef)
	{
		char *defstr;

		defstr = RelationGetColumnDefault(relation, attrForm->attnum,
										  dpcontext, NULL);

		append_string_object(tmpobj, "%{default}s", defstr);
	}
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(column, "%{default}s", tmpobj);

	/*
	 * Generated columns are not supported on typed tables, so we are done.
	 */

	ReleaseSysCache(attrTup);

	return column;
}

/*
 * Deparse the definition of column identity.
 */
static ObjTree *
deparse_ColumnIdentity(Oid seqrelid, char identity, bool alter_table)
{
	ObjTree	   *column;
	ObjTree	   *identobj;
	List	   *elems = NIL;
	Relation	rel;
	HeapTuple	seqtuple;
	Form_pg_sequence seqform;
	Form_pg_sequence_data seqdata;
	char	   *identfmt;
	char	   *objfmt;

	column = new_objtree("");

	if (!OidIsValid(seqrelid))
	{
		append_bool_object(column, "present", false);
		return column;
	}

	if (alter_table)
	{
		identfmt = "SET GENERATED ";
		objfmt = "%{option}s";
	}
	else
	{
		identfmt = "GENERATED ";
		objfmt = "%{option}s AS IDENTITY";
	}

	identobj = new_objtree(identfmt);

	if (identity == ATTRIBUTE_IDENTITY_ALWAYS)
		append_string_object(identobj, objfmt, "ALWAYS");
	else if (identity ==  ATTRIBUTE_IDENTITY_BY_DEFAULT)
		append_string_object(identobj, objfmt, "BY DEFAULT");
	else
		append_bool_object(identobj, "present", false);

	append_object_object(column, "%{identity_type}s", identobj);

	rel = table_open(SequenceRelationId, RowExclusiveLock);
	seqtuple = SearchSysCacheCopy1(SEQRELID, seqrelid);
	if (!HeapTupleIsValid(seqtuple))
		elog(ERROR, "cache lookup failed for sequence %u",
			 seqrelid);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);
	seqdata = get_sequence_values(seqrelid);

	/* Definition elements */
	elems = lappend(elems, deparse_Seq_Cache(NULL, seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Cycle(NULL, seqform, alter_table));
	elems = lappend(elems, deparse_Seq_IncrementBy(NULL, seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Minvalue(NULL, seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Maxvalue(NULL, seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Startwith(NULL, seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Restart(NULL, seqdata));
	/* We purposefully do not emit OWNED BY here */

	if (alter_table)
		append_array_object(column, "%{seq_definition: }s", elems);
	else
		append_array_object(column, "( %{seq_definition: }s )", elems);

	table_close(rel, RowExclusiveLock);

	return column;
}

/*
 * ... ALTER COLUMN ... SET/RESET (...)
 */
static ObjTree *
deparse_ColumnSetOptions(AlterTableCmd *subcmd)
{
	List	   *sets = NIL;
	ListCell   *cell;
	ObjTree	   *colset;
	char	   *fmt;
	bool		is_reset = subcmd->subtype == AT_ResetOptions;

	/*
	 * Verbose syntax
	 *
	 * ALTER COLUMN %{column}I RESET|SET (%{options:, }s)
	 */
	if (is_reset)
		fmt = "ALTER COLUMN %{column}I RESET ";
	else
		fmt = "ALTER COLUMN %{column}I SET ";

	colset = new_objtree_VA(fmt, 1, "column", ObjTypeString, subcmd->name);

	foreach(cell, (List *) subcmd->def)
	{
		DefElem	   *elem;
		ObjTree	   *set;

		elem = (DefElem *) lfirst(cell);
		set = deparse_DefElem(elem, is_reset);
		sets = lappend(sets, new_object_object(set));
	}

	Assert(sets);
	append_array_object(colset, "(%{options:, }s)", sets);

	return colset;
}

/*
 * Deparse the CREATE DOMAIN
 *
 * Given a function OID and the parsetree that created it, return the JSON
 * blob representing the creation command.
 */
static ObjTree *
deparse_CreateDomain(Oid objectId, Node *parsetree)
{
	ObjTree	   *createDomain;
	ObjTree	   *tmpobj;
	HeapTuple	typTup;
	Form_pg_type typForm;
	List	   *constraints;

	typTup = SearchSysCache1(TYPEOID, objectId);
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for domain with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE DOMAIN %{identity}D AS %{type}T %{not_null}s %{constraints}s
	 * %{collation}s
	 */
	createDomain = new_objtree("CREATE");

	append_object_object(createDomain,
						 "DOMAIN %{identity}D AS",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));
	append_object_object(createDomain,
						 "%{type}T",
						 new_objtree_for_type(typForm->typbasetype, typForm->typtypmod));

	if (typForm->typnotnull)
		append_string_object(createDomain, "%{not_null}s", "NOT NULL");
	else
		append_string_object(createDomain, "%{not_null}s", "");

	constraints = obtainConstraints(NIL, InvalidOid, objectId);
	if (constraints == NIL)
	{
		tmpobj = new_objtree("");
		append_bool_object(tmpobj, "present", false);
	}
	else
		tmpobj = new_objtree_VA("%{elements:, }s", 1,
							 "elements", ObjTypeArray, constraints);
	append_object_object(createDomain, "%{constraints}s", tmpobj);

	tmpobj = new_objtree("COLLATE");
	if (OidIsValid(typForm->typcollation))
	{
		ObjTree *collname;

		collname = new_objtree_for_qualname_id(CollationRelationId,
											   typForm->typcollation);
		append_object_object(tmpobj, "%{name}D", collname);
	}
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(createDomain, "%{collation}s", tmpobj);

	ReleaseSysCache(typTup);

	return createDomain;
}

/*
 * Deparse a CreateFunctionStmt (CREATE FUNCTION)
 *
 * Given a function OID and the parsetree that created it, return the JSON
 * blob representing the creation command.
 */
static ObjTree *
deparse_CreateFunction(Oid objectId, Node *parsetree)
{
	CreateFunctionStmt *node = (CreateFunctionStmt *) parsetree;
	ObjTree	   *createFunc;
	ObjTree	   *tmpobj;
	Datum		tmpdatum;
	char	   *source;
	char	   *probin;
	List	   *params;
	List	   *defaults;
	List	   *sets = NIL;
	List	   *types = NIL;
	ListCell   *cell;
	ListCell   *curdef;
	ListCell   *table_params = NULL;
	HeapTuple	procTup;
	Form_pg_proc procForm;
	HeapTuple	langTup;
	Oid		   *typarray;
	Oid		   *trftypes;
	Form_pg_language langForm;
	int			i;
	int			typnum;
	int			ntypes;
	bool		isnull;
	bool		isfunction;

	/* Get the pg_proc tuple */
	procTup = SearchSysCache1(PROCOID, objectId);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failure for function with OID %u",
			 objectId);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	/* Get the corresponding pg_language tuple */
	langTup = SearchSysCache1(LANGOID, procForm->prolang);
	if (!HeapTupleIsValid(langTup))
		elog(ERROR, "cache lookup failure for language with OID %u",
			 procForm->prolang);
	langForm = (Form_pg_language) GETSTRUCT(langTup);

	/*
	 * Determine useful values for prosrc and probin.  We cope with probin
	 * being either NULL or "-", but prosrc must have a valid value.
	 */
	tmpdatum = SysCacheGetAttr(PROCOID, procTup,
							   Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		elog(ERROR, "null prosrc in function with OID %u", objectId);
	source = TextDatumGetCString(tmpdatum);

	/* Determine a useful value for probin */
	tmpdatum = SysCacheGetAttr(PROCOID, procTup,
							   Anum_pg_proc_probin, &isnull);
	if (isnull)
		probin = NULL;
	else
	{
		probin = TextDatumGetCString(tmpdatum);
		if (probin[0] == '\0' || strcmp(probin, "-") == 0)
			probin = NULL;
	}

	/*
	 * Verbose syntax
	 *
	 * CREATE %{or_replace}s FUNCTION %{signature}s RETURNS %{return_type}s
	 * LANGUAGE %{transform_type}s %{language}I %{window}s %{volatility}s
	 * %{parallel_safety}s %{leakproof}s %{strict}s %{security_definer}s
	 * %{cost}s %{rows}s %{support}s %{set_options: }s AS %{objfile}L,
	 * %{symbol}L
	 */
	createFunc = new_objtree("CREATE");

	append_string_object(createFunc, "%{or_replace}s",
						 node->replace ? "OR REPLACE" : "");

	/*
	 * To construct the arguments array, extract the type OIDs from the
	 * function's pg_proc entry.  If pronargs equals the parameter list length,
	 * there are no OUT parameters and thus we can extract the type OID from
	 * proargtypes; otherwise we need to decode proallargtypes, which is
	 * a bit more involved.
	 */
	typarray = palloc(list_length(node->parameters) * sizeof(Oid));
	if (list_length(node->parameters) > procForm->pronargs)
	{
		Datum	alltypes;
		Datum  *values;
		bool   *nulls;
		int		nelems;

		alltypes = SysCacheGetAttr(PROCOID, procTup,
								   Anum_pg_proc_proallargtypes, &isnull);
		if (isnull)
			elog(ERROR, "NULL proallargtypes, but more parameters than args");
		deconstruct_array(DatumGetArrayTypeP(alltypes),
						  OIDOID, 4, 't', 'i',
						  &values, &nulls, &nelems);
		if (nelems != list_length(node->parameters))
			elog(ERROR, "mismatched proallargatypes");
		for (i = 0; i < list_length(node->parameters); i++)
			typarray[i] = values[i];
	}
	else
	{
		for (i = 0; i < list_length(node->parameters); i++)
			 typarray[i] = procForm->proargtypes.values[i];
	}

	/*
	 * If there are any default expressions, we read the deparsed expression as
	 * a list so that we can attach them to each argument.
	 */
	tmpdatum = SysCacheGetAttr(PROCOID, procTup,
							   Anum_pg_proc_proargdefaults, &isnull);
	if (!isnull)
	{
		defaults = FunctionGetDefaults(DatumGetTextP(tmpdatum));
		curdef = list_head(defaults);
	}
	else
	{
		defaults = NIL;
		curdef = NULL;
	}

	/*
	 * Now iterate over each parameter in the parsetree to create the
	 * parameters array.
	 */
	params = NIL;
	typnum = 0;
	foreach(cell, node->parameters)
	{
		FunctionParameter *param = (FunctionParameter *) lfirst(cell);
		ObjTree	   *paramobj;
		ObjTree	   *defaultval;
		ObjTree	   *name;

		/*
		 * A PARAM_TABLE parameter indicates end of input arguments; the
		 * following parameters are part of the return type.  We ignore them
		 * here, but keep track of the current position in the list so that
		 * we can easily produce the return type below.
		 */
		if (param->mode == FUNC_PARAM_TABLE)
		{
			table_params = cell;
			break;
		}

		/*
		 * Verbose syntax for paramater: %{mode}s %{name}s %{type}T %{default}s
		 *
		 * Note that %{name}s is a string here, not an identifier; the reason
		 * for this is that an absent parameter name must produce an empty
		 * string, not "", which is what would happen if we were to use
		 * %{name}I here.  So we add another level of indirection to allow us
		 * to inject a "present" parameter.
		 */
		paramobj = new_objtree("");
		append_string_object(paramobj, "%{mode}s",
							 param->mode == FUNC_PARAM_IN ? "IN" :
							 param->mode == FUNC_PARAM_OUT ? "OUT" :
							 param->mode == FUNC_PARAM_INOUT ? "INOUT" :
							 param->mode == FUNC_PARAM_VARIADIC ? "VARIADIC" :
							 "IN");

		/* Optional wholesale suppression of "name" occurs here */

		name = new_objtree("");
		append_string_object(name, "%{name}I",
							 param->name ? param->name : "NULL");

		append_bool_object(name, "present",
						   param->name ? true : false);

		append_object_object(paramobj, "%{name}s", name);

		defaultval = new_objtree("DEFAULT");
		if (PointerIsValid(param->defexpr))
		{
			char *expr;

			if (curdef == NULL)
				elog(ERROR, "proargdefaults list too short");
			expr = lfirst(curdef);

			append_string_object(defaultval, "%{value}s", expr);
			curdef = lnext(defaults, curdef);
		}
		else
			append_bool_object(defaultval, "present", false);

		append_object_object(paramobj, "%{type}T",
							 new_objtree_for_type(typarray[typnum++], -1));

		append_object_object(paramobj, "%{default}s", defaultval);

		params = lappend(params, new_object_object(paramobj));
	}

	tmpobj = new_objtree_VA("%{identity}D", 1,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname_id(ProcedureRelationId,
													  objectId));

	append_format_string(tmpobj, "(");
	append_array_object(tmpobj, "%{arguments:, }s", params);
	append_format_string(tmpobj, ")");

	isfunction = (procForm->prokind != PROKIND_PROCEDURE);

	if (isfunction)
		append_object_object(createFunc, "FUNCTION %{signature}s", tmpobj);
	else
		append_object_object(createFunc, "PROCEDURE %{signature}s", tmpobj);

	/*
	 * A return type can adopt one of two forms: either a [SETOF] some_type, or
	 * a TABLE(list-of-types).  We can tell the second form because we saw a
	 * table param above while scanning the argument list.
	 */
	if (table_params == NULL)
	{
		tmpobj = new_objtree_VA("", 1,
							 "return_form", ObjTypeString, "plain");
		append_string_object(tmpobj, "%{setof}s",
							 procForm->proretset ? "SETOF" : "");
		append_object_object(tmpobj, "%{rettype}T",
							 new_objtree_for_type(procForm->prorettype, -1));
	}
	else
	{
		List	   *rettypes = NIL;
		ObjTree	   *paramobj;

		tmpobj = new_objtree_VA("TABLE", 1,
							 "return_form", ObjTypeString, "table");
		for (; table_params != NULL; table_params = lnext(node->parameters, table_params))
		{
			FunctionParameter *param = lfirst(table_params);

			paramobj = new_objtree("");
			append_string_object(paramobj, "%{name}I", param->name);
			append_object_object(paramobj, "%{type}T",
								 new_objtree_for_type(typarray[typnum++], -1));
			rettypes = lappend(rettypes, new_object_object(paramobj));
		}

		append_array_object(tmpobj, "(%{rettypes:, }s)", rettypes);
	}

	if (isfunction)
		append_object_object(createFunc, "RETURNS %{return_type}s", tmpobj);

	/* TRANSFORM FOR TYPE */
	tmpobj = new_objtree("TRANSFORM");

	ntypes = get_func_trftypes(procTup, &trftypes);
	for (i = 0; i < ntypes; i++)
	{
		tmpobj = new_objtree_VA("FOR TYPE %{type}T", 1,
							 "type", ObjTypeObject,
							 new_objtree_for_type(trftypes[i], -1));
		types = lappend(types, tmpobj);
	}

	if (types)
		append_array_object(tmpobj, "%{for_type:, }s", types);
	else
		append_bool_object(tmpobj, "present", false);

	append_object_object(createFunc, "%{transform_type}s", tmpobj);

	append_string_object(createFunc, "LANGUAGE %{language}I",
						 NameStr(langForm->lanname));

	if (isfunction)
	{
		append_string_object(createFunc, "%{window}s",
							 procForm->prokind == PROKIND_WINDOW ? "WINDOW" : "");
		append_string_object(createFunc, "%{volatility}s",
							 procForm->provolatile == PROVOLATILE_VOLATILE ?
							 "VOLATILE" :
							 procForm->provolatile == PROVOLATILE_STABLE ?
							 "STABLE" : "IMMUTABLE");

		append_string_object(createFunc, "%{parallel_safety}s",
							 procForm->proparallel == PROPARALLEL_SAFE ?
							 "PARALLEL SAFE" :
							 procForm->proparallel == PROPARALLEL_RESTRICTED ?
							 "PARALLEL RESTRICTED" : "PARALLEL UNSAFE");

		append_string_object(createFunc, "%{leakproof}s",
							 procForm->proleakproof ? "LEAKPROOF" : "");
		append_string_object(createFunc, "%{strict}s",
							 procForm->proisstrict ?
							 "RETURNS NULL ON NULL INPUT" :
							 "CALLED ON NULL INPUT");

		append_string_object(createFunc, "%{security_definer}s",
							 procForm->prosecdef ?
							 "SECURITY DEFINER" : "SECURITY INVOKER");

		append_object_object(createFunc, "%{cost}s",
							 new_objtree_VA("COST %{cost}n", 1,
											"cost", ObjTypeFloat,
											procForm->procost));

		tmpobj = new_objtree("ROWS");
		if (procForm->prorows == 0)
			append_bool_object(tmpobj, "present", false);
		else
			append_float_object(tmpobj, "%{rows}n", procForm->prorows);
		append_object_object(createFunc, "%{rows}s", tmpobj);

		tmpobj = new_objtree("SUPPORT %{name}s");
		if (procForm->prosupport)
		{
			Oid			argtypes[1];

			/*
			 * We should qualify the support function's name if it wouldn't be
			 * resolved by lookup in the current search path.
			 */
			argtypes[0] = INTERNALOID;
			append_string_object(tmpobj, "%{name}s",
								 generate_function_name(procForm->prosupport, 1,
														NIL, argtypes,
														false, NULL,
														EXPR_KIND_NONE));
		}
		else
			append_bool_object(tmpobj, "present", false);

		append_object_object(createFunc, "%{support}s", tmpobj);
	}

	foreach(cell, node->options)
	{
		DefElem	*defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "set") == 0)
		{
			VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
			char *value = ExtractSetVariableArgs(sstmt);

			tmpobj = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
			sets = lappend(sets, new_object_object(tmpobj));
		}
	}
	append_array_object(createFunc, "%{set_options: }s", sets);

	/* Add the function definition */
	(void) SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prosqlbody, &isnull);
	if (procForm->prolang == SQLlanguageId && !isnull)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		print_function_sqlbody(&buf, procTup);

		append_string_object(createFunc, "%{definition}s", buf.data);
	}
	else if (probin == NULL)
	{
		append_string_object(createFunc, "AS %{definition}L",
							 source);
	}
	else
	{
		append_string_object(createFunc, "AS %{objfile}L", probin);
		append_string_object(createFunc, ", %{symbol}L", source);
	}

	ReleaseSysCache(langTup);
	ReleaseSysCache(procTup);

	return createFunc;
}

/*
 * Deparse a CREATE OPERATOR CLASS command.
 */
static ObjTree *
deparse_CreateOpClassStmt(CollectedCommand *cmd)
{
	Oid			opcoid = cmd->d.createopc.address.objectId;
	HeapTuple   opcTup;
	HeapTuple   opfTup;
	Form_pg_opfamily opfForm;
	Form_pg_opclass opcForm;
	ObjTree	   *stmt;
	ObjTree	   *tmpobj;
	List	   *list;
	ListCell   *cell;

	/* Don't deparse sql commands generated while creating extension */
	if (cmd->in_extension)
		return NULL;

	opcTup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opcoid));
	if (!HeapTupleIsValid(opcTup))
		elog(ERROR, "cache lookup failed for opclass with OID %u", opcoid);
	opcForm = (Form_pg_opclass) GETSTRUCT(opcTup);

	opfTup = SearchSysCache1(OPFAMILYOID, opcForm->opcfamily);
	if (!HeapTupleIsValid(opfTup))
		elog(ERROR, "cache lookup failed for operator family with OID %u", opcForm->opcfamily);
	opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE OPERATOR CLASS %{identity}D %{default}s FOR TYPE %{type}T USING
	 * %{amname}I %{opfamily}s AS %{items:, }s
	 */

	stmt = new_objtree_VA("CREATE OPERATOR CLASS %{identity}D", 1,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname(opcForm->opcnamespace,
												   NameStr(opcForm->opcname)));

	/* Add the DEFAULT clause */
	append_string_object(stmt, "%{default}s",
						 opcForm->opcdefault ? "DEFAULT" : "");

	/* Add the FOR TYPE clause */
	append_object_object(stmt, "FOR TYPE %{type}T",
						 new_objtree_for_type(opcForm->opcintype, -1));

	/* Add the USING clause */
	append_string_object(stmt, "USING %{amname}I", get_am_name(opcForm->opcmethod));

	/*
	 * Add the FAMILY clause; but if it has the same name and namespace as the
	 * opclass, then have it expand to empty, because it would cause a failure
	 * if the opfamily was created internally.
	 */
	tmpobj = new_objtree_VA("FAMILY %{opfamily}D", 1,
						 "opfamily", ObjTypeObject,
						 new_objtree_for_qualname(opfForm->opfnamespace,
												  NameStr(opfForm->opfname)));

	if (strcmp(NameStr(opfForm->opfname), NameStr(opcForm->opcname)) == 0 &&
		opfForm->opfnamespace == opcForm->opcnamespace)
		append_bool_object(tmpobj, "present", false);

	append_object_object(stmt, "%{opfamily}s",  tmpobj);

	/*
	 * Add the initial item list.  Note we always add the STORAGE clause, even
	 * when it is implicit in the original command.
	 */
	tmpobj = new_objtree("STORAGE");
	append_object_object(tmpobj, "%{type}T",
						 new_objtree_for_type(opcForm->opckeytype != InvalidOid ?
											  opcForm->opckeytype : opcForm->opcintype,
											  -1));
	list = list_make1(new_object_object(tmpobj));

	/* Add the declared operators */
	foreach(cell, cmd->d.createopc.operators)
	{
		OpFamilyMember *oper = lfirst(cell);

		tmpobj = new_objtree_VA("OPERATOR %{num}n %{operator}O(%{ltype}T, %{rtype}T)",
							 4,
							 "num", ObjTypeInteger, oper->number,
							 "operator", ObjTypeObject,
							 new_objtree_for_qualname_id(OperatorRelationId,
														 oper->object),
							"ltype", ObjTypeObject,
							new_objtree_for_type(oper->lefttype, -1),
							"rtype", ObjTypeObject,
							new_objtree_for_type(oper->righttype, -1));

		/* Add the FOR SEARCH / FOR ORDER BY clause */
		if (oper->sortfamily == InvalidOid)
			append_string_object(tmpobj, "%{purpose}s", "FOR SEARCH");
		else
		{
			ObjTree	   *tmpobj2;

			tmpobj2 = new_objtree_VA("FOR ORDER BY %{opfamily}D", 0);
			append_object_object(tmpobj2, "opfamily",
								 new_objtree_for_qualname_id(OperatorFamilyRelationId,
															 oper->sortfamily));
			append_object_object(tmpobj, "%{purpose}s", tmpobj2);
		}

		list = lappend(list, new_object_object(tmpobj));
	}

	/* Add the declared support functions */
	foreach(cell, cmd->d.createopc.procedures)
	{
		OpFamilyMember *proc = lfirst(cell);
		HeapTuple	procTup;
		Form_pg_proc procForm;
		Oid		   *proargtypes;
		List	   *arglist = NIL;
		int			i;

		procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc->object));
		if (!HeapTupleIsValid(procTup))
			elog(ERROR, "cache lookup failed for procedure %u", proc->object);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);

		tmpobj = new_objtree_VA("FUNCTION %{num}n (%{ltype}T, %{rtype}T) %{function}D",
							 4,
							 "num", ObjTypeInteger, proc->number,
							 "ltype", ObjTypeObject,
							 new_objtree_for_type(proc->lefttype, -1),
							 "rtype", ObjTypeObject,
							 new_objtree_for_type(proc->righttype, -1),
							 "function", ObjTypeObject,
							 new_objtree_for_qualname(procForm->pronamespace,
													  NameStr(procForm->proname)));

		proargtypes = procForm->proargtypes.values;
		for (i = 0; i < procForm->pronargs; i++)
		{
			ObjTree	   *arg;

			arg = new_objtree_for_type(proargtypes[i], -1);
			arglist = lappend(arglist, new_object_object(arg));
		}

		append_format_string(tmpobj, "(");
		append_array_object(tmpobj, "%{argtypes:, }T", arglist);
		append_format_string(tmpobj, ")");

		ReleaseSysCache(procTup);

		list = lappend(list, new_object_object(tmpobj));
	}

	append_array_object(stmt, "AS %{items:, }s", list);

	ReleaseSysCache(opfTup);
	ReleaseSysCache(opcTup);

	return stmt;
}

/*
 * Deparse a CreateTrigStmt (CREATE OPERATOR FAMILY)
 *
 * Given a trigger OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateOpFamily(Oid objectId, Node *parsetree)
{
	HeapTuple   opfTup;
	HeapTuple   amTup;
	Form_pg_opfamily opfForm;
	Form_pg_am  amForm;
	ObjTree	   *copfStmt;

	opfTup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(opfTup))
		elog(ERROR, "cache lookup failed for operator family with OID %u", objectId);
	opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

	amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
	if (!HeapTupleIsValid(amTup))
		elog(ERROR, "cache lookup failed for access method %u",
			 opfForm->opfmethod);
	amForm = (Form_pg_am) GETSTRUCT(amTup);

	copfStmt = new_objtree_VA("CREATE OPERATOR FAMILY %{identity}D USING %{amname}I",
							  2,
							  "identity", ObjTypeObject,
							  new_objtree_for_qualname(opfForm->opfnamespace,
													   NameStr(opfForm->opfname)),
							  "amname", ObjTypeString, NameStr(amForm->amname));

	ReleaseSysCache(amTup);
	ReleaseSysCache(opfTup);

	return copfStmt;
}

static ObjTree *
deparse_CreatePolicyStmt(Oid objectId, Node *parsetree)
{
	CreatePolicyStmt *node = (CreatePolicyStmt *) parsetree;
	ObjTree	   *policy;

	policy = new_objtree_VA("CREATE POLICY %{identity}I", 1,
							"identity", ObjTypeString, node->policy_name);

	/* add the rest of the stuff */
	add_policy_clauses(policy, objectId, node->roles, !!node->qual,
					   !!node->with_check);

	return policy;
}

static ObjTree *
deparse_AlterPolicyStmt(Oid objectId, Node *parsetree)
{
	AlterPolicyStmt *node = (AlterPolicyStmt *) parsetree;
	ObjTree	   *policy;

	policy = new_objtree_VA("ALTER POLICY %{identity}I", 1,
							"identity", ObjTypeString, node->policy_name);

	/* add the rest of the stuff */
	add_policy_clauses(policy, objectId, node->roles, !!node->qual,
					   !!node->with_check);

	return policy;
}

/*
 * Deparse a CreateSchemaStmt.
 *
 * Given a schema OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 *
 */
static ObjTree *
deparse_CreateSchemaStmt(Oid objectId, Node *parsetree)
{
	CreateSchemaStmt *node = (CreateSchemaStmt *) parsetree;
	ObjTree			 *createSchema;
	ObjTree			 *auth;

	/*
	 * Verbose syntax
	 *
	 * CREATE SCHEMA %{if_not_exists}s %{name}I %{authorization}s
	 */
	createSchema = new_objtree("CREATE SCHEMA");

	append_string_object(createSchema, "%{if_not_exists}s",
						 node->if_not_exists ? "IF NOT EXISTS" : "");

	append_string_object(createSchema, "%{name}I", node->schemaname);

	auth = new_objtree("AUTHORIZATION");
	if (node->authrole)
		append_string_object(auth, "%{authorization_role}I",
							 get_rolespec_name(node->authrole));
	else
	{
		append_null_object(auth, "%{authorization_role}I ");
		append_bool_object(auth, "present", false);
	}
	append_object_object(createSchema, "%{authorization}s", auth);

	return createSchema;
}

/*
 * Return the default value of a domain.
 */
static char *
DomainGetDefault(HeapTuple domTup)
{
	Datum	def;
	Node   *defval;
	char   *defstr;
	bool	isnull;

	def = SysCacheGetAttr(TYPEOID, domTup, Anum_pg_type_typdefaultbin,
							 &isnull);
	if (isnull)
		elog(ERROR, "domain \"%s\" does not have a default value",
			 NameStr(((Form_pg_type) GETSTRUCT(domTup))->typname));
	defval = stringToNode(TextDatumGetCString(def));
	defstr = deparse_expression(defval, NULL /* dpcontext? */,
									   false, false);

	return defstr;
}

/*
 * Deparse a AlterDomainStmt.
 */
static ObjTree *
deparse_AlterDomainStmt(Oid objectId, Node *parsetree,
						ObjectAddress constraintAddr)
{
	AlterDomainStmt *node = (AlterDomainStmt *) parsetree;
	HeapTuple	domTup;
	Form_pg_type domForm;
	ObjTree	   *alterDom;
	char	   *fmt;
	const char *type;

	/* ALTER DOMAIN DROP CONSTRAINT is handled by the DROP support code */
	if (node->subtype == 'X')
		return NULL;

	domTup = SearchSysCache1(TYPEOID, objectId);
	if (!HeapTupleIsValid(domTup))
		elog(ERROR, "cache lookup failed for domain with OID %u",
			 objectId);
	domForm = (Form_pg_type) GETSTRUCT(domTup);

	switch (node->subtype)
	{
		case 'T':
			/* SET DEFAULT / DROP DEFAULT */
			if (node->def == NULL)
			{
				fmt = "ALTER DOMAIN";
				type = "drop default";
				alterDom = new_objtree_VA(fmt, 1, "type", ObjTypeString, type);
				append_object_object(alterDom, "%{identity}D DROP DEFAULT",
									 new_objtree_for_qualname(domForm->typnamespace,
															  NameStr(domForm->typname)));
			}
			else
			{
				fmt = "ALTER DOMAIN";
				type = "set default";
				alterDom = new_objtree_VA(fmt, 1, "type", ObjTypeString, type);
				append_object_object(alterDom, "%{identity}D SET DEFAULT",
									 new_objtree_for_qualname(domForm->typnamespace,
															  NameStr(domForm->typname)));
				append_string_object(alterDom, "%{default}s", DomainGetDefault(domTup));
			}

			break;
		case 'N':
			/* DROP NOT NULL */
			fmt = "ALTER DOMAIN";
			type = "drop not null";
			alterDom = new_objtree_VA(fmt, 1, "type", ObjTypeString, type);
			append_object_object(alterDom, "%{identity}D DROP NOT NULL",
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)));
			break;
		case 'O':
			/* SET NOT NULL */
			fmt = "ALTER DOMAIN";
			type = "set not null";
			alterDom = new_objtree_VA(fmt, 1, "type", ObjTypeString, type);
			append_object_object(alterDom, "%{identity}D SET NOT NULL",
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)));
			break;
		case 'C':
			/* ADD CONSTRAINT.  Only CHECK constraints are supported by domains */
			fmt = "ALTER DOMAIN";
			type = "add constraint";
			alterDom = new_objtree_VA(fmt, 1, "type", ObjTypeString, type);
			append_object_object(alterDom, "%{identity}D",
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)));
			/* a new constraint has a name and definition */
			append_string_object(alterDom, "ADD CONSTRAINT %{constraint_name}s",
								 get_constraint_name(constraintAddr.objectId));
			append_string_object(alterDom, "%{definition}s",
								 pg_get_constraintdef_command_simple(constraintAddr.objectId));
			break;
		case 'V':
			/* VALIDATE CONSTRAINT */
			fmt = "ALTER DOMAIN";
			type = "validate constraint";

			/*
			 * Process subtype=specific options. Validation a constraint
			 * requires its name.
			 */
			alterDom = new_objtree_VA(fmt, 1, "type", ObjTypeString, type);
			append_object_object(alterDom, "%{identity}D",
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)));
			append_string_object(alterDom, "VALIDATE CONSTRAINT %{constraint_name}s", node->name);

			break;
		default:
			elog(ERROR, "invalid subtype %c", node->subtype);
	}

	/* done */
	ReleaseSysCache(domTup);

	return alterDom;
}

/*
 * Deparse an CreateForeignServerStmt (CREATE SERVER)
 *
 * Given a server OID and the parsetree that created it, return the JSON
 * blob representing the alter command.
 */
static ObjTree *
deparse_CreateForeignServerStmt(Oid objectId, Node *parsetree)
{
	CreateForeignServerStmt *node = (CreateForeignServerStmt *) parsetree;
	ObjTree    *createServer;
	ObjTree    *tmp;

	createServer = new_objtree_VA("CREATE SERVER %{identity}I", 1,
								  "identity", ObjTypeString, node->servername);

	/* add a TYPE clause, if any */
	tmp = new_objtree_VA("TYPE", 0);
	if (node->servertype)
		append_string_object(tmp, "%{type}L", node->servertype);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(createServer, "%{type}s", tmp);

	/* add a VERSION clause, if any */
	tmp = new_objtree_VA("VERSION", 0);
	if (node->version)
		append_string_object(tmp, "%{version}L", node->version);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(createServer, "%{version}s", tmp);

	append_string_object(createServer, "FOREIGN DATA WRAPPER %{fdw}I", node->fdwname);
	/* add an OPTIONS clause, if any */
	append_object_object(createServer, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	return createServer;
}

/*
 * Deparse an AlterForeignServerStmt (ALTER SERVER)
 *
 * Given a server OID and the parsetree that created it, return the JSON
 * blob representing the alter command.
 */
static ObjTree *
deparse_AlterForeignServerStmt(Oid objectId, Node *parsetree)
{
	AlterForeignServerStmt *node = (AlterForeignServerStmt *) parsetree;
	ObjTree    *alterServer;
	ObjTree    *tmp;

	alterServer = new_objtree_VA("ALTER SERVER %{identity}I", 1,
								 "identity", ObjTypeString, node->servername);

	/* add a VERSION clause, if any */
	tmp = new_objtree_VA("VERSION", 0);
	if (node->has_version && node->version)
		append_string_object(tmp, "%{version}L", node->version);
	else if (node->has_version)
		append_string_object(tmp, "version", "NULL");
	else
		append_bool_object(tmp, "present", false);
	append_object_object(alterServer, "%{version}s", tmp);

	/* add a VERSION clause, if any */
	tmp = new_objtree_VA("VERSION", 0);
	if (node->has_version && node->version)
		append_string_object(tmp, "%{version}L", node->version);
	else if (node->has_version)
		append_string_object(tmp, "version", "NULL");
	else
		append_bool_object(tmp, "present", false);
	append_object_object(alterServer, "%{version}s", tmp);

	/* add an OPTIONS clause, if any */
	append_object_object(alterServer, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	return alterServer;
}

/*
 * Deparse a CreateSeqStmt.
 *
 * Given a sequence OID and the parsetree that create it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree	*createSeq;
	ObjTree	*tmpobj;
	Relation	relation;
	Form_pg_sequence_data seqdata;
	List	   *elems = NIL;
	Form_pg_sequence seqform;
	Relation	rel;
	HeapTuple	seqtuple;
	CreateSeqStmt *createSeqStmt = (CreateSeqStmt *) parsetree;

	/*
	 * Sequence for IDENTITY COLUMN output separately(via CREATE TABLE or
	 * ALTER TABLE); return empty here.
	 */
	if (createSeqStmt->for_identity)
		return NULL;

	seqdata = get_sequence_values(objectId);

	relation = relation_open(objectId, AccessShareLock);
	rel = table_open(SequenceRelationId, RowExclusiveLock);
	seqtuple = SearchSysCacheCopy1(SEQRELID,
								   ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(seqtuple))
		elog(ERROR, "cache lookup failed for sequence %u",
			 objectId);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

	/*
	 * Verbose syntax
	 *
	 * CREATE %{persistence}s SEQUENCE %{identity}D
	 */
	createSeq = new_objtree("CREATE");

	append_string_object(createSeq, "%{persistence}s",
						 get_persistence_str(relation->rd_rel->relpersistence));

	tmpobj = new_objtree_for_qualname(relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(createSeq, "SEQUENCE %{identity}D", tmpobj);

	/* Definition elements */
	elems = lappend(elems, deparse_Seq_Cache(createSeq, seqform, false));
	elems = lappend(elems, deparse_Seq_Cycle(createSeq, seqform, false));
	elems = lappend(elems, deparse_Seq_IncrementBy(createSeq, seqform, false));
	elems = lappend(elems, deparse_Seq_Minvalue(createSeq, seqform, false));
	elems = lappend(elems, deparse_Seq_Maxvalue(createSeq, seqform, false));
	elems = lappend(elems, deparse_Seq_Startwith(createSeq, seqform, false));
	elems = lappend(elems, deparse_Seq_Restart(createSeq, seqdata));

	/* We purposefully do not emit OWNED BY here */
	append_array_object(createSeq, "%{definition: }s", elems);

	table_close(rel, RowExclusiveLock);
	relation_close(relation, AccessShareLock);

	return createSeq;
}

/*
 * Deparse a CreateStmt (CREATE TABLE).
 *
 * Given a table OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	ObjTree	*createStmt;
	ObjTree	*tmpobj;
	List	   *list = NIL;
	ListCell   *cell;

	/*
	 * Verbose syntax
	 *
	 * CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D [OF
	 * %{of_type}T | PARTITION OF %{parent_identity}D] %{table_elements}s
	 * %{inherits}s %{partition_by}s %{access_method}s %{with_clause}s
	 * %{on_commit}s %{tablespace}s
	 */
	createStmt = new_objtree("CREATE");

	append_string_object(createStmt, "%{persistence}s",
						 get_persistence_str(relation->rd_rel->relpersistence));

	append_format_string(createStmt, "TABLE");

	append_string_object(createStmt, "%{if_not_exists}s",
						 node->if_not_exists ? "IF NOT EXISTS" : "");

	tmpobj = new_objtree_for_qualname(relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(createStmt, "%{identity}D", tmpobj);

	dpcontext = deparse_context_for(RelationGetRelationName(relation),
									objectId);

	/*
	 * Typed tables and partitions use a slightly different format string: we
	 * must not put table_elements with parents directly in the fmt string,
	 * because if there are no options the parens must not be emitted; and
	 * also, typed tables do not allow for inheritance.
	 */
	if (node->ofTypename || node->partbound)
	{
		List       *tableelts = NIL;

		/*
		 * We can't put table elements directly in the fmt string as an array
		 * surrounded by parens here, because an empty clause would cause a
		 * syntax error.  Therefore, we use an indirection element and set
		 * present=false when there are no elements.
		 */
		if (node->ofTypename)
		{
			tmpobj = new_objtree_for_type(relation->rd_rel->reloftype, -1);
			append_object_object(createStmt, "OF %{of_type}T", tmpobj);
		}
		else
		{
			List     *parents;
			ObjElem  *elem;

			parents = deparse_InhRelations(objectId);
			elem = (ObjElem *) linitial(parents);

			Assert(list_length(parents) == 1);

			append_format_string(createStmt, "PARTITION OF");

			append_object_object(createStmt, "%{parent_identity}D",
								 elem->value.object);
		}

		tableelts = deparse_TableElements(relation, node->tableElts, dpcontext,
										  true,      /* typed table */
										  false);    /* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		if (tableelts == NIL)
		{
			tmpobj = new_objtree("");
			append_bool_object(tmpobj, "present", false);
		}
		else
			tmpobj = new_objtree_VA("(%{elements:, }s)", 1,
								 "elements", ObjTypeArray, tableelts);

		append_object_object(createStmt, "%{table_elements}s", tmpobj);
	}
	else
	{
		List       *tableelts = NIL;

		/*
		 * There is no need to process LIKE clauses separately; they have
		 * already been transformed into columns and constraints.
		 */

		/*
		 * Process table elements: column definitions and constraints.  Only
		 * the column definitions are obtained from the parse node itself.  To
		 * get constraints we rely on pg_constraint, because the parse node
		 * might be missing some things such as the name of the constraints.
		 */
		tableelts = deparse_TableElements(relation, node->tableElts, dpcontext,
										  false,     /* not typed table */
										  false);    /* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		append_array_object(createStmt, "(%{table_elements:, }s)", tableelts);

		/*
		 * Add inheritance specification.  We cannot simply scan the list of
		 * parents from the parser node, because that may lack the actual
		 * qualified names of the parent relations.  Rather than trying to
		 * re-resolve them from the information in the parse node, it seems
		 * more accurate and convenient to grab it from pg_inherits.
		 */
		tmpobj = new_objtree("INHERITS");
		if (list_length(node->inhRelations) > 0)
			append_array_object(tmpobj, "(%{parents:, }D)", deparse_InhRelations(objectId));
		else
		{
			append_null_object(tmpobj, "(%{parents:, }D)");
			append_bool_object(tmpobj, "present", false);
		}
		append_object_object(createStmt, "%{inherits}s", tmpobj);
	}

	tmpobj = new_objtree("TABLESPACE");
	if (node->tablespacename)
		append_string_object(tmpobj, "%{tablespace}I", node->tablespacename);
	else
	{
		append_null_object(tmpobj, "%{tablespace}I");
		append_bool_object(tmpobj, "present", false);
	}
	append_object_object(createStmt, "%{tablespace}s", tmpobj);
	append_object_object(createStmt, "%{on_commit}s",
						  deparse_OnCommitClause(node->oncommit));

	/* FOR VALUES clause */
	if (node->partbound)
	{
		/*
		 * Get pg_class.relpartbound. We cannot use partbound in the
		 * parsetree directly as it's the original partbound expression
		 * which haven't been transformed.
		 */
		append_string_object(createStmt, "%{partition_bound}s",
							 RelationGetPartitionBound(objectId));
	}

	/* PARTITION BY clause */
	tmpobj = new_objtree("PARTITION BY");
	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		append_string_object(tmpobj, "%{definition}s", pg_get_partkeydef_simple(objectId));
	else
	{
		append_null_object(tmpobj, "%{definition}s");
		append_bool_object(tmpobj, "present", false);
	}
	append_object_object(createStmt, "%{partition_by}s", tmpobj);

	/* USING clause */
	tmpobj = new_objtree("USING");
	if (node->accessMethod)
		append_string_object(tmpobj, "%{access_method}I", node->accessMethod);
	else
	{
		append_null_object(tmpobj, "%{access_method}I");
		append_bool_object(tmpobj, "present", false);
	}
	append_object_object(createStmt, "%{access_method}s", tmpobj);

	/* WITH clause */
	tmpobj = new_objtree("WITH");

	foreach(cell, node->options)
	{
		ObjTree    *tmpobj2;
		DefElem *opt = (DefElem *) lfirst(cell);

		tmpobj2 = deparse_DefElem(opt, false);
		list = lappend(list, new_object_object(tmpobj2));
	}

	if (list)
		append_array_object(tmpobj, "(%{with:, }s)", list);
	else
		append_bool_object(tmpobj, "present", false);

	append_object_object(createStmt, "%{with_clause}s", tmpobj);

	relation_close(relation, AccessShareLock);

	return createStmt;
}

/*
 * Deparse a DefineStmt.
 */
static ObjTree *
deparse_DefineStmt(Oid objectId, Node *parsetree, ObjectAddress secondaryObj)
{
	DefineStmt *define = (DefineStmt *) parsetree;
	ObjTree	   *defStmt = NULL;

	switch (define->kind)
	{
		case OBJECT_COLLATION:
			defStmt = deparse_DefineStmt_Collation(objectId, define, secondaryObj);
			break;

		case OBJECT_OPERATOR:
			defStmt = deparse_DefineStmt_Operator(objectId, define);
			break;

		case OBJECT_TYPE:
			defStmt = deparse_DefineStmt_Type(objectId, define);
			break;

		case OBJECT_TSCONFIGURATION:
			defStmt = deparse_DefineStmt_TSConfig(objectId, define, secondaryObj);
			break;

		case OBJECT_TSPARSER:
			defStmt = deparse_DefineStmt_TSParser(objectId, define);
			break;

		case OBJECT_TSDICTIONARY:
			defStmt = deparse_DefineStmt_TSDictionary(objectId, define);
			break;

		case OBJECT_TSTEMPLATE:
			defStmt = deparse_DefineStmt_TSTemplate(objectId, define);
			break;

		default:
			elog(ERROR, "unsupported object kind");
	}

	return defStmt;
}

/*
 * Deparse a DefineStmt (CREATE COLLATION)
 *
 * Given a collation OID, return the JSON blob representing the alter command.
 */
static ObjTree *
deparse_DefineStmt_Collation(Oid objectId, DefineStmt *define,
							 ObjectAddress fromCollid)
{
	ObjTree    *stmt;
	HeapTuple	colTup;
	Form_pg_collation colForm;
	Datum		datum;
	bool		isnull;
	ObjTree    *tmp;
	List	   *list = NIL;

	colTup = SearchSysCache1(COLLOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(colTup))
		elog(ERROR, "cache lookup failed for collation with OID %u", objectId);
	colForm = (Form_pg_collation) GETSTRUCT(colTup);

	stmt = new_objtree_VA("CREATE COLLATION", 0);

	append_object_object(stmt, "%{identity}D",
						 new_objtree_for_qualname(colForm->collnamespace,
												  NameStr(colForm->collname)));

	if (fromCollid.objectId != InvalidOid)
	{
		Oid			collid = fromCollid.objectId;
		HeapTuple	tp;
		Form_pg_collation fromColForm;

		/*
		 * CREATE COLLATION %{identity}D FROM existing_collation;
		 */
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", collid);

		fromColForm = (Form_pg_collation) GETSTRUCT(tp);

		append_object_object(stmt, "FROM %{from_identity}D",
							 new_objtree_for_qualname(fromColForm->collnamespace,
													  NameStr(fromColForm->collname)));


		ReleaseSysCache(tp);
		ReleaseSysCache(colTup);
		return stmt;
	}

	/* LOCALE */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_colliculocale, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("LOCALE=", 1,
							 "clause", ObjTypeString, "locale");
		append_string_object(tmp, "%{locale}L",
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	/* LC_COLLATE */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_collcollate, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("LC_COLLATE=", 1,
							 "clause", ObjTypeString, "collate");
		append_string_object(tmp, "%{collate}L",
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	/* LC_CTYPE */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_collctype, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("LC_CTYPE=", 1,
							 "clause", ObjTypeString, "ctype");
		append_string_object(tmp, "%{ctype}L",
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	/* PROVIDER */
	if (colForm->collprovider == COLLPROVIDER_ICU)
	{
		tmp = new_objtree_VA("PROVIDER=", 1,
							 "clause", ObjTypeString, "provider");
		append_string_object(tmp, "%{provider}L",
							 psprintf("%s", "icu"));
		list = lappend(list, new_object_object(tmp));
	}
	else if (colForm->collprovider == COLLPROVIDER_LIBC)
	{
		tmp = new_objtree_VA("PROVIDER=", 1,
							 "clause", ObjTypeString, "provider");
		append_string_object(tmp, "%{provider}L",
							 psprintf("%s", "libc"));
		list = lappend(list, new_object_object(tmp));
	}

	/* DETERMINISTIC */
	if (colForm->collisdeterministic)
	{
		tmp = new_objtree_VA("DETERMINISTIC=", 1,
							 "clause", ObjTypeString, "deterministic");
		append_string_object(tmp, "%{deterministic}L",
							 psprintf("%s", "true"));
		list = lappend(list, new_object_object(tmp));
	}

	/* VERSION */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_collversion, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("VERSION=", 1,
							 "clause", ObjTypeString, "version");
		append_string_object(tmp, "%{version}L",
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	append_array_object(stmt, "(%{elems:, }s)", list);
	ReleaseSysCache(colTup);

	return stmt;
}

/*
 * Deparse a DefineStmt (CREATE OPERATOR)
 *
 * Given a trigger OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_DefineStmt_Operator(Oid objectId, DefineStmt *define)
{
	HeapTuple   oprTup;
	ObjTree	   *stmt;
	ObjTree	   *tmpobj;
	List	   *list = NIL;
	Form_pg_operator oprForm;

	oprTup = SearchSysCache1(OPEROID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(oprTup))
		elog(ERROR, "cache lookup failed for operator with OID %u", objectId);
	oprForm = (Form_pg_operator) GETSTRUCT(oprTup);

	stmt = new_objtree_VA("CREATE OPERATOR %{identity}O", 1,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname(oprForm->oprnamespace,
												   NameStr(oprForm->oprname)));

	/* PROCEDURE */
	tmpobj = new_objtree_VA("PROCEDURE=%{procedure}D", 2,
						 "clause", ObjTypeString, "procedure",
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 oprForm->oprcode));
	list = lappend(list, new_object_object(tmpobj));

	/* LEFTARG */
	tmpobj = new_objtree_VA("LEFTARG=", 1,
						 "clause", ObjTypeString, "leftarg");
	if (OidIsValid(oprForm->oprleft))
		append_object_object(tmpobj, "%{type}T",
							 new_objtree_for_type(oprForm->oprleft, -1));
	else
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* RIGHTARG */
	tmpobj = new_objtree_VA("RIGHTARG=", 1,
						 "clause", ObjTypeString, "rightarg");
	if (OidIsValid(oprForm->oprright))
		append_object_object(tmpobj, "%{type}T",
							 new_objtree_for_type(oprForm->oprright, -1));
	else
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* COMMUTATOR */
	tmpobj = new_objtree_VA("COMMUTATOR=", 1,
						 "clause", ObjTypeString, "commutator");
	if (OidIsValid(oprForm->oprcom))
		append_object_object(tmpobj, "%{oper}D",
							 new_objtree_for_qualname_id(OperatorRelationId,
														 oprForm->oprcom));
	else
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* NEGATOR */
	tmpobj = new_objtree_VA("NEGATOR=", 1,
						 "clause", ObjTypeString, "negator");
	if (OidIsValid(oprForm->oprnegate))
		append_object_object(tmpobj, "%{oper}D",
							 new_objtree_for_qualname_id(OperatorRelationId,
														 oprForm->oprnegate));
	else
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* RESTRICT */
	tmpobj = new_objtree_VA("RESTRICT=", 1,
						 "clause", ObjTypeString, "restrict");
	if (OidIsValid(oprForm->oprrest))
		append_object_object(tmpobj, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 oprForm->oprrest));
	else
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* JOIN */
	tmpobj = new_objtree_VA("JOIN=", 1,
						 "clause", ObjTypeString, "join");
	if (OidIsValid(oprForm->oprjoin))
		append_object_object(tmpobj, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 oprForm->oprjoin));
	else
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* MERGES */
	tmpobj = new_objtree_VA("MERGES", 1,
						 "clause", ObjTypeString, "merges");
	if (!oprForm->oprcanmerge)
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	/* HASHES */
	tmpobj = new_objtree_VA("HASHES", 1,
						 "clause", ObjTypeString, "hashes");
	if (!oprForm->oprcanhash)
		append_bool_object(tmpobj, "present", false);
	list = lappend(list, new_object_object(tmpobj));

	append_array_object(stmt, "(%{elems:, }s)", list);

	ReleaseSysCache(oprTup);

	return stmt;
}

/*
 * Deparse a CREATE TYPE statement.
 */
static ObjTree *
deparse_DefineStmt_Type(Oid objectId, DefineStmt *define)
{
	HeapTuple   typTup;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list;
	char	   *str;
	Datum		dflt;
	bool		isnull;
	Form_pg_type typForm;

	typTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for type with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	/* Shortcut processing for shell types. */
	if (!typForm->typisdefined)
	{
		stmt = new_objtree_VA("CREATE TYPE", 0);
		append_object_object(stmt, "%{identity}D",
							 new_objtree_for_qualname(typForm->typnamespace,
													  NameStr(typForm->typname)));
		append_bool_object(stmt, "present", true);
		ReleaseSysCache(typTup);
		return stmt;
	}

	stmt = new_objtree_VA("CREATE TYPE", 0);
	append_object_object(stmt, "%{identity}D",
							new_objtree_for_qualname(typForm->typnamespace,
													NameStr(typForm->typname)));
	append_bool_object(stmt, "present", true);

	/* Add the definition clause */
	list = NIL;

	/* INPUT */
	tmp = new_objtree_VA("(INPUT=", 1,
						 "clause", ObjTypeString, "input");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 typForm->typinput));
	list = lappend(list, new_object_object(tmp));

	/* OUTPUT */
	tmp = new_objtree_VA("OUTPUT=", 1,
						 "clause", ObjTypeString, "output");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 typForm->typoutput));
	list = lappend(list, new_object_object(tmp));

	/* RECEIVE */
	tmp = new_objtree_VA("RECEIVE=", 1,
						 "clause", ObjTypeString, "receive");
	if (OidIsValid(typForm->typreceive))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typreceive));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* SEND */
	tmp = new_objtree_VA("SEND=", 1,
						 "clause", ObjTypeString, "send");
	if (OidIsValid(typForm->typsend))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typsend));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* TYPMOD_IN */
	tmp = new_objtree_VA("TYPMOD_IN=", 1,
						 "clause", ObjTypeString, "typmod_in");
	if (OidIsValid(typForm->typmodin))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodin));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* TYPMOD_OUT */
	tmp = new_objtree_VA("TYPMOD_OUT=", 1,
						 "clause", ObjTypeString, "typmod_out");
	if (OidIsValid(typForm->typmodout))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodout));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* ANALYZE */
	tmp = new_objtree_VA("ANALYZE=", 1,
						 "clause", ObjTypeString, "analyze");
	if (OidIsValid(typForm->typanalyze))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typanalyze));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* INTERNALLENGTH */
	if (typForm->typlen == -1)
	{
		tmp = new_objtree_VA("INTERNALLENGTH=VARIABLE", 0);
	}
	else
	{
		tmp = new_objtree_VA("INTERNALLENGTH=%{typlen}n", 1,
							 "typlen", ObjTypeInteger, typForm->typlen);
	}

	list = lappend(list, new_object_object(tmp));

	/* PASSEDBYVALUE */
	tmp = new_objtree_VA("PASSEDBYVALUE", 1,
						 "clause", ObjTypeString, "passedbyvalue");
	if (!typForm->typbyval)
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* ALIGNMENT */
	tmp = new_objtree_VA("ALIGNMENT=", 1,
						 "clause", ObjTypeString, "alignment");
	/* XXX it's odd to represent alignment with schema-qualified type names */
	switch (typForm->typalign)
	{
		case 'd':
			str = "pg_catalog.float8";
			break;
		case 'i':
			str = "pg_catalog.int4";
			break;
		case 's':
			str = "pg_catalog.int2";
			break;
		case 'c':
			str = "pg_catalog.bpchar";
			break;
		default:
			elog(ERROR, "invalid alignment %c", typForm->typalign);
	}
	append_string_object(tmp, "%{align}s", str);
	list = lappend(list, new_object_object(tmp));

	tmp = new_objtree_VA("STORAGE=", 1,
						 "clause", ObjTypeString, "storage");
	switch (typForm->typstorage)
	{
		case 'p':
			str = "plain";
			break;
		case 'e':
			str = "external";
			break;
		case 'x':
			str = "extended";
			break;
		case 'm':
			str = "main";
			break;
		default:
			elog(ERROR, "invalid storage specifier %c", typForm->typstorage);
	}
	append_string_object(tmp, "%{storage}s", str);
	list = lappend(list, new_object_object(tmp));

	/* CATEGORY */
	tmp = new_objtree_VA("CATEGORY=", 1,
						 "clause", ObjTypeString, "category");
	append_string_object(tmp, "%{category}s",
						 psprintf("%c", typForm->typcategory));
	list = lappend(list, new_object_object(tmp));

	/* PREFERRED */
	tmp = new_objtree_VA("PREFERRED=", 1,
						 "clause", ObjTypeString, "preferred");
	if (!typForm->typispreferred)
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* DEFAULT */
	dflt = SysCacheGetAttr(TYPEOID, typTup,
						   Anum_pg_type_typdefault,
						   &isnull);
	tmp = new_objtree_VA("DEFAULT=", 1,
						 "clause", ObjTypeString, "default");
	if (!isnull)
		append_string_object(tmp, "%{default}s", TextDatumGetCString(dflt));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* ELEMENT */
	tmp = new_objtree_VA("ELEMENT=", 1,
						 "clause", ObjTypeString, "element");
	if (OidIsValid(typForm->typelem))
		append_object_object(tmp, "elem",
							 new_objtree_for_type(typForm->typelem, -1));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* DELIMITER */
	tmp = new_objtree_VA("DELIMITER=", 1,
						 "clause", ObjTypeString, "delimiter");
	append_string_object(tmp, "%{delim}L",
						 psprintf("%c", typForm->typdelim));
	list = lappend(list, new_object_object(tmp));

	/* COLLATABLE */
	tmp = new_objtree_VA("COLLATABLE=", 1,
						 "clause", ObjTypeString, "collatable");
	if (!OidIsValid(typForm->typcollation))
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	append_array_object(stmt, "%{elems:, }s)", list);

	ReleaseSysCache(typTup);

	return stmt;
}

static ObjTree *
deparse_DefineStmt_TSConfig(Oid objectId, DefineStmt *define,
							ObjectAddress copied)
{
	HeapTuple   tscTup;
	HeapTuple   tspTup;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	Form_pg_ts_config tscForm;
	Form_pg_ts_parser tspForm;
	List	   *list;

	tscTup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tscTup))
		elog(ERROR, "cache lookup failed for text search configuration %u",
			 objectId);
	tscForm = (Form_pg_ts_config) GETSTRUCT(tscTup);

	tspTup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(tscForm->cfgparser));
	if (!HeapTupleIsValid(tspTup))
		elog(ERROR, "cache lookup failed for text search parser %u",
			 tscForm->cfgparser);
	tspForm = (Form_pg_ts_parser) GETSTRUCT(tspTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE TEXT SEARCH CONFIGURATION %{identity}D (%{elems:, }s)
	 */
	stmt = new_objtree("CREATE");

	append_object_object(stmt, "TEXT SEARCH CONFIGURATION %{identity}D",
						 new_objtree_for_qualname(tscForm->cfgnamespace,
												  NameStr(tscForm->cfgname)));

	/*
	 * Add the definition clause.  If we have COPY'ed an existing config, add
	 * a COPY clause; otherwise add a PARSER clause.
	 */
	list = NIL;
	/* COPY */
	tmp = new_objtree_VA("COPY=", 1,
						 "clause", ObjTypeString, "copy");
	if (copied.objectId != InvalidOid)
		append_object_object(tmp, "%{tsconfig}D",
							 new_objtree_for_qualname_id(TSConfigRelationId,
														 copied.objectId));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* PARSER */
	tmp = new_objtree_VA("PARSER=", 1,
						 "clause", ObjTypeString, "parser");
	if (copied.objectId == InvalidOid)
		append_object_object(tmp, "%{parser}D",
							 new_objtree_for_qualname(tspForm->prsnamespace,
													  NameStr(tspForm->prsname)));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	append_array_object(stmt, "(%{elems:, }s)", list);

	ReleaseSysCache(tspTup);
	ReleaseSysCache(tscTup);

	return stmt;
}

static ObjTree *
deparse_DefineStmt_TSParser(Oid objectId, DefineStmt *define)
{
	HeapTuple   tspTup;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list;
	Form_pg_ts_parser tspForm;

	tspTup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tspTup))
		elog(ERROR, "cache lookup failed for text search parser with OID %u",
			 objectId);
	tspForm = (Form_pg_ts_parser) GETSTRUCT(tspTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE TEXT SEARCH PARSER %{identity}D (%{elems:, }s)
	 */
	stmt = new_objtree("CREATE");

	append_object_object(stmt, "TEXT SEARCH PARSER %{identity}D",
						 new_objtree_for_qualname(tspForm->prsnamespace,
												  NameStr(tspForm->prsname)));

	/* Add the definition clause */
	list = NIL;

	/* START */
	tmp = new_objtree_VA("START=", 1,
						 "clause", ObjTypeString, "start");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prsstart));
	list = lappend(list, new_object_object(tmp));

	/* GETTOKEN */
	tmp = new_objtree_VA("GETTOKEN=", 1,
						 "clause", ObjTypeString, "gettoken");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prstoken));
	list = lappend(list, new_object_object(tmp));

	/* END */
	tmp = new_objtree_VA("END=", 1,
						 "clause", ObjTypeString, "end");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prsend));
	list = lappend(list, new_object_object(tmp));

	/* LEXTYPES */
	tmp = new_objtree_VA("LEXTYPES=", 1,
						 "clause", ObjTypeString, "lextypes");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prslextype));
	list = lappend(list, new_object_object(tmp));

	/* HEADLINE */
	tmp = new_objtree_VA("HEADLINE=", 1,
						 "clause", ObjTypeString, "headline");
	if (OidIsValid(tspForm->prsheadline))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 tspForm->prsheadline));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	append_array_object(stmt, "(%{elems:, }s)", list);

	ReleaseSysCache(tspTup);

	return stmt;
}

static ObjTree *
deparse_DefineStmt_TSDictionary(Oid objectId, DefineStmt *define)
{
	HeapTuple   tsdTup;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list;
	Datum		options;
	bool		isnull;
	Form_pg_ts_dict tsdForm;

	tsdTup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tsdTup))
		elog(ERROR, "cache lookup failed for text search dictionary "
			 "with OID %u", objectId);
	tsdForm = (Form_pg_ts_dict) GETSTRUCT(tsdTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE TEXT SEARCH DICTIONARY %{identity}D (%{elems:, }s)
	 */
	stmt = new_objtree("CREATE");

	append_object_object(stmt, "TEXT SEARCH DICTIONARY %{identity}D",
						 new_objtree_for_qualname(tsdForm->dictnamespace,
												  NameStr(tsdForm->dictname)));

	/* Add the definition clause */
	list = NIL;

	/* TEMPLATE */
	tmp = new_objtree_VA("TEMPLATE=", 1,
						 "clause", ObjTypeString, "template");
	append_object_object(tmp, "%{template}D",
						 new_objtree_for_qualname_id(TSTemplateRelationId,
													 tsdForm->dicttemplate));
	list = lappend(list, new_object_object(tmp));

	/*
	 * options.  XXX this is a pretty useless representation, but we can't do
	 * better without more ts_cache.c cooperation ...
	 */
	options = SysCacheGetAttr(TSDICTOID, tsdTup,
							  Anum_pg_ts_dict_dictinitoption,
							  &isnull);
	tmp = new_objtree_VA("", 0);
	if (!isnull)
		append_string_object(tmp, "%{options}s", TextDatumGetCString(options));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	append_array_object(stmt, "(%{elems:, }s)", list);

	ReleaseSysCache(tsdTup);

	return stmt;
}

static ObjTree *
deparse_DefineStmt_TSTemplate(Oid objectId, DefineStmt *define)
{
	HeapTuple   tstTup;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list;
	Form_pg_ts_template tstForm;

	tstTup = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tstTup))
		elog(ERROR, "cache lookup failed for text search template with OID %u",
			 objectId);
	tstForm = (Form_pg_ts_template) GETSTRUCT(tstTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE TEXT SEARCH TEMPLATE %{identity}D (%{elems:, }s)
	 */
	stmt = new_objtree("CREATE");

	append_object_object(stmt, "TEXT SEARCH TEMPLATE %{identity}D",
						 new_objtree_for_qualname(tstForm->tmplnamespace,
												  NameStr(tstForm->tmplname)));

	/* Add the definition clause */
	list = NIL;

	/* INIT */
	tmp = new_objtree_VA("INIT=", 1,
						 "clause", ObjTypeString, "init");
	if (OidIsValid(tstForm->tmplinit))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 tstForm->tmplinit));
	else
		append_bool_object(tmp, "present", false);
	list = lappend(list, new_object_object(tmp));

	/* LEXIZE */
	tmp = new_objtree_VA("LEXIZE=", 1,
						 "clause", ObjTypeString, "lexize");
	append_object_object(tmp, "%{procedure}D",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tstForm->tmpllexize));
	list = lappend(list, new_object_object(tmp));

	append_array_object(stmt, "(%{elems:, }s)", list);

	ReleaseSysCache(tstTup);

	return stmt;
}

static ObjTree *
deparse_AlterTSConfigurationStmt(CollectedCommand *cmd)
{
	AlterTSConfigurationStmt *node = (AlterTSConfigurationStmt *) cmd->parsetree;
	ObjTree *config;
	ObjTree *tmp;
	List	   *list;
	ListCell   *cell;
	int			i;

	/*
	 * Verbose syntax
	 * case ALTER_TSCONFIG_ADD_MAPPING:
	 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ADD MAPPING FOR %{tokentype:, }I WITH %{dictionaries:, }D
	 *
	 * case ALTER_TSCONFIG_DROP_MAPPING:
	 * ALTER TEXT SEARCH CONFIGURATION %{identity}D DROP MAPPING %{if_exists}s FOR %{tokentype}I
	 *
	 * case ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN:
	 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ALTER MAPPING FOR %{tokentype:, }I WITH %{dictionaries:, }D
	 *
	 * case ALTER_TSCONFIG_REPLACE_DICT:
	 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ALTER MAPPING REPLACE %{old_dictionary}D WITH %{new_dictionary}D
	 *
	 * case ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN:
	 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ALTER MAPPING FOR %{tokentype:, }I REPLACE %{old_dictionary}D WITH %{new_dictionary}D
	 */

	config = new_objtree("ALTER TEXT SEARCH CONFIGURATION");

	/* determine the format string appropriate to each subcommand */
	switch (node->kind)
	{
		case ALTER_TSCONFIG_ADD_MAPPING:
			append_object_object(config, "%{identity}D ADD MAPPING",
						 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
													 cmd->d.atscfg.address.objectId));
			break;

		case ALTER_TSCONFIG_DROP_MAPPING:
			append_object_object(config, "%{identity}D DROP MAPPING",
								 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
															 cmd->d.atscfg.address.objectId));
			tmp = new_objtree_VA("IF EXISTS", 0);
			append_bool_object(tmp, "present", node->missing_ok);
			append_object_object(config, "%{if_exists}s", tmp);
			break;

		case ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN:
			append_object_object(config, "%{identity}D ALTER MAPPING",
								 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
															 cmd->d.atscfg.address.objectId));
			break;

		case ALTER_TSCONFIG_REPLACE_DICT:
			append_object_object(config, "%{identity}D ALTER MAPPING",
								 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
															 cmd->d.atscfg.address.objectId));
			break;

		case ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN:
			append_object_object(config, "%{identity}D ALTER MAPPING",
								 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
															 cmd->d.atscfg.address.objectId));
			break;
	}

	/* Add the affected token list, for subcommands that have one */
	if (node->kind == ALTER_TSCONFIG_ADD_MAPPING ||
		node->kind == ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN ||
		node->kind == ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN ||
		node->kind == ALTER_TSCONFIG_DROP_MAPPING)
	{
		list = NIL;
		foreach(cell, node->tokentype)
			list = lappend(list, new_string_object(strVal(lfirst(cell))));
		append_array_object(config, "FOR %{tokentype:, }I", list);
	}

	/* add further subcommand-specific elements */
	if (node->kind == ALTER_TSCONFIG_ADD_MAPPING ||
		node->kind == ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN)
	{
		/* ADD MAPPING and ALTER MAPPING FOR need a list of dictionaries */
		list = NIL;
		for (i = 0; i < cmd->d.atscfg.ndicts; i++)
		{
			ObjTree	*dictobj;

			dictobj = new_objtree_for_qualname_id(TSDictionaryRelationId,
												  cmd->d.atscfg.dictIds[i]);
			list = lappend(list,
						   new_object_object(dictobj));
		}
		append_array_object(config, "WITH %{dictionaries:, }D", list);
	}
	else if (node->kind == ALTER_TSCONFIG_REPLACE_DICT ||
			 node->kind == ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN)
	{
		/* the REPLACE forms want old and new dictionaries */
		Assert(cmd->d.atscfg.ndicts == 2);
		append_object_object(config, "REPLACE %{old_dictionary}D",
							 new_objtree_for_qualname_id(TSDictionaryRelationId,
														 cmd->d.atscfg.dictIds[0]));
		append_object_object(config, "WITH %{new_dictionary}D",
							 new_objtree_for_qualname_id(TSDictionaryRelationId,
														 cmd->d.atscfg.dictIds[1]));
	}

	return config;
}

static ObjTree *
deparse_AlterTSDictionaryStmt(Oid objectId, Node *parsetree)
{
	ObjTree *alterTSD;
	ObjTree *tmp;
	Datum		options;
	List	   *definition = NIL;
	bool		isnull;
	HeapTuple   tsdTup;
	Form_pg_ts_dict tsdForm;

	tsdTup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tsdTup))
		elog(ERROR, "cache lookup failed for text search dictionary "
			 "with OID %u", objectId);
	tsdForm = (Form_pg_ts_dict) GETSTRUCT(tsdTup);

	/*
	 * Verbose syntax
	 * ALTER TEXT SEARCH DICTIONARY %{identity}D (%{definition:, }s)
	 */

	alterTSD = new_objtree("ALTER TEXT SEARCH DICTIONARY");

	append_object_object(alterTSD, "%{identity}D",
						 new_objtree_for_qualname(tsdForm->dictnamespace,
												  NameStr(tsdForm->dictname)));

	/* Add the definition list according to the pg_ts_dict dictinitoption column */
	options = SysCacheGetAttr(TSDICTOID, tsdTup,
							  Anum_pg_ts_dict_dictinitoption,
							  &isnull);
	tmp = new_objtree_VA("", 0);
	if (!isnull)
		append_string_object(tmp, "%{options}s", TextDatumGetCString(options));
	else
		append_bool_object(tmp, "present", false);

	definition = lappend(definition, new_object_object(tmp));
	append_array_object(alterTSD, "(%{definition:, }s)", definition);
	ReleaseSysCache(tsdTup);

	return alterTSD;
}

/*
 * ... ALTER COLUMN ... SET/RESET (...)
 */
static ObjTree *
deparse_RelSetOptions(AlterTableCmd *subcmd)
{
	List	   *sets = NIL;
	ListCell   *cell;
	ObjTree	   *relset;
	char	   *fmt;
	bool		is_reset = subcmd->subtype == AT_ResetRelOptions;

	/*
	 * Verbose syntax
	 *
	 * RESET|SET (%{options:, }s)
	 */
	if (is_reset)
		fmt = "RESET ";
	else
		fmt = "SET ";

	relset = new_objtree(fmt);

	foreach(cell, (List *) subcmd->def)
	{
		DefElem	   *elem;
		ObjTree	   *set;

		elem = (DefElem *) lfirst(cell);
		set = deparse_DefElem(elem, is_reset);
		sets = lappend(sets, new_object_object(set));
	}

	Assert(sets);
	append_array_object(relset, "(%{options:, }s)", sets);

	return relset;
}

/*
 * Deparse a CreateTrigStmt (CREATE TRIGGER)
 *
 * Given a trigger OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateTrigStmt(Oid objectId, Node *parsetree)
{
	CreateTrigStmt *node = (CreateTrigStmt *) parsetree;
	Relation	pg_trigger;
	HeapTuple	trigTup;
	Form_pg_trigger trigForm;
	ObjTree	   *trigger;
	ObjTree	   *tmpobj;
	int			tgnargs;
	List	   *list;
	List	   *events;

	pg_trigger = table_open(TriggerRelationId, AccessShareLock);

	trigTup = get_catalog_object_by_oid(pg_trigger, Anum_pg_trigger_oid, objectId);
	trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE %{constraint}s TRIGGER %{name}I %{time}s %{events: OR }s
	 * ON %{relation}D %{from_table}s %{constraint_attrs: }s
	 * FOR EACH %{for_each}s %{when}s EXECUTE PROCEDURE %{function}s
	 */
	trigger = new_objtree("CREATE");

	append_string_object(trigger, "%{constraint}s",
						 node->isconstraint ? "CONSTRAINT" : "");

	append_string_object(trigger, "TRIGGER %{name}I", node->trigname);

	if (node->timing == TRIGGER_TYPE_BEFORE)
		append_string_object(trigger, "%{time}s", "BEFORE");
	else if (node->timing == TRIGGER_TYPE_AFTER)
		append_string_object(trigger, "%{time}s", "AFTER");
	else if (node->timing == TRIGGER_TYPE_INSTEAD)
		append_string_object(trigger, "%{time}s", "INSTEAD OF");
	else
		elog(ERROR, "unrecognized trigger timing type %d", node->timing);

	/*
	 * Decode the events that the trigger fires for.  The output is a list;
	 * in most cases it will just be a string with the event name, but when
	 * there's an UPDATE with a list of columns, we return a JSON object.
	 */
	events = NIL;
	if (node->events & TRIGGER_TYPE_INSERT)
		events = lappend(events, new_string_object("INSERT"));
	if (node->events & TRIGGER_TYPE_DELETE)
		events = lappend(events, new_string_object("DELETE"));
	if (node->events & TRIGGER_TYPE_TRUNCATE)
		events = lappend(events, new_string_object("TRUNCATE"));
	if (node->events & TRIGGER_TYPE_UPDATE)
	{
		if (node->columns == NIL)
		{
			events = lappend(events, new_string_object("UPDATE"));
		}
		else
		{
			ObjTree	   *update;
			ListCell   *cell;
			List	   *cols = NIL;

			/*
			 * Currently only UPDATE OF can be objects in the output JSON, but
			 * we add a "kind" element so that user code can distinguish
			 * possible future new event types.
			 */
			update = new_objtree_VA("UPDATE OF", 1,
									"kind", ObjTypeString, "update_of");

			foreach(cell, node->columns)
			{
				char   *colname = strVal(lfirst(cell));

				cols = lappend(cols, new_string_object(colname));
			}

			append_array_object(update, "%{columns:, }I", cols);

			events = lappend(events, new_object_object(update));
		}
	}
	append_array_object(trigger, "%{events: OR }s", events);

	tmpobj = new_objtree_for_qualname_id(RelationRelationId,
									  trigForm->tgrelid);
	append_object_object(trigger, "ON %{relation}D", tmpobj);

	tmpobj = new_objtree_VA("FROM", 0);
	if (trigForm->tgconstrrelid)
	{
		ObjTree	   *rel;

		rel = new_objtree_for_qualname_id(RelationRelationId,
										  trigForm->tgconstrrelid);
		append_object_object(tmpobj, "%{relation}D", rel);
	}
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(trigger, "%{from_table}s", tmpobj);

	list = NIL;
	if (node->deferrable)
		list = lappend(list,
					   new_string_object("DEFERRABLE"));
	if (node->initdeferred)
		list = lappend(list,
					   new_string_object("INITIALLY DEFERRED"));
	append_array_object(trigger, "%{constraint_attrs: }s", list);

	append_string_object(trigger, "FOR EACH %{for_each}s",
						 node->row ? "ROW" : "STATEMENT");

	tmpobj = new_objtree_VA("WHEN", 0);
	if (node->whenClause)
	{
		Node	   *whenClause;
		Datum		value;
		bool		isnull;

		value = fastgetattr(trigTup, Anum_pg_trigger_tgqual,
							RelationGetDescr(pg_trigger), &isnull);
		if (isnull)
			elog(ERROR, "bogus NULL tgqual");

		whenClause = stringToNode(TextDatumGetCString(value));
		append_string_object(tmpobj, "(%{clause}s)",
							 pg_get_trigger_whenclause(trigForm,
													   whenClause,
													   false));
	}
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(trigger, "%{when}s", tmpobj);

	tmpobj = new_objtree_VA("%{funcname}D", 1,
						 "funcname", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 trigForm->tgfoid));
	list = NIL;
	tgnargs = trigForm->tgnargs;
	if (tgnargs > 0)
	{
		bytea  *tgargs;
		char   *argstr;
		bool	isnull;
		int		findx;
		int		lentgargs;
		char   *p;

		tgargs = DatumGetByteaP(fastgetattr(trigTup,
											Anum_pg_trigger_tgargs,
											RelationGetDescr(pg_trigger),
											&isnull));
		if (isnull)
			elog(ERROR, "invalid NULL tgargs");
		argstr = (char *) VARDATA(tgargs);
		lentgargs = VARSIZE_ANY_EXHDR(tgargs);

		p = argstr;
		for (findx = 0; findx < tgnargs; findx++)
		{
			size_t	tlen;

			/* Verify that the argument encoding is correct */
			tlen = strlen(p);
			if (p + tlen >= argstr + lentgargs)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid argument string (%s) for trigger \"%s\"",
								argstr, NameStr(trigForm->tgname))));

			list = lappend(list, new_string_object(p));

			p += tlen + 1;
		}
	}

	append_format_string(tmpobj, "(");
	append_array_object(tmpobj, "%{args:, }L", list);		/* might be NIL */
	append_format_string(tmpobj, ")");

	append_object_object(trigger, "EXECUTE PROCEDURE %{function}s", tmpobj);

	table_close(pg_trigger, AccessShareLock);

	return trigger;
}

/*
 * Deparse a CreateUserMappingStmt (CREATE USER MAPPING)
 *
 * Given a User Mapping OID and the parsetree that created it,
 * return an ObjTree representing the CREATE USER MAPPING command.
 */
static ObjTree *
deparse_CreateUserMappingStmt(Oid objectId, Node *parsetree)
{
	CreateUserMappingStmt *node = (CreateUserMappingStmt *) parsetree;
	ObjTree	   *createStmt;
	Relation	rel;
	HeapTuple	tp;
	Form_pg_user_mapping form;
	ForeignServer *server;

	rel = table_open(UserMappingRelationId, RowExclusiveLock);

	/*
	 * Lookup up object in the catalog, so we don't have to deal with
	 * current_user and such.
	 */
	tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for user mapping %u", objectId);

	form = (Form_pg_user_mapping) GETSTRUCT(tp);

	server = GetForeignServer(form->umserver);

	createStmt = new_objtree("CREATE USER MAPPING ");

	append_object_object(createStmt, "FOR %{role}R", new_objtree_for_role_id(form->umuser));

	append_string_object(createStmt, "SERVER %{server}I", server->servername);

	/* add an OPTIONS clause, if any */
	append_object_object(createStmt, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(tp);
	table_close(rel, RowExclusiveLock);
	return createStmt;
}

/*
 * deparse_AlterUserMapping
 *
 * Given a User Mapping OID and the parsetree that created it,
 * return the JSON blob representing the alter command.
 */
static ObjTree *
deparse_AlterUserMappingStmt(Oid objectId, Node *parsetree)
{
	AlterUserMappingStmt *node = (AlterUserMappingStmt *) parsetree;
	ObjTree	   *alterStmt;
	Relation	rel;
	HeapTuple	tp;
	Form_pg_user_mapping form;
	ForeignServer *server;

	rel = table_open(UserMappingRelationId, RowExclusiveLock);

	/*
	 * Lookup up object in the catalog, so we don't have to deal with
	 * current_user and such.
	 */

	tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for user mapping %u", objectId);

	form = (Form_pg_user_mapping) GETSTRUCT(tp);

	/*
	 * Lookup up object in the catalog, so we don't have to deal with
	 * current_user and such.
	 */

	tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for user mapping %u", objectId);

	form = (Form_pg_user_mapping) GETSTRUCT(tp);


	server = GetForeignServer(form->umserver);

	alterStmt = new_objtree("ALTER USER MAPPING");

	append_object_object(alterStmt, "FOR %{role}R", new_objtree_for_role_id(form->umuser));

	append_string_object(alterStmt, "SERVER %{server}I", server->servername);

	/* add an OPTIONS clause, if any */
	append_object_object(alterStmt, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(tp);
	table_close(rel, RowExclusiveLock);
	return alterStmt;
}

/*
 * Deparse a RefreshMatViewStmt (REFRESH MATERIALIZED VIEW)
 *
 * Given a materialized view OID and the parsetree that created it, return an
 * ObjTree representing the refresh command.
 */
static ObjTree *
deparse_RefreshMatViewStmt(Oid objectId, Node *parsetree)
{
	RefreshMatViewStmt *node = (RefreshMatViewStmt *) parsetree;
	ObjTree	   *refreshStmt;
	ObjTree	   *tmp;

	refreshStmt = new_objtree_VA("REFRESH MATERIALIZED VIEW", 0);

	/* add a CONCURRENTLY clause */
	append_string_object(refreshStmt, "%{concurrently}s",
						 node->concurrent ? "CONCURRENTLY" : "");
	/* add the matview name */
	append_object_object(refreshStmt, "%{identity}D",
						 new_objtree_for_qualname_id(RelationRelationId,
													 objectId));
	/* add a WITH NO DATA clause */
	tmp = new_objtree_VA("WITH NO DATA", 1,
						 "present", ObjTypeBool,
						 node->skipData ? true : false);
	append_object_object(refreshStmt, "%{with_no_data}s", tmp);

	return refreshStmt;
}

/*
 * Deparse DefElems, as used e.g. by ALTER COLUMN ... SET, into a list of SET
 * (...)  or RESET (...) contents.
 */
static ObjTree *
deparse_DefElem(DefElem *elem, bool is_reset)
{
	ObjTree	   *set;
	ObjTree	   *optname;

	set = new_objtree("");
	optname = new_objtree("");

	if (elem->defnamespace != NULL)
		append_string_object(optname, "%{schema}I.", elem->defnamespace);

	append_string_object(optname, "%{label}I", elem->defname);

	append_object_object(set, "%{label}s", optname);

	if (!is_reset)
		append_string_object(set, " = %{value}L",
							 elem->arg ? defGetString(elem) :
							 defGetBoolean(elem) ? "TRUE" : "FALSE");

	return set;
}

/*
 * Handle deparsing of DROP commands.
 */
char *
deparse_drop_command(const char *objidentity, const char *objecttype,
					 DropBehavior behavior)
{
	StringInfoData  str;
	char		   *command;
	char		   *identity = (char *) objidentity;
	char		   *fmt;
	ObjTree		*stmt, *stmt2;
	Jsonb		  *jsonb;

	initStringInfo(&str);

	fmt = psprintf("DROP %s IF EXISTS %%{objidentity}s", objecttype);

	stmt = new_objtree_VA(fmt, 1, "objidentity", ObjTypeString, identity);
	stmt2 = new_objtree_VA("CASCADE", 1,
						   "present", ObjTypeBool, behavior == DROP_CASCADE);

	append_object_object(stmt, "%{cascade}s", stmt2);

	jsonb = objtree_to_jsonb(stmt);
	command = JsonbToCString(&str, &jsonb->root, JSONB_ESTIMATED_LEN);

	return command;
}

/*
 * Handle deparsing setting of Function
 */
static ObjTree *
deparse_FunctionSet(VariableSetKind kind, char *name, char *value)
{
	ObjTree	   *obj;

	if (kind == VAR_RESET_ALL)
	{
		obj = new_objtree("RESET ALL");
	}
	else if (value != NULL)
	{
		obj = new_objtree_VA("SET %{set_name}I", 1,
						   "set_name", ObjTypeString, name);

		/*
		 * Some GUC variable names are 'LIST' type and hence must not be
		 * quoted.
		 */
		if (GetConfigOptionFlags(name, true) & GUC_LIST_QUOTE)
			append_string_object(obj, "TO %{set_value}s", value);
		else
			append_string_object(obj, "TO %{set_value}L", value);
	}
	else
	{
		obj = new_objtree("RESET");
		append_string_object(obj, "%{set_name}I", name);
	}

	return obj;
}

/*
 * Deparse an IndexStmt.
 *
 * Given an index OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 *
 * If the index corresponds to a constraint, NULL is returned.
 */
static ObjTree *
deparse_IndexStmt(Oid objectId, Node *parsetree)
{
	IndexStmt  *node = (IndexStmt *) parsetree;
	ObjTree	   *indexStmt;
	ObjTree	   *tmpobj;
	Relation	idxrel;
	Relation	heaprel;
	char	   *index_am;
	char	   *definition;
	char	   *reloptions;
	char	   *tablespace;
	char	   *whereClause;

	if (node->primary || node->isconstraint)
	{
		/*
		 * Indexes for PRIMARY KEY and other constraints are output
		 * separately; return empty here.
		 */
		return NULL;
	}

	idxrel = relation_open(objectId, AccessShareLock);
	heaprel = relation_open(idxrel->rd_index->indrelid, AccessShareLock);

	pg_get_indexdef_detailed(objectId,
							 &index_am, &definition, &reloptions,
							 &tablespace, &whereClause);

	/*
	 * Verbose syntax
	 *
	 * CREATE %{unique}s INDEX %{concurrently}s %{if_not_exists}s %{name}I ON
	 * %{table}D USING %{index_am}s (%{definition}s) %{with}s %{tablespace}s
	 * %{where_clause}s
	 */
	indexStmt = new_objtree("CREATE");

	append_string_object(indexStmt, "%{unique}s",
						 node->unique ? "UNIQUE" : "");

	append_format_string(indexStmt, "INDEX");

	append_string_object(indexStmt, "%{concurrently}s",
						 node->concurrent ? "CONCURRENTLY" : "");

	append_string_object(indexStmt, "%{if_not_exists}s",
						 node->if_not_exists ? "IF NOT EXISTS" : "");

	append_string_object(indexStmt, "%{name}I",
						 RelationGetRelationName(idxrel));

	append_object_object(indexStmt, "ON %{table}D",
						 new_objtree_for_qualname(heaprel->rd_rel->relnamespace,
						 RelationGetRelationName(heaprel)));

	append_string_object(indexStmt, "USING %{index_am}s", index_am);

	append_string_object(indexStmt, "(%{definition}s)", definition);

	/* reloptions */
	tmpobj = new_objtree("WITH");
	if (reloptions)
		append_string_object(tmpobj, "(%{opts}s)", reloptions);
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(indexStmt, "%{with}s", tmpobj);

	/* tablespace */
	tmpobj = new_objtree("TABLESPACE");
	if (tablespace)
		append_string_object(tmpobj, "%{tablespace}s", tablespace);
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(indexStmt, "%{tablespace}s", tmpobj);

	/* WHERE clause */
	tmpobj = new_objtree("WHERE");
	if (whereClause)
		append_string_object(tmpobj, "%{where}s", whereClause);
	else
		append_bool_object(tmpobj, "present", false);
	append_object_object(indexStmt, "%{where_clause}s", tmpobj);

	table_close(idxrel, AccessShareLock);
	table_close(heaprel, AccessShareLock);

	return indexStmt;
}

/*
 * Deparse the INHERITS relations.
 *
 * Given a table OID, return a schema qualified table list representing
 * the parent tables.
 */
static List *
deparse_InhRelations(Oid objectId)
{
	List	   *parents = NIL;
	Relation	inhRel;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;

	inhRel = table_open(InheritsRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId,
							  true, NULL, 1, &key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		ObjTree	*parent;
		Form_pg_inherits formInh = (Form_pg_inherits) GETSTRUCT(tuple);

		parent = new_objtree_for_qualname_id(RelationRelationId,
											 formInh->inhparent);
		parents = lappend(parents, new_object_object(parent));
	}

	systable_endscan(scan);
	table_close(inhRel, RowExclusiveLock);

	return parents;
}

/*
 * Deparse the ON COMMMIT ... clause for CREATE ... TEMPORARY ...
 */
static ObjTree *
deparse_OnCommitClause(OnCommitAction option)
{
	ObjTree	   *oncommit;

	oncommit = new_objtree("ON COMMIT");
	switch (option)
	{
		case ONCOMMIT_DROP:
			append_string_object(oncommit, "%{on_commit_value}s", "DROP");
			break;

		case ONCOMMIT_DELETE_ROWS:
			append_string_object(oncommit, "%{on_commit_value}s", "DELETE ROWS");
			break;

		case ONCOMMIT_PRESERVE_ROWS:
			append_string_object(oncommit, "%{on_commit_value}s", "PRESERVE ROWS");
			break;

		case ONCOMMIT_NOOP:
			append_null_object(oncommit, "%{on_commit_value}s");
			append_bool_object(oncommit, "present", false);
			break;
	}

	return oncommit;
}

/*
 * Deparse a RenameStmt.
 */
static ObjTree *
deparse_RenameStmt(ObjectAddress address, Node *parsetree)
{
	RenameStmt *node = (RenameStmt *) parsetree;
	ObjTree	   *renameStmt;
	char	   *fmtstr;
	const char *objtype;
	Relation	relation;
	Oid			schemaId;

	/*
	 * In an ALTER .. RENAME command, we don't have the original name of the
	 * object in system catalogs: since we inspect them after the command has
	 * executed, the old name is already gone.  Therefore, we extract it from
	 * the parse node.  Note we still extract the schema name from the catalog
	 * (it might not be present in the parse node); it cannot possibly have
	 * changed anyway.
	 */
	switch (node->renameType)
	{
		case OBJECT_TABLE:
		case OBJECT_INDEX:
		case OBJECT_SEQUENCE:

			/*
			 * Verbose syntax
			 *
			 * ALTER %s %{if_exists}s %{identity}D RENAME TO %{newname}I
			 */
			objtype = stringify_objtype(node->renameType);
			fmtstr = psprintf("ALTER %s", objtype);
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);
			renameStmt = new_objtree_VA(fmtstr, 0);
			append_string_object(renameStmt, "%{if_exists}s",
								 node->missing_ok ? "IF EXISTS" : "");
			append_object_object(renameStmt, "%{identity}D",
								 new_objtree_for_qualname(schemaId,
														  node->relation->relname));
			append_string_object(renameStmt, "RENAME TO %{newname}I",
								 node->newname);
			relation_close(relation, AccessShareLock);
			break;

		case OBJECT_POLICY:
			{
				HeapTuple	polTup;
				Form_pg_policy polForm;
				Relation	pg_policy;
				ScanKeyData	key;
				SysScanDesc	scan;

				pg_policy = relation_open(PolicyRelationId, AccessShareLock);
				ScanKeyInit(&key, Anum_pg_policy_oid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(address.objectId));
				scan = systable_beginscan(pg_policy, PolicyOidIndexId, true,
										  NULL, 1, &key);
				polTup = systable_getnext(scan);
				if (!HeapTupleIsValid(polTup))
					elog(ERROR, "cache lookup failed for policy %u", address.objectId);
				polForm = (Form_pg_policy) GETSTRUCT(polTup);

				renameStmt = new_objtree_VA("ALTER POLICY", 0);
				append_string_object(renameStmt, "%{if_exists}s",
									 node->missing_ok ? "IF EXISTS" : "");
				append_string_object(renameStmt, "%{policyname}I", node->subname);
				append_object_object(renameStmt, "ON %{identity}D",
									 new_objtree_for_qualname_id(RelationRelationId,
									 polForm->polrelid));
				append_string_object(renameStmt, "RENAME TO %{newname}I",
									 node->newname);
				systable_endscan(scan);
				relation_close(pg_policy, AccessShareLock);

			}
			break;

		case OBJECT_ATTRIBUTE:
		case OBJECT_COLUMN:
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);

			/*
			 * Verbose syntax
			 *
			 * Composite types: ALTER objtype %{identity}D RENAME ATTRIBUTE
			 * %{colname}I TO %{newname}I %{cascade}s
			 *
			 * Normal types: ALTER objtype %{if_exists}s %%{identity}D RENAME
			 * COLUMN %{colname}I TO %{newname}I
			 */

			if (node->renameType == OBJECT_ATTRIBUTE)
			{
				renameStmt = new_objtree("ALTER TYPE");
						append_object_object(renameStmt, "%{identity}D",
								 new_objtree_for_qualname(schemaId,
														  node->relation->relname));
				fmtstr = psprintf("RENAME ATTRIBUTE %%{colname}I");
			}
			else
			{
				objtype = stringify_objtype(node->relationType);
				fmtstr = psprintf("ALTER %s", objtype);
				renameStmt = new_objtree(fmtstr);

				/* Composite types do not support IF EXISTS */
				if (node->renameType == OBJECT_COLUMN)
					append_string_object(renameStmt, "%{if_exists}s",
										node->missing_ok ? "IF EXISTS" : "");

				append_object_object(renameStmt, "%{identity}D",
									 new_objtree_for_qualname(schemaId,
															  node->relation->relname));
				fmtstr = psprintf("RENAME COLUMN %%{colname}I");
			}

			append_string_object(renameStmt, fmtstr, node->subname);
			append_string_object(renameStmt, "TO %{newname}I", node->newname);

			if (node->renameType == OBJECT_ATTRIBUTE)
				append_object_object(renameStmt, "%{cascade}s",
									 new_objtree_VA("CASCADE", 1,
													"present", ObjTypeBool,
													node->behavior == DROP_CASCADE));

			relation_close(relation, AccessShareLock);

			break;
		case OBJECT_FUNCTION:
			{
				char	*ident;
				HeapTuple proctup;
				Form_pg_proc procform;
				List	   *identity;

				Assert(IsA(node->object, ObjectWithArgs));
				identity = ((ObjectWithArgs *) node->object)->objname;

				proctup = SearchSysCache1(PROCOID,
										  ObjectIdGetDatum(address.objectId));
				if (!HeapTupleIsValid(proctup))
					elog(ERROR, "cache lookup failed for procedure %u",
						 address.objectId);
				procform = (Form_pg_proc) GETSTRUCT(proctup);

				/* XXX does this work for ordered-set aggregates? */
				ident = psprintf("%s%s",
								 quote_qualified_identifier(get_namespace_name(procform->pronamespace),
															strVal(llast(identity))),
								 format_procedure_args(address.objectId, true));

				fmtstr = psprintf("ALTER %s %%{identity}s RENAME TO %%{newname}I",
								  stringify_objtype(node->renameType));
				renameStmt = new_objtree_VA(fmtstr, 2,
											"identity", ObjTypeString, ident,
											"newname", ObjTypeString, node->newname);

				ReleaseSysCache(proctup);
			}
			break;

		case OBJECT_OPCLASS:
			{
				HeapTuple   opcTup;
				Form_pg_opclass opcForm;
				List	   *oldnames;
				char	   *schemaname;
				char	   *opcname;
				char	   *fmt;

				fmt = psprintf("ALTER %s %%{identity}D USING %%{index_method}s RENAME TO %%{newname}I",
							   stringify_objtype(node->renameType));

				opcTup = SearchSysCache1(CLAOID, ObjectIdGetDatum(address.objectId));
				if (!HeapTupleIsValid(opcTup))
					elog(ERROR, "cache lookup failed for opclass with OID %u",
						 address.objectId);

				opcForm = (Form_pg_opclass) GETSTRUCT(opcTup);

				oldnames = list_copy_tail((List *) node->object, 1);

				/* deconstruct the name list */
				DeconstructQualifiedName(oldnames, &schemaname, &opcname);

				renameStmt = new_objtree_VA(fmt, 3,
											"identity", ObjTypeObject,
											new_objtree_for_qualname(opcForm->opcnamespace,
																	 opcname),
											"index_method", ObjTypeString,
											get_am_name(opcForm->opcmethod),
											"newname", ObjTypeString, node->newname);

				ReleaseSysCache(opcTup);
			}
			break;

		case OBJECT_OPFAMILY:
			{
				HeapTuple   opfTup;
				HeapTuple   amTup;
				Form_pg_opfamily opfForm;
				Form_pg_am  amForm;
				List	   *oldnames;
				char	   *schemaname;
				char	   *opfname;
				char	   *fmt;

				fmt = psprintf("ALTER %s %%{identity}D USING %%{index_method}s RENAME TO %%{newname}I",
							   stringify_objtype(node->renameType));

				opfTup = SearchSysCache1(OPFAMILYOID, address.objectId);
				if (!HeapTupleIsValid(opfTup))
					elog(ERROR, "cache lookup failed for operator family with OID %u",
						 address.objectId);
				opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

				amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
				if (!HeapTupleIsValid(amTup))
					elog(ERROR, "cache lookup failed for access method %u",
						 opfForm->opfmethod);

				amForm = (Form_pg_am) GETSTRUCT(amTup);

				oldnames = list_copy_tail((List *) node->object, 1);

				/* deconstruct the name list */
				DeconstructQualifiedName(oldnames, &schemaname, &opfname);

				renameStmt = new_objtree_VA(fmt, 3,
											"identity", ObjTypeObject,
											new_objtree_for_qualname(opfForm->opfnamespace,
																	 opfname),
											"index_method", ObjTypeString,
											NameStr(amForm->amname),
											"newname", ObjTypeString, node->newname);

				ReleaseSysCache(amTup);
				ReleaseSysCache(opfTup);
			}
			break;

		case OBJECT_SCHEMA:
			renameStmt =
				new_objtree_VA("ALTER SCHEMA %{identity}I RENAME TO %{newname}I", 2,
							   "identity", ObjTypeString, node->subname,
							   "newname", ObjTypeString, node->newname);
			break;

		case OBJECT_FDW:
		case OBJECT_FOREIGN_SERVER:
			{
				String *identity = castNode(String, node->object);

				fmtstr = psprintf("ALTER %s %%{identity}s RENAME TO %%{newname}I",
												stringify_objtype(node->renameType));
				renameStmt =
					new_objtree_VA(fmtstr, 2,
								   "identity", ObjTypeString, strVal(identity),
								   "newname", ObjTypeString, node->newname);
			}
			break;

		case OBJECT_RULE:
			{
				HeapTuple	rewrTup;
				Form_pg_rewrite rewrForm;
				Relation	pg_rewrite;

				pg_rewrite = relation_open(RewriteRelationId, AccessShareLock);
				rewrTup = get_catalog_object_by_oid(pg_rewrite, Anum_pg_rewrite_oid,  address.objectId);
				rewrForm = (Form_pg_rewrite) GETSTRUCT(rewrTup);

				renameStmt = new_objtree_VA("ALTER RULE %{rulename}I ON %{identity}D RENAME TO %{newname}I",
											3,
											"rulename", ObjTypeString, node->subname,
											"identity", ObjTypeObject,
											new_objtree_for_qualname_id(RelationRelationId,
																		rewrForm->ev_class),
											"newname", ObjTypeString, node->newname);
				relation_close(pg_rewrite, AccessShareLock);
			}
			break;

		case OBJECT_TRIGGER:
			{
				HeapTuple	trigTup;
				Form_pg_trigger trigForm;
				Relation	pg_trigger;

				pg_trigger = relation_open(TriggerRelationId, AccessShareLock);
				trigTup = get_catalog_object_by_oid(pg_trigger, get_object_attnum_oid(address.classId), address.objectId);
				trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

				renameStmt = new_objtree_VA("ALTER TRIGGER %{triggername}I ON %{identity}D RENAME TO %{newname}I",
											3,
											"triggername", ObjTypeString, node->subname,
											"identity", ObjTypeObject,
											new_objtree_for_qualname_id(RelationRelationId,
																		trigForm->tgrelid),
											"newname", ObjTypeString, node->newname);

				relation_close(pg_trigger, AccessShareLock);
			}
			break;

		case OBJECT_COLLATION:
		case OBJECT_TYPE:
		case OBJECT_CONVERSION:
		case OBJECT_DOMAIN:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TSPARSER:
		case OBJECT_TSTEMPLATE:
		case OBJECT_TSCONFIGURATION:
			{
				HeapTuple	objTup;
				Relation	catalog;
				Datum		objnsp;
				bool		isnull;
				AttrNumber	Anum_namespace;
				List	   *identity = castNode(List, node->object);
				char	   *fmtstring;

				/* obtain object tuple */
				catalog = relation_open(address.classId, AccessShareLock);
				objTup = get_catalog_object_by_oid(catalog, get_object_attnum_oid(address.classId), address.objectId);

				/* obtain namespace */
				Anum_namespace = get_object_attnum_namespace(address.classId);
				objnsp = heap_getattr(objTup, Anum_namespace,
									  RelationGetDescr(catalog), &isnull);
				if (isnull)
					elog(ERROR, "invalid NULL namespace");

				objtype = stringify_objtype(node->renameType);
				fmtstring = psprintf("ALTER %s", objtype);

				renameStmt = new_objtree_VA(fmtstring,
										0);
				append_object_object(renameStmt, "%{identity}D",
									new_objtree_for_qualname(DatumGetObjectId(objnsp),
															  strVal(llast(identity))));

				append_string_object(renameStmt, "RENAME TO %{newname}I",
									 node->newname);
				relation_close(catalog, AccessShareLock);
			}
			break;

		default:
			elog(ERROR, "unsupported object type %d", node->renameType);
	}

	return renameStmt;
}

/*
 * Deparse a AlterObjectDependsStmt (ALTER ... DEPENDS ON ...).
 */
static ObjTree *
deparse_AlterDependStmt(Oid objectId, Node *parsetree)
{
	AlterObjectDependsStmt *node = (AlterObjectDependsStmt *) parsetree;
	ObjTree		   *alterDependeStmt = NULL;


	if (node->objectType == OBJECT_INDEX)
	{
		Relation        relation = relation_open(objectId, AccessShareLock);
		ObjTree *qualified;

		alterDependeStmt = new_objtree("ALTER INDEX");

		qualified = new_objtree_for_qualname(relation->rd_rel->relnamespace,
											 node->relation->relname);
		append_object_object(alterDependeStmt, "%{identity}D", qualified);
		relation_close(relation, AccessShareLock);

		append_string_object(alterDependeStmt, "%{NO}s",
						 node->remove ? "NO" : "");

		append_format_string(alterDependeStmt, "DEPENDS ON EXTENSION");
		append_string_object(alterDependeStmt, "%{newname}I", strVal(node->extname));
	}
	else
		elog(LOG, "unrecognized node type in deparse command: %d",
			 (int) nodeTag(parsetree));

	return alterDependeStmt;
}

/*
 * Deparse the sequence CACHE option.
 */
static inline ObjElem *
deparse_Seq_Cache(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree	   *cache;
	char	   *tmpstr;
	char	   *fmt;

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqcache);

	if (alter_table)
		fmt = "SET CACHE %{value}s";
	else
		fmt = "CACHE %{value}s";

	cache = new_objtree_VA(fmt, 2,
						  "clause", ObjTypeString, "cache",
						  "value", ObjTypeString, tmpstr);

	return new_object_object(cache);
}

/*
 * Deparse the sequence CYCLE option.
 */
static inline ObjElem *
deparse_Seq_Cycle(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree	   *cycle;
	char	   *fmt;

	if (alter_table)
		fmt = "SET %{no}s CYCLE";
	else
		fmt = "%{no}s CYCLE";

	cycle = new_objtree_VA(fmt, 2,
						   "clause", ObjTypeString, "cycle",
						   "no", ObjTypeString,
						   seqdata->seqcycle ? "" : "NO");
	return new_object_object(cycle);
}

/*
 * Deparse the sequence INCREMENT BY option.
 */
static inline ObjElem *
deparse_Seq_IncrementBy(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree	   *incrementalby;
	char	   *tmpstr;
	char	   *fmt;

	if (alter_table)
		fmt = "SET INCREMENT BY %{value}s";
	else
		fmt = "INCREMENT BY %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqincrement);
	incrementalby = new_objtree_VA(fmt, 2,
								   "clause", ObjTypeString, "seqincrement",
								   "value", ObjTypeString, tmpstr);
	return new_object_object(incrementalby);
}

/*
 * Deparse the sequence MAXVALUE option.
 */
static inline ObjElem *
deparse_Seq_Maxvalue(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree	   *maxvalue;
	char	   *tmpstr;
	char	   *fmt;

	if (alter_table)
		fmt = "SET MAXVALUE %{value}s";
	else
		fmt = "MAXVALUE %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqmax);
	maxvalue = new_objtree_VA(fmt, 2,
							  "clause", ObjTypeString, "maxvalue",
							  "value", ObjTypeString, tmpstr);
	return new_object_object(maxvalue);
}

/*
 * Deparse the sequence MINVALUE option.
 */
static inline ObjElem *
deparse_Seq_Minvalue(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree	   *minvalue;
	char	   *tmpstr;
	char	   *fmt;

	if (alter_table)
		fmt = "SET MINVALUE %{value}s";
	else
		fmt = "MINVALUE %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqmin);
	minvalue = new_objtree_VA(fmt, 2,
							  "clause", ObjTypeString, "minvalue",
							  "value", ObjTypeString, tmpstr);
	return new_object_object(minvalue);
}

/*
 * Deparse the sequence OWNED BY command.
 */
static ObjElem *
deparse_Seq_OwnedBy(ObjTree *parent, Oid sequenceId, bool alter_table)
{
	ObjTree	   *ownedby = NULL;
	Relation	depRel;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple   tuple;

	depRel = table_open(DependRelationId, AccessShareLock);
	ScanKeyInit(&keys[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&keys[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sequenceId));
	ScanKeyInit(&keys[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(0));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 3, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Oid		 ownerId;
		Form_pg_depend depform;
		ObjTree	*tmpobj;
		char	   *colname;

		depform = (Form_pg_depend) GETSTRUCT(tuple);

		/* Only consider AUTO dependencies on pg_class */
		if (depform->deptype != DEPENDENCY_AUTO)
			continue;
		if (depform->refclassid != RelationRelationId)
			continue;
		if (depform->refobjsubid <= 0)
			continue;

		ownerId = depform->refobjid;
		colname = get_attname(ownerId, depform->refobjsubid, false);
		if (colname == NULL)
			continue;

		tmpobj = new_objtree_for_qualname_id(RelationRelationId, ownerId);
		append_string_object(tmpobj, "attrname", colname);
		ownedby = new_objtree_VA("OWNED BY %{owner}D",
								 2,
								 "clause", ObjTypeString, "owned",
								 "owner", ObjTypeObject, tmpobj);
	}

	systable_endscan(scan);
	relation_close(depRel, AccessShareLock);

	/*
	 * If there's no owner column, emit an empty OWNED BY element, set up so
	 * that it won't print anything.
	 */
	if (!ownedby)
		/* XXX this shouldn't happen ... */
		ownedby = new_objtree_VA("OWNED BY %{owner}D",
								 3,
								 "clause", ObjTypeString, "owned",
								 "owner", ObjTypeNull,
								 "present", ObjTypeBool, false);

	return new_object_object(ownedby);
}

/*
 * Deparse the sequence RESTART option.
 */
static inline ObjElem *
deparse_Seq_Restart(ObjTree *parent, Form_pg_sequence_data seqdata)
{
	ObjTree	   *restart;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->last_value);
	restart = new_objtree_VA("RESTART %{value}s", 2,
							 "clause", ObjTypeString, "restart",
							 "value", ObjTypeString, tmpstr);
	return new_object_object(restart);
}

/*
 * Deparse the sequence START WITH option.
 */
static inline ObjElem *
deparse_Seq_Startwith(ObjTree *parent, Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree	   *startwith;
	char	   *tmpstr;
	char	   *fmt;

	if (alter_table)
		fmt = "SET START WITH %{value}s";
	else
		fmt = "START WITH %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqstart);
	startwith = new_objtree_VA(fmt, 2,
							   "clause", ObjTypeString, "start",
							   "value", ObjTypeString, tmpstr);
	return new_object_object(startwith);
}

/*
 * Deparse the type STORAGE option.
 */
static inline ObjElem *
deparse_Type_Storage(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *storage;
	char	   *tmpstr;
	char	   *fmt;
	char	   *str;

	switch (typForm->typstorage)
	{
		case 'p':
			str = "plain";
			break;
		case 'e':
			str = "external";
			break;
		case 'x':
			str = "extended";
			break;
		case 'm':
			str = "main";
			break;
		default:
			elog(ERROR, "invalid storage specifier %c", typForm->typstorage);
	}

	tmpstr = psprintf("%s", str);

	fmt = "STORAGE = %{value}s";

	storage = new_objtree_VA(fmt, 2,
						  "clause", ObjTypeString, "storage",
						  "value", ObjTypeString, tmpstr);

	return new_object_object(storage);
}

/*
 * Deparse the type RECEIVE option.
 */
static inline ObjElem *
deparse_Type_Receive(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *receive;

	receive = new_objtree_VA("RECEIVE=", 1,
						 "clause", ObjTypeString, "receive");
	if (OidIsValid(typForm->typreceive))
		append_object_object(receive, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typreceive));
	else
		append_bool_object(receive, "present", false);

	return new_object_object(receive);
}

/*
 * Deparse the type SEND option.
 */
static inline ObjElem *
deparse_Type_Send(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *send;

	send = new_objtree_VA("SEND=", 1,
						 "clause", ObjTypeString, "send");
	if (OidIsValid(typForm->typsend))
		append_object_object(send, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typsend));
	else
		append_bool_object(send, "present", false);

	return new_object_object(send);
}

/*
 * Deparse the type typmod_in option.
 */
static inline ObjElem *
deparse_Type_Typmod_In(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *typmodin;

	typmodin = new_objtree_VA("TYPMOD_IN=", 1,
						 "clause", ObjTypeString, "typmod_in");
	if (OidIsValid(typForm->typmodin))
		append_object_object(typmodin, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodin));
	else
		append_bool_object(typmodin, "present", false);

	return new_object_object(typmodin);
}


/*
 * Deparse the type typmod_out option.
 */
static inline ObjElem *
deparse_Type_Typmod_Out(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *typmodout;

	typmodout = new_objtree_VA("TYPMOD_OUT=", 1,
						 "clause", ObjTypeString, "typmod_out");
	if (OidIsValid(typForm->typmodout))
		append_object_object(typmodout, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodout));
	else
		append_bool_object(typmodout, "present", false);

	return new_object_object(typmodout);
}


/*
 * Deparse the type analyze option.
 */
static inline ObjElem *
deparse_Type_Analyze(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *typanalyze;

	typanalyze = new_objtree_VA("ANALYZE=", 1,
						 "clause", ObjTypeString, "analyze");
	if (OidIsValid(typForm->typanalyze))
		append_object_object(typanalyze, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typanalyze));
	else
		append_bool_object(typanalyze, "present", false);

	return new_object_object(typanalyze);
}

/*
 * Deparse the type subscript option.
 */
static inline ObjElem *
deparse_Type_Subscript(ObjTree *parent, Form_pg_type typForm)
{
	ObjTree	   *typsubscript;

	typsubscript = new_objtree_VA("SUBSCRIPT=", 1,
						 "clause", ObjTypeString, "subscript");
	if (OidIsValid(typForm->typsubscript))
		append_object_object(typsubscript, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typsubscript));
	else
		append_bool_object(typsubscript, "present", false);

	return new_object_object(typsubscript);
}

/*
 * Deparse a RuleStmt (CREATE RULE).
 *
 * Given a rule OID and the parsetree that created it, return an ObjTree
 * representing the rule command.
 */
static ObjTree *
deparse_RuleStmt(Oid objectId, Node *parsetree)
{
	RuleStmt *node = (RuleStmt *) parsetree;
	ObjTree	   *ruleStmt;
	ObjTree	   *tmp;
	Relation	pg_rewrite;
	Form_pg_rewrite rewrForm;
	HeapTuple	rewrTup;
	SysScanDesc	scan;
	ScanKeyData	key;
	Datum		ev_qual;
	Datum		ev_actions;
	bool		isnull;
	char	   *qual;
	List	   *actions;
	List	   *list;
	ListCell   *cell;

	pg_rewrite = table_open(RewriteRelationId, AccessShareLock);
	ScanKeyInit(&key,
				Anum_pg_rewrite_oid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(objectId));

	scan = systable_beginscan(pg_rewrite, RewriteOidIndexId, true,
							  NULL, 1, &key);
	rewrTup = systable_getnext(scan);
	if (!HeapTupleIsValid(rewrTup))
		elog(ERROR, "cache lookup failed for rewrite rule with oid %u",
			 objectId);

	rewrForm = (Form_pg_rewrite) GETSTRUCT(rewrTup);

	ruleStmt = new_objtree("CREATE RULE");

	append_string_object(ruleStmt, "%{or_replace}s",
						 node->replace ? "OR REPLACE" : "");

	append_string_object(ruleStmt, "%{identity}I",
						 node->rulename);

	append_string_object(ruleStmt, "AS ON %{event}s",
						 node->event == CMD_SELECT ? "SELECT" :
						 node->event == CMD_UPDATE ? "UPDATE" :
						 node->event == CMD_DELETE ? "DELETE" :
						 node->event == CMD_INSERT ? "INSERT" : "XXX");
	append_object_object(ruleStmt, "TO %{table}D",
						 new_objtree_for_qualname_id(RelationRelationId,
													 rewrForm->ev_class));

	append_string_object(ruleStmt, "DO %{instead}s",
						 node->instead ? "INSTEAD" : "ALSO");

	ev_qual = heap_getattr(rewrTup, Anum_pg_rewrite_ev_qual,
						   RelationGetDescr(pg_rewrite), &isnull);
	ev_actions = heap_getattr(rewrTup, Anum_pg_rewrite_ev_action,
							  RelationGetDescr(pg_rewrite), &isnull);

	pg_get_ruledef_detailed(ev_qual, ev_actions, &qual, &actions);

	tmp = new_objtree_VA("WHERE %{clause}s", 0);

	if (qual)
		append_string_object(tmp, "clause", qual);
	else
	{
		append_null_object(tmp, "clause");
		append_bool_object(tmp, "present", false);
	}

	append_object_object(ruleStmt, "where_clause", tmp);

	list = NIL;
	foreach(cell, actions)
	{
		char *action = lfirst(cell);

		list = lappend(list, new_string_object(action));
	}
	append_array_object(ruleStmt, "%{actions:, }s", list);

	systable_endscan(scan);
	table_close(pg_rewrite, AccessShareLock);

	return ruleStmt;
}

/*
 * Deparse a CreateTransformStmt (CREATE TRANSFORM).
 *
 * Given a transform OID and the parsetree that created it, return an ObjTree
 * representing the CREATE TRANSFORM command.
 */
static ObjTree *
deparse_CreateTransformStmt(Oid objectId, Node *parsetree)
{
	CreateTransformStmt *node = (CreateTransformStmt *) parsetree;
	ObjTree	*createTransform;
	ObjTree *sign;
	HeapTuple trfTup;
	HeapTuple langTup;
	HeapTuple procTup;
	Form_pg_transform trfForm;
	Form_pg_language langForm;
	Form_pg_proc procForm;
	int i;

	/* Get the pg_transform tuple */
	trfTup = SearchSysCache1(TRFOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(trfTup))
		elog(ERROR, "cache lookup failure for transform with OID %u",
			 objectId);
	trfForm = (Form_pg_transform) GETSTRUCT(trfTup);

	/* Get the corresponding pg_language tuple */
	langTup = SearchSysCache1(LANGOID, trfForm->trflang);
	if (!HeapTupleIsValid(langTup))
		elog(ERROR, "cache lookup failure for language with OID %u",
			 trfForm->trflang);
	langForm = (Form_pg_language) GETSTRUCT(langTup);

	/*
	 * Verbose syntax
	 *
	 * CREATE %{or_replace}s TRANSFORM FOR %{typename}D LANGUAGE %{language}I
	 * ( FROM SQL WITH FUNCTION %{signature}s, TO SQL WITH FUNCTION %{signature_tof}s )
	 */
	createTransform = new_objtree("CREATE");

	append_string_object(createTransform, "%{or_replace}s",
						 node->replace ? "OR REPLACE" : "");
	append_object_object(createTransform, "TRANSFORM FOR %{typename}D",
						 new_objtree_for_qualname_id(TypeRelationId,
													 trfForm->trftype));
	append_string_object(createTransform, "LANGUAGE %{language}I",
						 NameStr(langForm->lanname));

	/* deparse the transform_element_list */
	if (trfForm->trffromsql != InvalidOid)
	{
		List *params = NIL;

		/*
		 * Verbose syntax
		 *
		 * CREATE %{or_replace}s TRANSFORM FOR %{typename}D LANGUAGE %{language}I
		 * ( FROM SQL WITH FUNCTION %{signature}s )
		 */

		/* Get the pg_proc tuple for the FROM FUNCTION */
		procTup = SearchSysCache1(PROCOID, trfForm->trffromsql);
		if (!HeapTupleIsValid(procTup))
			elog(ERROR, "cache lookup failure for function with OID %u",
				trfForm->trffromsql);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);

		/*
		 * CREATE TRANSFORM does not change function signature so we can use catalog
		 * to get input type Oids.
		 */
		for (i = 0; i < procForm->pronargs; i++)
		{
			ObjTree *paramobj = new_objtree("");

			append_object_object(paramobj, "%{type}T",
								 new_objtree_for_type(procForm->proargtypes.values[i], -1));
			params = lappend(params, new_object_object(paramobj));
		}

		sign = new_objtree("");

		append_object_object(sign, "%{identity}D",
							 new_objtree_for_qualname(procForm->pronamespace,
													  NameStr(procForm->proname)));
		append_array_object(sign, "(%{arguments:, }s)", params);

		append_object_object(createTransform, "(FROM SQL WITH FUNCTION %{signature}s", sign);
		ReleaseSysCache(procTup);
	}
	if (trfForm->trftosql != InvalidOid)
	{
		List *params = NIL;

		/*
		 * Verbose syntax
		 *
		 * CREATE %{or_replace}s TRANSFORM FOR %{typename}D LANGUAGE %{language}I
		 * ( FROM SQL WITH FUNCTION %{signature}s, TO SQL WITH FUNCTION %{signature_tof}s )
		 *
		 * OR
		 *
		 * CREATE %{or_replace}s TRANSFORM FOR %{typename}D LANGUAGE %{language}I
		 * ( TO SQL WITH FUNCTION %{signature_tof}s )
		 */

		/* Append a ',' if trffromsql is present, else append '(' */
		append_string_object(createTransform, "%{comma}s",
							 trfForm->trffromsql != InvalidOid ? "," : "(");

		/* Get the pg_proc tuple for the TO FUNCTION */
		procTup = SearchSysCache1(PROCOID, trfForm->trftosql);
		if (!HeapTupleIsValid(procTup))
			elog(ERROR, "cache lookup failure for function with OID %u",
				trfForm->trftosql);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);

		/*
		 * CREATE TRANSFORM does not change function signature so we can use catalog
		 * to get input type Oids.
		 */
		for (i = 0; i < procForm->pronargs; i++)
		{
			ObjTree *paramobj = new_objtree("");

			append_object_object(paramobj, "%{type}T",
								 new_objtree_for_type(procForm->proargtypes.values[i], -1));
			params = lappend(params, new_object_object(paramobj));
		}

		sign = new_objtree("");

		append_object_object(sign, "%{identity}D",
							new_objtree_for_qualname(procForm->pronamespace,
													 NameStr(procForm->proname)));
		append_array_object(sign, "(%{arguments:, }s)", params);

		append_object_object(createTransform, "TO SQL WITH FUNCTION %{signature_tof}s", sign);
		ReleaseSysCache(procTup);
	}
	append_string_object(createTransform, "%{close_bracket}s", ")");

	ReleaseSysCache(langTup);
	ReleaseSysCache(trfTup);
	return createTransform;
}

/*
 * Deparse CREATE TABLE AS command.
 *
 * deparse_CreateStmt do the actual work as we deparse the final CreateStmt for
 * CREATE TABLE AS command.
 */
static ObjTree *
deparse_CreateTableAsStmt(CollectedCommand *cmd)
{
	Oid			objectId;
	Node	   *parsetree;

	Assert(cmd->type == SCT_CreateTableAs);

	parsetree = cmd->d.ctas.real_create;
	objectId = cmd->d.ctas.address.objectId;

	return deparse_CreateStmt(objectId, parsetree);
}


/*
 * Handle deparsing of simple commands.
 *
 * This function should cover all cases handled in ProcessUtilitySlow.
 */
static ObjTree *
deparse_simple_command(CollectedCommand *cmd)
{
	Oid			objectId;
	Node	   *parsetree;
	ObjTree	   *command;

	Assert(cmd->type == SCT_Simple);

	parsetree = cmd->parsetree;
	objectId = cmd->d.simple.address.objectId;

	if (cmd->in_extension && (nodeTag(parsetree) != T_CreateExtensionStmt))
		return NULL;


	/* This switch needs to handle everything that ProcessUtilitySlow does */
	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			command = deparse_CreateSchemaStmt(objectId, parsetree);
			break;

		case T_AlterDomainStmt:
			command = deparse_AlterDomainStmt(objectId, parsetree,
											  cmd->d.simple.secondaryObject);
			break;

		case T_CreateStmt:
			command = deparse_CreateStmt(objectId, parsetree);
			break;

		case T_RefreshMatViewStmt:
			command = deparse_RefreshMatViewStmt(objectId, parsetree);
			break;

		case T_CreateTrigStmt:
			command = deparse_CreateTrigStmt(objectId, parsetree);
			break;

		case T_RuleStmt:
			command = deparse_RuleStmt(objectId, parsetree);
			break;

		case T_CreateSeqStmt:
			command = deparse_CreateSeqStmt(objectId, parsetree);
			break;

		case T_CreateFdwStmt:
			command = deparse_CreateFdwStmt(objectId, parsetree);
			break;

		case T_CreateUserMappingStmt:
			command = deparse_CreateUserMappingStmt(objectId, parsetree);
			break;

		case T_AlterUserMappingStmt:
			command = deparse_AlterUserMappingStmt(objectId, parsetree);
			break;

		case T_AlterFdwStmt:
			command = deparse_AlterFdwStmt(objectId, parsetree);
			break;

		case T_AlterSeqStmt:
			command = deparse_AlterSeqStmt(objectId, parsetree);
			break;

		case T_DefineStmt:
			command = deparse_DefineStmt(objectId, parsetree,
										 cmd->d.simple.secondaryObject);
			break;

		case T_CreateConversionStmt:
			command = deparse_CreateConversion(objectId, parsetree);
			break;

		case T_CreateDomainStmt:
			command = deparse_CreateDomain(objectId, parsetree);
			break;

		case T_CreateExtensionStmt:
			command = deparse_CreateExtensionStmt(objectId, parsetree);
			break;

		case T_AlterExtensionStmt:
			command = deparse_AlterExtensionStmt(objectId, parsetree);
			break;

		case T_AlterExtensionContentsStmt:
			command = deparse_AlterExtensionContentsStmt(objectId, parsetree,
														 cmd->d.simple.secondaryObject);
			break;

		case T_CreateOpFamilyStmt:
			command = deparse_CreateOpFamily(objectId, parsetree);
			break;

		case T_CreatePolicyStmt:
			command = deparse_CreatePolicyStmt(objectId, parsetree);
			break;

		case T_IndexStmt:
			command = deparse_IndexStmt(objectId, parsetree);
			break;

		case T_CreateFunctionStmt:
			command = deparse_CreateFunction(objectId, parsetree);
			break;

		case T_AlterFunctionStmt:
			command = deparse_AlterFunction(objectId, parsetree);
			break;

		case T_AlterCollationStmt:
			command = deparse_AlterCollation(objectId, parsetree);
			break;

		case T_RenameStmt:
			command = deparse_RenameStmt(cmd->d.simple.address, parsetree);
			break;

		case T_AlterObjectDependsStmt:
			command = deparse_AlterDependStmt(objectId, parsetree);
			break;

		case T_AlterObjectSchemaStmt:
			command = deparse_AlterObjectSchemaStmt(cmd->d.simple.address,
													parsetree,
													cmd->d.simple.secondaryObject);
			break;

		case T_AlterOwnerStmt:
			command = deparse_AlterOwnerStmt(cmd->d.simple.address, parsetree);
			break;

		case T_AlterOperatorStmt:
			command = deparse_AlterOperatorStmt(objectId, parsetree);
			break;

		case T_AlterPolicyStmt:
			command = deparse_AlterPolicyStmt(objectId, parsetree);
			break;

		case T_AlterTypeStmt:
			command = deparse_AlterTypeSetStmt(objectId, parsetree);
			break;

		case T_CreateForeignServerStmt:
			command = deparse_CreateForeignServerStmt(objectId, parsetree);
			break;

		case T_AlterForeignServerStmt:
			command = deparse_AlterForeignServerStmt(objectId, parsetree);
			break;

		case T_CompositeTypeStmt:
			command = deparse_CompositeTypeStmt(objectId, parsetree);
			break;

		case T_CreateEnumStmt:	/* CREATE TYPE AS ENUM */
			command = deparse_CreateEnumStmt(objectId, parsetree);
			break;

		case T_CreateRangeStmt:	/* CREATE TYPE AS RANGE */
			command = deparse_CreateRangeStmt(objectId, parsetree);
			break;

		case T_AlterEnumStmt:
			command = deparse_AlterEnumStmt(objectId, parsetree);
			break;

		case T_CreateCastStmt:
			command = deparse_CreateCastStmt(objectId, parsetree);
			break;

		case T_AlterTSDictionaryStmt:
			command = deparse_AlterTSDictionaryStmt(objectId, parsetree);
			break;

		case T_CreateTransformStmt:
			command = deparse_CreateTransformStmt(objectId, parsetree);
			break;

		default:
			command = NULL;
			elog(LOG, "unrecognized node type in deparse command: %d",
				 (int) nodeTag(parsetree));
	}

	return command;
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Deal with all the table elements (columns and constraints).
 *
 * Note we ignore constraints in the parse node here; they are extracted from
 * system catalogs instead.
 */
static List *
deparse_TableElements(Relation relation, List *tableElements, List *dpcontext,
					  bool typed, bool composite)
{
	List	   *elements = NIL;
	ListCell   *lc;

	foreach(lc, tableElements)
	{
		Node	   *elt = (Node *) lfirst(lc);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					ObjTree	   *tree;

					tree = typed ?
						deparse_ColumnDef_typed(relation, dpcontext,
												(ColumnDef *) elt) :
						deparse_ColumnDef(relation, dpcontext,
											  composite, (ColumnDef *) elt,
											  false, NULL);
					if (tree != NULL)
					{
						ObjElem	*column;

						column = new_object_object(tree);
						elements = lappend(elements, column);
					}
				}
				break;
			case T_Constraint:
				break;
			default:
				elog(ERROR, "invalid node type %d", nodeTag(elt));
		}
	}

	return elements;
}

/*
 * Workhorse to deparse a CollectedCommand.
 */
char *
deparse_utility_command(CollectedCommand *cmd, bool verbose_mode)
{
	OverrideSearchPath *overridePath;
	MemoryContext	oldcxt;
	MemoryContext	tmpcxt;
	ObjTree		   *tree;
	char		   *command;
	StringInfoData  str;

	/*
	 * Allocate everything done by the deparsing routines into a temp context,
	 * to avoid having to sprinkle them with memory handling code; but allocate
	 * the output StringInfo before switching.
	 */
	initStringInfo(&str);
	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "deparse ctx",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	/*
	 * Many routines underlying this one will invoke ruleutils.c functionality
	 * in order to obtain deparsed versions of expressions.  In such results,
	 * we want all object names to be qualified, so that results are "portable"
	 * to environments with different search_path settings.  Rather than inject
	 * what would be repetitive calls to override search path all over the
	 * place, we do it centrally here.
	 */
	overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = false;
	overridePath->addTemp = true;
	PushOverrideSearchPath(overridePath);

	verbose = verbose_mode;

	switch (cmd->type)
	{
		case SCT_Simple:
			tree = deparse_simple_command(cmd);
			break;
		case SCT_AlterTable:
			tree = deparse_AlterTableStmt(cmd);
			break;
		case SCT_Grant:
			tree = deparse_GrantStmt(cmd);
			break;
		case SCT_CreateTableAs:
			tree = deparse_CreateTableAsStmt(cmd);
			break;
		case SCT_AlterOpFamily:
			tree = deparse_AlterOpFamily(cmd);
			break;
		case SCT_CreateOpClass:
			tree = deparse_CreateOpClassStmt(cmd);
			break;
		case SCT_AlterTSConfig:
			tree = deparse_AlterTSConfigurationStmt(cmd);
			break;
		default:
			elog(ERROR, "unexpected deparse node type %d", cmd->type);
	}

	PopOverrideSearchPath();

	if (tree)
	{
		Jsonb *jsonb;

		jsonb = objtree_to_jsonb(tree);
		command = JsonbToCString(&str, &jsonb->root, JSONB_ESTIMATED_LEN);
	}
	else
		command = NULL;

	/*
	 * Clean up.  Note that since we created the StringInfo in the caller's
	 * context, the output string is not deleted here.
	 */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	return command;
}
