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
 * Deparse object tree is created by using:
 * 	a) new_objtree("know contents") where the complete tree content is known or
 *     the initial tree content is known.
 * 	b) new_objtree("") for the syntax where the object tree will be derived
 *     based on some conditional checks.
 * 	c) new_objtree_VA where the complete tree can be derived using some fixed
 *     content and/or some variable arguments.
 *
 * IDENTIFICATION
 *	  src/backend/commands/ddl_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
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
#include "catalog/pg_statistic_ext.h"
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
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/ddl_deparse.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
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

/*
 * Represent the command as an object tree.
 */
typedef struct ObjTree
{
	slist_head	params;			/* Object tree parameters */
	int			numParams;		/* Number of parameters in the object tree */
	StringInfo	fmtinfo;		/* Format string of the ObjTree */
	bool		present;		/* Indicates if boolean value should be stored */
} ObjTree;

/*
 * An element of an object tree (ObjTree).
 */
typedef struct ObjElem
{
	char	   *name;			/* Name of object element */
	ObjType		objtype;		/* Object type */

	union
	{
		bool		boolean;
		char	   *string;
		int64		integer;
		float8		flt;
		ObjTree    *object;
		List	   *array;
	}			value;			/* Store the object value based on the object
								 * type */
	slist_node	node;			/* Used in converting back to ObjElem
								 * structure */
} ObjElem;

/*
 * Reduce some unnecessary strings from the output json when verbose
 * and "present" member is false. This means these strings won't be merged into
 * the last DDL command.
 */
bool		verbose = true;

static void append_array_object(ObjTree *tree, char *sub_fmt, List *array);
static void append_bool_object(ObjTree *tree, char *sub_fmt, bool value);
static void append_float_object(ObjTree *tree, char *sub_fmt, float8 value);
static void append_null_object(ObjTree *tree, char *sub_fmt);
static void append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value);
static char *append_object_to_format_string(ObjTree *tree, char *sub_fmt);
static void append_premade_object(ObjTree *tree, ObjElem *elem);
static void append_string_object(ObjTree *tree, char *sub_fmt, char *name,
								 char *value);
static void format_type_detailed(Oid type_oid, int32 typemod,
								 Oid *nspid, char **typname, char **typemodstr,
								 bool *typarray);
static List *FunctionGetDefaults(text *proargdefaults);
static ObjElem *new_object(ObjType type, char *name);
static ObjTree *new_objtree_for_qualname_id(Oid classId, Oid objectId);
static ObjTree *new_objtree_for_rolespec(RoleSpec *spec);
static ObjElem *new_object_object(ObjTree *value);
static ObjTree *new_objtree_VA(char *fmt, int numobjs,...);
static ObjTree *new_objtree(char *fmt);
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

static ObjTree *deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
								  ColumnDef *coldef, bool is_alter, List **exprs);
static ObjTree *deparse_ColumnIdentity(Oid seqrelid, char identity, bool alter_table);
static ObjTree *deparse_ColumnSetOptions(AlterTableCmd *subcmd);
static ObjTree *deparse_DefineStmt_Aggregate(Oid objectId, DefineStmt *define);
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

static inline ObjElem *deparse_Seq_Cache(Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Cycle(Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_IncrementBy(Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Minvalue(Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Maxvalue(Form_pg_sequence seqdata, bool alter_table);
static ObjElem *deparse_Seq_OwnedBy(Oid sequenceId, bool alter_table);
static inline ObjElem *deparse_Seq_Restart(Form_pg_sequence_data seqdata);
static inline ObjElem *deparse_Seq_Startwith(Form_pg_sequence seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_As(DefElem *elem);
static inline ObjElem *deparse_Type_Storage(Form_pg_type typForm);
static inline ObjElem *deparse_Type_Receive(Form_pg_type typForm);
static inline ObjElem *deparse_Type_Send(Form_pg_type typForm);
static inline ObjElem *deparse_Type_Typmod_In(Form_pg_type typForm);
static inline ObjElem *deparse_Type_Typmod_Out(Form_pg_type typForm);
static inline ObjElem *deparse_Type_Analyze(Form_pg_type typForm);
static inline ObjElem *deparse_Type_Subscript(Form_pg_type typForm);

static List *deparse_InhRelations(Oid objectId);
static List *deparse_TableElements(Relation relation, List *tableElements, List *dpcontext,
								   bool typed, bool composite);

static char *DomainGetDefault(HeapTuple domTup, bool missing_ok);

/*
 * Append present as false to a tree.
 */
static void
append_not_present(ObjTree *tree)
{
	append_bool_object(tree, "present", false);
}

/*
 * Append a float8 parameter to a tree.
 */
static void
append_float_object(ObjTree *tree, char *sub_fmt, float8 value)
{
	ObjElem    *param;
	char	   *object_name;

	Assert(sub_fmt);

	object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeFloat, object_name);
	param->value.flt = value;
	append_premade_object(tree, param);
}

/*
 * Append an array parameter to a tree.
 */
static void
append_array_object(ObjTree *tree, char *sub_fmt, List *array)
{
	ObjElem    *param;
	char	   *object_name;

	Assert(sub_fmt);

	if (list_length(array) == 0)
		return;

	if (!verbose)
	{
		ListCell   *lc;

		/* Remove elements where present flag is false */
		foreach(lc, array)
		{
			ObjElem    *elem = (ObjElem *) lfirst(lc);

			Assert(elem->objtype == ObjTypeObject ||
				   elem->objtype == ObjTypeString);

			if (!elem->value.object->present &&
				elem->objtype == ObjTypeObject)
				array = foreach_delete_current(array, lc);
		}

	}

	/* Check for empty list after removing elements */
	if (list_length(array) == 0)
		return;

	object_name = append_object_to_format_string(tree, sub_fmt);

	param = new_object(ObjTypeArray, object_name);
	param->value.array = array;
	append_premade_object(tree, param);
}

/*
 * Append a boolean parameter to a tree.
 */
static void
append_bool_object(ObjTree *tree, char *sub_fmt, bool value)
{
	ObjElem    *param;
	char	   *object_name = sub_fmt;
	bool		is_present_flag = false;

	Assert(sub_fmt);

	/*
	 * Check if the format string is 'present' and if yes, store the boolean
	 * value
	 */
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
 * Append the input format string to the ObjTree.
 */
static void
append_format_string(ObjTree *tree, char *sub_fmt)
{
	int			len;
	char	   *fmt;

	if (tree->fmtinfo == NULL)
		return;

	fmt = tree->fmtinfo->data;
	len = tree->fmtinfo->len;

	/* Add a separator if necessary */
	if (len > 0 && fmt[len - 1] != ' ')
		appendStringInfoSpaces(tree->fmtinfo, 1);

	appendStringInfoString(tree->fmtinfo, sub_fmt);
}

/*
 * Append a NULL object to a tree.
 */
static void
append_null_object(ObjTree *tree, char *sub_fmt)
{
	char	   *object_name;

	Assert(sub_fmt);

	if (!verbose)
		return;

	object_name = append_object_to_format_string(tree, sub_fmt);

	append_premade_object(tree, new_object(ObjTypeNull, object_name));
}

/*
 * Append an object parameter to a tree.
 */
static void
append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value)
{
	ObjElem    *param;
	char	   *object_name;

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
	StringInfoData object_name;
	const char *end_ptr, *start_ptr;
	int         length;
	char        *tmp_str;

	if (sub_fmt == NULL || tree->fmtinfo == NULL)
		return sub_fmt;

	initStringInfo(&object_name);

	start_ptr = strchr(sub_fmt, '{');
	end_ptr = strchr(sub_fmt, ':');
	if (end_ptr == NULL)
		end_ptr = strchr(sub_fmt, '}');

	if (start_ptr != NULL && end_ptr != NULL)
	{
		length = end_ptr - start_ptr - 1;
		tmp_str = (char *) palloc(length + 1);
		strncpy(tmp_str, start_ptr + 1, length);
		tmp_str[length] = '\0';
		appendStringInfoString(&object_name, tmp_str);
		pfree(tmp_str);
	}

	if (object_name.len == 0)
		elog(ERROR, "object name not found");

	append_format_string(tree, sub_fmt);

	return object_name.data;

}

/*
 * Append a preallocated parameter to a tree.
 */
static inline void
append_premade_object(ObjTree *tree, ObjElem *elem)
{
	slist_push_head(&tree->params, &elem->node);
	tree->numParams++;
}

/*
 * Append a string parameter to a tree.
 */
static void
append_string_object(ObjTree *tree, char *sub_fmt, char * object_name,
					 char *value)
{
	ObjElem    *param;

	Assert(sub_fmt);

	if (!verbose && (value == NULL || value[0] == '\0'))
		return;

	append_format_string(tree, sub_fmt);
	param = new_object(ObjTypeString, object_name);
	param->value.string = value;
	append_premade_object(tree, param);
}

/*
 * Append a NULL-or-quoted-literal clause.  Useful for COMMENT and SECURITY
 * LABEL.
 *
 * Verbose syntax
 * %{null}s %{literal}s
 */
static void
append_literal_or_null(ObjTree *parent, char *elemname, char *value)
{
	ObjTree    *top;
	ObjTree    *part;

	top = new_objtree("");
	part = new_objtree_VA("NULL", 1,
						  "present", ObjTypeBool, !value);
	append_object_object(top, "%{null}s", part);

	part = new_objtree_VA("", 1,
						  "present", ObjTypeBool, value != NULL);

	if (value)
		append_string_object(part, "%{value}L", "value", value);
	append_object_object(top, "%{literal}s", part);

	append_object_object(parent, elemname, top);
}

/*
 * Similar to format_type_extended, except we return each bit of information
 * separately:
 *
 * - nspid is the schema OID.  For certain SQL-standard types which have weird
 *   typmod rules, we return InvalidOid; the caller is expected to not schema-
 *   qualify the name nor add quotes to the type name in this case.
 *
 * - typname is set to the type name, without quotes
 *
 * - typemodstr is set to the typemod, if any, as a string with parentheses
 *
 * - typarray indicates whether []s must be added
 *
 * We don't try to decode type names to their standard-mandated names, except
 * in the cases of types with unusual typmod rules.
 */
static void
format_type_detailed(Oid type_oid, int32 typemod,
					 Oid *nspid, char **typename, char **typemodstr,
					 bool *typearray)
{
	HeapTuple	tuple;
	Form_pg_type typeform;
	Oid			array_base_type;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type with OID %u", type_oid);

	typeform = (Form_pg_type) GETSTRUCT(tuple);

	/*
	 * Check if it's a regular (variable length) array type.  As above,
	 * fixed-length array types such as "name" shouldn't get deconstructed.
	 */
	array_base_type = typeform->typelem;

	*typearray = (IsTrueArrayType(typeform) && typeform->typstorage != TYPSTORAGE_PLAIN);

	if (*typearray)
	{
		/* Switch our attention to the array element type */
		ReleaseSysCache(tuple);
		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type with OID %u", type_oid);

		typeform = (Form_pg_type) GETSTRUCT(tuple);
		type_oid = array_base_type;
	}

	/*
	 * Special-case crock for types with strange typmod rules where we put
	 * typemod at the middle of name (e.g. TIME(6) with time zone). We cannot
	 * schema-qualify nor add quotes to the type name in these cases.
	 */
	*nspid = InvalidOid;

	switch (type_oid)
	{
		case INTERVALOID:
			*typename = pstrdup("INTERVAL");
			break;
		case TIMESTAMPTZOID:
			if (typemod < 0)
				*typename = pstrdup("TIMESTAMP WITH TIME ZONE");
			else
				/* otherwise, WITH TZ is added by typmod. */
				*typename = pstrdup("TIMESTAMP");
			break;
		case TIMESTAMPOID:
			*typename = pstrdup("TIMESTAMP");
			break;
		case TIMETZOID:
			if (typemod < 0)
				*typename = pstrdup("TIME WITH TIME ZONE");
			else
				/* otherwise, WITH TZ is added by typmod. */
				*typename = pstrdup("TIME");
			break;
		case TIMEOID:
			*typename = pstrdup("TIME");
			break;
		default:

			/*
			 * No additional processing is required for other types, so get
			 * the type name and schema directly from the catalog.
			 */
			*nspid = typeform->typnamespace;
			*typename = pstrdup(NameStr(typeform->typname));
	}

	if (typemod >= 0)
		*typemodstr = printTypmod("", typemod, typeform->typmodout);
	else
		*typemodstr = pstrdup("");

	ReleaseSysCache(tuple);
}

/*
 * Return the default values of arguments to a function, as a list of
 * deparsed expressions.
 */
static List *
FunctionGetDefaults(text *proargdefaults)
{
	List	   *nodedefs;
	List	   *strdefs = NIL;
	ListCell   *cell;

	nodedefs = (List *) stringToNode(text_to_cstring(proargdefaults));
	if (!IsA(nodedefs, List))
		elog(ERROR, "proargdefaults is not a list");

	foreach(cell, nodedefs)
	{
		Node	   *onedef = lfirst(cell);

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
			return "";			/* make compiler happy */
	}
}

/*
 * Return the string representation of the given storagetype value.
 */
static inline char *
get_type_storage(char storagetype)
{
	switch (storagetype)
	{
		case 'p':
			return "plain";
		case 'e':
			return "external";
		case 'x':
			return "extended";
		case 'm':
			return "main";
		default:
			elog(ERROR, "invalid storage specifier %c", storagetype);
	}
}

/*
 * Allocate a new parameter.
 */
static ObjElem *
new_object(ObjType type, char *name)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = name;
	param->objtype = type;

	return param;
}

/*
 * Allocate a new object parameter.
 */
static ObjElem *
new_object_object(ObjTree *value)
{
	ObjElem    *param;

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
	ObjTree    *params;

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
 * A helper routine to set up %{}D and %{}O elements.
 *
 * Elements "schema_name" and "obj_name" are set.  If the namespace OID
 * corresponds to a temp schema, that's set to "pg_temp".
 *
 * The difference between those two element types is whether the obj_name will
 * be quoted as an identifier or not, which is not something that this routine
 * concerns itself with; that will be up to the expand function.
 */
static ObjTree *
new_objtree_for_qualname(Oid nspid, char *name)
{
	ObjTree    *qualified;
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
 * A helper routine to set up %{}D and %{}O elements, with the object specified
 * by classId/objId.
 */
static ObjTree *
new_objtree_for_qualname_id(Oid classId, Oid objectId)
{
	ObjTree    *qualified;
	Relation	catalog;
	HeapTuple	catobj;
	Datum		obj_nsp;
	Datum		obj_name;
	AttrNumber	Anum_name;
	AttrNumber	Anum_namespace;
	AttrNumber	Anum_oid = get_object_attnum_oid(classId);
	bool		isnull;

	catalog = table_open(classId, AccessShareLock);

	catobj = get_catalog_object_by_oid(catalog, Anum_oid, objectId);
	if (!catobj)
		elog(ERROR, "cache lookup failed for object with OID %u of catalog \"%s\"",
			 objectId, RelationGetRelationName(catalog));
	Anum_name = get_object_attnum_name(classId);
	Anum_namespace = get_object_attnum_namespace(classId);

	obj_nsp = heap_getattr(catobj, Anum_namespace, RelationGetDescr(catalog),
						  &isnull);
	if (isnull)
		elog(ERROR, "null namespace for object %u", objectId);

	obj_name = heap_getattr(catobj, Anum_name, RelationGetDescr(catalog),
						   &isnull);
	if (isnull)
		elog(ERROR, "null attribute name for object %u", objectId);

	qualified = new_objtree_for_qualname(DatumGetObjectId(obj_nsp),
										 NameStr(*DatumGetName(obj_name)));
	table_close(catalog, AccessShareLock);

	return qualified;
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree *
new_objtree_for_type(Oid typeId, int32 typmod)
{
	Oid			typnspid;
	char	   *type_nsp;
	char	   *type_name = NULL;
	char	   *typmodstr;
	bool		type_array;

	format_type_detailed(typeId, typmod,
						 &typnspid, &type_name, &typmodstr, &type_array);

	if (OidIsValid(typnspid))
		type_nsp = get_namespace_name_or_temp(typnspid);
	else
		type_nsp = pstrdup("");

	return new_objtree_VA(NULL, 4,
						  "schemaname", ObjTypeString, type_nsp,
						  "typename", ObjTypeString, type_name,
						  "typmod", ObjTypeString, typmodstr,
						  "typarray", ObjTypeBool, type_array);
}

/*
 * Helper routine for %{}R objects, with role specified by RoleSpec node.
 * Special values such as ROLESPEC_CURRENT_USER are expanded to their final
 * names.
 */
static ObjTree *
new_objtree_for_rolespec(RoleSpec *spec)
{
	char	   *roletype;

	if (spec->roletype != ROLESPEC_PUBLIC)
		roletype = get_rolespec_name(spec);
	else
		roletype = pstrdup("");

	return new_objtree_VA(NULL, 2,
						  "is_public", ObjTypeBool, spec->roletype == ROLESPEC_PUBLIC,
						  "rolename", ObjTypeString, roletype);
}

/*
 * Helper routine for %{}R objects, with role specified by OID. (ACL_ID_PUBLIC
 * means to use "public").
 */
static ObjTree *
new_objtree_for_role_id(Oid roleoid)
{
	ObjTree    *role;

	if (roleoid != ACL_ID_PUBLIC)
	{
		HeapTuple	roltup;
		char	   *role_name;

		roltup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleoid));
		if (!HeapTupleIsValid(roltup))
			elog(ERROR, "cache lookup failed for role with OID %u", roleoid);

		role_name = NameStr(((Form_pg_authid) GETSTRUCT(roltup))->rolname);
		role = new_objtree_VA("%{rolename}I", 1,
							  "rolename", ObjTypeString, pstrdup(role_name));

		ReleaseSysCache(roltup);
	}
	else
		role = new_objtree_VA("%{rolename}I", 1,
							  "rolename", ObjTypeString, "public");

	return role;
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
	ObjTree    *tree;
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
		ObjElem    *elem;

		name = va_arg(args, char *);
		type = va_arg(args, ObjType);
		elem = new_object(type, NULL);

		/*
		 * For all param types other than ObjTypeNull, there must be a value in
		 * the varargs. Fetch it and add the fully formed subobject into the
		 * main object.
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
				elem->value.integer = va_arg(args, int);
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

/*
 * Allocate a new string object.
 */
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
			{
				ListCell   *cell;

				pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
				foreach(cell, object->value.array)
				{
					ObjElem    *elem = lfirst(cell);

					objtree_to_jsonb_element(state, elem, WJB_ELEM);
				}
				pushJsonbValue(&state, WJB_END_ARRAY, NULL);
			}
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
		ObjElem    *object = slist_container(ObjElem, node, iter.cur);
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
	ObjTree    *constr;

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
				continue;		/* not here */
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
		constr = new_objtree_VA("CONSTRAINT %{name}I %{definition}s", 4,
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
 * Return an index definition, split into several pieces.
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

	*tablespace = NULL;
	*whereClause = NULL;

	/* Fetch the pg_index tuple by the Oid of the index */
	ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(ht_idx))
		elog(ERROR, "cache lookup failed for index with OID %u", indexrelid);
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

	/* Fetch the pg_class tuple of the index relation */
	ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(ht_idxrel))
		elog(ERROR, "cache lookup failed for relation with OID %u", indexrelid);
	idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

	/* Fetch the pg_am tuple of the index' access method */
	ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
	if (!HeapTupleIsValid(ht_am))
		elog(ERROR, "cache lookup failed for access method with OID %u",
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
	appendStringInfoString(&definitionBuf, "(");
	for (keyno = 0; keyno < idxrec->indnatts; keyno++)
	{
		AttrNumber	attnum = idxrec->indkey.values[keyno];
		int16		opt = indoption->values[keyno];
		Oid			keycoltype;
		Oid			keycolcollation;

		/* Print INCLUDE to divide key and non-key attrs. */
		if (keyno == idxrec->indnkeyatts)
		{
			appendStringInfoString(&definitionBuf, ") INCLUDE (");
		}
		else
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

		/* Print additional decoration for (selected) key columns, even if default */
		if (keyno < idxrec->indnkeyatts)
		{
			Oid indcoll = indcollation->values[keyno];
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
	}
	appendStringInfoString(&definitionBuf, ")");
	*definition = definitionBuf.data;

	/* Output reloptions */
	*reloptions = flatten_reloptions(indexrelid);

	/* Output tablespace */
	{
		Oid			tblspc;

		tblspc = get_rel_tablespace(indexrelid);
		if (OidIsValid(tblspc))
			*tablespace = pstrdup(quote_identifier(get_tablespace_name(tblspc)));
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
		*whereClause = deparse_expression(node, context, false, false);
	}

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
	Node	   *defval;
	char	   *defstr;

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
		elog(ERROR, "cache lookup failed for relation with OID %u", relid);

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
 * Deparse a ColumnDef node within a regular (non-typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway).
 *
 * Verbose syntax
 * %{name}I %{coltype}T %{compression}s %{default}s %{not_null}s %{collation}s
 */
static ObjTree *
deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
				  ColumnDef *coldef, bool is_alter, List **exprs)
{
	ObjTree    *ret;
	ObjTree    *tmp_obj;
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
	 * XXX maybe it is useful to have them with "present = false" or some
	 * such?
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

	ret = new_objtree_VA("%{name}I %{coltype}T", 3,
						 "type", ObjTypeString, "column",
						 "name", ObjTypeString, coldef->colname,
						 "coltype", ObjTypeObject,
						 new_objtree_for_type(typid, typmod));

	if (!composite)
		append_string_object(ret, "STORAGE %{colstorage}s", "colstorage",
							 get_type_storage(attrForm->attstorage));

	/* USING clause */
	tmp_obj = new_objtree("COMPRESSION");
	if (coldef->compression)
		append_string_object(tmp_obj, "%{compression_method}I",
							 "compression_method", coldef->compression);
	else
	{
		append_null_object(tmp_obj, "%{compression_method}I");
		append_not_present(tmp_obj);
	}
	append_object_object(ret, "%{compression}s", tmp_obj);

	tmp_obj = new_objtree("COLLATE");
	if (OidIsValid(typcollation))
		append_object_object(tmp_obj, "%{name}D",
							 new_objtree_for_qualname_id(CollationRelationId,
														 typcollation));
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{collation}s", tmp_obj);

	if (!composite)
	{
		Oid			seqrelid = InvalidOid;

		/*
		 * Emit a NOT NULL declaration if necessary.  Note that we cannot
		 * trust pg_attribute.attnotnull here, because that bit is also set
		 * when primary keys are specified; we must not emit a NOT NULL
		 * constraint in that case, unless explicitly specified.  Therefore,
		 * we scan the list of constraints attached to this column to
		 * determine whether we need to emit anything. (Fortunately, NOT NULL
		 * constraints cannot be table constraints.)
		 *
		 * In the ALTER TABLE cases, we also add a NOT NULL if the colDef is
		 * marked is_not_null.
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

		if (is_alter && coldef->is_not_null)
			saw_notnull = true;

		append_string_object(ret, "%{not_null}s", "not_null",
							 saw_notnull ? "NOT NULL" : "");

		tmp_obj = new_objtree("DEFAULT");
		if (attrForm->atthasdef &&
			coldef->generated != ATTRIBUTE_GENERATED_STORED)
		{
			char	   *defstr;

			defstr = RelationGetColumnDefault(relation, attrForm->attnum,
											  dpcontext, exprs);

			append_string_object(tmp_obj, "%{default}s", "default", defstr);
		}
		else
			append_not_present(tmp_obj);
		append_object_object(ret, "%{default}s", tmp_obj);

		/* IDENTITY COLUMN */
		if (coldef->identity)
		{
			Oid			attno = get_attnum(relid, coldef->colname);

			seqrelid = getIdentitySequence(relid, attno, false);
		}

		if (OidIsValid(seqrelid))
		{
			tmp_obj = deparse_ColumnIdentity(seqrelid, coldef->identity, is_alter);
			append_object_object(ret, "%{identity_column}s", tmp_obj);
		}

		/* GENERATED COLUMN EXPRESSION */
		tmp_obj = new_objtree("GENERATED ALWAYS AS");
		if (coldef->generated == ATTRIBUTE_GENERATED_STORED)
		{
			char	   *defstr;

			defstr = RelationGetColumnDefault(relation, attrForm->attnum,
											  dpcontext, exprs);
			append_string_object(tmp_obj, "%{generation_expr}s STORED",
								 "generation_expr", defstr);
		}
		else
			append_not_present(tmp_obj);

		append_object_object(ret, "%{generated_column}s", tmp_obj);
	}

	ReleaseSysCache(attrTup);

	return ret;
}

/*
 * Deparse a ColumnDef node within a typed table creation.	This is simpler
 * than the regular case, because we don't have to emit the type declaration,
 * collation, or default.  Here we only return something if the column is being
 * declared NOT NULL.
 *
 * As in deparse_ColumnDef, any other constraint is processed elsewhere.
 *
 * Verbose syntax
 * %{name}I WITH OPTIONS %{default}s %{not_null}s.
 */
static ObjTree *
deparse_ColumnDef_typed(Relation relation, List *dpcontext, ColumnDef *coldef)
{
	ObjTree    *ret = NULL;
	ObjTree    *tmp_obj;
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

	tmp_obj = new_objtree("DEFAULT");
	if (attrForm->atthasdef)
	{
		char	   *defstr;

		defstr = RelationGetColumnDefault(relation, attrForm->attnum,
										  dpcontext, NULL);

		append_string_object(tmp_obj, "%{default}s", "default", defstr);
	}
	else
		append_not_present(tmp_obj);

	ret = new_objtree_VA("%{name}I WITH OPTIONS %{not_null}s %{default}s", 4,
						 "type", ObjTypeString, "column",
						 "name", ObjTypeString, coldef->colname,
						 "not_null", ObjTypeString,
						 saw_notnull ? "NOT NULL" : "",
						 "default", ObjTypeObject, tmp_obj);

	/* Generated columns are not supported on typed tables, so we are done */

	ReleaseSysCache(attrTup);

	return ret;
}

/*
 * Deparse the definition of column identity.
 *
 * Verbose syntax
 * SET GENERATED %{option}s %{identity_type}s %{seq_definition: }s
 * 	OR
 * GENERATED %{option}s AS IDENTITY %{identity_type}s ( %{seq_definition: }s )
 */
static ObjTree *
deparse_ColumnIdentity(Oid seqrelid, char identity, bool alter_table)
{
	ObjTree    *ret;
	ObjTree    *ident_obj;
	List	   *elems = NIL;
	Relation	rel;
	HeapTuple	seqtuple;
	Form_pg_sequence seqform;
	Form_pg_sequence_data seqdata;
	char	   *identfmt;
	char	   *objfmt;

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

	ident_obj = new_objtree(identfmt);

	if (identity == ATTRIBUTE_IDENTITY_ALWAYS)
		append_string_object(ident_obj, objfmt, "option", "ALWAYS");
	else if (identity == ATTRIBUTE_IDENTITY_BY_DEFAULT)
		append_string_object(ident_obj, objfmt, "option", "BY DEFAULT");
	else
		append_not_present(ident_obj);

	ret = new_objtree_VA("%{identity_type}s", 1,
						 "identity_type", ObjTypeObject, ident_obj);

	rel = table_open(SequenceRelationId, RowExclusiveLock);
	seqtuple = SearchSysCacheCopy1(SEQRELID, seqrelid);
	if (!HeapTupleIsValid(seqtuple))
		elog(ERROR, "cache lookup failed for sequence with OID %u",
			 seqrelid);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);
	seqdata = get_sequence_values(seqrelid);

	/* Definition elements */
	elems = lappend(elems, deparse_Seq_Cache(seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Cycle(seqform, alter_table));
	elems = lappend(elems, deparse_Seq_IncrementBy(seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Minvalue(seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Maxvalue(seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Startwith(seqform, alter_table));
	elems = lappend(elems, deparse_Seq_Restart(seqdata));
	/* We purposefully do not emit OWNED BY here */

	if (alter_table)
		append_array_object(ret, "%{seq_definition: }s", elems);
	else
		append_array_object(ret, "( %{seq_definition: }s )", elems);

	table_close(rel, RowExclusiveLock);

	return ret;
}

/*
 * ... ALTER COLUMN ... SET/RESET (...)
 *
 * Verbose syntax
 * ALTER COLUMN %{column}I RESET|SET (%{options:, }s)
 */
static ObjTree *
deparse_ColumnSetOptions(AlterTableCmd *subcmd)
{
	List	   *sets = NIL;
	ListCell   *cell;
	ObjTree    *ret;
	bool		is_reset = subcmd->subtype == AT_ResetOptions;

	ret = new_objtree_VA("ALTER COLUMN %{column}I %{option}s", 2,
						 "column", ObjTypeString, subcmd->name,
						 "option", ObjTypeString, is_reset ? "RESET" : "SET");

	foreach(cell, (List *) subcmd->def)
	{
		DefElem    *elem;
		ObjTree    *set;

		elem = (DefElem *) lfirst(cell);
		set = deparse_DefElem(elem, is_reset);
		sets = lappend(sets, new_object_object(set));
	}

	Assert(sets);
	append_array_object(ret, "(%{options:, }s)", sets);

	return ret;
}

/*
 * ... ALTER COLUMN ... SET/RESET (...)
 *
 * Verbose syntax
 * RESET|SET (%{options:, }s)
 */
static ObjTree *
deparse_RelSetOptions(AlterTableCmd *subcmd)
{
	List	   *sets = NIL;
	ListCell   *cell;
	bool		is_reset = subcmd->subtype == AT_ResetRelOptions;

	foreach(cell, (List *) subcmd->def)
	{
		DefElem    *elem;
		ObjTree    *set;

		elem = (DefElem *) lfirst(cell);
		set = deparse_DefElem(elem, is_reset);
		sets = lappend(sets, new_object_object(set));
	}

	Assert(sets);

	return new_objtree_VA("%{set_reset}s (%{options:, }s)", 2,
						  "set_reset", ObjTypeString, is_reset ? "RESET" : "SET",
						  "options", ObjTypeArray, sets);
}

/*
 * Deparse DefElems, as used e.g. by ALTER COLUMN ... SET, into a list of SET
 * (...)  or RESET (...) contents.
 *
 * Verbose syntax
 * %{label}s = %{value}L
 */
static ObjTree *
deparse_DefElem(DefElem *elem, bool is_reset)
{
	ObjTree    *ret;
	ObjTree    *optname = new_objtree("");

	if (elem->defnamespace != NULL)
		append_string_object(optname, "%{schema}I.", "schema",
							 elem->defnamespace);

	append_string_object(optname, "%{label}I", "label", elem->defname);

	ret = new_objtree_VA("%{label}s", 1,
						 "label", ObjTypeObject, optname);

	if (!is_reset)
		append_string_object(ret, "= %{value}L", "value",
							 elem->arg ? defGetString(elem) :
							 defGetBoolean(elem) ? "TRUE" : "FALSE");

	return ret;
}

/*
 * Deparse the INHERITS relations.
 *
 * Given a table OID, return a schema-qualified table list representing
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
		ObjTree    *parent;
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
 * Deparse the ON COMMIT ... clause for CREATE ... TEMPORARY ...
 *
 * Verbose syntax
 * ON COMMIT %{on_commit_value}s
 */
static ObjTree *
deparse_OnCommitClause(OnCommitAction option)
{
	ObjTree    *ret  = new_objtree("ON COMMIT");
	switch (option)
	{
		case ONCOMMIT_DROP:
			append_string_object(ret, "%{on_commit_value}s",
								 "on_commit_value", "DROP");
			break;

		case ONCOMMIT_DELETE_ROWS:
			append_string_object(ret, "%{on_commit_value}s",
								 "on_commit_value", "DELETE ROWS");
			break;

		case ONCOMMIT_PRESERVE_ROWS:
			append_string_object(ret, "%{on_commit_value}s",
								 "on_commit_value", "PRESERVE ROWS");
			break;

		case ONCOMMIT_NOOP:
			append_null_object(ret, "%{on_commit_value}s");
			append_not_present(ret);
			break;
	}

	return ret;
}

/*
 * Deparse the sequence CACHE option.
 *
 * Verbose syntax
 * SET CACHE %{value}s
 * OR
 * CACHE %{value}
 */
static inline ObjElem *
deparse_Seq_Cache(Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree    *ret;
	char	   *tmpstr;
	char	   *fmt;

	fmt = alter_table ? "SET CACHE %{value}s" : "CACHE %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqcache);
	ret = new_objtree_VA(fmt, 2,
						 "clause", ObjTypeString, "cache",
						 "value", ObjTypeString, tmpstr);

	return new_object_object(ret);
}

/*
 * Deparse the sequence CYCLE option.
 *
 * Verbose syntax
 * SET %{no}s CYCLE
 * OR
 * %{no}s CYCLE
 */
static inline ObjElem *
deparse_Seq_Cycle(Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree    *ret;
	char	   *fmt;

	fmt = alter_table ? "SET %{no}s CYCLE" : "%{no}s CYCLE";

	ret = new_objtree_VA(fmt, 2,
						 "clause", ObjTypeString, "cycle",
						 "no", ObjTypeString,
						 seqdata->seqcycle ? "" : "NO");

	return new_object_object(ret);
}

/*
 * Deparse the sequence INCREMENT BY option.
 *
 * Verbose syntax
 * SET INCREMENT BY %{value}s
 * OR
 * INCREMENT BY %{value}s
 */
static inline ObjElem *
deparse_Seq_IncrementBy(Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree    *ret;
	char	   *tmpstr;
	char	   *fmt;

	fmt = alter_table ? "SET INCREMENT BY %{value}s" : "INCREMENT BY %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqincrement);
	ret = new_objtree_VA(fmt, 2,
						 "clause", ObjTypeString, "seqincrement",
						 "value", ObjTypeString, tmpstr);

	return new_object_object(ret);
}

/*
 * Deparse the sequence MAXVALUE option.
 *
 * Verbose syntax
 * SET MAXVALUE %{value}s
 * OR
 * MAXVALUE %{value}s
 */
static inline ObjElem *
deparse_Seq_Maxvalue(Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree    *ret;
	char	   *tmpstr;
	char	   *fmt;

	fmt = alter_table ? "SET MAXVALUE %{value}s" : "MAXVALUE %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqmax);
	ret = new_objtree_VA(fmt, 2,
						 "clause", ObjTypeString, "maxvalue",
						 "value", ObjTypeString, tmpstr);

	return new_object_object(ret);
}

/*
 * Deparse the sequence MINVALUE option.
 *
 * Verbose syntax
 * SET MINVALUE %{value}s
 * OR
 * MINVALUE %{value}s
 */
static inline ObjElem *
deparse_Seq_Minvalue(Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree    *ret;
	char	   *tmpstr;
	char	   *fmt;

	fmt = alter_table ? "SET MINVALUE %{value}s" : "MINVALUE %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqmin);
	ret = new_objtree_VA(fmt, 2,
						 "clause", ObjTypeString, "minvalue",
						 "value", ObjTypeString, tmpstr);

	return new_object_object(ret);
}

/*
 * Deparse the sequence OWNED BY command.
 *
 * Verbose syntax
 * OWNED BY %{owner}D
 */
static ObjElem *
deparse_Seq_OwnedBy(Oid sequenceId, bool alter_table)
{
	ObjTree    *ret = NULL;
	Relation	depRel;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;

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
		Oid			ownerId;
		Form_pg_depend depform;
		ObjTree    *tmp_obj;
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

		tmp_obj = new_objtree_for_qualname_id(RelationRelationId, ownerId);
		append_string_object(tmp_obj, "attrname", "attrname", colname);
		ret = new_objtree_VA("OWNED BY %{owner}D", 2,
							 "clause", ObjTypeString, "owned",
							 "owner", ObjTypeObject, tmp_obj);
	}

	systable_endscan(scan);
	relation_close(depRel, AccessShareLock);

	/*
	 * If there's no owner column, emit an empty OWNED BY element, set up so
	 * that it won't print anything.
	 */
	if (!ret)
		/* XXX this shouldn't happen ... */
		ret = new_objtree_VA("OWNED BY %{owner}D", 3,
							 "clause", ObjTypeString, "owned",
							 "owner", ObjTypeNull,
							 "present", ObjTypeBool, false);

	return new_object_object(ret);
}

/*
 * Deparse the sequence RESTART option.
 *
 * Verbose syntax
 * RESTART %{value}s
 */
static inline ObjElem *
deparse_Seq_Restart(Form_pg_sequence_data seqdata)
{
	ObjTree    *ret;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->last_value);
	ret = new_objtree_VA("RESTART %{value}s", 2,
						 "clause", ObjTypeString, "restart",
						 "value", ObjTypeString, tmpstr);

	return new_object_object(ret);
}

/*
 * Deparse the sequence AS option.
 *
 * Verbose syntax
 * AS %{identity}D
 */
static inline ObjElem *
deparse_Seq_As(DefElem *elem)
{
	ObjTree    *ret;
	Type		likeType;
	Form_pg_type likeForm;

	likeType = typenameType(NULL, defGetTypeName(elem), NULL);
	likeForm = (Form_pg_type) GETSTRUCT(likeType);

	ret = new_objtree_VA("AS %{identity}D", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(likeForm->typnamespace,
												  NameStr(likeForm->typname)));
	ReleaseSysCache(likeType);

	return new_object_object(ret);
}

/*
 * Deparse the sequence START WITH option.
 *
 * Verbose syntax
 * SET START WITH %{value}s
 * OR
 * START WITH %{value}s
 */
static inline ObjElem *
deparse_Seq_Startwith(Form_pg_sequence seqdata, bool alter_table)
{
	ObjTree    *ret;
	char	   *tmpstr;
	char	   *fmt;

	fmt = alter_table ? "SET START WITH %{value}s" : "START WITH %{value}s";

	tmpstr = psprintf(INT64_FORMAT, seqdata->seqstart);
	ret = new_objtree_VA(fmt, 2,
						 "clause", ObjTypeString, "start",
						 "value", ObjTypeString, tmpstr);

	return new_object_object(ret);
}

/*
 * Deparse the type STORAGE option.
 *
 * Verbose syntax
 * STORAGE=%{value}s
 */
static inline ObjElem *
deparse_Type_Storage(Form_pg_type typForm)
{
	ObjTree    *ret;
	ret = new_objtree_VA("STORAGE = %{value}s", 2,
						 "clause", ObjTypeString, "storage",
						 "value", ObjTypeString, get_type_storage(typForm->typstorage));

	return new_object_object(ret);
}

/*
 * Deparse the type RECEIVE option.
 *
 * Verbose syntax
 * RECEIVE=%{procedure}D
 */
static inline ObjElem *
deparse_Type_Receive(Form_pg_type typForm)
{
	ObjTree    *ret;

	ret = new_objtree_VA("RECEIVE=", 1,
						 "clause", ObjTypeString, "receive");
	if (OidIsValid(typForm->typreceive))
		append_object_object(ret, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typreceive));
	else
		append_not_present(ret);

	return new_object_object(ret);
}

/*
 * Deparse the type SEND option.
 *
 * Verbose syntax
 * SEND=%{procedure}D
 */
static inline ObjElem *
deparse_Type_Send(Form_pg_type typForm)
{
	ObjTree    *ret;

	ret = new_objtree_VA("SEND=", 1,
						 "clause", ObjTypeString, "send");
	if (OidIsValid(typForm->typsend))
		append_object_object(ret, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typsend));
	else
		append_not_present(ret);

	return new_object_object(ret);
}

/*
 * Deparse the type typmod_in option.
 *
 * Verbose syntax
 * TYPMOD_IN=%{procedure}D
 */
static inline ObjElem *
deparse_Type_Typmod_In(Form_pg_type typForm)
{
	ObjTree    *ret;

	ret = new_objtree_VA("TYPMOD_IN=", 1,
						 "clause", ObjTypeString, "typmod_in");
	if (OidIsValid(typForm->typmodin))
		append_object_object(ret, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodin));
	else
		append_not_present(ret);

	return new_object_object(ret);
}

/*
 * Deparse the type typmod_out option.
 *
 * Verbose syntax
 * TYPMOD_OUT=%{procedure}D
 */
static inline ObjElem *
deparse_Type_Typmod_Out(Form_pg_type typForm)
{
	ObjTree    *ret;

	ret = new_objtree_VA("TYPMOD_OUT=", 1,
						 "clause", ObjTypeString, "typmod_out");
	if (OidIsValid(typForm->typmodout))
		append_object_object(ret, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodout));
	else
		append_not_present(ret);

	return new_object_object(ret);
}

/*
 * Deparse the type analyze option.
 *
 * Verbose syntax
 * ANALYZE=%{procedure}D
 */
static inline ObjElem *
deparse_Type_Analyze(Form_pg_type typForm)
{
	ObjTree    *ret;

	ret = new_objtree_VA("ANALYZE=", 1,
						 "clause", ObjTypeString, "analyze");
	if (OidIsValid(typForm->typanalyze))
		append_object_object(ret, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typanalyze));
	else
		append_not_present(ret);

	return new_object_object(ret);
}

/*
 * Deparse the type subscript option.
 *
 * Verbose syntax
 * SUBSCRIPT=%{procedure}D
 */
static inline ObjElem *
deparse_Type_Subscript(Form_pg_type typForm)
{
	ObjTree    *ret;

	ret = new_objtree_VA("SUBSCRIPT=", 1,
						 "clause", ObjTypeString, "subscript");
	if (OidIsValid(typForm->typsubscript))
		append_object_object(ret, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typsubscript));
	else
		append_not_present(ret);

	return new_object_object(ret);
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
					ObjTree    *tree;

					tree = typed ?
						deparse_ColumnDef_typed(relation, dpcontext,
												(ColumnDef *) elt) :
						deparse_ColumnDef(relation, dpcontext,
										  composite, (ColumnDef *) elt,
										  false, NULL);
					if (tree != NULL)
						elements = lappend(elements, new_object_object(tree));
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
 * Deparse a CreateSeqStmt.
 *
 * Given a sequence OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{persistence}s SEQUENCE %{identity}D
 */
static ObjTree *
deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *ret;
	Relation	relation;
	Form_pg_sequence_data seqdata;
	List	   *elems = NIL;
	Form_pg_sequence seqform;
	Relation	rel;
	HeapTuple	seqtuple;
	CreateSeqStmt *createSeqStmt = (CreateSeqStmt *) parsetree;

	/*
	 * Sequence for IDENTITY COLUMN output separately (via CREATE TABLE or
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
		elog(ERROR, "cache lookup failed for sequence with OID %u",
			 objectId);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

	/* Definition elements */
	elems = lappend(elems, deparse_Seq_Cache(seqform, false));
	elems = lappend(elems, deparse_Seq_Cycle(seqform, false));
	elems = lappend(elems, deparse_Seq_IncrementBy(seqform, false));
	elems = lappend(elems, deparse_Seq_Minvalue(seqform, false));
	elems = lappend(elems, deparse_Seq_Maxvalue(seqform, false));
	elems = lappend(elems, deparse_Seq_Startwith(seqform, false));
	elems = lappend(elems, deparse_Seq_Restart(seqdata));

	/* We purposefully do not emit OWNED BY here */

	ret = new_objtree_VA("CREATE %{persistence}s SEQUENCE %{if_not_exists}s %{identity}D %{definition: }s", 4,
						 "persistence", ObjTypeString,
						 get_persistence_str(relation->rd_rel->relpersistence),
						 "if_not_exists", ObjTypeString,
						 createSeqStmt->if_not_exists ? "IF NOT EXISTS" : "",
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(relation->rd_rel->relnamespace,
												  RelationGetRelationName(relation)),
						 "definition", ObjTypeArray, elems);

	table_close(rel, RowExclusiveLock);
	relation_close(relation, AccessShareLock);

	return ret;
}

/*
 * Deparse an IndexStmt.
 *
 * Given an index OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * If the index corresponds to a constraint, NULL is returned.
 *
 * Verbose syntax
 * CREATE %{unique}s INDEX %{concurrently}s %{if_not_exists}s %{name}I ON
 * %{table}D USING %{index_am}s %{definition}s %{with}s %{tablespace}s
 * %{where_clause}s
 */
static ObjTree *
deparse_IndexStmt(Oid objectId, Node *parsetree)
{
	IndexStmt  *node = (IndexStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *tmp_obj;
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

	ret = new_objtree_VA("CREATE %{unique}s INDEX %{concurrently}s %{if_not_exists}s %{name}I ON %{table}D USING %{index_am}s %{definition}s", 7,
						 "unique", ObjTypeString,
						 node->unique ? "UNIQUE" : "",
						 "concurrently", ObjTypeString,
						 node->concurrent ? "CONCURRENTLY" : "",
						 "if_not_exists", ObjTypeString,
						 node->if_not_exists ? "IF NOT EXISTS" : "",
						 "name", ObjTypeString,
						 RelationGetRelationName(idxrel),
						 "table", ObjTypeObject,
						 new_objtree_for_qualname(heaprel->rd_rel->relnamespace,
												  RelationGetRelationName(heaprel)),
						 "index_am", ObjTypeString, index_am,
						 "definition", ObjTypeString, definition);

	/* reloptions */
	tmp_obj = new_objtree("WITH");
	if (reloptions)
		append_string_object(tmp_obj, "(%{opts}s)", "opts", reloptions);
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{with}s", tmp_obj);

	/* tablespace */
	tmp_obj = new_objtree("TABLESPACE");
	if (tablespace)
		append_string_object(tmp_obj, "%{tablespace}s", "tablespace", tablespace);
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{tablespace}s", tmp_obj);

	/* WHERE clause */
	tmp_obj = new_objtree("WHERE");
	if (whereClause)
		append_string_object(tmp_obj, "%{where}s", "where", whereClause);
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{where_clause}s", tmp_obj);

	table_close(idxrel, AccessShareLock);
	table_close(heaprel, AccessShareLock);

	return ret;
}

/*
 * Deparse a CreateStmt (CREATE TABLE).
 *
 * Given a table OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D [OF
 * %{of_type}T | PARTITION OF %{parent_identity}D] %{table_elements}s
 * %{inherits}s %{partition_by}s %{access_method}s %{with_clause}s
 * %{on_commit}s %{tablespace}s
 */
static ObjTree *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	List	   *list = NIL;
	ListCell   *cell;

	ret = new_objtree_VA("CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D", 3,
						 "persistence", ObjTypeString,
						 get_persistence_str(relation->rd_rel->relpersistence),
						 "if_not_exists", ObjTypeString,
						 node->if_not_exists ? "IF NOT EXISTS" : "",
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(relation->rd_rel->relnamespace,
												  RelationGetRelationName(relation)));

	dpcontext = deparse_context_for(RelationGetRelationName(relation),
									objectId);

	/*
	 * Typed tables and partitions use a slightly different format string: we
	 * must not put table_elements with parents directly in the fmt string,
	 * because if there are no options the parentheses must not be emitted;
	 * and also, typed tables do not allow for inheritance.
	 */
	if (node->ofTypename || node->partbound)
	{
		List	   *tableelts = NIL;

		/*
		 * We can't put table elements directly in the fmt string as an array
		 * surrounded by parentheses here, because an empty clause would cause
		 * a syntax error.  Therefore, we use an indirection element and set
		 * present=false when there are no elements.
		 */
		if (node->ofTypename)
		{
			tmp_obj = new_objtree_for_type(relation->rd_rel->reloftype, -1);
			append_object_object(ret, "OF %{of_type}T", tmp_obj);
		}
		else
		{
			List	   *parents;
			ObjElem    *elem;

			parents = deparse_InhRelations(objectId);
			elem = (ObjElem *) linitial(parents);

			Assert(list_length(parents) == 1);

			append_format_string(ret, "PARTITION OF");

			append_object_object(ret, "%{parent_identity}D",
								 elem->value.object);
		}

		tableelts = deparse_TableElements(relation, node->tableElts, dpcontext,
										  true, /* typed table */
										  false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		tmp_obj = new_objtree("");
		if (tableelts)
			append_array_object(tmp_obj, "(%{elements:, }s)", tableelts);
		else
			append_not_present(tmp_obj);

		append_object_object(ret, "%{table_elements}s", tmp_obj);
	}
	else
	{
		List	   *tableelts = NIL;

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
										  false,	/* not typed table */
										  false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		if (tableelts)
			append_array_object(ret, "(%{table_elements:, }s)", tableelts);
		else
			append_format_string(ret, "()");

		/*
		 * Add inheritance specification.  We cannot simply scan the list of
		 * parents from the parser node, because that may lack the actual
		 * qualified names of the parent relations.  Rather than trying to
		 * re-resolve them from the information in the parse node, it seems
		 * more accurate and convenient to grab it from pg_inherits.
		 */
		tmp_obj = new_objtree("INHERITS");
		if (node->inhRelations != NIL)
			append_array_object(tmp_obj, "(%{parents:, }D)", deparse_InhRelations(objectId));
		else
		{
			append_null_object(tmp_obj, "(%{parents:, }D)");
			append_not_present(tmp_obj);
		}
		append_object_object(ret, "%{inherits}s", tmp_obj);
	}

	tmp_obj = new_objtree("TABLESPACE");
	if (node->tablespacename)
		append_string_object(tmp_obj, "%{tablespace}I", "tablespace",
							 node->tablespacename);
	else
	{
		append_null_object(tmp_obj, "%{tablespace}I");
		append_not_present(tmp_obj);
	}
	append_object_object(ret, "%{tablespace}s", tmp_obj);
	append_object_object(ret, "%{on_commit}s",
						 deparse_OnCommitClause(node->oncommit));

	/* FOR VALUES clause */
	if (node->partbound)
	{
		/*
		 * Get pg_class.relpartbound. We cannot use partbound in the parsetree
		 * directly as it's the original partbound expression which haven't
		 * been transformed.
		 */
		append_string_object(ret, "%{partition_bound}s", "partition_bound",
							 RelationGetPartitionBound(objectId));
	}

	/* PARTITION BY clause */
	tmp_obj = new_objtree("PARTITION BY");
	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		append_string_object(tmp_obj, "%{definition}s", "definition",
							 pg_get_partkeydef_simple(objectId));
	else
	{
		append_null_object(tmp_obj, "%{definition}s");
		append_not_present(tmp_obj);
	}
	append_object_object(ret, "%{partition_by}s", tmp_obj);

	/* USING clause */
	tmp_obj = new_objtree("USING");
	if (node->accessMethod)
		append_string_object(tmp_obj, "%{access_method}I", "access_method",
							 node->accessMethod);
	else
	{
		append_null_object(tmp_obj, "%{access_method}I");
		append_not_present(tmp_obj);
	}
	append_object_object(ret, "%{access_method}s", tmp_obj);

	/* WITH clause */
	tmp_obj = new_objtree("WITH");

	foreach(cell, node->options)
	{
		ObjTree    *tmp_obj2;
		DefElem    *opt = (DefElem *) lfirst(cell);

		tmp_obj2 = deparse_DefElem(opt, false);
		list = lappend(list, new_object_object(tmp_obj2));
	}

	if (list)
		append_array_object(tmp_obj, "(%{with:, }s)", list);
	else
		append_not_present(tmp_obj);

	append_object_object(ret, "%{with_clause}s", tmp_obj);

	relation_close(relation, AccessShareLock);

	return ret;
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
 * Deparse all the collected subcommands and return an ObjTree representing the
 * alter command.
 *
 * Verbose syntax
 * ALTER reltype %{identity}D %{subcmds:, }s
 */
static ObjTree *
deparse_AlterRelation(CollectedCommand *cmd)
{
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	ObjTree    *tmp_obj2;
	List	   *dpcontext;
	Relation	rel;
	List	   *subcmds = NIL;
	ListCell   *cell;
	const char *reltype;
	bool		istype = false;
	bool		istable = false;
	List	   *exprs = NIL;
	Oid			relId = cmd->d.alterTable.objectId;
	AlterTableStmt *stmt = NULL;

	Assert(cmd->type == SCT_AlterTable);
	stmt = (AlterTableStmt *) cmd->parsetree;
	Assert(IsA(stmt, AlterTableStmt));

	/*
	 * ALTER TABLE subcommands generated for TableLikeClause is processed in
	 * the top level CREATE TABLE command; return empty here.
	 */
	if (stmt->table_like)
		return NULL;

	rel = relation_open(relId, AccessShareLock);
	dpcontext = deparse_context_for(RelationGetRelationName(rel),
									relId);

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_PARTITIONED_TABLE:
			reltype = "TABLE";
			istable = true;
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
		case RELKIND_MATVIEW:
			reltype = "MATERIALIZED VIEW";
			break;

			/* TODO support for partitioned table */

		default:
			elog(ERROR, "unexpected relkind %d", rel->rd_rel->relkind);
	}

	ret = new_objtree_VA("ALTER %{objtype}s %{identity}D", 2,
						 "objtype", ObjTypeString, reltype,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(rel->rd_rel->relnamespace,
												  RelationGetRelationName(rel)));

	foreach(cell, cmd->d.alterTable.subcmds)
	{
		CollectedATSubcmd *sub = (CollectedATSubcmd *) lfirst(cell);
		AlterTableCmd *subcmd = (AlterTableCmd *) sub->parsetree;
		ObjTree    *tree;

		Assert(IsA(subcmd, AlterTableCmd));

	   /*
		* Skip deparse of the subcommand if the objectId doesn't match the
		* target relation ID. It can happen for inherited tables when
		* subcommands for inherited tables and the parent table are both
		* collected in the ALTER TALBE command for the parent table. With the
		* exception of the internally generated AddConstraint (for
		* ALTER TABLE ADD CONSTRAINT FOREIGN KEY REFERENCES) where the
		* objectIds could mismatch (forein table id and the referenced table
		* id).
		*/
		if ((sub->address.objectId != relId &&
			 sub->address.objectId != InvalidOid) &&
			!(subcmd->subtype == AT_AddConstraint &&
			  subcmd->recurse) &&
			istable)
			continue;

		switch (subcmd->subtype)
		{
			case AT_AddColumn:
				/* XXX need to set the "recurse" bit somewhere? */
				Assert(IsA(subcmd->def, ColumnDef));
				tree = deparse_ColumnDef(rel, dpcontext, false,
										 (ColumnDef *) subcmd->def, true, &exprs);
				tmp_obj = new_objtree_VA("ADD %{objtype}s %{if_not_exists}s %{definition}s", 4,
										"objtype", ObjTypeString,
										istype ? "ATTRIBUTE" : "COLUMN",
										"type", ObjTypeString, "add column",
										"if_not_exists", ObjTypeString,
										subcmd->missing_ok ? "IF NOT EXISTS" : "",
										"definition", ObjTypeObject, tree);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
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
					tmp_obj = new_objtree_VA("ADD CONSTRAINT %{name}I %{constraint_type}s USING INDEX %{index_name}I %{deferrable}s %{init_deferred}s", 6,
											"type", ObjTypeString, "add constraint using index",
											"name", ObjTypeString, get_constraint_name(constrOid),
											"constraint_type", ObjTypeString,
											istmt->primary ? "PRIMARY KEY" : "UNIQUE",
											"index_name", ObjTypeString,
											RelationGetRelationName(idx),
											"deferrable", ObjTypeString,
											istmt->deferrable ? "DEFERRABLE" : "NOT DEFERRABLE",
											"init_deferred", ObjTypeString,
											istmt->initdeferred ? "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE");

					subcmds = lappend(subcmds, new_object_object(tmp_obj));

					relation_close(idx, AccessShareLock);
				}
				break;

			case AT_ReAddIndex:
			case AT_ReAddConstraint:
			case AT_ReAddComment:
			case AT_ReplaceRelOptions:
			case AT_CheckNotNull:
			case AT_ReAddStatistics:
				/* Subtypes used for internal operations; nothing to do here */
				break;

			case AT_CookedColumnDefault:
				{
					Relation	attrrel;
					HeapTuple	atttup;
					Form_pg_attribute attStruct;

					attrrel = table_open(AttributeRelationId, RowExclusiveLock);
					atttup = SearchSysCacheCopy2(ATTNUM,
												 ObjectIdGetDatum(RelationGetRelid(rel)),
												 Int16GetDatum(subcmd->num));
					if (!HeapTupleIsValid(atttup))
						elog(ERROR, "cache lookup failed for attribute %d of relation with OID %u",
							 subcmd->num, RelationGetRelid(rel));
					attStruct = (Form_pg_attribute) GETSTRUCT(atttup);

					/*
					 * Both default and generation expression not supported
					 * together.
					 */
					if (!attStruct->attgenerated)
						elog(WARNING, "unsupported alter table subtype %d",
							 subcmd->subtype);

					heap_freetuple(atttup);
					table_close(attrrel, RowExclusiveLock);
					break;
				}

			case AT_AddColumnToView:
				/* CREATE OR REPLACE VIEW -- nothing to do here */
				break;

			case AT_ColumnDefault:
				if (subcmd->def == NULL)
					tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I DROP DEFAULT", 2,
											"type", ObjTypeString, "drop default",
											"column", ObjTypeString, subcmd->name);
				else
				{
					List	   *dpcontext_rel;
					HeapTuple	attrtup;
					AttrNumber	attno;

					tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET DEFAULT", 2,
											"type", ObjTypeString, "set default",
											"column", ObjTypeString, subcmd->name);

					dpcontext_rel = deparse_context_for(RelationGetRelationName(rel),
														RelationGetRelid(rel));
					attrtup = SearchSysCacheAttName(RelationGetRelid(rel), subcmd->name);
					attno = ((Form_pg_attribute) GETSTRUCT(attrtup))->attnum;
					append_string_object(tmp_obj, "%{definition}s", "definition",
										 RelationGetColumnDefault(rel, attno,
																  dpcontext_rel,
																  NULL));
					ReleaseSysCache(attrtup);
				}

				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropNotNull:
				tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I DROP NOT NULL", 2,
										"type", ObjTypeString, "drop not null",
										"column", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_ForceRowSecurity:
				tmp_obj = new_objtree("FORCE ROW LEVEL SECURITY");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_NoForceRowSecurity:
				tmp_obj = new_objtree("NO FORCE ROW LEVEL SECURITY");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_SetNotNull:
				tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET NOT NULL", 2,
										"type", ObjTypeString, "set not null",
										"column", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropExpression:
				tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I DROP EXPRESSION %{if_exists}s", 3,
										"type", ObjTypeString, "drop expression",
										"column", ObjTypeString, subcmd->name,
										"if_exists", ObjTypeString,
										subcmd->missing_ok ? "IF EXISTS" : "");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_SetStatistics:
				{
					Assert(IsA(subcmd->def, Integer));
					if (subcmd->name)
						tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET STATISTICS %{statistics}n", 3,
												"type", ObjTypeString, "set statistics",
												"column", ObjTypeString, subcmd->name,
												"statistics", ObjTypeInteger,
												intVal((Integer *) subcmd->def));
					else
						tmp_obj = new_objtree_VA("ALTER COLUMN %{column}n SET STATISTICS %{statistics}n", 3,
												"type", ObjTypeString, "set statistics",
												"column", ObjTypeInteger, subcmd->num,
												"statistics", ObjTypeInteger,
												intVal((Integer *) subcmd->def));
					subcmds = lappend(subcmds, new_object_object(tmp_obj));
				}
				break;

			case AT_SetOptions:
			case AT_ResetOptions:
				subcmds = lappend(subcmds, new_object_object(
															 deparse_ColumnSetOptions(subcmd)));
				break;

			case AT_SetStorage:
				Assert(IsA(subcmd->def, String));
				tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET STORAGE %{storage}s", 3,
										"type", ObjTypeString, "set storage",
										"column", ObjTypeString, subcmd->name,
										"storage", ObjTypeString,
										strVal((String *) subcmd->def));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_SetCompression:
				Assert(IsA(subcmd->def, String));
				tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET COMPRESSION %{compression_method}s", 3,
										"type", ObjTypeString, "set compression",
										"column", ObjTypeString, subcmd->name,
										"compression_method", ObjTypeString,
										strVal((String *) subcmd->def));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropColumn:
				tmp_obj = new_objtree_VA("DROP %{objtype}s %{if_not_exists}s %{column}I", 4,
										"objtype", ObjTypeString,
										istype ? "ATTRIBUTE" : "COLUMN",
										"type", ObjTypeString, "drop column",
										"if_not_exists", ObjTypeString,
										subcmd->missing_ok ? "IF NOT EXISTS" : "",
										"column", ObjTypeString, subcmd->name);
				tmp_obj2 = new_objtree_VA("CASCADE", 1,
										 "present", ObjTypeBool, subcmd->behavior);
				append_object_object(tmp_obj, "%{cascade}s", tmp_obj2);

				subcmds = lappend(subcmds, new_object_object(tmp_obj));
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

					tmp_obj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
											"type", ObjTypeString, "add constraint",
											"name", ObjTypeString, idxname,
											"definition", ObjTypeString,
											pg_get_constraintdef_command_simple(constrOid));
					subcmds = lappend(subcmds, new_object_object(tmp_obj));

					relation_close(idx, AccessShareLock);
				}
				break;

			case AT_AddConstraint:
				{
					/* XXX need to set the "recurse" bit somewhere? */
					Oid			constrOid = sub->address.objectId;
					bool		isnull;
					HeapTuple	tup;
					Datum		val;
					Constraint *constr;

					Assert(IsA(subcmd->def, Constraint));
					constr = castNode(Constraint, subcmd->def);

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

					tmp_obj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
											"type", ObjTypeString, "add constraint",
											"name", ObjTypeString, get_constraint_name(constrOid),
											"definition", ObjTypeString,
											pg_get_constraintdef_command_simple(constrOid));
					subcmds = lappend(subcmds, new_object_object(tmp_obj));
				}
				break;

			case AT_AlterConstraint:
				{
					Oid			constrOid = sub->address.objectId;
					Constraint *c = (Constraint *) subcmd->def;

					/* If no constraint was altered, silently skip it */
					if (!OidIsValid(constrOid))
						break;

					Assert(IsA(c, Constraint));
					tmp_obj = new_objtree_VA("ALTER CONSTRAINT %{name}I %{deferrable}s %{init_deferred}s", 4,
											"type", ObjTypeString, "alter constraint",
											"name", ObjTypeString, get_constraint_name(constrOid),
											"deferrable", ObjTypeString,
											c->deferrable ? "DEFERRABLE" : "NOT DEFERRABLE",
											"init_deferred", ObjTypeString,
											c->initdeferred ? "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE");

					subcmds = lappend(subcmds, new_object_object(tmp_obj));
				}
				break;

			case AT_ValidateConstraint:
				tmp_obj = new_objtree_VA("VALIDATE CONSTRAINT %{constraint}I", 2,
										"type", ObjTypeString, "validate constraint",
										"constraint", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropConstraint:
				tmp_obj = new_objtree_VA("DROP CONSTRAINT %{constraint}I %{if_not_exists}s", 3,
										"type", ObjTypeString, "drop constraint",
										"if_not_exists", ObjTypeString,
										subcmd->missing_ok ? "IF NOT EXISTS" : "",
										"constraint", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_AlterColumnType:
				{
					TupleDesc	tupdesc = RelationGetDescr(rel);
					Form_pg_attribute att;
					ColumnDef  *def;

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
					tmp_obj = new_objtree_VA("ALTER %{objtype}s %{column}I SET DATA TYPE %{datatype}T", 4,
											"objtype", ObjTypeString,
											istype ? "ATTRIBUTE" : "COLUMN",
											"type", ObjTypeString, "alter column type",
											"column", ObjTypeString, subcmd->name,
											"datatype", ObjTypeObject,
											new_objtree_for_type(att->atttypid,
																 att->atttypmod));

					/* Add a COLLATE clause, if needed */
					tmp_obj2 = new_objtree("COLLATE");
					if (OidIsValid(att->attcollation))
					{
						ObjTree    *collname;

						collname = new_objtree_for_qualname_id(CollationRelationId,
															   att->attcollation);
						append_object_object(tmp_obj2, "%{name}D", collname);
					}
					else
						append_not_present(tmp_obj2);
					append_object_object(tmp_obj, "%{collation}s", tmp_obj2);

					/* If not a composite type, add the USING clause */
					if (!istype)
					{
						/*
						 * If there's a USING clause, transformAlterTableStmt
						 * ran it through transformExpr and stored the
						 * resulting node in cooked_default, which we can use
						 * here.
						 */
						tmp_obj2 = new_objtree("USING");
						if (def->raw_default)
							append_string_object(tmp_obj2, "%{expression}s",
												 "expression",
												 sub->usingexpr);
						else
							append_not_present(tmp_obj2);
						append_object_object(tmp_obj, "%{using}s", tmp_obj2);
					}

					/* If it's a composite type, add the CASCADE clause */
					if (istype)
					{
						tmp_obj2 = new_objtree("CASCADE");
						if (subcmd->behavior != DROP_CASCADE)
							append_not_present(tmp_obj2);
						append_object_object(tmp_obj, "%{cascade}s", tmp_obj2);
					}

					subcmds = lappend(subcmds, new_object_object(tmp_obj));
				}
				break;

#ifdef TODOLIST
			case AT_AlterColumnGenericOptions:
				tmp_obj = deparse_FdwOptions((List *) subcmd->def,
											subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
#endif
			case AT_ChangeOwner:
				tmp_obj = new_objtree_VA("OWNER TO %{owner}I", 2,
										"type", ObjTypeString, "change owner",
										"owner", ObjTypeString,
										get_rolespec_name(subcmd->newowner));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_ClusterOn:
				tmp_obj = new_objtree_VA("CLUSTER ON %{index}I", 2,
										"type", ObjTypeString, "cluster on",
										"index", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropCluster:
				tmp_obj = new_objtree_VA("SET WITHOUT CLUSTER", 1,
										"type", ObjTypeString, "set without cluster");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_SetLogged:
				tmp_obj = new_objtree_VA("SET LOGGED", 1,
										"type", ObjTypeString, "set logged");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_SetUnLogged:
				tmp_obj = new_objtree_VA("SET UNLOGGED", 1,
										"type", ObjTypeString, "set unlogged");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropOids:
				tmp_obj = new_objtree_VA("SET WITHOUT OIDS", 1,
										"type", ObjTypeString, "set without oids");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
			case AT_SetAccessMethod:
				tmp_obj = new_objtree_VA("SET ACCESS METHOD %{access_method}I", 2,
										"type", ObjTypeString, "set access method",
										"access_method", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
			case AT_SetTableSpace:
				tmp_obj = new_objtree_VA("SET TABLESPACE %{tablespace}I", 2,
										"type", ObjTypeString, "set tablespace",
										"tablespace", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_SetRelOptions:
			case AT_ResetRelOptions:
				subcmds = lappend(subcmds, new_object_object(
															 deparse_RelSetOptions(subcmd)));
				break;

			case AT_EnableTrig:
				tmp_obj = new_objtree_VA("ENABLE TRIGGER %{trigger}I", 2,
										"type", ObjTypeString, "enable trigger",
										"trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableAlwaysTrig:
				tmp_obj = new_objtree_VA("ENABLE ALWAYS TRIGGER %{trigger}I", 2,
										"type", ObjTypeString, "enable always trigger",
										"trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableReplicaTrig:
				tmp_obj = new_objtree_VA("ENABLE REPLICA TRIGGER %{trigger}I", 2,
										"type", ObjTypeString, "enable replica trigger",
										"trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DisableTrig:
				tmp_obj = new_objtree_VA("DISABLE TRIGGER %{trigger}I", 2,
										"type", ObjTypeString, "disable trigger",
										"trigger", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableTrigAll:
				tmp_obj = new_objtree_VA("ENABLE TRIGGER ALL", 1,
										"type", ObjTypeString, "enable trigger all");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DisableTrigAll:
				tmp_obj = new_objtree_VA("DISABLE TRIGGER ALL", 1,
										"type", ObjTypeString, "disable trigger all");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableTrigUser:
				tmp_obj = new_objtree_VA("ENABLE TRIGGER USER", 1,
										"type", ObjTypeString, "enable trigger user");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DisableTrigUser:
				tmp_obj = new_objtree_VA("DISABLE TRIGGER USER", 1,
										"type", ObjTypeString, "disable trigger user");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableRule:
				tmp_obj = new_objtree_VA("ENABLE RULE %{rule}I", 2,
										"type", ObjTypeString, "enable rule",
										"rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableAlwaysRule:
				tmp_obj = new_objtree_VA("ENABLE ALWAYS RULE %{rule}I", 2,
										"type", ObjTypeString, "enable always rule",
										"rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableReplicaRule:
				tmp_obj = new_objtree_VA("ENABLE REPLICA RULE %{rule}I", 2,
										"type", ObjTypeString, "enable replica rule",
										"rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DisableRule:
				tmp_obj = new_objtree_VA("DISABLE RULE %{rule}I", 2,
										"type", ObjTypeString, "disable rule",
										"rule", ObjTypeString, subcmd->name);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_AddInherit:
				tmp_obj = new_objtree_VA("INHERIT %{parent}D", 2,
										"type", ObjTypeString, "inherit",
										"parent", ObjTypeObject,
										new_objtree_for_qualname_id(RelationRelationId,
																	sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropInherit:
				tmp_obj = new_objtree_VA("NO INHERIT %{parent}D", 2,
										"type", ObjTypeString, "drop inherit",
										"parent", ObjTypeObject,
										new_objtree_for_qualname_id(RelationRelationId,
																	sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_AddOf:
				tmp_obj = new_objtree_VA("OF %{type_of}T", 2,
										"type", ObjTypeString, "add of",
										"type_of", ObjTypeObject,
										new_objtree_for_type(sub->address.objectId, -1));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DropOf:
				tmp_obj = new_objtree_VA("NOT OF", 1,
										"type", ObjTypeString, "not of");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_ReplicaIdentity:
				tmp_obj = new_objtree_VA("REPLICA IDENTITY", 1,
										"type", ObjTypeString, "replica identity");
				switch (((ReplicaIdentityStmt *) subcmd->def)->identity_type)
				{
					case REPLICA_IDENTITY_DEFAULT:
						append_string_object(tmp_obj, "%{ident}s", "ident",
											 "DEFAULT");
						break;
					case REPLICA_IDENTITY_FULL:
						append_string_object(tmp_obj, "%{ident}s", "ident",
											 "FULL");
						break;
					case REPLICA_IDENTITY_NOTHING:
						append_string_object(tmp_obj, "%{ident}s", "ident",
											 "NOTHING");
						break;
					case REPLICA_IDENTITY_INDEX:
						tmp_obj2 = new_objtree_VA("USING INDEX %{index}I", 1,
												 "index", ObjTypeString,
												 ((ReplicaIdentityStmt *) subcmd->def)->name);
						append_object_object(tmp_obj, "%{ident}s", tmp_obj2);
						break;
				}
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_EnableRowSecurity:
				tmp_obj = new_objtree_VA("ENABLE ROW LEVEL SECURITY", 1,
										"type", ObjTypeString, "enable row security");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;

			case AT_DisableRowSecurity:
				tmp_obj = new_objtree_VA("DISABLE ROW LEVEL SECURITY", 1,
										"type", ObjTypeString, "disable row security");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
#ifdef TODOLIST
			case AT_GenericOptions:
				tmp_obj = deparse_FdwOptions((List *) subcmd->def, NULL);
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
#endif
			case AT_AttachPartition:
				tmp_obj = new_objtree_VA("ATTACH PARTITION %{partition_identity}D", 2,
										"type", ObjTypeString, "attach partition",
										"partition_identity", ObjTypeObject,
										new_objtree_for_qualname_id(RelationRelationId,
																	sub->address.objectId));

				if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
					append_string_object(tmp_obj, "%{partition_bound}s",
										 "partition_bound",
										 RelationGetPartitionBound(sub->address.objectId));

				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
			case AT_DetachPartition:
			{
				PartitionCmd *cmd;

				Assert(IsA(subcmd->def, PartitionCmd));
				cmd = (PartitionCmd *) subcmd->def;

				tmp_obj = new_objtree_VA("DETACH PARTITION %{partition_identity}D %{concurrent}s", 3,
										"type", ObjTypeString,
										"detach partition",
										"partition_identity", ObjTypeObject,
										new_objtree_for_qualname_id(RelationRelationId,
																	sub->address.objectId),
										cmd->concurrent ? "CONCURRENTLY" : "");
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
			}
			case AT_DetachPartitionFinalize:
				tmp_obj = new_objtree_VA("DETACH PARTITION %{partition_identity}D FINALIZE", 2,
										"type", ObjTypeString, "detach partition finalize",
										"partition_identity", ObjTypeObject,
										new_objtree_for_qualname_id(RelationRelationId,
																	sub->address.objectId));
				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
			case AT_AddIdentity:
				{
					AttrNumber	attnum;
					Oid			seq_relid;
					ObjTree    *seqdef;
					ColumnDef  *coldef = (ColumnDef *) subcmd->def;

					tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I", 2,
											"type", ObjTypeString, "add identity",
											"column", ObjTypeString, subcmd->name);

					attnum = get_attnum(RelationGetRelid(rel), subcmd->name);
					seq_relid = getIdentitySequence(RelationGetRelid(rel), attnum, true);

					if (OidIsValid(seq_relid))
					{
						seqdef = deparse_ColumnIdentity(seq_relid, coldef->identity, false);
						append_object_object(tmp_obj, "ADD %{identity_column}s", seqdef);
					}

					subcmds = lappend(subcmds, new_object_object(tmp_obj));
				}
				break;
			case AT_SetIdentity:
				{
					DefElem    *defel;
					char		identity = 0;
					ObjTree    *seqdef;
					AttrNumber	attnum;
					Oid			seq_relid;


					tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I", 2,
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

					if (OidIsValid(seq_relid))
					{
						seqdef = deparse_ColumnIdentity(seq_relid, identity, true);
						append_object_object(tmp_obj, "%{definition}s", seqdef);
					}

					subcmds = lappend(subcmds, new_object_object(tmp_obj));
					break;
				}
			case AT_DropIdentity:
				tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I DROP IDENTITY", 2,
										"type", ObjTypeString, "drop identity",
										"column", ObjTypeString, subcmd->name);

				append_string_object(tmp_obj, "%{if_exists}s",
									 "if_exists",
									 subcmd->missing_ok ? "IF EXISTS" : "");

				subcmds = lappend(subcmds, new_object_object(tmp_obj));
				break;
			default:
				elog(WARNING, "unsupported alter table subtype %d",
					 subcmd->subtype);
				break;
		}

		/*
		 * We don't support replicating ALTER TABLE which contains volatile
		 * functions because It's possible the functions contain DDL/DML in
		 * which case these operations will be executed twice and cause
		 * duplicate data. In addition, we don't know whether the tables being
		 * accessed by these DDL/DML are published or not. So blindly allowing
		 * such functions can allow unintended clauses like the tables
		 * accessed in those functions may not even exist on the subscriber.
		 */
		if (contain_volatile_functions((Node *) exprs))
			elog(ERROR, "ALTER TABLE command using volatile function cannot be replicated");

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

	append_array_object(ret, "%{subcmds:, }s", subcmds);

	return ret;
}

/*
 * Handle deparsing of DROP commands.
 *
 * Verbose syntax
 * DROP %s IF EXISTS %%{objidentity}s %{cascade}s
 */
char *
deparse_drop_command(const char *objidentity, const char *objecttype,
					 DropBehavior behavior)
{
	StringInfoData str;
	char	   *command;
	char	   *identity = (char *) objidentity;
	ObjTree    *stmt;
	ObjTree    *tmp_obj;
	Jsonb	   *jsonb;

	initStringInfo(&str);

	stmt = new_objtree_VA("DROP %{objtype}s IF EXISTS %{objidentity}s", 2,
						  "objtype", ObjTypeString, objecttype,
						  "objidentity", ObjTypeString, identity);

	tmp_obj = new_objtree_VA("CASCADE", 1,
							"present", ObjTypeBool, behavior == DROP_CASCADE);
	append_object_object(stmt, "%{cascade}s", tmp_obj);

	jsonb = objtree_to_jsonb(stmt);
	command = JsonbToCString(&str, &jsonb->root, JSONB_ESTIMATED_LEN);

	return command;
}

/*
 * Deparse an AlterCollationStmt (ALTER COLLATION)
 *
 * Given a collation OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax:
 * ALTER COLLATION %{identity}O REFRESH VERSION
 */
static ObjTree *
deparse_AlterCollation(Oid objectId, Node *parsetree)
{
	ObjTree    *ret;
	HeapTuple	colTup;
	Form_pg_collation colForm;

	colTup = SearchSysCache1(COLLOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(colTup))
		elog(ERROR, "cache lookup failed for collation with OID %u", objectId);
	colForm = (Form_pg_collation) GETSTRUCT(colTup);

	ret = new_objtree_VA("ALTER COLLATION %{identity}O REFRESH VERSION", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(colForm->collnamespace,
												  NameStr(colForm->collname)));

	ReleaseSysCache(colTup);

	return ret;
}

/*
 * Handle deparsing setting of Function
 *
 * Verbose syntax
 * RESET ALL
 * OR
 * SET %{set_name}I TO %{set_value}s
 * OR
 * RESET %{set_name}I
 */
static ObjTree *
deparse_FunctionSet(VariableSetKind kind, char *name, char *value)
{
	ObjTree    *ret;

	if (kind == VAR_RESET_ALL)
		ret = new_objtree("RESET ALL");
	else if (kind == VAR_SET_VALUE)
	{
		ret = new_objtree_VA("SET %{set_name}I", 1,
							 "set_name", ObjTypeString, name);

		/*
		 * Some GUC variable names are 'LIST' type and hence must not be
		 * quoted.
		 */
		if (GetConfigOptionFlags(name, true) & GUC_LIST_QUOTE)
			append_string_object(ret, "TO %{set_value}s", "set_value", value);
		else
			append_string_object(ret, "TO %{set_value}L", "set_value", value);
	}
	else
		ret = new_objtree_VA("RESET %{set_name}I", 1,
							 "set_name", ObjTypeString, name);

	return ret;
}

/*
 * Deparse an AlterFunctionStmt (ALTER FUNCTION/ROUTINE/PROCEDURE)
 *
 * Given a function OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax:
 * ALTER FUNCTION/ROUTINE/PROCEDURE %{signature}s %{definition: }s
 */
static ObjTree *
deparse_AlterFunction(Oid objectId, Node *parsetree)
{
	AlterFunctionStmt *node = (AlterFunctionStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *sign;
	HeapTuple	procTup;
	Form_pg_proc procForm;
	List	   *params = NIL;
	List	   *elems = NIL;
	ListCell   *cell;
	int			i;

	/* Get the pg_proc tuple */
	procTup = SearchSysCache1(PROCOID, objectId);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failed for function with OID %u", objectId);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	if (procForm->prokind == PROKIND_PROCEDURE)
		ret = new_objtree("ALTER PROCEDURE");
	else
		ret = new_objtree(node->objtype == OBJECT_ROUTINE ?
						  "ALTER ROUTINE" : "ALTER FUNCTION");

	/*
	 * ALTER FUNCTION does not change signature so we can use catalog to get
	 * input type Oids.
	 */
	for (i = 0; i < procForm->pronargs; i++)
	{
		ObjTree    *tmp_obj;

		tmp_obj = new_objtree_VA("%{type}T", 1,
								"type", ObjTypeObject,
								new_objtree_for_type(procForm->proargtypes.values[i], -1));
		params = lappend(params, new_object_object(tmp_obj));
	}

	sign = new_objtree_VA("%{identity}D (%{arguments:, }s)", 2,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname_id(ProcedureRelationId, objectId),
						  "arguments", ObjTypeArray, params);

	append_object_object(ret, "%{signature}s", sign);

	foreach(cell, node->actions)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);
		ObjTree    *tmp_obj = NULL;

		if (strcmp(defel->defname, "volatility") == 0)
			tmp_obj = new_objtree(strVal(defel->arg));
		else if (strcmp(defel->defname, "strict") == 0)
			tmp_obj = new_objtree(boolVal(defel->arg) ?
								 "RETURNS NULL ON NULL INPUT" :
								 "CALLED ON NULL INPUT");
		else if (strcmp(defel->defname, "security") == 0)
			tmp_obj = new_objtree(boolVal(defel->arg) ?
								 "SECURITY DEFINER" : "SECURITY INVOKER");
		else if (strcmp(defel->defname, "leakproof") == 0)
			tmp_obj = new_objtree(boolVal(defel->arg) ?
								 "LEAKPROOF" : "NOT LEAKPROOF");
		else if (strcmp(defel->defname, "cost") == 0)
			tmp_obj = new_objtree_VA("COST %{cost}n", 1,
									"cost", ObjTypeFloat,
									defGetNumeric(defel));
		else if (strcmp(defel->defname, "rows") == 0)
		{
			tmp_obj = new_objtree("ROWS");
			if (defGetNumeric(defel) == 0)
				append_not_present(tmp_obj);
			else
				append_float_object(tmp_obj, "%{rows}n",
									defGetNumeric(defel));
		}
		else if (strcmp(defel->defname, "set") == 0)
		{
			VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
			char	   *value = ExtractSetVariableArgs(sstmt);

			tmp_obj = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
		}
		else if (strcmp(defel->defname, "support") == 0)
		{
			Oid			argtypes[1];

			tmp_obj = new_objtree("SUPPORT");

			Assert(procForm->prosupport);

			/*
			 * We should qualify the support function's name if it wouldn't be
			 * resolved by lookup in the current search path.
			 */
			argtypes[0] = INTERNALOID;
			append_string_object(tmp_obj, "%{name}s", "name",
								 generate_function_name(procForm->prosupport, 1,
														NIL, argtypes,
														false, NULL,
														EXPR_KIND_NONE));
		}
		else if (strcmp(defel->defname, "parallel") == 0)
			tmp_obj = new_objtree_VA("PARALLEL %{value}s", 1,
									"value", ObjTypeString, strVal(defel->arg));

		elems = lappend(elems, new_object_object(tmp_obj));
	}

	append_array_object(ret, "%{definition: }s", elems);

	ReleaseSysCache(procTup);

	return ret;
}

/*
 * Deparse an AlterObjectSchemaStmt (ALTER ... SET SCHEMA command)
 *
 * Given the object address and the parse tree that created it, return an
 * ObjTree representing the alter command.
 *
 * Verbose syntax
 * ALTER %s %{identity}s SET SCHEMA %{newschema}I
 */
static ObjTree *
deparse_AlterObjectSchemaStmt(ObjectAddress address, Node *parsetree,
							  ObjectAddress old_schema)
{
	AlterObjectSchemaStmt *node = (AlterObjectSchemaStmt *) parsetree;
	char	   *identity;
	char	   *new_schema = node->newschema;
	char	   *old_schname;
	char	   *ident;

	/*
	 * Since the command has already taken place from the point of view of
	 * catalogs, getObjectIdentity returns the object name with the already
	 * changed schema.  The output of our deparsing must return the original
	 * schema name, however, so we chop the schema name off the identity
	 * string and then prepend the quoted schema name.
	 *
	 * XXX This is pretty clunky. Can we do better?
	 */
	identity = getObjectIdentity(&address, false);
	old_schname = get_namespace_name(old_schema.objectId);
	if (!old_schname)
		elog(ERROR, "cache lookup failed for schema with OID %u",
			 old_schema.objectId);

	ident = psprintf("%s%s", quote_identifier(old_schname),
					 identity + strlen(quote_identifier(new_schema)));

	return new_objtree_VA("ALTER %{objtype}s %{identity}s SET SCHEMA %{newschema}I", 3,
						  "objtype", ObjTypeString,
						  stringify_objtype(node->objectType, false),
						  "identity", ObjTypeString, ident,
						  "newschema", ObjTypeString, new_schema);
}

/*
 * Deparse a GRANT/REVOKE command.
 *
 * Verbose syntax
 * GRANT %{privileges:, }s ON" %{objtype}s %{privtarget:, }s TO %{grantees:, }s
 * %{grant_option}s GRANTED BY %{rolename}s
 * 		OR
 * REVOKE %{privileges:, }s ON" %{objtype}s %{privtarget:, }s
 * FROM %{grantees:, }s %{grant_option}s %{cascade}s
 */
static ObjTree *
deparse_GrantStmt(CollectedCommand *cmd)
{
	InternalGrant *istmt;
	ObjTree    *ret;
	List	   *list = NIL;
	ListCell   *cell;
	Oid			classId;
	ObjTree    *tmp;

	/* Don't deparse SQL commands generated while creating extension */
	if (cmd->in_extension)
		return NULL;

	istmt = cmd->d.grant.istmt;

	/*
	 * If there are no objects from "ALL ... IN SCHEMA" to be granted, then
	 * nothing to do.
	 */
	if (istmt->objects == NIL)
		return NULL;

	switch (istmt->objtype)
	{
		case OBJECT_COLUMN:
		case OBJECT_TABLE:
		case OBJECT_SEQUENCE:
			classId = RelationRelationId;
			break;
		case OBJECT_DOMAIN:
		case OBJECT_TYPE:
			classId = TypeRelationId;
			break;
		case OBJECT_FDW:
			classId = ForeignDataWrapperRelationId;
			break;
		case OBJECT_FOREIGN_SERVER:
			classId = ForeignServerRelationId;
			break;
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
			classId = ProcedureRelationId;
			break;
		case OBJECT_LANGUAGE:
			classId = LanguageRelationId;
			break;
		case OBJECT_LARGEOBJECT:
			classId = LargeObjectRelationId;
			break;
		case OBJECT_SCHEMA:
			classId = NamespaceRelationId;
			break;
		case OBJECT_DATABASE:
		case OBJECT_TABLESPACE:
			classId = InvalidOid;
			elog(ERROR, "global objects not supported");
			break;
		default:
			elog(ERROR, "invalid OBJECT value %d", istmt->objtype);
	}

	/* GRANT TO or REVOKE FROM */
	ret = new_objtree(istmt->is_grant ? "GRANT" : "REVOKE");

	/* Build a list of privileges to grant/revoke */
	if (istmt->all_privs)
	{
		tmp = new_objtree("ALL PRIVILEGES");
		list = list_make1(new_object_object(tmp));
	}
	else
	{
		char *priv;
		if (istmt->privileges & ACL_INSERT)
		{
			priv = (char *)privilege_to_string(ACL_INSERT);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_SELECT)
		{
			priv = (char *)privilege_to_string(ACL_SELECT);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_UPDATE)
		{
			priv = (char *)privilege_to_string(ACL_UPDATE);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_DELETE)
		{
			priv = (char *)privilege_to_string(ACL_DELETE);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_TRUNCATE)
		{
			priv = (char *)privilege_to_string(ACL_TRUNCATE);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_REFERENCES)
		{
			priv = (char *)privilege_to_string(ACL_REFERENCES);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_TRIGGER)
		{
			priv = (char *)privilege_to_string(ACL_TRIGGER);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_EXECUTE)
		{
			priv = (char *)privilege_to_string(ACL_EXECUTE);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_USAGE)
		{
			priv = (char *)privilege_to_string(ACL_USAGE);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_CREATE)
		{
			priv = (char *)privilege_to_string(ACL_CREATE);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_CREATE_TEMP)
		{
			priv = (char *)privilege_to_string(ACL_CREATE_TEMP);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_CONNECT)
		{
			priv = (char *)privilege_to_string(ACL_CONNECT);
			list = lappend(list, new_string_object(priv));
		}
		if (istmt->privileges & ACL_MAINTAIN)
		{
			priv = (char *)privilege_to_string(ACL_MAINTAIN);
			list = lappend(list, new_string_object(priv));
		}

		if (!istmt->is_grant && istmt->grant_option)
			append_string_object(ret, "%{grant_option}s", "grant_option",
								 istmt->grant_option ? "GRANT OPTION FOR" : "");

		if (istmt->col_privs != NIL)
		{
			ListCell   *ocell;

			foreach(ocell, istmt->col_privs)
			{
				AccessPriv *priv = lfirst(ocell);
				List	   *cols = NIL;

				foreach(cell, priv->cols)
				{
					String	   *colname = lfirst_node(String, cell);

					cols = lappend(cols,
								   new_string_object(strVal(colname)));
				}

				tmp = new_objtree_VA("(%{cols:, }s) %{priv}s", 2,
									 "cols", ObjTypeArray, cols,
									 "priv", ObjTypeString,
									  priv->priv_name ? priv->priv_name : "ALL PRIVILEGES");

				list = lappend(list, new_object_object(tmp));
			}
		}
	}
	append_array_object(ret, "%{privileges:, }s ON", list);

	append_string_object(ret, "%{objtype}s", "objtype",
						 (char *)stringify_objtype(istmt->objtype, true));

	/* Target objects.  We use object identities here */
	list = NIL;
	foreach(cell, istmt->objects)
	{
		Oid			objid = lfirst_oid(cell);
		ObjectAddress addr;

		addr.classId = classId;
		addr.objectId = objid;
		addr.objectSubId = 0;

		tmp = new_objtree_VA("%{identity}s", 1,
							 "identity", ObjTypeString,
							 getObjectIdentity(&addr, false));

		list = lappend(list, new_object_object(tmp));
	}
	append_array_object(ret, "%{privtarget:, }s", list);

	append_format_string(ret, istmt->is_grant ? "TO" : "FROM");

	/* List of grantees */
	list = NIL;
	foreach(cell, istmt->grantees)
	{
		Oid			grantee = lfirst_oid(cell);

		tmp = new_objtree_for_role_id(grantee);
		list = lappend(list, new_object_object(tmp));
	}

	append_array_object(ret, "%{grantees:, }s", list);

	/* The wording of the grant option is variable ... */
	if (istmt->is_grant)
		append_string_object(ret, "%{grant_option}s", "grant_option",
							 istmt->grant_option ? "WITH GRANT OPTION" : "");

	if (istmt->grantor_uid)
	{
		HeapTuple	roltup;
		char	   *rolename;

		roltup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(istmt->grantor_uid));
		if (!HeapTupleIsValid(roltup))
			elog(ERROR, "cache lookup failed for role with OID %u",
				 istmt->grantor_uid);

		rolename = NameStr(((Form_pg_authid) GETSTRUCT(roltup))->rolname);
		append_string_object(ret, "GRANTED BY %{rolename}s",
							 "rolename", rolename);
		ReleaseSysCache(roltup);
	}

	if (!istmt->is_grant)
		append_string_object(ret, "%{cascade}s", "cascade",
							 istmt->behavior == DROP_CASCADE ? "CASCADE" : "");

	return ret;
}

/*
 * Deparse an AlterOperatorStmt (ALTER OPERATOR ... SET ...).
 *
 * Given an operator OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax
 * ALTER OPERATOR %{identity}O (%{left_type}T, %{right_type}T)
 * SET (%{elems:, }s)
 */
static ObjTree *
deparse_AlterOperatorStmt(Oid objectId, Node *parsetree)
{
	HeapTuple	oprTup;
	AlterOperatorStmt *node = (AlterOperatorStmt *) parsetree;
	ObjTree    *ret;
	Form_pg_operator oprForm;
	ListCell   *cell;
	List	   *list = NIL;

	oprTup = SearchSysCache1(OPEROID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(oprTup))
		elog(ERROR, "cache lookup failed for operator with OID %u", objectId);
	oprForm = (Form_pg_operator) GETSTRUCT(oprTup);

	ret = new_objtree_VA("ALTER OPERATOR %{identity}O", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(oprForm->oprnamespace,
												  NameStr(oprForm->oprname)));

	/* LEFTARG */
	if (OidIsValid(oprForm->oprleft))
		append_object_object(ret, "(%{left_type}T",
							 new_objtree_for_type(oprForm->oprleft, -1));
	else
		append_string_object(ret, "(%{left_type}s", "left_type", "NONE");

	/* RIGHTARG */
	Assert(OidIsValid(oprForm->oprleft));
	append_object_object(ret, ", %{right_type}T)",
						 new_objtree_for_type(oprForm->oprright, -1));

	foreach(cell, node->options)
	{
		DefElem    *elem = (DefElem *) lfirst(cell);
		ObjTree    *tmp_obj = NULL;

		if (strcmp(elem->defname, "restrict") == 0)
		{
			tmp_obj = new_objtree_VA("RESTRICT=", 1,
									"clause", ObjTypeString, "restrict");
			if (OidIsValid(oprForm->oprrest))
				append_object_object(tmp_obj, "%{procedure}D",
									 new_objtree_for_qualname_id(ProcedureRelationId,
																 oprForm->oprrest));
			else
				append_string_object(tmp_obj, "%{procedure}s", "procedure",
									 "NONE");
		}
		else if (strcmp(elem->defname, "join") == 0)
		{
			tmp_obj = new_objtree_VA("JOIN=", 1,
									"clause", ObjTypeString, "join");
			if (OidIsValid(oprForm->oprjoin))
				append_object_object(tmp_obj, "%{procedure}D",
									 new_objtree_for_qualname_id(ProcedureRelationId,
																 oprForm->oprjoin));
			else
				append_string_object(tmp_obj, "%{procedure}s", "procedure",
									 "NONE");
		}

		Assert(tmp_obj);
		list = lappend(list, new_object_object(tmp_obj));
	}

	append_array_object(ret, "SET (%{elems:, }s)", list);

	ReleaseSysCache(oprTup);

	return ret;
}

/*
 * Deparse an ALTER OPERATOR FAMILY ADD/DROP command.
 *
 * Given the CollectedCommand, return an ObjTree representing the Alter
 * Operator command.
 *
 * Verbose syntax
 * ALTER OPERATOR FAMILY %{identity}D USING %{amname}I ADD/DROP %{items:,}s
 */
static ObjTree *
deparse_AlterOpFamily(CollectedCommand *cmd)
{
	ObjTree    *ret;
	AlterOpFamilyStmt *stmt = (AlterOpFamilyStmt *) cmd->parsetree;
	HeapTuple	ftp;
	Form_pg_opfamily opfForm;
	List	   *list = NIL;
	ListCell   *cell;

	/* Don't deparse SQL commands generated while creating extension */
	if (cmd->in_extension)
		return NULL;

	ftp = SearchSysCache1(OPFAMILYOID,
						  ObjectIdGetDatum(cmd->d.opfam.address.objectId));
	if (!HeapTupleIsValid(ftp))
		elog(ERROR, "cache lookup failed for operator family with OID %u",
			 cmd->d.opfam.address.objectId);
	opfForm = (Form_pg_opfamily) GETSTRUCT(ftp);

	ret = new_objtree_VA("ALTER OPERATOR FAMILY %{identity}D USING %{amname}I", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(opfForm->opfnamespace,
												  NameStr(opfForm->opfname)),
						 "amname", ObjTypeString, stmt->amname);

	foreach(cell, cmd->d.opfam.operators)
	{
		OpFamilyMember *oper = lfirst(cell);
		ObjTree    *tmp_obj;

		/*
		 * Verbose syntax
		 *
		 * OPERATOR %{num}n %{operator}O(%{ltype}T, %{rtype}T) %{purpose}s
		 */
		tmp_obj = new_objtree_VA("OPERATOR %{num}n", 1,
								"num", ObjTypeInteger, oper->number);

		/* Add the operator name; the DROP case doesn't have this */
		if (!stmt->isDrop)
			append_object_object(tmp_obj, "%{operator}O",
								 new_objtree_for_qualname_id(OperatorRelationId,
															 oper->object));

		/* Add the types */
		append_object_object(tmp_obj, "(%{ltype}T,",
							 new_objtree_for_type(oper->lefttype, -1));
		append_object_object(tmp_obj, "%{rtype}T)",
							 new_objtree_for_type(oper->righttype, -1));

		/* Add the FOR SEARCH / FOR ORDER BY clause; not in the DROP case */
		if (!stmt->isDrop)
		{
			if (oper->sortfamily == InvalidOid)
				append_string_object(tmp_obj, "%{purpose}s", "purpose",
									 "FOR SEARCH");
			else
			{
				ObjTree    *orderby_obj;

				orderby_obj = new_objtree_VA("FOR ORDER BY %{opfamily}D", 1,
											"opfamily", ObjTypeObject,
											new_objtree_for_qualname_id(OperatorFamilyRelationId,
																		oper->sortfamily));
				append_object_object(tmp_obj, "%{purpose}s", orderby_obj);
			}
		}

		list = lappend(list, new_object_object(tmp_obj));
	}

	foreach(cell, cmd->d.opfam.procedures)
	{
		OpFamilyMember *proc = lfirst(cell);
		ObjTree    *tmp_obj;

		tmp_obj = new_objtree_VA("FUNCTION %{num}n (%{ltype}T, %{rtype}T)", 3,
								"num", ObjTypeInteger, proc->number,
								"ltype", ObjTypeObject,
								new_objtree_for_type(proc->lefttype, -1),
								"rtype", ObjTypeObject,
								new_objtree_for_type(proc->righttype, -1));

		/*
		 * Add the function name and arg types; the DROP case doesn't have
		 * this
		 */
		if (!stmt->isDrop)
		{
			HeapTuple	procTup;
			Form_pg_proc procForm;
			Oid		   *proargtypes;
			List	   *arglist = NIL;
			int			i;

			procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc->object));
			if (!HeapTupleIsValid(procTup))
				elog(ERROR, "cache lookup failed for procedure with OID %u",
					 proc->object);
			procForm = (Form_pg_proc) GETSTRUCT(procTup);

			proargtypes = procForm->proargtypes.values;
			for (i = 0; i < procForm->pronargs; i++)
			{
				ObjTree    *arg;

				arg = new_objtree_for_type(proargtypes[i], -1);
				arglist = lappend(arglist, new_object_object(arg));
			}

			append_object_object(tmp_obj, "%{function}D",
								 new_objtree_for_qualname(procForm->pronamespace,
														  NameStr(procForm->proname)));

			append_format_string(tmp_obj, "(");
			append_array_object(tmp_obj, "%{argtypes:, }T", arglist);
			append_format_string(tmp_obj, ")");

			ReleaseSysCache(procTup);
		}

		list = lappend(list, new_object_object(tmp_obj));
	}

	if (stmt->isDrop)
		append_format_string(ret, "DROP");
	else
		append_format_string(ret, "ADD");

	append_array_object(ret, "%{items:, }s", list);

	ReleaseSysCache(ftp);

	return ret;
}

/*
 * Deparse an AlterOwnerStmt (ALTER ... OWNER TO ...).
 *
 * Given the object address and the parse tree that created it, return an
 * ObjTree representing the alter command.
 *
 * Verbose syntax
 * ALTER %s %{identity}s OWNER TO %{newowner}I
 */
static ObjTree *
deparse_AlterOwnerStmt(ObjectAddress address, Node *parsetree)
{
	AlterOwnerStmt *node = (AlterOwnerStmt *) parsetree;

	return new_objtree_VA("ALTER %{objtype}s %{identity}s OWNER TO %{newowner}I", 3,
						  "objtype", ObjTypeString,
						  stringify_objtype(node->objectType, false),
						  "identity", ObjTypeString,
						  getObjectIdentity(&address, false),
						  "newowner", ObjTypeString,
						  get_rolespec_name(node->newowner));
}

/*
 * Deparse an AlterSeqStmt.
 *
 * Given a sequence OID and a parse tree that modified it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax
 * ALTER SEQUENCE %{identity}D %{definition: }s
 */
static ObjTree *
deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *ret;
	Relation	relation;
	Form_pg_sequence_data seqdata;
	List	   *elems = NIL;
	ListCell   *cell;
	Form_pg_sequence seqform;
	Relation	rel;
	HeapTuple	seqtuple;
	AlterSeqStmt *alterSeqStmt = (AlterSeqStmt *) parsetree;

	/*
	 * Sequence for IDENTITY COLUMN output separately (via CREATE TABLE or
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
		elog(ERROR, "cache lookup failed for sequence with OID  %u", objectId);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

	foreach(cell, ((AlterSeqStmt *) parsetree)->options)
	{
		DefElem    *elem = (DefElem *) lfirst(cell);
		ObjElem    *newelm;

		if (strcmp(elem->defname, "cache") == 0)
			newelm = deparse_Seq_Cache(seqform, false);
		else if (strcmp(elem->defname, "cycle") == 0)
			newelm = deparse_Seq_Cycle(seqform, false);
		else if (strcmp(elem->defname, "increment") == 0)
			newelm = deparse_Seq_IncrementBy(seqform, false);
		else if (strcmp(elem->defname, "minvalue") == 0)
			newelm = deparse_Seq_Minvalue(seqform, false);
		else if (strcmp(elem->defname, "maxvalue") == 0)
			newelm = deparse_Seq_Maxvalue(seqform, false);
		else if (strcmp(elem->defname, "start") == 0)
			newelm = deparse_Seq_Startwith(seqform, false);
		else if (strcmp(elem->defname, "restart") == 0)
			newelm = deparse_Seq_Restart(seqdata);
		else if (strcmp(elem->defname, "owned_by") == 0)
			newelm = deparse_Seq_OwnedBy(objectId, false);
		else if (strcmp(elem->defname, "as") == 0)
			newelm = deparse_Seq_As(elem);
		else
			elog(ERROR, "invalid sequence option %s", elem->defname);

		elems = lappend(elems, newelm);
	}

	ret = new_objtree_VA("ALTER SEQUENCE %{identity}D %{definition: }s", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(relation->rd_rel->relnamespace,
												  RelationGetRelationName(relation)),
						 "definition", ObjTypeArray, elems);

	table_close(rel, RowExclusiveLock);
	relation_close(relation, AccessShareLock);

	return ret;
}

/*
 * Deparse an AlterTypeStmt.
 *
 * Given a type OID and a parse tree that modified it, return an ObjTree
 * representing the alter type.
 *
 * Verbose syntax
 * ALTER TYPE %{identity}D (%{definition: }s)
 */
static ObjTree *
deparse_AlterTypeSetStmt(Oid objectId, Node *cmd)
{
	AlterTypeStmt *alterTypeSetStmt = (AlterTypeStmt *) cmd;
	ListCell   *pl;
	List	   *elems = NIL;
	HeapTuple	typTup;
	Form_pg_type typForm;

	typTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for type with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	foreach(pl, alterTypeSetStmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		ObjElem    *newelm;

		if (strcmp(defel->defname, "storage") == 0)
			newelm = deparse_Type_Storage(typForm);
		if (strcmp(defel->defname, "receive") == 0)
			newelm = deparse_Type_Receive(typForm);
		if (strcmp(defel->defname, "send") == 0)
			newelm = deparse_Type_Send(typForm);
		if (strcmp(defel->defname, "typmod_in") == 0)
			newelm = deparse_Type_Typmod_In(typForm);
		if (strcmp(defel->defname, "typmod_out") == 0)
			newelm = deparse_Type_Typmod_Out(typForm);
		if (strcmp(defel->defname, "analyze") == 0)
			newelm = deparse_Type_Analyze(typForm);
		if (strcmp(defel->defname, "subscript") == 0)
			newelm = deparse_Type_Subscript(typForm);
		else
			elog(ERROR, "invalid type option %s", defel->defname);

		elems = lappend(elems, newelm);
	}

	ReleaseSysCache(typTup);

	return new_objtree_VA("ALTER TYPE %{identity}D SET (%{definition: }s)", 2,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname_id(TypeRelationId,
													  objectId),
						  "definition", ObjTypeArray, elems);
}

/*
 * Deparse a CompositeTypeStmt (CREATE TYPE AS)
 *
 * Given a Composite type OID and the parse tree that created it, return an
 * ObjTree representing the creation command.
 *
 * Verbose syntax
 * CREATE TYPE %{identity}D AS (%{columns:, }s)
 */
static ObjTree *
deparse_CompositeTypeStmt(Oid objectId, Node *parsetree)
{
	CompositeTypeStmt *node = (CompositeTypeStmt *) parsetree;
	HeapTuple	typtup;
	Form_pg_type typform;
	Relation	typerel;
	List	   *dpcontext;
	List	   *tableelts = NIL;

	/* Find the pg_type entry and open the corresponding relation */
	typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type with OID %u", objectId);
	typform = (Form_pg_type) GETSTRUCT(typtup);
	typerel = relation_open(typform->typrelid, AccessShareLock);

	dpcontext = deparse_context_for(RelationGetRelationName(typerel),
									RelationGetRelid(typerel));

	tableelts = deparse_TableElements(typerel, node->coldeflist, dpcontext,
									  false,	/* not typed */
									  true);	/* composite type */

	table_close(typerel, AccessShareLock);
	ReleaseSysCache(typtup);

	return new_objtree_VA("CREATE TYPE %{identity}D AS (%{columns:, }s)", 2,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname_id(TypeRelationId, objectId),
						  "columns", ObjTypeArray, tableelts);
}

/*
 * Deparse a CreateConversionStmt
 *
 * Given a conversion OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{default}s CONVERSION %{identity}D FOR %{source}L TO %{dest}L
 * FROM %{function}D
 */
static ObjTree *
deparse_CreateConversion(Oid objectId, Node *parsetree)
{
	HeapTuple	conTup;
	Relation	convrel;
	Form_pg_conversion conForm;
	ObjTree    *ret;

	convrel = table_open(ConversionRelationId, AccessShareLock);
	conTup = get_catalog_object_by_oid(convrel, Anum_pg_conversion_oid, objectId);
	if (!HeapTupleIsValid(conTup))
		elog(ERROR, "cache lookup failed for conversion with OID %u", objectId);
	conForm = (Form_pg_conversion) GETSTRUCT(conTup);

	ret = new_objtree_VA("CREATE %{default}s CONVERSION %{identity}D FOR %{source}L TO %{dest}L FROM %{function}D", 5,
						  "default", ObjTypeString,
						  conForm->condefault ? "DEFAULT" : "",
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname(conForm->connamespace,
												   NameStr(conForm->conname)),
						  "source", ObjTypeString,
						  (char *)pg_encoding_to_char(conForm->conforencoding),
						  "dest", ObjTypeString,
						  (char *)pg_encoding_to_char(conForm->contoencoding),
						  "function", ObjTypeObject,
						  new_objtree_for_qualname_id(ProcedureRelationId,
													  conForm->conproc));

	table_close(convrel, AccessShareLock);

	return ret;
}

/*
 * Deparse a CreateEnumStmt (CREATE TYPE AS ENUM)
 *
 * Given a Enum type OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE TYPE %{identity}D AS ENUM (%{values:, }L)
 */
static ObjTree *
deparse_CreateEnumStmt(Oid objectId, Node *parsetree)
{
	CreateEnumStmt *node = (CreateEnumStmt *) parsetree;
	List	   *values = NIL;
	ListCell   *cell;

	foreach(cell, node->vals)
		values = lappend(values, new_string_object(strVal(lfirst_node(String, cell))));

	return new_objtree_VA("CREATE TYPE %{identity}D AS ENUM (%{values:, }L)", 2,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname_id(TypeRelationId, objectId),
						  "values", ObjTypeArray, values);
}

/*
 * Deparse a CreateExtensionStmt
 *
 * Given an extension OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE EXTENSION %{if_not_exists}s %{name}I %{options: }s
 */
static ObjTree *
deparse_CreateExtensionStmt(Oid objectId, Node *parsetree)
{
	CreateExtensionStmt *node = (CreateExtensionStmt *) parsetree;
	Relation	pg_extension;
	HeapTuple	extTup;
	Form_pg_extension extForm;
	ObjTree    *tmp;
	List	   *list = NIL;
	ListCell   *cell;

	pg_extension = table_open(ExtensionRelationId, AccessShareLock);
	extTup = get_catalog_object_by_oid(pg_extension, Anum_pg_extension_oid, objectId);
	if (!HeapTupleIsValid(extTup))
		elog(ERROR, "cache lookup failed for extension with OID %u", objectId);
	extForm = (Form_pg_extension) GETSTRUCT(extTup);

	/* List of options */
	foreach(cell, node->options)
	{
		DefElem    *opt = (DefElem *) lfirst(cell);

		if (strcmp(opt->defname, "schema") == 0)
		{
			/* Skip this one; we add one unconditionally below */
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
	tmp = new_objtree_VA("SCHEMA %{schema}I", 2,
						 "type", ObjTypeString, "schema",
						 "schema", ObjTypeString,
						 get_namespace_name(extForm->extnamespace));
	list = lappend(list, new_object_object(tmp));
	table_close(pg_extension, AccessShareLock);

	return new_objtree_VA("CREATE EXTENSION %{if_not_exists}s %{name}I %{options: }s", 3,
						  "if_not_exists", ObjTypeString,
						  node->if_not_exists ? "IF NOT EXISTS" : "",
						  "name", ObjTypeString, node->extname,
						  "options", ObjTypeArray, list);
}

/*
 * If a column name is specified, add an element for it; otherwise it's a
 * table-level option.
 */
static ObjTree *
deparse_FdwOptions(List *options, char *colname)
{
	ObjTree    *ret;

	if (colname)
		ret = new_objtree_VA("ALTER COLUMN %{column}I", 1,
							 "column", ObjTypeString, colname);
	else
		ret = new_objtree("OPTIONS");

	if (options != NIL)
	{
		List	   *optout = NIL;
		ListCell   *cell;

		foreach(cell, options)
		{
			DefElem    *elem;
			ObjTree    *opt;

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

		append_array_object(ret, "(%{option: ,}s)", optout);
	}
	else
		append_not_present(ret);

	return ret;
}

/*
 * Deparse a CreateFdwStmt (CREATE FOREIGN DATA WRAPPER)
 *
 * Given an FDW OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE FOREIGN DATA WRAPPER %{identity}I %{handler}s %{validator}s %{generic_options}s
 */
static ObjTree *
deparse_CreateFdwStmt(Oid objectId, Node *parsetree)
{
	CreateFdwStmt *node = (CreateFdwStmt *) parsetree;
	HeapTuple	fdwTup;
	Form_pg_foreign_data_wrapper fdwForm;
	Relation	rel;
	ObjTree    *ret;
	ObjTree    *tmp;

	rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

	fdwTup = SearchSysCache1(FOREIGNDATAWRAPPEROID,
							 ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(fdwTup))
		elog(ERROR, "cache lookup failed for foreign-data wrapper with OID %u",
			 objectId);

	fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(fdwTup);

	ret = new_objtree_VA("CREATE FOREIGN DATA WRAPPER %{identity}I", 1,
						 "identity", ObjTypeString, NameStr(fdwForm->fdwname));

	/* Add HANDLER clause */
	if (!OidIsValid(fdwForm->fdwhandler))
		tmp = new_objtree("NO HANDLER");
	else
	{
		tmp = new_objtree_VA("HANDLER %{procedure}D", 1,
							 "procedure", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 fdwForm->fdwhandler));
	}
	append_object_object(ret, "%{handler}s", tmp);

	/* Add VALIDATOR clause */
	if (!OidIsValid(fdwForm->fdwvalidator))
		tmp = new_objtree("NO VALIDATOR");
	else
	{
		tmp = new_objtree_VA("VALIDATOR %{procedure}D", 1,
							 "procedure", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 fdwForm->fdwvalidator));
	}
	append_object_object(ret, "%{validator}s", tmp);

	/* Add an OPTIONS clause, if any */
	append_object_object(ret, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(fdwTup);
	table_close(rel, RowExclusiveLock);

	return ret;
}

/*
 * Deparse an AlterFdwStmt (ALTER FOREIGN DATA WRAPPER)
 *
 * Given an FDW OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax
 * ALTER FOREIGN DATA WRAPPER %{identity}I %{fdw_options: }s %{generic_options}D
 */
static ObjTree *
deparse_AlterFdwStmt(Oid objectId, Node *parsetree)
{
	AlterFdwStmt *node = (AlterFdwStmt *) parsetree;
	HeapTuple	fdwTup;
	Form_pg_foreign_data_wrapper fdwForm;
	Relation	rel;
	ObjTree    *ret;
	List	   *fdw_options = NIL;
	ListCell   *cell;

	rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

	fdwTup = SearchSysCache1(FOREIGNDATAWRAPPEROID,
							 ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(fdwTup))
		elog(ERROR, "cache lookup failed for foreign-data wrapper with OID %u",
			 objectId);

	fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(fdwTup);

	ret = new_objtree_VA("ALTER FOREIGN DATA WRAPPER %{identity}I", 1,
						 "identity", ObjTypeString, NameStr(fdwForm->fdwname));

	/*
	 * Iterate through options, to see what changed, but use catalog as a
	 * basis for new values.
	 */
	foreach(cell, node->func_options)
	{
		DefElem    *elem;
		ObjTree    *tmp;

		elem = lfirst(cell);

		if (pg_strcasecmp(elem->defname, "handler") == 0)
		{
			/* add HANDLER clause */
			if (!OidIsValid(fdwForm->fdwhandler))
				tmp = new_objtree("NO HANDLER");
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
			if (!OidIsValid(fdwForm->fdwvalidator))
				tmp = new_objtree("NO VALIDATOR");
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
	append_array_object(ret, "%{fdw_options: }s", fdw_options);

	/* Add an OPTIONS clause, if any */
	append_object_object(ret, "%{generic_options}D",
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(fdwTup);
	table_close(rel, RowExclusiveLock);

	return ret;
}

/*
 * Deparse a CREATE TYPE AS RANGE statement
 *
 * Given a Range type OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE TYPE %{identity}D AS RANGE (%{definition:, }s)
 */
static ObjTree *
deparse_CreateRangeStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *tmp;
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
		elog(ERROR, "cache lookup failed for range with type OID %u",
			 objectId);

	rangeForm = (Form_pg_range) GETSTRUCT(rangeTup);

	/* SUBTYPE */
	tmp = new_objtree_VA("SUBTYPE = %{type}D", 2,
						 "clause", ObjTypeString, "subtype",
						 "type", ObjTypeObject,
						 new_objtree_for_qualname_id(TypeRelationId, rangeForm->rngsubtype));
	definition = lappend(definition, new_object_object(tmp));

	/* SUBTYPE_OPCLASS */
	if (OidIsValid(rangeForm->rngsubopc))
	{
		tmp = new_objtree_VA("SUBTYPE_OPCLASS = %{opclass}D", 2,
							 "clause", ObjTypeString, "opclass",
							 "opclass", ObjTypeObject,
							 new_objtree_for_qualname_id(OperatorClassRelationId,
														 rangeForm->rngsubopc));
		definition = lappend(definition, new_object_object(tmp));
	}

	/* COLLATION */
	if (OidIsValid(rangeForm->rngcollation))
	{
		tmp = new_objtree_VA("COLLATION = %{collation}D", 2,
							 "clause", ObjTypeString, "collation",
							 "collation", ObjTypeObject,
							 new_objtree_for_qualname_id(CollationRelationId,
														 rangeForm->rngcollation));
		definition = lappend(definition, new_object_object(tmp));
	}

	/* CANONICAL */
	if (OidIsValid(rangeForm->rngcanonical))
	{
		tmp = new_objtree_VA("CANONICAL = %{canonical}D", 2,
							 "clause", ObjTypeString, "canonical",
							 "canonical", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 rangeForm->rngcanonical));
		definition = lappend(definition, new_object_object(tmp));
	}

	/* SUBTYPE_DIFF */
	if (OidIsValid(rangeForm->rngsubdiff))
	{
		tmp = new_objtree_VA("SUBTYPE_DIFF = %{subtype_diff}D", 2,
							 "clause", ObjTypeString, "subtype_diff",
							 "subtype_diff", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 rangeForm->rngsubdiff));
		definition = lappend(definition, new_object_object(tmp));
	}

	systable_endscan(scan);
	table_close(pg_range, RowExclusiveLock);

	return new_objtree_VA("CREATE TYPE %{identity}D AS RANGE (%{definition:, }s)", 2,
						  "identity", ObjTypeObject,
						  new_objtree_for_qualname_id(TypeRelationId, objectId),
						  "definition", ObjTypeArray, definition);
}

/*
 * Deparse an AlterEnumStmt.
 *
 * Given an enum OID and a parse tree that modified it, return an ObjTree
 * representing the alter type.
 *
 * Verbose syntax
 * ALTER TYPE %{identity}D ADD VALUE %{if_not_exists}s %{value}L
 * %{after_or_before}s %{neighbor}L
 * 	OR
 * ALTER TYPE %{identity}D RENAME VALUE %{value}L TO %{newvalue}L
 */
static ObjTree *
deparse_AlterEnumStmt(Oid objectId, Node *parsetree)
{
	AlterEnumStmt *node = (AlterEnumStmt *) parsetree;
	ObjTree    *ret;

	ret = new_objtree_VA("ALTER TYPE %{identity}D", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));

	if (!node->oldVal)
	{
		append_format_string(ret, "ADD VALUE");
		append_string_object(ret, "%{if_not_exists}s", "if_not_exists",
							 node->skipIfNewValExists ? "IF NOT EXISTS" : "");

		append_string_object(ret, "%{value}L", "value", node->newVal);

		if (node->newValNeighbor)
		{
			append_string_object(ret, "%{after_or_before}s",
								 "after_or_before",
								 node->newValIsAfter ? "AFTER" : "BEFORE");
			append_string_object(ret, "%{neighbor}L", "neighbor",
								 node->newValNeighbor);
		}
	}
	else
	{
		append_string_object(ret, "RENAME VALUE %{value}L TO", "value",
							 node->oldVal);
		append_string_object(ret, "%{newvalue}L", "newvalue", node->newVal);
	}

	return ret;
}

/*
 * Deparse an AlterExtensionStmt (ALTER EXTENSION .. UPDATE TO VERSION)
 *
 * Given an extension OID and a parse tree that modified it, return an ObjTree
 * representing the alter type.
 *
 * Verbose syntax
 * ALTER EXTENSION %{identity}I UPDATE %{options: }s
 */
static ObjTree *
deparse_AlterExtensionStmt(Oid objectId, Node *parsetree)
{
	AlterExtensionStmt *node = (AlterExtensionStmt *) parsetree;
	Relation	pg_extension;
	HeapTuple	extTup;
	Form_pg_extension extForm;
	ObjTree    *ret;
	List	   *list = NIL;
	ListCell   *cell;
	DefElem    *d_new_version = NULL;

	pg_extension = table_open(ExtensionRelationId, AccessShareLock);
	extTup = get_catalog_object_by_oid(pg_extension, Anum_pg_extension_oid, objectId);
	if (!HeapTupleIsValid(extTup))
		elog(ERROR, "cache lookup failed for extension with OID %u",
			 objectId);
	extForm = (Form_pg_extension) GETSTRUCT(extTup);

	foreach(cell, node->options)
	{
		DefElem    *opt = (DefElem *) lfirst(cell);

		if (strcmp(opt->defname, "new_version") == 0)
		{
			ObjTree    *tmp;

			if (d_new_version)
				elog(ERROR, "conflicting or redundant options for extension with OID %u",
					 objectId);

			d_new_version = opt;

			tmp = new_objtree_VA("TO %{version}L", 2,
								 "type", ObjTypeString, "version",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else
			elog(ERROR, "unsupported option %s", opt->defname);
	}

	ret = new_objtree_VA("ALTER EXTENSION %{identity}I UPDATE %{options: }s", 2,
						 "identity", ObjTypeString, NameStr(extForm->extname),
						 "options", ObjTypeArray, list);

	table_close(pg_extension, AccessShareLock);

	return ret;
}

/*
 * Deparse an AlterExtensionContentsStmt (ALTER EXTENSION ext ADD/DROP object)
 *
 * Verbose syntax
 * ALTER EXTENSION %{extidentity}I ADD/DROP %{objidentity}s
 */
static ObjTree *
deparse_AlterExtensionContentsStmt(Oid objectId, Node *parsetree,
								   ObjectAddress objectAddress)
{
	AlterExtensionContentsStmt *node = (AlterExtensionContentsStmt *) parsetree;

	Assert(node->action == +1 || node->action == -1);

	return new_objtree_VA("ALTER EXTENSION %{extidentity}I %{extoption}s %{extobjtype}s %{objidentity}s", 4,
						  "extidentity", ObjTypeString, node->extname,
						  "extoption", ObjTypeString,
						  node->action == +1 ? "ADD" : "DROP",
						  "extobjtype", ObjTypeString,
						  stringify_objtype(node->objtype, false),
						  "objidentity", ObjTypeString,
						  getObjectIdentity(&objectAddress, false));
}

/*
 * Deparse an CreateCastStmt.
 *
 * Given a sequence OID and a parse tree that modified it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE CAST (%{sourcetype}T AS %{targettype}T) %{mechanism}s %{context}s
 */
static ObjTree *
deparse_CreateCastStmt(Oid objectId, Node *parsetree)
{
	CreateCastStmt *node = (CreateCastStmt *) parsetree;
	Relation	castrel;
	HeapTuple	castTup;
	Form_pg_cast castForm;
	ObjTree    *ret;
	char	   *context;

	castrel = table_open(CastRelationId, AccessShareLock);
	castTup = get_catalog_object_by_oid(castrel, Anum_pg_cast_oid, objectId);
	if (!HeapTupleIsValid(castTup))
		elog(ERROR, "cache lookup failed for cast with OID %u", objectId);
	castForm = (Form_pg_cast) GETSTRUCT(castTup);

	ret = new_objtree_VA("CREATE CAST (%{sourcetype}T AS %{targettype}T)", 2,
						 "sourcetype", ObjTypeObject,
						 new_objtree_for_type(castForm->castsource, -1),
						 "targettype", ObjTypeObject,
						 new_objtree_for_type(castForm->casttarget, -1));

	if (node->inout)
		append_string_object(ret, "%{mechanism}s", "mechanism",
							 "WITH INOUT");
	else if (node->func == NULL)
		append_string_object(ret, "%{mechanism}s", "mechanism",
							 "WITHOUT FUNCTION");
	else
	{
		ObjTree    *tmp_obj;
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
		{
			if (i != 0)
				appendStringInfoChar(&func, ',');

			appendStringInfoString(&func,
								   format_type_be_qualified(funcForm->proargtypes.values[i]));
		}
		appendStringInfoChar(&func, ')');

		tmp_obj = new_objtree_VA("WITH FUNCTION %{castfunction}s", 1,
								"castfunction", ObjTypeString, func.data);
		append_object_object(ret, "%{mechanism}s", tmp_obj);

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
			return NULL;		/* keep compiler quiet */
	}
	append_string_object(ret, "%{context}s", "context", context);

	table_close(castrel, AccessShareLock);

	return ret;
}

/*
 * Deparse an ALTER DEFAULT PRIVILEGES statement.
 *
 * Verbose syntax
 * ALTER DEFAULT PRIVILEGES %{in_schema}s %{for_roles}s %{grant}s
 */
static ObjTree *
deparse_AlterDefaultPrivilegesStmt(CollectedCommand *cmd)
{
	ObjTree    *ret;
	AlterDefaultPrivilegesStmt *stmt = (AlterDefaultPrivilegesStmt *) cmd->parsetree;
	List	   *roles = NIL;
	List	   *schemas = NIL;
	List	   *grantees;
	List	   *privs;
	ListCell   *cell;
	ObjTree    *tmp;
	ObjTree    *grant;
	char	   *objtype;

	ret = new_objtree("ALTER DEFAULT PRIVILEGES");

	/* Scan the parse node to dig out the FOR ROLE and IN SCHEMA clauses */
	foreach(cell, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(cell);
		ListCell   *cell2;

		Assert(IsA(opt, DefElem));
		Assert(IsA(opt->arg, List));
		if (strcmp(opt->defname, "roles") == 0)
		{
			foreach(cell2, (List *) opt->arg)
			{
				RoleSpec   *rolespec = lfirst(cell2);
				ObjTree    *obj = new_objtree_for_rolespec(rolespec);

				roles = lappend(roles, new_object_object(obj));
			}
		}
		else if (strcmp(opt->defname, "schemas") == 0)
		{
			foreach(cell2, (List *) opt->arg)
			{
				String	   *val = lfirst_node(String, cell2);

				schemas = lappend(schemas, new_string_object(strVal(val)));
			}
		}
	}

	/* Add the IN SCHEMA clause, if any */
	tmp = new_objtree("IN SCHEMA");
	append_array_object(tmp, "%{schemas:, }I", schemas);
	if (schemas == NIL)
		append_not_present(tmp);
	append_object_object(ret, "%{in_schema}s", tmp);

	/* Add the FOR ROLE clause, if any */
	tmp = new_objtree("FOR ROLE");
	append_array_object(tmp, "%{roles:, }R", roles);
	if (roles == NIL)
		append_not_present(tmp);
	append_object_object(ret, "%{for_roles}s", tmp);

	/*
	 * Add the GRANT subcommand
	 * Verbose syntax
	 * GRANT %{privileges:, }s ON %{target}s TO %{grantees:, }R %{grant_option}s
	 * or
	 * REVOKE %{grant_option}s %{privileges:, }s ON %{target}s FROM %{grantees:, }R
	 */
	if (stmt->action->is_grant)
		grant = new_objtree("GRANT");
	else
	{
		grant = new_objtree("REVOKE");

		/* Add the GRANT OPTION clause for REVOKE subcommand */
		tmp = new_objtree_VA("GRANT OPTION FOR", 1,
							 "present", ObjTypeBool,
							 stmt->action->grant_option);
		append_object_object(grant, "%{grant_option}s", tmp);
	}

	/*
	 * Add the privileges list.  This uses the parser struct, as opposed to
	 * the InternalGrant format used by GRANT.  There are enough other
	 * differences that this doesn't seem worth improving.
	 */
	if (stmt->action->privileges == NIL)
		privs = list_make1(new_string_object("ALL PRIVILEGES"));
	else
	{
		privs = NIL;

		foreach(cell, stmt->action->privileges)
		{
			AccessPriv *priv = lfirst(cell);

			Assert(priv->cols == NIL);
			privs = lappend(privs, new_string_object(priv->priv_name));
		}
	}

	append_array_object(grant, "%{privileges:, }s", privs);

	switch (cmd->d.defprivs.objtype)
	{
		case OBJECT_TABLE:
			objtype = "TABLES";
			break;
		case OBJECT_FUNCTION:
			objtype = "FUNCTIONS";
			break;
		case OBJECT_ROUTINE:
			objtype = "ROUTINES";
			break;
		case OBJECT_SEQUENCE:
			objtype = "SEQUENCES";
			break;
		case OBJECT_TYPE:
			objtype = "TYPES";
			break;
		case OBJECT_SCHEMA:
			objtype = "SCHEMAS";
			break;
		default:
			elog(ERROR, "invalid OBJECT value %d for default privileges", cmd->d.defprivs.objtype);
	}

	/* Add the target object type */
	append_string_object(grant, "ON %{target}s", "target", objtype);

	/* Add the grantee list */
	grantees = NIL;
	foreach(cell, stmt->action->grantees)
	{
		RoleSpec   *spec = (RoleSpec *) lfirst(cell);
		ObjTree    *obj = new_objtree_for_rolespec(spec);

		grantees = lappend(grantees, new_object_object(obj));
	}

	if (stmt->action->is_grant)
	{
		append_array_object(grant, "TO %{grantees:, }R", grantees);

		/* Add the WITH GRANT OPTION clause for GRANT subcommand */
		tmp = new_objtree_VA("WITH GRANT OPTION", 1,
							 "present", ObjTypeBool,
							 stmt->action->grant_option);
		append_object_object(grant, "%{grant_option}s", tmp);
	}
	else
		append_array_object(grant, "FROM %{grantees:, }R", grantees);

	append_object_object(ret, "%{grant}s", grant);

	return ret;
}

/*
 * Deparse the CREATE DOMAIN
 *
 * Given a function OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE DOMAIN %{identity}D AS %{type}T %{not_null}s %{constraints}s
 * %{collation}s
 */
static ObjTree *
deparse_CreateDomain(Oid objectId, Node *parsetree)
{
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	HeapTuple	typTup;
	Form_pg_type typForm;
	List	   *constraints;
	char	   *defval;

	typTup = SearchSysCache1(TYPEOID, objectId);
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for domain with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	ret = new_objtree_VA("CREATE DOMAIN %{identity}D AS %{type}T %{not_null}s", 3,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname_id(TypeRelationId, objectId),
						 "type", ObjTypeObject,
						 new_objtree_for_type(typForm->typbasetype,
											  typForm->typtypmod),
						 "not_null", ObjTypeString,
						 typForm->typnotnull ? "NOT NULL" : "");

	constraints = obtainConstraints(NIL, InvalidOid, objectId);
	if (constraints == NIL)
	{
		tmp_obj = new_objtree("");
		append_not_present(tmp_obj);
	}
	else
		tmp_obj = new_objtree_VA("%{elements:, }s", 1,
								"elements", ObjTypeArray, constraints);
	append_object_object(ret, "%{constraints}s", tmp_obj);

	tmp_obj = new_objtree("COLLATE");
	if (OidIsValid(typForm->typcollation))
	{
		ObjTree    *collname;

		collname = new_objtree_for_qualname_id(CollationRelationId,
											   typForm->typcollation);
		append_object_object(tmp_obj, "%{name}D", collname);
	}
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{collation}s", tmp_obj);

	defval = DomainGetDefault(typTup, true);
	if (defval)
		append_string_object(ret, "DEFAULT %{default}s", "default", defval);

	ReleaseSysCache(typTup);

	return ret;
}

/*
 * Deparse a CreateFunctionStmt (CREATE FUNCTION)
 *
 * Given a function OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 *
 * CREATE %{or_replace}s FUNCTION %{signature}s RETURNS %{return_type}s
 * LANGUAGE %{transform_type}s %{language}I %{window}s %{volatility}s
 * %{parallel_safety}s %{leakproof}s %{strict}s %{security_definer}s
 * %{cost}s %{rows}s %{support}s %{set_options: }s AS %{objfile}L,
 * %{symbol}L
 */
static ObjTree *
deparse_CreateFunction(Oid objectId, Node *parsetree)
{
	CreateFunctionStmt *node = (CreateFunctionStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	Datum		tmpdatum;
	char	   *source;
	char	   *probin = NULL;
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
	int			paramcount = list_length(node->parameters);
	bool		isnull;
	bool		isfunction;

	/* Get the pg_proc tuple */
	procTup = SearchSysCache1(PROCOID, objectId);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failed for function with OID %u",
			 objectId);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	/* Get the corresponding pg_language tuple */
	langTup = SearchSysCache1(LANGOID, procForm->prolang);
	if (!HeapTupleIsValid(langTup))
		elog(ERROR, "cache lookup failed for language with OID %u",
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
	if (!isnull)
	{
		probin = TextDatumGetCString(tmpdatum);
		if (probin[0] == '\0' || strcmp(probin, "-") == 0)
			probin = NULL;
	}

	ret = new_objtree_VA("CREATE %{or_replace}s", 1,
						 "or_replace", ObjTypeString,
						 node->replace ? "OR REPLACE" : "");

	/*
	 * To construct the arguments array, extract the type OIDs from the
	 * function's pg_proc entry.  If pronargs equals the parameter list
	 * length, there are no OUT parameters and thus we can extract the type
	 * OID from proargtypes; otherwise we need to decode proallargtypes, which
	 * is a bit more involved.
	 */
	typarray = palloc(paramcount * sizeof(Oid));
	if (paramcount > procForm->pronargs)
	{
		Datum		alltypes;
		Datum	   *values;
		bool	   *nulls;
		int			nelems;

		alltypes = SysCacheGetAttr(PROCOID, procTup,
								   Anum_pg_proc_proallargtypes, &isnull);
		if (isnull)
			elog(ERROR, "null proallargtypes, more number of parameters than args in function with OID %u",
				 objectId);
		deconstruct_array(DatumGetArrayTypeP(alltypes),
						  OIDOID, 4, 't', 'i',
						  &values, &nulls, &nelems);
		if (nelems != paramcount)
			elog(ERROR, "mismatched proallargatypes");
		for (i = 0; i < paramcount; i++)
			typarray[i] = values[i];
	}
	else
	{
		for (i = 0; i < paramcount; i++)
			typarray[i] = procForm->proargtypes.values[i];
	}

	/*
	 * If there are any default expressions, we read the deparsed expression
	 * as a list so that we can attach them to each argument.
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
	 * Now iterate over each parameter in the parse tree to create the
	 * parameters array.
	 */
	params = NIL;
	typnum = 0;
	foreach(cell, node->parameters)
	{
		FunctionParameter *param = (FunctionParameter *) lfirst(cell);
		ObjTree    *param_obj;
		ObjTree    *defaultval;
		ObjTree    *name;

		/*
		 * A PARAM_TABLE parameter indicates the end of input arguments; the
		 * following parameters are part of the return type.  We ignore them
		 * here, but keep track of the current position in the list so that we
		 * can easily produce the return type below.
		 */
		if (param->mode == FUNC_PARAM_TABLE)
		{
			table_params = cell;
			break;
		}

		/*
		 * Verbose syntax for paramater: %{mode}s %{name}s %{type}T
		 * %{default}s
		 */
		param_obj = new_objtree_VA("%{mode}s", 1,
								  "mode", ObjTypeString,
								  param->mode == FUNC_PARAM_OUT ? "OUT" :
								  param->mode == FUNC_PARAM_INOUT ? "INOUT" :
								  param->mode == FUNC_PARAM_VARIADIC ? "VARIADIC" :
								  "IN");

		/* Optional wholesale suppression of "name" occurs here */
		name = new_objtree("");
		if (param->name)
			append_string_object(name, "%{name}I", "name", param->name);
		else
		{
			append_null_object(name, "%{name}I");
			append_not_present(name);
		}

		append_object_object(param_obj, "%{name}s", name);

		defaultval = new_objtree("DEFAULT");
		if (PointerIsValid(param->defexpr))
		{
			char	   *expr;

			if (curdef == NULL)
				elog(ERROR, "proargdefaults list too short");
			expr = lfirst(curdef);

			append_string_object(defaultval, "%{value}s", "value", expr);
			curdef = lnext(defaults, curdef);
		}
		else
			append_not_present(defaultval);

		append_object_object(param_obj, "%{type}T",
							 new_objtree_for_type(typarray[typnum++], -1));

		append_object_object(param_obj, "%{default}s", defaultval);

		params = lappend(params, new_object_object(param_obj));
	}

	tmp_obj = new_objtree_VA("%{identity}D", 1,
							"identity", ObjTypeObject,
							new_objtree_for_qualname_id(ProcedureRelationId,
														objectId));

	append_format_string(tmp_obj, "(");
	append_array_object(tmp_obj, "%{arguments:, }s", params);
	append_format_string(tmp_obj, ")");

	isfunction = (procForm->prokind != PROKIND_PROCEDURE);

	if (isfunction)
		append_object_object(ret, "FUNCTION %{signature}s", tmp_obj);
	else
		append_object_object(ret, "PROCEDURE %{signature}s", tmp_obj);

	/*
	 * A return type can adopt one of two forms: either a [SETOF] some_type,
	 * or a TABLE(list-of-types).  We can tell the second form because we saw
	 * a table param above while scanning the argument list.
	 */
	if (table_params == NULL)
	{
		tmp_obj = new_objtree_VA("", 1,
								"return_form", ObjTypeString, "plain");
		append_string_object(tmp_obj, "%{setof}s", "setof",
							 procForm->proretset ? "SETOF" : "");
		append_object_object(tmp_obj, "%{rettype}T",
							 new_objtree_for_type(procForm->prorettype, -1));
	}
	else
	{
		List	   *rettypes = NIL;
		ObjTree    *param_obj;

		tmp_obj = new_objtree_VA("TABLE", 1,
								"return_form", ObjTypeString, "table");
		for (; table_params != NULL; table_params = lnext(node->parameters, table_params))
		{
			FunctionParameter *param = lfirst(table_params);

			param_obj = new_objtree_VA("%{name}I %{type}T", 2,
									  "name", ObjTypeString, param->name,
									  "type", ObjTypeObject,
									  new_objtree_for_type(typarray[typnum++], -1));
			rettypes = lappend(rettypes, new_object_object(param_obj));
		}

		append_array_object(tmp_obj, "(%{rettypes:, }s)", rettypes);
	}

	if (isfunction)
		append_object_object(ret, "RETURNS %{return_type}s", tmp_obj);

	/* TRANSFORM FOR TYPE */
	tmp_obj = new_objtree("TRANSFORM");

	ntypes = get_func_trftypes(procTup, &trftypes);
	for (i = 0; i < ntypes; i++)
	{
		tmp_obj = new_objtree_VA("FOR TYPE %{type}T", 1,
								"type", ObjTypeObject,
								new_objtree_for_type(trftypes[i], -1));
		types = lappend(types, tmp_obj);
	}

	if (types)
		append_array_object(tmp_obj, "%{for_type:, }s", types);
	else
		append_not_present(tmp_obj);

	append_object_object(ret, "%{transform_type}s", tmp_obj);

	append_string_object(ret, "LANGUAGE %{language}I", "language",
						 NameStr(langForm->lanname));

	if (isfunction)
	{
		append_string_object(ret, "%{window}s", "window",
							 procForm->prokind == PROKIND_WINDOW ? "WINDOW" : "");
		append_string_object(ret, "%{volatility}s", "volatility",
							 procForm->provolatile == PROVOLATILE_VOLATILE ?
							 "VOLATILE" :
							 procForm->provolatile == PROVOLATILE_STABLE ?
							 "STABLE" : "IMMUTABLE");

		append_string_object(ret, "%{parallel_safety}s",
							 "parallel_safety",
							 procForm->proparallel == PROPARALLEL_SAFE ?
							 "PARALLEL SAFE" :
							 procForm->proparallel == PROPARALLEL_RESTRICTED ?
							 "PARALLEL RESTRICTED" : "PARALLEL UNSAFE");

		append_string_object(ret, "%{leakproof}s", "leakproof",
							 procForm->proleakproof ? "LEAKPROOF" : "");
		append_string_object(ret, "%{strict}s", "strict",
							 procForm->proisstrict ?
							 "RETURNS NULL ON NULL INPUT" :
							 "CALLED ON NULL INPUT");

		append_string_object(ret, "%{security_definer}s",
							 "security_definer",
							 procForm->prosecdef ?
							 "SECURITY DEFINER" : "SECURITY INVOKER");

		append_object_object(ret, "%{cost}s",
							 new_objtree_VA("COST %{cost}n", 1,
											"cost", ObjTypeFloat,
											procForm->procost));

		tmp_obj = new_objtree("ROWS");
		if (procForm->prorows == 0)
			append_not_present(tmp_obj);
		else
			append_float_object(tmp_obj, "%{rows}n", procForm->prorows);
		append_object_object(ret, "%{rows}s", tmp_obj);

		tmp_obj = new_objtree("SUPPORT %{name}s");
		if (procForm->prosupport)
		{
			Oid			argtypes[] = { INTERNALOID };

			/*
			 * We should qualify the support function's name if it wouldn't be
			 * resolved by lookup in the current search path.
			 */
			append_string_object(tmp_obj, "%{name}s", "name",
								 generate_function_name(procForm->prosupport, 1,
														NIL, argtypes,
														false, NULL,
														EXPR_KIND_NONE));
		}
		else
			append_not_present(tmp_obj);

		append_object_object(ret, "%{support}s", tmp_obj);
	}

	foreach(cell, node->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "set") == 0)
		{
			VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
			char	   *value = ExtractSetVariableArgs(sstmt);

			tmp_obj = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
			sets = lappend(sets, new_object_object(tmp_obj));
		}
	}
	append_array_object(ret, "%{set_options: }s", sets);

	/* Add the function definition */
	(void) SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prosqlbody, &isnull);
	if (procForm->prolang == SQLlanguageId && !isnull)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		print_function_sqlbody(&buf, procTup);

		append_string_object(ret, "%{definition}s", "definition",
							 buf.data);
	}
	else if (probin == NULL)
		append_string_object(ret, "AS %{definition}L", "definition", source);
	else
	{
		append_string_object(ret, "AS %{objfile}L", "objfile", probin);
		append_string_object(ret, ", %{symbol}L", "symbol", source);
	}

	ReleaseSysCache(langTup);
	ReleaseSysCache(procTup);

	return ret;
}

/*
 * Deparse a CREATE OPERATOR CLASS command.
 *
 * Verbose syntax
 * CREATE OPERATOR CLASS %{identity}D %{default}s FOR TYPE %{type}T USING
 * %{amname}I %{opfamily}s AS %{items:, }s
 */
static ObjTree *
deparse_CreateOpClassStmt(CollectedCommand *cmd)
{
	Oid			opcoid = cmd->d.createopc.address.objectId;
	HeapTuple	opcTup;
	HeapTuple	opfTup;
	Form_pg_opfamily opfForm;
	Form_pg_opclass opcForm;
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	List	   *list;
	ListCell   *cell;

	/* Don't deparse SQL commands generated while creating extension */
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

	ret = new_objtree_VA("CREATE OPERATOR CLASS %{identity}D %{default}s FOR TYPE %{type}T USING %{amname}I", 4,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(opcForm->opcnamespace,
												  NameStr(opcForm->opcname)),
						 "default", ObjTypeString,
						 opcForm->opcdefault ? "DEFAULT" : "",
						 "type", ObjTypeObject,
						 new_objtree_for_type(opcForm->opcintype, -1),
						 "amname", ObjTypeString, get_am_name(opcForm->opcmethod));

	/*
	 * Add the FAMILY clause, but if it has the same name and namespace as the
	 * opclass, then have it expand to empty because it would cause a failure
	 * if the opfamily was created internally.
	 */
	tmp_obj = new_objtree_VA("FAMILY %{opfamily}D", 1,
							"opfamily", ObjTypeObject,
							new_objtree_for_qualname(opfForm->opfnamespace,
													 NameStr(opfForm->opfname)));

	if (strcmp(NameStr(opfForm->opfname), NameStr(opcForm->opcname)) == 0 &&
		opfForm->opfnamespace == opcForm->opcnamespace)
		append_not_present(tmp_obj);

	append_object_object(ret, "%{opfamily}s", tmp_obj);

	/*
	 * Add the initial item list.  Note we always add the STORAGE clause, even
	 * when it is implicit in the original command.
	 */
	tmp_obj = new_objtree("STORAGE");
	append_object_object(tmp_obj, "%{type}T",
						 new_objtree_for_type(OidIsValid(opcForm->opckeytype) ?
											  opcForm->opckeytype : opcForm->opcintype,
											  -1));
	list = list_make1(new_object_object(tmp_obj));

	/* Add the declared operators */
	foreach(cell, cmd->d.createopc.operators)
	{
		OpFamilyMember *oper = lfirst(cell);

		tmp_obj = new_objtree_VA("OPERATOR %{num}n %{operator}O(%{ltype}T, %{rtype}T)", 4,
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
			append_string_object(tmp_obj, "%{purpose}s", "purpose",
								 "FOR SEARCH");
		else
		{
			ObjTree    *tmp_obj2;

			tmp_obj2 = new_objtree("FOR ORDER BY %{opfamily}D");
			append_object_object(tmp_obj2, "opfamily",
								 new_objtree_for_qualname_id(OperatorFamilyRelationId,
															 oper->sortfamily));
			append_object_object(tmp_obj, "%{purpose}s", tmp_obj2);
		}

		list = lappend(list, new_object_object(tmp_obj));
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
			elog(ERROR, "cache lookup failed for procedure with OID %u",
				 proc->object);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);

		tmp_obj = new_objtree_VA("FUNCTION %{num}n (%{ltype}T, %{rtype}T) %{function}D", 4,
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
			ObjTree    *arg;

			arg = new_objtree_for_type(proargtypes[i], -1);
			arglist = lappend(arglist, new_object_object(arg));
		}

		append_format_string(tmp_obj, "(");
		append_array_object(tmp_obj, "%{argtypes:, }T", arglist);
		append_format_string(tmp_obj, ")");

		ReleaseSysCache(procTup);

		list = lappend(list, new_object_object(tmp_obj));
	}

	append_array_object(ret, "AS %{items:, }s", list);

	ReleaseSysCache(opfTup);
	ReleaseSysCache(opcTup);

	return ret;
}

/*
 * Deparse a CreateOpFamilyStmt (CREATE OPERATOR FAMILY)
 *
 * Given a operator family OID and the parse tree that created it, return an
 * ObjTree representing the creation command.
 *
 * Verbose syntax
 * CREATE OPERATOR FAMILY %{identity}D USING %{amname}I
 */
static ObjTree *
deparse_CreateOpFamily(Oid objectId, Node *parsetree)
{
	HeapTuple	opfTup;
	HeapTuple	amTup;
	Form_pg_opfamily opfForm;
	Form_pg_am	amForm;
	ObjTree    *ret;

	opfTup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(opfTup))
		elog(ERROR, "cache lookup failed for operator family with OID %u", objectId);
	opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

	amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
	if (!HeapTupleIsValid(amTup))
		elog(ERROR, "cache lookup failed for access method with OID %u",
			 opfForm->opfmethod);
	amForm = (Form_pg_am) GETSTRUCT(amTup);

	ret = new_objtree_VA("CREATE OPERATOR FAMILY %{identity}D USING %{amname}I", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(opfForm->opfnamespace,
												  NameStr(opfForm->opfname)),
						 "amname", ObjTypeString, NameStr(amForm->amname));

	ReleaseSysCache(amTup);
	ReleaseSysCache(opfTup);

	return ret;
}

/*
 * Add common clauses to CreatePolicy or AlterPolicy deparse objects.
 */
static void
add_policy_clauses(ObjTree *ret, Oid policyOid, List *roles, bool do_qual,
				   bool do_with_check)
{
	Relation	polRel = table_open(PolicyRelationId, AccessShareLock);
	HeapTuple	polTup = get_catalog_object_by_oid(polRel, Anum_pg_policy_oid, policyOid);
	Form_pg_policy polForm;

	if (!HeapTupleIsValid(polTup))
		elog(ERROR, "cache lookup failed for policy with OID %u", policyOid);

	polForm = (Form_pg_policy) GETSTRUCT(polTup);

	/* Add the "ON table" clause */
	append_object_object(ret, "ON %{table}D",
						 new_objtree_for_qualname_id(RelationRelationId,
													 polForm->polrelid));

	/*
	 * Add the "TO role" clause, if any.  In the CREATE case, it always
	 * contains at least PUBLIC, but in the ALTER case it might be empty.
	 */
	if (roles)
	{
		List	   *list = NIL;
		ListCell   *cell;

		foreach(cell, roles)
		{
			RoleSpec   *spec = (RoleSpec *) lfirst(cell);

			list = lappend(list,
						   new_object_object(new_objtree_for_rolespec(spec)));
		}
		append_array_object(ret, "TO %{role:, }R", list);
	}
	else
		append_not_present(ret);

	/* Add the USING clause, if any */
	if (do_qual)
	{
		Datum		deparsed;
		Datum		storedexpr;
		bool		isnull;

		storedexpr = heap_getattr(polTup, Anum_pg_policy_polqual,
								  RelationGetDescr(polRel), &isnull);
		if (isnull)
			elog(ERROR, "null polqual expression in policy %u", policyOid);
		deparsed = DirectFunctionCall2(pg_get_expr, storedexpr, polForm->polrelid);
		append_string_object(ret, "USING (%{expression}s)", "expression",
							 TextDatumGetCString(deparsed));
	}
	else
		append_not_present(ret);

	/* Add the WITH CHECK clause, if any */
	if (do_with_check)
	{
		Datum		deparsed;
		Datum		storedexpr;
		bool		isnull;

		storedexpr = heap_getattr(polTup, Anum_pg_policy_polwithcheck,
								  RelationGetDescr(polRel), &isnull);
		if (isnull)
			elog(ERROR, "null polwithcheck expression in policy %u", policyOid);
		deparsed = DirectFunctionCall2(pg_get_expr, storedexpr, polForm->polrelid);
		append_string_object(ret, "WITH CHECK (%{expression}s)",
							 "expression", TextDatumGetCString(deparsed));
	}
	else
		append_not_present(ret);

	relation_close(polRel, AccessShareLock);
}

/*
 * Deparse a CreatePolicyStmt (CREATE POLICY)
 *
 * Given a policy OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE POLICY %{identity}I
 */
static ObjTree *
deparse_CreatePolicyStmt(Oid objectId, Node *parsetree)
{
	CreatePolicyStmt *node = (CreatePolicyStmt *) parsetree;
	ObjTree    *ret;

	ret = new_objtree_VA("CREATE POLICY %{identity}I", 1,
						 "identity", ObjTypeString, node->policy_name);

	/* Add the rest of the stuff */
	add_policy_clauses(ret, objectId, node->roles, node->qual != NULL,
					   node->with_check != NULL);

	return ret;
}

/*
 * Deparse a AlterPolicyStmt (ALTER POLICY)
 *
 * Given a policy OID and the parse tree of the ALTER POLICY command, return
 * an ObjTree representing the alter command.
 *
 * Verbose syntax
 * ALTER POLICY %{identity}I
 */
static ObjTree *
deparse_AlterPolicyStmt(Oid objectId, Node *parsetree)
{
	AlterPolicyStmt *node = (AlterPolicyStmt *) parsetree;
	ObjTree    *ret;

	ret = new_objtree_VA("ALTER POLICY %{identity}I", 1,
						 "identity", ObjTypeString, node->policy_name);

	/* Add the rest of the stuff */
	add_policy_clauses(ret, objectId, node->roles, node->qual != NULL,
					   node->with_check != NULL);

	return ret;
}

/*
 * Deparse a CreateSchemaStmt.
 *
 * Given a schema OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE SCHEMA %{if_not_exists}s %{name}I %{authorization}s
*/
static ObjTree *
deparse_CreateSchemaStmt(Oid objectId, Node *parsetree)
{
	CreateSchemaStmt *node = (CreateSchemaStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *auth;

	ret = new_objtree_VA("CREATE SCHEMA %{if_not_exists}s %{name}I", 2,
						 "if_not_exists", ObjTypeString,
						 node->if_not_exists ? "IF NOT EXISTS" : "",
						 "name", ObjTypeString,
						 node->schemaname ? node->schemaname : "");

	auth = new_objtree("AUTHORIZATION");
	if (node->authrole)
		append_string_object(auth, "%{authorization_role}I",
							 "authorization_role",
							 get_rolespec_name(node->authrole));
	else
	{
		append_null_object(auth, "%{authorization_role}I");
		append_not_present(auth);
	}
	append_object_object(ret, "%{authorization}s", auth);

	return ret;
}

/*
 * Return the default value of a domain.
 */
static char *
DomainGetDefault(HeapTuple domTup, bool missing_ok)
{
	Datum		def;
	Node	   *defval;
	char	   *defstr;
	bool		isnull;

	def = SysCacheGetAttr(TYPEOID, domTup, Anum_pg_type_typdefaultbin,
						  &isnull);
	if (isnull)
	{
		if (!missing_ok)
			elog(ERROR, "domain \"%s\" does not have a default value",
				NameStr(((Form_pg_type) GETSTRUCT(domTup))->typname));
		else
			return NULL;
	}

	defval = stringToNode(TextDatumGetCString(def));
	defstr = deparse_expression(defval, NULL /* dpcontext? */ ,
								false, false);

	return defstr;
}

/*
 * Deparse a AlterDomainStmt.
 *
 * Verbose syntax
 * ALTER DOMAIN %{identity}D DROP DEFAULT
 * OR
 * ALTER DOMAIN %{identity}D SET DEFAULT %{default}s
 * OR
 * ALTER DOMAIN %{identity}D DROP NOT NULL
 * OR
 * ALTER DOMAIN %{identity}D SET NOT NULL
 * OR
 * ALTER DOMAIN %{identity}D ADD CONSTRAINT %{constraint_name}s %{definition}s
 * OR
 * ALTER DOMAIN %{identity}D DROP CONSTRAINT IF EXISTS %{constraint_name}s %{cascade}s
 * OR
 * ALTER DOMAIN %{identity}D VALIDATE CONSTRAINT %{constraint_name}s
 */
static ObjTree *
deparse_AlterDomainStmt(Oid objectId, Node *parsetree,
						ObjectAddress constraintAddr)
{
	AlterDomainStmt *node = (AlterDomainStmt *) parsetree;
	HeapTuple	domTup;
	Form_pg_type domForm;
	ObjTree    *ret;

	domTup = SearchSysCache1(TYPEOID, objectId);
	if (!HeapTupleIsValid(domTup))
		elog(ERROR, "cache lookup failed for domain with OID %u", objectId);
	domForm = (Form_pg_type) GETSTRUCT(domTup);

	switch (node->subtype)
	{
		case 'T':
			/* SET DEFAULT / DROP DEFAULT */
			if (node->def == NULL)
				ret = new_objtree_VA("ALTER DOMAIN %{identity}D DROP DEFAULT", 2,
									 "type", ObjTypeString, "drop default",
									 "identity", ObjTypeObject,
									 new_objtree_for_qualname(domForm->typnamespace,
															  NameStr(domForm->typname)));
			else
				ret = new_objtree_VA("ALTER DOMAIN %{identity}D SET DEFAULT %{default}s", 3,
									 "type", ObjTypeString, "set default",
									 "identity", ObjTypeObject,
									 new_objtree_for_qualname(domForm->typnamespace,
															  NameStr(domForm->typname)),
									 "default", ObjTypeString,
									 DomainGetDefault(domTup, false));
			break;
		case 'N':
			/* DROP NOT NULL */
			ret = new_objtree_VA("ALTER DOMAIN %{identity}D DROP NOT NULL", 2,
									  "type", ObjTypeString, "drop not null",
									  "identity", ObjTypeObject,
									  new_objtree_for_qualname(domForm->typnamespace,
															   NameStr(domForm->typname)));
			break;
		case 'O':
			/* SET NOT NULL */
			ret = new_objtree_VA("ALTER DOMAIN %{identity}D SET NOT NULL", 2,
								 "type", ObjTypeString, "set not null",
								 "identity", ObjTypeObject,
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)));
			break;
		case 'C':
			/*
			 * ADD CONSTRAINT.  Only CHECK constraints are supported by
			 * domains
			 */
			ret = new_objtree_VA("ALTER DOMAIN %{identity}D ADD CONSTRAINT %{constraint_name}s %{definition}s", 4,
								 "type", ObjTypeString, "add constraint",
								 "identity", ObjTypeObject,
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)),
								 "constraint_name", ObjTypeString,
								 get_constraint_name(constraintAddr.objectId),
								 "definition", ObjTypeString,
								 pg_get_constraintdef_command_simple(constraintAddr.objectId));
			break;
		case 'V':
			/* VALIDATE CONSTRAINT */
			/*
			 * Process subtype=specific options. Validation a constraint
			 * requires its name.
			 */
			ret = new_objtree_VA("ALTER DOMAIN %{identity}D VALIDATE CONSTRAINT %{constraint_name}s", 3,
								 "type", ObjTypeString, "validate constraint",
								 "identity", ObjTypeObject,
								 new_objtree_for_qualname(domForm->typnamespace,
														  NameStr(domForm->typname)),
								 "constraint_name", ObjTypeString, node->name);
			break;
		case 'X':
			{
				ObjTree    *tmp_obj;

				/* DROP CONSTRAINT */
				ret = new_objtree_VA("ALTER DOMAIN %{identity}D DROP CONSTRAINT IF EXISTS %{constraint_name}s", 3,
										"type", ObjTypeString, "drop constraint",
										"identity", ObjTypeObject,
										new_objtree_for_qualname(domForm->typnamespace,
																NameStr(domForm->typname)),
										"constraint_name", ObjTypeString, node->name);

				tmp_obj = new_objtree_VA("CASCADE", 1,
										"present", ObjTypeBool, node->behavior == DROP_CASCADE);

				append_object_object(ret, "%{cascade}s", tmp_obj);
			}
			break;
		default:
			elog(ERROR, "invalid subtype %c", node->subtype);
	}

	/* Done */
	ReleaseSysCache(domTup);

	return ret;
}

/*
 * Deparse a CreateStatsStmt.
 *
 * Given a statistics OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE STATISTICS %{if_not_exists}s %{identity}D ON %{expr:, }s FROM %{stat_table_identity}D
 */
static ObjTree *
deparse_CreateStatisticsStmt(Oid objectId, Node *parsetree)
{
	CreateStatsStmt *node = (CreateStatsStmt *) parsetree;
	Form_pg_statistic_ext statform;
	ObjTree    *ret;
	HeapTuple	tup;
	Datum		datum;
	bool		isnull;
	List	   *statexprs = NIL;

	tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for statistic with OID %u", objectId);

	statform = (Form_pg_statistic_ext) GETSTRUCT(tup);

	ret = new_objtree_VA("CREATE STATISTICS %{if_not_exists}s %{identity}D", 2,
						 "if_not_exists", ObjTypeString,
						 node->if_not_exists ? "IF NOT EXISTS" : "",
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(statform->stxnamespace,
												  NameStr(statform->stxname)));

	datum = SysCacheGetAttr(STATEXTOID, tup, Anum_pg_statistic_ext_stxexprs,
							&isnull);
	if (!isnull)
	{
		ListCell   *lc;
		Relation	statsrel;
		List	   *context;
		List	   *exprs = NIL;
		char	   *exprsString;

		statsrel = relation_open(statform->stxrelid, AccessShareLock);
		context = deparse_context_for(RelationGetRelationName(statsrel),
									  RelationGetRelid(statsrel));

		exprsString = TextDatumGetCString(datum);
		exprs = (List *) stringToNode(exprsString);

		foreach(lc, exprs)
		{
			Node	   *expr = (Node *) lfirst(lc);
			char	   *statexpr;

			statexpr = deparse_expression(expr, context, false, false);
			statexprs = lappend(statexprs, new_string_object(statexpr));
		}

		append_array_object(ret, "ON %{expr:, }s", statexprs);
		pfree(exprsString);
		relation_close(statsrel, AccessShareLock);
	}

	datum = SysCacheGetAttr(STATEXTOID, tup, Anum_pg_statistic_ext_stxkeys,
							&isnull);
	if (!isnull)
	{
		int			keyno;
		char	   *attname;
		List	   *statcols = NIL;
		int2vector *indoption;

		indoption = (int2vector *) DatumGetPointer(datum);

		for (keyno = 0; keyno < indoption->dim1; keyno++)
		{
			attname = get_attname(statform->stxrelid, indoption->values[keyno],
								  false);
			statcols = lappend(statcols, new_string_object(attname));
		}

		if (indoption->dim1)
		{
			/* Append a ',' if statexprs is present else append 'ON' */
			append_string_object(ret, "%{comma}s", "comma",
								 statexprs ? "," : "ON");
			append_array_object(ret, "%{cols:, }s", statcols);
		}
	}

	append_object_object(ret, "FROM %{stat_table_identity}D",
						 new_objtree_for_qualname(get_rel_namespace(statform->stxrelid),
												  get_rel_name(statform->stxrelid)));

	ReleaseSysCache(tup);

	return ret;
}

/*
 * Deparse an CreateForeignServerStmt (CREATE SERVER)
 *
 * Given a server OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax
 * CREATE SERVER %{identity}I %{type}s %{version}s FOREIGN DATA WRAPPER %{fdw}I
 * %{generic_options}s
 */
static ObjTree *
deparse_CreateForeignServerStmt(Oid objectId, Node *parsetree)
{
	CreateForeignServerStmt *node = (CreateForeignServerStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *tmp;

	ret = new_objtree_VA("CREATE SERVER %{identity}I", 1,
						 "identity", ObjTypeString, node->servername);

	/* Add a TYPE clause, if any */
	tmp = new_objtree("TYPE");
	if (node->servertype)
		append_string_object(tmp, "%{type}L", "type", node->servertype);
	else
		append_not_present(tmp);
	append_object_object(ret, "%{type}s", tmp);

	/* Add a VERSION clause, if any */
	tmp = new_objtree("VERSION");
	if (node->version)
		append_string_object(tmp, "%{version}L", "version", node->version);
	else
		append_not_present(tmp);
	append_object_object(ret, "%{version}s", tmp);

	append_string_object(ret, "FOREIGN DATA WRAPPER %{fdw}I", "fdw",
						 node->fdwname);

	/* Add an OPTIONS clause, if any */
	append_object_object(ret, "%{generic_options}s",
						 deparse_FdwOptions(node->options, NULL));

	return ret;
}

/*
 * Deparse an AlterForeignServerStmt (ALTER SERVER)
 *
 * Given a server OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax
 * ALTER SERVER %{identity}I %{version}s %{generic_options}s
 */
static ObjTree *
deparse_AlterForeignServerStmt(Oid objectId, Node *parsetree)
{
	AlterForeignServerStmt *node = (AlterForeignServerStmt *) parsetree;
	ObjTree    *tmp;

	/* Add a VERSION clause, if any */
	tmp = new_objtree("VERSION");
	if (node->has_version && node->version)
		append_string_object(tmp, "%{version}L", "version", node->version);
	else if (node->has_version)
		append_string_object(tmp, "version", "version", "NULL");
	else
		append_not_present(tmp);

	return new_objtree_VA("ALTER SERVER %{identity}I %{version}s %{generic_options}s", 3,
						  "identity", ObjTypeString, node->servername,
						  "version", ObjTypeObject, tmp,
						  "generic_options", ObjTypeObject,
						  deparse_FdwOptions(node->options, NULL));
}

/*
 * Deparse a CreatePLangStmt.
 *
 * Given a language OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{or_replace}s %{trusted}s LANGUAGE %{identity}s HANDLER %{handler}D
 * %{inline}s %{validator}s
 */
static ObjTree *
deparse_CreateLangStmt(Oid objectId, Node *parsetree)
{
	CreatePLangStmt *node = (CreatePLangStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *tmp;
	HeapTuple	langTup;
	Form_pg_language langForm;

	langTup = SearchSysCache1(LANGOID,
							  ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(langTup))
		elog(ERROR, "cache lookup failed for language with OID %u", objectId);
	langForm = (Form_pg_language) GETSTRUCT(langTup);

	ret = new_objtree_VA("CREATE %{or_replace}s %{trusted}s LANGUAGE %{identity}s", 3,
						 "or_replace", ObjTypeString,
						 node->replace ? "OR REPLACE" : "",
						 "trusted", ObjTypeString,
						 langForm->lanpltrusted ? "TRUSTED" : "",
						 "identity", ObjTypeString, node->plname);

	if (node->plhandler != NIL)
	{
		/* Add the HANDLER clause */
		append_object_object(ret, "HANDLER %{handler}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 langForm->lanplcallfoid));

		/* Add the INLINE clause, if any */
		tmp = new_objtree("INLINE");
		if (OidIsValid(langForm->laninline))
		{
			append_object_object(tmp, "%{handler_name}D",
								 new_objtree_for_qualname_id(ProcedureRelationId,
															 langForm->laninline));
		}
		else
			append_not_present(tmp);
		append_object_object(ret, "%{inline}s", tmp);

		/* Add the VALIDATOR clause, if any */
		tmp = new_objtree("VALIDATOR");
		if (OidIsValid(langForm->lanvalidator))
		{
			append_object_object(tmp, "%{handler_name}D",
								 new_objtree_for_qualname_id(ProcedureRelationId,
															 langForm->lanvalidator));
		}
		else
			append_not_present(tmp);
		append_object_object(ret, "%{validator}s", tmp);
	}

	ReleaseSysCache(langTup);

	return ret;
}

/*
 * Deparse a CreateForeignTableStmt (CREATE FOREIGN TABLE).
 *
 * Given a table OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * CREATE FOREIGN TABLE %{if_not_exists}s %{identity}D
 * [(%{table_elements:, }s) %{inherits}s
 *  | PARTITION OF %{parent_identity}D (%{typed_table_elements:, }s) %{partition_bound}s]
 * SERVER %{server}I %{generic_options}s
 */
static ObjTree *
deparse_CreateForeignTableStmt(Oid objectId, Node *parsetree)
{
	CreateForeignTableStmt *stmt = (CreateForeignTableStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	ObjTree    *createStmt;
	ObjTree    *tmpobj;
	List	   *tableelts = NIL;

	createStmt = new_objtree("CREATE FOREIGN TABLE");

	append_string_object(createStmt, "%{if_not_exists}s", "if_not_exists",
						 stmt->base.if_not_exists ? "IF NOT EXISTS" : "");

	tmpobj = new_objtree_for_qualname(relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(createStmt, "%{identity}D", tmpobj);

	dpcontext = deparse_context_for(RelationGetRelationName(relation),
									objectId);
	if (stmt->base.partbound)
	{
		/* Partitioned table */
		List	   *parents;
		ObjElem    *elem;

		parents = deparse_InhRelations(objectId);
		elem = (ObjElem *) linitial(parents);

		Assert(list_length(parents) == 1);

		append_format_string(createStmt, "PARTITION OF");

		append_object_object(createStmt, "%{parent_identity}D",
										 elem->value.object);

		tableelts = deparse_TableElements(relation, stmt->base.tableElts, dpcontext,
										  true, /* typed table */
										  false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		tmpobj = new_objtree("");
		if (tableelts)
			append_array_object(tmpobj, "(%{elements:, }s)", tableelts);
		else
			append_not_present(tmpobj);

		append_object_object(createStmt, "%{typed_table_elements}s", tmpobj);

		/*
		 * Add the partition_bound_spec, i.e. the FOR VALUES clause.
		 * Get pg_class.relpartbound. We cannot use partbound in the parsetree
		 * directly as it's the original partbound expression which haven't
		 * been transformed.
		 */
		append_string_object(createStmt, "%{partition_bound}s", "partition_bound",
							 RelationGetPartitionBound(objectId));

		/* No PARTITION BY clause for CREATE FOREIGN TABLE */
	}
	else
	{
		tableelts = deparse_TableElements(relation, stmt->base.tableElts, dpcontext,
										  false,		/* not typed table */
										  false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		if (tableelts)
			append_array_object(createStmt, "(%{table_elements:, }s)", tableelts);
		else
			append_format_string(createStmt, "()");

		/*
		* Add inheritance specification.  We cannot simply scan the list of
		* parents from the parser node, because that may lack the actual
		* qualified names of the parent relations.  Rather than trying to
		* re-resolve them from the information in the parse node, it seems
		* more accurate and convenient to grab it from pg_inherits.
		*/
		tmpobj = new_objtree("INHERITS");
		if (stmt->base.inhRelations != NIL)
			append_array_object(tmpobj, "(%{parents:, }D)", deparse_InhRelations(objectId));
		else
		{
			append_null_object(tmpobj, "(%{parents:, }D)");
			append_not_present(tmpobj);
		}
		append_object_object(createStmt, "%{inherits}s", tmpobj);
	}

	append_string_object(createStmt, "SERVER %{server}I", "server", stmt->servername);

	/* add an OPTIONS clause, if any */
	append_object_object(createStmt, "%{generic_options}s",
						 deparse_FdwOptions(stmt->options, NULL));

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

	switch (define->kind)
	{
		case OBJECT_AGGREGATE:
			return deparse_DefineStmt_Aggregate(objectId, define);

		case OBJECT_COLLATION:
			return deparse_DefineStmt_Collation(objectId, define, secondaryObj);

		case OBJECT_OPERATOR:
			return deparse_DefineStmt_Operator(objectId, define);

		case OBJECT_TSCONFIGURATION:
			return deparse_DefineStmt_TSConfig(objectId, define, secondaryObj);

		case OBJECT_TSDICTIONARY:
			return deparse_DefineStmt_TSDictionary(objectId, define);

		case OBJECT_TSPARSER:
			return deparse_DefineStmt_TSParser(objectId, define);

		case OBJECT_TSTEMPLATE:
			return deparse_DefineStmt_TSTemplate(objectId, define);

		case OBJECT_TYPE:
			return deparse_DefineStmt_Type(objectId, define);

		default:
			elog(ERROR, "unsupported object kind: %d", define->kind);
	}

	return NULL;
}

/*
 * Form an ObjElem to be used as a single argument in an aggregate argument
 * list
 */
static ObjElem *
form_agg_argument(int idx, char *modes, char **names, Oid *types)
{
	ObjTree	   *arg;

	arg = new_objtree("");

	append_string_object(arg, "%{mode}s", "mode",
						 (modes && modes[idx] == 'v') ? "VARIADIC" : "");
	append_string_object(arg, "%{name}s", "name", names ? names[idx] : "");
	append_object_object(arg, "%{type}T",
						 new_objtree_for_type(types[idx], -1));

	return new_object_object(arg);
}

/*
 * Deparse a DefineStmt (CREATE AGGREGATE)
 *
 * Given a aggregate OID, return an ObjTree representing the CREATE command.
 *
 * Verbose syntax
 * CREATE AGGREGATE %{identity}D(%{types}s) (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_Aggregate(Oid objectId, DefineStmt *define)
{
	HeapTuple   aggTup;
	HeapTuple   procTup;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list;
	Datum		initval;
	bool		isnull;
	Form_pg_aggregate agg;
	Form_pg_proc proc;
	Form_pg_operator op;
	HeapTuple	tup;

	aggTup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(aggTup))
		elog(ERROR, "cache lookup failed for aggregate with OID %u", objectId);
	agg = (Form_pg_aggregate) GETSTRUCT(aggTup);

	procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(agg->aggfnoid));
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failed for procedure with OID %u",
			 agg->aggfnoid);
	proc = (Form_pg_proc) GETSTRUCT(procTup);

	stmt = new_objtree("CREATE AGGREGATE");

	append_object_object(stmt, "%{identity}D",
						 new_objtree_for_qualname(proc->pronamespace,
												  NameStr(proc->proname)));

	/*
	 * Add the argument list.  There are three cases to consider:
	 *
	 * 1. no arguments, in which case the signature is (*).
	 *
	 * 2. Not an ordered-set aggregate.  In this case there's one or more
	 * arguments.
	 *
	 * 3. Ordered-set aggregates. These have zero or more direct arguments, and
	 * one or more ordered arguments.
	 *
	 * We don't need to consider default values or table parameters, and the
	 * only mode that we need to consider is VARIADIC.
	 */

	if (proc->pronargs == 0)
		append_string_object(stmt, "(%{types}s)", "types", "*");
	else if (!AGGKIND_IS_ORDERED_SET(agg->aggkind))
	{
		int			i;
		int			nargs;
		Oid		   *types;
		char	   *modes;
		char	  **names;

		nargs = get_func_arg_info(procTup, &types, &names, &modes);

		/* only direct arguments in this case */
		list = NIL;
		for (i = 0; i < nargs; i++)
		{
			list = lappend(list, form_agg_argument(i, modes, names, types));
		}

		tmp = new_objtree_VA("%{direct:, }s", 1,
							 "direct", ObjTypeArray, list);
		append_object_object(stmt, "(%{types}s)", tmp);
	}
	else
	{
		int			i;
		int			nargs;
		Oid		   *types;
		char	   *modes;
		char	  **names;

		nargs = get_func_arg_info(procTup, &types, &names, &modes);

		/* direct arguments ... */
		list = NIL;
		for (i = 0; i < agg->aggnumdirectargs; i++)
		{
			list = lappend(list, form_agg_argument(i, modes, names, types));
		}
		tmp = new_objtree_VA("%{direct:, }s", 1,
							 "direct", ObjTypeArray, list);

		/*
		 * ... and aggregated arguments.  If the last direct argument is
		 * VARIADIC, we need to repeat it here rather than searching for more
		 * arguments.  (See aggr_args production in gram.y for an explanation.)
		 */
		if (modes && modes[agg->aggnumdirectargs - 1] == 'v')
		{
			list = list_make1(form_agg_argument(agg->aggnumdirectargs - 1,
												modes, names, types));
		}
		else
		{
			list = NIL;
			for (i = agg->aggnumdirectargs; i < nargs; i++)
			{
				list = lappend(list, form_agg_argument(i, modes, names, types));
			}
		}
		append_array_object(tmp, "ORDER BY %{aggregated:, }s", list);

		append_object_object(stmt, "(%{types}s)", tmp);
	}

	/* Add the definition clause */
	list = NIL;

	/* SFUNC */
	tmp = new_objtree_VA("SFUNC=%{procedure}D", 1,
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId, agg->aggtransfn));
	list = lappend(list, new_object_object(tmp));

	/* STYPE */
	tmp = new_objtree_VA("STYPE=%{type}T", 1,
						 "type", ObjTypeObject,
						 new_objtree_for_type(agg->aggtranstype, -1));
	list = lappend(list, new_object_object(tmp));

	/* SSPACE */
	if (agg->aggtransspace != 0)
	{
		tmp = new_objtree_VA("SSPACE=%{space}n", 1,
							 "space", ObjTypeInteger, agg->aggtransspace);
	}
	else
	{
		tmp = new_objtree("SSPACE=%{space}n");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* FINALFUNC */
	if (OidIsValid(agg->aggfinalfn))
		tmp = new_objtree_VA("FINALFUNC=%{procedure}D", 1,
							 "procedure", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 agg->aggfinalfn));
	else
	{
		tmp = new_objtree("FINALFUNC=%{procedure}D");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* FINALFUNC_EXTRA */
	if (agg->aggfinalextra)
		tmp = new_objtree_VA("FINALFUNC_EXTRA=%{value}s", 1,
							 "value", ObjTypeString, "true");
	else
		tmp = new_objtree_VA("FINALFUNC_EXTRA=%{value}s", 1,
							 "value", ObjTypeString, "false");
	list = lappend(list, new_object_object(tmp));

	/* INITCOND */
	initval = SysCacheGetAttr(AGGFNOID, aggTup,
							  Anum_pg_aggregate_agginitval,
							  &isnull);
	tmp = new_objtree("INITCOND=");
	if (!isnull)
		tmp = new_objtree_VA("INITCOND=%{initval}L", 1,
							 "initval", ObjTypeString, TextDatumGetCString(initval));
	else
	{
		tmp = new_objtree("INITCOND=%{initval}L");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MSFUNC */
	if (OidIsValid(agg->aggmtransfn))
		tmp = new_objtree_VA("MSFUNC=%{procedure}D", 1,
							 "procedure", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId, agg->aggmtransfn));
	else
	{
		tmp = new_objtree("MSFUNC=%{procedure}D");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MSTYPE */
	if (OidIsValid(agg->aggmtranstype))
		tmp = new_objtree_VA("MSTYPE=%{type}T", 1,
							 "type", ObjTypeObject, new_objtree_for_type(agg->aggmtranstype, -1));
	else
	{
		tmp = new_objtree("MSTYPE=%{type}T");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MSSPACE */
	if (agg->aggmtransspace != 0)
		tmp = new_objtree_VA("MSSPACE=%{space}n", 1,
							 "space", ObjTypeInteger,agg->aggmtransspace);
	else
	{
		tmp = new_objtree("MSSPACE=%{space}n");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MINVFUNC */
	if (OidIsValid(agg->aggminvtransfn))
		tmp = new_objtree_VA("MINVFUNC=%{type}T", 1,
							 "type", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 agg->aggminvtransfn));
	else
	{
		tmp = new_objtree("MINVFUNC==%{type}T");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MFINALFUNC */
	if (OidIsValid(agg->aggmfinalfn))
		tmp = new_objtree_VA("MFINALFUNC=%{procedure}D", 1,
							 "procedure", ObjTypeObject,
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 agg->aggmfinalfn));
	else
	{
		tmp = new_objtree("MFINALFUNC=%{procedure}D");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MFINALFUNC_EXTRA */
	if (agg->aggmfinalextra)
		tmp = new_objtree_VA("MFINALFUNC_EXTRA=%{value}s", 1,
							 "value", ObjTypeString, "true");
	else
	{
		tmp = new_objtree("MFINALFUNC_EXTRA=%{value}s");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* MINITCOND */
	initval = SysCacheGetAttr(AGGFNOID, aggTup,
							  Anum_pg_aggregate_aggminitval,
							  &isnull);
	if (!isnull)
		tmp = new_objtree_VA("MINITCOND=%{initval}L", 1,
							 "initval", ObjTypeString, TextDatumGetCString(initval));
	else
	{
		tmp = new_objtree("MINITCOND=%{initval}L");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* HYPOTHETICAL */
	if (agg->aggkind == AGGKIND_HYPOTHETICAL)
		tmp = new_objtree_VA("HYPOTHETICAL=%{value}s", 1,
							 "value", ObjTypeString, "true");
	else
	{
		tmp = new_objtree("HYPOTHETICAL=%{value}s");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* SORTOP */
	if (OidIsValid(agg->aggsortop))
	{
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(agg->aggsortop));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for operator with OID %u", agg->aggsortop);
		op = (Form_pg_operator) GETSTRUCT(tup);
		tmp = new_objtree_VA("SORTOP=%{operator}D", 1,
							 "operator", ObjTypeObject,
							 new_objtree_for_qualname(op->oprnamespace,
													  NameStr(op->oprname)));

		ReleaseSysCache(tup);
	}
	else
	{
		tmp = new_objtree("SORTOP=%{operator}D");
		append_bool_object(tmp, "present", false);
	}
	list = lappend(list, new_object_object(tmp));

	/* Done with the definition clause */
	append_array_object(stmt, "(%{elems:, }s)", list);

	ReleaseSysCache(procTup);
	ReleaseSysCache(aggTup);

	return stmt;
}

/*
 * Deparse a DefineStmt (CREATE COLLATION)
 *
 * Given a collation OID, return an ObjTree representing the CREATE command.
 *
 * Verbose syntax
 * CREATE COLLATION %{identity}D FROM %{from_identity}D (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_Collation(Oid objectId, DefineStmt *define,
							 ObjectAddress fromCollid)
{
	ObjTree    *ret;
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

	ret = new_objtree_VA("CREATE COLLATION %{identity}D", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(colForm->collnamespace,
												  NameStr(colForm->collname)));

	if (OidIsValid(fromCollid.objectId))
	{
		Oid			collid = fromCollid.objectId;
		HeapTuple	tp;
		Form_pg_collation fromColForm;

		/*
		 * CREATE COLLATION %{identity}D FROM existing_collation;
		 */
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation with OID %u", collid);

		fromColForm = (Form_pg_collation) GETSTRUCT(tp);

		append_object_object(ret, "FROM %{from_identity}D",
							 new_objtree_for_qualname(fromColForm->collnamespace,
													  NameStr(fromColForm->collname)));

		ReleaseSysCache(tp);
		ReleaseSysCache(colTup);
		return ret;
	}

	/* LOCALE */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_colliculocale, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("LOCALE=%{locale}L", 2,
							 "clause", ObjTypeString, "locale",
							 "locale", ObjTypeString,
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	/* LC_COLLATE */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_collcollate, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("LC_COLLATE=%{collate}L", 2,
							 "clause", ObjTypeString, "collate",
							 "collate", ObjTypeString,
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	/* LC_CTYPE */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_collctype, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("LC_CTYPE=%{ctype}L", 2,
							 "clause", ObjTypeString, "ctype",
							 "ctype", ObjTypeString,
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	/* PROVIDER */
	if (colForm->collprovider == COLLPROVIDER_ICU)
	{
		tmp = new_objtree_VA("PROVIDER=%{provider}L", 2,
							 "clause", ObjTypeString, "provider",
							 "provider", ObjTypeString,
							 psprintf("%s", "icu"));
		list = lappend(list, new_object_object(tmp));
	}
	else if (colForm->collprovider == COLLPROVIDER_LIBC)
	{
		tmp = new_objtree_VA("PROVIDER=%{provider}L", 2,
							 "clause", ObjTypeString, "provider",
							 "provider", ObjTypeString,
							 psprintf("%s", "libc"));
		list = lappend(list, new_object_object(tmp));
	}

	/* DETERMINISTIC */
	if (colForm->collisdeterministic)
	{
		tmp = new_objtree_VA("DETERMINISTIC=%{deterministic}L", 2,
							 "clause", ObjTypeString, "deterministic",
							 "deterministic", ObjTypeString,
							 psprintf("%s", "true"));
		list = lappend(list, new_object_object(tmp));
	}

	/* VERSION */
	datum = SysCacheGetAttr(COLLOID, colTup, Anum_pg_collation_collversion, &isnull);
	if (!isnull)
	{
		tmp = new_objtree_VA("VERSION=%{version}L", 2,
							 "clause", ObjTypeString, "version",
							 "version", ObjTypeString,
							 psprintf("%s", TextDatumGetCString(datum)));
		list = lappend(list, new_object_object(tmp));
	}

	append_array_object(ret, "(%{elems:, }s)", list);
	ReleaseSysCache(colTup);

	return ret;
}

/*
 * Deparse a DefineStmt (CREATE OPERATOR)
 *
 * Given a operator OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE OPERATOR %{identity}O (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_Operator(Oid objectId, DefineStmt *define)
{
	HeapTuple	oprTup;
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	List	   *list = NIL;
	Form_pg_operator oprForm;

	oprTup = SearchSysCache1(OPEROID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(oprTup))
		elog(ERROR, "cache lookup failed for operator with OID %u", objectId);
	oprForm = (Form_pg_operator) GETSTRUCT(oprTup);

	ret = new_objtree_VA("CREATE OPERATOR %{identity}O", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(oprForm->oprnamespace,
												  NameStr(oprForm->oprname)));

	/* PROCEDURE */
	tmp_obj = new_objtree_VA("PROCEDURE=%{procedure}D", 2,
							"clause", ObjTypeString, "procedure",
							"procedure", ObjTypeObject,
							new_objtree_for_qualname_id(ProcedureRelationId,
														oprForm->oprcode));
	list = lappend(list, new_object_object(tmp_obj));

	/* LEFTARG */
	tmp_obj = new_objtree_VA("LEFTARG=", 1,
							"clause", ObjTypeString, "leftarg");
	if (OidIsValid(oprForm->oprleft))
		append_object_object(tmp_obj, "%{type}T",
							 new_objtree_for_type(oprForm->oprleft, -1));
	else
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* RIGHTARG */
	tmp_obj = new_objtree_VA("RIGHTARG=", 1,
							"clause", ObjTypeString, "rightarg");
	if (OidIsValid(oprForm->oprright))
		append_object_object(tmp_obj, "%{type}T",
							 new_objtree_for_type(oprForm->oprright, -1));
	else
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* COMMUTATOR */
	tmp_obj = new_objtree_VA("COMMUTATOR=", 1,
							"clause", ObjTypeString, "commutator");
	if (OidIsValid(oprForm->oprcom))
		append_object_object(tmp_obj, "%{oper}D",
							 new_objtree_for_qualname_id(OperatorRelationId,
														 oprForm->oprcom));
	else
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* NEGATOR */
	tmp_obj = new_objtree_VA("NEGATOR=", 1,
							"clause", ObjTypeString, "negator");
	if (OidIsValid(oprForm->oprnegate))
		append_object_object(tmp_obj, "%{oper}D",
							 new_objtree_for_qualname_id(OperatorRelationId,
														 oprForm->oprnegate));
	else
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* RESTRICT */
	tmp_obj = new_objtree_VA("RESTRICT=", 1,
							"clause", ObjTypeString, "restrict");
	if (OidIsValid(oprForm->oprrest))
		append_object_object(tmp_obj, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 oprForm->oprrest));
	else
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* JOIN */
	tmp_obj = new_objtree_VA("JOIN=", 1,
							"clause", ObjTypeString, "join");
	if (OidIsValid(oprForm->oprjoin))
		append_object_object(tmp_obj, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 oprForm->oprjoin));
	else
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* HASHES */
	tmp_obj = new_objtree_VA("HASHES", 1,
							"clause", ObjTypeString, "hashes");
	if (!oprForm->oprcanhash)
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	/* MERGES */
	tmp_obj = new_objtree_VA("MERGES", 1,
							"clause", ObjTypeString, "merges");
	if (!oprForm->oprcanmerge)
		append_not_present(tmp_obj);
	list = lappend(list, new_object_object(tmp_obj));

	append_array_object(ret, "(%{elems:, }s)", list);

	ReleaseSysCache(oprTup);

	return ret;
}

/*
 * Deparse a CREATE TYPE statement.
 *
 * Verbose syntax
 * CREATE TYPE %{identity}D %{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_Type(Oid objectId, DefineStmt *define)
{
	HeapTuple	typTup;
	ObjTree    *ret;
	ObjTree    *tmp;
	List	   *list = NIL;
	char	   *str;
	Datum		dflt;
	bool		isnull;
	Form_pg_type typForm;

	typTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for type with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	ret = new_objtree_VA("CREATE TYPE %{identity}D", 1,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(typForm->typnamespace,
												  NameStr(typForm->typname)));

	/* Shell types. */
	if (!typForm->typisdefined)
	{
		ReleaseSysCache(typTup);
		return ret;
	}

	/* Add the definition clause */
	/* INPUT */
	tmp = new_objtree_VA("(INPUT=%{procedure}D", 2,
						 "clause", ObjTypeString, "input",
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 typForm->typinput));
	list = lappend(list, new_object_object(tmp));

	/* OUTPUT */
	tmp = new_objtree_VA("OUTPUT=%{procedure}D", 2,
						 "clause", ObjTypeString, "output",
						 "procedure", ObjTypeObject,
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
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* SEND */
	tmp = new_objtree_VA("SEND=", 1,
						 "clause", ObjTypeString, "send");
	if (OidIsValid(typForm->typsend))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typsend));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* TYPMOD_IN */
	tmp = new_objtree_VA("TYPMOD_IN=", 1,
						 "clause", ObjTypeString, "typmod_in");
	if (OidIsValid(typForm->typmodin))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodin));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* TYPMOD_OUT */
	tmp = new_objtree_VA("TYPMOD_OUT=", 1,
						 "clause", ObjTypeString, "typmod_out");
	if (OidIsValid(typForm->typmodout))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typmodout));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* ANALYZE */
	tmp = new_objtree_VA("ANALYZE=", 1,
						 "clause", ObjTypeString, "analyze");
	if (OidIsValid(typForm->typanalyze))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 typForm->typanalyze));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* INTERNALLENGTH */
	if (typForm->typlen == -1)
		tmp = new_objtree("INTERNALLENGTH=VARIABLE");
	else
		tmp = new_objtree_VA("INTERNALLENGTH=%{typlen}n", 1,
							 "typlen", ObjTypeInteger, typForm->typlen);

	list = lappend(list, new_object_object(tmp));

	/* PASSEDBYVALUE */
	tmp = new_objtree_VA("PASSEDBYVALUE", 1,
						 "clause", ObjTypeString, "passedbyvalue");
	if (!typForm->typbyval)
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

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

	/* ALIGNMENT */
	tmp = new_objtree_VA("ALIGNMENT=%{align}s", 2,
						 "clause", ObjTypeString, "alignment",
						 "align", ObjTypeString, str);
	list = lappend(list, new_object_object(tmp));

	/* STORAGE */
	tmp = new_objtree_VA("STORAGE=%{storage}s", 2,
						 "clause", ObjTypeString, "storage",
						 "storage", ObjTypeString,
						 get_type_storage(typForm->typstorage));
	list = lappend(list, new_object_object(tmp));

	/* CATEGORY */
	tmp = new_objtree_VA("CATEGORY=%{category}s", 2,
						 "clause", ObjTypeString, "category",
						 "category", ObjTypeString,
						 psprintf("%c", typForm->typcategory));
	list = lappend(list, new_object_object(tmp));

	/* PREFERRED */
	tmp = new_objtree_VA("PREFERRED=", 1,
						 "clause", ObjTypeString, "preferred");
	if (!typForm->typispreferred)
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* DEFAULT */
	dflt = SysCacheGetAttr(TYPEOID, typTup,
						   Anum_pg_type_typdefault,
						   &isnull);
	tmp = new_objtree_VA("DEFAULT=", 1,
						 "clause", ObjTypeString, "default");
	if (!isnull)
		append_string_object(tmp, "%{default}s", "default",
							 TextDatumGetCString(dflt));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* ELEMENT */
	tmp = new_objtree_VA("ELEMENT=", 1,
						 "clause", ObjTypeString, "element");
	if (OidIsValid(typForm->typelem))
		append_object_object(tmp, "elem",
							 new_objtree_for_type(typForm->typelem, -1));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* DELIMITER */
	tmp = new_objtree_VA("DELIMITER=", 1,
						 "clause", ObjTypeString, "delimiter");
	append_string_object(tmp, "%{delim}L", "delim",
						 psprintf("%c", typForm->typdelim));
	list = lappend(list, new_object_object(tmp));

	/* COLLATABLE */
	tmp = new_objtree_VA("COLLATABLE=", 1,
						 "clause", ObjTypeString, "collatable");
	if (!OidIsValid(typForm->typcollation))
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	append_array_object(ret, "%{elems:, }s)", list);

	ReleaseSysCache(typTup);

	return ret;
}

/*
 * Deparse a CREATE TEXT SEARCH CONFIGURATION statement.
 *
 * Verbose syntax
 * CREATE TEXT SEARCH CONFIGURATION %{identity}D (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_TSConfig(Oid objectId, DefineStmt *define,
							ObjectAddress copied)
{
	HeapTuple	tscTup;
	HeapTuple	tspTup;
	ObjTree    *ret;
	ObjTree    *tmp;
	Form_pg_ts_config tscForm;
	Form_pg_ts_parser tspForm;
	List	   *list = NIL;

	tscTup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tscTup))
		elog(ERROR, "cache lookup failed for text search configuration with OID %u",
			 objectId);
	tscForm = (Form_pg_ts_config) GETSTRUCT(tscTup);

	tspTup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(tscForm->cfgparser));
	if (!HeapTupleIsValid(tspTup))
		elog(ERROR, "cache lookup failed for text search parser %u",
			 tscForm->cfgparser);
	tspForm = (Form_pg_ts_parser) GETSTRUCT(tspTup);

	/*
	 * Add the definition clause.  If we have COPY'ed an existing config, add
	 * a COPY clause; otherwise add a PARSER clause.
	 */
	/* COPY */
	tmp = new_objtree_VA("COPY=", 1,
						 "clause", ObjTypeString, "copy");
	if (OidIsValid(copied.objectId))
		append_object_object(tmp, "%{tsconfig}D",
							 new_objtree_for_qualname_id(TSConfigRelationId,
														 copied.objectId));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* PARSER */
	tmp = new_objtree_VA("PARSER=", 1,
						 "clause", ObjTypeString, "parser");
	if (copied.objectId == InvalidOid)
		append_object_object(tmp, "%{parser}D",
							 new_objtree_for_qualname(tspForm->prsnamespace,
													  NameStr(tspForm->prsname)));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	ret = new_objtree_VA("CREATE TEXT SEARCH CONFIGURATION %{identity}D (%{elems:, }s)", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(tscForm->cfgnamespace,
												  NameStr(tscForm->cfgname)),
						 "elems", ObjTypeArray, list);

	ReleaseSysCache(tspTup);
	ReleaseSysCache(tscTup);
	return ret;
}

/*
 * Deparse a CREATE TEXT SEARCH PARSER statement.
 *
 * Verbose syntax
 * CREATE TEXT SEARCH PARSER %{identity}D (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_TSParser(Oid objectId, DefineStmt *define)
{
	HeapTuple	tspTup;
	ObjTree    *ret;
	ObjTree    *tmp;
	List	   *list = NIL;
	Form_pg_ts_parser tspForm;

	tspTup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tspTup))
		elog(ERROR, "cache lookup failed for text search parser with OID %u",
			 objectId);
	tspForm = (Form_pg_ts_parser) GETSTRUCT(tspTup);

	/* Add the definition clause */
	/* START */
	tmp = new_objtree_VA("START=%{procedure}D", 2,
						 "clause", ObjTypeString, "start",
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prsstart));
	list = lappend(list, new_object_object(tmp));

	/* GETTOKEN */
	tmp = new_objtree_VA("GETTOKEN=%{procedure}D", 2,
						 "clause", ObjTypeString, "gettoken",
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prstoken));
	list = lappend(list, new_object_object(tmp));

	/* END */
	tmp = new_objtree_VA("END=%{procedure}D", 2,
						 "clause", ObjTypeString, "end",
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tspForm->prsend));
	list = lappend(list, new_object_object(tmp));

	/* LEXTYPES */
	tmp = new_objtree_VA("LEXTYPES=%{procedure}D", 2,
						 "clause", ObjTypeString, "lextypes",
						 "procedure", ObjTypeObject,
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
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	ret = new_objtree_VA("CREATE TEXT SEARCH PARSER %{identity}D (%{elems:, }s)", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(tspForm->prsnamespace,
												  NameStr(tspForm->prsname)),
						 "elems", ObjTypeArray, list);

	ReleaseSysCache(tspTup);
	return ret;
}

/*
 * Deparse a CREATE TEXT SEARCH DICTIONARY statement.
 *
 * Verbose syntax
 * CREATE TEXT SEARCH DICTIONARY %{identity}D (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_TSDictionary(Oid objectId, DefineStmt *define)
{
	HeapTuple	tsdTup;
	ObjTree    *ret;
	ObjTree    *tmp;
	List	   *list = NIL;
	Datum		options;
	bool		isnull;
	Form_pg_ts_dict tsdForm;

	tsdTup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tsdTup))
		elog(ERROR, "cache lookup failed for text search dictionary with OID %u",
			 objectId);
	tsdForm = (Form_pg_ts_dict) GETSTRUCT(tsdTup);



	/* Add the definition clause */
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
	tmp = new_objtree("");
	if (!isnull)
		append_string_object(tmp, "%{options}s", "options",
							 TextDatumGetCString(options));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	ret = new_objtree_VA("CREATE TEXT SEARCH DICTIONARY %{identity}D (%{elems:, }s)", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(tsdForm->dictnamespace,
												  NameStr(tsdForm->dictname)),
						 "elems", ObjTypeArray, list);

	ReleaseSysCache(tsdTup);
	return ret;
}

/*
 * Deparse a CREATE TEXT SEARCH TEMPLATE statement.
 *
 * Verbose syntax
 * CREATE TEXT SEARCH TEMPLATE %{identity}D (%{elems:, }s)
 */
static ObjTree *
deparse_DefineStmt_TSTemplate(Oid objectId, DefineStmt *define)
{
	HeapTuple	tstTup;
	ObjTree    *ret;
	ObjTree    *tmp;
	List	   *list = NIL;
	Form_pg_ts_template tstForm;

	tstTup = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tstTup))
		elog(ERROR, "cache lookup failed for text search template with OID %u",
			 objectId);
	tstForm = (Form_pg_ts_template) GETSTRUCT(tstTup);

	/* Add the definition clause */
	/* INIT */
	tmp = new_objtree_VA("INIT=", 1,
						 "clause", ObjTypeString, "init");
	if (OidIsValid(tstForm->tmplinit))
		append_object_object(tmp, "%{procedure}D",
							 new_objtree_for_qualname_id(ProcedureRelationId,
														 tstForm->tmplinit));
	else
		append_not_present(tmp);
	list = lappend(list, new_object_object(tmp));

	/* LEXIZE */
	tmp = new_objtree_VA("LEXIZE=%{procedure}D", 2,
						 "clause", ObjTypeString, "lexize",
						 "procedure", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 tstForm->tmpllexize));
	list = lappend(list, new_object_object(tmp));

	ret = new_objtree_VA("CREATE TEXT SEARCH TEMPLATE %{identity}D (%{elems:, }s)", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(tstForm->tmplnamespace,
												  NameStr(tstForm->tmplname)),
						 "elems", ObjTypeArray, list);

	ReleaseSysCache(tstTup);
	return ret;
}

/*
 * Deparse an ALTER TEXT SEARCH CONFIGURATION statement.
 *
 * Verbose syntax
 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ADD MAPPING
 * FOR %{tokentype:, }I WITH %{dictionaries:, }D
 *	OR
 * ALTER TEXT SEARCH CONFIGURATION %{identity}D DROP MAPPING %{if_exists}s
 * FOR %{tokentype}I
 *	OR
 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ALTER MAPPING
 * FOR %{tokentype:, }I WITH %{dictionaries:, }D
 *	OR
 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ALTER MAPPING
 * REPLACE %{old_dictionary}D WITH %{new_dictionary}D
 *	OR
 * ALTER TEXT SEARCH CONFIGURATION %{identity}D ALTER MAPPING
 * FOR %{tokentype:, }I REPLACE %{old_dictionary}D WITH %{new_dictionary}D
 */
static ObjTree *
deparse_AlterTSConfigurationStmt(CollectedCommand *cmd)
{
	AlterTSConfigurationStmt *node = (AlterTSConfigurationStmt *) cmd->parsetree;
	ObjTree    *ret;
	ObjTree    *tmp;
	List	   *list = NIL;
	ListCell   *cell;
	int			i;

	ret = new_objtree("ALTER TEXT SEARCH CONFIGURATION");

	/* Determine the format string appropriate to each subcommand */
	switch (node->kind)
	{
		case ALTER_TSCONFIG_ADD_MAPPING:
			append_object_object(ret, "%{identity}D ADD MAPPING",
								 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
															 cmd->d.atscfg.address.objectId));
			break;

		case ALTER_TSCONFIG_DROP_MAPPING:
			append_object_object(ret, "%{identity}D DROP MAPPING",
								 new_objtree_for_qualname_id(cmd->d.atscfg.address.classId,
															 cmd->d.atscfg.address.objectId));
			tmp = new_objtree("IF EXISTS");
			append_bool_object(tmp, "present", node->missing_ok);
			append_object_object(ret, "%{if_exists}s", tmp);
			break;

		case ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN:
		case ALTER_TSCONFIG_REPLACE_DICT:
		case ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN:
			append_object_object(ret, "%{identity}D ALTER MAPPING",
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
		foreach(cell, node->tokentype)
			list = lappend(list, new_string_object(strVal(lfirst(cell))));
		append_array_object(ret, "FOR %{tokentype:, }I", list);
	}

	/* Add further subcommand-specific elements */
	if (node->kind == ALTER_TSCONFIG_ADD_MAPPING ||
		node->kind == ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN)
	{
		/* ADD MAPPING and ALTER MAPPING FOR need a list of dictionaries */
		list = NIL;
		for (i = 0; i < cmd->d.atscfg.ndicts; i++)
		{
			ObjTree    *dict_obj;

			dict_obj = new_objtree_for_qualname_id(TSDictionaryRelationId,
												  cmd->d.atscfg.dictIds[i]);
			list = lappend(list,
						   new_object_object(dict_obj));
		}
		append_array_object(ret, "WITH %{dictionaries:, }D", list);
	}
	else if (node->kind == ALTER_TSCONFIG_REPLACE_DICT ||
			 node->kind == ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN)
	{
		/* The REPLACE forms want old and new dictionaries */
		Assert(cmd->d.atscfg.ndicts == 2);
		append_object_object(ret, "REPLACE %{old_dictionary}D",
							 new_objtree_for_qualname_id(TSDictionaryRelationId,
														 cmd->d.atscfg.dictIds[0]));
		append_object_object(ret, "WITH %{new_dictionary}D",
							 new_objtree_for_qualname_id(TSDictionaryRelationId,
														 cmd->d.atscfg.dictIds[1]));
	}

	return ret;
}

/*
 * Deparse an ALTER TEXT SEARCH DICTIONARY statement.
 *
 * Verbose syntax
 * ALTER TEXT SEARCH DICTIONARY %{identity}D (%{definition:, }s)
 */
static ObjTree *
deparse_AlterTSDictionaryStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *ret;
	ObjTree    *tmp;
	Datum		options;
	List	   *definition = NIL;
	bool		isnull;
	HeapTuple	tsdTup;
	Form_pg_ts_dict tsdForm;

	tsdTup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tsdTup))
		elog(ERROR, "cache lookup failed for text search dictionary with OID %u",
			 objectId);
	tsdForm = (Form_pg_ts_dict) GETSTRUCT(tsdTup);

	/*
	 * Add the definition list according to the pg_ts_dict dictinitoption
	 * column
	 */
	options = SysCacheGetAttr(TSDICTOID, tsdTup,
							  Anum_pg_ts_dict_dictinitoption,
							  &isnull);
	tmp = new_objtree("");
	if (!isnull)
		append_string_object(tmp, "%{options}s", "options",
							 TextDatumGetCString(options));
	else
		append_not_present(tmp);

	definition = lappend(definition, new_object_object(tmp));

	ret = new_objtree_VA("ALTER TEXT SEARCH DICTIONARY %{identity}D (%{definition:, }s)", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(tsdForm->dictnamespace,
												  NameStr(tsdForm->dictname)),
						 "definition", ObjTypeArray, definition);

	ReleaseSysCache(tsdTup);
	return ret;
}

/*
 * deparse_ViewStmt
 *		deparse a ViewStmt
 *
 * Given a view OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{or_replace}s %{persistence}s VIEW %{identity}D AS %{query}s
 */
static ObjTree *
deparse_ViewStmt(Oid objectId, Node *parsetree)
{
	ViewStmt   *node = (ViewStmt *) parsetree;
	ObjTree    *ret;
	Relation	relation;

	relation = relation_open(objectId, AccessShareLock);

	ret = new_objtree_VA("CREATE %{or_replace}s %{persistence}s VIEW %{identity}D AS %{query}s", 4,
						 "or_replace", ObjTypeString,
						 node->replace ? "OR REPLACE" : "",
						 "persistence", ObjTypeString,
						 get_persistence_str(relation->rd_rel->relpersistence),
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(relation->rd_rel->relnamespace,
												  RelationGetRelationName(relation)),
						 "query", ObjTypeString,
						 pg_get_viewdef_internal(objectId));

	relation_close(relation, AccessShareLock);
	return ret;
}

/*
 * Deparse CREATE Materialized View statement, it is a variant of
 * CreateTableAsStmt
 *
 * Note that CREATE TABLE AS SELECT INTO can also be deparsed by
 * deparse_CreateTableAsStmt to remove the SELECT INTO clause.
 *
 * Verbose syntax
 * CREATE %{persistence}s [MATERIALIZED VIEW | TABLE] %{if_not_exists}s
 * 		%{identity}D %{columns}s [%{on_commit}s] %{tablespace}s
 *  		AS %{query}s %{with_no_data}s"
 */
static ObjTree *
deparse_CreateTableAsStmt_vanilla(Oid objectId, Node *parsetree)
{
	CreateTableAsStmt *node = (CreateTableAsStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	ObjTree    *ret;
	ObjTree    *tmp;
	ObjTree    *tmp2;
	char	   *fmt;
	List	   *list = NIL;
	ListCell   *cell;

	/* Reject unsupported case right away. */
	if (((Query *) (node->query))->commandType == CMD_UTILITY)
		elog(ERROR, "unimplemented deparse of CREATE TABLE AS EXECUTE");

	/*
	 * Note that INSERT INTO is deparsed as CREATE TABLE AS.  They are
	 * functionally equivalent synonyms so there is no harm from this.
	 */
	if (node->objtype == OBJECT_MATVIEW)
		fmt = "CREATE %{persistence}s MATERIALIZED VIEW %{if_not_exists}s %{identity}D";
	else
		fmt = "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D";

	ret = new_objtree_VA(fmt, 3,
						 "persistence", ObjTypeString,
						 get_persistence_str(node->into->rel->relpersistence),
						 "if_not_exists", ObjTypeString,
						 node->if_not_exists ? "IF NOT EXISTS" : "",
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname_id(RelationRelationId,
													 objectId));

	/* COLUMNS clause */
	if (node->into->colNames)
	{
		foreach(cell, node->into->colNames)
			list = lappend(list, new_string_object(strVal(lfirst(cell))));

		tmp = new_objtree_VA("(%{columns:, }I)", 1,
							 "columns", ObjTypeArray, list);
	}
	else
		tmp = new_objtree_VA("", 1,
							 "present", ObjTypeBool, false);

	append_object_object(ret, "%{columns}s", tmp);

	/* USING clause */
	tmp = new_objtree("USING");
	if (node->into->accessMethod)
		append_string_object(tmp, "%{access_method}I", "access_method",
							 node->into->accessMethod);
	else
	{
		append_null_object(tmp, "%{access_method}I");
		append_not_present(tmp);
	}
	append_object_object(ret, "%{access_method}s", tmp);

	/* WITH clause */
	tmp = new_objtree("WITH");
	list = NIL;

	foreach(cell, node->into->options)
	{
		DefElem    *opt = (DefElem *) lfirst(cell);

		tmp2 = deparse_DefElem(opt, false);
		list = lappend(list, new_object_object(tmp2));
	}

	if (list)
		append_array_object(tmp, "(%{with:, }s)", list);
	else
		append_not_present(tmp);

	append_object_object(ret, "%{with_clause}s", tmp);

	/* ON COMMIT clause.  CREATE MATERIALIZED VIEW doesn't have one */
	if (node->objtype == OBJECT_TABLE)
		append_object_object(ret, "%{on_commit}s",
							 deparse_OnCommitClause(node->into->onCommit));

	/* TABLESPACE clause */
	tmp = new_objtree("TABLESPACE %{tablespace}I");
	if (node->into->tableSpaceName)
		append_string_object(tmp, "%{tablespace}s", "tablespace",
							 node->into->tableSpaceName);
	else
	{
		append_null_object(tmp, "%{tablespace}I");
		append_not_present(tmp);
	}
	append_object_object(ret, "%{tablespace}s", tmp);

	/* Add the query */
	Assert(IsA(node->query, Query));
	append_string_object(ret, "AS %{query}s", "query",
						 pg_get_querydef((Query *) node->query, false));

	/* Add a WITH NO DATA clause */
	tmp = new_objtree_VA("WITH NO DATA", 1,
						 "present", ObjTypeBool,
						 node->into->skipData ? true : false);
	append_object_object(ret, "%{with_no_data}s", tmp);

	relation_close(relation, AccessShareLock);

	return ret;
}

/*
 * Deparse a CreateTrigStmt (CREATE TRIGGER)
 *
 * Given a trigger OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{constraint}s TRIGGER %{name}I %{time}s %{events: OR }s ON
 * %{relation}D %{from_table}s %{constraint_attrs: }s FOR EACH
 * %{for_each}s %{when}s EXECUTE PROCEDURE %{function}s
 */
static ObjTree *
deparse_CreateTrigStmt(Oid objectId, Node *parsetree)
{
	CreateTrigStmt *node = (CreateTrigStmt *) parsetree;
	Relation	pg_trigger;
	HeapTuple	trigTup;
	Form_pg_trigger trigForm;
	ObjTree    *ret;
	ObjTree    *tmp_obj;
	int			tgnargs;
	List	   *list = NIL;
	List	   *events;
	char	   *trigtiming;

	pg_trigger = table_open(TriggerRelationId, AccessShareLock);

	trigTup = get_catalog_object_by_oid(pg_trigger, Anum_pg_trigger_oid, objectId);
	trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

	trigtiming = node->timing == TRIGGER_TYPE_BEFORE ? "BEFORE" :
		node->timing == TRIGGER_TYPE_AFTER ? "AFTER" :
		node->timing == TRIGGER_TYPE_INSTEAD ? "INSTEAD OF" :
		NULL;
	if (!trigtiming)
		elog(ERROR, "unrecognized trigger timing type %d", node->timing);

	ret = new_objtree_VA("CREATE %{constraint}s TRIGGER %{name}I %{time}s", 3,
						 "constraint", ObjTypeString, node->isconstraint ? "CONSTRAINT" : "",
						 "name", ObjTypeString, node->trigname,
						 "time", ObjTypeString, trigtiming);

	/*
	 * Decode the events that the trigger fires for.  The output is a list; in
	 * most cases it will just be a string with the event name, but when
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
			ObjTree    *update;
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
				char	   *colname = strVal(lfirst(cell));

				cols = lappend(cols, new_string_object(colname));
			}

			append_array_object(update, "%{columns:, }I", cols);

			events = lappend(events, new_object_object(update));
		}
	}
	append_array_object(ret, "%{events: OR }s", events);

	tmp_obj = new_objtree_for_qualname_id(RelationRelationId,
										 trigForm->tgrelid);
	append_object_object(ret, "ON %{relation}D", tmp_obj);

	tmp_obj = new_objtree("FROM");
	if (trigForm->tgconstrrelid)
	{
		ObjTree    *rel;

		rel = new_objtree_for_qualname_id(RelationRelationId,
										  trigForm->tgconstrrelid);
		append_object_object(tmp_obj, "%{relation}D", rel);
	}
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{from_table}s", tmp_obj);

	if (node->deferrable)
		list = lappend(list, new_string_object("DEFERRABLE"));
	if (node->initdeferred)
		list = lappend(list, new_string_object("INITIALLY DEFERRED"));
	append_array_object(ret, "%{constraint_attrs: }s", list);

	append_string_object(ret, "FOR EACH %{for_each}s", "for_each",
						 node->row ? "ROW" : "STATEMENT");

	tmp_obj = new_objtree("WHEN");
	if (node->whenClause)
	{
		Node	   *whenClause;
		Datum		value;
		bool		isnull;

		value = fastgetattr(trigTup, Anum_pg_trigger_tgqual,
							RelationGetDescr(pg_trigger), &isnull);
		if (isnull)
			elog(ERROR, "null tgqual for trigger \"%s\"",
				 NameStr(trigForm->tgname));

		whenClause = stringToNode(TextDatumGetCString(value));
		append_string_object(tmp_obj, "(%{clause}s)", "clause",
							 pg_get_trigger_whenclause(trigForm,
													   whenClause,
													   false));
	}
	else
		append_not_present(tmp_obj);
	append_object_object(ret, "%{when}s", tmp_obj);

	tmp_obj = new_objtree_VA("%{funcname}D", 1,
							"funcname", ObjTypeObject,
							new_objtree_for_qualname_id(ProcedureRelationId,
														trigForm->tgfoid));
	list = NIL;
	tgnargs = trigForm->tgnargs;
	if (tgnargs > 0)
	{
		bytea	   *tgargs;
		char	   *argstr;
		bool		isnull;
		int			findx;
		int			lentgargs;
		char	   *p;

		tgargs = DatumGetByteaP(fastgetattr(trigTup,
											Anum_pg_trigger_tgargs,
											RelationGetDescr(pg_trigger),
											&isnull));
		if (isnull)
			elog(ERROR, "null tgargs for trigger \"%s\"",
				 NameStr(trigForm->tgname));
		argstr = (char *) VARDATA(tgargs);
		lentgargs = VARSIZE_ANY_EXHDR(tgargs);

		p = argstr;
		for (findx = 0; findx < tgnargs; findx++)
		{
			size_t		tlen;

			/* Verify that the argument encoding is correct */
			tlen = strlen(p);
			if (p + tlen >= argstr + lentgargs)
				elog(ERROR, "invalid argument string (%s) for trigger \"%s\"",
					 argstr, NameStr(trigForm->tgname));

			list = lappend(list, new_string_object(p));

			p += tlen + 1;
		}
	}

	append_format_string(tmp_obj, "(");
	append_array_object(tmp_obj, "%{args:, }L", list);	/* might be NIL */
	append_format_string(tmp_obj, ")");

	append_object_object(ret, "EXECUTE PROCEDURE %{function}s", tmp_obj);

	table_close(pg_trigger, AccessShareLock);

	return ret;
}

/*
 * Deparse a CreateUserMappingStmt (CREATE USER MAPPING)
 *
 * Given a User Mapping OID and the parse tree that created it,
 * return an ObjTree representing the CREATE USER MAPPING command.
 *
 * Verbose syntax
 * CREATE USER MAPPING FOR %{role}R SERVER %{server}I
 */
static ObjTree *
deparse_CreateUserMappingStmt(Oid objectId, Node *parsetree)
{
	CreateUserMappingStmt *node = (CreateUserMappingStmt *) parsetree;
	ObjTree    *ret;
	Relation	rel;
	HeapTuple	tp;
	Form_pg_user_mapping form;
	ForeignServer *server;

	rel = table_open(UserMappingRelationId, RowExclusiveLock);

	/*
	 * Lookup object in the catalog, so we don't have to deal with
	 * current_user and such.
	 */
	tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for user mapping with OID %u",
			 objectId);

	form = (Form_pg_user_mapping) GETSTRUCT(tp);

	server = GetForeignServer(form->umserver);

	ret = new_objtree_VA("CREATE USER MAPPING FOR %{role}R SERVER %{server}I %{generic_options}s", 3,
						 "role", ObjTypeObject,
						 new_objtree_for_role_id(form->umuser),
						 "server", ObjTypeString, server->servername,
						 "generic_options", ObjTypeObject,
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(tp);
	table_close(rel, RowExclusiveLock);
	return ret;
}

/*
 * deparse_AlterUserMapping
 *
 * Given a User Mapping OID and the parse tree that created it, return an
 * ObjTree representing the alter command.
 *
 * Verbose syntax
 * ALTER USER MAPPING FOR %{role}R SERVER %{server}I
 */
static ObjTree *
deparse_AlterUserMappingStmt(Oid objectId, Node *parsetree)
{
	AlterUserMappingStmt *node = (AlterUserMappingStmt *) parsetree;
	ObjTree    *ret;
	Relation	rel;
	HeapTuple	tp;
	Form_pg_user_mapping form;
	ForeignServer *server;

	rel = table_open(UserMappingRelationId, RowExclusiveLock);

	/*
	 * Lookup object in the catalog, so we don't have to deal with
	 * current_user and such.
	 */
	tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for user mapping with OID %u",
			 objectId);

	form = (Form_pg_user_mapping) GETSTRUCT(tp);

	server = GetForeignServer(form->umserver);

	ret = new_objtree_VA("ALTER USER MAPPING FOR %{role}R SERVER %{server}I %{generic_options}s", 3,
						 "role", ObjTypeObject,
						 new_objtree_for_role_id(form->umuser),
						 "server", ObjTypeString, server->servername,
						 "generic_options", ObjTypeObject,
						 deparse_FdwOptions(node->options, NULL));

	ReleaseSysCache(tp);
	table_close(rel, RowExclusiveLock);
	return ret;
}

/*
 * Deparse an AlterStatsStmt (ALTER STATISTICS)
 *
 * Given a alter statistics OID and the parse tree that created it, return an
 * ObjTree representing the alter command.
 *
 * Verbose syntax
 * ALTER STATISTICS %{identity}D SET STATISTICS %{target}n
 */
static ObjTree *
deparse_AlterStatsStmt(Oid objectId, Node *parsetree)
{
	AlterStatsStmt *node = (AlterStatsStmt *) parsetree;
	ObjTree    *ret;
	HeapTuple	tp;
	Form_pg_statistic_ext statform;

	if (!node->stxstattarget)
		return NULL;

	/* Lookup object in the catalog */
	tp = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for statistic with OID %u", objectId);

	statform = (Form_pg_statistic_ext) GETSTRUCT(tp);
	ret = new_objtree_VA("ALTER STATISTICS %{identity}D SET STATISTICS %{target}n", 2,
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname(statform->stxnamespace,
												  NameStr(statform->stxname)),
						 "target", ObjTypeInteger, node->stxstattarget);

	ReleaseSysCache(tp);
	return ret;
}

/*
 * Deparse a RefreshMatViewStmt (REFRESH MATERIALIZED VIEW)
 *
 * Given a materialized view OID and the parse tree that created it, return an
 * ObjTree representing the refresh command.
 *
 * Verbose syntax
 * REFRESH MATERIALIZED VIEW %{concurrently}s %{identity}D %{with_no_data}s
 */
static ObjTree *
deparse_RefreshMatViewStmt(Oid objectId, Node *parsetree)
{
	RefreshMatViewStmt *node = (RefreshMatViewStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *tmp;

	ret = new_objtree_VA("REFRESH MATERIALIZED VIEW %{concurrently}s %{identity}D", 2,
						 "concurrently", ObjTypeString,
						 node->concurrent ? "CONCURRENTLY" : "",
						 "identity", ObjTypeObject,
						 new_objtree_for_qualname_id(RelationRelationId,
													 objectId));

	/* Add a WITH NO DATA clause */
	tmp = new_objtree_VA("WITH NO DATA", 1,
						 "present", ObjTypeBool,
						 node->skipData ? true : false);
	append_object_object(ret, "%{with_no_data}s", tmp);

	return ret;
}

/*
 * Deparse a RenameStmt.
 *
 * Verbose syntax
 * ALTER %s %{if_exists}s %{identity}D RENAME TO %{newname}I
 * OR
 * ALTER POLICY %{if_exists}s %{policyname}I ON %{identity}D RENAME TO %{newname}I
 * OR
 * ALTER objtype %{identity}D RENAME ATTRIBUTE %{colname}I TO %{newname}I %{cascade}s
 * OR
 * ALTER objtype %{if_exists}s %%{identity}D RENAME COLUMN %{colname}I TO %{newname}I
 * OR
 * ALTER %s %%{identity}s RENAME TO %%{newname}I
 * OR
 * ALTER %s %%{identity}D USING %%{index_method}s RENAME TO %%{newname}I
 * OR
 * ALTER SCHEMA %{identity}I RENAME TO %{newname}I
 * OR
 * ALTER RULE %{rulename}I ON %{identity}D RENAME TO %{newname}I
 * OR
 * ALTER TRIGGER %{triggername}I ON %{identity}D RENAME TO %{newname}I
 * OR
 * ALTER %s %%{identity}D RENAME TO %%{newname}I
 */
static ObjTree *
deparse_RenameStmt(ObjectAddress address, Node *parsetree)
{
	RenameStmt *node = (RenameStmt *) parsetree;
	ObjTree    *ret;
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
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);
			ret = new_objtree_VA("ALTER %{objtype}s %{if_exists}s %{identity}D RENAME TO %{newname}I", 4,
								 "objtype", ObjTypeString,
								 stringify_objtype(node->renameType, false),
								 "if_exists", ObjTypeString,
								 node->missing_ok ? "IF EXISTS" : "",
								 "identity", ObjTypeObject,
								 new_objtree_for_qualname(schemaId,
														  node->relation->relname),
								 "newname", ObjTypeString,
								 node->newname);
			relation_close(relation, AccessShareLock);
			break;

		case OBJECT_POLICY:
			{
				HeapTuple	polTup;
				Form_pg_policy polForm;
				Relation	pg_policy;
				ScanKeyData key;
				SysScanDesc scan;

				pg_policy = relation_open(PolicyRelationId, AccessShareLock);
				ScanKeyInit(&key, Anum_pg_policy_oid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(address.objectId));
				scan = systable_beginscan(pg_policy, PolicyOidIndexId, true,
										  NULL, 1, &key);
				polTup = systable_getnext(scan);
				if (!HeapTupleIsValid(polTup))
					elog(ERROR, "cache lookup failed for policy with OID %u",
						 address.objectId);
				polForm = (Form_pg_policy) GETSTRUCT(polTup);

				ret = new_objtree_VA("ALTER POLICY %{if_exists}s %{policyname}I ON %{identity}D RENAME TO %{newname}I", 4,
									 "if_exists", ObjTypeString,
									 node->missing_ok ? "IF EXISTS" : "",
									 "policyname", ObjTypeString,
									 node->subname,
									 "identity", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 polForm->polrelid),
									 "newname", ObjTypeString,
									 node->newname);
				systable_endscan(scan);
				relation_close(pg_policy, AccessShareLock);
			}
			break;

		case OBJECT_ATTRIBUTE:
		case OBJECT_COLUMN:
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);

			if (node->renameType == OBJECT_ATTRIBUTE)
				ret = new_objtree_VA("ALTER TYPE %{identity}D RENAME ATTRIBUTE %{colname}I", 2,
									 "identity", ObjTypeObject,
									 new_objtree_for_qualname(schemaId,
															  node->relation->relname),
									 "colname", ObjTypeString, node->subname);
			else
			{
				ret = new_objtree_VA("ALTER %{objtype}s", 1,
									 "objtype", ObjTypeString,
									 stringify_objtype(node->relationType, false));

				/* Composite types do not support IF EXISTS */
				if (node->renameType == OBJECT_COLUMN)
					append_string_object(ret, "%{if_exists}s",
										 "if_exists",
										 node->missing_ok ? "IF EXISTS" : "");

				append_object_object(ret, "%{identity}D",
									 new_objtree_for_qualname(schemaId,
															  node->relation->relname));
				append_string_object(ret, "RENAME COLUMN %{colname}I",
									 "colname", node->subname);
			}

			append_string_object(ret, "TO %{newname}I", "newname",
								 node->newname);

			if (node->renameType == OBJECT_ATTRIBUTE)
				append_object_object(ret, "%{cascade}s",
									 new_objtree_VA("CASCADE", 1,
													"present", ObjTypeBool,
													node->behavior == DROP_CASCADE));

			relation_close(relation, AccessShareLock);
			break;

		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		case OBJECT_ROUTINE:
			{
				char	   *ident;
				HeapTuple	proctup;
				Form_pg_proc procform;
				List	   *identity;

				Assert(IsA(node->object, ObjectWithArgs));
				identity = ((ObjectWithArgs *) node->object)->objname;

				proctup = SearchSysCache1(PROCOID,
										  ObjectIdGetDatum(address.objectId));
				if (!HeapTupleIsValid(proctup))
					elog(ERROR, "cache lookup failed for procedure with OID %u",
						 address.objectId);
				procform = (Form_pg_proc) GETSTRUCT(proctup);

				/* XXX does this work for ordered-set aggregates? */
				ident = psprintf("%s%s",
								 quote_qualified_identifier(get_namespace_name(procform->pronamespace),
															strVal(llast(identity))),
								 format_procedure_args(address.objectId, true));

				ret = new_objtree_VA("ALTER %{objtype}s %{identity}s RENAME TO %{newname}I", 3,
									 "objtype", ObjTypeString,
									 stringify_objtype(node->renameType, false),
									 "identity", ObjTypeString, ident,
									 "newname", ObjTypeString, node->newname);

				ReleaseSysCache(proctup);
			}
			break;

		case OBJECT_OPCLASS:
			{
				HeapTuple	opcTup;
				Form_pg_opclass opcForm;
				List	   *oldnames;
				char	   *schemaname;
				char	   *opcname;

				opcTup = SearchSysCache1(CLAOID, ObjectIdGetDatum(address.objectId));
				if (!HeapTupleIsValid(opcTup))
					elog(ERROR, "cache lookup failed for opclass with OID %u",
						 address.objectId);

				opcForm = (Form_pg_opclass) GETSTRUCT(opcTup);

				oldnames = list_copy_tail((List *) node->object, 1);

				/* Deconstruct the name list */
				DeconstructQualifiedName(oldnames, &schemaname, &opcname);

				ret = new_objtree_VA("ALTER %{objtype}s %{identity}D USING %{index_method}s RENAME TO %{newname}I", 4,
									 "objtype", ObjTypeString,
									 stringify_objtype(node->renameType, false),
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
				HeapTuple	opfTup;
				HeapTuple	amTup;
				Form_pg_opfamily opfForm;
				Form_pg_am	amForm;
				List	   *oldnames;
				char	   *schemaname;
				char	   *opfname;

				opfTup = SearchSysCache1(OPFAMILYOID, address.objectId);
				if (!HeapTupleIsValid(opfTup))
					elog(ERROR, "cache lookup failed for operator family with OID %u",
						 address.objectId);
				opfForm = (Form_pg_opfamily) GETSTRUCT(opfTup);

				amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(opfForm->opfmethod));
				if (!HeapTupleIsValid(amTup))
					elog(ERROR, "cache lookup failed for access method with OID %u",
						 opfForm->opfmethod);

				amForm = (Form_pg_am) GETSTRUCT(amTup);

				oldnames = list_copy_tail((List *) node->object, 1);

				/* Deconstruct the name list */
				DeconstructQualifiedName(oldnames, &schemaname, &opfname);

				ret = new_objtree_VA("ALTER %{objtype}s %{identity}D USING %{index_method}s RENAME TO %{newname}I", 4,
									 "objtype", ObjTypeString,
									 stringify_objtype(node->renameType, false),
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
			ret = new_objtree_VA("ALTER SCHEMA %{identity}I RENAME TO %{newname}I", 2,
								 "identity", ObjTypeString, node->subname,
								 "newname", ObjTypeString, node->newname);
			break;

		case OBJECT_FDW:
		case OBJECT_LANGUAGE:
		case OBJECT_FOREIGN_SERVER:
		case OBJECT_PUBLICATION:
			ret = new_objtree_VA("ALTER %{objtype}s %{identity}s RENAME TO %{newname}I", 3,
								 "objtype", ObjTypeString,
								 stringify_objtype(node->renameType, false),
								 "identity", ObjTypeString,
								 strVal(castNode(String, node->object)),
								 "newname", ObjTypeString, node->newname);
			break;

		case OBJECT_RULE:
			{
				HeapTuple	rewrTup;
				Form_pg_rewrite rewrForm;
				Relation	pg_rewrite;

				pg_rewrite = relation_open(RewriteRelationId, AccessShareLock);
				rewrTup = get_catalog_object_by_oid(pg_rewrite, Anum_pg_rewrite_oid, address.objectId);
				rewrForm = (Form_pg_rewrite) GETSTRUCT(rewrTup);

				ret = new_objtree_VA("ALTER RULE %{rulename}I ON %{identity}D RENAME TO %{newname}I", 3,
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

				ret = new_objtree_VA("ALTER TRIGGER %{triggername}I ON %{identity}D RENAME TO %{newname}I", 3,
									 "triggername", ObjTypeString, node->subname,
									 "identity", ObjTypeObject,
									 new_objtree_for_qualname_id(RelationRelationId,
																 trigForm->tgrelid),
									 "newname", ObjTypeString, node->newname);

				relation_close(pg_trigger, AccessShareLock);
			}
			break;

		case OBJECT_COLLATION:
		case OBJECT_STATISTIC_EXT:
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

				/* Obtain object tuple */
				catalog = relation_open(address.classId, AccessShareLock);
				objTup = get_catalog_object_by_oid(catalog, get_object_attnum_oid(address.classId), address.objectId);

				/* Obtain namespace */
				Anum_namespace = get_object_attnum_namespace(address.classId);
				objnsp = heap_getattr(objTup, Anum_namespace,
									  RelationGetDescr(catalog), &isnull);
				if (isnull)
					elog(ERROR, "invalid NULL namespace");

				ret = new_objtree_VA("ALTER %{objtype}s %{identity}D RENAME TO %{newname}I", 3,
									 "objtype", ObjTypeString,
									 stringify_objtype(node->renameType, false),
									 "identity", ObjTypeObject,
									 new_objtree_for_qualname(DatumGetObjectId(objnsp),
															  strVal(llast(identity))),
									 "newname", ObjTypeString, node->newname);
				relation_close(catalog, AccessShareLock);
			}
			break;

		default:
			elog(ERROR, "unsupported object type %d", node->renameType);
	}

	return ret;
}

/*
 * Deparse a AlterObjectDependsStmt (ALTER ... DEPENDS ON ...).
 *
 * Verbose syntax
 * ALTER INDEX %{identity}D %{no}s DEPENDS ON EXTENSION %{newname}I
 */
static ObjTree *
deparse_AlterDependStmt(Oid objectId, Node *parsetree)
{
	AlterObjectDependsStmt *node = (AlterObjectDependsStmt *) parsetree;
	ObjTree    *ret = NULL;

	if (node->objectType == OBJECT_INDEX)
	{
		ObjTree    *qualified;
		Relation	relation = relation_open(objectId, AccessShareLock);

		qualified = new_objtree_for_qualname(relation->rd_rel->relnamespace,
											 node->relation->relname);
		relation_close(relation, AccessShareLock);

		ret = new_objtree_VA("ALTER INDEX %{identity}D %{no}s DEPENDS ON EXTENSION %{newname}I", 3,
							 "identity", ObjTypeObject, qualified,
							 "no", ObjTypeString,
							 node->remove ? "NO" : "",
							 "newname", ObjTypeString, strVal(node->extname));
	}
	else
		elog(LOG, "unrecognized node type in deparse command: %d",
			 (int) nodeTag(parsetree));

	return ret;
}

/*
 * Helper for deparse_CreatePublicationStmt and deparse_AlterPublicationStmt.
 */
static void
deparse_PublicationObjects(List *rels, List *schemas,
						   List **pubrels, List **pubschemas)
{
	ListCell   *lc1;
	ListCell   *lc2;

	*pubrels = NIL;
	*pubschemas = NIL;

	foreach(lc1, rels)
	{
		ObjTree    *tmp_rel;
		List	   *collist = NIL;
		Relation	relation;

		publication_rel *pub_rel = (publication_rel *) lfirst(lc1);

		relation = relation_open(pub_rel->relid, AccessShareLock);

		tmp_rel = new_objtree_VA("%{tablename}D", 1,
								 "tablename", ObjTypeObject,
								 new_objtree_for_qualname(relation->rd_rel->relnamespace,
														  RelationGetRelationName(relation)));

		if (pub_rel->columnList)
		{
			foreach(lc2, pub_rel->columnList)
				collist = lappend(collist,
								  new_string_object(strVal(lfirst(lc2))));

			append_array_object(tmp_rel, "(%{cols:, }s)", collist);
		}

		if (pub_rel->whereClause)
		{
			List	   *context;
			char	   *whereClause;

			context = deparse_context_for(RelationGetRelationName(relation),
										  pub_rel->relid);

			/* Deparse the expression */
			whereClause =
				deparse_expression(pub_rel->whereClause, context, false, false);

			append_string_object(tmp_rel, "WHERE %{where}s", "where", whereClause);
		}

		relation_close(relation, AccessShareLock);

		*pubrels = lappend(*pubrels, new_object_object(tmp_rel));
	}

	foreach(lc1, schemas)
		* pubschemas = lappend(*pubschemas,
							   new_string_object(get_namespace_name(lfirst_oid(lc1))));
}

/*
 * Deparse a CREATE PUBLICATION statement.
 *
 * Verbose syntax
 * CREATE PUBLICATION %{identity}s FOR ALL TABLES WITH (%{with:, }s)
 *	OR
 * CREATE PUBLICATION %{identity}s FOR TABLE %{tables:, }s WITH (%{with:, }s)
 *	OR
 * CREATE PUBLICATION %{identity}s FOR TABLES IN SCHEMA %{schemas:, }s
 * WITH (%{with:, }s)
 *	OR
 * CREATE PUBLICATION %{identity}s FOR TABLE %{tables:, }s,
 * TABLES IN SCHEMA %{schemas:, }s WITH (%{with:, }s)
 */
static ObjTree *
deparse_CreatePublicationStmt(CollectedCommand *cmd)
{
	Oid			objectId = cmd->d.createpub.address.objectId;
	CreatePublicationStmt *node = (CreatePublicationStmt *) cmd->parsetree;
	ObjTree    *createPub;
	HeapTuple	pubtup;
	Form_pg_publication pubform;
	List	   *list = NIL;
	ListCell   *cell;
	List	   *rellist = NIL;
	List	   *schlist = NIL;

	/* Don't deparse sql commands generated while creating extension */
	if (cmd->in_extension)
		return NULL;

	/* Find the pg_publication entry and open the corresponding relation */
	pubtup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(pubtup))
		elog(ERROR, "cache lookup failed for publication with OID %u",
			 objectId);

	pubform = (Form_pg_publication) GETSTRUCT(pubtup);
	pubform->oid = pubform->oid;

	createPub = new_objtree_VA("CREATE PUBLICATION %{identity}s", 1,
							   "identity", ObjTypeString,
							   NameStr(pubform->pubname));

	/* FOR ALL TABLES publication */
	if (pubform->puballtables)
		append_format_string(createPub, "FOR ALL TABLES");
	else
	{
		deparse_PublicationObjects(cmd->d.createpub.rels,
								   cmd->d.createpub.schemas,
								   &rellist, &schlist);

		/* Append the publication tables */
		if (rellist)
			append_array_object(createPub, "FOR TABLE %{tables:, }s", rellist);

		/* Append the publication schemas */
		if (schlist)
		{
			if (rellist)
				append_format_string(createPub, ", TABLES IN SCHEMA");
			else
				append_format_string(createPub, "FOR TABLES IN SCHEMA");

			append_array_object(createPub, "%{schemas:, }s", schlist);
		}
	}

	/* WITH clause */
	foreach(cell, node->options)
	{
		ObjTree    *tmp_obj;
		DefElem    *opt = (DefElem *) lfirst(cell);

		tmp_obj = deparse_DefElem(opt, false);
		list = lappend(list, new_object_object(tmp_obj));
	}

	append_array_object(createPub, "WITH (%{with:, }s)", list);

	ReleaseSysCache(pubtup);
	return createPub;
}

/*
 * Deparse a ALTER PUBLICATION statement.
 *
 * Verbose syntax
 * ALTER PUBLICATION %{identity}s ADD|DROP|SET TABLE %{tables:, }s
 * 	OR
 * ALTER PUBLICATION %{identity}s ADD|DROP|SET TABLES IN SCHEMA %{schemas:, }s
 *	OR
 * ALTER PUBLICATION %{identity}s ADD|DROP|SET TABLE %{tables:, }s,
 * TABLES IN SCHEMA %{schemas:, }s
 */
static ObjTree *
deparse_AlterPublicationStmt(CollectedCommand *cmd)
{
	Oid			objectId = cmd->d.createpub.address.objectId;
	AlterPublicationStmt *node = (AlterPublicationStmt *) cmd->parsetree;
	ObjTree    *alterpub;
	HeapTuple	pubtup;
	Form_pg_publication pubform;
	ListCell   *cell;
	List	   *rellist = NIL;
	List	   *schlist = NIL;

	/* Don't deparse sql commands generated while creating extension */
	if (cmd->in_extension)
		return NULL;

	/* Find the pg_publication entry and open the corresponding relation */
	pubtup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(pubtup))
		elog(ERROR, "cache lookup failed for publication with OID %u",
			 objectId);
	pubform = (Form_pg_publication) GETSTRUCT(pubtup);

	pubform->oid = pubform->oid;

	alterpub = new_objtree_VA("ALTER PUBLICATION %{identity}s", 1,
							  "identity", ObjTypeString,
							  NameStr(pubform->pubname));

	if (node->options)
	{
		/* SET option */
		List	   *list = NIL;

		foreach(cell, node->options)
		{
			ObjTree    *tmp_obj;
			DefElem    *opt = (DefElem *) lfirst(cell);

			tmp_obj = deparse_DefElem(opt, false);
			list = lappend(list, new_object_object(tmp_obj));
		}

		/* Append the publication options */
		append_array_object(alterpub, "SET (%{options:, }s)", list);

		ReleaseSysCache(pubtup);
		return alterpub;
	}

	if (node->action == AP_AddObjects)
		append_format_string(alterpub, "ADD");
	else if (node->action == AP_DropObjects)
		append_format_string(alterpub, "DROP");
	else
		append_format_string(alterpub, "SET");

	deparse_PublicationObjects(cmd->d.alterpub.rels, cmd->d.alterpub.schemas,
							   &rellist, &schlist);

	/* Append the publication tables */
	if (rellist)
		append_array_object(alterpub, "TABLE %{tables:, }s", rellist);

	if (schlist)
	{
		if (cmd->d.createpub.rels)
			append_format_string(alterpub, ", TABLES IN SCHEMA");
		else
			append_format_string(alterpub, "TABLES IN SCHEMA");

		/* Append the publication schemas */
		append_array_object(alterpub, "%{schemas:, }s", schlist);
	}

	ReleaseSysCache(pubtup);
	return alterpub;
}

/*
 * Deparse a RuleStmt (CREATE RULE).
 *
 * Given a rule OID and the parse tree that created it, return an ObjTree
 * representing the rule command.
 *
 * Verbose syntax
 * CREATE RULE %{or_replace}s %{identity}I AS ON %{event}s TO %{table}D
 * %{where_clause}s DO %{instead}s %{actions:, }s
 */
static ObjTree *
deparse_RuleStmt(Oid objectId, Node *parsetree)
{
	RuleStmt   *node = (RuleStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *tmp;
	Relation	pg_rewrite;
	Form_pg_rewrite rewrForm;
	HeapTuple	rewrTup;
	SysScanDesc scan;
	ScanKeyData key;
	Datum		ev_qual;
	Datum		ev_actions;
	bool		isnull;
	char	   *qual;
	List	   *actions;
	List	   *list = NIL;
	ListCell   *cell;
	char	   *event_str;

	pg_rewrite = table_open(RewriteRelationId, AccessShareLock);
	ScanKeyInit(&key,
				Anum_pg_rewrite_oid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(objectId));

	scan = systable_beginscan(pg_rewrite, RewriteOidIndexId, true,
							  NULL, 1, &key);
	rewrTup = systable_getnext(scan);
	if (!HeapTupleIsValid(rewrTup))
		elog(ERROR, "cache lookup failed for rewrite rule with OID %u",
			 objectId);

	rewrForm = (Form_pg_rewrite) GETSTRUCT(rewrTup);

	event_str = node->event == CMD_SELECT ? "SELECT" :
		node->event == CMD_UPDATE ? "UPDATE" :
		node->event == CMD_DELETE ? "DELETE" :
		node->event == CMD_INSERT ? "INSERT" : NULL;
	Assert(event_str != NULL);

	ev_qual = heap_getattr(rewrTup, Anum_pg_rewrite_ev_qual,
						   RelationGetDescr(pg_rewrite), &isnull);
	ev_actions = heap_getattr(rewrTup, Anum_pg_rewrite_ev_action,
							  RelationGetDescr(pg_rewrite), &isnull);

	pg_get_ruledef_detailed(ev_qual, ev_actions, &qual, &actions);

	tmp = new_objtree("");
	if (qual)
		append_string_object(tmp, "WHERE %{clause}s", "clause", qual);
	else
	{
		append_null_object(tmp, "WHERE %{clause}s");
		append_not_present(tmp);
	}

	if (actions == NIL)
		list = lappend(list, new_string_object("NOTHING"));
	else
	{
		foreach(cell, actions)
			list = lappend(list, new_string_object(lfirst(cell)));
	}

	ret = new_objtree_VA("CREATE RULE %{or_replace}s %{identity}I AS ON %{event}s TO %{table}D %{where_clause}s DO %{instead}s %{actions:, }s", 7,
						 "or_replace", ObjTypeString,
						 node->replace ? "OR REPLACE" : "",
						 "identity", ObjTypeString, node->rulename,
						 "event", ObjTypeString, event_str,
						 "table", ObjTypeObject,
						 new_objtree_for_qualname_id(RelationRelationId,
													 rewrForm->ev_class),
						 "where_clause", ObjTypeObject, tmp,
						 "instead", ObjTypeString,
						 node->instead ? "INSTEAD" : "ALSO",
						 "actions", ObjTypeArray, list);

	systable_endscan(scan);
	table_close(pg_rewrite, AccessShareLock);

	return ret;
}

/*
 * Deparse a CreateTransformStmt (CREATE TRANSFORM).
 *
 * Given a transform OID and the parse tree that created it, return an ObjTree
 * representing the CREATE TRANSFORM command.
 *
 * Verbose syntax
 * CREATE %{or_replace}s TRANSFORM FOR %{typename}D LANGUAGE %{language}I
 * ( FROM SQL WITH FUNCTION %{signature}s, TO SQL WITH FUNCTION
 * %{signature_tof}s )
 */
static ObjTree *
deparse_CreateTransformStmt(Oid objectId, Node *parsetree)
{
	CreateTransformStmt *node = (CreateTransformStmt *) parsetree;
	ObjTree    *ret;
	ObjTree    *signature;
	HeapTuple	trfTup;
	HeapTuple	langTup;
	HeapTuple	procTup;
	Form_pg_transform trfForm;
	Form_pg_language langForm;
	Form_pg_proc procForm;
	int			i;

	/* Get the pg_transform tuple */
	trfTup = SearchSysCache1(TRFOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(trfTup))
		elog(ERROR, "cache lookup failed for transform with OID %u",
			 objectId);
	trfForm = (Form_pg_transform) GETSTRUCT(trfTup);

	/* Get the corresponding pg_language tuple */
	langTup = SearchSysCache1(LANGOID, trfForm->trflang);
	if (!HeapTupleIsValid(langTup))
		elog(ERROR, "cache lookup failed for language with OID %u",
			 trfForm->trflang);
	langForm = (Form_pg_language) GETSTRUCT(langTup);

	ret = new_objtree_VA("CREATE %{or_replace}s TRANSFORM FOR %{typename}D LANGUAGE %{language}I", 3,
						 "or_replace", ObjTypeString,
						 node->replace ? "OR REPLACE" : "",
						 "typename", ObjTypeObject,
						 new_objtree_for_qualname_id(TypeRelationId,
													 trfForm->trftype),
						 "language", ObjTypeString,
						 NameStr(langForm->lanname));

	/* Deparse the transform_element_list */
	if (OidIsValid(trfForm->trffromsql))
	{
		List	   *params = NIL;

		/* Get the pg_proc tuple for the FROM FUNCTION */
		procTup = SearchSysCache1(PROCOID, trfForm->trffromsql);
		if (!HeapTupleIsValid(procTup))
			elog(ERROR, "cache lookup failed for function with OID %u",
				 trfForm->trffromsql);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);

		/*
		 * CREATE TRANSFORM does not change function signature so we can use
		 * catalog to get input type Oids.
		 */
		for (i = 0; i < procForm->pronargs; i++)
		{
			ObjTree    *param_obj;

			param_obj = new_objtree_VA("%{type}T", 1,
									  "type", ObjTypeObject,
								 new_objtree_for_type(procForm->proargtypes.values[i], -1));
			params = lappend(params, new_object_object(param_obj));
		}

		signature = new_objtree_VA("%{identity}D (%{arguments:, }s)", 2,
								   "identity", ObjTypeObject,
								   new_objtree_for_qualname(procForm->pronamespace,
															NameStr(procForm->proname)),
								   "arguments", ObjTypeArray, params);

		append_object_object(ret, "(FROM SQL WITH FUNCTION %{signature}s",
							 signature);
		ReleaseSysCache(procTup);
	}
	if (OidIsValid(trfForm->trftosql))
	{
		List	   *params = NIL;

		/* Append a ',' if trffromsql is present, else append '(' */
		append_format_string(ret, OidIsValid(trfForm->trffromsql) ? "," : "(");

		/* Get the pg_proc tuple for the TO FUNCTION */
		procTup = SearchSysCache1(PROCOID, trfForm->trftosql);
		if (!HeapTupleIsValid(procTup))
			elog(ERROR, "cache lookup failed for function with OID %u",
				 trfForm->trftosql);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);

		/*
		 * CREATE TRANSFORM does not change function signature so we can use
		 * catalog to get input type Oids.
		 */
		for (i = 0; i < procForm->pronargs; i++)
		{
			ObjTree    *param_obj = new_objtree("");

			param_obj = new_objtree_VA("%{type}T", 1,
									  "type", ObjTypeObject,
									  new_objtree_for_type(procForm->proargtypes.values[i], -1));
			params = lappend(params, new_object_object(param_obj));
		}

		signature = new_objtree_VA("%{identity}D (%{arguments:, }s)", 2,
								   "identity", ObjTypeObject,
								   new_objtree_for_qualname(procForm->pronamespace,
															NameStr(procForm->proname)),
								   "arguments", ObjTypeArray, params);

		append_object_object(ret, "TO SQL WITH FUNCTION %{signature_tof}s",
							 signature);
		ReleaseSysCache(procTup);
	}

	append_format_string(ret, ")");

	ReleaseSysCache(langTup);
	ReleaseSysCache(trfTup);
	return ret;
}

/*
 * Deparse a CommentStmt when it pertains to a constraint.
 *
 * Verbose syntax
 * COMMENT ON CONSTRAINT %{identity}s ON [DOMAIN] %{parentobj}s IS %{comment}s
 */
static ObjTree *
deparse_CommentOnConstraintSmt(Oid objectId, Node *parsetree)
{
	CommentStmt *node = (CommentStmt *) parsetree;
	ObjTree    *ret;
	HeapTuple	constrTup;
	Form_pg_constraint constrForm;
	ObjectAddress addr;

	Assert(node->objtype == OBJECT_TABCONSTRAINT || node->objtype == OBJECT_DOMCONSTRAINT);

	constrTup = SearchSysCache1(CONSTROID, objectId);
	if (!HeapTupleIsValid(constrTup))
		elog(ERROR, "cache lookup failed for constraint with OID %u", objectId);
	constrForm = (Form_pg_constraint) GETSTRUCT(constrTup);

	if (OidIsValid(constrForm->conrelid))
		ObjectAddressSet(addr, RelationRelationId, constrForm->conrelid);
	else
		ObjectAddressSet(addr, TypeRelationId, constrForm->contypid);

	ret = new_objtree_VA("COMMENT ON CONSTRAINT %{identity}s ON %{domain}s %{parentobj}s", 3,
						 "identity", ObjTypeString, pstrdup(NameStr(constrForm->conname)),
						 "domain", ObjTypeString,
						 (node->objtype == OBJECT_DOMCONSTRAINT) ? "DOMAIN" : "",
						 "parentobj", ObjTypeString,
						 getObjectIdentity(&addr, false));

	/* Add the comment clause */
	append_literal_or_null(ret, "IS %{comment}s", node->comment);

	ReleaseSysCache(constrTup);
	return ret;
}

/*
 * Deparse an CommentStmt (COMMENT ON ...).
 *
 * Given the object address and the parse tree that created it, return an
 * ObjTree representing the comment command.
 *
 * Verbose syntax
 * COMMENT ON %{objtype}s %{identity}s IS %{comment}s
 */
static ObjTree *
deparse_CommentStmt(ObjectAddress address, Node *parsetree)
{
	CommentStmt *node = (CommentStmt *) parsetree;
	ObjTree    *ret;
	char	   *identity;

	/*
	 * Constraints are sufficiently different that it is easier to handle them
	 * separately.
	 */
	if (node->objtype == OBJECT_DOMCONSTRAINT ||
		node->objtype == OBJECT_TABCONSTRAINT)
	{
		Assert(address.classId == ConstraintRelationId);
		return deparse_CommentOnConstraintSmt(address.objectId, parsetree);
	}

	ret = new_objtree_VA("COMMENT ON %{objtype}s", 1,
						 "objtype", ObjTypeString,
						 (char *) stringify_objtype(node->objtype, false));

	/*
	 * Add the object identity clause.  For zero argument aggregates we need
	 * to add the (*) bit; in all other cases we can just use
	 * getObjectIdentity.
	 *
	 * XXX shouldn't we instead fix the object identities for zero-argument
	 * aggregates?
	 */
	if (node->objtype == OBJECT_AGGREGATE)
	{
		HeapTuple	procTup;
		Form_pg_proc procForm;

		procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(address.objectId));
		if (!HeapTupleIsValid(procTup))
			elog(ERROR, "cache lookup failed for procedure with OID %u",
				 address.objectId);
		procForm = (Form_pg_proc) GETSTRUCT(procTup);
		if (procForm->pronargs == 0)
			identity = psprintf("%s(*)",
								quote_qualified_identifier(get_namespace_name(procForm->pronamespace),
														   NameStr(procForm->proname)));
		else
			identity = getObjectIdentity(&address, false);
		ReleaseSysCache(procTup);
	}
	else
		identity = getObjectIdentity(&address, false);

	append_string_object(ret, "%{identity}s", "identity", identity);

	/* Add the comment clause; can be either NULL or a quoted literal. */
	append_literal_or_null(ret, "IS %{comment}s", node->comment);

	return ret;
}

/*
 * Deparse a SecLabelStmt (SECURITY LABEL)
 *
 * Given the ObjectAddress of the target object and the parse tree of the
 * SECURITY LABEL command, return an ObjTree representing the SECURITY LABEL
 * command.
 *
 * Verbose syntax
 * SECURITY LABEL FOR %{provider}s ON %{object_type_name}s %{identity}s IS %{label}s
 */
static ObjTree *
deparse_SecLabelStmt(ObjectAddress address, Node *parsetree)
{
	SecLabelStmt *node = (SecLabelStmt *) parsetree;
	ObjTree	   *ret;

	Assert(node->provider);

	ret = new_objtree_VA("SECURITY LABEL FOR %{provider}s ON %{objtype}s %{identity}s", 3,
						 "provider", ObjTypeString, node->provider,
						 "objtype", ObjTypeString,
						 stringify_objtype(node->objtype, false),
						 "identity", ObjTypeString, getObjectIdentity(&address, false));

	/* Add the label clause; can be either NULL or a quoted literal. */
	append_literal_or_null(ret, "IS %{label}s", node->label);

	return ret;
}

/*
 * Deparse a CreateAmStmt (CREATE ACCESS METHOD).
 *
 * Given an access method OID and the parse tree that created it, return an
 * ObjTree representing the CREATE ACCESS METHOD command.
 *
 * Verbose syntax
 * CREATE ACCESS METHOD %{identity}I TYPE %{am_type}s HANDLER %{handler}D
 */
static ObjTree *
deparse_CreateAmStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *ret;
	HeapTuple	amTup;
	Form_pg_am	amForm;
	char	   *amtype;

	amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(amTup))
		elog(ERROR, "cache lookup failed for access method with OID %u",
			 objectId);
	amForm = (Form_pg_am) GETSTRUCT(amTup);

	switch (amForm->amtype)
	{
		case 'i':
			amtype = "INDEX";
			break;
		case 't':
			amtype = "TABLE";
			break;
		default:
			elog(ERROR, "invalid type %c for access method", amForm->amtype);
	}

	ret = new_objtree_VA("CREATE ACCESS METHOD %{identity}I TYPE %{am_type}s HANDLER %{handler}D", 3,
						 "identity", ObjTypeString,
						 NameStr(amForm->amname),
						 "am_type", ObjTypeString, amtype,
						 "handler", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 amForm->amhandler));

	ReleaseSysCache(amTup);

	return ret;
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

	Assert(cmd->type == SCT_Simple);

	parsetree = cmd->parsetree;
	objectId = cmd->d.simple.address.objectId;

	if (cmd->in_extension && (nodeTag(parsetree) != T_CreateExtensionStmt))
		return NULL;

	/* This switch needs to handle everything that ProcessUtilitySlow does */
	switch (nodeTag(parsetree))
	{
		case T_AlterCollationStmt:
			return deparse_AlterCollation(objectId, parsetree);

		case T_AlterDomainStmt:
			return deparse_AlterDomainStmt(objectId, parsetree,
										   cmd->d.simple.secondaryObject);

		case T_AlterEnumStmt:
			return deparse_AlterEnumStmt(objectId, parsetree);

		case T_AlterExtensionContentsStmt:
			return deparse_AlterExtensionContentsStmt(objectId, parsetree,
													  cmd->d.simple.secondaryObject);

		case T_AlterExtensionStmt:
			return deparse_AlterExtensionStmt(objectId, parsetree);

		case T_AlterFdwStmt:
			return deparse_AlterFdwStmt(objectId, parsetree);

		case T_AlterForeignServerStmt:
			return deparse_AlterForeignServerStmt(objectId, parsetree);

		case T_AlterFunctionStmt:
			return deparse_AlterFunction(objectId, parsetree);

		case T_AlterObjectDependsStmt:
			return deparse_AlterDependStmt(objectId, parsetree);

		case T_AlterObjectSchemaStmt:
			return deparse_AlterObjectSchemaStmt(cmd->d.simple.address,
												 parsetree,
												 cmd->d.simple.secondaryObject);

		case T_AlterOperatorStmt:
			return deparse_AlterOperatorStmt(objectId, parsetree);

		case T_AlterOwnerStmt:
			return deparse_AlterOwnerStmt(cmd->d.simple.address, parsetree);

		case T_AlterPolicyStmt:
			return deparse_AlterPolicyStmt(objectId, parsetree);

		case T_AlterSeqStmt:
			return deparse_AlterSeqStmt(objectId, parsetree);

		case T_AlterStatsStmt:
			return deparse_AlterStatsStmt(objectId, parsetree);

		case T_AlterTSDictionaryStmt:
			return deparse_AlterTSDictionaryStmt(objectId, parsetree);

		case T_AlterTypeStmt:
			return deparse_AlterTypeSetStmt(objectId, parsetree);

		case T_AlterUserMappingStmt:
			return deparse_AlterUserMappingStmt(objectId, parsetree);

		case T_CommentStmt:
			return deparse_CommentStmt(cmd->d.simple.address, parsetree);

		case T_CompositeTypeStmt:
			return deparse_CompositeTypeStmt(objectId, parsetree);

		case T_CreateAmStmt:
			return deparse_CreateAmStmt(objectId, parsetree);

		case T_CreateCastStmt:
			return deparse_CreateCastStmt(objectId, parsetree);

		case T_CreateConversionStmt:
			return deparse_CreateConversion(objectId, parsetree);

		case T_CreateDomainStmt:
			return deparse_CreateDomain(objectId, parsetree);

		case T_CreateEnumStmt:	/* CREATE TYPE AS ENUM */
			return deparse_CreateEnumStmt(objectId, parsetree);

		case T_CreateExtensionStmt:
			return deparse_CreateExtensionStmt(objectId, parsetree);

		case T_CreateFdwStmt:
			return deparse_CreateFdwStmt(objectId, parsetree);

		case T_CreateForeignServerStmt:
			return deparse_CreateForeignServerStmt(objectId, parsetree);

		case T_CreateFunctionStmt:
			return deparse_CreateFunction(objectId, parsetree);

		case T_CreateOpFamilyStmt:
			return deparse_CreateOpFamily(objectId, parsetree);

		case T_CreatePLangStmt:
			return deparse_CreateLangStmt(objectId, parsetree);

		case T_CreatePolicyStmt:
			return deparse_CreatePolicyStmt(objectId, parsetree);

		case T_CreateRangeStmt: /* CREATE TYPE AS RANGE */
			return deparse_CreateRangeStmt(objectId, parsetree);

		case T_CreateSchemaStmt:
			return deparse_CreateSchemaStmt(objectId, parsetree);

		case T_CreateSeqStmt:
			return deparse_CreateSeqStmt(objectId, parsetree);

		case T_CreateStatsStmt:
			return deparse_CreateStatisticsStmt(objectId, parsetree);

		case T_CreateStmt:
			return deparse_CreateStmt(objectId, parsetree);

		case T_CreateForeignTableStmt:
			return deparse_CreateForeignTableStmt(objectId, parsetree);

		case T_CreateTableAsStmt:	/* CREATE MATERIALIZED VIEW */
			return deparse_CreateTableAsStmt_vanilla(objectId, parsetree);

		case T_CreateTransformStmt:
			return deparse_CreateTransformStmt(objectId, parsetree);

		case T_CreateTrigStmt:
			return deparse_CreateTrigStmt(objectId, parsetree);

		case T_CreateUserMappingStmt:
			return deparse_CreateUserMappingStmt(objectId, parsetree);

		case T_DefineStmt:
			return deparse_DefineStmt(objectId, parsetree,
									  cmd->d.simple.secondaryObject);

		case T_IndexStmt:
			return deparse_IndexStmt(objectId, parsetree);

		case T_RefreshMatViewStmt:
			return deparse_RefreshMatViewStmt(objectId, parsetree);

		case T_RenameStmt:
			return deparse_RenameStmt(cmd->d.simple.address, parsetree);

		case T_RuleStmt:
			return deparse_RuleStmt(objectId, parsetree);

		case T_ViewStmt:		/* CREATE VIEW */
			return deparse_ViewStmt(objectId, parsetree);

		case T_SecLabelStmt:
			return deparse_SecLabelStmt(cmd->d.simple.address, parsetree);

		default:
			elog(LOG, "unrecognized node type in deparse command: %d",
				 (int) nodeTag(parsetree));
	}

	return NULL;
}

/*
 * Workhorse to deparse a CollectedCommand.
 */
char *
deparse_utility_command(CollectedCommand *cmd, bool verbose_mode)
{
	OverrideSearchPath *overridePath;
	MemoryContext oldcxt;
	MemoryContext tmpcxt;
	ObjTree    *tree;
	char	   *command = NULL;
	StringInfoData str;

	/*
	 * Allocate everything done by the deparsing routines into a temp context,
	 * to avoid having to sprinkle them with memory handling code, but
	 * allocate the output StringInfo before switching.
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
	 * to obtain deparsed versions of expressions.  In such results, we want
	 * all object names to be qualified, so that results are "portable" to
	 * environments with different search_path settings.  Rather than inject
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
			tree = deparse_AlterRelation(cmd);
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
		case SCT_AlterDefaultPrivileges:
			tree = deparse_AlterDefaultPrivilegesStmt(cmd);
			break;
		case SCT_CreatePublication:
			tree = deparse_CreatePublicationStmt(cmd);
			break;
		case SCT_AlterPublication:
			tree = deparse_AlterPublicationStmt(cmd);
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
		Jsonb	   *jsonb;

		jsonb = objtree_to_jsonb(tree);
		command = JsonbToCString(&str, &jsonb->root, JSONB_ESTIMATED_LEN);
	}

	/*
	 * Clean up.  Note that since we created the StringInfo in the caller's
	 * context, the output string is not deleted here.
	 */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	return command;
}

/*
 * Given a CollectedCommand, return a JSON representation of it.
 *
 * The command is expanded fully so that there are no ambiguities even in the
 * face of search_path changes.
 */
Datum
ddl_deparse_to_json(PG_FUNCTION_ARGS)
{
	CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);
	char	   *command;

	command = deparse_utility_command(cmd, true);

	if (command)
		PG_RETURN_TEXT_P(cstring_to_text(command));

	PG_RETURN_NULL();
}
