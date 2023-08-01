/*-------------------------------------------------------------------------
 *
 * ddldeparse.c
 *	  Functions to convert utility commands to machine-parseable
 *	  representation
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
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
 * Deparsed JsonbValue is created by using:
 * 	new_jsonb_VA where the key-value pairs composing an jsonb object can be
 * 	derived using the passed variable arguments. In order to successfully
 *  construct one key:value pair, a set of three arguments consisting of a name
 * 	(string), type (from the jbvType enum) and value must be supplied. It can
 *  take multiple such sets and construct multiple key-value pairs and append
 *  those to output parse-state.
 *
 * IDENTIFICATION
 *	  src/backend/commands/ddldeparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "partitioning/partbounds.h"
#include "tcop/ddldeparse.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

/* Estimated length of the generated jsonb string */
#define JSONB_ESTIMATED_LEN 128

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
			return NULL;
		default:
			elog(ERROR, "unexpected persistence marking %c",
				 persistence);
			return NULL;		/* make compiler happy */
	}
}

/*
 * Insert JsonbValue key to the output parse state.
 */
static void
insert_jsonb_key(JsonbParseState *state, char *name)
{
	JsonbValue	key;

	/* Push the key */
	key.type = jbvString;
	key.val.string.val = name;
	key.val.string.len = strlen(name);
	pushJsonbValue(&state, WJB_KEY, &key);
}

/*
 * Append new jsonb key:value pairs to the output parse state -- varargs
 * function
 *
 * Arguments:
 * "state": the output jsonb state where each key-value pair is pushed.
 *
 * "numobjs": the number of key:value pairs to be pushed to JsonbParseState;
 * for each one, a name (string), type (from the jbvType enum) and value must
 * be supplied.  The value must match the type given; for instance, jbvBool
 * requires an bool, jbvString requires a char * and so on.
 * Each element type must match the conversion specifier given in the format
 * string, as described in ddl_deparse_expand_command.
 *
 * Notes:
 * a) The caller can pass "fmt":"fmtstr" as a regular key:value pair to this,
 * no special handling needed for that.
 * b) The caller need to carefully pass sets of arguments, we don't have the
 * luxury of sprintf-like compiler warnings for malformed argument lists.
 */
static void
new_jsonb_VA(JsonbParseState *state, int numobjs,...)
{
	va_list		args;
	int			i;
	JsonbValue	val;

	/* Process the given varargs */
	va_start(args, numobjs);

	for (i = 0; i < numobjs; i++)
	{
		char	   *name;
		enum jbvType type;

		name = va_arg(args, char *);
		type = va_arg(args, enum jbvType);

		/* Push the key first */
		insert_jsonb_key(state, name);

		/*
		 * For all param types other than jbvNull, there must be a value in
		 * the varargs. Fetch it and add the fully formed subobject into the
		 * main object.
		 */
		switch (type)
		{
			case jbvNull:
				/* Null params don't have a value (obviously) */
				val.type = jbvNull;
				pushJsonbValue(&state, WJB_VALUE, &val);
				break;

			case jbvBool:
				/* Push the bool value */
				val.type = jbvBool;
				val.val.boolean = va_arg(args, int);
				pushJsonbValue(&state, WJB_VALUE, &val);
				break;

			case jbvString:
				/* Push the string value */
				val.type = jbvString;
				val.val.string.val = pstrdup(va_arg(args, char *));
				val.val.string.len = strlen(val.val.string.val);
				pushJsonbValue(&state, WJB_VALUE, &val);
				break;

			case jbvNumeric:
				/* Push the numeric value */
				val.type = jbvNumeric;
				val.val.numeric = (Numeric)
					DatumGetNumeric(DirectFunctionCall1(
														int8_numeric,
														va_arg(args, int)));

				pushJsonbValue(&state, WJB_VALUE, &val);
				break;

			default:
				elog(ERROR, "unrecognized jbvType %d", type);
		}
	}

	va_end(args);
}

/*
 * A helper routine to insert jsonb for typId to the output parse state.
 */
static void
new_jsonb_for_type(JsonbParseState *state, char *parentKey,
				   Oid typId, int32 typmod)
{
	Oid			typnspid;
	char	   *type_nsp;
	char	   *type_name = NULL;
	char	   *typmodstr;
	bool		type_array;

	Assert(parentKey);

	format_type_detailed(typId, typmod, &typnspid, &type_name, &typmodstr,
						 &type_array);

	if (OidIsValid(typnspid))
		type_nsp = get_namespace_name_or_temp(typnspid);
	else
		type_nsp = pstrdup("");

	insert_jsonb_key(state, parentKey);
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 4,
				 "schemaname", jbvString, type_nsp,
				 "typename", jbvString, type_name,
				 "typmod", jbvString, typmodstr,
				 "typarray", jbvBool, type_array);
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * A helper routine to set up name: schemaname, objname
 *
 * Elements "schema_name" and "obj_name" are set.  If the namespace OID
 * corresponds to a temp schema, that's set to "pg_temp".
 *
 * The difference between those two element types is whether the obj_name will
 * be quoted as an identifier or not, which is not something that this routine
 * concerns itself with; that will be up to the expand function.
 */
static void
new_jsonb_for_qualname(JsonbParseState *state, Oid nspid, char *objName,
					   char *keyName, bool createObject)
{
	char	   *namespace;

	if (isAnyTempNamespace(nspid))
		namespace = pstrdup("pg_temp");
	else
		namespace = get_namespace_name(nspid);

	/* Push the key first */
	if (keyName)
		insert_jsonb_key(state, keyName);

	if (createObject)
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	new_jsonb_VA(state, 2,
				 "schemaname", jbvString, namespace,
				 "objname", jbvString, objName);

	if (createObject)
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * A helper routine to set up name: 'schemaname, objname' where the object is
 * specified by classId and objId.
 */
static void
new_jsonb_for_qualname_id(JsonbParseState *state, Oid classId, Oid objectId,
						  char *keyName, bool createObject)
{
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

	new_jsonb_for_qualname(state, DatumGetObjectId(obj_nsp),
						   NameStr(*DatumGetName(obj_name)),
						   keyName, createObject);
	table_close(catalog, AccessShareLock);
}

/*
 * A helper routine to insert key:value where value is array of qualname to
 * the output parse state.
 */
static void
new_jsonbArray_for_qualname_id(JsonbParseState *state,
							   char *keyname, List *array)
{
	ListCell   *lc;

	/* Push the key first */
	insert_jsonb_key(state, keyname);

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	/* Push the array elements now */
	foreach(lc, array)
		new_jsonb_for_qualname_id(state, RelationRelationId, lfirst_oid(lc),
								  NULL, true);

	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
}

/*
 * A helper routine to insert collate object for column
 * definition to the output parse state.
 */
static void
insert_collate_object(JsonbParseState *state, char *parentKey, char *fmt,
					  Oid classId, Oid objectId, char *key)
{
	/*
	 * Insert parent key for which we are going to create value object here.
	 */
	if (parentKey)
		insert_jsonb_key(state, parentKey);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	new_jsonb_VA(state, 1, "fmt", jbvString, fmt);

	/* push object now */
	new_jsonb_for_qualname_id(state, classId, objectId, key, true);

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * A helper routine to insert identity object for the table definition
 * to the output parse state.
 */
static void
insert_identity_object(JsonbParseState *state, Oid nspid, char *relname)
{
	new_jsonb_for_qualname(state, nspid, relname, "identity", true);
}

/*
 * Deparse the sequence CACHE option to Jsonb
 *
 * Verbose syntax
 * CACHE %{value}
 */
static inline void
deparse_Seq_Cache(JsonbParseState *state, Form_pg_sequence seqdata,
				  bool alter_table)
{
	char	   *fmt;

	fmt = alter_table ? "SET CACHE %{value}s" : "CACHE %{value}s";

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 3,
				 "fmt", jbvString, fmt,
				 "clause", jbvString, "cache",
				 "value", jbvString,
				 psprintf(INT64_FORMAT, seqdata->seqcache));
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence CYCLE option to Jsonb.
 *
 * Verbose syntax
 * SET %{no}s CYCLE
 * OR
 * %{no}s CYCLE
 */
static inline void
deparse_Seq_Cycle(JsonbParseState *state, Form_pg_sequence seqdata,
				  bool alter_table)
{
	StringInfoData fmtStr;

	initStringInfo(&fmtStr);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	if (alter_table)
		appendStringInfoString(&fmtStr, "SET ");

	if (!seqdata->seqcycle)
	{
		appendStringInfoString(&fmtStr, "%{no}s ");
		new_jsonb_VA(state, 1, "no", jbvString, "NO");
	}

	appendStringInfoString(&fmtStr, "CYCLE");
	new_jsonb_VA(state, 2,
				 "fmt", jbvString, fmtStr.data,
				 "clause", jbvString, "cycle");

	pfree(fmtStr.data);
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence INCREMENT BY option to Jsonb
 *
 * Verbose syntax
 * SET INCREMENT BY %{value}s
 * OR
 * INCREMENT BY %{value}s
 */
static inline void
deparse_Seq_IncrementBy(JsonbParseState *state, Form_pg_sequence seqdata,
						bool alter_table)
{
	char	   *fmt;

	fmt = alter_table ? "SET INCREMENT BY %{value}s"
		: "INCREMENT BY %{value}s";

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 3,
				 "fmt", jbvString, fmt,
				 "clause", jbvString, "seqincrement",
				 "value", jbvString,
				 psprintf(INT64_FORMAT, seqdata->seqincrement));
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence MAXVALUE option to Jsonb.
 *
 * Verbose syntax
 * SET MAXVALUE %{value}s
 * OR
 * MAXVALUE %{value}s
 */
static inline void
deparse_Seq_Maxvalue(JsonbParseState *state, Form_pg_sequence seqdata,
					 bool alter_table)
{
	char	   *fmt;

	fmt = alter_table ? "SET MAXVALUE %{value}s" : "MAXVALUE %{value}s";

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 3,
				 "fmt", jbvString, fmt,
				 "clause", jbvString, "maxvalue",
				 "value", jbvString,
				 psprintf(INT64_FORMAT, seqdata->seqmax));
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence MINVALUE option to Jsonb
 *
 * Verbose syntax
 * SET MINVALUE %{value}s
 * OR
 * MINVALUE %{value}s
 */
static inline void
deparse_Seq_Minvalue(JsonbParseState *state, Form_pg_sequence seqdata,
					 bool alter_table)
{
	char	   *fmt;

	fmt = alter_table ? "SET MINVALUE %{value}s" : "MINVALUE %{value}s";

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 3,
				 "fmt", jbvString, fmt,
				 "clause", jbvString, "minvalue",
				 "value", jbvString,
				 psprintf(INT64_FORMAT, seqdata->seqmin));
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence OWNED BY command.
 *
 * Verbose syntax
 * OWNED BY %{owner}D
 */
static void
deparse_Seq_OwnedBy(JsonbParseState *state, Oid sequenceId,
					bool alter_table)
{
	Relation	depRel;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	bool		elem_found PG_USED_FOR_ASSERTS_ONLY = false;

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

		/* mark the begin of owner's definition object */
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		new_jsonb_VA(state, 2,
					 "fmt", jbvString, "OWNED BY %{owner}D",
					 "clause", jbvString, "owned");

		/* owner key */
		insert_jsonb_key(state, "owner");

		/* owner value begin */
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		new_jsonb_for_qualname_id(state, RelationRelationId,
								  ownerId, NULL, false);
		new_jsonb_VA(state, 1, "attrname", jbvString, colname);

		/* owner value end */
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);

		/* mark the end of owner's definition object */
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);

#ifdef USE_ASSERT_CHECKING
		elem_found = true;
#endif
	}

	systable_endscan(scan);
	relation_close(depRel, AccessShareLock);

	/*
	 * If there's no owner column, assert. The caller must have checked
	 * presence of owned_by element before invoking this.
	 */
	Assert(elem_found);
}

/*
 * Deparse the sequence START WITH option to Jsonb.
 *
 * Verbose syntax
 * SET START WITH %{value}s
 * OR
 * START WITH %{value}s
 */
static inline void
deparse_Seq_Startwith(JsonbParseState *state,
					  Form_pg_sequence seqdata, bool alter_table)
{
	char	   *fmt;

	fmt = alter_table ? "SET START WITH %{value}s" : "START WITH %{value}s";

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 3,
				 "fmt", jbvString, fmt,
				 "clause", jbvString, "start",
				 "value", jbvString,
				 psprintf(INT64_FORMAT, seqdata->seqstart));
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence RESTART option to Jsonb
 *
 * Verbose syntax
 * RESTART %{value}s
 */
static inline void
deparse_Seq_Restart(JsonbParseState *state, int64 last_value)
{
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 3,
				 "fmt", jbvString, "RESTART %{value}s",
				 "clause", jbvString, "restart",
				 "value", jbvString,
				 psprintf(INT64_FORMAT, last_value));
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the sequence AS option.
 *
 * Verbose syntax
 * AS %{seqtype}T
 */
static inline void
deparse_Seq_As(JsonbParseState *state, Form_pg_sequence seqdata)
{
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 1, "fmt", jbvString, "AS %{seqtype}T");
	new_jsonb_for_type(state, "seqtype", seqdata->seqtypid, -1);
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse the definition of column identity to Jsonb.
 *
 * Verbose syntax
 * SET GENERATED %{option}s %{seq_definition: }s
 * OR
 * GENERATED %{option}s AS IDENTITY ( %{seq_definition: }s )
 */
static void
deparse_ColumnIdentity(JsonbParseState *state, char *parentKey,
					   Oid seqrelid, char identity, bool alter_table)
{
	Form_pg_sequence seqform;
	Sequence_values *seqvalues;
	char	   *identfmt;
	StringInfoData fmtStr;

	initStringInfo(&fmtStr);

	/*
	 * Insert parent key for which we are going to create value object here.
	 */
	if (parentKey)
		insert_jsonb_key(state, parentKey);

	/* create object now for value of identity_column */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	/* identity_type object creation */
	if (identity == ATTRIBUTE_IDENTITY_ALWAYS ||
		identity == ATTRIBUTE_IDENTITY_BY_DEFAULT)
	{
		appendStringInfoString(&fmtStr, "%{identity_type}s");
		insert_jsonb_key(state, "identity_type");

		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		identfmt = alter_table ? "SET GENERATED %{option}s" :
			"GENERATED %{option}s AS IDENTITY";

		new_jsonb_VA(state, 2,
					 "fmt", jbvString, identfmt,
					 "option", jbvString,
					 (identity == ATTRIBUTE_IDENTITY_ALWAYS ?
					  "ALWAYS" : "BY DEFAULT"));
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	/* seq_definition array object creation */
	insert_jsonb_key(state, "seq_definition");

	appendStringInfoString(&fmtStr, alter_table ? " %{seq_definition: }s" :
						   " ( %{seq_definition: }s )");

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	seqvalues = get_sequence_values(seqrelid);
	seqform = seqvalues->seqform;

	/* Definition elements */
	deparse_Seq_Cache(state, seqform, alter_table);
	deparse_Seq_Cycle(state, seqform, alter_table);
	deparse_Seq_IncrementBy(state, seqform, alter_table);
	deparse_Seq_Minvalue(state, seqform, alter_table);
	deparse_Seq_Maxvalue(state, seqform, alter_table);
	deparse_Seq_Startwith(state, seqform, alter_table);
	deparse_Seq_Restart(state, seqvalues->last_value);
	/* We purposefully do not emit OWNED BY here */

	pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

	pfree(fmtStr.data);

	/* end of idendity_column object */
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse a ColumnDef node within a regular (non-typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway).
 *
 * Verbose syntax
 * "%{name}I %{coltype}T STORAGE %{colstorage}s %{compression}s %{collation}s
 *  %{not_null}s %{default}s %{identity_column}s %{generated_column}s"
 */
static void
deparse_ColumnDef(JsonbParseState *state, Relation relation,
				  List *dpcontext, bool composite, ColumnDef *coldef,
				  bool is_alter)
{
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;
	StringInfoData fmtStr;

	initStringInfo(&fmtStr);

	/*
	 * Inherited columns without local definitions should be skipped. We don't
	 * want those to be part of final string.
	 */
	if (!coldef->is_local)
		return;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/* start making column object */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	/* create name and type elements for column */
	appendStringInfoString(&fmtStr, "%{name}I");
	new_jsonb_VA(state, 2,
				 "name", jbvString, coldef->colname,
				 "type", jbvString, "column");

	/*
	 * create coltype object having 4 elements: schemaname, typename, typemod,
	 * typearray
	 */
	appendStringInfoString(&fmtStr, " %{coltype}T");
	new_jsonb_for_type(state, "coltype", typid, typmod);

	/* STORAGE clause */
	if (!composite)
	{
		appendStringInfoString(&fmtStr, " STORAGE %{colstorage}s");
		new_jsonb_VA(state, 1,
					 "colstorage", jbvString,
					 storage_name(attrForm->attstorage));
	}

	/* COMPRESSION clause */
	if (coldef->compression)
	{
		appendStringInfoString(&fmtStr, " %{compression}s");
		insert_jsonb_key(state, "compression");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		new_jsonb_VA(state, 2,
					 "fmt", jbvString, "COMPRESSION %{compression_method}I",
					 "compression_method", jbvString, coldef->compression);
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	/* COLLATE clause */
	if (OidIsValid(typcollation))
	{
		appendStringInfoString(&fmtStr, " %{collation}s");
		insert_collate_object(state, "collation",
							  "COLLATE %{collation_name}D",
							  CollationRelationId, typcollation,
							  "collation_name");
	}

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

		/* NOT NULL */
		if (saw_notnull)
		{
			appendStringInfoString(&fmtStr, " %{not_null}s");
			new_jsonb_VA(state, 1,
						 "not_null", jbvString, "NOT NULL");
		}


		/* DEFAULT */
		if (attrForm->atthasdef &&
			coldef->generated != ATTRIBUTE_GENERATED_STORED)
		{
			char	   *defstr;

			appendStringInfoString(&fmtStr, " %{default}s");

			defstr = relation_get_column_default(relation, attrForm->attnum,
												 dpcontext);

			insert_jsonb_key(state, "default");
			pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
			new_jsonb_VA(state, 2,
						 "fmt", jbvString, "DEFAULT %{default}s",
						 "default", jbvString, defstr);

			pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		}

		/* IDENTITY COLUMN */
		if (coldef->identity)
		{
			/*
			 * For identity column, find the sequence owned by column in order
			 * to deparse the column definition.
			 */
			seqrelid = getIdentitySequence(relid, attrForm->attnum, true);
			if (OidIsValid(seqrelid) && coldef->identitySequence)
				seqrelid = RangeVarGetRelid(coldef->identitySequence,
											NoLock, false);

			if (OidIsValid(seqrelid))
			{
				appendStringInfoString(&fmtStr, " %{identity_column}s");
				deparse_ColumnIdentity(state, "identity_column",
									   seqrelid,
									   coldef->identity, is_alter);
			}
		}


		/* GENERATED COLUMN EXPRESSION */
		if (coldef->generated == ATTRIBUTE_GENERATED_STORED)
		{
			char	   *defstr;

			appendStringInfoString(&fmtStr, " %{generated_column}s");
			defstr = relation_get_column_default(relation, attrForm->attnum,
												 dpcontext);

			insert_jsonb_key(state, "generated_column");
			pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
			new_jsonb_VA(state, 2,
						 "fmt", jbvString, "GENERATED ALWAYS AS"
						 " (%{generation_expr}s) STORED",
						 "generation_expr", jbvString, defstr);

			pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		}
	}

	ReleaseSysCache(attrTup);

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

	pfree(fmtStr.data);

	/* mark the end of one column object */
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Helper for deparse_ColumnDef_typed()
 *
 * Returns true if we need to deparse a ColumnDef node within a typed
 * table creation.
 */
static bool
deparse_ColDef_typed_needed(Relation relation, ColumnDef *coldef,
							Form_pg_attribute *atFormOut, bool *notnull)
{
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

	if (atFormOut)
		*atFormOut = attrForm;

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/*
	 * Search for a NOT NULL declaration. As in deparse_ColumnDef, we rely on
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

	if (notnull)
		*notnull = saw_notnull;

	if (!saw_notnull && !attrForm->atthasdef)
	{
		ReleaseSysCache(attrTup);
		return false;
	}

	ReleaseSysCache(attrTup);
	return true;
}

/*
 * Deparse a ColumnDef node within a typed table creation. This is simpler
 * than the regular case, because we don't have to emit the type declaration,
 * collation, or default. Here we only return something if the column is being
 * declared NOT NULL.
 *
 * As in deparse_ColumnDef, any other constraint is processed elsewhere.
 *
 * Verbose syntax
 * %{name}I WITH OPTIONS %{not_null}s %{default}s.
 */
static void
deparse_ColumnDef_typed(JsonbParseState *state, Relation relation,
						List *dpcontext, ColumnDef *coldef)
{
	bool		needed;
	Form_pg_attribute attrForm;
	bool		saw_notnull;
	StringInfoData fmtStr;

	initStringInfo(&fmtStr);

	needed = deparse_ColDef_typed_needed(relation, coldef,
										 &attrForm, &saw_notnull);
	if (!needed)
		return;

	/* start making column object */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	appendStringInfoString(&fmtStr, "%{name}I WITH OPTIONS");

	/* TYPE and NAME */
	new_jsonb_VA(state, 2,
				 "type", jbvString, "column",
				 "name", jbvString, coldef->colname);

	/* NOT NULL */
	if (saw_notnull)
	{
		appendStringInfoString(&fmtStr, " %{not_null}s");
		new_jsonb_VA(state, 1, "not_null", jbvString, "NOT NULL");
	}

	/* DEFAULT */
	if (attrForm->atthasdef)
	{
		char	   *defstr;

		appendStringInfoString(&fmtStr, " %{default}s");
		defstr = relation_get_column_default(relation, attrForm->attnum,
											 dpcontext);

		insert_jsonb_key(state, "default");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		new_jsonb_VA(state, 2,
					 "fmt", jbvString, "DEFAULT %{default}s",
					 "default", jbvString, defstr);

		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

	pfree(fmtStr.data);

	/* mark the end of column object */
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	/* Generated columns are not supported on typed tables, so we are done */
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Deal with all the table elements (columns and constraints).
 *
 * Note we ignore constraints in the parse node here; they are extracted from
 * system catalogs instead.
 */
static void
deparse_TableElems(JsonbParseState *state, Relation relation,
				   List *tableElements, List *dpcontext,
				   bool typed, bool composite)
{
	ListCell   *lc;

	foreach(lc, tableElements)
	{
		Node	   *elt = (Node *) lfirst(lc);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					if (typed)
						deparse_ColumnDef_typed(state, relation,
												dpcontext,
												(ColumnDef *) elt);
					else
						deparse_ColumnDef(state, relation, dpcontext,
										  composite, (ColumnDef *) elt, false);
				}
				break;
			case T_Constraint:
				break;
			default:
				elog(ERROR, "invalid node type %d", nodeTag(elt));
		}
	}
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Given a table OID, obtain its constraints and append them to the given
 * JsonbParseState.
 *
 * This works for typed tables, regular tables.
 *
 * Note that CONSTRAINT_FOREIGN constraints are always ignored.
 */
static void
deparse_Constraints(JsonbParseState *state, Oid relationId)
{
	Relation	conRel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;

	Assert(OidIsValid(relationId));

	/*
	 * Scan pg_constraint to fetch all constraints linked to the given
	 * relation.
	 */
	conRel = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&key, Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relationId));
	scan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId, true,
							  NULL, 1, &key);

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
		StringInfoData fmtStr;

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
			case CONSTRAINT_EXCLUSION:
				contype = "exclusion";
				break;
			default:
				elog(ERROR, "unrecognized constraint type");
		}

		/* No need to deparse constraints inherited from parent table. */
		if (OidIsValid(constrForm->conparentid))
			continue;

		/*
		 * "type" and "contype" are not part of the printable output, but are
		 * useful to programmatically distinguish these from columns and among
		 * different constraint types.
		 *
		 * XXX it might be useful to also list the column names in a PK, etc.
		 */
		initStringInfo(&fmtStr);
		appendStringInfoString(&fmtStr, "CONSTRAINT %{name}I %{definition}s");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		new_jsonb_VA(state, 4,
					 "type", jbvString, "constraint",
					 "contype", jbvString, contype,
					 "name", jbvString, NameStr(constrForm->conname),
					 "definition", jbvString,
					 pg_get_constraintdef_string(constrForm->oid));

		if (constrForm->conindid &&
			(constrForm->contype == CONSTRAINT_PRIMARY ||
			 constrForm->contype == CONSTRAINT_UNIQUE ||
			 constrForm->contype == CONSTRAINT_EXCLUSION))
		{
			Oid			tblspc = get_rel_tablespace(constrForm->conindid);

			if (OidIsValid(tblspc))
			{
				char	   *tblspcname = get_tablespace_name(tblspc);

				if (!tblspcname)
					elog(ERROR, "cache lookup failed for tablespace %u",
						 tblspc);

				appendStringInfoString(&fmtStr,
									   " USING INDEX TABLESPACE %{tblspc}s");
				new_jsonb_VA(state, 1,
							 "tblspc", jbvString, tblspcname);
			}
		}

		/* We have full fmt by now, so add jsonb element for that */
		new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

		pfree(fmtStr.data);

		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	systable_endscan(scan);
	table_close(conRel, AccessShareLock);
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Insert columns and constraints elements(if any) in output JsonbParseState
 */
static void
add_table_elems(JsonbParseState *state, StringInfo fmtStr,
					   Relation relation, List *tableElts, List *dpcontext,
					   Oid objectId, bool inherit, bool typed, bool composite)
{
	insert_jsonb_key(state, "table_elements");

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	/*
	 * Process table elements: column definitions and constraints. Only the
	 * column definitions are obtained from the parse node itself. To get
	 * constraints we rely on pg_constraint, because the parse node might be
	 * missing some things such as the name of the constraints.
	 */
	deparse_TableElems(state, relation, tableElts, dpcontext,
					   typed,	/* typed table */
					   composite);	/* not composite */

	deparse_Constraints(state, objectId);

	/*
	 * Decide if we need to put '()' around table_elements. It is needed for
	 * below cases:
	 *
	 * a) where actual table-elements are present, eg: create table t1 (a int)
	 *
	 * a) inherit case with no local table-elements present, eg: create table
	 * t1 () inherits (t2)
	 *
	 * OTOH, '()' is not needed for below cases when no table-elements are
	 * present:
	 *
	 * a) 'partition of' case, eg: create table t2 partition of t1
	 *
	 * b) 'of type' case, eg: create table t1 of type1;
	 */
	if ((state->contVal.type == jbvArray) &&
		(inherit || (state->contVal.val.array.nElems > 0)))
	{
		appendStringInfoString(fmtStr, " (%{table_elements:, }s)");
	}
	else
		appendStringInfoString(fmtStr, " %{table_elements:, }s");

	/* end of table_elements array */
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
}

/*
 * Deparse DefElems, as used by Create Table
 *
 * Verbose syntax
 * %{label}s = %{value}L
 * where label is: %{schema}I %{label}I
 */
static void
deparse_DefElem(JsonbParseState *state, DefElem *elem, bool is_reset)
{
	StringInfoData fmtStr;
	StringInfoData labelfmt;

	initStringInfo(&fmtStr);

	appendStringInfoString(&fmtStr, "%{label}s");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	/* LABEL */
	initStringInfo(&labelfmt);

	insert_jsonb_key(state, "label");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	if (elem->defnamespace != NULL)
	{
		appendStringInfoString(&labelfmt, "%{schema}I.");
		new_jsonb_VA(state, 1,
					 "schema", jbvString, elem->defnamespace);
	}

	appendStringInfoString(&labelfmt, "%{label}I");
	new_jsonb_VA(state, 2,
				 "label", jbvString, elem->defname,
				 "fmt", jbvString, labelfmt.data);
	pfree(labelfmt.data);

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	/* VALUE */
	if (!is_reset)
	{
		appendStringInfoString(&fmtStr, " = %{value}L");
		new_jsonb_VA(state, 1, "value", jbvString,
					 elem->arg ? defGetString(elem) :
					 defGetBoolean(elem) ? "true" : "false");
	}

	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);
	pfree(fmtStr.data);

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse SET/RESET as used by
 * ALTER TABLE ... ALTER COLUMN ... SET/RESET (...)
 *
 * Verbose syntax
 * ALTER COLUMN %{column}I RESET|SET (%{options:, }s)
 */
static void
deparse_ColumnSetOptions(JsonbParseState *state, AlterTableCmd *subcmd)
{
	ListCell   *cell;
	bool		is_reset = subcmd->subtype == AT_ResetOptions;
	bool		elem_found PG_USED_FOR_ASSERTS_ONLY = false;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	new_jsonb_VA(state, 3,
				 "fmt", jbvString, "ALTER COLUMN %{column}I"
				 " %{option}s (%{options:, }s)",
				 "column", jbvString, subcmd->name,
				 "option", jbvString, is_reset ? "RESET" : "SET");

	insert_jsonb_key(state, "options");
	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	foreach(cell, (List *) subcmd->def)
	{
		DefElem    *elem;

		elem = (DefElem *) lfirst(cell);
		deparse_DefElem(state, elem, is_reset);

#ifdef USE_ASSERT_CHECKING
		elem_found = true;
#endif
	}

	pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	Assert(elem_found);

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse SET/RESET as used by ALTER TABLE ... SET/RESET (...)
 *
 * Verbose syntax
 * RESET|SET (%{options:, }s)
 */
static void
deparse_RelSetOptions(JsonbParseState *state, AlterTableCmd *subcmd)
{
	ListCell   *cell;
	bool		is_reset = subcmd->subtype == AT_ResetRelOptions;
	bool		elem_found PG_USED_FOR_ASSERTS_ONLY = false;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	new_jsonb_VA(state, 2,
				 "fmt", jbvString, "%{set_reset}s (%{options:, }s)",
				 "set_reset", jbvString, is_reset ? "RESET" : "SET");

	/* insert options array */
	insert_jsonb_key(state, "options");

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	foreach(cell, (List *) subcmd->def)
	{
		DefElem    *elem;

		elem = (DefElem *) lfirst(cell);
		deparse_DefElem(state, elem, is_reset);

#ifdef USE_ASSERT_CHECKING
		elem_found = true;
#endif
	}

	pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	Assert(elem_found);

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Deparse WITH clause, as used by Create Table.
 *
 * Verbose syntax (formulated in helper function deparse_DefElem)
 * %{label}s = %{value}L
 */
static void
deparse_withObj(JsonbParseState *state, CreateStmt *node)
{
	ListCell   *cell;

	/* WITH */
	insert_jsonb_key(state, "with");
	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	/* add elements to array */
	foreach(cell, node->options)
	{
		DefElem    *opt = (DefElem *) lfirst(cell);

		deparse_DefElem(state, opt, false);
	}

	/* with's array end */
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
}

/*
 * Deparse a CreateStmt (CREATE TABLE).
 *
 * Given a table OID and the parse tree that created it, return JsonbValue
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D [OF
 * %{of_type}T | PARTITION OF %{parent_identity}D] %{table_elements}s
 * %{inherits}s %{partition_bound}s %{partition_by}s %{access_method}s
 * %{with_clause}s %{tablespace}s
 */
static Jsonb *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	Oid			nspid = relation->rd_rel->relnamespace;
	char	   *relname = RelationGetRelationName(relation);
	List	   *dpcontext;
	char	   *perstr;
	StringInfoData fmtStr;
	JsonbParseState *state = NULL;
	JsonbValue *value;

	initStringInfo(&fmtStr);

	/* mark the begin of ROOT object and start adding elements to it. */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	appendStringInfoString(&fmtStr, "CREATE");

	/* PERSISTENCE */
	perstr = get_persistence_str(relation->rd_rel->relpersistence);
	if (perstr)
	{
		appendStringInfoString(&fmtStr, " %{persistence}s");
		new_jsonb_VA(state, 1,
					 "persistence", jbvString, perstr);
	}

	appendStringInfoString(&fmtStr, " TABLE");

	/* IF NOT EXISTS */
	if (node->if_not_exists)
	{
		appendStringInfoString(&fmtStr, " %{if_not_exists}s");
		new_jsonb_VA(state, 1,
					 "if_not_exists", jbvString, "IF NOT EXISTS");
	}

	/* IDENTITY */
	appendStringInfoString(&fmtStr, " %{identity}D");
	insert_identity_object(state, nspid, relname);

	dpcontext = deparse_context_for(RelationGetRelationName(relation),
									objectId);

	/*
	 * TABLE-ELEMENTS array creation
	 */
	if (node->ofTypename || node->partbound)
	{
		/* Insert the "of type" or "partition of" clause whichever present */
		if (node->ofTypename)
		{
			appendStringInfoString(&fmtStr, " OF %{of_type}T");
			new_jsonb_for_type(state, "of_type",
							   relation->rd_rel->reloftype, -1);
		}
		else
		{
			List	   *parents;
			Oid			objid;

			appendStringInfoString(&fmtStr, " PARTITION OF %{parent_identity}D");
			parents = relation_get_inh_parents(objectId);
			objid = linitial_oid(parents);
			Assert(list_length(parents) == 1);
			new_jsonb_for_qualname_id(state, RelationRelationId,
									  objid, "parent_identity", true);
		}

		add_table_elems(state, &fmtStr, relation,
							   node->tableElts, dpcontext, objectId,
							   false,	/* not inherit */
							   true,	/* typed table */
							   false);	/* not composite */
	}
	else
	{
		List	   *inhrelations;

		/*
		 * There is no need to process LIKE clauses separately; they have
		 * already been transformed into columns and constraints.
		 */

		add_table_elems(state, &fmtStr, relation,
							   node->tableElts, dpcontext, objectId,
							   true,	/* inherit */
							   false,	/* not typed table */
							   false);	/* not composite */

		/*
		 * Add inheritance specification.  We cannot simply scan the list of
		 * parents from the parser node, because that may lack the actual
		 * qualified names of the parent relations.  Rather than trying to
		 * re-resolve them from the information in the parse node, it seems
		 * more accurate and convenient to grab it from pg_inherits.
		 */
		if (node->inhRelations != NIL)
		{
			appendStringInfoString(&fmtStr, " %{inherits}s");
			insert_jsonb_key(state, "inherits");

			pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

			new_jsonb_VA(state, 1, "fmt", jbvString, "INHERITS (%{parents:, }D)");
			inhrelations = relation_get_inh_parents(objectId);

			new_jsonbArray_for_qualname_id(state, "parents", inhrelations);
			pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		}
	}

	/* FOR VALUES clause */
	if (node->partbound)
	{
		appendStringInfoString(&fmtStr, " %{partition_bound}s");

		/*
		 * Get pg_class.relpartbound. We cannot use partbound in the parsetree
		 * directly as it's the original partbound expression which haven't
		 * been transformed.
		 */
		new_jsonb_VA(state, 1,
					 "partition_bound", jbvString,
					 relation_get_part_bound(objectId));
	}

	/* PARTITION BY clause */
	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		appendStringInfoString(&fmtStr, " %{partition_by}s");
		insert_jsonb_key(state, "partition_by");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		new_jsonb_VA(state, 2,
					 "fmt", jbvString, "PARTITION BY %{definition}s",
					 "definition", jbvString,
					 pg_get_partkeydef_string(objectId));

		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	/* USING clause */
	if (node->accessMethod)
	{
		appendStringInfoString(&fmtStr, " %{access_method}s");
		insert_jsonb_key(state, "access_method");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		new_jsonb_VA(state, 2,
					 "fmt", jbvString, "USING %{access_method}I",
					 "access_method", jbvString, node->accessMethod);
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	/* WITH clause */
	if (node->options)
	{
		appendStringInfoString(&fmtStr, " %{with_clause}s");
		insert_jsonb_key(state, "with_clause");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		new_jsonb_VA(state, 1,
					 "fmt", jbvString, "WITH (%{with:, }s)");

		deparse_withObj(state, node);

		pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	}

	/* TABLESPACE */
	if (node->tablespacename)
	{
		appendStringInfoString(&fmtStr, " %{tablespace}s");
		insert_jsonb_key(state, "tablespace");
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		new_jsonb_VA(state, 2,
					 "fmt", jbvString, "TABLESPACE %{tablespace}I",
					 "tablespace", jbvString, node->tablespacename);
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	relation_close(relation, AccessShareLock);

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

	pfree(fmtStr.data);

	/* Mark the end of ROOT object */
	value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(value);
}

/*
 * Deparse a DropStmt (DROP TABLE).
 *
 * Given an object identity and the parse tree that created it, return
 * jsonb string representing the drop command.
 *
 * Verbose syntax
 * DROP TABLE %{concurrently}s %{if_exists}s %{objidentity}s %{cascade}s
 */
char *
deparse_drop_table(const char *objidentity, Node *parsetree)
{
	DropStmt   *node = (DropStmt *) parsetree;
	StringInfoData fmtStr;
	JsonbValue *jsonbval;
	Jsonb	   *jsonb;
	StringInfoData str;
	JsonbParseState *state = NULL;

	initStringInfo(&str);
	initStringInfo(&fmtStr);

	/* mark the begin of ROOT object and start adding elements to it. */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	/* Start constructing fmt string */
	appendStringInfoString(&fmtStr, "DROP TABLE");

	/* CONCURRENTLY */
	if (node->concurrent)
	{
		appendStringInfoString(&fmtStr, " %{concurrently}s");
		new_jsonb_VA(state, 1,
					 "concurrently", jbvString, "CONCURRENTLY");
	}

	/* IF EXISTS */
	if (node->missing_ok)
	{
		appendStringInfoString(&fmtStr, " %{if_exists}s");
		new_jsonb_VA(state, 1, "if_exists", jbvString, "IF EXISTS");
	}

	/* IDENTITY */
	appendStringInfoString(&fmtStr, " %{objidentity}s");
	new_jsonb_VA(state, 1, "objidentity", jbvString, objidentity);

	/* CASCADE */
	if (node->behavior == DROP_CASCADE)
	{
		appendStringInfoString(&fmtStr, " %{cascade}s");
		new_jsonb_VA(state, 1, "cascade", jbvString, "CASCADE");
	}

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);
	pfree(fmtStr.data);

	jsonbval = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	jsonb = JsonbValueToJsonb(jsonbval);
	return JsonbToCString(&str, &jsonb->root, JSONB_ESTIMATED_LEN);
}

/*
 * Deparse all the collected subcommands and return jsonb string representing
 * the alter command.
 *
 * Verbose syntax
 * ALTER TABLE %{only}s %{identity}D %{subcmds:, }s
 */
static Jsonb *
deparse_AlterTableStmt(CollectedCommand *cmd)
{
	List	   *dpcontext;
	Relation	rel;
	ListCell   *cell;
	Oid			relId = cmd->d.alterTable.objectId;
	AlterTableStmt *stmt = NULL;
	StringInfoData fmtStr;
	JsonbParseState *state = NULL;
	bool		subCmdArray = false;
	JsonbValue *value;

	Assert(cmd->type == SCT_AlterTable);
	stmt = (AlterTableStmt *) cmd->parsetree;

	Assert(IsA(stmt, AlterTableStmt) || IsA(stmt, AlterTableMoveAllStmt));

	initStringInfo(&fmtStr);

	/*
	 * ALTER TABLE subcommands generated for TableLikeClause is processed in
	 * the top level CREATE TABLE command; return empty here.
	 */
	if (IsA(stmt, AlterTableStmt) && stmt->table_like)
		return NULL;

	rel = relation_open(relId, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		/* unsupported relkind */
		table_close(rel, AccessShareLock);
		return NULL;
	}

	dpcontext = deparse_context_for(RelationGetRelationName(rel), relId);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	/* Start constructing fmt string */
	appendStringInfoString(&fmtStr, "ALTER TABLE");

	if (!stmt->relation->inh)
	{
		appendStringInfoString(&fmtStr, " %{only}s");
		new_jsonb_VA(state, 1, "only", jbvString, "ONLY");
	}

	appendStringInfoString(&fmtStr, " %{identity}D");
	insert_identity_object(state, rel->rd_rel->relnamespace,
						   RelationGetRelationName(rel));

	foreach(cell, cmd->d.alterTable.subcmds)
	{
		CollectedATSubcmd *sub = (CollectedATSubcmd *) lfirst(cell);
		AlterTableCmd *subcmd = (AlterTableCmd *) sub->parsetree;

		Assert(IsA(subcmd, AlterTableCmd));

		/*
		 * Skip deparse of the subcommand if the objectId doesn't match the
		 * target relation ID. It can happen for inherited tables when
		 * subcommands for inherited tables and the parent table are both
		 * collected in the ALTER TABLE command for the parent table.
		 */
		if (subcmd->subtype != AT_AttachPartition &&
			sub->address.objectId != relId &&
			has_superclass(sub->address.objectId))
			continue;

		/* Mark the begin of subcmds array */
		if (!subCmdArray)
		{
			appendStringInfoString(&fmtStr, " %{subcmds:, }s");
			insert_jsonb_key(state, "subcmds");
			pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
			subCmdArray = true;
		}

		switch (subcmd->subtype)
		{
			case AT_AddColumn:
				{
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/* XXX need to set the "recurse" bit somewhere? */
					Assert(IsA(subcmd->def, ColumnDef));

					/*
					 * Syntax: ADD COLUMN %{if_not_exists}s %{definition}s"
					 * where definition: "%{name}I %{coltype}T STORAGE
					 * %{colstorage}s %{compression}s %{collation}s
					 * %{not_null}s %{default}s %{identity_column}s
					 * %{generated_column}s"
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					appendStringInfoString(&fmtSub, "ADD COLUMN");
					new_jsonb_VA(state, 1, "type", jbvString, "add column");

					if (subcmd->missing_ok)
					{
						appendStringInfoString(&fmtSub, " %{if_not_exists}s");
						new_jsonb_VA(state, 1,
									 "if_not_exists", jbvString, "IF NOT EXISTS");
					}

					/* Push definition key-value pair */
					appendStringInfoString(&fmtSub, " %{definition}s");
					insert_jsonb_key(state, "definition");

					deparse_ColumnDef(state, rel, dpcontext,
									  false, (ColumnDef *) subcmd->def,
									  true);

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					break;
				}

			case AT_AddIndexConstraint:
				{
					IndexStmt  *istmt;
					Relation	idx;
					Oid			conOid = sub->address.objectId;

					Assert(IsA(subcmd->def, IndexStmt));
					istmt = (IndexStmt *) subcmd->def;

					Assert(istmt->isconstraint && istmt->unique);

					idx = relation_open(istmt->indexOid, AccessShareLock);

					/*
					 * Syntax: ADD CONSTRAINT %{name}I %{constraint_type}s
					 * USING INDEX %index_name}I %{deferrable}s
					 * %{init_deferred}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					new_jsonb_VA(state, 7,
								 "fmt", jbvString,
								 "ADD CONSTRAINT %{name}I %{constraint_type}s"
								 " USING INDEX %{index_name}I %{deferrable}s"
								 " %{init_deferred}s",
								 "type", jbvString, "add constraint using index",
								 "name", jbvString, get_constraint_name(conOid),
								 "constraint_type", jbvString,
								 istmt->primary ? "PRIMARY KEY" : "UNIQUE",
								 "index_name", jbvString,
								 RelationGetRelationName(idx),
								 "deferrable", jbvString,
								 istmt->deferrable ? "DEFERRABLE" :
								 "NOT DEFERRABLE",
								 "init_deferred", jbvString,
								 istmt->initdeferred ? "INITIALLY DEFERRED" :
								 "INITIALLY IMMEDIATE");

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					relation_close(idx, AccessShareLock);
					break;
				}
			case AT_ReAddIndex:
			case AT_ReAddConstraint:
			case AT_ReAddDomainConstraint:
			case AT_ReAddComment:
			case AT_ReplaceRelOptions:
			case AT_CheckNotNull:
			case AT_ReAddStatistics:
				/* Subtypes used for internal operations; nothing to do here */
				break;

			case AT_ColumnDefault:
				if (subcmd->def == NULL)
				{
					/*
					 * Syntax: ALTER COLUMN %{column}I DROP DEFAULT
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					new_jsonb_VA(state, 3,
								 "fmt", jbvString,
								 "ALTER COLUMN %{column}I DROP DEFAULT",
								 "type", jbvString, "drop default",
								 "column", jbvString, subcmd->name);
					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				}
				else
				{
					List	   *dpcontext_rel;
					HeapTuple	attrtup;
					AttrNumber	attno;

					dpcontext_rel = deparse_context_for(
														RelationGetRelationName(rel),
														RelationGetRelid(rel));
					attrtup = SearchSysCacheAttName(RelationGetRelid(rel),
													subcmd->name);
					attno = ((Form_pg_attribute) GETSTRUCT(attrtup))->attnum;

					/*
					 * Syntax: ALTER COLUMN %{column}I SET DEFAULT
					 * %{definition}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					new_jsonb_VA(state, 4,
								 "fmt", jbvString,
								 "ALTER COLUMN %{column}I SET DEFAULT"
								 " %{definition}s",
								 "type", jbvString, "set default",
								 "column", jbvString, subcmd->name,
								 "definition", jbvString,
								 relation_get_column_default(rel, attno,
															 dpcontext_rel));

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					ReleaseSysCache(attrtup);
				}

				break;

			case AT_DropNotNull:

				/*
				 * Syntax: ALTER COLUMN %{column}I DROP NOT NULL
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString,
							 "ALTER COLUMN %{column}I DROP NOT NULL",
							 "type", jbvString, "drop not null",
							 "column", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_ForceRowSecurity:

				/*
				 * Syntax: FORCE ROW LEVEL SECURITY
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 1, "fmt", jbvString,
							 "FORCE ROW LEVEL SECURITY");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_NoForceRowSecurity:

				/*
				 * Syntax: NO FORCE ROW LEVEL SECURITY
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 1, "fmt", jbvString,
							 "NO FORCE ROW LEVEL SECURITY");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetNotNull:

				/*
				 * Syntax: ALTER COLUMN %{column}I SET NOT NULL
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString,
							 "ALTER COLUMN %{column}I SET NOT NULL",
							 "type", jbvString, "set not null",
							 "column", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DropExpression:
				{
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: ALTER COLUMN %{column}I DROP EXPRESSION
					 * %{if_exists}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					appendStringInfoString(&fmtSub, "ALTER COLUMN"
										   " %{column}I DROP EXPRESSION");
					new_jsonb_VA(state, 2,
								 "type", jbvString, "drop expression",
								 "column", jbvString, subcmd->name);

					if (subcmd->missing_ok)
					{
						appendStringInfoString(&fmtSub, " %{if_exists}s");
						new_jsonb_VA(state, 1,
									 "if_exists", jbvString, "IF EXISTS");
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					break;
				}

			case AT_SetStatistics:
				Assert(IsA(subcmd->def, Integer));

				/*
				 * Syntax: ALTER COLUMN %{column}I SET STATISTICS
				 * %{statistics}n
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 4,
							 "fmt", jbvString,
							 "ALTER COLUMN %{column}I SET STATISTICS"
							 " %{statistics}n",
							 "type", jbvString, "set statistics",
							 "column", jbvString, subcmd->name,
							 "statistics", jbvNumeric,
							 intVal((Integer *) subcmd->def));
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetOptions:
			case AT_ResetOptions:

				/*
				 * Syntax: ALTER COLUMN %{column}I RESET|SET (%{options:, }s)
				 */
				deparse_ColumnSetOptions(state, subcmd);
				break;

			case AT_SetStorage:
				Assert(IsA(subcmd->def, String));

				/*
				 * Syntax: ALTER COLUMN %{column}I SET STORAGE %{storage}s
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 4,
							 "fmt", jbvString,
							 "ALTER COLUMN %{column}I SET STORAGE"
							 " %{storage}s",
							 "type", jbvString, "set storage",
							 "column", jbvString, subcmd->name,
							 "storage", jbvString,
							 strVal((String *) subcmd->def));
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetCompression:
				Assert(IsA(subcmd->def, String));

				/*
				 * Syntax: ALTER COLUMN %{column}I SET COMPRESSION
				 * %{compression_method}s
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 4,
							 "fmt", jbvString,
							 "ALTER COLUMN %{column}I SET COMPRESSION"
							 " %{compression_method}s",
							 "type", jbvString, "set compression",
							 "column", jbvString, subcmd->name,
							 "compression_method", jbvString,
							 strVal((String *) subcmd->def));
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DropColumn:
				{
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: DROP COLUMN %{if_exists}s %{column}I
					 * %{cascade}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					appendStringInfoString(&fmtSub, "DROP COLUMN");
					new_jsonb_VA(state, 1, "type", jbvString, "drop column");

					if (subcmd->missing_ok)
					{
						appendStringInfoString(&fmtSub, " %{if_exists}s");
						new_jsonb_VA(state, 1,
									 "if_exists", jbvString, "IF EXISTS");
					}

					appendStringInfoString(&fmtSub, " %{column}I");
					new_jsonb_VA(state, 1, "column", jbvString, subcmd->name);

					if (subcmd->behavior == DROP_CASCADE)
					{
						appendStringInfoString(&fmtSub, " %{cascade}s");
						new_jsonb_VA(state, 1,
									 "cascade", jbvString, "CASCADE");
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);

					break;
				}
			case AT_AddIndex:
				{
					Oid			idxOid = sub->address.objectId;
					IndexStmt  *istmt PG_USED_FOR_ASSERTS_ONLY =
						(IndexStmt *) subcmd->def;
					Relation	idx;
					const char *idxname;
					Oid			constrOid;

					Assert(IsA(subcmd->def, IndexStmt));
					Assert(istmt->isconstraint);

					idx = relation_open(idxOid, AccessShareLock);
					idxname = RelationGetRelationName(idx);

					constrOid = get_relation_constraint_oid(
															cmd->d.alterTable.objectId,
															idxname, false);

					/*
					 * Syntax: ADD CONSTRAINT %{name}I %{definition}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					new_jsonb_VA(state, 4,
								 "fmt", jbvString,
								 "ADD CONSTRAINT %{name}I %{definition}s",
								 "type", jbvString, "add constraint",
								 "name", jbvString, idxname,
								 "definition", jbvString,
								 pg_get_constraintdef_string(constrOid));

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);

					relation_close(idx, AccessShareLock);
				}
				break;
			case AT_AddConstraint:
				{
					/* XXX need to set the "recurse" bit somewhere? */
					Oid			constrOid = sub->address.objectId;

					/* Skip adding constraint for inherits table sub command */
					if (!OidIsValid(constrOid))
						continue;

					Assert(IsA(subcmd->def, Constraint));

					/*
					 * Syntax: ADD CONSTRAINT %{name}I %{definition}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					new_jsonb_VA(state, 4,
								 "fmt", jbvString,
								 "ADD CONSTRAINT %{name}I %{definition}s",
								 "type", jbvString, "add constraint",
								 "name", jbvString, get_constraint_name(constrOid),
								 "definition", jbvString,
								 pg_get_constraintdef_string(constrOid));
					pushJsonbValue(&state, WJB_END_OBJECT, NULL);

					break;
				}

			case AT_AlterConstraint:
				{
					Oid			conOid = sub->address.objectId;
					Constraint *c = (Constraint *) subcmd->def;

					/* If no constraint was altered, silently skip it */
					if (!OidIsValid(conOid))
						break;

					Assert(IsA(c, Constraint));

					/*
					 * Syntax: ALTER CONSTRAINT %{name}I %{deferrable}s
					 * %{init_deferred}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					new_jsonb_VA(state, 5,
								 "fmt", jbvString,
								 "ALTER CONSTRAINT %{name}I %{deferrable}s"
								 " %{init_deferred}s",
								 "type", jbvString, "alter constraint",
								 "name", jbvString, get_constraint_name(conOid),
								 "deferrable", jbvString,
								 c->deferrable ? "DEFERRABLE" : "NOT DEFERRABLE",
								 "init_deferred", jbvString,
								 c->initdeferred ? "INITIALLY DEFERRED" :
								 "INITIALLY IMMEDIATE");
					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				}
				break;

			case AT_ValidateConstraint:

				/*
				 * Syntax: VALIDATE CONSTRAINT %{constraint}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString,
							 "VALIDATE CONSTRAINT %{constraint}I",
							 "type", jbvString, "validate constraint",
							 "constraint", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DropConstraint:
				{
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: DROP CONSTRAINT %{if_exists}s %{constraint}I
					 * %{cascade}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					appendStringInfoString(&fmtSub, "DROP CONSTRAINT");
					new_jsonb_VA(state, 1, "type", jbvString, "drop constraint");

					if (subcmd->missing_ok)
					{
						appendStringInfoString(&fmtSub, " %{if_exists}s");
						new_jsonb_VA(state, 1,
									 "if_exists", jbvString, "IF EXISTS");
					}

					appendStringInfoString(&fmtSub, " %{constraint}I");
					new_jsonb_VA(state, 1,
								 "constraint", jbvString, subcmd->name);

					if (subcmd->behavior == DROP_CASCADE)
					{
						appendStringInfoString(&fmtSub, " %{cascade}s");
						new_jsonb_VA(state, 1, "cascade", jbvString, "CASCADE");
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				}
				break;

			case AT_AlterColumnType:
				{
					TupleDesc	tupdesc = RelationGetDescr(rel);
					Form_pg_attribute att;
					ColumnDef  *def;
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					att = &(tupdesc->attrs[sub->address.objectSubId - 1]);
					def = (ColumnDef *) subcmd->def;
					Assert(IsA(def, ColumnDef));

					/*
					 * Syntax: ALTER COLUMN %{column}I SET DATA TYPE
					 * %{datatype}T %{collation}s %{using}s where using: USING
					 * %{expression}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					appendStringInfoString(&fmtSub, "ALTER COLUMN %{column}I"
										   " SET DATA TYPE %{datatype}T");
					new_jsonb_VA(state, 2,
								 "type", jbvString, "alter column type",
								 "column", jbvString, subcmd->name);

					new_jsonb_for_type(state, "datatype",
									   att->atttypid, att->atttypmod);

					/* Add a COLLATE clause, if needed */
					if (OidIsValid(att->attcollation))
					{
						appendStringInfoString(&fmtSub, " %{collation}s");
						insert_collate_object(state, "collation",
											  "COLLATE %{name}D",
											  CollationRelationId,
											  att->attcollation, "name");
					}

					/*
					 * If there's a USING clause, transformAlterTableStmt ran
					 * it through transformExpr and stored the resulting node
					 * in cooked_default, which we can use here.
					 */
					if (def->raw_default)
					{
						appendStringInfoString(&fmtSub, " %{using}s");
						insert_jsonb_key(state, "using");
						pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
						new_jsonb_VA(state, 2,
									 "fmt", jbvString, "USING %{expression}s",
									 "expression", jbvString,
									 sub->usingexpr);
						pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);

					pfree(fmtSub.data);
					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				}
				break;

			case AT_ChangeOwner:

				/*
				 * Syntax: OWNER TO %{owner}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "OWNER TO %{owner}I",
							 "type", jbvString, "change owner",
							 "owner", jbvString,
							 get_rolespec_name(subcmd->newowner));
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_ClusterOn:

				/*
				 * Syntax: CLUSTER ON %{index}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "CLUSTER ON %{index}I",
							 "type", jbvString, "cluster on",
							 "index", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;


			case AT_DropCluster:

				/*
				 * Syntax: SET WITHOUT CLUSTER
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "SET WITHOUT CLUSTER",
							 "type", jbvString, "set without cluster");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetLogged:

				/*
				 * Syntax: SET LOGGED
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "SET LOGGED",
							 "type", jbvString, "set logged");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetUnLogged:

				/*
				 * Syntax: SET UNLOGGED
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "SET UNLOGGED",
							 "type", jbvString, "set unlogged");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DropOids:

				/*
				 * Syntax: SET WITHOUT OIDS
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "SET WITHOUT OIDS",
							 "type", jbvString, "set without oids");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetAccessMethod:

				/*
				 * Syntax: SET ACCESS METHOD %{access_method}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString,
							 "SET ACCESS METHOD %{access_method}I",
							 "type", jbvString, "set access method",
							 "access_method", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetTableSpace:

				/*
				 * Syntax: SET TABLESPACE %{tablespace}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "SET TABLESPACE %{tablespace}I",
							 "type", jbvString, "set tablespace",
							 "tablespace", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_SetRelOptions:
			case AT_ResetRelOptions:

				/*
				 * Syntax: SET|RESET (%{options:, }s)
				 */
				deparse_RelSetOptions(state, subcmd);
				break;

			case AT_EnableTrig:

				/*
				 * Syntax: ENABLE TRIGGER %{trigger}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "ENABLE TRIGGER %{trigger}I",
							 "type", jbvString, "enable trigger",
							 "trigger", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableAlwaysTrig:

				/*
				 * Syntax: ENABLE ALWAYS TRIGGER %{trigger}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString,
							 "ENABLE ALWAYS TRIGGER %{trigger}I",
							 "type", jbvString, "enable always trigger",
							 "trigger", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableReplicaTrig:

				/*
				 * Syntax: ENABLE REPLICA TRIGGER %{trigger}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString,
							 "ENABLE REPLICA TRIGGER %{trigger}I",
							 "type", jbvString, "enable replica trigger",
							 "trigger", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DisableTrig:

				/*
				 * Syntax: DISABLE TRIGGER %{trigger}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "DISABLE TRIGGER %{trigger}I",
							 "type", jbvString, "disable trigger",
							 "trigger", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableTrigAll:

				/*
				 * Syntax: ENABLE TRIGGER ALL
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "ENABLE TRIGGER ALL",
							 "type", jbvString, "enable trigger all");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DisableTrigAll:

				/*
				 * Syntax: DISABLE TRIGGER ALL
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "DISABLE TRIGGER ALL",
							 "type", jbvString, "disable trigger all");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableTrigUser:

				/*
				 * Syntax: ENABLE TRIGGER USER
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "ENABLE TRIGGER USER",
							 "type", jbvString, "enable trigger user");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DisableTrigUser:

				/*
				 * Syntax: DISABLE TRIGGER USER
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "DISABLE TRIGGER USER",
							 "type", jbvString, "disable trigger user");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableRule:

				/*
				 * Syntax: ENABLE RULE %{rule}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "ENABLE RULE %{rule}I",
							 "type", jbvString, "enable rule",
							 "rule", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableAlwaysRule:

				/*
				 * Syntax: ENABLE ALWAYS RULE %{rule}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "ENABLE ALWAYS RULE %{rule}I",
							 "type", jbvString, "enable always rule",
							 "rule", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableReplicaRule:

				/*
				 * Syntax: ENABLE REPLICA RULE %{rule}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "ENABLE REPLICA RULE %{rule}I",
							 "type", jbvString, "enable replica rule",
							 "rule", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DisableRule:

				/*
				 * Syntax: DISABLE RULE %{rule}I
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 3,
							 "fmt", jbvString, "DISABLE RULE %{rule}I",
							 "type", jbvString, "disable rule",
							 "rule", jbvString, subcmd->name);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_AddInherit:

				/*
				 * Syntax: INHERIT %{parent}D
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "INHERIT %{parent}D",
							 "type", jbvString, "inherit");
				new_jsonb_for_qualname_id(state, RelationRelationId,
										  sub->address.objectId, "parent", true);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DropInherit:

				/*
				 * Syntax: NO INHERIT %{parent}D
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "NO INHERIT %{parent}D",
							 "type", jbvString, "drop inherit");
				new_jsonb_for_qualname_id(state, RelationRelationId,
										  sub->address.objectId, "parent", true);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_AddOf:

				/*
				 * Syntax: OF %{type_of}T
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "OF %{type_of}T",
							 "type", jbvString, "add of");
				new_jsonb_for_type(state, "type_of", sub->address.objectId, -1);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DropOf:

				/*
				 * Syntax: NOT OF
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "NOT OF",
							 "type", jbvString, "not of");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_ReplicaIdentity:

				/*
				 * Syntax: REPLICA IDENTITY %{ident}s
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString,
							 "REPLICA IDENTITY %{ident}s",
							 "type", jbvString, "replica identity");
				switch (((ReplicaIdentityStmt *) subcmd->def)->identity_type)
				{
					case REPLICA_IDENTITY_DEFAULT:
						new_jsonb_VA(state, 1, "ident", jbvString, "DEFAULT");
						break;
					case REPLICA_IDENTITY_FULL:
						new_jsonb_VA(state, 1, "ident", jbvString, "FULL");
						break;
					case REPLICA_IDENTITY_NOTHING:
						new_jsonb_VA(state, 1, "ident", jbvString, "NOTHING");
						break;
					case REPLICA_IDENTITY_INDEX:
						insert_jsonb_key(state, "ident");
						new_jsonb_VA(state, 2,
									 "fmt", jbvString, "USING INDEX %{index}I",
									 "index", jbvString,
									 ((ReplicaIdentityStmt *) subcmd->def)->name);
						break;
				}
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_EnableRowSecurity:

				/*
				 * Syntax: ENABLE ROW LEVEL SECURITY
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString,
							 "ENABLE ROW LEVEL SECURITY",
							 "type", jbvString, "enable row security");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_DisableRowSecurity:

				/*
				 * Syntax: DISABLE ROW LEVEL SECURITY
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString,
							 "DISABLE ROW LEVEL SECURITY",
							 "type", jbvString, "disable row security");
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;

			case AT_AttachPartition:
				{
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: ATTACH PARTITION %{partition_identity}D
					 * %{partition_bound}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					appendStringInfoString(&fmtSub, "ATTACH PARTITION"
										   " %{partition_identity}D");

					new_jsonb_VA(state, 1, "type", jbvString,
								 "attach partition");
					new_jsonb_for_qualname_id(state, RelationRelationId,
											  sub->address.objectId,
											  "partition_identity", true);

					if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
					{
						appendStringInfoString(&fmtSub, " %{partition_bound}s");
						new_jsonb_VA(state, 1,
									 "partition_bound", jbvString,
									 relation_get_part_bound(sub->address.objectId));
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);

					break;
				}
			case AT_DetachPartition:
				{
					PartitionCmd *cmd;
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					Assert(IsA(subcmd->def, PartitionCmd));
					cmd = (PartitionCmd *) subcmd->def;

					/*
					 * Syntax: DETACH PARTITION %{partition_identity}D
					 * %{concurrent}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					appendStringInfoString(&fmtSub, "DETACH PARTITION"
										   " %{partition_identity}D");

					new_jsonb_VA(state, 1, "type", jbvString, "detach partition");
					new_jsonb_for_qualname_id(state, RelationRelationId,
											  sub->address.objectId,
											  "partition_identity", true);
					if (cmd->concurrent)
					{
						appendStringInfoString(&fmtSub, " %{concurrent}s");
						new_jsonb_VA(state, 1,
									 "concurrent", jbvString, "CONCURRENTLY");
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					break;
				}
			case AT_DetachPartitionFinalize:

				/*
				 * Syntax: DETACH PARTITION %{partition_identity}D FINALIZE
				 */
				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 2,
							 "fmt", jbvString, "DETACH PARTITION"
							 " %{partition_identity}D FINALIZE",
							 "type", jbvString, "detach partition finalize");

				new_jsonb_for_qualname_id(state, RelationRelationId,
										  sub->address.objectId,
										  "partition_identity", true);
				pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				break;
			case AT_AddIdentity:
				{
					AttrNumber	attnum;
					Oid			seq_relid;
					ColumnDef  *coldef = (ColumnDef *) subcmd->def;
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: ALTER COLUMN %{column}I %{definition}s where
					 * definition : ADD %{identity_column}s where
					 * identity_column: %{identity_type}s ( %{seq_definition:
					 * }s )
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					appendStringInfoString(&fmtSub, "ALTER COLUMN %{column}I");
					new_jsonb_VA(state, 2,
								 "type", jbvString, "add identity",
								 "column", jbvString, subcmd->name);

					attnum = get_attnum(RelationGetRelid(rel), subcmd->name);
					seq_relid = getIdentitySequence(RelationGetRelid(rel),
													attnum, true);

					if (OidIsValid(seq_relid))
					{

						appendStringInfoString(&fmtSub, " %{definition}s");
						insert_jsonb_key(state, "definition");

						/* insert definition's value now */
						pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
						new_jsonb_VA(state, 1,
									 "fmt", jbvString, "ADD %{identity_column}s");

						/* insert identity_column */
						deparse_ColumnIdentity(state, "identity_column",
											   seq_relid,
											   coldef->identity, false);

						/* mark definition's value end */
						pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);

					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
				}
				break;
			case AT_SetIdentity:
				{
					DefElem    *defel;
					char		identity = 0;
					AttrNumber	attnum;
					Oid			seq_relid;
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: ALTER COLUMN %{column}I %{definition}s where
					 * definition : %{identity_type}s ( %{seq_definition: }s )
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

					appendStringInfoString(&fmtSub, "ALTER COLUMN %{column}I");
					new_jsonb_VA(state, 2,
								 "type", jbvString, "set identity",
								 "column", jbvString, subcmd->name);

					if (subcmd->def)
					{
						List	   *def = (List *) subcmd->def;

						Assert(IsA(subcmd->def, List));

						defel = linitial_node(DefElem, def);
						identity = defGetInt32(defel);
					}

					attnum = get_attnum(RelationGetRelid(rel), subcmd->name);
					seq_relid = getIdentitySequence(RelationGetRelid(rel),
													attnum, true);

					if (OidIsValid(seq_relid))
					{
						appendStringInfoString(&fmtSub, " %{definition}s");
						deparse_ColumnIdentity(state, "definition",
											   seq_relid, identity,
											   true);
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);

					pfree(fmtSub.data);
					pushJsonbValue(&state, WJB_END_OBJECT, NULL);

					break;
				}
			case AT_DropIdentity:
				{
					StringInfoData fmtSub;

					initStringInfo(&fmtSub);

					/*
					 * Syntax: ALTER COLUMN %{column}I DROP IDENTITY
					 * %{if_exists}s
					 */
					pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
					appendStringInfoString(&fmtSub, "ALTER COLUMN"
										   " %{column}I DROP IDENTITY");
					new_jsonb_VA(state, 2,
								 "type", jbvString, "drop identity",
								 "column", jbvString, subcmd->name);

					if (subcmd->missing_ok)
					{
						appendStringInfoString(&fmtSub, " %{if_exists}s");
						new_jsonb_VA(state, 1,
									 "if_exists", jbvString, "IF EXISTS");
					}

					/* We have full fmt by now, so add jsonb element for that */
					new_jsonb_VA(state, 1, "fmt", jbvString, fmtSub.data);
					pfree(fmtSub.data);

					pushJsonbValue(&state, WJB_END_OBJECT, NULL);
					break;
				}
			default:
				elog(WARNING, "unsupported alter table subtype %d",
					 subcmd->subtype);
				break;
		}
	}

	table_close(rel, AccessShareLock);

	/* if subcmds array is not even created or has 0 elements, return NULL */
	if (!subCmdArray ||
		((state->contVal.type == jbvArray) &&
		 (state->contVal.val.array.nElems == 0)))
	{
		pfree(fmtStr.data);
		return NULL;
	}

	/* Mark the end of subcmds array */
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

	pfree(fmtStr.data);

	/* Mark the end of ROOT object */
	value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(value);
}

/*
 * Deparse a CreateSeqStmt.
 *
 * Given a sequence OID and the parse tree that created it, return Jsonb
 * representing the creation command.
 *
 * Note: We need to deparse the CREATE SEQUENCE command for the TABLE
 * commands. For example, When creating a table, if we specify a column as a
 * serial type, then we will create a sequence for that column and set that
 * sequence OWNED BY the table. The serial column type information is not
 * available during deparsing phase as that has already been converted to
 * the column default value and sequences creation while parsing.
 *
 * Verbose syntax
 * CREATE %{persistence}s SEQUENCE %{if_not_exists}s %{identity}D
 * %{definition: }s
 */
static Jsonb *
deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
{
	Relation	relation;
	Form_pg_sequence seqform;
	Sequence_values *seqvalues;
	CreateSeqStmt *createSeqStmt = (CreateSeqStmt *) parsetree;
	JsonbParseState *state = NULL;
	JsonbValue *value;
	StringInfoData fmtStr;
	char	   *perstr;

	/*
	 * Only support sequence for IDENTITY COLUMN output separately (via CREATE
	 * TABLE or ALTER TABLE). Otherwise, return empty here.
	 */
	if (createSeqStmt->for_identity)
		return NULL;

	initStringInfo(&fmtStr);
	relation = relation_open(objectId, AccessShareLock);

	/* mark the start of ROOT object */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	appendStringInfoString(&fmtStr, "CREATE");

	/* PERSISTENCE */
	perstr = get_persistence_str(relation->rd_rel->relpersistence);
	if (perstr)
	{
		appendStringInfoString(&fmtStr, " %{persistence}s");
		new_jsonb_VA(state, 1,
					 "persistence", jbvString, perstr);
	}

	appendStringInfoString(&fmtStr, " SEQUENCE");

	/* IF NOT EXISTS */
	if (createSeqStmt->if_not_exists)
	{
		appendStringInfoString(&fmtStr, " %{if_not_exists}s");
		new_jsonb_VA(state, 1,
					 "if_not_exists", jbvString, "IF NOT EXISTS");
	}

	/* IDENTITY */
	appendStringInfoString(&fmtStr, " %{identity}D");
	insert_identity_object(state, relation->rd_rel->relnamespace,
						   RelationGetRelationName(relation));

	relation_close(relation, AccessShareLock);

	seqvalues = get_sequence_values(objectId);
	seqform = seqvalues->seqform;

	/* sequence definition array object creation, push the key first */
	appendStringInfoString(&fmtStr, " %{definition: }s");
	insert_jsonb_key(state, "definition");

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	/* Definition elements */
	deparse_Seq_Cache(state, seqform, false);
	deparse_Seq_Cycle(state, seqform, false);
	deparse_Seq_IncrementBy(state, seqform, false);
	deparse_Seq_Minvalue(state, seqform, false);
	deparse_Seq_Maxvalue(state, seqform, false);
	deparse_Seq_Startwith(state, seqform, false);
	deparse_Seq_Restart(state, seqvalues->last_value);
	deparse_Seq_As(state, seqform);
	/* We purposefully do not emit OWNED BY here */

	/* mark the end of sequence definition array */
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	/* We have full fmt by now, so add jsonb element for that */
	new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

	pfree(fmtStr.data);

	/* mark the end of ROOT object */
	value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(value);
}

/*
 * Deparse an AlterSeqStmt.
 *
 * Given a sequence OID and a parse tree that modified it, return Jsonb
 * representing the alter command.
 *
 * Note: We need to deparse the ALTER SEQUENCE command for the TABLE commands.
 * For example, When creating a table, if we specify a column as a serial
 * type, then we will create a sequence for that column and set that sequence
 * OWNED BY the table. The serial column type information is not available
 * during deparsing phase as that has already been converted to the column
 * default value and sequences creation while parsing.
 *
 * Verbose syntax
 * ALTER SEQUENCE %{identity}D %{definition: }s
 */
static Jsonb *
deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
{
	Relation	relation;
	ListCell   *cell;
	Form_pg_sequence seqform;
	Sequence_values *seqvalues;
	AlterSeqStmt *alterSeqStmt = (AlterSeqStmt *) parsetree;
	JsonbParseState *state = NULL;
	JsonbValue *value;

	/*
	 * Sequence for IDENTITY COLUMN output separately (via CREATE TABLE or
	 * ALTER TABLE); return empty here.
	 */
	if (alterSeqStmt->for_identity)
		return NULL;

	relation = relation_open(objectId, AccessShareLock);

	/* mark the start of ROOT object */
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	new_jsonb_VA(state, 1,
				 "fmt", jbvString, "ALTER SEQUENCE %{identity}D %{definition: }s");

	insert_identity_object(state, relation->rd_rel->relnamespace,
						   RelationGetRelationName(relation));
	relation_close(relation, AccessShareLock);

	seqvalues = get_sequence_values(objectId);
	seqform = seqvalues->seqform;

	/* sequence definition array object creation, push the key first */
	insert_jsonb_key(state, "definition");

	/* mark the start of sequence definition array */
	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	foreach(cell, ((AlterSeqStmt *) parsetree)->options)
	{
		DefElem    *elem = (DefElem *) lfirst(cell);

		if (strcmp(elem->defname, "cache") == 0)
			deparse_Seq_Cache(state, seqform, false);
		else if (strcmp(elem->defname, "cycle") == 0)
			deparse_Seq_Cycle(state, seqform, false);
		else if (strcmp(elem->defname, "increment") == 0)
			deparse_Seq_IncrementBy(state, seqform, false);
		else if (strcmp(elem->defname, "minvalue") == 0)
			deparse_Seq_Minvalue(state, seqform, false);
		else if (strcmp(elem->defname, "maxvalue") == 0)
			deparse_Seq_Maxvalue(state, seqform, false);
		else if (strcmp(elem->defname, "start") == 0)
			deparse_Seq_Startwith(state, seqform, false);
		else if (strcmp(elem->defname, "restart") == 0)
			deparse_Seq_Restart(state, seqvalues->last_value);
		else if (strcmp(elem->defname, "owned_by") == 0)
			deparse_Seq_OwnedBy(state, objectId, false);
		else if (strcmp(elem->defname, "as") == 0)
			deparse_Seq_As(state, seqform);
		else
			elog(ERROR, "invalid sequence option %s", elem->defname);
	}

	/* mark the end of sequence definition array */
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	/* mark the end of ROOT object */
	value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(value);
}

/*
 * Deparse a RenameStmt.
 *
 * Verbose syntax
 * ALTER TABLE %{if_exists}s %{identity}D RENAME TO %{newname}I
 * OR
 * ALTER TABLE %{only}s %{identity}D RENAME CONSTRAINT %{oldname}I
 * TO %{newname}I
 * OR
 * ALTER %{objtype}s %{if_exists}s %{only}s %{identity}D RENAME COLUMN
 * %{colname}I TO %{newname}I %{cascade}s
 */

static Jsonb *
deparse_RenameStmt(ObjectAddress address, Node *parsetree)
{
	RenameStmt *node = (RenameStmt *) parsetree;
	Relation	relation;
	Oid			schemaId;
	JsonbParseState *state = NULL;
	JsonbValue *value;

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
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);

			pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
			new_jsonb_VA(state, 3,
						 "fmt", jbvString,
						 "ALTER TABLE %{if_exists}s %{identity}D"
						 " RENAME TO %{newname}I",
						 "if_exists", jbvString,
						 node->missing_ok ? "IF EXISTS" : "",
						 "newname", jbvString, node->newname);

			insert_identity_object(state, schemaId, node->relation->relname);
			value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

			relation_close(relation, AccessShareLock);
			break;

		case OBJECT_TABCONSTRAINT:
			{
				HeapTuple	constrtup;
				Form_pg_constraint constform;

				constrtup = SearchSysCache1(CONSTROID,
											ObjectIdGetDatum(address.objectId));
				if (!HeapTupleIsValid(constrtup))
					elog(ERROR, "cache lookup failed for constraint with OID %u",
						 address.objectId);
				constform = (Form_pg_constraint) GETSTRUCT(constrtup);

				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				new_jsonb_VA(state, 4,
							 "fmt", jbvString,
							 "ALTER TABLE %{only}s %{identity}D RENAME"
							 " CONSTRAINT %{oldname}I TO %{newname}I",
							 "only", jbvString,
							 node->relation->inh ? "" : "ONLY",
							 "oldname", jbvString, node->subname,
							 "newname", jbvString, node->newname);

				new_jsonb_for_qualname_id(state, RelationRelationId,
										  constform->conrelid, "identity", true);
				value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

				ReleaseSysCache(constrtup);
			}
			break;

		case OBJECT_COLUMN:
			{
				StringInfoData fmtStr;

				initStringInfo(&fmtStr);

				relation = relation_open(address.objectId, AccessShareLock);
				schemaId = RelationGetNamespace(relation);

				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

				appendStringInfoString(&fmtStr, "ALTER %{objtype}s");

				new_jsonb_VA(state, 1,
							 "objtype", jbvString,
							 stringify_objtype(node->relationType));

				/* Composite types do not support IF EXISTS */
				if (node->renameType == OBJECT_COLUMN)
				{
					appendStringInfoString(&fmtStr, " %{if_exists}s");
					new_jsonb_VA(state, 1,
								 "if_exists", jbvString,
								 node->missing_ok ? "IF EXISTS" : "");
				}

				if (!node->relation->inh)
				{
					appendStringInfoString(&fmtStr, " %{only}s");
					new_jsonb_VA(state, 1, "only", jbvString, "ONLY");
				}

				appendStringInfoString(&fmtStr, " %{identity}D RENAME COLUMN"
									   " %{colname}I TO %{newname}I");
				insert_identity_object(state, schemaId, node->relation->relname);
				new_jsonb_VA(state, 2,
							 "colname", jbvString, node->subname,
							 "newname", jbvString, node->newname);

				if (node->renameType == OBJECT_ATTRIBUTE)
				{

					if (node->behavior == DROP_CASCADE)
					{
						appendStringInfoString(&fmtStr, " %{cascade}s");
						new_jsonb_VA(state, 1, "cascade", jbvString, "CASCADE");
					}
				}

				/* We have full fmt by now, so add jsonb element for that */
				new_jsonb_VA(state, 1, "fmt", jbvString, fmtStr.data);

				pfree(fmtStr.data);

				value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

				relation_close(relation, AccessShareLock);
				break;
			}

		default:
			elog(ERROR, "unsupported object type %d", node->renameType);
	}

	return JsonbValueToJsonb(value);
}

/*
 * Deparse an AlterObjectSchemaStmt (ALTER TABLE... SET SCHEMA command)
 *
 * Given the object(table) address and the parse tree that created it, return
 * Jsonb representing the alter command.
 *
 * Verbose syntax
 * ALTER %{objtype}s %{identity}s SET SCHEMA %{newschema}I
 */
static Jsonb *
deparse_AlterObjectSchemaStmt(ObjectAddress address, Node *parsetree,
							  ObjectAddress old_schema)
{
	AlterObjectSchemaStmt *node = (AlterObjectSchemaStmt *) parsetree;
	char	   *identity;
	char	   *new_schema = node->newschema;
	char	   *old_schname;
	char	   *ident;
	JsonbParseState *state = NULL;
	JsonbValue *value;

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

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	new_jsonb_VA(state, 4,
				 "fmt", jbvString,
				 "ALTER %{objtype}s %{identity}s SET SCHEMA"
				 " %{newschema}I",
				 "objtype", jbvString,
				 stringify_objtype(node->objectType),
				 "identity", jbvString, ident,
				 "newschema", jbvString, new_schema);
	value = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(value);
}

/*
 * Handle deparsing of simple commands.
 *
 * This function should cover all cases handled in ProcessUtilitySlow.
 */
static Jsonb *
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
		case T_AlterObjectSchemaStmt:
			return deparse_AlterObjectSchemaStmt(cmd->d.simple.address,
												 parsetree,
												 cmd->d.simple.secondaryObject);

		case T_AlterSeqStmt:
			return deparse_AlterSeqStmt(objectId, parsetree);

		case T_CreateSeqStmt:
			return deparse_CreateSeqStmt(objectId, parsetree);

		case T_CreateStmt:
			return deparse_CreateStmt(objectId, parsetree);
		case T_RenameStmt:
			return deparse_RenameStmt(cmd->d.simple.address, parsetree);

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
deparse_utility_command(CollectedCommand *cmd)
{
	OverrideSearchPath *overridePath;
	MemoryContext oldcxt;
	MemoryContext tmpcxt;
	char	   *command = NULL;
	StringInfoData str;
	Jsonb	   *jsonb;

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
	 * environments with different search_path settings.  Rather than
	 * injecting what would be repetitive calls to override search path all
	 * over the place, we do it centrally here.
	 */
	overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = false;
	overridePath->addTemp = true;
	PushOverrideSearchPath(overridePath);

	switch (cmd->type)
	{
		case SCT_Simple:
			jsonb = deparse_simple_command(cmd);
			break;
		case SCT_AlterTable:
			jsonb = deparse_AlterTableStmt(cmd);
			break;
		default:
			elog(ERROR, "unexpected deparse node type %d", cmd->type);
	}

	PopOverrideSearchPath();

	if (jsonb)
		command = JsonbToCString(&str, &jsonb->root, JSONB_ESTIMATED_LEN);

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

	command = deparse_utility_command(cmd);

	if (command)
		PG_RETURN_TEXT_P(cstring_to_text(command));

	PG_RETURN_NULL();
}
