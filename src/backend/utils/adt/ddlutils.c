/*-------------------------------------------------------------------------
 *
 * ddlutils.c
 *		Utility functions for generating DDL statements
 *
 * This file contains the pg_get_*_ddl family of functions that generate
 * DDL statements to recreate database objects such as roles, tablespaces,
 * databases, and tables, along with common infrastructure for
 * pretty-printing.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ddlutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/toast_compression.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_am.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

/*
 * Object-class kinds that the only / except options on
 * pg_get_table_ddl can filter on.  Members are stored as integers in a
 * Bitmapset on TableDdlContext.  Keep table_ddl_kind_names[] in sync
 * with the order of additions here.
 */
typedef enum TableDdlKind
{
	TABLE_DDL_KIND_TABLE,
	TABLE_DDL_KIND_INDEX,
	TABLE_DDL_KIND_PRIMARY_KEY,
	TABLE_DDL_KIND_UNIQUE,
	TABLE_DDL_KIND_CHECK,
	TABLE_DDL_KIND_FOREIGN_KEY,
	TABLE_DDL_KIND_EXCLUSION,
	TABLE_DDL_KIND_RULE,
	TABLE_DDL_KIND_STATISTICS,
	TABLE_DDL_KIND_TRIGGER,
	TABLE_DDL_KIND_POLICY,
	TABLE_DDL_KIND_RLS,
	TABLE_DDL_KIND_REPLICA_IDENTITY,
	TABLE_DDL_KIND_PARTITION,
}			TableDdlKind;

static const struct
{
	const char *name;
	TableDdlKind kind;
}			table_ddl_kind_names[] =
{
	{"table", TABLE_DDL_KIND_TABLE},
	{"index", TABLE_DDL_KIND_INDEX},
	{"primary_key", TABLE_DDL_KIND_PRIMARY_KEY},
	{"unique", TABLE_DDL_KIND_UNIQUE},
	{"check", TABLE_DDL_KIND_CHECK},
	{"foreign_key", TABLE_DDL_KIND_FOREIGN_KEY},
	{"exclusion", TABLE_DDL_KIND_EXCLUSION},
	{"rule", TABLE_DDL_KIND_RULE},
	{"statistics", TABLE_DDL_KIND_STATISTICS},
	{"trigger", TABLE_DDL_KIND_TRIGGER},
	{"policy", TABLE_DDL_KIND_POLICY},
	{"rls", TABLE_DDL_KIND_RLS},
	{"replica_identity", TABLE_DDL_KIND_REPLICA_IDENTITY},
	{"partition", TABLE_DDL_KIND_PARTITION},
};

static Bitmapset *parse_kind_array(const char *paramname, ArrayType *arr);
static void append_ddl_option(StringInfo buf, bool pretty, int indent,
							  const char *fmt, ...)
			pg_attribute_printf(4, 5);
static void append_guc_value(StringInfo buf, const char *name,
							 const char *value);
static List *pg_get_role_ddl_internal(Oid roleid, bool pretty,
									  bool memberships);
static List *pg_get_tablespace_ddl_internal(Oid tsid, bool pretty, bool no_owner);
static Datum pg_get_tablespace_ddl_srf(FunctionCallInfo fcinfo, Oid tsid);
static List *pg_get_database_ddl_internal(Oid dbid, bool pretty,
										  bool no_owner, bool no_tablespace);

/*
 * Per-column cache of locally-declared NOT NULL constraints.  Built once
 * by collect_local_not_null() and consulted by the column-emit helpers
 * and the post-CREATE constraint loop.  Entries with conoid == InvalidOid
 * mean the column has no local NOT NULL constraint row in pg_constraint
 * (the column may still have attnotnull=true on a pre-PG-18-upgraded
 * catalog, in which case emit plain inline NOT NULL).
 */
typedef struct LocalNotNullEntry
{
	Oid			conoid;
	char	   *name;
	bool		is_auto;		/* matches "<table>_<col>_not_null" */
	bool		no_inherit;
}			LocalNotNullEntry;

/*
 * Working context threaded through the per-pass helpers below.  Inputs
 * (caller-provided option flags) are filled in once at the top of
 * pg_get_table_ddl_internal and treated as read-only thereafter.  Derived
 * fields (qualname, nn_entries, skip_notnull_oids) are computed once
 * during setup.  Each pass appends to ctx->buf and pushes the finished
 * statement onto ctx->statements via append_stmt().
 */
typedef struct TableDdlContext
{
	Relation	rel;
	Oid			relid;
	bool		pretty;
	bool		no_owner;
	bool		no_tablespace;
	bool		schema_qualified;

	/*
	 * Object-class filtering.  If only_kinds is non-NULL, only the
	 * kinds in that set are emitted; if except_kinds is non-NULL, all
	 * kinds *except* those in that set are emitted; if both are NULL,
	 * every kind is emitted (the default).  The two are mutually
	 * exclusive at the user-facing layer (the SRF entry rejects
	 * specifying both).
	 */
	Bitmapset  *only_kinds;
	Bitmapset  *except_kinds;

	/* Derived during setup */
	Oid			base_namespace;
	char	   *qualname;
	int			save_nestlevel; /* >= 0 if we narrowed search_path */
	LocalNotNullEntry *nn_entries;
	List	   *skip_notnull_oids;

	/* Mutable working state */
	StringInfoData buf;
	List	   *statements;
}			TableDdlContext;

static List *pg_get_table_ddl_internal(TableDdlContext *ctx);
static bool is_kind_included(const TableDdlContext *ctx, TableDdlKind kind);
static void append_column_defs(StringInfo buf, Relation rel, bool pretty,
							   bool include_check,
							   bool schema_qualified,
							   LocalNotNullEntry *nn_entries);
static void append_typed_column_overrides(StringInfo buf, Relation rel,
										  bool pretty, bool include_check,
										  LocalNotNullEntry *nn_entries);
static void append_inline_check_constraints(StringInfo buf, Relation rel,
											bool pretty, bool *first);
static char *find_attrdef_text(Relation rel, AttrNumber attnum,
							   List **dpcontext);
static char *lookup_relname_for_emit(Oid relid, bool schema_qualified,
									 Oid base_namespace);
static LocalNotNullEntry *collect_local_not_null(Relation rel,
												  List **skip_oids);

static void append_stmt(TableDdlContext *ctx);
static void emit_create_table_stmt(TableDdlContext *ctx);
static void emit_owner_stmt(TableDdlContext *ctx);
static void emit_child_default_overrides(TableDdlContext *ctx);
static void emit_attoptions(TableDdlContext *ctx);
static void emit_indexes(TableDdlContext *ctx);
static void emit_local_constraints(TableDdlContext *ctx);
static void emit_rules(TableDdlContext *ctx);
static void emit_statistics(TableDdlContext *ctx);
static void emit_replica_identity(TableDdlContext *ctx);
static void emit_rls_toggles(TableDdlContext *ctx);
static void emit_partition_children(TableDdlContext *ctx);


/*
 * parse_kind_array
 *		Parse a text[] of object-class kind names into a Bitmapset of
 *		TableDdlKind values.
 *
 * Each element is matched case-insensitively; surrounding whitespace is
 * stripped.  NULL elements are rejected.  Unknown kind names raise an error
 * citing the supplied parameter name.  An empty array is also rejected.
 * Duplicate entries are silently de-duplicated by the Bitmapset.
 */
static Bitmapset *
parse_kind_array(const char *paramname, ArrayType *arr)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	Bitmapset  *result = NULL;

	deconstruct_array_builtin(arr, TEXTOID, &elems, &nulls, &nelems);

	if (nelems == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter \"%s\" must specify at least one kind",
						paramname)));

	for (int i = 0; i < nelems; i++)
	{
		char	   *raw;
		char	   *token;
		char	   *end;
		bool		found = false;

		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("parameter \"%s\" must not contain NULL elements",
							paramname)));

		raw = text_to_cstring(DatumGetTextPP(elems[i]));

		/* Trim leading whitespace. */
		token = raw;
		while (*token == ' ' || *token == '\t')
			token++;

		/* Trim trailing whitespace. */
		end = token + strlen(token);
		while (end > token && (end[-1] == ' ' || end[-1] == '\t'))
			end--;
		*end = '\0';

		for (size_t j = 0; j < lengthof(table_ddl_kind_names); j++)
		{
			if (pg_strcasecmp(token, table_ddl_kind_names[j].name) == 0)
			{
				result = bms_add_member(result,
										(int) table_ddl_kind_names[j].kind);
				found = true;
				break;
			}
		}

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized kind \"%s\" in parameter \"%s\"",
							token, paramname)));

		pfree(raw);
	}

	return result;
}

/*
 * is_kind_included
 *		Determine whether DDL for the given object-class kind should be
 *		emitted under the current options.
 */
static bool
is_kind_included(const TableDdlContext *ctx, TableDdlKind kind)
{
	if (ctx->only_kinds != NULL)
		return bms_is_member((int) kind, ctx->only_kinds);
	if (ctx->except_kinds != NULL)
		return !bms_is_member((int) kind, ctx->except_kinds);
	return true;
}

/*
 * Helper to append a formatted string with optional pretty-printing.
 */
static void
append_ddl_option(StringInfo buf, bool pretty, int indent,
				  const char *fmt, ...)
{
	if (pretty)
	{
		appendStringInfoChar(buf, '\n');
		appendStringInfoSpaces(buf, indent);
	}
	else
		appendStringInfoChar(buf, ' ');

	for (;;)
	{
		va_list		args;
		int			needed;

		va_start(args, fmt);
		needed = appendStringInfoVA(buf, fmt, args);
		va_end(args);
		if (needed == 0)
			break;
		enlargeStringInfo(buf, needed);
	}
}

/*
 * append_guc_value
 *		Append a GUC setting value to buf, handling GUC_LIST_QUOTE properly.
 *
 * Variables marked GUC_LIST_QUOTE were already fully quoted before they
 * were stored in the setconfig array.  We break the list value apart
 * and re-quote the elements as string literals.  For all other variables
 * we simply quote the value as a single string literal.
 *
 * The caller has already appended "SET <name> TO " to buf.
 */
static void
append_guc_value(StringInfo buf, const char *name, const char *value)
{
	char	   *rawval;

	rawval = pstrdup(value);

	if (GetConfigOptionFlags(name, true) & GUC_LIST_QUOTE)
	{
		List	   *namelist;
		bool		first = true;

		/* Parse string into list of identifiers */
		if (!SplitGUCList(rawval, ',', &namelist))
		{
			/* this shouldn't fail really */
			elog(ERROR, "invalid list syntax in setconfig item");
		}
		/* Special case: represent an empty list as NULL */
		if (namelist == NIL)
			appendStringInfoString(buf, "NULL");
		foreach_ptr(char, curname, namelist)
		{
			if (first)
				first = false;
			else
				appendStringInfoString(buf, ", ");
			appendStringInfoString(buf, quote_literal_cstr(curname));
		}
		list_free(namelist);
	}
	else
		appendStringInfoString(buf, quote_literal_cstr(rawval));

	pfree(rawval);
}

/*
 * pg_get_role_ddl_internal
 *		Generate DDL statements to recreate a role
 *
 * Returns a List of palloc'd strings, each being a complete SQL statement.
 * The first list element is always the CREATE ROLE statement; subsequent
 * elements are ALTER ROLE SET statements for any role-specific or
 * role-in-database configuration settings.  If memberships is true,
 * GRANT statements for role memberships are appended.
 */
static List *
pg_get_role_ddl_internal(Oid roleid, bool pretty, bool memberships)
{
	HeapTuple	tuple;
	Form_pg_authid roleform;
	StringInfoData buf;
	char	   *rolname;
	Datum		rolevaliduntil;
	bool		isnull;
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	List	   *statements = NIL;

	tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role with OID %u does not exist", roleid)));

	roleform = (Form_pg_authid) GETSTRUCT(tuple);
	rolname = pstrdup(NameStr(roleform->rolname));

	/* User must have SELECT privilege on pg_authid. */
	if (pg_class_aclcheck(AuthIdRelationId, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for role %s", rolname)));
	}

	/*
	 * We don't support generating DDL for system roles.  The primary reason
	 * for this is that users shouldn't be recreating them.
	 */
	if (IsReservedName(rolname))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("role name \"%s\" is reserved", rolname),
				 errdetail("Role names starting with \"pg_\" are reserved for system roles.")));

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE ROLE %s", quote_identifier(rolname));

	/*
	 * Append role attributes.  The order here follows the same sequence as
	 * you'd typically write them in a CREATE ROLE command, though any order
	 * is actually acceptable to the parser.
	 */
	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolsuper ? "SUPERUSER" : "NOSUPERUSER");

	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolinherit ? "INHERIT" : "NOINHERIT");

	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolcreaterole ? "CREATEROLE" : "NOCREATEROLE");

	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolcreatedb ? "CREATEDB" : "NOCREATEDB");

	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolcanlogin ? "LOGIN" : "NOLOGIN");

	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolreplication ? "REPLICATION" : "NOREPLICATION");

	append_ddl_option(&buf, pretty, 4, "%s",
					  roleform->rolbypassrls ? "BYPASSRLS" : "NOBYPASSRLS");

	/*
	 * CONNECTION LIMIT is only interesting if it's not -1 (the default,
	 * meaning no limit).
	 */
	if (roleform->rolconnlimit >= 0)
		append_ddl_option(&buf, pretty, 4, "CONNECTION LIMIT %d",
						  roleform->rolconnlimit);

	rolevaliduntil = SysCacheGetAttr(AUTHOID, tuple,
									 Anum_pg_authid_rolvaliduntil,
									 &isnull);
	if (!isnull)
	{
		TimestampTz ts;
		int			tz;
		struct pg_tm tm;
		fsec_t		fsec;
		const char *tzn;
		char		ts_str[MAXDATELEN + 1];

		ts = DatumGetTimestampTz(rolevaliduntil);
		if (TIMESTAMP_NOT_FINITE(ts))
			EncodeSpecialTimestamp(ts, ts_str);
		else if (timestamp2tm(ts, &tz, &tm, &fsec, &tzn, NULL) == 0)
			EncodeDateTime(&tm, fsec, true, tz, tzn, USE_ISO_DATES, ts_str);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		append_ddl_option(&buf, pretty, 4, "VALID UNTIL %s",
						  quote_literal_cstr(ts_str));
	}

	ReleaseSysCache(tuple);

	/*
	 * We intentionally omit PASSWORD.  There's no way to retrieve the
	 * original password text from the stored hash, and even if we could,
	 * exposing passwords through a SQL function would be a security issue.
	 * Users must set passwords separately after recreating roles.
	 */

	appendStringInfoChar(&buf, ';');

	statements = lappend(statements, pstrdup(buf.data));

	/*
	 * Now scan pg_db_role_setting for ALTER ROLE SET configurations.
	 *
	 * These can be role-wide (setdatabase = 0) or specific to a particular
	 * database (setdatabase = a valid DB OID).  It generates one ALTER
	 * statement per setting.
	 */
	rel = table_open(DbRoleSettingRelationId, AccessShareLock);
	ScanKeyInit(&scankey,
				Anum_pg_db_role_setting_setrole,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(roleid));
	scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_db_role_setting setting = (Form_pg_db_role_setting) GETSTRUCT(tuple);
		Oid			datid = setting->setdatabase;
		Datum		datum;
		ArrayType  *role_settings;
		Datum	   *settings;
		bool	   *nulls;
		int			nsettings;
		char	   *datname = NULL;

		/*
		 * If setdatabase is valid, this is a role-in-database setting;
		 * otherwise it's a role-wide setting.  Look up the database name once
		 * for all settings in this row.
		 */
		if (OidIsValid(datid))
		{
			datname = get_database_name(datid);
			/* Database has been dropped; skip all settings in this row. */
			if (datname == NULL)
				continue;
		}

		/*
		 * The setconfig column is a text array in "name=value" format. It
		 * should never be null for a valid row, but be defensive.
		 */
		datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
							 RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;

		role_settings = DatumGetArrayTypePCopy(datum);

		deconstruct_array_builtin(role_settings, TEXTOID, &settings, &nulls, &nsettings);

		for (int i = 0; i < nsettings; i++)
		{
			char	   *s,
					   *p;

			if (nulls[i])
				continue;

			s = TextDatumGetCString(settings[i]);
			p = strchr(s, '=');
			if (p == NULL)
			{
				pfree(s);
				continue;
			}
			*p++ = '\0';

			/* Build a fresh ALTER ROLE statement for this setting */
			resetStringInfo(&buf);
			appendStringInfo(&buf, "ALTER ROLE %s", quote_identifier(rolname));

			if (datname != NULL)
				appendStringInfo(&buf, " IN DATABASE %s",
								 quote_identifier(datname));

			appendStringInfo(&buf, " SET %s TO ",
							 quote_identifier(s));

			append_guc_value(&buf, s, p);

			appendStringInfoChar(&buf, ';');

			statements = lappend(statements, pstrdup(buf.data));

			pfree(s);
		}

		pfree(settings);
		pfree(nulls);
		pfree(role_settings);

		if (datname != NULL)
			pfree(datname);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	/*
	 * Scan pg_auth_members for role memberships.  We look for rows where
	 * member = roleid, meaning this role has been granted membership in other
	 * roles.
	 */
	if (memberships)
	{
		rel = table_open(AuthMemRelationId, AccessShareLock);
		ScanKeyInit(&scankey,
					Anum_pg_auth_members_member,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(roleid));
		scan = systable_beginscan(rel, AuthMemMemRoleIndexId, true,
								  NULL, 1, &scankey);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			Form_pg_auth_members memform = (Form_pg_auth_members) GETSTRUCT(tuple);
			char	   *granted_role;
			char	   *grantor;

			granted_role = GetUserNameFromId(memform->roleid, false);
			grantor = GetUserNameFromId(memform->grantor, false);

			resetStringInfo(&buf);
			appendStringInfo(&buf, "GRANT %s TO %s",
							 quote_identifier(granted_role),
							 quote_identifier(rolname));
			appendStringInfo(&buf, " WITH ADMIN %s, INHERIT %s, SET %s",
							 memform->admin_option ? "TRUE" : "FALSE",
							 memform->inherit_option ? "TRUE" : "FALSE",
							 memform->set_option ? "TRUE" : "FALSE");
			appendStringInfo(&buf, " GRANTED BY %s;",
							 quote_identifier(grantor));

			statements = lappend(statements, pstrdup(buf.data));

			pfree(granted_role);
			pfree(grantor);
		}

		systable_endscan(scan);
		table_close(rel, AccessShareLock);
	}

	pfree(buf.data);
	pfree(rolname);

	return statements;
}

/*
 * pg_get_role_ddl
 *		Return DDL to recreate a role as a set of text rows.
 *
 * Each row is a complete SQL statement.  The first row is always the
 * CREATE ROLE statement; subsequent rows are ALTER ROLE SET statements
 * and optionally GRANT statements for role memberships.
 */
Datum
pg_get_role_ddl(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	List	   *statements;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		Oid			roleid;
		bool		pretty;
		bool		memberships;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		roleid = PG_GETARG_OID(0);
		pretty = PG_GETARG_BOOL(1);
		memberships = PG_GETARG_BOOL(2);

		statements = pg_get_role_ddl_internal(roleid, pretty, memberships);
		funcctx->user_fctx = statements;
		funcctx->max_calls = list_length(statements);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	statements = (List *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		char	   *stmt;

		stmt = list_nth(statements, funcctx->call_cntr);

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(stmt));
	}
	else
	{
		list_free_deep(statements);
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * pg_get_tablespace_ddl_internal
 *		Generate DDL statements to recreate a tablespace.
 *
 * Returns a List of palloc'd strings.  The first element is the
 * CREATE TABLESPACE statement; if the tablespace has reloptions,
 * a second element with ALTER TABLESPACE SET (...) is appended.
 */
static List *
pg_get_tablespace_ddl_internal(Oid tsid, bool pretty, bool no_owner)
{
	HeapTuple	tuple;
	Form_pg_tablespace tspForm;
	StringInfoData buf;
	char	   *spcname;
	char	   *spcowner;
	char	   *path;
	bool		isNull;
	Datum		datum;
	List	   *statements = NIL;

	tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(tsid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist",
						tsid)));

	tspForm = (Form_pg_tablespace) GETSTRUCT(tuple);
	spcname = pstrdup(NameStr(tspForm->spcname));

	/* User must have SELECT privilege on pg_tablespace. */
	if (pg_class_aclcheck(TableSpaceRelationId, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
	{
		ReleaseSysCache(tuple);
		aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLESPACE, spcname);
	}

	/*
	 * We don't support generating DDL for system tablespaces.  The primary
	 * reason for this is that users shouldn't be recreating them.
	 */
	if (IsReservedName(spcname))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("tablespace name \"%s\" is reserved", spcname),
				 errdetail("Tablespace names starting with \"pg_\" are reserved for system tablespaces.")));

	initStringInfo(&buf);

	/* Start building the CREATE TABLESPACE statement */
	appendStringInfo(&buf, "CREATE TABLESPACE %s", quote_identifier(spcname));

	/* Add OWNER clause */
	if (!no_owner)
	{
		spcowner = GetUserNameFromId(tspForm->spcowner, false);
		append_ddl_option(&buf, pretty, 4, "OWNER %s",
						  quote_identifier(spcowner));
		pfree(spcowner);
	}

	/* Find tablespace directory path */
	path = get_tablespace_location(tsid);

	/* Add directory LOCATION (path), if it exists */
	if (path[0] != '\0')
	{
		/*
		 * Special case: if the tablespace was created with GUC
		 * "allow_in_place_tablespaces = true" and "LOCATION ''", path will
		 * begin with "pg_tblspc/". In that case, show "LOCATION ''" as the
		 * user originally specified.
		 */
		if (strncmp(PG_TBLSPC_DIR_SLASH, path, strlen(PG_TBLSPC_DIR_SLASH)) == 0)
			append_ddl_option(&buf, pretty, 4, "LOCATION ''");
		else
			append_ddl_option(&buf, pretty, 4, "LOCATION %s",
							  quote_literal_cstr(path));
	}
	pfree(path);

	appendStringInfoChar(&buf, ';');
	statements = lappend(statements, pstrdup(buf.data));

	/* Check for tablespace options */
	datum = SysCacheGetAttr(TABLESPACEOID, tuple,
							Anum_pg_tablespace_spcoptions, &isNull);
	if (!isNull)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf, "ALTER TABLESPACE %s SET (",
						 quote_identifier(spcname));
		get_reloptions(&buf, datum);
		appendStringInfoString(&buf, ");");
		statements = lappend(statements, pstrdup(buf.data));
	}

	ReleaseSysCache(tuple);
	pfree(spcname);
	pfree(buf.data);

	return statements;
}

/*
 * pg_get_tablespace_ddl_srf - common SRF logic for tablespace DDL
 */
static Datum
pg_get_tablespace_ddl_srf(FunctionCallInfo fcinfo, Oid tsid)
{
	FuncCallContext *funcctx;
	List	   *statements;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		bool		pretty;
		bool		no_owner;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		pretty = PG_GETARG_BOOL(1);
		no_owner = !PG_GETARG_BOOL(2);

		statements = pg_get_tablespace_ddl_internal(tsid, pretty, no_owner);
		funcctx->user_fctx = statements;
		funcctx->max_calls = list_length(statements);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	statements = (List *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		char	   *stmt;

		stmt = (char *) list_nth(statements, funcctx->call_cntr);

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(stmt));
	}
	else
	{
		list_free_deep(statements);
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * pg_get_tablespace_ddl_oid
 *		Return DDL to recreate a tablespace, taking OID.
 */
Datum
pg_get_tablespace_ddl_oid(PG_FUNCTION_ARGS)
{
	Oid			tsid = PG_GETARG_OID(0);

	return pg_get_tablespace_ddl_srf(fcinfo, tsid);
}

/*
 * pg_get_tablespace_ddl_name
 *		Return DDL to recreate a tablespace, taking name.
 */
Datum
pg_get_tablespace_ddl_name(PG_FUNCTION_ARGS)
{
	Name		tspname = PG_GETARG_NAME(0);
	Oid			tsid = get_tablespace_oid(NameStr(*tspname), false);

	return pg_get_tablespace_ddl_srf(fcinfo, tsid);
}

/*
 * pg_get_database_ddl_internal
 *		Generate DDL statements to recreate a database.
 *
 * Returns a List of palloc'd strings.  The first element is the
 * CREATE DATABASE statement; subsequent elements are ALTER DATABASE
 * statements for properties and configuration settings.
 */
static List *
pg_get_database_ddl_internal(Oid dbid, bool pretty,
							 bool no_owner, bool no_tablespace)
{
	HeapTuple	tuple;
	Form_pg_database dbform;
	StringInfoData buf;
	bool		isnull;
	Datum		datum;
	const char *encoding;
	char	   *dbname;
	char	   *collate;
	char	   *ctype;
	Relation	rel;
	ScanKeyData scankey[2];
	SysScanDesc scan;
	List	   *statements = NIL;
	AclResult	aclresult;

	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("database with OID %u does not exist", dbid)));

	/* User must have connect privilege for target database. */
	aclresult = object_aclcheck(DatabaseRelationId, dbid, GetUserId(), ACL_CONNECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(dbid));

	dbform = (Form_pg_database) GETSTRUCT(tuple);
	dbname = pstrdup(NameStr(dbform->datname));

	/*
	 * Reject invalid databases. Deparsing a pg_database row in invalid state
	 * can produce SQL that is not executable, such as CONNECTION LIMIT = -2.
	 */
	if (database_is_invalid_form(dbform))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot generate DDL for invalid database \"%s\"",
						dbname)));

	/*
	 * We don't support generating DDL for system databases.  The primary
	 * reason for this is that users shouldn't be recreating them.
	 */
	if (strcmp(dbname, "template0") == 0 || strcmp(dbname, "template1") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("database \"%s\" is a system database", dbname),
				 errdetail("DDL generation is not supported for template0 and template1.")));

	initStringInfo(&buf);

	/* --- Build CREATE DATABASE statement --- */
	appendStringInfo(&buf, "CREATE DATABASE %s", quote_identifier(dbname));

	/*
	 * Always use template0: the target database already contains the catalog
	 * data from whatever template was used originally, so we must start from
	 * the pristine template to avoid duplication.
	 */
	append_ddl_option(&buf, pretty, 4, "WITH TEMPLATE = template0");

	/* ENCODING */
	encoding = pg_encoding_to_char(dbform->encoding);
	if (strlen(encoding) > 0)
		append_ddl_option(&buf, pretty, 4, "ENCODING = %s",
						  quote_literal_cstr(encoding));

	/* LOCALE_PROVIDER */
	if (dbform->datlocprovider == COLLPROVIDER_BUILTIN ||
		dbform->datlocprovider == COLLPROVIDER_ICU ||
		dbform->datlocprovider == COLLPROVIDER_LIBC)
		append_ddl_option(&buf, pretty, 4, "LOCALE_PROVIDER = %s",
						  collprovider_name(dbform->datlocprovider));
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("unrecognized locale provider: %c",
						dbform->datlocprovider)));

	/* LOCALE, LC_COLLATE, LC_CTYPE */
	datum = SysCacheGetAttr(DATABASEOID, tuple,
							Anum_pg_database_datcollate, &isnull);
	collate = isnull ? NULL : TextDatumGetCString(datum);
	datum = SysCacheGetAttr(DATABASEOID, tuple,
							Anum_pg_database_datctype, &isnull);
	ctype = isnull ? NULL : TextDatumGetCString(datum);
	if (collate != NULL && ctype != NULL && strcmp(collate, ctype) == 0)
	{
		append_ddl_option(&buf, pretty, 4, "LOCALE = %s",
						  quote_literal_cstr(collate));
	}
	else
	{
		if (collate != NULL)
			append_ddl_option(&buf, pretty, 4, "LC_COLLATE = %s",
							  quote_literal_cstr(collate));
		if (ctype != NULL)
			append_ddl_option(&buf, pretty, 4, "LC_CTYPE = %s",
							  quote_literal_cstr(ctype));
	}

	/* LOCALE (provider-specific) */
	datum = SysCacheGetAttr(DATABASEOID, tuple,
							Anum_pg_database_datlocale, &isnull);
	if (!isnull)
	{
		const char *locale = TextDatumGetCString(datum);

		if (dbform->datlocprovider == COLLPROVIDER_BUILTIN)
			append_ddl_option(&buf, pretty, 4, "BUILTIN_LOCALE = %s",
							  quote_literal_cstr(locale));
		else if (dbform->datlocprovider == COLLPROVIDER_ICU)
			append_ddl_option(&buf, pretty, 4, "ICU_LOCALE = %s",
							  quote_literal_cstr(locale));
	}

	/* ICU_RULES */
	datum = SysCacheGetAttr(DATABASEOID, tuple,
							Anum_pg_database_daticurules, &isnull);
	if (!isnull && dbform->datlocprovider == COLLPROVIDER_ICU)
		append_ddl_option(&buf, pretty, 4, "ICU_RULES = %s",
						  quote_literal_cstr(TextDatumGetCString(datum)));

	/* TABLESPACE */
	if (!no_tablespace && OidIsValid(dbform->dattablespace))
	{
		char	   *spcname = get_tablespace_name(dbform->dattablespace);

		if (spcname == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace with OID %u does not exist",
							dbform->dattablespace),
					 errdetail("It may have been concurrently dropped.")));

		if (pg_strcasecmp(spcname, "pg_default") != 0)
			append_ddl_option(&buf, pretty, 4, "TABLESPACE = %s",
							  quote_identifier(spcname));
	}

	appendStringInfoChar(&buf, ';');
	statements = lappend(statements, pstrdup(buf.data));

	/* OWNER */
	if (!no_owner && OidIsValid(dbform->datdba))
	{
		char	   *owner = GetUserNameFromId(dbform->datdba, false);

		resetStringInfo(&buf);
		appendStringInfo(&buf, "ALTER DATABASE %s OWNER TO %s;",
						 quote_identifier(dbname), quote_identifier(owner));
		pfree(owner);
		statements = lappend(statements, pstrdup(buf.data));
	}

	/* CONNECTION LIMIT */
	if (dbform->datconnlimit != -1)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf, "ALTER DATABASE %s CONNECTION LIMIT = %d;",
						 quote_identifier(dbname), dbform->datconnlimit);
		statements = lappend(statements, pstrdup(buf.data));
	}

	/* IS_TEMPLATE */
	if (dbform->datistemplate)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf, "ALTER DATABASE %s IS_TEMPLATE = true;",
						 quote_identifier(dbname));
		statements = lappend(statements, pstrdup(buf.data));
	}

	/* ALLOW_CONNECTIONS */
	if (!dbform->datallowconn)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf, "ALTER DATABASE %s ALLOW_CONNECTIONS = false;",
						 quote_identifier(dbname));
		statements = lappend(statements, pstrdup(buf.data));
	}

	ReleaseSysCache(tuple);

	/*
	 * Now scan pg_db_role_setting for ALTER DATABASE SET configurations.
	 *
	 * It is only database-wide (setrole = 0). It generates one ALTER
	 * statement per setting.
	 */
	rel = table_open(DbRoleSettingRelationId, AccessShareLock);
	ScanKeyInit(&scankey[0],
				Anum_pg_db_role_setting_setdatabase,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dbid));
	ScanKeyInit(&scankey[1],
				Anum_pg_db_role_setting_setrole,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));

	scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true,
							  NULL, 2, scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		ArrayType  *dbconfig;
		Datum	   *settings;
		bool	   *nulls;
		int			nsettings;

		/*
		 * The setconfig column is a text array in "name=value" format. It
		 * should never be null for a valid row, but be defensive.
		 */
		datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
							 RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;

		dbconfig = DatumGetArrayTypePCopy(datum);

		deconstruct_array_builtin(dbconfig, TEXTOID, &settings, &nulls, &nsettings);

		for (int i = 0; i < nsettings; i++)
		{
			char	   *s,
					   *p;

			if (nulls[i])
				continue;

			s = TextDatumGetCString(settings[i]);
			p = strchr(s, '=');
			if (p == NULL)
			{
				pfree(s);
				continue;
			}
			*p++ = '\0';

			resetStringInfo(&buf);
			appendStringInfo(&buf, "ALTER DATABASE %s SET %s TO ",
							 quote_identifier(dbname),
							 quote_identifier(s));

			append_guc_value(&buf, s, p);

			appendStringInfoChar(&buf, ';');

			statements = lappend(statements, pstrdup(buf.data));

			pfree(s);
		}

		pfree(settings);
		pfree(nulls);
		pfree(dbconfig);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	pfree(buf.data);
	pfree(dbname);

	return statements;
}

/*
 * pg_get_database_ddl
 *		Return DDL to recreate a database as a set of text rows.
 */
Datum
pg_get_database_ddl(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	List	   *statements;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		Oid			dbid;
		bool		pretty;
		bool		no_owner;
		bool		no_tablespace;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		dbid = PG_GETARG_OID(0);
		pretty = PG_GETARG_BOOL(1);
		no_owner = !PG_GETARG_BOOL(2);
		no_tablespace = !PG_GETARG_BOOL(3);

		statements = pg_get_database_ddl_internal(dbid, pretty, no_owner,
												  no_tablespace);
		funcctx->user_fctx = statements;
		funcctx->max_calls = list_length(statements);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	statements = (List *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		char	   *stmt;

		stmt = list_nth(statements, funcctx->call_cntr);

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(stmt));
	}
	else
	{
		list_free_deep(statements);
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * lookup_relname_for_emit
 *		Return either the schema-qualified or the bare quoted name of a
 *		relation, depending on the schema_qualified flag.
 *
 * Temporary relations are never schema-qualified regardless of
 * schema_qualified: the TEMPORARY keyword in CREATE TEMPORARY TABLE
 * already places the table in the session's temp schema, so emitting
 * pg_temp_NN.relname would produce DDL that cannot be replayed.
 *
 * When schema_qualified is true the schema-qualified name is always
 * returned for non-temporary relations.  When false, the bare relname
 * is returned only if the target relation lives in base_namespace (the
 * namespace of the table whose DDL is being generated); otherwise the
 * schema-qualified form is returned, because cross-schema references
 * (for example an inheritance parent or foreign key target in a
 * different schema) are not safe to omit.
 *
 * This replaces the unsafe pattern
 *	  quote_qualified_identifier(get_namespace_name(get_rel_namespace(oid)),
 *	                             get_rel_name(oid))
 * which dereferences NULL when a concurrent transaction has dropped the
 * referenced relation (or its schema) between when we cached its OID and
 * when we ask the syscache for its name.  Holding AccessShareLock on a
 * dependent relation makes this race vanishingly unlikely in practice, but
 * we still defend against it because the alternative is a SIGSEGV.
 *
 * Caller is responsible for pfree()ing the result.
 */
static char *
lookup_relname_for_emit(Oid relid, bool schema_qualified, Oid base_namespace)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("relation with OID %u does not exist", relid),
				 errdetail("It may have been concurrently dropped.")));

	reltup = (Form_pg_class) GETSTRUCT(tp);

	/*
	 * Temporary relations are never schema-qualified: the TEMPORARY keyword
	 * already places them in pg_temp, and pg_temp_NN.relname cannot be
	 * replayed in any other session.
	 */
	if (isTempNamespace(reltup->relnamespace))
	{
		result = pstrdup(quote_identifier(NameStr(reltup->relname)));
		ReleaseSysCache(tp);
		return result;
	}

	/* Bare name only when caller asked and target is in the base namespace. */
	if (!schema_qualified && reltup->relnamespace == base_namespace)
	{
		result = pstrdup(quote_identifier(NameStr(reltup->relname)));
		ReleaseSysCache(tp);
		return result;
	}

	nspname = get_namespace_name(reltup->relnamespace);
	if (nspname == NULL)
	{
		Oid			nspoid = reltup->relnamespace;

		ReleaseSysCache(tp);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("schema with OID %u does not exist", nspoid),
				 errdetail("It may have been concurrently dropped.")));
	}

	result = quote_qualified_identifier(nspname, NameStr(reltup->relname));

	pfree(nspname);
	ReleaseSysCache(tp);

	return result;
}

/*
 * collect_local_not_null
 *		Scan pg_constraint once for locally-declared NOT NULL constraints
 *		on rel, returning a palloc'd array indexed by attnum (1..natts).
 *
 * Entries with conoid==InvalidOid mean "no local NOT NULL row for this
 * column".  When the constraint name does not match the auto-generated
 * pattern "<tablename>_<columnname>_not_null", the constraint OID is
 * appended to *skip_oids so the post-CREATE constraint loop can avoid
 * re-emitting it as ALTER TABLE ... ADD CONSTRAINT - the column-emit
 * pass will produce it inline as "CONSTRAINT name NOT NULL" instead.
 */
static LocalNotNullEntry *
collect_local_not_null(Relation rel, List **skip_oids)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			natts = tupdesc->natts;
	LocalNotNullEntry *entries;
	const char *relname = RelationGetRelationName(rel);
	Relation	conRel;
	SysScanDesc conScan;
	ScanKeyData conKey;
	HeapTuple	conTup;

	entries = (LocalNotNullEntry *) palloc0(sizeof(LocalNotNullEntry) *
											(natts + 1));

	conRel = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&conKey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	conScan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId,
								 true, NULL, 1, &conKey);

	while (HeapTupleIsValid(conTup = systable_getnext(conScan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);
		Datum		conkeyDat;
		bool		conkeyNull;
		ArrayType  *conkeyArr;
		int16	   *conkeyVals;
		int			attnum;
		Form_pg_attribute att;
		char		autoname[NAMEDATALEN];

		if (con->contype != CONSTRAINT_NOTNULL)
			continue;
		if (!con->conislocal)
			continue;

		conkeyDat = heap_getattr(conTup, Anum_pg_constraint_conkey,
								 RelationGetDescr(conRel), &conkeyNull);
		if (conkeyNull)
			continue;
		conkeyArr = DatumGetArrayTypeP(conkeyDat);

		/*
		 * Defend against a malformed conkey: a NOT NULL constraint is always
		 * a single-column 1-D int2 array, but a corrupted catalog or a future
		 * patch that stores wider conkeys mustn't trip us into reading past
		 * the array header.
		 */
		if (ARR_NDIM(conkeyArr) != 1 ||
			ARR_DIMS(conkeyArr)[0] < 1 ||
			ARR_HASNULL(conkeyArr) ||
			ARR_ELEMTYPE(conkeyArr) != INT2OID)
			continue;

		conkeyVals = (int16 *) ARR_DATA_PTR(conkeyArr);
		attnum = conkeyVals[0];
		if (attnum < 1 || attnum > natts)
			continue;

		att = TupleDescAttr(tupdesc, attnum - 1);
		snprintf(autoname, sizeof(autoname), "%s_%s_not_null",
				 relname, NameStr(att->attname));

		entries[attnum].conoid = con->oid;
		entries[attnum].name = pstrdup(NameStr(con->conname));
		entries[attnum].is_auto = (strcmp(NameStr(con->conname), autoname) == 0);
		entries[attnum].no_inherit = con->connoinherit;

		/*
		 * Inline emission of NOT NULL only happens for columns that the
		 * column list actually emits, i.e. attislocal columns.  For those
		 * the inline pass already materializes the constraint (either as
		 * "CONSTRAINT name NOT NULL" for user-named, or as a bare
		 * "NOT NULL" that PG re-creates under the auto-name pattern), so
		 * the post-CREATE constraint loop must not emit a second
		 * ALTER TABLE ... ADD CONSTRAINT for the same column - PG only
		 * allows one NOT NULL constraint per column and rejects the
		 * second with a name mismatch whenever the saved auto-name no
		 * longer matches the current table name (e.g. after a rename, or
		 * when CREATE TABLE uniquifies the auto-name to dodge a sequence
		 * collision).  For a locally-declared NOT NULL sitting on an
		 * inherited (non-local) column the inline path never fires, so
		 * leave the OID out of skip_oids and let the post-CREATE loop
		 * emit ALTER TABLE ... ADD CONSTRAINT.
		 */
		if (att->attislocal && skip_oids != NULL)
			*skip_oids = lappend_oid(*skip_oids, con->oid);
	}
	systable_endscan(conScan);
	table_close(conRel, AccessShareLock);

	return entries;
}

/*
 * find_attrdef_text
 *		Return the deparsed DEFAULT/GENERATED expression for attnum on rel,
 *		or NULL if no entry exists in TupleConstr->defval.
 *
 * The caller passes a List ** so that the deparse context is built lazily
 * and reused across calls (deparse_context_for is not cheap).  Returned
 * string is palloc'd in the current memory context; caller pfree's it.
 */
static char *
find_attrdef_text(Relation rel, AttrNumber attnum, List **dpcontext)
{
	TupleConstr *constr = RelationGetDescr(rel)->constr;

	if (constr == NULL)
		return NULL;

	for (int j = 0; j < constr->num_defval; j++)
	{
		if (constr->defval[j].adnum != attnum)
			continue;

		if (*dpcontext == NIL)
			*dpcontext = deparse_context_for(RelationGetRelationName(rel),
											 RelationGetRelid(rel));

		return deparse_expression(stringToNode(constr->defval[j].adbin),
								  *dpcontext, false, false);
	}
	return NULL;
}

/*
 * append_inline_check_constraints
 *		Emit each locally-declared CHECK constraint on rel as
 *		"CONSTRAINT name <pg_get_constraintdef>", separated by ',' from any
 *		previously-emitted column or constraint.
 *
 * *first tracks whether anything has been emitted on this list yet, so the
 * caller can chain column emission and constraint emission through the same
 * buffer.  Inherited CHECK constraints (!conislocal) come from the parent's
 * DDL and aren't repeated here.
 */
static void
append_inline_check_constraints(StringInfo buf, Relation rel, bool pretty,
								bool *first)
{
	Relation	conRel;
	SysScanDesc conScan;
	ScanKeyData conKey;
	HeapTuple	conTup;

	conRel = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&conKey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	conScan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId,
								 true, NULL, 1, &conKey);

	while (HeapTupleIsValid(conTup = systable_getnext(conScan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);
		Datum		defDatum;
		char	   *defbody;

		if (con->contype != CONSTRAINT_CHECK)
			continue;
		if (!con->conislocal)
			continue;

		if (!*first)
			appendStringInfoChar(buf, ',');
		if (pretty)
			appendStringInfoString(buf, "\n    ");
		else if (!*first)
			appendStringInfoChar(buf, ' ');
		*first = false;

		defDatum = OidFunctionCall1(F_PG_GET_CONSTRAINTDEF_OID,
									ObjectIdGetDatum(con->oid));
		defbody = TextDatumGetCString(defDatum);
		appendStringInfo(buf, "CONSTRAINT %s %s",
						 quote_identifier(NameStr(con->conname)),
						 defbody);
		pfree(defbody);
	}
	systable_endscan(conScan);
	table_close(conRel, AccessShareLock);
}

/*
 * append_column_defs
 *		Append the comma-separated column definition list for a table.
 *
 * Emits each non-dropped, locally-declared column as
 *		name type [COLLATE x] [STORAGE s] [COMPRESSION c]
 *		[GENERATED ... | DEFAULT e] [NOT NULL]
 * followed by any locally-declared inline CHECK constraints.  Optional
 * clauses are omitted when their value matches what the system would
 * reapply on round-trip (e.g. type-default COLLATE, type-default STORAGE).
 */
static void
append_column_defs(StringInfo buf, Relation rel, bool pretty,
				   bool include_check,
				   bool schema_qualified,
				   LocalNotNullEntry *nn_entries)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Oid			base_namespace = RelationGetNamespace(rel);
	List	   *dpcontext = NIL;
	bool		first = true;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		char	   *typstr;

		if (att->attisdropped)
			continue;

		/*
		 * Columns inherited from a parent are covered by the INHERITS clause,
		 * not the column list, unless the child redeclared them locally
		 * (attislocal=true).
		 */
		if (!att->attislocal)
			continue;

		if (!first)
			appendStringInfoChar(buf, ',');
		if (pretty)
			appendStringInfoString(buf, "\n    ");
		else if (!first)
			appendStringInfoChar(buf, ' ');
		first = false;

		appendStringInfoString(buf, quote_identifier(NameStr(att->attname)));
		appendStringInfoChar(buf, ' ');

		typstr = format_type_with_typemod(att->atttypid, att->atttypmod);
		appendStringInfoString(buf, typstr);
		pfree(typstr);

		/* COLLATE clause, only if it differs from the type's default. */
		if (OidIsValid(att->attcollation) &&
			att->attcollation != get_typcollation(att->atttypid))
			appendStringInfo(buf, " COLLATE %s",
							 generate_collation_name(att->attcollation));

		/* STORAGE clause, only if it differs from the type's default. */
		if (att->attstorage != get_typstorage(att->atttypid))
			appendStringInfo(buf, " STORAGE %s", storage_name(att->attstorage));

		/* COMPRESSION clause, only if explicitly set on the column. */
		if (CompressionMethodIsValid(att->attcompression))
			appendStringInfo(buf, " COMPRESSION %s",
							 GetCompressionMethodName(att->attcompression));

		/*
		 * Look up the default/generated expression text up front; generated
		 * columns have atthasdef=true with an entry in pg_attrdef just like
		 * regular defaults.
		 */
		{
			char	   *defexpr = NULL;

			if (att->atthasdef)
				defexpr = find_attrdef_text(rel, att->attnum, &dpcontext);

			/* GENERATED / IDENTITY / DEFAULT are mutually exclusive. */
			if (att->attgenerated == ATTRIBUTE_GENERATED_STORED && defexpr)
				appendStringInfo(buf, " GENERATED ALWAYS AS (%s) STORED", defexpr);
			else if (att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL && defexpr)
				appendStringInfo(buf, " GENERATED ALWAYS AS (%s) VIRTUAL", defexpr);
			else if (att->attidentity == ATTRIBUTE_IDENTITY_ALWAYS ||
					 att->attidentity == ATTRIBUTE_IDENTITY_BY_DEFAULT)
			{
				const char *idkind =
					(att->attidentity == ATTRIBUTE_IDENTITY_ALWAYS)
					? "ALWAYS" : "BY DEFAULT";
				Oid			seqid = getIdentitySequence(rel, att->attnum, true);

				appendStringInfo(buf, " GENERATED %s AS IDENTITY", idkind);

				/*
				 * Emit only the sequence options that differ from their
				 * defaults - mirroring pg_get_database_ddl's pattern of
				 * omitting values that the system would reapply on its own.
				 */
				if (OidIsValid(seqid))
				{
					HeapTuple	seqTup = SearchSysCache1(SEQRELID,
														 ObjectIdGetDatum(seqid));

					if (HeapTupleIsValid(seqTup))
					{
						Form_pg_sequence seq = (Form_pg_sequence) GETSTRUCT(seqTup);
						StringInfoData opts;
						bool		first_opt = true;
						int64		def_min,
									def_max,
									def_start;
						int64		typ_min,
									typ_max;

						/*
						 * Per-type bounds for the sequence's underlying
						 * integer type.  Defaults to int8 if the column type
						 * is something else (shouldn't happen for IDENTITY,
						 * but be defensive).
						 */
						switch (att->atttypid)
						{
							case INT2OID:
								typ_min = PG_INT16_MIN;
								typ_max = PG_INT16_MAX;
								break;
							case INT4OID:
								typ_min = PG_INT32_MIN;
								typ_max = PG_INT32_MAX;
								break;
							default:
								typ_min = PG_INT64_MIN;
								typ_max = PG_INT64_MAX;
								break;
						}

						if (seq->seqincrement > 0)
						{
							def_min = 1;
							def_max = typ_max;
							def_start = def_min;
						}
						else
						{
							def_min = typ_min;
							def_max = -1;
							def_start = def_max;
						}

						initStringInfo(&opts);

						/*
						 * SEQUENCE NAME - omit when it matches the implicit
						 * "<tablename>_<columnname>_seq" pattern in the same
						 * schema, since CREATE TABLE will regenerate that
						 * exact name.  The sequence is an INTERNAL dependency
						 * of the column, so the lock we hold on the table
						 * also pins it, but the lookup helper still defends
						 * against a missing pg_class row.
						 */
						{
							HeapTuple	seqClassTup;
							Form_pg_class seqClass;
							char		autoname[NAMEDATALEN];

							seqClassTup = SearchSysCache1(RELOID,
														  ObjectIdGetDatum(seqid));
							if (!HeapTupleIsValid(seqClassTup))
								ereport(ERROR,
										(errcode(ERRCODE_UNDEFINED_OBJECT),
										 errmsg("identity sequence with OID %u does not exist",
												seqid),
										 errdetail("It may have been concurrently dropped.")));
							seqClass = (Form_pg_class) GETSTRUCT(seqClassTup);

							snprintf(autoname, sizeof(autoname), "%s_%s_seq",
									 RelationGetRelationName(rel),
									 NameStr(att->attname));
							if (seqClass->relnamespace != RelationGetNamespace(rel) ||
								strcmp(NameStr(seqClass->relname), autoname) != 0)
							{
								char	   *seqQual =
									lookup_relname_for_emit(seqid,
															schema_qualified,
															base_namespace);

								appendStringInfo(&opts, "%sSEQUENCE NAME %s",
												 first_opt ? "" : " ", seqQual);
								first_opt = false;
								pfree(seqQual);
							}
							ReleaseSysCache(seqClassTup);
						}

						if (seq->seqstart != def_start)
						{
							appendStringInfo(&opts, "%sSTART WITH " INT64_FORMAT,
											 first_opt ? "" : " ", seq->seqstart);
							first_opt = false;
						}
						if (seq->seqincrement != 1)
						{
							appendStringInfo(&opts, "%sINCREMENT BY " INT64_FORMAT,
											 first_opt ? "" : " ", seq->seqincrement);
							first_opt = false;
						}
						if (seq->seqmin != def_min)
						{
							appendStringInfo(&opts, "%sMINVALUE " INT64_FORMAT,
											 first_opt ? "" : " ", seq->seqmin);
							first_opt = false;
						}
						if (seq->seqmax != def_max)
						{
							appendStringInfo(&opts, "%sMAXVALUE " INT64_FORMAT,
											 first_opt ? "" : " ", seq->seqmax);
							first_opt = false;
						}
						if (seq->seqcache != 1)
						{
							appendStringInfo(&opts, "%sCACHE " INT64_FORMAT,
											 first_opt ? "" : " ", seq->seqcache);
							first_opt = false;
						}
						if (seq->seqcycle)
						{
							appendStringInfo(&opts, "%sCYCLE", first_opt ? "" : " ");
							first_opt = false;
						}

						if (!first_opt)
							appendStringInfo(buf, " (%s)", opts.data);

						pfree(opts.data);
						ReleaseSysCache(seqTup);
					}
				}
			}
			else if (defexpr)
				appendStringInfo(buf, " DEFAULT %s", defexpr);

			if (defexpr)
				pfree(defexpr);
		}

		if (att->attnotnull)
		{
			LocalNotNullEntry *nn = &nn_entries[att->attnum];

			if (nn->name != NULL && !nn->is_auto)
				appendStringInfo(buf, " CONSTRAINT %s NOT NULL",
								 quote_identifier(nn->name));
			else
				appendStringInfoString(buf, " NOT NULL");
			if (nn->name != NULL && nn->no_inherit)
				appendStringInfoString(buf, " NO INHERIT");
		}
	}

	/*
	 * Table-level CHECK constraints - emitted inline in the CREATE TABLE body
	 * so they appear alongside the columns (the pg_dump shape).  The
	 * constraint loop later in pg_get_table_ddl_internal skips CHECK
	 * constraints to avoid double-emission.
	 */
	if (include_check)
		append_inline_check_constraints(buf, rel, pretty, &first);
}

/*
 * append_typed_column_overrides
 *		For a typed table (CREATE TABLE ... OF type_name), append the
 *		optional "(col WITH OPTIONS ..., ...)" list carrying locally
 *		applied per-column overrides - DEFAULT, NOT NULL, and any locally
 *		declared CHECK constraints.
 *
 * Columns whose type is fully dictated by reloftype emit nothing.  The
 * parenthesised list is suppressed entirely when no column needs an
 * override and there are no locally-declared CHECK constraints, matching
 * the canonical "CREATE TABLE x OF t;" shape.
 */
static void
append_typed_column_overrides(StringInfo buf, Relation rel, bool pretty,
							  bool include_check,
							  LocalNotNullEntry *nn_entries)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	List	   *dpcontext = NIL;
	StringInfoData inner;
	bool		first = true;

	initStringInfo(&inner);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		char	   *defexpr = NULL;
		bool		has_default;
		bool		has_notnull;

		if (att->attisdropped)
			continue;

		if (att->atthasdef)
			defexpr = find_attrdef_text(rel, att->attnum, &dpcontext);

		has_default = (defexpr != NULL);
		has_notnull = att->attnotnull;

		if (!has_default && !has_notnull)
		{
			if (defexpr)
				pfree(defexpr);
			continue;
		}

		if (!first)
			appendStringInfoChar(&inner, ',');
		if (pretty)
			appendStringInfoString(&inner, "\n    ");
		else if (!first)
			appendStringInfoChar(&inner, ' ');
		first = false;

		appendStringInfo(&inner, "%s WITH OPTIONS",
						 quote_identifier(NameStr(att->attname)));
		if (has_default)
			appendStringInfo(&inner, " DEFAULT %s", defexpr);
		if (has_notnull)
		{
			LocalNotNullEntry *nn = &nn_entries[att->attnum];

			if (nn->name != NULL && !nn->is_auto)
				appendStringInfo(&inner, " CONSTRAINT %s NOT NULL",
								 quote_identifier(nn->name));
			else
				appendStringInfoString(&inner, " NOT NULL");
			if (nn->name != NULL && nn->no_inherit)
				appendStringInfoString(&inner, " NO INHERIT");
		}

		if (defexpr)
			pfree(defexpr);
	}

	/*
	 * Locally-declared CHECK constraints on a typed table belong in the
	 * column-list parentheses, same as for an untyped table.  The out-of-line
	 * constraint loop later still skips CHECKs.
	 */
	if (include_check)
		append_inline_check_constraints(&inner, rel, pretty, &first);

	if (!first)
	{
		appendStringInfoString(buf, " (");
		appendStringInfoString(buf, inner.data);
		if (pretty)
			appendStringInfoString(buf, "\n)");
		else
			appendStringInfoChar(buf, ')');
	}
	pfree(inner.data);
}

/*
 * append_stmt
 *		Push ctx->buf onto ctx->statements.
 *
 * Used for all DDL emissions.  When schema_qualified is false, the
 * active search_path has already been narrowed to the base schema, so
 * ruleutils helpers (pg_get_indexdef_ddl, pg_get_ruledef_ddl,
 * pg_get_constraintdef_body, pg_get_statisticsobjdef_ddl) produce
 * unqualified names for same-schema objects automatically.
 */
static void
append_stmt(TableDdlContext *ctx)
{
	ctx->statements = lappend(ctx->statements, pstrdup(ctx->buf.data));
}

/*
 * emit_create_table_stmt
 *		Build the leading CREATE TABLE statement, including persistence
 *		(TEMPORARY / UNLOGGED), body (column list / OF type_name /
 *		PARTITION OF parent), INHERITS, PARTITION BY, USING method,
 *		WITH (reloptions), TABLESPACE, and ON COMMIT.
 */
static void
emit_create_table_stmt(TableDdlContext *ctx)
{
	Relation	rel = ctx->rel;
	char		relpersistence = rel->rd_rel->relpersistence;
	char		relkind = rel->rd_rel->relkind;
	bool		is_typed = OidIsValid(rel->rd_rel->reloftype);
	HeapTuple	classtup;
	Datum		reloptDatum;
	bool		reloptIsnull;

	classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(ctx->relid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for relation %u", ctx->relid);

	reloptDatum = SysCacheGetAttr(RELOID, classtup,
								  Anum_pg_class_reloptions, &reloptIsnull);

	resetStringInfo(&ctx->buf);
	appendStringInfoString(&ctx->buf, "CREATE ");
	if (relpersistence == RELPERSISTENCE_TEMP)
		appendStringInfoString(&ctx->buf, "TEMPORARY ");
	else if (relpersistence == RELPERSISTENCE_UNLOGGED)
		appendStringInfoString(&ctx->buf, "UNLOGGED ");
	appendStringInfo(&ctx->buf, "TABLE %s", ctx->qualname);

	if (rel->rd_rel->relispartition)
	{
		Oid			parentOid = get_partition_parent(ctx->relid, true);
		char	   *parentQual = lookup_relname_for_emit(parentOid,
														 ctx->schema_qualified,
														 ctx->base_namespace);
		char	   *parentRelname = get_rel_name(parentOid);
		Datum		boundDatum;
		bool		boundIsnull;
		char	   *forValues = NULL;
		char	   *boundStr = NULL;

		if (parentRelname == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("partition parent with OID %u does not exist",
							parentOid),
					 errdetail("It may have been concurrently dropped.")));

		boundDatum = SysCacheGetAttr(RELOID, classtup,
									 Anum_pg_class_relpartbound, &boundIsnull);
		if (!boundIsnull)
		{
			Node	   *boundNode;
			List	   *dpcontext;

			boundStr = TextDatumGetCString(boundDatum);
			boundNode = stringToNode(boundStr);
			dpcontext = deparse_context_for(parentRelname, parentOid);
			forValues = deparse_expression(boundNode, dpcontext, false, false);
		}

		appendStringInfo(&ctx->buf, " PARTITION OF %s %s",
						 parentQual, forValues ? forValues : "DEFAULT");
		if (forValues)
			pfree(forValues);
		if (boundStr)
			pfree(boundStr);
		pfree(parentQual);
		pfree(parentRelname);
	}
	else if (is_typed)
	{
		char	   *typname = format_type_be_qualified(rel->rd_rel->reloftype);

		appendStringInfo(&ctx->buf, " OF %s", typname);
		pfree(typname);

		append_typed_column_overrides(&ctx->buf, rel, ctx->pretty,
									  is_kind_included(ctx, TABLE_DDL_KIND_CHECK),
									  ctx->nn_entries);
	}
	else
	{
		List	   *parents;
		ListCell   *lc;
		bool		first;

		appendStringInfoString(&ctx->buf, " (");

		append_column_defs(&ctx->buf, rel, ctx->pretty,
						   is_kind_included(ctx, TABLE_DDL_KIND_CHECK),
						   ctx->schema_qualified, ctx->nn_entries);

		if (ctx->pretty)
			appendStringInfoString(&ctx->buf, "\n)");
		else
			appendStringInfoChar(&ctx->buf, ')');

		parents = find_inheritance_parents(ctx->relid, NoLock);
		if (parents != NIL)
		{
			appendStringInfoString(&ctx->buf, " INHERITS (");
			first = true;
			foreach(lc, parents)
			{
				Oid			poid = lfirst_oid(lc);
				char	   *pname = lookup_relname_for_emit(poid,
															ctx->schema_qualified,
															ctx->base_namespace);

				if (!first)
					appendStringInfoString(&ctx->buf, ", ");
				first = false;
				appendStringInfoString(&ctx->buf, pname);
				pfree(pname);
			}
			appendStringInfoChar(&ctx->buf, ')');
			list_free(parents);
		}
	}

	if (relkind == RELKIND_PARTITIONED_TABLE)
	{
		Datum		partkeyDatum;
		char	   *partkey;

		partkeyDatum = OidFunctionCall1(F_PG_GET_PARTKEYDEF,
										ObjectIdGetDatum(ctx->relid));
		partkey = TextDatumGetCString(partkeyDatum);
		appendStringInfo(&ctx->buf, " PARTITION BY %s", partkey);
		pfree(partkey);
	}

	if (OidIsValid(rel->rd_rel->relam) &&
		rel->rd_rel->relam != HEAP_TABLE_AM_OID)
	{
		char	   *amname = get_am_name(rel->rd_rel->relam);

		if (amname != NULL)
		{
			appendStringInfo(&ctx->buf, " USING %s",
							 quote_identifier(amname));
			pfree(amname);
		}
	}

	if (!reloptIsnull)
	{
		appendStringInfoString(&ctx->buf, " WITH (");
		get_reloptions(&ctx->buf, reloptDatum);
		appendStringInfoChar(&ctx->buf, ')');
	}

	ReleaseSysCache(classtup);

	if (!ctx->no_tablespace && OidIsValid(rel->rd_rel->reltablespace))
	{
		char	   *tsname = get_tablespace_name(rel->rd_rel->reltablespace);

		if (tsname != NULL)
		{
			appendStringInfo(&ctx->buf, " TABLESPACE %s",
							 quote_identifier(tsname));
			pfree(tsname);
		}
	}

	if (relpersistence == RELPERSISTENCE_TEMP)
	{
		OnCommitAction oc = get_on_commit_action(ctx->relid);

		if (oc == ONCOMMIT_DELETE_ROWS)
			appendStringInfoString(&ctx->buf, " ON COMMIT DELETE ROWS");
		else if (oc == ONCOMMIT_DROP)
			appendStringInfoString(&ctx->buf, " ON COMMIT DROP");
	}

	appendStringInfoChar(&ctx->buf, ';');
	append_stmt(ctx);
}

/*
 * emit_owner_stmt
 *		ALTER TABLE qualname OWNER TO role.
 */
static void
emit_owner_stmt(TableDdlContext *ctx)
{
	char	   *owner;

	if (ctx->no_owner)
		return;

	owner = GetUserNameFromId(ctx->rel->rd_rel->relowner, false);
	resetStringInfo(&ctx->buf);
	appendStringInfo(&ctx->buf, "ALTER TABLE %s OWNER TO %s;",
					 ctx->qualname, quote_identifier(owner));
	append_stmt(ctx);
	pfree(owner);
}

/*
 * emit_child_default_overrides
 *		ALTER TABLE qualname ALTER COLUMN col SET DEFAULT expr - one per
 *		inherited (attislocal=false) non-generated column carrying a
 *		locally-set default.  Generated columns are skipped: their
 *		expression is inherited automatically and SET DEFAULT would
 *		fail at replay.
 */
static void
emit_child_default_overrides(TableDdlContext *ctx)
{
	TupleDesc	tupdesc = RelationGetDescr(ctx->rel);
	List	   *dpcontext = NIL;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		char	   *defstr;

		if (att->attisdropped || att->attislocal || !att->atthasdef)
			continue;
		if (att->attgenerated != '\0')
			continue;

		defstr = find_attrdef_text(ctx->rel, att->attnum, &dpcontext);
		if (defstr == NULL)
			continue;

		resetStringInfo(&ctx->buf);
		appendStringInfo(&ctx->buf,
						 "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
						 ctx->qualname,
						 quote_identifier(NameStr(att->attname)),
						 defstr);
		append_stmt(ctx);
		pfree(defstr);
	}
}

/*
 * emit_attoptions
 *		ALTER TABLE qualname ALTER COLUMN col SET (...) - one per column
 *		with non-null pg_attribute.attoptions.  The inline form of these
 *		options isn't available in CREATE TABLE, so they come out here.
 */
static void
emit_attoptions(TableDdlContext *ctx)
{
	TupleDesc	tupdesc = RelationGetDescr(ctx->rel);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		HeapTuple	attTup;
		Datum		optDatum;
		bool		optIsnull;

		if (att->attisdropped)
			continue;

		attTup = SearchSysCache2(ATTNUM,
								 ObjectIdGetDatum(ctx->relid),
								 Int16GetDatum(att->attnum));
		if (!HeapTupleIsValid(attTup))
			continue;

		optDatum = SysCacheGetAttr(ATTNUM, attTup,
								   Anum_pg_attribute_attoptions, &optIsnull);
		if (!optIsnull)
		{
			resetStringInfo(&ctx->buf);
			appendStringInfo(&ctx->buf,
							 "ALTER TABLE %s ALTER COLUMN %s SET (",
							 ctx->qualname,
							 quote_identifier(NameStr(att->attname)));
			get_reloptions(&ctx->buf, optDatum);
			appendStringInfoString(&ctx->buf, ");");
			append_stmt(ctx);
		}
		ReleaseSysCache(attTup);
	}
}

/*
 * emit_indexes
 *		CREATE INDEX per non-constraint-backed index on the relation.
 *		Indexes that back PK / UNIQUE / EXCLUDE constraints are emitted
 *		out-of-line by emit_local_constraints (the ALTER TABLE ... ADD
 *		CONSTRAINT statement creates the index implicitly).
 */
static void
emit_indexes(TableDdlContext *ctx)
{
	List	   *indexoids;
	ListCell   *lc;

	if (!is_kind_included(ctx, TABLE_DDL_KIND_INDEX))
		return;

	indexoids = RelationGetIndexList(ctx->rel);
	foreach(lc, indexoids)
	{
		Oid			idxoid = lfirst_oid(lc);
		char	   *idxdef;

		if (OidIsValid(get_index_constraint(idxoid)))
			continue;

		idxdef = pg_get_indexdef_ddl(idxoid);
		resetStringInfo(&ctx->buf);
		appendStringInfo(&ctx->buf, "%s;", idxdef);
		append_stmt(ctx);
		pfree(idxdef);
	}
	list_free(indexoids);
}

/*
 * emit_local_constraints
 *		ALTER TABLE ... ADD CONSTRAINT for each locally-defined constraint
 *		on the relation.  Inherited constraints (conislocal=false) come
 *		from the parent's DDL.  CHECK constraints are emitted inline for
 *		regular/typed tables (skip here) but out-of-line for partition
 *		children (no column list to live in).  Local NOT NULL
 *		constraints on attislocal columns - whether user-named or
 *		matching the auto-name pattern - are emitted inline by the
 *		column-emit helpers and are skipped via skip_notnull_oids,
 *		because PG only allows one NOT NULL per column and would reject
 *		a second ALTER TABLE ... ADD CONSTRAINT.  Partition children
 *		have no column list, so their NOT NULLs fall through here.
 *
 *		Each contype is gated on the matching kind in the only / except
 *		vocabulary, so callers can produce e.g. an FK-only pass
 *		(only => 'foreign_key') or a pub/sub clone that keeps only
 *		the primary key (except => 'unique,check,foreign_key,exclusion').
 *		NOT NULL is intentionally not in the vocabulary - always emitted
 *		so cloned schemas don't silently accept NULLs the source would
 *		have rejected.
 */
static void
emit_local_constraints(TableDdlContext *ctx)
{
	Relation	conRel;
	SysScanDesc conScan;
	ScanKeyData conKey;
	HeapTuple	conTup;
	bool		is_partition = ctx->rel->rd_rel->relispartition;
	List	   *fk_stmts = NIL;

	conRel = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&conKey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ctx->relid));
	conScan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId,
								 true, NULL, 1, &conKey);

	while (HeapTupleIsValid(conTup = systable_getnext(conScan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);

		if (!con->conislocal)
			continue;

		/*
		 * Each contype is gated on its kind in the only / except
		 * vocabulary.  CHECK is also skipped for non-partition relations
		 * because append_inline_check_constraints emits those inline in
		 * the CREATE TABLE body; partition children have no column list
		 * to live in, so they come through this loop.  NOT NULL is not a
		 * filterable kind - emitted unconditionally to avoid producing
		 * schemas that silently accept NULLs the source would have
		 * rejected.
		 */
		switch (con->contype)
		{
			case CONSTRAINT_PRIMARY:
				if (!is_kind_included(ctx, TABLE_DDL_KIND_PRIMARY_KEY))
					continue;
				break;
			case CONSTRAINT_UNIQUE:
				if (!is_kind_included(ctx, TABLE_DDL_KIND_UNIQUE))
					continue;
				break;
			case CONSTRAINT_CHECK:
				if (!is_kind_included(ctx, TABLE_DDL_KIND_CHECK))
					continue;

				/*
				 * For non-partition relations, CHECK is normally emitted
				 * inline in the CREATE TABLE body by
				 * append_inline_check_constraints, so we skip it here to
				 * avoid double emission.  But when KIND_TABLE is not in
				 * the active filter (e.g. only=>'check' or a clone that
				 * targets only sub-objects), the inline pass never runs;
				 * fall through and emit each CHECK via ALTER TABLE so the
				 * user actually gets the constraint they asked for.
				 */
				if (!is_partition &&
					is_kind_included(ctx, TABLE_DDL_KIND_TABLE))
					continue;
				break;
			case CONSTRAINT_FOREIGN:
				if (!is_kind_included(ctx, TABLE_DDL_KIND_FOREIGN_KEY))
					continue;
				break;
			case CONSTRAINT_EXCLUSION:
				if (!is_kind_included(ctx, TABLE_DDL_KIND_EXCLUSION))
					continue;
				break;
			case CONSTRAINT_NOTNULL:
				/*
				 * Out-of-line NOT NULL is conceptually part of the
				 * table definition: gating it on KIND_TABLE means an
				 * only=foreign_key second pass does not re-emit
				 * NOT NULLs that the first pass already created,
				 * while the no-options default still emits them.
				 */
				if (!is_kind_included(ctx, TABLE_DDL_KIND_TABLE))
					continue;
				if (!is_partition &&
					list_member_oid(ctx->skip_notnull_oids, con->oid))
					continue;
				break;
			default:
				/*
				 * Any future contype the vocabulary does not yet cover:
				 * fall through and emit it via pg_get_constraintdef_command,
				 * matching the original loop's "emit unless filtered"
				 * behavior.  This is unreachable in current PG (every
				 * contype above is enumerated) but kept defensive against
				 * a new contype being introduced.
				 */
				break;
		}

		{
			char	   *conbody = pg_get_constraintdef_body(con->oid);

			resetStringInfo(&ctx->buf);
			appendStringInfo(&ctx->buf, "ALTER TABLE %s ADD CONSTRAINT %s %s;",
							 ctx->qualname,
							 quote_identifier(NameStr(con->conname)),
							 conbody);

			/*
			 * Defer FK statements so they are emitted after all other
			 * constraints.  A self-referencing FK (REFERENCES same_table)
			 * requires the PK/UNIQUE it targets to exist first, and because
			 * the catalog scan returns constraints in name order, an FK whose
			 * name sorts before the PK name would otherwise be emitted first
			 * and fail with "there is no unique constraint matching given
			 * keys".
			 */
			if (con->contype == CONSTRAINT_FOREIGN)
				fk_stmts = lappend(fk_stmts, pstrdup(ctx->buf.data));
			else
				append_stmt(ctx);

			pfree(conbody);
		}
	}
	systable_endscan(conScan);
	table_close(conRel, AccessShareLock);

	/* Append deferred FK statements after all other constraints. */
	ctx->statements = list_concat(ctx->statements, fk_stmts);
}

/*
 * emit_rules
 *		CREATE RULE per cached rewrite rule on the relation.
 */
static void
emit_rules(TableDdlContext *ctx)
{
	if (!is_kind_included(ctx, TABLE_DDL_KIND_RULE) ||
		ctx->rel->rd_rules == NULL)
		return;

	for (int i = 0; i < ctx->rel->rd_rules->numLocks; i++)
	{
		Oid			ruleid = ctx->rel->rd_rules->rules[i]->ruleId;
		char	   *ruledef_str;

		ruledef_str = pg_get_ruledef_ddl(ruleid);
		resetStringInfo(&ctx->buf);
		appendStringInfoString(&ctx->buf, ruledef_str);
		append_stmt(ctx);
		pfree(ruledef_str);
	}
}

/*
 * emit_statistics
 *		CREATE STATISTICS per extended statistics object on the relation.
 */
static void
emit_statistics(TableDdlContext *ctx)
{
	Relation	statRel;
	SysScanDesc statScan;
	ScanKeyData statKey;
	HeapTuple	statTup;

	if (!is_kind_included(ctx, TABLE_DDL_KIND_STATISTICS))
		return;

	statRel = table_open(StatisticExtRelationId, AccessShareLock);
	ScanKeyInit(&statKey,
				Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ctx->relid));
	statScan = systable_beginscan(statRel, StatisticExtRelidIndexId,
								  true, NULL, 1, &statKey);

	while (HeapTupleIsValid(statTup = systable_getnext(statScan)))
	{
		Form_pg_statistic_ext stat = (Form_pg_statistic_ext) GETSTRUCT(statTup);
		char	   *statdef = pg_get_statisticsobjdef_ddl(stat->oid);

		resetStringInfo(&ctx->buf);
		appendStringInfo(&ctx->buf, "%s;", statdef);
		append_stmt(ctx);
		pfree(statdef);
	}
	systable_endscan(statScan);
	table_close(statRel, AccessShareLock);
}

/*
 * emit_replica_identity
 *		ALTER TABLE qualname REPLICA IDENTITY ... - emitted only when the
 *		relreplident differs from the default ('d' = use primary key).
 */
static void
emit_replica_identity(TableDdlContext *ctx)
{
	if (!is_kind_included(ctx, TABLE_DDL_KIND_REPLICA_IDENTITY))
		return;
	if (ctx->rel->rd_rel->relreplident == REPLICA_IDENTITY_DEFAULT)
		return;

	resetStringInfo(&ctx->buf);
	switch (ctx->rel->rd_rel->relreplident)
	{
		case REPLICA_IDENTITY_NOTHING:
			appendStringInfo(&ctx->buf,
							 "ALTER TABLE %s REPLICA IDENTITY NOTHING;",
							 ctx->qualname);
			append_stmt(ctx);
			break;
		case REPLICA_IDENTITY_FULL:
			appendStringInfo(&ctx->buf,
							 "ALTER TABLE %s REPLICA IDENTITY FULL;",
							 ctx->qualname);
			append_stmt(ctx);
			break;
		case REPLICA_IDENTITY_INDEX:
			{
				Oid			replidx = RelationGetReplicaIndex(ctx->rel);

				if (OidIsValid(replidx))
				{
					char	   *idxname = get_rel_name(replidx);

					if (idxname == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("replica identity index with OID %u does not exist",
										replidx),
								 errdetail("It may have been concurrently dropped.")));

					appendStringInfo(&ctx->buf,
									 "ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;",
									 ctx->qualname,
									 quote_identifier(idxname));
					append_stmt(ctx);
					pfree(idxname);
				}
			}
			break;
	}
}

/*
 * emit_rls_toggles
 *		ALTER TABLE qualname ENABLE / FORCE ROW LEVEL SECURITY.
 */
static void
emit_rls_toggles(TableDdlContext *ctx)
{
	if (!is_kind_included(ctx, TABLE_DDL_KIND_RLS))
		return;

	if (ctx->rel->rd_rel->relrowsecurity)
	{
		resetStringInfo(&ctx->buf);
		appendStringInfo(&ctx->buf,
						 "ALTER TABLE %s ENABLE ROW LEVEL SECURITY;",
						 ctx->qualname);
		append_stmt(ctx);
	}
	if (ctx->rel->rd_rel->relforcerowsecurity)
	{
		resetStringInfo(&ctx->buf);
		appendStringInfo(&ctx->buf,
						 "ALTER TABLE %s FORCE ROW LEVEL SECURITY;",
						 ctx->qualname);
		append_stmt(ctx);
	}
}

/*
 * emit_partition_children
 *		For each direct partition child of a partitioned-table parent,
 *		recursively call pg_get_table_ddl_internal and append the child's
 *		statements.  Each child's own DDL handles further levels through
 *		the same recursion.
 */
static void
emit_partition_children(TableDdlContext *ctx)
{
	List	   *children;
	ListCell   *lc;

	if (!is_kind_included(ctx, TABLE_DDL_KIND_PARTITION) ||
		ctx->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return;

	children = find_inheritance_children(ctx->relid, AccessShareLock);
	foreach(lc, children)
	{
		Oid			childoid = lfirst_oid(lc);
		TableDdlContext childctx = {0};
		List	   *childstmts;

		/*
		 * Each recursive invocation re-derives its own rel, namespace,
		 * qualname, NOT NULL cache, buffer, and statement list inside
		 * pg_get_table_ddl_internal.  The user-supplied option fields
		 * carry over verbatim with one exception: KIND_PARTITION is a
		 * "gate" kind that controls whether we recurse at all, not a
		 * kind that the children themselves ever emit.  If we propagated
		 * an only-set that contained PARTITION into a child, the child
		 * would not emit its own CREATE TABLE (KIND_TABLE absent) nor
		 * any sub-objects, and the recursion would produce nothing.  So
		 * strip PARTITION out of only_kinds when recursing; if that
		 * empties the set, drop the filter entirely so the child emits
		 * its full DDL.  except_kinds passes through unchanged because
		 * PARTITION in the except-set already stopped us from getting
		 * here.
		 */
		childctx.relid = childoid;
		childctx.pretty = ctx->pretty;
		childctx.no_owner = ctx->no_owner;
		childctx.no_tablespace = ctx->no_tablespace;
		childctx.schema_qualified = ctx->schema_qualified;
		childctx.only_kinds = ctx->only_kinds;
		childctx.except_kinds = ctx->except_kinds;

		/*
		 * When schema_qualified is false, the contract is that the output
		 * is replayable with the parent's schema in search_path.  That
		 * contract cannot hold for a child that lives in a different schema:
		 * its own name would be ambiguous (landing in whatever schema is
		 * first in search_path) and references back to the parent would have
		 * to be schema-qualified anyway.  Force full qualification so the
		 * child's DDL is unambiguous regardless of the caller's search_path.
		 */
		if (!childctx.schema_qualified &&
			get_rel_namespace(childoid) != ctx->base_namespace)
			childctx.schema_qualified = true;

		if (childctx.only_kinds != NULL &&
			bms_is_member((int) TABLE_DDL_KIND_PARTITION,
						  childctx.only_kinds))
		{
			Bitmapset  *child_only = bms_copy(childctx.only_kinds);

			child_only = bms_del_member(child_only,
										(int) TABLE_DDL_KIND_PARTITION);
			if (bms_is_empty(child_only))
			{
				bms_free(child_only);
				childctx.only_kinds = NULL;
			}
			else
				childctx.only_kinds = child_only;
		}

		childstmts = pg_get_table_ddl_internal(&childctx);
		ctx->statements = list_concat(ctx->statements, childstmts);
	}
	list_free(children);
}

/*
 * pg_get_table_ddl_internal
 *		Generate DDL statements to recreate a regular or partitioned table.
 *
 * The caller initializes *ctx with the user-supplied option fields
 * (relid, pretty, no_owner, no_tablespace, schema_qualified,
 * only_kinds, except_kinds).  This function opens the relation,
 * validates access, populates the derived fields (rel, qualname,
 * nn_entries, ...), runs the emission passes, and returns the
 * accumulated statement list.
 *
 * Each emission helper consults is_kind_included() to decide whether
 * it should run.  The table-proper passes (CREATE TABLE / OWNER /
 * ALTER COLUMN ... SET DEFAULT / ALTER COLUMN ... SET (...)) are
 * grouped under KIND_TABLE so the FK-only / sub-object-only workflow
 * is expressible as a single only or except list.
 *
 * Trigger and policy emission are scaffolded but currently disabled
 * (#if 0) - they will become a single helper call once the standalone
 * pg_get_trigger_ddl / pg_get_policy_ddl helpers land.
 */
static List *
pg_get_table_ddl_internal(TableDdlContext *ctx)
{
	Relation	rel;
	char		relkind;
	AclResult	aclresult;

	rel = table_open(ctx->relid, AccessShareLock);

	relkind = rel->rd_rel->relkind;

	/*
	 * The initial cut only supports ordinary and partitioned tables.  Views,
	 * matviews, foreign tables, sequences, indexes, composite types, and
	 * TOAST tables are out of scope for now.
	 */
	if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
	{
		char	   *relname = pstrdup(RelationGetRelationName(rel));

		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an ordinary or partitioned table",
						relname)));
	}

	/* Caller needs SELECT on the table to read its definition. */
	aclresult = pg_class_aclcheck(ctx->relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE,
					   RelationGetRelationName(rel));

	/*
	 * Validation: if the table has REPLICA IDENTITY USING INDEX and the
	 * referenced index would not be emitted under the active filter, the
	 * emitted REPLICA IDENTITY clause would reference an index the same DDL
	 * never produced.  Determine which kind would emit the index (one of
	 * primary_key / unique / exclusion for constraint-backed indexes,
	 * otherwise the generic "index" kind) and require it to be in scope
	 * whenever "replica_identity" is.  The check uses is_kind_included so
	 * it covers both forms naturally: an "except" list that omits the
	 * source kind, and an "only" list that omits it.
	 */
	if (rel->rd_rel->relreplident == REPLICA_IDENTITY_INDEX &&
		is_kind_included(ctx, TABLE_DDL_KIND_REPLICA_IDENTITY))
	{
		Oid			replidx = RelationGetReplicaIndex(rel);

		if (OidIsValid(replidx))
		{
			TableDdlKind idx_kind = TABLE_DDL_KIND_INDEX;
			Oid			conoid = get_index_constraint(replidx);

			if (OidIsValid(conoid))
			{
				HeapTuple	conTup = SearchSysCache1(CONSTROID,
													 ObjectIdGetDatum(conoid));

				if (HeapTupleIsValid(conTup))
				{
					Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);

					switch (con->contype)
					{
						case CONSTRAINT_PRIMARY:
							idx_kind = TABLE_DDL_KIND_PRIMARY_KEY;
							break;
						case CONSTRAINT_UNIQUE:
							idx_kind = TABLE_DDL_KIND_UNIQUE;
							break;
						case CONSTRAINT_EXCLUSION:
							idx_kind = TABLE_DDL_KIND_EXCLUSION;
							break;
						default:
							break;
					}
					ReleaseSysCache(conTup);
				}
			}

			if (!is_kind_included(ctx, idx_kind))
			{
				char	   *relname = pstrdup(RelationGetRelationName(rel));
				const char *idx_name = table_ddl_kind_names[(int) idx_kind].name;

				table_close(rel, AccessShareLock);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("REPLICA IDENTITY for table \"%s\" requires kind \"%s\" to be emitted",
								relname, idx_name),
						 errdetail("The table's REPLICA IDENTITY USING INDEX references an index produced by the \"%s\" kind, which is not in the active filter.",
								   idx_name),
						 errhint("Either add \"%s\" to the filter or remove \"replica_identity\" from it.",
								 idx_name)));
			}
		}
	}

	/*
	 * Populate derived fields now that the relation is open and validated.
	 * The remaining derived fields (nn_entries, skip_notnull_oids, buf,
	 * statements) start zeroed via the caller's `TableDdlContext ctx = {0}`
	 * initializer; the NOT NULL cache is populated by collect_local_not_null,
	 * the buffer is initStringInfo'd, and each emit pass appends to
	 * statements via lappend.
	 */
	ctx->rel = rel;
	ctx->base_namespace = RelationGetNamespace(rel);
	ctx->save_nestlevel = -1;
	ctx->qualname = lookup_relname_for_emit(ctx->relid, ctx->schema_qualified,
											ctx->base_namespace);

	/*
	 * Temporarily override search_path so that the ruleutils helpers
	 * (pg_get_indexdef_ddl, pg_get_constraintdef_body,
	 * pg_get_statisticsobjdef_ddl, pg_get_ruledef_ddl, etc.) produce names
	 * that match the schema_qualified flag.  Those helpers decide whether to
	 * qualify a name by calling RelationIsVisible(), which checks whether the
	 * object's schema appears in the active search_path.
	 *
	 * schema_qualified = false: narrow to the base schema so that same-schema
	 * references in DEFAULT expressions, FK targets, indexes, rules, and
	 * statistics come out unqualified automatically.  Cross-schema references
	 * stay qualified, which is the correctness requirement.
	 *
	 * schema_qualified = true: narrow to pg_catalog only so that objects in
	 * the base schema (or any user schema the caller placed on search_path)
	 * are not reachable without qualification, forcing fully-qualified output
	 * from every helper regardless of the caller's session search_path.
	 *
	 * AtEOXact_GUC cleans up at xact end if anything throws between here and
	 * the explicit restore below; on the normal path we restore right before
	 * returning.
	 */
	if (!ctx->schema_qualified)
	{
		char	   *nspname = get_namespace_name(ctx->base_namespace);

		if (nspname != NULL)
		{
			const char *qnsp = quote_identifier(nspname);

			ctx->save_nestlevel = NewGUCNestLevel();
			(void) set_config_option("search_path", qnsp,
									 PGC_USERSET, PGC_S_SESSION,
									 GUC_ACTION_SAVE, true, 0, false);
			if (qnsp != nspname)
				pfree((char *) qnsp);
			pfree(nspname);
		}
	}
	else
	{
		ctx->save_nestlevel = NewGUCNestLevel();
		(void) set_config_option("search_path", "pg_catalog",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	}

	/*
	 * Cache locally-declared NOT NULL constraint metadata so the column- emit
	 * helpers can produce "CONSTRAINT name NOT NULL" inline for user-named
	 * constraints, and so the constraint loop can avoid double-emitting them.
	 */
	ctx->nn_entries = collect_local_not_null(rel, &ctx->skip_notnull_oids);

	initStringInfo(&ctx->buf);

	/*
	 * Emission passes.  Order is significant: CREATE TABLE first; OWNER and
	 * the per-column ALTER COLUMN passes before sub-object emission;
	 * sub-objects in dependency-friendly order (indexes before constraints,
	 * since constraint-backed indexes are emitted out-of-line by the
	 * constraint loop); partition children last so the parent already exists
	 * at replay time.  Each helper gates itself on is_kind_included for the
	 * relevant TABLE_DDL_KIND_*; the four "table itself" passes are grouped
	 * here under KIND_TABLE so all of CREATE TABLE / OWNER / SET DEFAULT /
	 * SET (...) drop out together when the user asks for only sub-objects
	 * (e.g. only => 'foreign_key' for the second pass of a two-pass FK
	 * clone).
	 */
	if (is_kind_included(ctx, TABLE_DDL_KIND_TABLE))
	{
		emit_create_table_stmt(ctx);
		emit_owner_stmt(ctx);
		emit_child_default_overrides(ctx);
		emit_attoptions(ctx);
	}
	emit_indexes(ctx);
	emit_local_constraints(ctx);
	emit_rules(ctx);
	emit_statistics(ctx);
	emit_replica_identity(ctx);
	emit_rls_toggles(ctx);

	/*
	 * Triggers and row-level security policies - disabled until the
	 * standalone pg_get_trigger_ddl() and pg_get_policy_ddl() helpers land.
	 * The scan + lock + filter scaffolding below is preserved (inside #if 0)
	 * so wiring up each emission becomes a one-liner in the loop body once
	 * those helpers are available.
	 */
#if 0
	if (is_kind_included(ctx, TABLE_DDL_KIND_TRIGGER))
	{
		Relation	trigRel;
		SysScanDesc trigScan;
		ScanKeyData trigKey;
		HeapTuple	trigTup;

		trigRel = table_open(TriggerRelationId, AccessShareLock);
		ScanKeyInit(&trigKey,
					Anum_pg_trigger_tgrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(ctx->relid));
		trigScan = systable_beginscan(trigRel, TriggerRelidNameIndexId,
									  true, NULL, 1, &trigKey);
		while (HeapTupleIsValid(trigTup = systable_getnext(trigScan)))
		{
			Form_pg_trigger trg = (Form_pg_trigger) GETSTRUCT(trigTup);

			if (trg->tgisinternal)
				continue;

			/* TODO: append pg_get_trigger_ddl(trg->oid) output here. */
		}
		systable_endscan(trigScan);
		table_close(trigRel, AccessShareLock);
	}

	if (is_kind_included(ctx, TABLE_DDL_KIND_POLICY))
	{
		Relation	polRel;
		SysScanDesc polScan;
		ScanKeyData polKey;
		HeapTuple	polTup;

		polRel = table_open(PolicyRelationId, AccessShareLock);
		ScanKeyInit(&polKey,
					Anum_pg_policy_polrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(ctx->relid));
		polScan = systable_beginscan(polRel, PolicyPolrelidPolnameIndexId,
									 true, NULL, 1, &polKey);
		while (HeapTupleIsValid(polTup = systable_getnext(polScan)))
		{
			/* TODO: append pg_get_policy_ddl(relid, polname) output here. */
		}
		systable_endscan(polScan);
		table_close(polRel, AccessShareLock);
	}
#endif

	emit_partition_children(ctx);

	{
		int			natts = RelationGetDescr(rel)->natts;

		for (int i = 1; i <= natts; i++)
			if (ctx->nn_entries[i].name != NULL)
				pfree(ctx->nn_entries[i].name);
	}
	pfree(ctx->nn_entries);

	table_close(rel, AccessShareLock);
	pfree(ctx->buf.data);
	pfree(ctx->qualname);
	list_free(ctx->skip_notnull_oids);

	/*
	 * Pop the narrowed search_path now that all helpers have run.  Errors
	 * thrown earlier are cleaned up by AtEOXact_GUC at xact end.
	 */
	if (ctx->save_nestlevel >= 0)
		AtEOXact_GUC(true, ctx->save_nestlevel);

	return ctx->statements;
}

/*
 * pg_get_table_ddl
 *		Return DDL to recreate a table as a set of text rows.
 */
Datum
pg_get_table_ddl(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	List	   *statements;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TableDdlContext ctx = {0};

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (PG_ARGISNULL(0))
		{
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		if (!PG_ARGISNULL(5) && !PG_ARGISNULL(6))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"only_kinds\" and \"except_kinds\" parameters are mutually exclusive")));

		/* Option defaults (match proargdefaults in pg_proc.dat). */
		ctx.relid = PG_GETARG_OID(0);
		ctx.pretty = false;
		ctx.no_owner = false;		/* owner DEFAULT true  -> no_owner = false */
		ctx.no_tablespace = false;	/* tablespace DEFAULT true -> no_tablespace = false */
		ctx.schema_qualified = true;
		ctx.only_kinds = NULL;
		ctx.except_kinds = NULL;

		/* Override defaults with any explicitly supplied values. */
		if (!PG_ARGISNULL(1))
			ctx.pretty = PG_GETARG_BOOL(1);
		if (!PG_ARGISNULL(2))
			ctx.no_owner = !PG_GETARG_BOOL(2);
		if (!PG_ARGISNULL(3))
			ctx.no_tablespace = !PG_GETARG_BOOL(3);
		if (!PG_ARGISNULL(4))
			ctx.schema_qualified = PG_GETARG_BOOL(4);
		if (!PG_ARGISNULL(5))
			ctx.only_kinds = parse_kind_array("only_kinds",
											  PG_GETARG_ARRAYTYPE_P(5));
		if (!PG_ARGISNULL(6))
			ctx.except_kinds = parse_kind_array("except_kinds",
												PG_GETARG_ARRAYTYPE_P(6));

		statements = pg_get_table_ddl_internal(&ctx);
		funcctx->user_fctx = statements;
		funcctx->max_calls = list_length(statements);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	statements = (List *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		char	   *stmt;

		stmt = list_nth(statements, funcctx->call_cntr);

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(stmt));
	}
	else
	{
		list_free_deep(statements);
		SRF_RETURN_DONE(funcctx);
	}
}
