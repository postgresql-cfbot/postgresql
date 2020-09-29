/*-------------------------------------------------------------------------
 *
 * ri_triggers.c
 *
 *	Generic trigger procedures for referential integrity constraint
 *	checks.
 *
 *	Note about memory management: the private hashtables kept here live
 *	across query and transaction boundaries, in fact they live as long as
 *	the backend does.  This works because the hashtable structures
 *	themselves are allocated by dynahash.c in its permanent DynaHashCxt,
 *	and the SPI plans they point to are saved using SPI_keepplan().
 *	There is not currently any provision for throwing away a no-longer-needed
 *	plan --- consider improving this someday.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/ri_triggers.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * Local definitions
 */

#define RI_MAX_NUMKEYS					INDEX_MAX_KEYS

#define RI_INIT_CONSTRAINTHASHSIZE		64
#define RI_INIT_QUERYHASHSIZE			(RI_INIT_CONSTRAINTHASHSIZE * 4)

#define RI_KEYS_ALL_NULL				0
#define RI_KEYS_SOME_NULL				1
#define RI_KEYS_NONE_NULL				2

/* RI query type codes */
/* these queries are executed against the PK (referenced) table: */
#define RI_PLAN_CHECK_LOOKUPPK_SINGLE	1	/* check single row  */
#define RI_PLAN_CHECK_LOOKUPPK_INS		2
#define RI_PLAN_CHECK_LOOKUPPK_UPD		3
#define RI_PLAN_CHECK_LOOKUPPK_FROM_PK	4
#define RI_PLAN_LAST_ON_PK				RI_PLAN_CHECK_LOOKUPPK_FROM_PK
/* these queries are executed against the FK (referencing) table: */
#define RI_PLAN_CASCADE_DEL_DODELETE	5
#define RI_PLAN_CASCADE_UPD_DOUPDATE	6
#define RI_PLAN_RESTRICT_CHECKREF		7
#define RI_PLAN_RESTRICT_CHECKREF_NO_ACTION		8
#define RI_PLAN_SETNULL_DOUPDATE		9
#define RI_PLAN_SETDEFAULT_DOUPDATE		10

#define MAX_QUOTED_NAME_LEN  (NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN  (MAX_QUOTED_NAME_LEN*2)

#define RIAttName(rel, attnum)	NameStr(*attnumAttName(rel, attnum))
#define RIAttType(rel, attnum)	attnumTypeId(rel, attnum)
#define RIAttCollation(rel, attnum) attnumCollationId(rel, attnum)

#define RI_TRIGTYPE_INSERT 1
#define RI_TRIGTYPE_UPDATE 2
#define RI_TRIGTYPE_DELETE 3


/*
 * RI_ConstraintInfo
 *
 * Information extracted from an FK pg_constraint entry.  This is cached in
 * ri_constraint_cache.
 */
typedef struct RI_ConstraintInfo
{
	Oid			constraint_id;	/* OID of pg_constraint entry (hash key) */
	bool		valid;			/* successfully initialized? */
	uint32		oidHashValue;	/* hash value of pg_constraint OID */
	NameData	conname;		/* name of the FK constraint */
	Oid			pk_relid;		/* referenced relation */
	Oid			fk_relid;		/* referencing relation */
	char		confupdtype;	/* foreign key's ON UPDATE action */
	char		confdeltype;	/* foreign key's ON DELETE action */
	char		confmatchtype;	/* foreign key's match type */
	int			nkeys;			/* number of key columns */
	int16		pk_attnums[RI_MAX_NUMKEYS]; /* attnums of referenced cols */
	int16		fk_attnums[RI_MAX_NUMKEYS]; /* attnums of referencing cols */
	Oid			pf_eq_oprs[RI_MAX_NUMKEYS]; /* equality operators (PK = FK) */
	Oid			pp_eq_oprs[RI_MAX_NUMKEYS]; /* equality operators (PK = PK) */
	Oid			ff_eq_oprs[RI_MAX_NUMKEYS]; /* equality operators (FK = FK) */
	TupleTableSlot *slot_pk;	/* slot for PK attributes */
	TupleTableSlot *slot_fk;	/* slot for FK attributes */
	TupleTableSlot *slot_both;	/* Both OLD an NEW version of PK table row. */
	MemoryContext slot_mcxt;	/* the slots will exist in this context  */
	dlist_node	valid_link;		/* Link in list of valid entries */
} RI_ConstraintInfo;

/*
 * RI_QueryKey
 *
 * The key identifying a prepared SPI plan in our query hashtable
 */
typedef struct RI_QueryKey
{
	Oid			constr_id;		/* OID of pg_constraint entry */
	int32		constr_queryno; /* query type ID, see RI_PLAN_XXX above */
	bool		single_row;		/* Checking a single row? */
} RI_QueryKey;

/*
 * RI_QueryHashEntry
 */
typedef struct RI_QueryHashEntry
{
	RI_QueryKey key;
	SPIPlanPtr	plan;
} RI_QueryHashEntry;

/*
 * RI_CompareKey
 *
 * The key identifying an entry showing how to compare two values
 */
typedef struct RI_CompareKey
{
	Oid			eq_opr;			/* the equality operator to apply */
	Oid			typeid;			/* the data type to apply it to */
} RI_CompareKey;

/*
 * RI_CompareHashEntry
 */
typedef struct RI_CompareHashEntry
{
	RI_CompareKey key;
	bool		valid;			/* successfully initialized? */
	FmgrInfo	eq_opr_finfo;	/* call info for equality fn */
	FmgrInfo	cast_func_finfo;	/* in case we must coerce input */
} RI_CompareHashEntry;


/*
 * Local data
 */
static HTAB *ri_constraint_cache = NULL;
static HTAB *ri_query_cache = NULL;
static HTAB *ri_compare_cache = NULL;
static dlist_head ri_constraint_cache_valid_list;
static int	ri_constraint_cache_valid_count = 0;


/*
 * Local function prototypes
 */
static char *RI_FKey_check_query(const RI_ConstraintInfo *riinfo,
								 Relation fk_rel, Relation pk_rel,
								 bool insert);
static char *RI_FKey_check_query_single_row(const RI_ConstraintInfo *riinfo,
											Relation fk_rel, Relation pk_rel,
											Oid *paramtypes);
static bool RI_FKey_check_query_required(Trigger *trigger, Relation fk_rel,
										 TupleTableSlot *newslot);
static bool ri_Check_Pk_Match(Relation pk_rel, Relation fk_rel,
							  TupleTableSlot *oldslot,
							  const RI_ConstraintInfo *riinfo);
static Datum ri_restrict(TriggerData *trigdata, bool is_no_action);
static char *ri_restrict_query(const RI_ConstraintInfo *riinfo,
							   Relation fk_rel, Relation pk_rel,
							   bool no_action);
static char *ri_restrict_query_single_row(const RI_ConstraintInfo *riinfo,
										  Relation fk_rel,
										  Relation pk_rel, Oid *paramtypes);
static char *ri_cascade_del_query(const RI_ConstraintInfo *riinfo,
								  Relation fk_rel, Relation pk_rel);
static char *ri_cascade_del_query_single_row(const RI_ConstraintInfo *riinfo,
											 Relation fk_rel, Relation pk_rel,
											 Oid *paramtypes);
static char *ri_cascade_upd_query(const RI_ConstraintInfo *riinfo,
								  Relation fk_rel, Relation pk_rel);
static char *ri_cascade_upd_query_single_row(const RI_ConstraintInfo *riinfo,
											 Relation fk_rel, Relation pk_rel,
											 Oid *paramtypes);
static Datum ri_set(TriggerData *trigdata, bool is_set_null);
static char *ri_set_query(const RI_ConstraintInfo *riinfo, Relation fk_rel,
						  Relation pk_rel, bool is_set_null);
static char *ri_set_query_single_row(const RI_ConstraintInfo *riinfo,
									 Relation fk_rel, Relation pk_rel,
									 Oid *paramtypes, bool is_set_null);
static void quoteOneName(char *buffer, const char *name);
static void quoteRelationName(char *buffer, Relation rel);
static char *ri_ColNameQuoted(const char *tabname, const char *attname);

/*
 * Use one of these values to tell ri_GenerateQual() where the parameter
 * markers ($1, $2, ...) should appear in the qualifier.
 */
typedef enum GenQualParams
{
	GQ_PARAMS_NONE,				/* No parameters, only attribute names. */
	GQ_PARAMS_LEFT,				/* The left side of the qual contains
								 * parameters. */
	GQ_PARAMS_RIGHT,			/* The right side of the qual contains
								 * parameters. */
} GenQualParams;
static void ri_GenerateQual(StringInfo buf, char *sep, int nkeys,
							const char *ltabname, Relation lrel,
							const int16 *lattnums,
							const char *rtabname, Relation rrel,
							const int16 *rattnums, const Oid *eq_oprs,
							GenQualParams params, Oid *paramtypes);

static void ri_GenerateKeyList(StringInfo buf, int nkeys,
							   const char *tabname, Relation rel,
							   const int16 *attnums);
static void ri_GenerateQualComponent(StringInfo buf,
									 const char *sep,
									 const char *leftop, Oid leftoptype,
									 Oid opoid,
									 const char *rightop, Oid rightoptype);
static void ri_GenerateQualCollation(StringInfo buf, Oid collation);
static int	ri_NullCheck(TupleDesc tupdesc, TupleTableSlot *slot,
						 const RI_ConstraintInfo *riinfo, bool rel_is_pk,
						 bool ignore_attnums);
static void ri_BuildQueryKey(RI_QueryKey *key,
							 const RI_ConstraintInfo *riinfo,
							 int32 constr_queryno,
							 bool single_row);
static bool ri_KeysEqual(Relation rel, TupleTableSlot *oldslot, TupleTableSlot *newslot,
						 const RI_ConstraintInfo *riinfo, bool rel_is_pk);
static bool ri_AttributesEqual(Oid eq_opr, Oid typeid,
							   Datum oldvalue, Datum newvalue);

static void ri_InitHashTables(void);
static void InvalidateConstraintCacheCallBack(Datum arg, int cacheid, uint32 hashvalue);
static SPIPlanPtr ri_FetchPreparedPlan(RI_QueryKey *key);
static void ri_HashPreparedPlan(RI_QueryKey *key, SPIPlanPtr plan);
static RI_CompareHashEntry *ri_HashCompareOp(Oid eq_opr, Oid typeid);

static void ri_CheckTrigger(FunctionCallInfo fcinfo, const char *funcname,
							int tgkind);
static const RI_ConstraintInfo *ri_FetchConstraintInfo(Trigger *trigger,
													   Relation trig_rel, bool rel_is_pk);
static const RI_ConstraintInfo *ri_LoadConstraintInfo(Oid constraintOid,
													  Relation trig_rel,
													  bool rel_is_pk);
static SPIPlanPtr ri_PlanCheck(const char *querystr, int nargs, Oid *argtypes,
							   RI_QueryKey *qkey,
							   Relation fk_rel, Relation pk_rel);
static bool ri_PerformCheck(const RI_ConstraintInfo *riinfo,
							RI_QueryKey *qkey, SPIPlanPtr qplan,
							Relation fk_rel, Relation pk_rel,
							TupleTableSlot *oldslot,
							bool detectNewRows, int expect_OK);
static void ri_ExtractValues(TupleTableSlot *slot, int first, int nkeys,
							 Datum *vals, char *nulls);
static void ri_ReportViolation(const RI_ConstraintInfo *riinfo,
							   Relation pk_rel, Relation fk_rel,
							   TupleTableSlot *violatorslot, TupleDesc tupdesc,
							   int queryno, bool partgone) pg_attribute_noreturn();
static int	ri_register_trigger_data(TriggerData *tdata,
									 Tuplestorestate *oldtable,
									 Tuplestorestate *newtable,
									 TupleDesc desc);
static Tuplestorestate *get_event_tuplestore(TriggerData *trigdata, int nkeys,
											 const int16 *attnums, bool old,
											 TupleDesc tupdesc, Snapshot snapshot);
static Tuplestorestate *get_event_tuplestore_for_cascade_update(TriggerData *trigdata,
																const RI_ConstraintInfo *riinfo);
static void add_key_attrs_to_tupdesc(TupleDesc tupdesc, Relation rel,
									 const RI_ConstraintInfo *riinfo, int16 *attnums,
									 int first, bool generate_attnames);
static void add_key_values(TupleTableSlot *slot,
						   const RI_ConstraintInfo *riinfo,
						   Relation rel, ItemPointer ip,
						   Datum *key_values, bool *key_nulls,
						   Datum *values, bool *nulls, int first);
static TupleTableSlot *get_violator_tuple(Relation rel);


/*
 * RI_FKey_check -
 *
 * Check foreign key existence (combined for INSERT and UPDATE).
 */
static Datum
RI_FKey_check(TriggerData *trigdata)
{
	const RI_ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	bool		is_insert;
	int			queryno;
	RI_QueryKey qkey;
	SPIPlanPtr	qplan;
	Tuplestorestate *oldtable = NULL;
	Tuplestorestate *newtable = NULL;
	Tuplestorestate *table;
	bool		single_row;
	TupleTableSlot *slot = NULL;
	bool		found;

	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, false);

	/*
	 * Get the relation descriptors of the FK and PK tables.
	 *
	 * pk_rel is opened in RowShareLock mode since that's what our eventual
	 * SELECT FOR KEY SHARE will get on it.
	 */
	fk_rel = trigdata->tg_relation;
	pk_rel = table_open(riinfo->pk_relid, RowShareLock);

	/*
	 * Retrieve the changed rows and put them into the appropriate tuplestore.
	 */
	is_insert = TRIGGER_FIRED_BY_INSERT(trigdata->tg_event);
	if (is_insert)
	{
		if (trigdata->ri_tids_old)
			oldtable = get_event_tuplestore(trigdata,
											riinfo->nkeys,
											riinfo->fk_attnums,
											true,
											riinfo->slot_fk->tts_tupleDescriptor,
											SnapshotSelf);
		else
		{
			/* The table is passed by caller if not called from trigger.c */
			oldtable = trigdata->tg_oldtable;
		}
		table = oldtable;
	}
	else
	{
		Assert((TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)));

		if (trigdata->ri_tids_new)
			newtable = get_event_tuplestore(trigdata,
											riinfo->nkeys,
											riinfo->fk_attnums,
											false,
											riinfo->slot_fk->tts_tupleDescriptor,
											SnapshotSelf);
		else
		{
			/* The table is passed by caller if not called from trigger.c */
			newtable = trigdata->tg_newtable;
		}
		table = newtable;
	}

	/*
	 * The query to check a single row requires parameters, so retrieve them
	 * now if that's the case.
	 */
	single_row = tuplestore_tuple_count(table) == 1;
	if (single_row)
	{
		slot = riinfo->slot_fk;
		tuplestore_gettupleslot(table, true, false, slot);
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Bulk processing needs the appropriate "transient table" to be
	 * registered.
	 */
	if (!single_row &&
		ri_register_trigger_data(trigdata, oldtable, newtable,
								 riinfo->slot_fk->tts_tupleDescriptor) !=
		SPI_OK_TD_REGISTER)
		elog(ERROR, "ri_register_trigger_data failed");

	if (single_row)
		queryno = RI_PLAN_CHECK_LOOKUPPK_SINGLE;
	else if (is_insert)
		queryno = RI_PLAN_CHECK_LOOKUPPK_INS;
	else
		queryno = RI_PLAN_CHECK_LOOKUPPK_UPD;
	ri_BuildQueryKey(&qkey, riinfo, queryno, single_row);

	/* Fetch or prepare a saved plan for the real check */
	if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
	{
		char	   *query;
		int			nparams;
		Oid			paramtypes[RI_MAX_NUMKEYS];

		if (single_row)
		{
			query = RI_FKey_check_query_single_row(riinfo, fk_rel, pk_rel,
												   paramtypes);

			nparams = riinfo->nkeys;
		}
		else
		{
			query = RI_FKey_check_query(riinfo, fk_rel, pk_rel, is_insert);

			nparams = 0;
		}

		/* Prepare and save the plan */
		qplan = ri_PlanCheck(query, nparams, paramtypes, &qkey, fk_rel,
							 pk_rel);
	}

	/*
	 * Now check that foreign key exists in PK table
	 */
	found = ri_PerformCheck(riinfo, &qkey, qplan,
							fk_rel, pk_rel,
							slot,
							false,
							SPI_OK_SELECT);

	/*
	 * The query for bulk processing returns the first FK row that violates
	 * the constraint, so use that row to report the violation.
	 */
	if (!single_row && found)
	{
		TupleTableSlot *violatorslot = get_violator_tuple(fk_rel);

		ri_ReportViolation(riinfo,
						   pk_rel, fk_rel,
						   violatorslot,
						   NULL,
						   qkey.constr_queryno, false);
	}

	/*
	 * In contrast, the query to check a single FK row returns the matching PK
	 * row. Failure to find that PK row indicates constraint violation and the
	 * violating row is in "slot".
	 */
	else if (single_row && !found)
		ri_ReportViolation(riinfo,
						   pk_rel, fk_rel,
						   slot,
						   NULL,
						   qkey.constr_queryno, false);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	table_close(pk_rel, RowShareLock);

	return PointerGetDatum(NULL);
}

/* ----------
 * Construct the query to check inserted/updated rows of the FK table.
 *
 * If "insert" is true, the rows are inserted, otherwise they are updated.
 *
 * The query string built is
 *	SELECT t.fkatt1 [, ...]
 *		FROM <tgtable> t LEFT JOIN LATERAL
 *		    (SELECT t.fkatt1 [, ...]
 *               FROM [ONLY] <pktable> p
 *		         WHERE t.fkatt1 = p.pkatt1 [AND ...]
 *		         FOR KEY SHARE OF p) AS m
 *		     ON t.fkatt1 = m.fkatt1 [AND ...]
 *		WHERE m.fkatt1 ISNULL
 *	    LIMIT 1
 *
 * where <tgtable> is "tgoldtable" for INSERT and "tgnewtable" for UPDATE
 * events.
 *
 * It returns the first row that violates the constraint.
 *
 * "m" returns the new rows that do have matching PK row. It is a subquery
 * because the FOR KEY SHARE clause cannot reference the nullable side of an
 * outer join.
 *
 * XXX "tgoldtable" looks confusing for insert, but that's where
 * AfterTriggerExecute() stores tuples whose events don't have
 * AFTER_TRIGGER_2CTID set. For a non-RI trigger, the inserted tuple would
 * fall into tg_trigtuple as opposed to tg_newtuple, which seems a similar
 * problem. It doesn't seem worth any renaming or adding extra tuplestores to
 * TriggerData.
 * ----------
 */
static char *
RI_FKey_check_query(const RI_ConstraintInfo *riinfo, Relation fk_rel,
					Relation pk_rel, bool insert)
{
	StringInfo	querybuf = makeStringInfo();
	StringInfo	subquerybuf = makeStringInfo();
	char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *pk_only;
	const char *tgtable;
	char	   *col_test;

	tgtable = insert ? "tgoldtable" : "tgnewtable";

	quoteRelationName(pkrelname, pk_rel);

	/* Construct the subquery. */
	pk_only = pk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	appendStringInfoString(subquerybuf,
						   "(SELECT ");
	ri_GenerateKeyList(subquerybuf, riinfo->nkeys, "t", fk_rel,
					   riinfo->fk_attnums);
	appendStringInfo(subquerybuf,
					 " FROM %s%s p WHERE ",
					 pk_only, pkrelname);
	ri_GenerateQual(subquerybuf, "AND", riinfo->nkeys,
					"p", pk_rel, riinfo->pk_attnums,
					"t", fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_NONE, NULL);
	appendStringInfoString(subquerybuf, " FOR KEY SHARE OF p) AS m");

	/* Now the main query. */
	appendStringInfoString(querybuf, "SELECT ");
	ri_GenerateKeyList(querybuf, riinfo->nkeys, "t", fk_rel,
					   riinfo->fk_attnums);
	appendStringInfo(querybuf,
					 " FROM %s t LEFT JOIN LATERAL %s ON ",
					 tgtable, subquerybuf->data);
	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					"t", fk_rel, riinfo->fk_attnums,
					"m", fk_rel, riinfo->fk_attnums,
					riinfo->ff_eq_oprs,
					GQ_PARAMS_NONE, NULL);
	col_test = ri_ColNameQuoted("m", RIAttName(fk_rel, riinfo->fk_attnums[0]));
	appendStringInfo(querybuf, " WHERE %s ISNULL ", col_test);
	appendStringInfoString(querybuf, " LIMIT 1");

	return querybuf->data;
}

/* ----------
 * Like RI_FKey_check_query(), but check a single row.
 *
 * The query string built is
 *	SELECT 1 FROM [ONLY] <pktable> x WHERE pkatt1 = $1 [AND ...]
 *		   FOR KEY SHARE OF x
 * The type id's for the $ parameters are those of the
 * corresponding FK attributes.
 *
 * The query is quite a bit simpler than the one for bulk processing, and so
 * it should execute faster.
 *
 * "paramtypes" will receive types of the query parameters (FK attributes).
 * ----------
 */
static char *
RI_FKey_check_query_single_row(const RI_ConstraintInfo *riinfo,
							   Relation fk_rel, Relation pk_rel,
							   Oid *paramtypes)
{
	StringInfo	querybuf = makeStringInfo();
	const char *pk_only;
	char		pkrelname[MAX_QUOTED_REL_NAME_LEN];

	pk_only = pk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(pkrelname, pk_rel);
	appendStringInfo(querybuf, "SELECT 1 FROM %s%s p WHERE ",
					 pk_only, pkrelname);
	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					NULL, pk_rel, riinfo->pk_attnums,
					NULL, fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_RIGHT, paramtypes);
	appendStringInfoString(querybuf, " FOR KEY SHARE OF p");

	return querybuf->data;
}

/*
 * Check if the PK table needs to be queried (using the query generated by
 * RI_FKey_check_query).
 */
static bool
RI_FKey_check_query_required(Trigger *trigger, Relation fk_rel,
							 TupleTableSlot *newslot)
{
	const RI_ConstraintInfo *riinfo;

	riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

	switch (ri_NullCheck(RelationGetDescr(fk_rel), newslot, riinfo, false,
						 false))
	{
		case RI_KEYS_ALL_NULL:

			/*
			 * No further check needed - an all-NULL key passes every type of
			 * foreign key constraint.
			 */
			return false;

		case RI_KEYS_SOME_NULL:

			/*
			 * This is the only case that differs between the three kinds of
			 * MATCH.
			 */
			switch (riinfo->confmatchtype)
			{
				case FKCONSTR_MATCH_FULL:

					/*
					 * Not allowed - MATCH FULL says either all or none of the
					 * attributes can be NULLs
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
							 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",
									RelationGetRelationName(fk_rel),
									NameStr(riinfo->conname)),
							 errdetail("MATCH FULL does not allow mixing of null and nonnull key values."),
							 errtableconstraint(fk_rel,
												NameStr(riinfo->conname))));
					break;

				case FKCONSTR_MATCH_SIMPLE:

					/*
					 * MATCH SIMPLE - if ANY column is null, the key passes
					 * the constraint.
					 */
					return false;

#ifdef NOT_USED
				case FKCONSTR_MATCH_PARTIAL:

					/*
					 * MATCH PARTIAL - all non-null columns must match. (not
					 * implemented, can be done by modifying the query to only
					 * include non-null columns, or by writing a special
					 * version)
					 */
					break;
#endif
			}

		case RI_KEYS_NONE_NULL:

			/*
			 * Have a full qualified key - regular check is needed.
			 */
			break;
	}

	return true;
}


/*
 * RI_FKey_check_ins -
 *
 * Check foreign key existence at insert event on FK table.
 */
Datum
RI_FKey_check_ins(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_check_ins", RI_TRIGTYPE_INSERT);

	/* Share code with UPDATE case. */
	return RI_FKey_check((TriggerData *) fcinfo->context);
}


/*
 * RI_FKey_check_upd -
 *
 * Check foreign key existence at update event on FK table.
 */
Datum
RI_FKey_check_upd(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_check_upd", RI_TRIGTYPE_UPDATE);

	/* Share code with INSERT case. */
	return RI_FKey_check((TriggerData *) fcinfo->context);
}


/*
 * ri_Check_Pk_Match
 *
 * Check to see if another PK row has been created that provides the same
 * key values as the "oldslot" that's been modified or deleted in our trigger
 * event.  Returns true if a match is found in the PK table.
 *
 * We assume the caller checked that the oldslot contains no NULL key values,
 * since otherwise a match is impossible.
 */
static bool
ri_Check_Pk_Match(Relation pk_rel, Relation fk_rel,
				  TupleTableSlot *oldslot,
				  const RI_ConstraintInfo *riinfo)
{
	SPIPlanPtr	qplan;
	RI_QueryKey qkey;
	bool		result;

	/* Only called for non-null rows */
	Assert(ri_NullCheck(RelationGetDescr(pk_rel), oldslot, riinfo, true,
						true) == RI_KEYS_NONE_NULL);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Fetch or prepare a saved plan for checking PK table with values coming
	 * from a PK row
	 */
	ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CHECK_LOOKUPPK_FROM_PK, true);

	if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
	{
		StringInfo	querybuf = makeStringInfo();
		char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
		const char *pk_only;
		Oid			paramtypes[RI_MAX_NUMKEYS];

		/* ----------
		 * The query string built is
		 *	SELECT 1 FROM [ONLY] <pktable> x WHERE pkatt1 = $1 [AND ...]
		 *		   FOR KEY SHARE OF x
		 * The type id's for the $ parameters are those of the
		 * PK attributes themselves.
		 * ----------
		 */
		pk_only = pk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
			"" : "ONLY ";
		quoteRelationName(pkrelname, pk_rel);
		appendStringInfo(querybuf, "SELECT 1 FROM %s%s x WHERE ",
						 pk_only, pkrelname);

		ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
						NULL, pk_rel, riinfo->pk_attnums,
						NULL, fk_rel, riinfo->fk_attnums,
						riinfo->pf_eq_oprs,
						GQ_PARAMS_RIGHT,
						paramtypes);

		appendStringInfoString(querybuf, " FOR KEY SHARE OF x");

		/* Prepare and save the plan */
		qplan = ri_PlanCheck(querybuf->data, riinfo->nkeys, paramtypes,
							 &qkey, fk_rel, pk_rel);
	}

	/*
	 * We have a plan now. Run it.
	 */
	result = ri_PerformCheck(riinfo, &qkey, qplan,
							 fk_rel, pk_rel,
							 oldslot,
							 true,	/* treat like update */
							 SPI_OK_SELECT);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return result;
}


/*
 * RI_FKey_noaction_del -
 *
 * Give an error and roll back the current transaction if the
 * delete has resulted in a violation of the given referential
 * integrity constraint.
 */
Datum
RI_FKey_noaction_del(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_noaction_del", RI_TRIGTYPE_DELETE);

	/* Share code with RESTRICT/UPDATE cases. */
	return ri_restrict((TriggerData *) fcinfo->context, true);
}

/*
 * RI_FKey_restrict_del -
 *
 * Restrict delete from PK table to rows unreferenced by foreign key.
 *
 * The SQL standard intends that this referential action occur exactly when
 * the delete is performed, rather than after.  This appears to be
 * the only difference between "NO ACTION" and "RESTRICT".  In Postgres
 * we still implement this as an AFTER trigger, but it's non-deferrable.
 */
Datum
RI_FKey_restrict_del(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_restrict_del", RI_TRIGTYPE_DELETE);

	/* Share code with NO ACTION/UPDATE cases. */
	return ri_restrict((TriggerData *) fcinfo->context, false);
}

/*
 * RI_FKey_noaction_upd -
 *
 * Give an error and roll back the current transaction if the
 * update has resulted in a violation of the given referential
 * integrity constraint.
 */
Datum
RI_FKey_noaction_upd(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_noaction_upd", RI_TRIGTYPE_UPDATE);

	/* Share code with RESTRICT/DELETE cases. */
	return ri_restrict((TriggerData *) fcinfo->context, true);
}

/*
 * RI_FKey_restrict_upd -
 *
 * Restrict update of PK to rows unreferenced by foreign key.
 *
 * The SQL standard intends that this referential action occur exactly when
 * the update is performed, rather than after.  This appears to be
 * the only difference between "NO ACTION" and "RESTRICT".  In Postgres
 * we still implement this as an AFTER trigger, but it's non-deferrable.
 */
Datum
RI_FKey_restrict_upd(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_restrict_upd", RI_TRIGTYPE_UPDATE);

	/* Share code with NO ACTION/DELETE cases. */
	return ri_restrict((TriggerData *) fcinfo->context, false);
}

/*
 * ri_restrict -
 *
 * Common code for ON DELETE RESTRICT, ON DELETE NO ACTION,
 * ON UPDATE RESTRICT, and ON UPDATE NO ACTION.
 */
static Datum
ri_restrict(TriggerData *trigdata, bool is_no_action)
{
	const RI_ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	RI_QueryKey qkey;
	SPIPlanPtr	qplan;
	Tuplestorestate *oldtable;
	bool		single_row;
	TupleTableSlot *oldslot = NULL;

	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, true);

	/*
	 * Get the relation descriptors of the FK and PK tables and the old tuple.
	 *
	 * fk_rel is opened in RowShareLock mode since that's what our eventual
	 * SELECT FOR KEY SHARE will get on it.
	 */
	fk_rel = table_open(riinfo->fk_relid, RowShareLock);
	pk_rel = trigdata->tg_relation;

	oldtable = get_event_tuplestore(trigdata,
									riinfo->nkeys,
									riinfo->pk_attnums,
									true,
									riinfo->slot_pk->tts_tupleDescriptor,
									NULL);

	/* Should we use a special query to check a single row? */
	single_row = tuplestore_tuple_count(oldtable) == 1;
	if (single_row)
	{
		/* The query needs parameters, so retrieve them now. */
		oldslot = riinfo->slot_pk;
		tuplestore_gettupleslot(oldtable, true, false, oldslot);

		/*
		 * If another PK row now exists providing the old key values, we
		 * should not do anything.  However, this check should only be made in
		 * the NO ACTION case; in RESTRICT cases we don't wish to allow
		 * another row to be substituted.
		 */
		if (is_no_action &&
			ri_Check_Pk_Match(pk_rel, fk_rel, oldslot, riinfo))
		{
			table_close(fk_rel, RowShareLock);
			return PointerGetDatum(NULL);
		}
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Bulk processing needs the "transient table" to be registered. */
	if (!single_row &&
		ri_register_trigger_data(trigdata, oldtable, NULL,
								 riinfo->slot_pk->tts_tupleDescriptor) !=
		SPI_OK_TD_REGISTER)
		elog(ERROR, "ri_register_trigger_data failed");

	if (single_row)
	{
		ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_RESTRICT_CHECKREF, true);

		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			char	   *query;
			Oid			paramtypes[RI_MAX_NUMKEYS];

			query = ri_restrict_query_single_row(riinfo, fk_rel, pk_rel,
												 paramtypes);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, riinfo->nkeys, paramtypes, &qkey,
								 fk_rel, pk_rel);
		}
	}
	else if (!is_no_action)
	{
		/*
		 * Fetch or prepare a saved plan for the restrict lookup (it's the
		 * same query for delete and update cases)
		 */
		ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_RESTRICT_CHECKREF, false);

		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			char	   *query;

			query = ri_restrict_query(riinfo, fk_rel, pk_rel, false);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, 0, NULL, &qkey, fk_rel, pk_rel);
		}
	}
	else
	{
		/*
		 * If another PK row now exists providing the old key values, we
		 * should not do anything.  However, this check should only be made in
		 * the NO ACTION case; in RESTRICT cases we don't wish to allow
		 * another row to be substituted.
		 */
		ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_RESTRICT_CHECKREF_NO_ACTION,
						 false);

		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{

			char	   *query;

			query = ri_restrict_query(riinfo, fk_rel, pk_rel, true);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, 0, NULL, &qkey, fk_rel, pk_rel);
		}
	}

	/*
	 * We have a plan now. Run it to check for existing references.
	 */
	if (ri_PerformCheck(riinfo, &qkey, qplan,
						fk_rel, pk_rel,
						oldslot,
						true,	/* must detect new rows */
						SPI_OK_SELECT))
	{
		TupleTableSlot *violatorslot;

		/*
		 * For a single row, oldslot contains the violating key. For bulk
		 * check, the problematic key value should have been returned by the
		 * query.
		 */
		violatorslot = single_row ? oldslot : get_violator_tuple(pk_rel);

		ri_ReportViolation(riinfo,
						   pk_rel, fk_rel,
						   violatorslot,
						   NULL,
						   qkey.constr_queryno, false);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	table_close(fk_rel, RowShareLock);

	return PointerGetDatum(NULL);
}

/* ----------
 * Construct the query to check whether deleted row of the PK table is still
 * referenced by the FK table.
 *
 * If "pk_rel" is NULL, the query string built is
 *	SELECT o.*
 *		FROM [ONLY] <fktable> f, tgoldtable o
 *		WHERE f.fkatt1 = o.pkatt1 [AND ...]
 *		FOR KEY SHARE OF f
 *		LIMIT 1
 *
 * If no_action is true,also check if the row being deleted was re-inserted
 * into the PK table (or in case of UPDATE, if row with the old key is there
 * again):
 *
 *	SELECT o.pkatt1 [, ...]
 *		FROM [ONLY] <fktable> f, tgoldtable o
 *		WHERE f.fkatt1 = o.pkatt1 [AND ...] AND	NOT EXISTS
 *			(SELECT 1
 *			FROM <pktable> p
 *			WHERE p.pkatt1 = o.pkatt1 [, ...]
 *			FOR KEY SHARE OF p)
 *		FOR KEY SHARE OF f
 *		LIMIT 1
 *
 * TODO Is ONLY needed for the the PK table?
 * ----------
 */
static char *
ri_restrict_query(const RI_ConstraintInfo *riinfo, Relation fk_rel,
				  Relation pk_rel, bool no_action)
{
	StringInfo	querybuf = makeStringInfo();
	StringInfo	subquerybuf = NULL;
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *fk_only;

	if (no_action)
	{
		char		pkrelname[MAX_QUOTED_REL_NAME_LEN];

		subquerybuf = makeStringInfo();
		quoteRelationName(pkrelname, pk_rel);
		appendStringInfo(subquerybuf,
						 "(SELECT 1 FROM %s p WHERE ", pkrelname);
		ri_GenerateQual(subquerybuf, "AND", riinfo->nkeys,
						"p", pk_rel, riinfo->pk_attnums,
						"o", pk_rel, riinfo->pk_attnums,
						riinfo->pp_eq_oprs,
						GQ_PARAMS_NONE, NULL);
		appendStringInfoString(subquerybuf, " FOR KEY SHARE OF p)");
	}

	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfoString(querybuf, "SELECT ");
	ri_GenerateKeyList(querybuf, riinfo->nkeys, "o", pk_rel,
					   riinfo->pk_attnums);
	appendStringInfo(querybuf, " FROM %s%s f, tgoldtable o WHERE ",
					 fk_only, fkrelname);
	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					"o", pk_rel, riinfo->pk_attnums,
					"f", fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_NONE, NULL);
	if (no_action)
		appendStringInfo(querybuf, " AND NOT EXISTS %s", subquerybuf->data);
	appendStringInfoString(querybuf, " FOR KEY SHARE OF f LIMIT 1");

	return querybuf->data;
}

/*
 * Like ri_restrict_query(), but check a single row.
 */
static char *
ri_restrict_query_single_row(const RI_ConstraintInfo *riinfo, Relation fk_rel,
							 Relation pk_rel, Oid *paramtypes)
{
	StringInfo	querybuf = makeStringInfo();
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *fk_only;

	/* ----------
	 * The query string built is
	 *
	 *	SELECT 1 FROM [ONLY] <fktable> x WHERE $1 = fkatt1 [AND ...]
	 *		   FOR KEY SHARE OF x
	 *
	 * The type id's for the $ parameters are those of the
	 * corresponding PK attributes.
	 * ----------
	 */
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfo(querybuf, "SELECT 1 FROM %s%s x WHERE ",
					 fk_only, fkrelname);

	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					NULL, pk_rel, riinfo->pk_attnums,
					NULL, fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_LEFT,
					paramtypes);

	appendStringInfoString(querybuf, " FOR KEY SHARE OF x");

	return querybuf->data;
}

/*
 * RI_FKey_cascade_del -
 *
 * Cascaded delete foreign key references at delete event on PK table.
 */
Datum
RI_FKey_cascade_del(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const RI_ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	RI_QueryKey qkey;
	SPIPlanPtr	qplan;
	Tuplestorestate *oldtable;
	bool		single_row;
	TupleTableSlot *oldslot = NULL;

	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_cascade_del", RI_TRIGTYPE_DELETE);

	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, true);

	/*
	 * Get the relation descriptors of the FK and PK tables and the old tuple.
	 *
	 * fk_rel is opened in RowExclusiveLock mode since that's what our
	 * eventual DELETE will get on it.
	 */
	fk_rel = table_open(riinfo->fk_relid, RowExclusiveLock);
	pk_rel = trigdata->tg_relation;

	oldtable = get_event_tuplestore(trigdata,
									riinfo->nkeys,
									riinfo->pk_attnums,
									true,
									riinfo->slot_pk->tts_tupleDescriptor,
									NULL);

	/* Should we use a special query to check a single row? */
	single_row = tuplestore_tuple_count(oldtable) == 1;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CASCADE_DEL_DODELETE, single_row);

	if (single_row)
	{
		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			Oid			paramtypes[RI_MAX_NUMKEYS];
			char	   *query = ri_cascade_del_query_single_row(riinfo,
																fk_rel,
																pk_rel,
																paramtypes);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, riinfo->nkeys, paramtypes, &qkey,
								 fk_rel, pk_rel);
		}

		/* The query needs parameters, so retrieve them now. */
		oldslot = riinfo->slot_pk;
		tuplestore_gettupleslot(oldtable, true, false, oldslot);
	}
	else
	{
		/* Bulk processing needs the "transient table" to be registered. */
		if (ri_register_trigger_data(trigdata, oldtable, NULL,
									 riinfo->slot_pk->tts_tupleDescriptor) !=
			SPI_OK_TD_REGISTER)
			elog(ERROR, "ri_register_trigger_data failed");

		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			char	   *query = ri_cascade_del_query(riinfo, fk_rel, pk_rel);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, 0, NULL, &qkey, fk_rel, pk_rel);
		}
	}

	/*
	 * We have a plan now. Build up the arguments from the key values in the
	 * deleted PK tuple and delete the referencing rows
	 */
	ri_PerformCheck(riinfo, &qkey, qplan,
					fk_rel, pk_rel,
					oldslot,
					true,		/* must detect new rows */
					SPI_OK_DELETE);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	table_close(fk_rel, RowExclusiveLock);

	return PointerGetDatum(NULL);
}

static char *
ri_cascade_del_query(const RI_ConstraintInfo *riinfo, Relation fk_rel,
					 Relation pk_rel)
{
	StringInfo	querybuf = makeStringInfo();
	StringInfo	subquerybuf = makeStringInfo();
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *fk_only;

	/* ----------
	 * The query string built is
	 *
	 *	DELETE FROM [ONLY] <fktable> f
	 *	    WHERE EXISTS
	 *			(SELECT 1
	 *			FROM tgoldtable o
	 *			WHERE o.pkatt1 = f.fkatt1 [AND ...])
	 * ----------
	 */
	appendStringInfoString(subquerybuf,
						   "SELECT 1 FROM tgoldtable o WHERE ");
	ri_GenerateQual(subquerybuf, "AND", riinfo->nkeys,
					"o", pk_rel, riinfo->pk_attnums,
					"f", fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_NONE, NULL);

	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfo(querybuf,
					 "DELETE FROM %s%s f WHERE EXISTS (%s) ",
					 fk_only, fkrelname, subquerybuf->data);

	return querybuf->data;
}

static char *
ri_cascade_del_query_single_row(const RI_ConstraintInfo *riinfo,
								Relation fk_rel, Relation pk_rel,
								Oid *paramtypes)
{
	StringInfo	querybuf = makeStringInfo();
	const char *fk_only;
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];

	/* ----------
	 * The query string built is
	 *
	 *	DELETE FROM [ONLY] <fktable> WHERE $1 = fkatt1 [AND ...]
	 *
	 * The type id's for the $ parameters are those of the
	 * corresponding PK attributes.
	 * ----------
	 */

	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);

	appendStringInfo(querybuf, "DELETE FROM %s%s WHERE ", fk_only,
					 fkrelname);

	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					NULL, pk_rel, riinfo->pk_attnums,
					NULL, fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_LEFT,
					paramtypes);

	return querybuf->data;
}

/*
 * RI_FKey_cascade_upd -
 *
 * Cascaded update foreign key references at update event on PK table.
 */
Datum
RI_FKey_cascade_upd(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const RI_ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	RI_QueryKey qkey;
	SPIPlanPtr	qplan;
	Tuplestorestate *newtable;
	bool		single_row;
	TupleTableSlot *newslot = NULL;

	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_cascade_upd", RI_TRIGTYPE_UPDATE);

	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, true);

	/*
	 * Get the relation descriptors of the FK and PK tables and the new and
	 * old tuple.
	 *
	 * fk_rel is opened in RowExclusiveLock mode since that's what our
	 * eventual UPDATE will get on it.
	 */
	fk_rel = table_open(riinfo->fk_relid, RowExclusiveLock);
	pk_rel = trigdata->tg_relation;

	/*
	 * In this case, both new and old values should be in the same tuplestore
	 * because there's no useful join column.
	 */
	newtable = get_event_tuplestore_for_cascade_update(trigdata, riinfo);

	/* Should we use a special query to check a single row? */
	single_row = tuplestore_tuple_count(newtable) == 1;

	/* Fetch or prepare a saved plan for the cascaded update */
	ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CASCADE_UPD_DOUPDATE, single_row);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (single_row)
	{
		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			Oid			paramtypes[RI_MAX_NUMKEYS * 2];
			char	   *query = ri_cascade_upd_query_single_row(riinfo,
																fk_rel,
																pk_rel,
																paramtypes);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, 2 * riinfo->nkeys, paramtypes, &qkey,
								 fk_rel, pk_rel);
		}

		/* The query needs parameters, so retrieve them now. */
		newslot = riinfo->slot_both;
		tuplestore_gettupleslot(newtable, true, false, newslot);
	}
	else
	{
		/* Here it doesn't matter whether we call the table "old" or "new". */
		if (ri_register_trigger_data(trigdata, NULL, newtable,
									 riinfo->slot_both->tts_tupleDescriptor) !=
			SPI_OK_TD_REGISTER)
			elog(ERROR, "ri_register_trigger_data failed");

		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			char	   *query = ri_cascade_upd_query(riinfo, fk_rel, pk_rel);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, 0, NULL, &qkey, fk_rel, pk_rel);
		}
	}

	/*
	 * We have a plan now. Run it to update the existing references.
	 */
	ri_PerformCheck(riinfo, &qkey, qplan,
					fk_rel, pk_rel,
					newslot,
					true,		/* must detect new rows */
					SPI_OK_UPDATE);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	table_close(fk_rel, RowExclusiveLock);

	return PointerGetDatum(NULL);
}

static char *
ri_cascade_upd_query(const RI_ConstraintInfo *riinfo, Relation fk_rel,
					 Relation pk_rel)
{
	StringInfo	querybuf = makeStringInfo();
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *fk_only;
	int			i;

	/* ----------
	 * The query string built is
	 *
	 * UPDATE [ONLY] <fktable> f
	 *     SET fkatt1 = n.pkatt1_new [, ...]
	 *     FROM tgnewtable n
	 *     WHERE
	 *         f.fkatt1 = n.pkatt1_old [AND ...]
	 *
	 * Note that we are assuming there is an assignment cast from the PK
	 * to the FK type; else the parser will fail.
	 * ----------
	 */
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfo(querybuf, "UPDATE %s%s f SET ", fk_only, fkrelname);

	for (i = 0; i < riinfo->nkeys; i++)
	{
		char	   *latt = ri_ColNameQuoted("", RIAttName(fk_rel, riinfo->fk_attnums[i]));
		Oid			lcoll = RIAttCollation(fk_rel, riinfo->fk_attnums[i]);
		char		ratt[NAMEDATALEN];
		Oid			rcoll = RIAttCollation(pk_rel, riinfo->pk_attnums[i]);

		snprintf(ratt, NAMEDATALEN, "n.pkatt%d_new", i + 1);

		if (i > 0)
			appendStringInfoString(querybuf, ", ");

		appendStringInfo(querybuf, "%s = %s", latt, ratt);

		if (lcoll != rcoll)
			ri_GenerateQualCollation(querybuf, lcoll);
	}

	appendStringInfo(querybuf, " FROM tgnewtable n WHERE");

	for (i = 0; i < riinfo->nkeys; i++)
	{
		char	   *fattname;

		if (i > 0)
			appendStringInfoString(querybuf, " AND");

		fattname =
			ri_ColNameQuoted("f",
							 RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(querybuf, " %s = n.pkatt%d_old", fattname, i + 1);
	}

	return querybuf->data;
}

static char *
ri_cascade_upd_query_single_row(const RI_ConstraintInfo *riinfo,
								Relation fk_rel, Relation pk_rel,
								Oid *paramtypes)
{
	StringInfo	querybuf = makeStringInfo();
	StringInfo	qualbuf = makeStringInfo();
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		attname[MAX_QUOTED_NAME_LEN];
	char		paramname[16];
	const char *querysep;
	const char *qualsep;
	const char *fk_only;

	/* ----------
	 * The query string built is
	 *
	 *	UPDATE [ONLY] <fktable> SET fkatt1 = $1 [, ...]
	 *			WHERE $n = fkatt1 [AND ...]
	 *
	 * The type id's for the $ parameters are those of the
	 * corresponding PK attributes.  Note that we are assuming
	 * there is an assignment cast from the PK to the FK type;
	 * else the parser will fail.
	 * ----------
	 */
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfo(querybuf, "UPDATE %s%s SET",
					 fk_only, fkrelname);
	querysep = "";
	qualsep = "WHERE";
	for (int i = 0, j = riinfo->nkeys; i < riinfo->nkeys; i++, j++)
	{
		Oid			pk_type = RIAttType(pk_rel, riinfo->pk_attnums[i]);
		Oid			fk_type = RIAttType(fk_rel, riinfo->fk_attnums[i]);
		Oid			pk_coll = RIAttCollation(pk_rel, riinfo->pk_attnums[i]);
		Oid			fk_coll = RIAttCollation(fk_rel, riinfo->fk_attnums[i]);

		quoteOneName(attname,
					 RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(querybuf,
						 "%s %s = $%d",
						 querysep, attname, i + 1);
		sprintf(paramname, "$%d", j + 1);
		ri_GenerateQualComponent(qualbuf, qualsep,
								 paramname, pk_type,
								 riinfo->pf_eq_oprs[i],
								 attname, fk_type);

		if (pk_coll != fk_coll && !get_collation_isdeterministic(pk_coll))
			ri_GenerateQualCollation(querybuf, pk_coll);

		querysep = ",";
		qualsep = "AND";
		paramtypes[i] = pk_type;
		paramtypes[j] = pk_type;
	}
	appendBinaryStringInfo(querybuf, qualbuf->data, qualbuf->len);

	return querybuf->data;
}

/*
 * RI_FKey_setnull_del -
 *
 * Set foreign key references to NULL values at delete event on PK table.
 */
Datum
RI_FKey_setnull_del(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_setnull_del", RI_TRIGTYPE_DELETE);

	/* Share code with UPDATE case */
	return ri_set((TriggerData *) fcinfo->context, true);
}

/*
 * RI_FKey_setnull_upd -
 *
 * Set foreign key references to NULL at update event on PK table.
 */
Datum
RI_FKey_setnull_upd(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_setnull_upd", RI_TRIGTYPE_UPDATE);

	/* Share code with DELETE case */
	return ri_set((TriggerData *) fcinfo->context, true);
}

/*
 * RI_FKey_setdefault_del -
 *
 * Set foreign key references to defaults at delete event on PK table.
 */
Datum
RI_FKey_setdefault_del(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_setdefault_del", RI_TRIGTYPE_DELETE);

	/* Share code with UPDATE case */
	return ri_set((TriggerData *) fcinfo->context, false);
}

/*
 * RI_FKey_setdefault_upd -
 *
 * Set foreign key references to defaults at update event on PK table.
 */
Datum
RI_FKey_setdefault_upd(PG_FUNCTION_ARGS)
{
	/* Check that this is a valid trigger call on the right time and event. */
	ri_CheckTrigger(fcinfo, "RI_FKey_setdefault_upd", RI_TRIGTYPE_UPDATE);

	/* Share code with DELETE case */
	return ri_set((TriggerData *) fcinfo->context, false);
}

/*
 * ri_set -
 *
 * Common code for ON DELETE SET NULL, ON DELETE SET DEFAULT, ON UPDATE SET
 * NULL, and ON UPDATE SET DEFAULT.
 */
static Datum
ri_set(TriggerData *trigdata, bool is_set_null)
{
	const RI_ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	RI_QueryKey qkey;
	SPIPlanPtr	qplan;
	Tuplestorestate *oldtable;
	bool		single_row;
	TupleTableSlot *oldslot = NULL;

	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, true);

	/*
	 * Get the relation descriptors of the FK and PK tables and the old tuple.
	 *
	 * fk_rel is opened in RowExclusiveLock mode since that's what our
	 * eventual UPDATE will get on it.
	 */
	fk_rel = table_open(riinfo->fk_relid, RowExclusiveLock);
	pk_rel = trigdata->tg_relation;

	oldtable = get_event_tuplestore(trigdata,
									riinfo->nkeys,
									riinfo->pk_attnums,
									true,
									riinfo->slot_pk->tts_tupleDescriptor,
									NULL);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Should we use a special query to check a single row? */
	single_row = tuplestore_tuple_count(oldtable) == 1;

	/*
	 * Fetch or prepare a saved plan for the set null/default operation (it's
	 * the same query for delete and update cases)
	 */
	ri_BuildQueryKey(&qkey, riinfo,
					 (is_set_null
					  ? RI_PLAN_SETNULL_DOUPDATE
					  : RI_PLAN_SETDEFAULT_DOUPDATE),
					 single_row);

	if (single_row)
	{
		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			Oid			paramtypes[RI_MAX_NUMKEYS];
			char	   *query = ri_set_query_single_row(riinfo, fk_rel, pk_rel,
														paramtypes, is_set_null);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, riinfo->nkeys, paramtypes, &qkey,
								 fk_rel, pk_rel);
		}

		/* The query needs parameters, so retrieve them now. */
		oldslot = riinfo->slot_pk;
		tuplestore_gettupleslot(oldtable, true, false, oldslot);
	}
	else
	{
		/* Here it doesn't matter whether we call the table "old" or "new". */
		if (ri_register_trigger_data(trigdata, oldtable, NULL,
									 riinfo->slot_pk->tts_tupleDescriptor) !=
			SPI_OK_TD_REGISTER)
			elog(ERROR, "ri_register_trigger_data failed");

		if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL)
		{
			char	   *query = ri_set_query(riinfo, fk_rel, pk_rel,
											 is_set_null);

			/* Prepare and save the plan */
			qplan = ri_PlanCheck(query, 0, NULL, &qkey, fk_rel, pk_rel);
		}
	}

	/*
	 * We have a plan now. Run it to update the existing references.
	 */
	ri_PerformCheck(riinfo, &qkey, qplan,
					fk_rel, pk_rel,
					oldslot,
					true,		/* must detect new rows */
					SPI_OK_UPDATE);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	table_close(fk_rel, RowExclusiveLock);

	if (is_set_null)
		return PointerGetDatum(NULL);
	else
	{
		/*
		 * If we just deleted or updated the PK row whose key was equal to the
		 * FK columns' default values, and a referencing row exists in the FK
		 * table, we would have updated that row to the same values it already
		 * had --- and RI_FKey_fk_upd_check_required would hence believe no
		 * check is necessary.  So we need to do another lookup now and in
		 * case a reference still exists, abort the operation.  That is
		 * already implemented in the NO ACTION trigger, so just run it. (This
		 * recheck is only needed in the SET DEFAULT case, since CASCADE would
		 * remove such rows in case of a DELETE operation or would change the
		 * FK key values in case of an UPDATE, while SET NULL is certain to
		 * result in rows that satisfy the FK constraint.)
		 */
		return ri_restrict(trigdata, true);
	}
}

static char *
ri_set_query(const RI_ConstraintInfo *riinfo, Relation fk_rel,
			 Relation pk_rel, bool is_set_null)
{
	StringInfo	querybuf = makeStringInfo();
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *querysep;
	const char *fk_only;

	/* ----------
	 * The query string built is
	 *	UPDATE [ONLY] <fktable> f
	 *	    SET fkatt1 = {NULL|DEFAULT} [, ...]
	 *	    FROM tgoldtable o
	 *		WHERE o.pkatt1 = f.fkatt1 [AND ...]
	 * ----------
	 */
	initStringInfo(querybuf);
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfo(querybuf, "UPDATE %s%s f SET",
					 fk_only, fkrelname);
	querysep = "";
	for (int i = 0; i < riinfo->nkeys; i++)
	{
		char		attname[MAX_QUOTED_NAME_LEN];

		quoteOneName(attname,
					 RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(querybuf,
						 "%s %s = %s",
						 querysep, attname,
						 is_set_null ? "NULL" : "DEFAULT");
		querysep = ",";
	}

	appendStringInfoString(querybuf, " FROM tgoldtable o WHERE ");
	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					"o", pk_rel, riinfo->pk_attnums,
					"f", fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_NONE, NULL);

	return querybuf->data;
}

static char *
ri_set_query_single_row(const RI_ConstraintInfo *riinfo, Relation fk_rel,
						Relation pk_rel, Oid *paramtypes, bool is_set_null)
{
	StringInfo	querybuf = makeStringInfo();
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	const char *fk_only;

	/* ----------
	 * The query string built is
	 *	UPDATE [ONLY] <fktable> SET fkatt1 = {NULL|DEFAULT} [, ...]
	 *			WHERE $1 = fkatt1 [AND ...]
	 * The type id's for the $ parameters are those of the
	 * corresponding PK attributes.
	 * ----------
	 */
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	quoteRelationName(fkrelname, fk_rel);
	appendStringInfo(querybuf, "UPDATE %s%s SET",
					 fk_only, fkrelname);

	for (int i = 0; i < riinfo->nkeys; i++)
	{
		char		attname[MAX_QUOTED_NAME_LEN];
		const char *sep = i > 0 ? "," : "";

		quoteOneName(attname,
					 RIAttName(fk_rel, riinfo->fk_attnums[i]));

		appendStringInfo(querybuf,
						 "%s %s = %s",
						 sep, attname,
						 is_set_null ? "NULL" : "DEFAULT");
	}

	appendStringInfo(querybuf, " WHERE ");
	ri_GenerateQual(querybuf, "AND", riinfo->nkeys,
					NULL, pk_rel, riinfo->pk_attnums,
					NULL, fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_LEFT, paramtypes);

	return querybuf->data;
}

/*
 * RI_FKey_pk_upd_check_required -
 *
 * Check if we really need to fire the RI trigger for an update or delete to a PK
 * relation.  This is called by the AFTER trigger queue manager to see if
 * it can skip queuing an instance of an RI trigger.  Returns true if the
 * trigger must be fired, false if we can prove the constraint will still
 * be satisfied.
 *
 * newslot will be NULL if this is called for a delete.
 */
bool
RI_FKey_pk_upd_check_required(Trigger *trigger, Relation pk_rel,
							  TupleTableSlot *oldslot, TupleTableSlot *newslot)
{
	const RI_ConstraintInfo *riinfo;

	riinfo = ri_FetchConstraintInfo(trigger, pk_rel, true);

	/*
	 * If any old key value is NULL, the row could not have been referenced by
	 * an FK row, so no check is needed.
	 */
	if (ri_NullCheck(RelationGetDescr(pk_rel), oldslot, riinfo, true,
					 false) != RI_KEYS_NONE_NULL)
		return false;

	/* If all old and new key values are equal, no check is needed */
	if (newslot && ri_KeysEqual(pk_rel, oldslot, newslot, riinfo, true))
		return false;

	/* Else we need to fire the trigger. */
	return true;
}

/*
 * RI_FKey_fk_upd_check_required -
 *
 * Check if we really need to fire the RI trigger for an update to an FK
 * relation.  This is called by the AFTER trigger queue manager to see if
 * it can skip queuing an instance of an RI trigger.  Returns true if the
 * trigger must be fired, false if we can prove the constraint will still
 * be satisfied.
 */
bool
RI_FKey_fk_upd_check_required(Trigger *trigger, Relation fk_rel,
							  TupleTableSlot *oldslot, TupleTableSlot *newslot)
{
	const RI_ConstraintInfo *riinfo;
	int			ri_nullcheck;
	Datum		xminDatum;
	TransactionId xmin;
	bool		isnull;

	riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

	ri_nullcheck = ri_NullCheck(RelationGetDescr(fk_rel), newslot, riinfo, false,
								false);

	/*
	 * If all new key values are NULL, the row satisfies the constraint, so no
	 * check is needed.
	 */
	if (ri_nullcheck == RI_KEYS_ALL_NULL)
		return false;

	/*
	 * If some new key values are NULL, the behavior depends on the match
	 * type.
	 */
	else if (ri_nullcheck == RI_KEYS_SOME_NULL)
	{
		switch (riinfo->confmatchtype)
		{
			case FKCONSTR_MATCH_SIMPLE:

				/*
				 * If any new key value is NULL, the row must satisfy the
				 * constraint, so no check is needed.
				 */
				return false;

			case FKCONSTR_MATCH_PARTIAL:

				/*
				 * Don't know, must run full check.
				 */
				break;

			case FKCONSTR_MATCH_FULL:

				/*
				 * If some new key values are NULL, the row fails the
				 * constraint.  We must not throw error here, because the row
				 * might get invalidated before the constraint is to be
				 * checked, but we should queue the event to apply the check
				 * later.
				 */
				return true;
		}
	}

	/*
	 * Continues here for no new key values are NULL, or we couldn't decide
	 * yet.
	 */

	/*
	 * If the original row was inserted by our own transaction, we must fire
	 * the trigger whether or not the keys are equal.  This is because our
	 * UPDATE will invalidate the INSERT so that the INSERT RI trigger will
	 * not do anything; so we had better do the UPDATE check.  (We could skip
	 * this if we knew the INSERT trigger already fired, but there is no easy
	 * way to know that.)
	 */
	xminDatum = slot_getsysattr(oldslot, MinTransactionIdAttributeNumber, &isnull);
	Assert(!isnull);
	xmin = DatumGetTransactionId(xminDatum);
	if (TransactionIdIsCurrentTransactionId(xmin))
		return true;

	/* If all old and new key values are equal, no check is needed */
	if (ri_KeysEqual(fk_rel, oldslot, newslot, riinfo, false))
		return false;

	/* Else we need to fire the trigger. */
	return true;
}

/*
 * RI_FKey_fk_attributes -
 *
 * Return tuple descriptor containing the FK attributes of given FK constraint
 * and only those. In addition, array containing the numbers of the key
 * attributes within the whole table is stored to *attnums_p.
 */
TupleDesc
RI_FKey_fk_attributes(Trigger *trigger, Relation trig_rel, const int16 **attnums_p)
{
	const RI_ConstraintInfo *riinfo;

	riinfo = ri_FetchConstraintInfo(trigger, trig_rel, false);
	*attnums_p = riinfo->fk_attnums;

	return riinfo->slot_fk->tts_tupleDescriptor;
}

/*
 * RI_Initial_Check -
 *
 * Check an entire table for non-matching values using a single query.
 * This is not a trigger procedure, but is called during ALTER TABLE
 * ADD FOREIGN KEY to validate the initial table contents.
 *
 * We expect that the caller has made provision to prevent any problems
 * caused by concurrent actions. This could be either by locking rel and
 * pkrel at ShareRowExclusiveLock or higher, or by otherwise ensuring
 * that triggers implementing the checks are already active.
 * Hence, we do not need to lock individual rows for the check.
 *
 * If the check fails because the current user doesn't have permissions
 * to read both tables, return false to let our caller know that they will
 * need to do something else to check the constraint.
 */
bool
RI_Initial_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel)
{
	const RI_ConstraintInfo *riinfo;
	StringInfoData querybuf;
	char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		pkattname[MAX_QUOTED_NAME_LEN + 3];
	char		fkattname[MAX_QUOTED_NAME_LEN + 3];
	RangeTblEntry *pkrte;
	RangeTblEntry *fkrte;
	const char *sep;
	const char *fk_only;
	const char *pk_only;
	int			save_nestlevel;
	char		workmembuf[32];
	int			spi_result;
	SPIPlanPtr	qplan;

	riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

	/*
	 * Check to make sure current user has enough permissions to do the test
	 * query.  (If not, caller can fall back to the trigger method, which
	 * works because it changes user IDs on the fly.)
	 *
	 * XXX are there any other show-stopper conditions to check?
	 */
	pkrte = makeNode(RangeTblEntry);
	pkrte->rtekind = RTE_RELATION;
	pkrte->relid = RelationGetRelid(pk_rel);
	pkrte->relkind = pk_rel->rd_rel->relkind;
	pkrte->rellockmode = AccessShareLock;
	pkrte->requiredPerms = ACL_SELECT;

	fkrte = makeNode(RangeTblEntry);
	fkrte->rtekind = RTE_RELATION;
	fkrte->relid = RelationGetRelid(fk_rel);
	fkrte->relkind = fk_rel->rd_rel->relkind;
	fkrte->rellockmode = AccessShareLock;
	fkrte->requiredPerms = ACL_SELECT;

	for (int i = 0; i < riinfo->nkeys; i++)
	{
		int			attno;

		attno = riinfo->pk_attnums[i] - FirstLowInvalidHeapAttributeNumber;
		pkrte->selectedCols = bms_add_member(pkrte->selectedCols, attno);

		attno = riinfo->fk_attnums[i] - FirstLowInvalidHeapAttributeNumber;
		fkrte->selectedCols = bms_add_member(fkrte->selectedCols, attno);
	}

	if (!ExecCheckRTPerms(list_make2(fkrte, pkrte), false))
		return false;

	/*
	 * Also punt if RLS is enabled on either table unless this role has the
	 * bypassrls right or is the table owner of the table(s) involved which
	 * have RLS enabled.
	 */
	if (!has_bypassrls_privilege(GetUserId()) &&
		((pk_rel->rd_rel->relrowsecurity &&
		  !pg_class_ownercheck(pkrte->relid, GetUserId())) ||
		 (fk_rel->rd_rel->relrowsecurity &&
		  !pg_class_ownercheck(fkrte->relid, GetUserId()))))
		return false;

	/*----------
	 * The query string built is:
	 *	SELECT fk.keycols FROM [ONLY] relname fk
	 *	 LEFT OUTER JOIN [ONLY] pkrelname pk
	 *	 ON (pk.pkkeycol1=fk.keycol1 [AND ...])
	 *	 WHERE pk.pkkeycol1 IS NULL AND
	 * For MATCH SIMPLE:
	 *	 (fk.keycol1 IS NOT NULL [AND ...])
	 * For MATCH FULL:
	 *	 (fk.keycol1 IS NOT NULL [OR ...])
	 *
	 * We attach COLLATE clauses to the operators when comparing columns
	 * that have different collations.
	 *----------
	 */
	initStringInfo(&querybuf);
	appendStringInfoString(&querybuf, "SELECT ");
	sep = "";
	for (int i = 0; i < riinfo->nkeys; i++)
	{
		quoteOneName(fkattname,
					 RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(&querybuf, "%sfk.%s", sep, fkattname);
		sep = ", ";
	}

	quoteRelationName(pkrelname, pk_rel);
	quoteRelationName(fkrelname, fk_rel);
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	pk_only = pk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	appendStringInfo(&querybuf,
					 " FROM %s%s fk LEFT OUTER JOIN %s%s pk ON (",
					 fk_only, fkrelname, pk_only, pkrelname);

	ri_GenerateQual(&querybuf, "AND", riinfo->nkeys,
					"pk", pk_rel, riinfo->pk_attnums,
					"fk", fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_NONE, NULL);

	/*
	 * It's sufficient to test any one pk attribute for null to detect a join
	 * failure.
	 */
	quoteOneName(pkattname, RIAttName(pk_rel, riinfo->pk_attnums[0]));
	appendStringInfo(&querybuf, ") WHERE pk.%s IS NULL AND (", pkattname);

	sep = "";
	for (int i = 0; i < riinfo->nkeys; i++)
	{
		quoteOneName(fkattname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(&querybuf,
						 "%sfk.%s IS NOT NULL",
						 sep, fkattname);
		switch (riinfo->confmatchtype)
		{
			case FKCONSTR_MATCH_SIMPLE:
				sep = " AND ";
				break;
			case FKCONSTR_MATCH_FULL:
				sep = " OR ";
				break;
		}
	}
	appendStringInfoChar(&querybuf, ')');

	/*
	 * Temporarily increase work_mem so that the check query can be executed
	 * more efficiently.  It seems okay to do this because the query is simple
	 * enough to not use a multiple of work_mem, and one typically would not
	 * have many large foreign-key validations happening concurrently.  So
	 * this seems to meet the criteria for being considered a "maintenance"
	 * operation, and accordingly we use maintenance_work_mem.  However, we
	 * must also set hash_mem_multiplier to 1, since it is surely not okay to
	 * let that get applied to the maintenance_work_mem value.
	 *
	 * We use the equivalent of a function SET option to allow the setting to
	 * persist for exactly the duration of the check query.  guc.c also takes
	 * care of undoing the setting on error.
	 */
	save_nestlevel = NewGUCNestLevel();

	snprintf(workmembuf, sizeof(workmembuf), "%d", maintenance_work_mem);
	(void) set_config_option("work_mem", workmembuf,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);
	(void) set_config_option("hash_mem_multiplier", "1",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Generate the plan.  We don't need to cache it, and there are no
	 * arguments to the plan.
	 */
	qplan = SPI_prepare(querybuf.data, 0, NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s",
			 SPI_result_code_string(SPI_result), querybuf.data);

	/*
	 * Run the plan.  For safety we force a current snapshot to be used. (In
	 * transaction-snapshot mode, this arguably violates transaction isolation
	 * rules, but we really haven't got much choice.) We don't need to
	 * register the snapshot, because SPI_execute_snapshot will see to it. We
	 * need at most one tuple returned, so pass limit = 1.
	 */
	spi_result = SPI_execute_snapshot(qplan,
									  NULL, NULL,
									  GetLatestSnapshot(),
									  InvalidSnapshot,
									  true, false, 1);

	/* Check result */
	if (spi_result != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	/* Did we find a tuple violating the constraint? */
	if (SPI_processed > 0)
	{
		TupleTableSlot *slot;
		HeapTuple	tuple = SPI_tuptable->vals[0];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		RI_ConstraintInfo fake_riinfo;

		slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);

		heap_deform_tuple(tuple, tupdesc,
						  slot->tts_values, slot->tts_isnull);
		ExecStoreVirtualTuple(slot);

		/*
		 * The columns to look at in the result tuple are 1..N, not whatever
		 * they are in the fk_rel.  Hack up riinfo so that the subroutines
		 * called here will behave properly.
		 *
		 * In addition to this, we have to pass the correct tupdesc to
		 * ri_ReportViolation, overriding its normal habit of using the pk_rel
		 * or fk_rel's tupdesc.
		 */
		memcpy(&fake_riinfo, riinfo, sizeof(RI_ConstraintInfo));
		for (int i = 0; i < fake_riinfo.nkeys; i++)
			fake_riinfo.fk_attnums[i] = i + 1;

		/*
		 * If it's MATCH FULL, and there are any nulls in the FK keys,
		 * complain about that rather than the lack of a match.  MATCH FULL
		 * disallows partially-null FK rows.
		 */
		if (fake_riinfo.confmatchtype == FKCONSTR_MATCH_FULL &&
			ri_NullCheck(tupdesc, slot, &fake_riinfo, false, false) != RI_KEYS_NONE_NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",
							RelationGetRelationName(fk_rel),
							NameStr(fake_riinfo.conname)),
					 errdetail("MATCH FULL does not allow mixing of null and nonnull key values."),
					 errtableconstraint(fk_rel,
										NameStr(fake_riinfo.conname))));

		/*
		 * We tell ri_ReportViolation we were doing the RI_PLAN_CHECK_LOOKUPPK
		 * query, which isn't true, but will cause it to use
		 * fake_riinfo.fk_attnums as we need.
		 */
		ri_ReportViolation(&fake_riinfo,
						   pk_rel, fk_rel,
						   slot, tupdesc,
						   RI_PLAN_CHECK_LOOKUPPK_INS, false);

		ExecDropSingleTupleTableSlot(slot);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/*
	 * Restore work_mem and hash_mem_multiplier.
	 */
	AtEOXact_GUC(true, save_nestlevel);

	return true;
}

/*
 * RI_PartitionRemove_Check -
 *
 * Verify no referencing values exist, when a partition is detached on
 * the referenced side of a foreign key constraint.
 */
void
RI_PartitionRemove_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel)
{
	const RI_ConstraintInfo *riinfo;
	StringInfoData querybuf;
	char	   *constraintDef;
	char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		fkattname[MAX_QUOTED_NAME_LEN + 3];
	const char *sep;
	const char *fk_only;
	int			save_nestlevel;
	char		workmembuf[32];
	int			spi_result;
	SPIPlanPtr	qplan;
	int			i;

	riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

	/*
	 * We don't check permissions before displaying the error message, on the
	 * assumption that the user detaching the partition must have enough
	 * privileges to examine the table contents anyhow.
	 */

	/*----------
	 * The query string built is:
	 *  SELECT fk.keycols FROM [ONLY] relname fk
	 *    JOIN pkrelname pk
	 *    ON (pk.pkkeycol1=fk.keycol1 [AND ...])
	 *    WHERE (<partition constraint>) AND
	 * For MATCH SIMPLE:
	 *   (fk.keycol1 IS NOT NULL [AND ...])
	 * For MATCH FULL:
	 *   (fk.keycol1 IS NOT NULL [OR ...])
	 *
	 * We attach COLLATE clauses to the operators when comparing columns
	 * that have different collations.
	 *----------
	 */
	initStringInfo(&querybuf);
	appendStringInfoString(&querybuf, "SELECT ");
	sep = "";
	for (i = 0; i < riinfo->nkeys; i++)
	{
		quoteOneName(fkattname,
					 RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(&querybuf, "%sfk.%s", sep, fkattname);
		sep = ", ";
	}

	quoteRelationName(pkrelname, pk_rel);
	quoteRelationName(fkrelname, fk_rel);
	fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
		"" : "ONLY ";
	appendStringInfo(&querybuf,
					 " FROM %s%s fk JOIN %s pk ON (",
					 fk_only, fkrelname, pkrelname);

	ri_GenerateQual(&querybuf, "AND", riinfo->nkeys,
					"pk", pk_rel, riinfo->pk_attnums,
					"fk", fk_rel, riinfo->fk_attnums,
					riinfo->pf_eq_oprs,
					GQ_PARAMS_NONE, NULL);

	/*
	 * Start the WHERE clause with the partition constraint (except if this is
	 * the default partition and there's no other partition, because the
	 * partition constraint is the empty string in that case.)
	 */
	constraintDef = pg_get_partconstrdef_string(RelationGetRelid(pk_rel), "pk");
	if (constraintDef && constraintDef[0] != '\0')
		appendStringInfo(&querybuf, ") WHERE %s AND (",
						 constraintDef);
	else
		appendStringInfo(&querybuf, ") WHERE (");

	sep = "";
	for (i = 0; i < riinfo->nkeys; i++)
	{
		quoteOneName(fkattname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
		appendStringInfo(&querybuf,
						 "%sfk.%s IS NOT NULL",
						 sep, fkattname);
		switch (riinfo->confmatchtype)
		{
			case FKCONSTR_MATCH_SIMPLE:
				sep = " AND ";
				break;
			case FKCONSTR_MATCH_FULL:
				sep = " OR ";
				break;
		}
	}
	appendStringInfoChar(&querybuf, ')');

	/*
	 * Temporarily increase work_mem so that the check query can be executed
	 * more efficiently.  It seems okay to do this because the query is simple
	 * enough to not use a multiple of work_mem, and one typically would not
	 * have many large foreign-key validations happening concurrently.  So
	 * this seems to meet the criteria for being considered a "maintenance"
	 * operation, and accordingly we use maintenance_work_mem.  However, we
	 * must also set hash_mem_multiplier to 1, since it is surely not okay to
	 * let that get applied to the maintenance_work_mem value.
	 *
	 * We use the equivalent of a function SET option to allow the setting to
	 * persist for exactly the duration of the check query.  guc.c also takes
	 * care of undoing the setting on error.
	 */
	save_nestlevel = NewGUCNestLevel();

	snprintf(workmembuf, sizeof(workmembuf), "%d", maintenance_work_mem);
	(void) set_config_option("work_mem", workmembuf,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);
	(void) set_config_option("hash_mem_multiplier", "1",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Generate the plan.  We don't need to cache it, and there are no
	 * arguments to the plan.
	 */
	qplan = SPI_prepare(querybuf.data, 0, NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s",
			 SPI_result_code_string(SPI_result), querybuf.data);

	/*
	 * Run the plan.  For safety we force a current snapshot to be used. (In
	 * transaction-snapshot mode, this arguably violates transaction isolation
	 * rules, but we really haven't got much choice.) We don't need to
	 * register the snapshot, because SPI_execute_snapshot will see to it. We
	 * need at most one tuple returned, so pass limit = 1.
	 */
	spi_result = SPI_execute_snapshot(qplan,
									  NULL, NULL,
									  GetLatestSnapshot(),
									  InvalidSnapshot,
									  true, false, 1);

	/* Check result */
	if (spi_result != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	/* Did we find a tuple that would violate the constraint? */
	if (SPI_processed > 0)
	{
		TupleTableSlot *slot;
		HeapTuple	tuple = SPI_tuptable->vals[0];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		RI_ConstraintInfo fake_riinfo;

		slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);

		heap_deform_tuple(tuple, tupdesc,
						  slot->tts_values, slot->tts_isnull);
		ExecStoreVirtualTuple(slot);

		/*
		 * The columns to look at in the result tuple are 1..N, not whatever
		 * they are in the fk_rel.  Hack up riinfo so that ri_ReportViolation
		 * will behave properly.
		 *
		 * In addition to this, we have to pass the correct tupdesc to
		 * ri_ReportViolation, overriding its normal habit of using the pk_rel
		 * or fk_rel's tupdesc.
		 */
		memcpy(&fake_riinfo, riinfo, sizeof(RI_ConstraintInfo));
		for (i = 0; i < fake_riinfo.nkeys; i++)
			fake_riinfo.pk_attnums[i] = i + 1;

		ri_ReportViolation(&fake_riinfo, pk_rel, fk_rel,
						   slot, tupdesc, 0, true);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/*
	 * Restore work_mem and hash_mem_multiplier.
	 */
	AtEOXact_GUC(true, save_nestlevel);
}


/* ----------
 * Local functions below
 * ----------
 */


/*
 * quoteOneName --- safely quote a single SQL name
 *
 * buffer must be MAX_QUOTED_NAME_LEN long (includes room for \0)
 */
static void
quoteOneName(char *buffer, const char *name)
{
	/* Rather than trying to be smart, just always quote it. */
	*buffer++ = '"';
	while (*name)
	{
		if (*name == '"')
			*buffer++ = '"';
		*buffer++ = *name++;
	}
	*buffer++ = '"';
	*buffer = '\0';
}

/*
 * quoteRelationName --- safely quote a fully qualified relation name
 *
 * buffer must be MAX_QUOTED_REL_NAME_LEN long (includes room for \0)
 */
static void
quoteRelationName(char *buffer, Relation rel)
{
	quoteOneName(buffer, get_namespace_name(RelationGetNamespace(rel)));
	buffer += strlen(buffer);
	*buffer++ = '.';
	quoteOneName(buffer, RelationGetRelationName(rel));
}

/*
 * ri_GenerateQual --- generate WHERE/ON clause.
 *
 * Note: to avoid unnecessary explicit casts, make sure that the left and
 * right operands match eq_oprs expect (ie don't swap the left and right
 * operands accidentally).
 */
static void
ri_GenerateQual(StringInfo buf, char *sep, int nkeys,
				const char *ltabname, Relation lrel,
				const int16 *lattnums,
				const char *rtabname, Relation rrel,
				const int16 *rattnums,
				const Oid *eq_oprs,
				GenQualParams params,
				Oid *paramtypes)
{
	for (int i = 0; i < nkeys; i++)
	{
		Oid			ltype = RIAttType(lrel, lattnums[i]);
		Oid			rtype = RIAttType(rrel, rattnums[i]);
		Oid			lcoll = RIAttCollation(lrel, lattnums[i]);
		Oid			rcoll = RIAttCollation(rrel, rattnums[i]);
		char		paramname[16];
		char	   *latt,
				   *ratt;
		char	   *sep_current = i > 0 ? sep : NULL;

		if (params != GQ_PARAMS_NONE)
			sprintf(paramname, "$%d", i + 1);

		if (params == GQ_PARAMS_LEFT)
		{
			latt = paramname;
			paramtypes[i] = ltype;
		}
		else
			latt = ri_ColNameQuoted(ltabname, RIAttName(lrel, lattnums[i]));

		if (params == GQ_PARAMS_RIGHT)
		{
			ratt = paramname;
			paramtypes[i] = rtype;
		}
		else
			ratt = ri_ColNameQuoted(rtabname, RIAttName(rrel, rattnums[i]));

		ri_GenerateQualComponent(buf, sep_current, latt, ltype, eq_oprs[i],
								 ratt, rtype);

		if (lcoll != rcoll)
			ri_GenerateQualCollation(buf, lcoll);
	}
}

/*
 * ri_GenerateQual --- generate a component of WHERE/ON clause equating two
 * variables, to be AND-ed to the other components.
 *
 * This basically appends " sep leftop op rightop" to buf, adding casts
 * and schema qualification as needed to ensure that the parser will select
 * the operator we specify.  leftop and rightop should be parenthesized
 * if they aren't variables or parameters.
 */
static void
ri_GenerateQualComponent(StringInfo buf,
						 const char *sep,
						 const char *leftop, Oid leftoptype,
						 Oid opoid,
						 const char *rightop, Oid rightoptype)
{
	if (sep)
		appendStringInfo(buf, " %s ", sep);
	generate_operator_clause(buf, leftop, leftoptype, opoid,
							 rightop, rightoptype);
}

/*
 * ri_GenerateKeyList --- generate comma-separated list of key attributes.
 */
static void
ri_GenerateKeyList(StringInfo buf, int nkeys,
				   const char *tabname, Relation rel,
				   const int16 *attnums)
{
	for (int i = 0; i < nkeys; i++)
	{
		char	   *att = ri_ColNameQuoted(tabname, RIAttName(rel, attnums[i]));

		if (i > 0)
			appendStringInfoString(buf, ", ");

		appendStringInfoString(buf, att);
	}
}

/*
 * ri_ColNameQuoted() --- return column name, with both table and column name
 * quoted.
 */
static char *
ri_ColNameQuoted(const char *tabname, const char *attname)
{
	char		quoted[MAX_QUOTED_NAME_LEN];
	StringInfo	result = makeStringInfo();

	if (tabname && strlen(tabname) > 0)
	{
		quoteOneName(quoted, tabname);
		appendStringInfo(result, "%s.", quoted);
	}

	quoteOneName(quoted, attname);
	appendStringInfoString(result, quoted);

	return result->data;
}

/*
 * Check that RI trigger function was called in expected context
 */
static void
ri_CheckTrigger(FunctionCallInfo fcinfo, const char *funcname, int tgkind)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager", funcname)));

	/*
	 * Check proper event
	 */
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER ROW", funcname)));

	switch (tgkind)
	{
		case RI_TRIGTYPE_INSERT:
			if (!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for INSERT", funcname)));
			break;
		case RI_TRIGTYPE_UPDATE:
			if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for UPDATE", funcname)));
			break;

		case RI_TRIGTYPE_DELETE:
			if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for DELETE", funcname)));
			break;
	}
}

/*
 * ri_GenerateQualCollation --- add a COLLATE spec to a WHERE clause
 *
 * At present, we intentionally do not use this function for RI queries that
 * compare a variable to a $n parameter.  Since parameter symbols always have
 * default collation, the effect will be to use the variable's collation.
 * Now that is only strictly correct when testing the referenced column, since
 * the SQL standard specifies that RI comparisons should use the referenced
 * column's collation.  However, so long as all collations have the same
 * notion of equality (which they do, because texteq reduces to bitwise
 * equality), there's no visible semantic impact from using the referencing
 * column's collation when testing it, and this is a good thing to do because
 * it lets us use a normal index on the referencing column.  However, we do
 * have to use this function when directly comparing the referencing and
 * referenced columns, if they are of different collations; else the parser
 * will fail to resolve the collation to use.
 */
static void
ri_GenerateQualCollation(StringInfo buf, Oid collation)
{
	HeapTuple	tp;
	Form_pg_collation colltup;
	char	   *collname;
	char		onename[MAX_QUOTED_NAME_LEN];

	/* Nothing to do if it's a noncollatable data type */
	if (!OidIsValid(collation))
		return;

	tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for collation %u", collation);
	colltup = (Form_pg_collation) GETSTRUCT(tp);
	collname = NameStr(colltup->collname);

	/*
	 * We qualify the name always, for simplicity and to ensure the query is
	 * not search-path-dependent.
	 */
	quoteOneName(onename, get_namespace_name(colltup->collnamespace));
	appendStringInfo(buf, " COLLATE %s", onename);
	quoteOneName(onename, collname);
	appendStringInfo(buf, ".%s", onename);

	ReleaseSysCache(tp);
}

/* ----------
 * ri_BuildQueryKey -
 *
 *	Construct a hashtable key for a prepared SPI plan of an FK constraint.
 *
 *		key: output argument, *key is filled in based on the other arguments
 *		riinfo: info from pg_constraint entry
 *		constr_queryno: an internal number identifying the query type
 *			(see RI_PLAN_XXX constants at head of file)
 * ----------
 */
static void
ri_BuildQueryKey(RI_QueryKey *key, const RI_ConstraintInfo *riinfo,
				 int32 constr_queryno, bool single_row)
{
	/*
	 * We assume struct RI_QueryKey contains no padding bytes, else we'd need
	 * to use memset to clear them.
	 */
	key->constr_id = riinfo->constraint_id;
	key->constr_queryno = constr_queryno;
	key->single_row = single_row;
}

/*
 * Fetch the RI_ConstraintInfo struct for the trigger's FK constraint.
 */
static const RI_ConstraintInfo *
ri_FetchConstraintInfo(Trigger *trigger, Relation trig_rel, bool rel_is_pk)
{
	Oid			constraintOid = trigger->tgconstraint;
	const RI_ConstraintInfo *riinfo;

	/*
	 * Check that the FK constraint's OID is available; it might not be if
	 * we've been invoked via an ordinary trigger or an old-style "constraint
	 * trigger".
	 */
	if (!OidIsValid(constraintOid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("no pg_constraint entry for trigger \"%s\" on table \"%s\"",
						trigger->tgname, RelationGetRelationName(trig_rel)),
				 errhint("Remove this referential integrity trigger and its mates, then do ALTER TABLE ADD CONSTRAINT.")));

	/* Find or create a hashtable entry for the constraint */
	riinfo = ri_LoadConstraintInfo(constraintOid, trig_rel, rel_is_pk);

	/* Do some easy cross-checks against the trigger call data */
	if (rel_is_pk)
	{
		if (riinfo->fk_relid != trigger->tgconstrrelid ||
			riinfo->pk_relid != RelationGetRelid(trig_rel))
			elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
				 trigger->tgname, RelationGetRelationName(trig_rel));
	}
	else
	{
		if (riinfo->fk_relid != RelationGetRelid(trig_rel) ||
			riinfo->pk_relid != trigger->tgconstrrelid)
			elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
				 trigger->tgname, RelationGetRelationName(trig_rel));
	}

	if (riinfo->confmatchtype != FKCONSTR_MATCH_FULL &&
		riinfo->confmatchtype != FKCONSTR_MATCH_PARTIAL &&
		riinfo->confmatchtype != FKCONSTR_MATCH_SIMPLE)
		elog(ERROR, "unrecognized confmatchtype: %d",
			 riinfo->confmatchtype);

	if (riinfo->confmatchtype == FKCONSTR_MATCH_PARTIAL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("MATCH PARTIAL not yet implemented")));

	return riinfo;
}

/*
 * Fetch or create the RI_ConstraintInfo struct for an FK constraint.
 */
static const RI_ConstraintInfo *
ri_LoadConstraintInfo(Oid constraintOid, Relation trig_rel, bool rel_is_pk)
{
	RI_ConstraintInfo *riinfo;
	bool		found;
	HeapTuple	tup;
	Form_pg_constraint conForm;
	MemoryContext oldcxt;
	TupleDesc	tupdesc;
	Relation	pk_rel,
				fk_rel;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_constraint_cache)
		ri_InitHashTables();

	/*
	 * Find or create a hash entry.  If we find a valid one, just return it.
	 */
	riinfo = (RI_ConstraintInfo *) hash_search(ri_constraint_cache,
											   (void *) &constraintOid,
											   HASH_ENTER, &found);
	if (!found)
	{
		riinfo->valid = false;
		riinfo->slot_mcxt = AllocSetContextCreate(TopMemoryContext,
												  "RI_ConstraintInfoSlots",
												  ALLOCSET_SMALL_SIZES);
	}
	else if (riinfo->valid)
		return riinfo;

	/*
	 * Fetch the pg_constraint row so we can fill in the entry.
	 */
	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for constraint %u", constraintOid);
	conForm = (Form_pg_constraint) GETSTRUCT(tup);

	if (conForm->contype != CONSTRAINT_FOREIGN) /* should not happen */
		elog(ERROR, "constraint %u is not a foreign key constraint",
			 constraintOid);

	/* And extract data */
	Assert(riinfo->constraint_id == constraintOid);
	riinfo->oidHashValue = GetSysCacheHashValue1(CONSTROID,
												 ObjectIdGetDatum(constraintOid));
	memcpy(&riinfo->conname, &conForm->conname, sizeof(NameData));
	riinfo->pk_relid = conForm->confrelid;
	riinfo->fk_relid = conForm->conrelid;
	riinfo->confupdtype = conForm->confupdtype;
	riinfo->confdeltype = conForm->confdeltype;
	riinfo->confmatchtype = conForm->confmatchtype;

	DeconstructFkConstraintRow(tup,
							   &riinfo->nkeys,
							   riinfo->fk_attnums,
							   riinfo->pk_attnums,
							   riinfo->pf_eq_oprs,
							   riinfo->pp_eq_oprs,
							   riinfo->ff_eq_oprs);

	ReleaseSysCache(tup);

	/*
	 * Construct auxiliary tuple descriptors containing only the key
	 * attributes.
	 */
	if (rel_is_pk)
	{
		pk_rel = trig_rel;
		fk_rel = table_open(riinfo->fk_relid, AccessShareLock);
	}
	else
	{
		pk_rel = table_open(riinfo->pk_relid, AccessShareLock);
		fk_rel = trig_rel;
	}

	/*
	 * Use a separate memory context for the slots so that memory does not
	 * leak if the riinfo needs to be reloaded.
	 */
	MemoryContextReset(riinfo->slot_mcxt);
	oldcxt = MemoryContextSwitchTo(riinfo->slot_mcxt);

	/* The PK attributes. */
	tupdesc = CreateTemplateTupleDesc(riinfo->nkeys);
	add_key_attrs_to_tupdesc(tupdesc, pk_rel, riinfo, riinfo->pk_attnums, 1,
							 false);
	riinfo->slot_pk = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

	/* The FK attributes. */
	tupdesc = CreateTemplateTupleDesc(riinfo->nkeys);
	add_key_attrs_to_tupdesc(tupdesc, fk_rel, riinfo, riinfo->fk_attnums, 1,
							 false);
	riinfo->slot_fk = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

	/*
	 * The descriptor to store both NEW and OLD tuple into when processing ON
	 * UPDATE CASCADE.
	 */
	tupdesc = CreateTemplateTupleDesc(2 * riinfo->nkeys);
	/* Add the key attributes for both NEW and OLD. */
	add_key_attrs_to_tupdesc(tupdesc, pk_rel, riinfo, riinfo->pk_attnums, 1,
							 true);
	add_key_attrs_to_tupdesc(tupdesc, pk_rel, riinfo, riinfo->pk_attnums,
							 riinfo->nkeys + 1, true);
	riinfo->slot_both = MakeSingleTupleTableSlot(tupdesc,
												 &TTSOpsMinimalTuple);

	MemoryContextSwitchTo(oldcxt);

	if (rel_is_pk)
		table_close(fk_rel, AccessShareLock);
	else
		table_close(pk_rel, AccessShareLock);

	/*
	 * For efficient processing of invalidation messages below, we keep a
	 * doubly-linked list, and a count, of all currently valid entries.
	 */
	dlist_push_tail(&ri_constraint_cache_valid_list, &riinfo->valid_link);
	ri_constraint_cache_valid_count++;

	riinfo->valid = true;

	return riinfo;
}

/*
 * Callback for pg_constraint inval events
 *
 * While most syscache callbacks just flush all their entries, pg_constraint
 * gets enough update traffic that it's probably worth being smarter.
 * Invalidate any ri_constraint_cache entry associated with the syscache
 * entry with the specified hash value, or all entries if hashvalue == 0.
 *
 * Note: at the time a cache invalidation message is processed there may be
 * active references to the cache.  Because of this we never remove entries
 * from the cache, but only mark them invalid, which is harmless to active
 * uses.  (Any query using an entry should hold a lock sufficient to keep that
 * data from changing under it --- but we may get cache flushes anyway.)
 */
static void
InvalidateConstraintCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	dlist_mutable_iter iter;

	Assert(ri_constraint_cache != NULL);

	/*
	 * If the list of currently valid entries gets excessively large, we mark
	 * them all invalid so we can empty the list.  This arrangement avoids
	 * O(N^2) behavior in situations where a session touches many foreign keys
	 * and also does many ALTER TABLEs, such as a restore from pg_dump.
	 */
	if (ri_constraint_cache_valid_count > 1000)
		hashvalue = 0;			/* pretend it's a cache reset */

	dlist_foreach_modify(iter, &ri_constraint_cache_valid_list)
	{
		RI_ConstraintInfo *riinfo = dlist_container(RI_ConstraintInfo,
													valid_link, iter.cur);

		if (hashvalue == 0 || riinfo->oidHashValue == hashvalue)
		{
			riinfo->valid = false;
			/* Remove invalidated entries from the list, too */
			dlist_delete(iter.cur);
			ri_constraint_cache_valid_count--;
		}
	}
}


/*
 * Prepare execution plan for a query to enforce an RI restriction
 *
 * If cache_plan is true, the plan is saved into our plan hashtable
 * so that we don't need to plan it again.
 */
static SPIPlanPtr
ri_PlanCheck(const char *querystr, int nargs, Oid *argtypes,
			 RI_QueryKey *qkey, Relation fk_rel,
			 Relation pk_rel)
{
	SPIPlanPtr	qplan;
	Relation	query_rel;
	Oid			save_userid;
	int			save_sec_context;

	/*
	 * Use the query type code to determine whether the query is run against
	 * the PK or FK table; we'll do the check as that table's owner
	 */
	if (qkey->constr_queryno <= RI_PLAN_LAST_ON_PK)
		query_rel = pk_rel;
	else
		query_rel = fk_rel;

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Create the plan */
	qplan = SPI_prepare(querystr, nargs, nargs > 0 ? argtypes : NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), querystr);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Save the plan */
	SPI_keepplan(qplan);
	ri_HashPreparedPlan(qkey, qplan);

	return qplan;
}

/*
 * Perform a query to enforce an RI restriction
 */
static bool
ri_PerformCheck(const RI_ConstraintInfo *riinfo,
				RI_QueryKey *qkey, SPIPlanPtr qplan,
				Relation fk_rel, Relation pk_rel,
				TupleTableSlot *slot,
				bool detectNewRows, int expect_OK)
{
	Relation	query_rel;
	Snapshot	test_snapshot;
	Snapshot	crosscheck_snapshot;
	int			limit;
	int			spi_result;
	Oid			save_userid;
	int			save_sec_context;
	Datum		vals_loc[RI_MAX_NUMKEYS * 2];
	char		nulls_loc[RI_MAX_NUMKEYS * 2];
	Datum	   *vals = NULL;
	char	   *nulls = NULL;

	/*
	 * Use the query type code to determine whether the query is run against
	 * the PK or FK table; we'll do the check as that table's owner
	 */
	if (qkey->constr_queryno <= RI_PLAN_LAST_ON_PK)
		query_rel = pk_rel;
	else
		query_rel = fk_rel;

	if (slot)
	{
		int			nparams = riinfo->nkeys;

		vals = vals_loc;
		nulls = nulls_loc;

		/* Extract the parameters to be passed into the query */
		ri_ExtractValues(slot, 0, nparams, vals, nulls);

		if (slot->tts_tupleDescriptor->natts != nparams)
		{
			/*
			 * In a special case (ON UPDATE CASCADE) the slot may contain both
			 * new and old values of the key.
			 */
			Assert(slot->tts_tupleDescriptor->natts == nparams * 2);

			ri_ExtractValues(slot, nparams, nparams, vals, nulls);
		}
	}

	/*
	 * In READ COMMITTED mode, we just need to use an up-to-date regular
	 * snapshot, and we will see all rows that could be interesting. But in
	 * transaction-snapshot mode, we can't change the transaction snapshot. If
	 * the caller passes detectNewRows == false then it's okay to do the query
	 * with the transaction snapshot; otherwise we use a current snapshot, and
	 * tell the executor to error out if it finds any rows under the current
	 * snapshot that wouldn't be visible per the transaction snapshot.  Note
	 * that SPI_execute_snapshot will register the snapshots, so we don't need
	 * to bother here.
	 */
	if (IsolationUsesXactSnapshot() && detectNewRows)
	{
		CommandCounterIncrement();	/* be sure all my own work is visible */
		test_snapshot = GetLatestSnapshot();
		crosscheck_snapshot = GetTransactionSnapshot();
	}
	else
	{
		/* the default SPI behavior is okay */
		test_snapshot = InvalidSnapshot;
		crosscheck_snapshot = InvalidSnapshot;
	}

	/*
	 * If this is a select query (e.g., for a 'no action' or 'restrict'
	 * trigger), we only need to see if there is a single row in the table,
	 * matching the key.  Otherwise, limit = 0 - because we want the query to
	 * affect ALL the matching rows.
	 */
	limit = (expect_OK == SPI_OK_SELECT) ? 1 : 0;

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Finally we can run the query. */
	spi_result = SPI_execute_snapshot(qplan,
									  vals, nulls,
									  test_snapshot, crosscheck_snapshot,
									  false, false, limit);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Check result */
	if (spi_result < 0)
		elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	if (expect_OK >= 0 && spi_result != expect_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("referential integrity query on \"%s\" from constraint \"%s\" on \"%s\" gave unexpected result",
						RelationGetRelationName(pk_rel),
						NameStr(riinfo->conname),
						RelationGetRelationName(fk_rel)),
				 errhint("This is most likely due to a rule having rewritten the query.")));

	return SPI_processed > 0;
}

/*
 * Extract fields from a tuple into Datum/nulls arrays
 */
static void
ri_ExtractValues(TupleTableSlot *slot, int first, int nkeys, Datum *vals,
				 char *nulls)
{
	bool		isnull;

	for (int i = first; i < first + nkeys; i++)
	{
		vals[i] = slot_getattr(slot, i + 1, &isnull);
		nulls[i] = isnull ? 'n' : ' ';
	}
}

/*
 * Produce an error report
 *
 * If the failed constraint was on insert/update to the FK table,
 * we want the key names and values extracted from there, and the error
 * message to look like 'key blah is not present in PK'.
 * Otherwise, the attr names and values come from the PK table and the
 * message looks like 'key blah is still referenced from FK'.
 */
static void
ri_ReportViolation(const RI_ConstraintInfo *riinfo,
				   Relation pk_rel, Relation fk_rel,
				   TupleTableSlot *violatorslot, TupleDesc tupdesc,
				   int queryno, bool partgone)
{
	StringInfoData key_names;
	StringInfoData key_values;
	bool		onfk;
	const int16 *attnums;
	Oid			rel_oid;
	AclResult	aclresult;
	bool		has_perm = true;

	/*
	 * Determine which relation to complain about.
	 */
	onfk = (queryno == RI_PLAN_CHECK_LOOKUPPK_SINGLE ||
			queryno == RI_PLAN_CHECK_LOOKUPPK_INS ||
			queryno == RI_PLAN_CHECK_LOOKUPPK_UPD);
	if (onfk)
	{
		attnums = riinfo->fk_attnums;
		rel_oid = fk_rel->rd_id;
	}
	else
	{
		attnums = riinfo->pk_attnums;
		rel_oid = pk_rel->rd_id;
	}

	/*
	 * If tupdesc wasn't passed by caller, assume the violator tuple matches
	 * the descriptor of the violatorslot.
	 */
	if (tupdesc == NULL)
		tupdesc = violatorslot->tts_tupleDescriptor;

	/*
	 * Check permissions- if the user does not have access to view the data in
	 * any of the key columns then we don't include the errdetail() below.
	 *
	 * Check if RLS is enabled on the relation first.  If so, we don't return
	 * any specifics to avoid leaking data.
	 *
	 * Check table-level permissions next and, failing that, column-level
	 * privileges.
	 *
	 * When a partition at the referenced side is being detached/dropped, we
	 * needn't check, since the user must be the table owner anyway.
	 */
	if (partgone)
		has_perm = true;
	else if (check_enable_rls(rel_oid, InvalidOid, true) != RLS_ENABLED)
	{
		aclresult = pg_class_aclcheck(rel_oid, GetUserId(), ACL_SELECT);
		if (aclresult != ACLCHECK_OK)
		{
			/* Try for column-level permissions */
			for (int idx = 0; idx < riinfo->nkeys; idx++)
			{
				aclresult = pg_attribute_aclcheck(rel_oid, attnums[idx],
												  GetUserId(),
												  ACL_SELECT);

				/* No access to the key */
				if (aclresult != ACLCHECK_OK)
				{
					has_perm = false;
					break;
				}
			}
		}
	}
	else
		has_perm = false;

	if (has_perm)
	{
		/* Get printable versions of the keys involved */
		initStringInfo(&key_names);
		initStringInfo(&key_values);
		for (int idx = 0; idx < riinfo->nkeys; idx++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, idx);
			char	   *name,
					   *val;
			Datum		datum;
			bool		isnull;

			name = NameStr(att->attname);

			datum = slot_getattr(violatorslot, idx + 1, &isnull);
			if (!isnull)
			{
				Oid			foutoid;
				bool		typisvarlena;

				getTypeOutputInfo(att->atttypid, &foutoid, &typisvarlena);
				val = OidOutputFunctionCall(foutoid, datum);
			}
			else
				val = "null";

			if (idx > 0)
			{
				appendStringInfoString(&key_names, ", ");
				appendStringInfoString(&key_values, ", ");
			}
			appendStringInfoString(&key_names, name);
			appendStringInfoString(&key_values, val);
		}
	}

	if (partgone)
		ereport(ERROR,
				(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
				 errmsg("removing partition \"%s\" violates foreign key constraint \"%s\"",
						RelationGetRelationName(pk_rel),
						NameStr(riinfo->conname)),
				 errdetail("Key (%s)=(%s) is still referenced from table \"%s\".",
						   key_names.data, key_values.data,
						   RelationGetRelationName(fk_rel)),
				 errtableconstraint(fk_rel, NameStr(riinfo->conname))));
	else if (onfk)
		ereport(ERROR,
				(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
				 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",
						RelationGetRelationName(fk_rel),
						NameStr(riinfo->conname)),
				 has_perm ?
				 errdetail("Key (%s)=(%s) is not present in table \"%s\".",
						   key_names.data, key_values.data,
						   RelationGetRelationName(pk_rel)) :
				 errdetail("Key is not present in table \"%s\".",
						   RelationGetRelationName(pk_rel)),
				 errtableconstraint(fk_rel, NameStr(riinfo->conname))));
	else
		ereport(ERROR,
				(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
				 errmsg("update or delete on table \"%s\" violates foreign key constraint \"%s\" on table \"%s\"",
						RelationGetRelationName(pk_rel),
						NameStr(riinfo->conname),
						RelationGetRelationName(fk_rel)),
				 has_perm ?
				 errdetail("Key (%s)=(%s) is still referenced from table \"%s\".",
						   key_names.data, key_values.data,
						   RelationGetRelationName(fk_rel)) :
				 errdetail("Key is still referenced from table \"%s\".",
						   RelationGetRelationName(fk_rel)),
				 errtableconstraint(fk_rel, NameStr(riinfo->conname))));
}


/*
 * ri_NullCheck -
 *
 * Determine the NULL state of all key values in a tuple
 *
 * Returns one of RI_KEYS_ALL_NULL, RI_KEYS_NONE_NULL or RI_KEYS_SOME_NULL.
 *
 * If the slot only contains key columns, pass ignore_attnums=true.
 */
static int
ri_NullCheck(TupleDesc tupDesc,
			 TupleTableSlot *slot,
			 const RI_ConstraintInfo *riinfo, bool rel_is_pk,
			 bool ignore_attnums)
{
	const int16 *attnums;
	bool		allnull = true;
	bool		nonenull = true;

	if (!ignore_attnums)
	{
		if (rel_is_pk)
			attnums = riinfo->pk_attnums;
		else
			attnums = riinfo->fk_attnums;
	}

	for (int i = 0; i < riinfo->nkeys; i++)
	{
		int16		attnum;

		attnum = !ignore_attnums ? attnums[i] : i + 1;

		if (slot_attisnull(slot, attnum))
			nonenull = false;
		else
			allnull = false;

		/*
		 * If seen both NULL and non-NULL, the next attributes cannot change
		 * the result.
		 */
		if (!nonenull && !allnull)
			return RI_KEYS_SOME_NULL;
	}

	if (allnull)
		return RI_KEYS_ALL_NULL;

	if (nonenull)
		return RI_KEYS_NONE_NULL;

	/* Should not happen. */
	Assert(false);
}


/*
 * ri_InitHashTables -
 *
 * Initialize our internal hash tables.
 */
static void
ri_InitHashTables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RI_ConstraintInfo);
	ri_constraint_cache = hash_create("RI constraint cache",
									  RI_INIT_CONSTRAINTHASHSIZE,
									  &ctl, HASH_ELEM | HASH_BLOBS);

	/* Arrange to flush cache on pg_constraint changes */
	CacheRegisterSyscacheCallback(CONSTROID,
								  InvalidateConstraintCacheCallBack,
								  (Datum) 0);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RI_QueryKey);
	ctl.entrysize = sizeof(RI_QueryHashEntry);
	ri_query_cache = hash_create("RI query cache",
								 RI_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RI_CompareKey);
	ctl.entrysize = sizeof(RI_CompareHashEntry);
	ri_compare_cache = hash_create("RI compare cache",
								   RI_INIT_QUERYHASHSIZE,
								   &ctl, HASH_ELEM | HASH_BLOBS);
}


/*
 * ri_FetchPreparedPlan -
 *
 * Lookup for a query key in our private hash table of prepared
 * and saved SPI execution plans. Return the plan if found or NULL.
 */
static SPIPlanPtr
ri_FetchPreparedPlan(RI_QueryKey *key)
{
	RI_QueryHashEntry *entry;
	SPIPlanPtr	plan;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_query_cache)
		ri_InitHashTables();

	/*
	 * Lookup for the key
	 */
	entry = (RI_QueryHashEntry *) hash_search(ri_query_cache,
											  (void *) key,
											  HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/*
	 * Check whether the plan is still valid.  If it isn't, we don't want to
	 * simply rely on plancache.c to regenerate it; rather we should start
	 * from scratch and rebuild the query text too.  This is to cover cases
	 * such as table/column renames.  We depend on the plancache machinery to
	 * detect possible invalidations, though.
	 *
	 * CAUTION: this check is only trustworthy if the caller has already
	 * locked both FK and PK rels.
	 */
	plan = entry->plan;
	if (plan && SPI_plan_is_valid(plan))
		return plan;

	/*
	 * Otherwise we might as well flush the cached plan now, to free a little
	 * memory space before we make a new one.
	 */
	entry->plan = NULL;
	if (plan)
		SPI_freeplan(plan);

	return NULL;
}


/*
 * ri_HashPreparedPlan -
 *
 * Add another plan to our private SPI query plan hashtable.
 */
static void
ri_HashPreparedPlan(RI_QueryKey *key, SPIPlanPtr plan)
{
	RI_QueryHashEntry *entry;
	bool		found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_query_cache)
		ri_InitHashTables();

	/*
	 * Add the new plan.  We might be overwriting an entry previously found
	 * invalid by ri_FetchPreparedPlan.
	 */
	entry = (RI_QueryHashEntry *) hash_search(ri_query_cache,
											  (void *) key,
											  HASH_ENTER, &found);
	Assert(!found || entry->plan == NULL);
	entry->plan = plan;
}


/*
 * ri_KeysEqual -
 *
 * Check if all key values in OLD and NEW are equal.
 *
 * Note: at some point we might wish to redefine this as checking for
 * "IS NOT DISTINCT" rather than "=", that is, allow two nulls to be
 * considered equal.  Currently there is no need since all callers have
 * previously found at least one of the rows to contain no nulls.
 */
static bool
ri_KeysEqual(Relation rel, TupleTableSlot *oldslot, TupleTableSlot *newslot,
			 const RI_ConstraintInfo *riinfo, bool rel_is_pk)
{
	const int16 *attnums;

	if (rel_is_pk)
		attnums = riinfo->pk_attnums;
	else
		attnums = riinfo->fk_attnums;

	/* XXX: could be worthwhile to fetch all necessary attrs at once */
	for (int i = 0; i < riinfo->nkeys; i++)
	{
		Datum		oldvalue;
		Datum		newvalue;
		bool		isnull;

		/*
		 * Get one attribute's oldvalue. If it is NULL - they're not equal.
		 */
		oldvalue = slot_getattr(oldslot, attnums[i], &isnull);
		if (isnull)
			return false;

		/*
		 * Get one attribute's newvalue. If it is NULL - they're not equal.
		 */
		newvalue = slot_getattr(newslot, attnums[i], &isnull);
		if (isnull)
			return false;

		if (rel_is_pk)
		{
			/*
			 * If we are looking at the PK table, then do a bytewise
			 * comparison.  We must propagate PK changes if the value is
			 * changed to one that "looks" different but would compare as
			 * equal using the equality operator.  This only makes a
			 * difference for ON UPDATE CASCADE, but for consistency we treat
			 * all changes to the PK the same.
			 */
			Form_pg_attribute att = TupleDescAttr(oldslot->tts_tupleDescriptor, attnums[i] - 1);

			if (!datum_image_eq(oldvalue, newvalue, att->attbyval, att->attlen))
				return false;
		}
		else
		{
			/*
			 * For the FK table, compare with the appropriate equality
			 * operator.  Changes that compare equal will still satisfy the
			 * constraint after the update.
			 */
			if (!ri_AttributesEqual(riinfo->ff_eq_oprs[i], RIAttType(rel, attnums[i]),
									oldvalue, newvalue))
				return false;
		}
	}

	return true;
}


/*
 * ri_AttributesEqual -
 *
 * Call the appropriate equality comparison operator for two values.
 *
 * NB: we have already checked that neither value is null.
 */
static bool
ri_AttributesEqual(Oid eq_opr, Oid typeid,
				   Datum oldvalue, Datum newvalue)
{
	RI_CompareHashEntry *entry = ri_HashCompareOp(eq_opr, typeid);

	/* Do we need to cast the values? */
	if (OidIsValid(entry->cast_func_finfo.fn_oid))
	{
		oldvalue = FunctionCall3(&entry->cast_func_finfo,
								 oldvalue,
								 Int32GetDatum(-1), /* typmod */
								 BoolGetDatum(false));	/* implicit coercion */
		newvalue = FunctionCall3(&entry->cast_func_finfo,
								 newvalue,
								 Int32GetDatum(-1), /* typmod */
								 BoolGetDatum(false));	/* implicit coercion */
	}

	/*
	 * Apply the comparison operator.
	 *
	 * Note: This function is part of a call stack that determines whether an
	 * update to a row is significant enough that it needs checking or action
	 * on the other side of a foreign-key constraint.  Therefore, the
	 * comparison here would need to be done with the collation of the *other*
	 * table.  For simplicity (e.g., we might not even have the other table
	 * open), we'll just use the default collation here, which could lead to
	 * some false negatives.  All this would break if we ever allow
	 * database-wide collations to be nondeterministic.
	 */
	return DatumGetBool(FunctionCall2Coll(&entry->eq_opr_finfo,
										  DEFAULT_COLLATION_OID,
										  oldvalue, newvalue));
}

/*
 * ri_HashCompareOp -
 *
 * See if we know how to compare two values, and create a new hash entry
 * if not.
 */
static RI_CompareHashEntry *
ri_HashCompareOp(Oid eq_opr, Oid typeid)
{
	RI_CompareKey key;
	RI_CompareHashEntry *entry;
	bool		found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_compare_cache)
		ri_InitHashTables();

	/*
	 * Find or create a hash entry.  Note we're assuming RI_CompareKey
	 * contains no struct padding.
	 */
	key.eq_opr = eq_opr;
	key.typeid = typeid;
	entry = (RI_CompareHashEntry *) hash_search(ri_compare_cache,
												(void *) &key,
												HASH_ENTER, &found);
	if (!found)
		entry->valid = false;

	/*
	 * If not already initialized, do so.  Since we'll keep this hash entry
	 * for the life of the backend, put any subsidiary info for the function
	 * cache structs into TopMemoryContext.
	 */
	if (!entry->valid)
	{
		Oid			lefttype,
					righttype,
					castfunc;
		CoercionPathType pathtype;

		/* We always need to know how to call the equality operator */
		fmgr_info_cxt(get_opcode(eq_opr), &entry->eq_opr_finfo,
					  TopMemoryContext);

		/*
		 * If we chose to use a cast from FK to PK type, we may have to apply
		 * the cast function to get to the operator's input type.
		 *
		 * XXX eventually it would be good to support array-coercion cases
		 * here and in ri_AttributesEqual().  At the moment there is no point
		 * because cases involving nonidentical array types will be rejected
		 * at constraint creation time.
		 *
		 * XXX perhaps also consider supporting CoerceViaIO?  No need at the
		 * moment since that will never be generated for implicit coercions.
		 */
		op_input_types(eq_opr, &lefttype, &righttype);
		Assert(lefttype == righttype);
		if (typeid == lefttype)
			castfunc = InvalidOid;	/* simplest case */
		else
		{
			pathtype = find_coercion_pathway(lefttype, typeid,
											 COERCION_IMPLICIT,
											 &castfunc);
			if (pathtype != COERCION_PATH_FUNC &&
				pathtype != COERCION_PATH_RELABELTYPE)
			{
				/*
				 * The declared input type of the eq_opr might be a
				 * polymorphic type such as ANYARRAY or ANYENUM, or other
				 * special cases such as RECORD; find_coercion_pathway
				 * currently doesn't subsume these special cases.
				 */
				if (!IsBinaryCoercible(typeid, lefttype))
					elog(ERROR, "no conversion function from %s to %s",
						 format_type_be(typeid),
						 format_type_be(lefttype));
			}
		}
		if (OidIsValid(castfunc))
			fmgr_info_cxt(castfunc, &entry->cast_func_finfo,
						  TopMemoryContext);
		else
			entry->cast_func_finfo.fn_oid = InvalidOid;
		entry->valid = true;
	}

	return entry;
}


/*
 * Given a trigger function OID, determine whether it is an RI trigger,
 * and if so whether it is attached to PK or FK relation.
 */
int
RI_FKey_trigger_type(Oid tgfoid)
{
	switch (tgfoid)
	{
		case F_RI_FKEY_CASCADE_DEL:
		case F_RI_FKEY_CASCADE_UPD:
		case F_RI_FKEY_RESTRICT_DEL:
		case F_RI_FKEY_RESTRICT_UPD:
		case F_RI_FKEY_SETNULL_DEL:
		case F_RI_FKEY_SETNULL_UPD:
		case F_RI_FKEY_SETDEFAULT_DEL:
		case F_RI_FKEY_SETDEFAULT_UPD:
		case F_RI_FKEY_NOACTION_DEL:
		case F_RI_FKEY_NOACTION_UPD:
			return RI_TRIGGER_PK;

		case F_RI_FKEY_CHECK_INS:
		case F_RI_FKEY_CHECK_UPD:
			return RI_TRIGGER_FK;
	}

	return RI_TRIGGER_NONE;
}

/*
 * Wrapper around SPI_register_trigger_data() that lets us register the RI
 * trigger tuplestores w/o having to set tg_oldtable/tg_newtable and also w/o
 * having to set tgoldtable/tgnewtable in pg_trigger.
 *
 * XXX This is rather a hack, try to invent something better.
 */
static int
ri_register_trigger_data(TriggerData *tdata, Tuplestorestate *oldtable,
						 Tuplestorestate *newtable, TupleDesc desc)
{
	TriggerData *td = (TriggerData *) palloc(sizeof(TriggerData));
	Trigger    *tg = (Trigger *) palloc(sizeof(Trigger));
	int			result;

	Assert(tdata->tg_trigger->tgoldtable == NULL &&
		   tdata->tg_trigger->tgnewtable == NULL);

	*td = *tdata;

	td->tg_oldtable = oldtable;
	td->tg_newtable = newtable;

	*tg = *tdata->tg_trigger;
	tg->tgoldtable = pstrdup("tgoldtable");
	tg->tgnewtable = pstrdup("tgnewtable");
	td->tg_trigger = tg;
	td->desc = desc;

	result = SPI_register_trigger_data(td);

	return result;
}

/*
 * Turn TID array into a tuplestore. If snapshot is passed, only use tuples
 * visible by this snapshot.
 */
static Tuplestorestate *
get_event_tuplestore(TriggerData *trigdata, int nkeys, const int16 *attnums,
					 bool old, TupleDesc tupdesc, Snapshot snapshot)
{
	ResourceOwner saveResourceOwner;
	Tuplestorestate *result;
	TIDArray   *ta;
	ItemPointer it;
	TupleTableSlot *slot;
	int			i;
	Datum		values[RI_MAX_NUMKEYS];
	bool		isnull[RI_MAX_NUMKEYS];

	saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = CurTransactionResourceOwner;
	result = tuplestore_begin_heap(false, false, work_mem);
	CurrentResourceOwner = saveResourceOwner;

	/* XXX Shouldn't tg_trigslot and tg_newslot be the same? */
	if (old)
	{
		ta = trigdata->ri_tids_old;
		slot = trigdata->tg_trigslot;
	}
	else
	{
		ta = trigdata->ri_tids_new;
		slot = trigdata->tg_newslot;
	}

	it = ta->tids;
	for (i = 0; i < ta->n; i++)
	{
		int			j;

		CHECK_FOR_INTERRUPTS();

		ExecClearTuple(slot);

		if (!table_tuple_fetch_row_version(trigdata->tg_relation, it,
										   SnapshotAny, slot))
		{
			const char *tuple_kind = old ? "tuple1" : "tuple2";

			elog(ERROR, "failed to fetch %s for AFTER trigger", tuple_kind);
		}

		if (snapshot)
		{
			/*
			 * We should not even consider checking the row if it is no longer
			 * valid, since it was either deleted (so the deferred check
			 * should be skipped) or updated (in which case only the latest
			 * version of the row should be checked).  Test its liveness
			 * according to SnapshotSelf. We need pin and lock on the buffer
			 * to call HeapTupleSatisfiesVisibility.  Caller should be holding
			 * pin, but not lock.
			 */
			if (!table_tuple_satisfies_snapshot(trigdata->tg_relation, slot,
												snapshot))
				continue;

			/*
			 * In fact the snapshot is passed iff the slot contains a tuple of
			 * the FK table being inserted / updated, so perform one more
			 * related in this branch while we have the tuple in the slot. If
			 * we tested this later, we might need to remove the tuple later,
			 * however tuplestore.c does not support such an operation.
			 */
			if (!RI_FKey_check_query_required(trigdata->tg_trigger,
											  trigdata->tg_relation, slot))
				continue;
		}

		/*
		 * Only store the key attributes.
		 */
		for (j = 0; j < nkeys; j++)
			values[j] = slot_getattr(slot, attnums[j], &isnull[j]);

		tuplestore_putvalues(result, tupdesc, values, isnull);
		it++;
	}

	return result;
}

/*
 * Like get_event_tuplestore(), but put both old and new key values into the
 * same tuple. If the query (see RI_FKey_cascade_upd) used two tuplestores, it
 * whould have to join them somehow, but there's not suitable join column.
 */
static Tuplestorestate *
get_event_tuplestore_for_cascade_update(TriggerData *trigdata,
										const RI_ConstraintInfo *riinfo)
{
	ResourceOwner saveResourceOwner;
	Tuplestorestate *result;
	TIDArray   *ta_old,
			   *ta_new;
	ItemPointer it_old,
				it_new;
	TupleTableSlot *slot_old,
			   *slot_new;
	int			i;
	Datum	   *values,
			   *key_values;
	bool	   *nulls,
			   *key_nulls;
	MemoryContext tuple_context;
	Relation	rel = trigdata->tg_relation;
	TupleDesc	desc_rel = RelationGetDescr(rel);

	saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = CurTransactionResourceOwner;
	result = tuplestore_begin_heap(false, false, work_mem);
	CurrentResourceOwner = saveResourceOwner;

	/*
	 * This context will be used for the contents of "values".
	 *
	 * CurrentMemoryContext should be the "batch context", as passed to
	 * AfterTriggerExecuteRI().
	 */
	tuple_context =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AfterTriggerCascadeUpdateContext",
							  ALLOCSET_DEFAULT_SIZES);

	ta_old = trigdata->ri_tids_old;
	ta_new = trigdata->ri_tids_new;
	Assert(ta_old->n == ta_new->n);

	slot_old = trigdata->tg_trigslot;
	slot_new = trigdata->tg_newslot;

	key_values = (Datum *) palloc(riinfo->nkeys * 2 * sizeof(Datum));
	key_nulls = (bool *) palloc(riinfo->nkeys * 2 * sizeof(bool));
	values = (Datum *) palloc(desc_rel->natts * sizeof(Datum));
	nulls = (bool *) palloc(desc_rel->natts * sizeof(bool));

	it_old = ta_old->tids;
	it_new = ta_new->tids;
	for (i = 0; i < ta_old->n; i++)
	{
		MemoryContext oldcxt;

		MemoryContextReset(tuple_context);
		oldcxt = MemoryContextSwitchTo(tuple_context);

		/*
		 * Add the new values, followed by the old ones. This order is
		 * expected to satisfy the parameters of the query generated in
		 * ri_cascade_upd_query_single_row().
		 */
		add_key_values(slot_new, riinfo, trigdata->tg_relation, it_new,
					   key_values, key_nulls, values, nulls, 0);
		add_key_values(slot_old, riinfo, trigdata->tg_relation, it_old,
					   key_values, key_nulls, values, nulls, riinfo->nkeys);
		MemoryContextSwitchTo(oldcxt);

		tuplestore_putvalues(result, riinfo->slot_both->tts_tupleDescriptor,
							 key_values, key_nulls);

		it_old++;
		it_new++;
	}
	MemoryContextDelete(tuple_context);

	return result;
}

/*
 * Add key attributes "attnums" of relation "rel" to "tupdesc", starting at
 * position "first".
 */
static void
add_key_attrs_to_tupdesc(TupleDesc tupdesc, Relation rel,
						 const RI_ConstraintInfo *riinfo, int16 *attnums,
						 int first, bool generate_attnames)
{
	int			i;

	for (i = 0; i < riinfo->nkeys; i++)
	{
		Oid			atttypid;
		const char *attname;
		char		attname_loc[NAMEDATALEN];
		Form_pg_attribute att;

		atttypid = RIAttType(rel, attnums[i]);

		if (!generate_attnames)
			attname = RIAttName(rel, attnums[i]);
		else
		{
			const char *kind;

			/*
			 * Tne NEW/OLD order does not matter for bulk update, but the
			 * tuple must start with the NEW values so that it fits the query
			 * to check a single row when processing ON UPDATE CASCADE --- see
			 * ri_cascade_upd_query_single_row().
			 */
			kind = first == 1 ? "new" : "old";

			/*
			 * Generate unique names instead of e.g. using prefix to
			 * distinguish the old values from new ones. The prefix might be a
			 * problem due to the limited attribute name length.
			 */
			snprintf(attname_loc, NAMEDATALEN, "pkatt%d_%s", i + 1, kind);
			attname = attname_loc;
		}

		att = tupdesc->attrs;
		TupleDescInitEntry(tupdesc, first + i, attname, atttypid,
						   att->atttypmod, att->attndims);
		att++;
	}
}

/*
 * Retrieve tuple using given slot, deform it and add the attribute values to
 * "key_values" and "key_null" arrays. "values" and "nulls" is a workspace to
 * deform the tuple into. "first" tells where in the output array we should
 * start.
 */
static void
add_key_values(TupleTableSlot *slot, const RI_ConstraintInfo *riinfo,
			   Relation rel, ItemPointer ip,
			   Datum *key_values, bool *key_nulls,
			   Datum *values, bool *nulls, int first)
{
	HeapTuple	tuple;
	bool		shouldfree;
	int			i,
				c;

	ExecClearTuple(slot);
	if (!table_tuple_fetch_row_version(rel, ip, SnapshotAny, slot))
	{
		const char *tuple_kind = first == 0 ? "tuple1" : "tuple2";

		elog(ERROR, "failed to fetch %s for AFTER trigger", tuple_kind);
	}
	tuple = ExecFetchSlotHeapTuple(slot, false, &shouldfree);

	heap_deform_tuple(tuple, slot->tts_tupleDescriptor, values, nulls);

	/* Pick the key values and store them in the output arrays. */
	c = first;
	for (i = 0; i < riinfo->nkeys; i++)
	{
		int16		attnum = riinfo->pk_attnums[i];

		key_values[c] = values[attnum - 1];
		key_nulls[c] = nulls[attnum - 1];
		c++;
	}

	if (shouldfree)
		pfree(tuple);
}


/*
 * Retrieve the row that violates RI constraint and return it in a tuple slot.
 */
static TupleTableSlot *
get_violator_tuple(Relation rel)
{
	HeapTuple	tuple;
	TupleTableSlot *slot;

	Assert(SPI_tuptable && SPI_tuptable->numvals == 1);

	tuple = SPI_tuptable->vals[0];
	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc, &TTSOpsHeapTuple);
	ExecStoreHeapTuple(tuple, slot, false);
	return slot;
}
