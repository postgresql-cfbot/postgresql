/*-------------------------------------------------------------------------
 *
 * fp_triggers.c
 *
 *	Generic trigger procedures for temporal update and delete commands.
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
 * src/backend/utils/adt/fp_triggers.c
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

/* Need a little more than the possible number of columns in a table */
#define FP_MAX_ATTS					1650

#define FP_INIT_CONSTRAINTHASHSIZE		64
#define FP_INIT_QUERYHASHSIZE			(FP_INIT_CONSTRAINTHASHSIZE * 4)

#define MAX_QUOTED_NAME_LEN  (NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN  (MAX_QUOTED_NAME_LEN*2)

#define FPAttName(rel, attnum)	NameStr(*attnumAttName(rel, attnum))


/*
 * FP_QueryKey
 *
 * The key identifying a prepared SPI plan in our query hashtable
 */
typedef struct FP_QueryKey
{
	Oid			relation_id;	/* OID of Relation */
} FP_QueryKey;

/*
 * FP_QueryHashEntry
 */
typedef struct FP_QueryHashEntry
{
	FP_QueryKey key;
	SPIPlanPtr	plan;
} FP_QueryHashEntry;

/*
 * FP_CompareKey
 *
 * The key identifying an entry showing how to compare two values
 */
typedef struct FP_CompareKey
{
	Oid			eq_opr;			/* the equality operator to apply */
	Oid			typeid;			/* the data type to apply it to */
} FP_CompareKey;


/*
 * Local data
 */
static HTAB *fp_query_cache = NULL;


/*
 * Local function prototypes
 */
static void quoteOneName(char *buffer, const char *name);
static void quoteRelationName(char *buffer, Relation rel);
static void fp_BuildQueryKey(FP_QueryKey *key,
							 const Relation rel);

static void fp_InitHashTables(void);
static SPIPlanPtr fp_FetchPreparedPlan(FP_QueryKey *key);
static void fp_HashPreparedPlan(FP_QueryKey *key, SPIPlanPtr plan);

static SPIPlanPtr fp_PlanInserts(const char *querystr, int nargs, Oid *argtypes,
								 FP_QueryKey *qkey, Relation query_rel);
static bool fp_PerformInserts(FP_QueryKey *qkey, SPIPlanPtr qplan,
							  Relation query_rel,
							  TupleTableSlot *oldslot, Datum targetRange);
static void fp_ExtractValues(TupleTableSlot *slot,
							 Datum targetRange,
							 Datum *vals, char *nulls);


/*
 * FP_insert_leftovers -
 *
 * Insert leftovers from a temporal UPDATE/DELETE
 */
Datum
FP_insert_leftovers(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = (TriggerData *) fcinfo->context;
	Relation		rel = trigdata->tg_relation;
	FP_QueryKey		qkey;
	SPIPlanPtr		qplan;

	/* Check that this is a valid trigger call on the right time and event. */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager", "FP_insert_leftovers")));

	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) &&
		!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for UPDATE or DELETE", "FP_insert_leftovers")));

	/* Only do something if the statement has FOR PORTION OF */
	if (!trigdata->tg_temporal)
		return PointerGetDatum(NULL);

	if (!trigdata->tg_temporal->fp_targetRange)
		elog(ERROR, "No target range found for temporal query");

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Fetch or prepare a saved plan for the inserts */
	fp_BuildQueryKey(&qkey, rel);

	if ((qplan = fp_FetchPreparedPlan(&qkey)) == NULL)
	{
		RangeType  *targetRange = DatumGetRangeTypeP(trigdata->tg_temporal->fp_targetRange);
		char	   *rangeTypeName = get_typname(RangeTypeGetOid(targetRange));
		StringInfoData	querybuf;
		int		natts = rel->rd_att->natts;
		char	relname[MAX_QUOTED_REL_NAME_LEN];
		char	attname[MAX_QUOTED_NAME_LEN];
		Oid		queryoids[FP_MAX_ATTS];
		int		rangeAttNum = InvalidAttrNumber;
		int		periodStartAttNum = InvalidAttrNumber;
		int		periodEndAttNum = InvalidAttrNumber;
		bool	usingPeriod;

		/* ----------
		 * The query string built is
		 *  INSERT INTO <relname>
		 *  (rangeatt, otheratt1, ...)
		 *  SELECT x.r, $1, ... $n
		 *  FROM (VALUES
		 *   (rangetype(lower($x), upper($n+1)) - $n+1),
		 *   (rangetype(lower($n+1), upper($x)) - $n+1)
		 *  ) x (r)
		 *  WHERE x.r <> 'empty'
		 * The SELECT list "$1, ... $n" includes every attribute except the rangeatt.
		 * The "$x" is whichever attribute is the range column.
		 * The $n+1 param has the FOR PORTION OF target range.
		 * The $1...$n params are the values of the pre-UPDATE/DELETE tuple.
		 * If there is a PERIOD instead of a range,
		 * then instead of rangeatt we use startatt and endatt.
		 * ----------
		 */
		initStringInfo(&querybuf);

		usingPeriod = trigdata->tg_temporal->fp_periodStartName != NULL;
		quoteRelationName(relname, rel);
		appendStringInfo(&querybuf, "INSERT INTO %s (", relname);
		if (usingPeriod)
		{
			quoteOneName(attname, trigdata->tg_temporal->fp_periodStartName);
			appendStringInfo(&querybuf, "%s", attname);
			quoteOneName(attname, trigdata->tg_temporal->fp_periodEndName);
			appendStringInfo(&querybuf, ", %s", attname);
		}
		else
		{
			quoteOneName(attname, trigdata->tg_temporal->fp_rangeName);
			appendStringInfo(&querybuf, "%s", attname);
		}

		/* INSERT into every attribute but the range column */
		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(rel->rd_att, i);
			const char *colname = NameStr(attr->attname);
			if (!usingPeriod && strcmp(colname, trigdata->tg_temporal->fp_rangeName) == 0)
				rangeAttNum = i + 1;
			else if (usingPeriod && strcmp(colname, trigdata->tg_temporal->fp_periodStartName) == 0)
				periodStartAttNum = i + 1;
			else if (usingPeriod && strcmp(colname, trigdata->tg_temporal->fp_periodEndName) == 0)
				periodEndAttNum = i + 1;
			else
			{
				quoteOneName(attname, colname);
				appendStringInfo(&querybuf, ", %s", attname);
			}
			queryoids[i] = attr->atttypid;
		}
		queryoids[natts] = trigdata->tg_temporal->fp_rangeType;
		if (!usingPeriod && rangeAttNum == InvalidAttrNumber)
			elog(ERROR, "range column %s not found", trigdata->tg_temporal->fp_rangeName);
		else if (usingPeriod && periodStartAttNum == InvalidAttrNumber)
			elog(ERROR, "period start column %s not found", trigdata->tg_temporal->fp_periodStartName);
		else if (usingPeriod && periodEndAttNum == InvalidAttrNumber)
			elog(ERROR, "period end column %s not found", trigdata->tg_temporal->fp_periodEndName);

		if (!usingPeriod)
			appendStringInfo(&querybuf, ") SELECT x.r");
		else
			appendStringInfo(&querybuf, ") SELECT lower(x.r), upper(x.r)");

		/* SELECT all the attributes but the range/start/end columns */
		for (int i = 0; i < natts; i++)
			if (!((!usingPeriod && i == rangeAttNum - 1) ||
				  (usingPeriod && i == periodStartAttNum - 1) ||
				  (usingPeriod && i == periodEndAttNum - 1)))
				appendStringInfo(&querybuf, ", $%d", i + 1);

		appendStringInfo(&querybuf, " FROM (VALUES");
		// TODO: Why use `- $n+1` instead of setting the bound to the edge of $n+1 directly?
		// (where $n+1 is the range build from FOR PORTION OF)
		if (!usingPeriod)
		{
			appendStringInfo(&querybuf, " (%s(lower($%d), upper($%d)) - $%d),", rangeTypeName, rangeAttNum, natts+1, natts+1);
			appendStringInfo(&querybuf, " (%s(lower($%d), upper($%d)) - $%d)", rangeTypeName, natts+1, rangeAttNum, natts+1);
		}
		else
		{
			appendStringInfo(&querybuf, " (%s($%d, upper($%d)) - $%d),", rangeTypeName, periodStartAttNum, natts+1, natts+1);
			appendStringInfo(&querybuf, " (%s(lower($%d), $%d) - $%d)", rangeTypeName, natts+1, periodEndAttNum, natts+1);
		}
		appendStringInfo(&querybuf, ") x(r) WHERE x.r <> 'empty'");

		/* Prepare and save the plan */
		qplan = fp_PlanInserts(querybuf.data, natts + (usingPeriod ? 2 : 1), queryoids, &qkey, rel);
	}

	/*
	 * We have a plan now. Run it.
	 */
	fp_PerformInserts(&qkey, qplan,
					  rel,
					  trigdata->tg_trigslot,
					  trigdata->tg_temporal->fp_targetRange);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return PointerGetDatum(NULL);
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

/* ----------
 * fp_BuildQueryKey -
 *
 *	Construct a hashtable key for a prepared SPI plan of a temporal leftovers insert
 *
 *		key: output argument, *key is filled in based on the other arguments
 *		Relation: info from pg_constraint entry
 * ----------
 */
static void
fp_BuildQueryKey(FP_QueryKey *key, const Relation rel)
{
	/*
	 * We assume struct FP_QueryKey contains no padding bytes, else we'd need
	 * to use memset to clear them.
	 */
	key->relation_id = RelationGetRelid(rel);
}

/*
 * Prepare execution plan for a query to insert temporal leftovers
 */
static SPIPlanPtr
fp_PlanInserts(const char *querystr, int nargs, Oid *argtypes,
			   FP_QueryKey *qkey, Relation query_rel)
{
	SPIPlanPtr	qplan;
	Oid			save_userid;
	int			save_sec_context;

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Create the plan */
	qplan = SPI_prepare(querystr, nargs, argtypes);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), querystr);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Save the plan */
	SPI_keepplan(qplan);
	fp_HashPreparedPlan(qkey, qplan);

	return qplan;
}

/*
 * Perform a query to enforce a temporal PK restriction
 */
static bool
fp_PerformInserts(FP_QueryKey *qkey, SPIPlanPtr qplan,
				  Relation query_rel,
				  TupleTableSlot *oldslot, Datum targetRange)
{
	Snapshot	test_snapshot;
	Snapshot	crosscheck_snapshot;
	int			spi_result;
	Oid			save_userid;
	int			save_sec_context;
	Datum		vals[FP_MAX_ATTS];
	char		nulls[FP_MAX_ATTS];
	bool detectNewRows = true;	// TODO: need this?

	/* Extract the parameters to be passed into the query */
	fp_ExtractValues(oldslot, targetRange, vals, nulls);

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

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Finally we can run the query. */
	spi_result = SPI_execute_snapshot(qplan,
									  vals, nulls,
									  test_snapshot, crosscheck_snapshot,
									  false, true, 0);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Check result */
	if (spi_result < 0)
		elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	if (spi_result != SPI_OK_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("temporal leftovers query on \"%s\" gave unexpected result",
						RelationGetRelationName(query_rel)),
				 errhint("This is most likely due to a rule having rewritten the query.")));

	return SPI_processed != 0;
}

/*
 * Extract fields from a tuple into Datum/nulls arrays
 */
static void
fp_ExtractValues(TupleTableSlot *slot,
				 Datum targetRange,
				 Datum *vals, char *nulls)
{
	int		natts = slot->tts_tupleDescriptor->natts;
	bool	isnull;

	for (int i = 0; i < natts; i++)
	{
		vals[i] = slot_getattr(slot, i + 1, &isnull);
		nulls[i] = isnull ? 'n' : ' ';
	}
	vals[natts] = targetRange;
	nulls[natts] = false;
}

/*
 * fp_InitHashTables -
 *
 * Initialize our internal hash tables.
 */
static void
fp_InitHashTables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FP_QueryKey);
	ctl.entrysize = sizeof(FP_QueryHashEntry);
	fp_query_cache = hash_create("FP query cache",
								 FP_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);
}


/*
 * fp_FetchPreparedPlan -
 *
 * Lookup for a query key in our private hash table of prepared
 * and saved SPI execution plans. Return the plan if found or NULL.
 */
static SPIPlanPtr
fp_FetchPreparedPlan(FP_QueryKey *key)
{
	FP_QueryHashEntry *entry;
	SPIPlanPtr	plan;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!fp_query_cache)
		fp_InitHashTables();

	/*
	 * Lookup for the key
	 */
	entry = (FP_QueryHashEntry *) hash_search(fp_query_cache,
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
	 * locked the rel.
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
 * fp_HashPreparedPlan -
 *
 * Add another plan to our private SPI query plan hashtable.
 */
static void
fp_HashPreparedPlan(FP_QueryKey *key, SPIPlanPtr plan)
{
	FP_QueryHashEntry *entry;
	bool		found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!fp_query_cache)
		fp_InitHashTables();

	/*
	 * Add the new plan.  We might be overwriting an entry previously found
	 * invalid by fp_FetchPreparedPlan.
	 */
	entry = (FP_QueryHashEntry *) hash_search(fp_query_cache,
											  (void *) key,
											  HASH_ENTER, &found);
	Assert(!found || entry->plan == NULL);
	entry->plan = plan;
}
