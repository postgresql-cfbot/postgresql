/*-------------------------------------------------------------------------
 * conflict.c
 *	   Support routines for logging conflicts.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/conflict.c
 *
 * This file contains the code for logging conflicts on the subscriber during
 * logical replication and setting up conflict resolvers for a subscription.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/commit_ts.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_subscription_conflict_d.h"
#include "commands/defrem.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "replication/conflict.h"
#include "replication/worker_internal.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static const char *const ConflictResolverNames[] = {
	[CR_APPLY_REMOTE] = "apply_remote",
	[CR_KEEP_LOCAL] = "keep_local",
	[CR_APPLY_OR_SKIP] = "apply_or_skip",
	[CR_APPLY_OR_ERROR] = "apply_or_error",
	[CR_SKIP] = "skip",
	[CR_ERROR] = "error"
};

StaticAssertDecl(lengthof(ConflictResolverNames) == CONFLICT_NUM_RESOLVERS,
				 "array length mismatch");

/*
 * Valid conflict resolvers for each conflict type.
 *
 * First member represents default resolver for each conflict_type.
 * The same defaults are used in pg_dump.c. If any default is changed here,
 * ensure the corresponding value is updated in pg_dump's is_default_resolver
 * function.
 *
 * XXX: If we do not want to maintain different resolvers such as
 * apply_or_skip and apply_or_error for update_missing conflict,
 * then we can retain apply_remote and keep_local only. Then these
 * resolvers in context of update_missing will mean:
 *
 * keep_local: do not apply the update as INSERT.
 * apply_remote: apply the update as INSERT. If we could not apply,
 * then log and skip.
 *
 * Similarly SKIP can be replaced with KEEP_LOCAL for both update_missing
 * and delete_missing conflicts. For missing rows, 'SKIP' sounds more user
 * friendly name for a resolver and thus has been added here.
 */
typedef struct ConflictInfo {
		ConflictType conflict_type;
		const char *conflict_name;
		ConflictResolver default_resolver;
		bool allowed_resolvers[CONFLICT_NUM_RESOLVERS];
} ConflictInfo;

static ConflictInfo ConflictInfoMap[] = {
	[CT_INSERT_EXISTS] = {
		.conflict_type = CT_INSERT_EXISTS,
		.conflict_name = "insert_exists",
		.default_resolver = CR_ERROR,
		.allowed_resolvers = {[CR_ERROR]=true, [CR_APPLY_REMOTE]=true, [CR_KEEP_LOCAL]=true},
	},
	[CT_UPDATE_EXISTS] = {
		.conflict_type = CT_UPDATE_EXISTS,
		.conflict_name = "update_exists",
		.default_resolver = CR_ERROR,
		.allowed_resolvers = {[CR_ERROR]=true, [CR_APPLY_REMOTE]=true, [CR_KEEP_LOCAL]=true},
	},
	[CT_UPDATE_ORIGIN_DIFFERS] = {
		.conflict_type = CT_UPDATE_ORIGIN_DIFFERS,
		.conflict_name = "update_origin_differs",
		.default_resolver = CR_APPLY_REMOTE,
		.allowed_resolvers = {[CR_APPLY_REMOTE]=true, [CR_KEEP_LOCAL]=true, [CR_ERROR]=true},
	},
	[CT_UPDATE_MISSING] = {
		.conflict_type = CT_UPDATE_MISSING,
		.conflict_name = "update_missing",
		.default_resolver = CR_SKIP,
		.allowed_resolvers = {[CR_SKIP]=true, [CR_APPLY_OR_SKIP]=true, [CR_APPLY_OR_ERROR]=true, [CR_ERROR]=true},
	},
	[CT_DELETE_MISSING] = {
		.conflict_type = CT_DELETE_MISSING,
		.conflict_name = "delete_missing",
		.default_resolver = CR_SKIP,
		.allowed_resolvers = {[CR_SKIP]=true, [CR_ERROR]=true},
	},
	[CT_DELETE_ORIGIN_DIFFERS] = {
		.conflict_type = CT_DELETE_ORIGIN_DIFFERS,
		.conflict_name = "delete_origin_differs",
		.default_resolver = CR_APPLY_REMOTE,
		.allowed_resolvers = {[CR_APPLY_REMOTE]=true, [CR_KEEP_LOCAL]=true, [CR_ERROR]=true},
	},
};

StaticAssertDecl(lengthof(ConflictInfoMap) == CONFLICT_NUM_TYPES,
				 "array length mismatch");

static int	errcode_apply_conflict(ConflictType type);
static int	errdetail_apply_conflict(EState *estate,
									 ResultRelInfo *relinfo,
									 ConflictType type,
									 ConflictResolver resolver,
									 TupleTableSlot *searchslot,
									 TupleTableSlot *localslot,
									 TupleTableSlot *remoteslot,
									 Oid indexoid, TransactionId localxmin,
									 RepOriginId localorigin,
									 TimestampTz localts,
									 bool apply_remote);
static char *build_tuple_value_details(EState *estate, ResultRelInfo *relinfo,
									   ConflictType type,
									   TupleTableSlot *searchslot,
									   TupleTableSlot *localslot,
									   TupleTableSlot *remoteslot,
									   Oid indexoid);
static char *build_index_value_desc(EState *estate, Relation localrel,
									TupleTableSlot *slot, Oid indexoid);

/*
 * Get the xmin and commit timestamp data (origin and timestamp) associated
 * with the provided local tuple.
 *
 * Return true if the commit timestamp data was found, false otherwise.
 */
bool
GetTupleTransactionInfo(TupleTableSlot *localslot, TransactionId *xmin,
						RepOriginId *localorigin, TimestampTz *localts)
{
	Datum		xminDatum;
	bool		isnull;

	xminDatum = slot_getsysattr(localslot, MinTransactionIdAttributeNumber,
								&isnull);
	*xmin = DatumGetTransactionId(xminDatum);
	Assert(!isnull);

	/*
	 * The commit timestamp data is not available if track_commit_timestamp is
	 * disabled.
	 */
	if (!track_commit_timestamp)
	{
		*localorigin = InvalidRepOriginId;
		*localts = 0;
		return false;
	}

	return TransactionIdGetCommitTsData(*xmin, localts, localorigin);
}

/*
 * This function is used to report a conflict and resolution applied while
 * applying replication changes.
 *
 * 'searchslot' should contain the tuple used to search the local tuple to be
 * updated or deleted.
 *
 * 'localslot' should contain the existing local tuple, if any, that conflicts
 * with the remote tuple. 'localxmin', 'localorigin', and 'localts' provide the
 * transaction information related to this existing local tuple.
 *
 * 'remoteslot' should contain the remote new tuple, if any.
 *
 * The 'indexoid' represents the OID of the unique index that triggered the
 * constraint violation error. We use this to report the key values for
 * conflicting tuple.
 *
 * The caller must ensure that the index with the OID 'indexoid' is locked so
 * that we can fetch and display the conflicting key value.
 */
void
ReportApplyConflict(EState *estate, ResultRelInfo *relinfo,
					ConflictType type, ConflictResolver resolver,
					TupleTableSlot *searchslot, TupleTableSlot *localslot,
					TupleTableSlot *remoteslot, Oid indexoid,
					TransactionId localxmin, RepOriginId localorigin,
					TimestampTz localts, bool apply_remote)

{
	Relation	localrel = relinfo->ri_RelationDesc;
	int			elevel;

	if (resolver == CR_ERROR ||
		(resolver == CR_APPLY_OR_ERROR && !apply_remote))
		elevel = ERROR;
	else
		elevel = LOG;

	Assert(!OidIsValid(indexoid) ||
		   CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true));

	pgstat_report_subscription_conflict(MySubscription->oid, type);

	ereport(elevel,
			errcode_apply_conflict(type),
			errmsg("conflict detected on relation \"%s.%s\": conflict=%s, resolution=%s.",
				   get_namespace_name(RelationGetNamespace(localrel)),
				   RelationGetRelationName(localrel),
				   ConflictInfoMap[type].conflict_name,
				   ConflictResolverNames[resolver]),
			errdetail_apply_conflict(estate, relinfo, type, resolver, searchslot,
									 localslot, remoteslot, indexoid,
									 localxmin, localorigin, localts, apply_remote));
}

/*
 * Find all unique indexes to check for a conflict and store them into
 * ResultRelInfo.
 */
void
InitConflictIndexes(ResultRelInfo *relInfo)
{
	List	   *uniqueIndexes = NIL;

	for (int i = 0; i < relInfo->ri_NumIndices; i++)
	{
		Relation	indexRelation = relInfo->ri_IndexRelationDescs[i];

		if (indexRelation == NULL)
			continue;

		/* Detect conflict only for unique indexes */
		if (!relInfo->ri_IndexRelationInfo[i]->ii_Unique)
			continue;

		/* Don't support conflict detection for deferrable index */
		if (!indexRelation->rd_index->indimmediate)
			continue;

		uniqueIndexes = lappend_oid(uniqueIndexes,
									RelationGetRelid(indexRelation));
	}

	relInfo->ri_onConflictArbiterIndexes = uniqueIndexes;
}

/*
 * Add SQLSTATE error code to the current conflict report.
 */
static int
errcode_apply_conflict(ConflictType type)
{
	switch (type)
	{
		case CT_INSERT_EXISTS:
		case CT_UPDATE_EXISTS:
			return errcode(ERRCODE_UNIQUE_VIOLATION);
		case CT_UPDATE_ORIGIN_DIFFERS:
		case CT_UPDATE_MISSING:
		case CT_DELETE_ORIGIN_DIFFERS:
		case CT_DELETE_MISSING:
			return errcode(ERRCODE_T_R_SERIALIZATION_FAILURE);
	}

	Assert(false);
	return 0;					/* silence compiler warning */
}

/*
 * Add an errdetail() line showing conflict detail.
 *
 * The DETAIL line comprises of two parts:
 * 1. Explanation of the conflict type, including the origin and commit
 *    timestamp of the existing local tuple.
 * 2. Display of conflicting key, existing local tuple, remote new tuple, and
 *    replica identity columns, if any. The remote old tuple is excluded as its
 *    information is covered in the replica identity columns.
 */
static int
errdetail_apply_conflict(EState *estate, ResultRelInfo *relinfo,
						 ConflictType type, ConflictResolver resolver,
						 TupleTableSlot *searchslot, TupleTableSlot *localslot,
						 TupleTableSlot *remoteslot, Oid indexoid,
						 TransactionId localxmin, RepOriginId localorigin,
						 TimestampTz localts, bool apply_remote)
{
	StringInfoData err_detail;
	char	   *val_desc;
	char	   *origin_name;
	char	   *applymsg;
	char	   *updmsg;

	initStringInfo(&err_detail);

	if (apply_remote)
		applymsg = "applying the remote changes.";
	else
		applymsg = "ignoring the remote changes.";

	/* First, construct a detailed message describing the type of conflict */
	switch (type)
	{
		case CT_INSERT_EXISTS:
		case CT_UPDATE_EXISTS:
			Assert(OidIsValid(indexoid));

			if (localts)
			{
				if (localorigin == InvalidRepOriginId)
					appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified locally in transaction %u at %s, %s"),
									 get_rel_name(indexoid), localxmin,
									 timestamptz_to_str(localts), applymsg);
				else if (replorigin_by_oid(localorigin, true, &origin_name))
					appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified by origin \"%s\" in transaction %u at %s, %s"),
									 get_rel_name(indexoid), origin_name,
									 localxmin, timestamptz_to_str(localts),
									 applymsg);

				/*
				 * The origin that modified this row has been removed. This
				 * can happen if the origin was created by a different apply
				 * worker and its associated subscription and origin were
				 * dropped after updating the row, or if the origin was
				 * manually dropped by the user.
				 */
				else
					appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified by a non-existent origin in transaction %u at %s, %s"),
									 get_rel_name(indexoid), localxmin,
									 timestamptz_to_str(localts), applymsg);
			}
			else
				appendStringInfo(&err_detail, _("Key already exists in unique index \"%s\", modified in transaction %u, %s"),
								 get_rel_name(indexoid), localxmin, applymsg);

			break;

		case CT_UPDATE_ORIGIN_DIFFERS:
			if (localorigin == InvalidRepOriginId)
				appendStringInfo(&err_detail, _("Updating the row that was modified locally in transaction %u at %s, %s"),
								 localxmin, timestamptz_to_str(localts),
								 applymsg);
			else if (replorigin_by_oid(localorigin, true, &origin_name))
				appendStringInfo(&err_detail, _("Updating the row that was modified by a different origin \"%s\" in transaction %u at %s, %s"),
								 origin_name, localxmin,
								 timestamptz_to_str(localts), applymsg);

			/* The origin that modified this row has been removed. */
			else
				appendStringInfo(&err_detail, _("Updating the row that was modified by a non-existent origin in transaction %u at %s, %s"),
								 localxmin, timestamptz_to_str(localts),
								 applymsg);

			break;

		case CT_UPDATE_MISSING:
			updmsg = "Could not find the row to be updated";
			if (resolver == CR_APPLY_OR_SKIP && !apply_remote)
				appendStringInfo(&err_detail, _("%s, and the UPDATE cannot be converted to an INSERT, thus skipping the remote changes."),
								 updmsg);
			else if (resolver == CR_APPLY_OR_ERROR && !apply_remote)
				appendStringInfo(&err_detail, _("%s, and the UPDATE cannot be converted to an INSERT, thus raising the error."),
								 updmsg);
			else if (apply_remote)
				appendStringInfo(&err_detail, _("%s, thus converting the UPDATE to INSERT and %s"),
								 updmsg, applymsg);
			else
				appendStringInfo(&err_detail, _("%s, %s"),
								 updmsg, applymsg);
			break;

		case CT_DELETE_ORIGIN_DIFFERS:
			if (localorigin == InvalidRepOriginId)
				appendStringInfo(&err_detail, _("Deleting the row that was modified locally in transaction %u at %s, %s"),
								 localxmin, timestamptz_to_str(localts),
								 applymsg);
			else if (replorigin_by_oid(localorigin, true, &origin_name))
				appendStringInfo(&err_detail, _("Deleting the row that was modified by a different origin \"%s\" in transaction %u at %s, %s"),
								 origin_name, localxmin,
								 timestamptz_to_str(localts), applymsg);

			/* The origin that modified this row has been removed. */
			else
				appendStringInfo(&err_detail, _("Deleting the row that was modified by a non-existent origin in transaction %u at %s, %s"),
								 localxmin, timestamptz_to_str(localts),
								 applymsg);

			break;

		case CT_DELETE_MISSING:
			appendStringInfo(&err_detail, _("Could not find the row to be deleted."));
			break;
	}

	Assert(err_detail.len > 0);

	val_desc = build_tuple_value_details(estate, relinfo, type, searchslot,
										 localslot, remoteslot, indexoid);

	/*
	 * Next, append the key values, existing local tuple, remote tuple and
	 * replica identity columns after the message.
	 */
	if (val_desc)
		appendStringInfo(&err_detail, "\n%s", val_desc);

	return errdetail_internal("%s", err_detail.data);
}

/*
 * Helper function to build the additional details for conflicting key,
 * existing local tuple, remote tuple, and replica identity columns.
 *
 * If the return value is NULL, it indicates that the current user lacks
 * permissions to view the columns involved.
 */
static char *
build_tuple_value_details(EState *estate, ResultRelInfo *relinfo,
						  ConflictType type,
						  TupleTableSlot *searchslot,
						  TupleTableSlot *localslot,
						  TupleTableSlot *remoteslot,
						  Oid indexoid)
{
	Relation	localrel = relinfo->ri_RelationDesc;
	Oid			relid = RelationGetRelid(localrel);
	TupleDesc	tupdesc = RelationGetDescr(localrel);
	StringInfoData tuple_value;
	char	   *desc = NULL;

	Assert(searchslot || localslot || remoteslot);

	initStringInfo(&tuple_value);

	/*
	 * Report the conflicting key values in the case of a unique constraint
	 * violation.
	 */
	if (type == CT_INSERT_EXISTS || type == CT_UPDATE_EXISTS)
	{
		Assert(OidIsValid(indexoid) && localslot);

		desc = build_index_value_desc(estate, localrel, localslot, indexoid);

		if (desc)
			appendStringInfo(&tuple_value, _("Key %s"), desc);
	}

	if (localslot)
	{
		/*
		 * The 'modifiedCols' only applies to the new tuple, hence we pass
		 * NULL for the existing local tuple.
		 */
		desc = ExecBuildSlotValueDescription(relid, localslot, tupdesc,
											 NULL, 64);

		if (desc)
		{
			if (tuple_value.len > 0)
			{
				appendStringInfoString(&tuple_value, "; ");
				appendStringInfo(&tuple_value, _("existing local tuple %s"),
								 desc);
			}
			else
			{
				appendStringInfo(&tuple_value, _("Existing local tuple %s"),
								 desc);
			}
		}
	}

	if (remoteslot)
	{
		Bitmapset  *modifiedCols;

		/*
		 * Although logical replication doesn't maintain the bitmap for the
		 * columns being inserted, we still use it to create 'modifiedCols'
		 * for consistency with other calls to ExecBuildSlotValueDescription.
		 *
		 * Note that generated columns are formed locally on the subscriber.
		 */
		modifiedCols = bms_union(ExecGetInsertedCols(relinfo, estate),
								 ExecGetUpdatedCols(relinfo, estate));
		desc = ExecBuildSlotValueDescription(relid, remoteslot, tupdesc,
											 modifiedCols, 64);

		if (desc)
		{
			if (tuple_value.len > 0)
			{
				appendStringInfoString(&tuple_value, "; ");
				appendStringInfo(&tuple_value, _("remote tuple %s"), desc);
			}
			else
			{
				appendStringInfo(&tuple_value, _("Remote tuple %s"), desc);
			}
		}
	}

	if (searchslot)
	{
		/*
		 * Note that while index other than replica identity may be used (see
		 * IsIndexUsableForReplicaIdentityFull for details) to find the tuple
		 * when applying update or delete, such an index scan may not result
		 * in a unique tuple and we still compare the complete tuple in such
		 * cases, thus such indexes are not used here.
		 */
		Oid			replica_index = GetRelationIdentityOrPK(localrel);

		Assert(type != CT_INSERT_EXISTS);

		/*
		 * If the table has a valid replica identity index, build the index
		 * key value string. Otherwise, construct the full tuple value for
		 * REPLICA IDENTITY FULL cases.
		 */
		if (OidIsValid(replica_index))
			desc = build_index_value_desc(estate, localrel, searchslot, replica_index);
		else
			desc = ExecBuildSlotValueDescription(relid, searchslot, tupdesc, NULL, 64);

		if (desc)
		{
			if (tuple_value.len > 0)
			{
				appendStringInfoString(&tuple_value, "; ");
				appendStringInfo(&tuple_value, OidIsValid(replica_index)
								 ? _("replica identity %s")
								 : _("replica identity full %s"), desc);
			}
			else
			{
				appendStringInfo(&tuple_value, OidIsValid(replica_index)
								 ? _("Replica identity %s")
								 : _("Replica identity full %s"), desc);
			}
		}
	}

	if (tuple_value.len == 0)
		return NULL;

	appendStringInfoChar(&tuple_value, '.');
	return tuple_value.data;
}

/*
 * Helper functions to construct a string describing the contents of an index
 * entry. See BuildIndexValueDescription for details.
 *
 * The caller must ensure that the index with the OID 'indexoid' is locked so
 * that we can fetch and display the conflicting key value.
 */
static char *
build_index_value_desc(EState *estate, Relation localrel, TupleTableSlot *slot,
					   Oid indexoid)
{
	char	   *index_value;
	Relation	indexDesc;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	TupleTableSlot *tableslot = slot;

	if (!tableslot)
		return NULL;

	Assert(CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true));

	indexDesc = index_open(indexoid, NoLock);

	/*
	 * If the slot is a virtual slot, copy it into a heap tuple slot as
	 * FormIndexDatum only works with heap tuple slots.
	 */
	if (TTS_IS_VIRTUAL(slot))
	{
		tableslot = table_slot_create(localrel, &estate->es_tupleTable);
		tableslot = ExecCopySlot(tableslot, slot);
	}

	/*
	 * Initialize ecxt_scantuple for potential use in FormIndexDatum when
	 * index expressions are present.
	 */
	GetPerTupleExprContext(estate)->ecxt_scantuple = tableslot;

	/*
	 * The values/nulls arrays passed to BuildIndexValueDescription should be
	 * the results of FormIndexDatum, which are the "raw" input to the index
	 * AM.
	 */
	FormIndexDatum(BuildIndexInfo(indexDesc), tableslot, estate, values, isnull);

	index_value = BuildIndexValueDescription(indexDesc, values, isnull);

	index_close(indexDesc, NoLock);

	return index_value;
}

/*
 * Report a warning about incomplete conflict detection and resolution if
 * track_commit_timestamp is disabled.
 */
static void
conf_detection_check_prerequisites(void)
{
	if (!track_commit_timestamp)
		ereport(WARNING,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("conflict detection and resolution could be incomplete due to disabled track_commit_timestamp"),
				errdetail("Conflict types 'update_origin_differs' and 'delete_origin_differs'"
						  "cannot be detected unless 'track_commit_timestamp' is enabled"));
}

/*
 * Validate the conflict type and return the corresponding ConflictType enum.
 */
ConflictType
ValidateConflictType(const char *conflict_type)
{
	ConflictType type;
	bool		valid = false;

	/* Check conflict type validity */
	for (type = 0; type < CONFLICT_NUM_TYPES; type++)
	{
		if (pg_strcasecmp(ConflictInfoMap[type].conflict_name, conflict_type) == 0)
		{
			valid = true;
			break;
		}
	}

	if (!valid)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("%s is not a valid conflict type", conflict_type));

	return type;
}

/*
 * Validate the conflict type and resolver. It returns an enum ConflictType
 * corresponding to the conflict type string passed by the caller.
 */
ConflictType
ValidateConflictTypeAndResolver(const char *conflict_type,
								const char *conflict_resolver)
{
	ConflictType type;
	ConflictResolver resolver;
	bool		valid = false;

	/* Validate conflict type */
	type = ValidateConflictType(conflict_type);

	/* Validate the conflict resolver name */
	for (resolver = 0; resolver < CONFLICT_NUM_RESOLVERS; resolver++)
	{
		if (pg_strcasecmp(ConflictResolverNames[resolver], conflict_resolver) == 0)
		{
			valid = true;
			break;
		}
	}

	if (!valid)
	{
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("%s is not a valid conflict resolver", conflict_resolver));
	}

	valid = ConflictInfoMap[type].allowed_resolvers[resolver];

	if (!valid)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("%s is not a valid conflict resolver for conflict type %s",
					   conflict_resolver,
					   conflict_type));

	return type;
}

/*
 * Get the conflict resolver configured at subscription level for
 * for the given conflict type.
 */
static ConflictResolver
get_conflict_resolver_internal(ConflictType type, Oid subid)
{

	ConflictResolver resolver;
	HeapTuple	tuple;
	Datum		datum;
	char	   *conflict_res;

	/*
	 * XXX: Currently, we fetch the conflict resolver from cache for each
	 * conflict detection. If needed, we can keep the info in global variable
	 * and fetch from cache only once after cache invalidation.
	 */
	tuple = SearchSysCache2(SUBSCRIPTIONCONFLMAP,
							ObjectIdGetDatum(subid),
							CStringGetTextDatum(ConflictInfoMap[type].conflict_name));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for conflict type %u", type);

	datum = SysCacheGetAttrNotNull(SUBSCRIPTIONCONFLMAP,
								   tuple, Anum_pg_subscription_conflict_confres);

	conflict_res = TextDatumGetCString(datum);

	for (resolver = 0; resolver < CONFLICT_NUM_RESOLVERS; resolver++)
	{
		if (pg_strcasecmp(ConflictResolverNames[resolver], conflict_res) == 0)
			break;
	}

	ReleaseSysCache(tuple);
	return resolver;
}

/*
 * Check if a full tuple can be created from the new tuple.
 * Return true if yes, false otherwise.
 */
static bool
can_create_full_tuple(Relation localrel,
					  LogicalRepTupleData *newtup)
{
	int			i;
	int			local_att = RelationGetNumberOfAttributes(localrel);

	if (newtup->ncols != local_att)
		return false;

	/*
	 * A full tuple cannot be created if any column contains a toast value.
	 * Columns with toast values are marked as LOGICALREP_COLUMN_UNCHANGED.
	 */
	for (i = 0; i < newtup->ncols; i++)
	{
		if (newtup->colstatus[i] == LOGICALREP_COLUMN_UNCHANGED)
			return false;
	}

	return true;
}

/*
 * Common 'CONFLICT RESOLVER' parsing function for CREATE and ALTER
 * SUBSCRIPTION commands.
 *
 * It reports an error if duplicate options are specified.
 *
 * Returns a list of conflict types along with their corresponding conflict
 * resolvers. If 'add_defaults' is true, it appends default resolvers for any
 * conflict types that have not been explicitly defined by the user.
 */
List *
ParseAndGetSubConflictResolvers(ParseState *pstate,
								List *stmtresolvers, bool add_defaults)
{
	List		   *res = NIL;
	bool			already_seen[CONFLICT_NUM_TYPES] = {0};
	ConflictType 	type;

	/* Loop through the user provided resolvers */
	foreach_ptr(DefElem, defel, stmtresolvers)
	{
		char	   *resolver;
		ConflictTypeResolver *conftyperesolver = NULL;

		/* Validate the conflict type and resolver */
		resolver = defGetString(defel);
		resolver = downcase_truncate_identifier(
												resolver, strlen(resolver), false);
		type = ValidateConflictTypeAndResolver(defel->defname,
											   resolver);

		/* Check if the conflict type has already been seen */
		if (already_seen[type])
			errorConflictingDefElem(defel, pstate);

		already_seen[type] = true;

		conftyperesolver = palloc(sizeof(ConflictTypeResolver));
		conftyperesolver->conflict_type_name = defel->defname;
		conftyperesolver->conflict_resolver_name = resolver;
		res = lappend(res, conftyperesolver);
	}

	/* Once validation is complete, warn users if prerequisites are not met */
	if (stmtresolvers)
		conf_detection_check_prerequisites();

	/*
	 * If add_defaults is true, fill remaining conflict types with default
	 * resolvers.
	 */
	if (add_defaults)
	{
		for (int i = 0; i < CONFLICT_NUM_TYPES; i++)
		{
			ConflictTypeResolver *conftyperesolver = NULL;

			if (!already_seen[i])
			{
				ConflictResolver def_resolver = ConflictInfoMap[i].default_resolver;

				conftyperesolver = palloc(sizeof(ConflictTypeResolver));
				conftyperesolver->conflict_type_name = ConflictInfoMap[i].conflict_name;
				conftyperesolver->conflict_resolver_name = ConflictResolverNames[def_resolver];;
				res = lappend(res, conftyperesolver);
			}
		}
	}

	return res;
}

/*
 * Get the list of conflict types and their corresponding default resolvers.
 */
List *
GetDefaultConflictResolvers()
{
	List	   *res = NIL;

	for (ConflictType type = 0; type < CONFLICT_NUM_TYPES; type++)
	{
		ConflictTypeResolver *resolver = NULL;
		ConflictResolver def_resolver = ConflictInfoMap[type].default_resolver;

		/* Allocate memory for each ConflictTypeResolver */
		resolver = palloc(sizeof(ConflictTypeResolver));

		resolver->conflict_type_name = ConflictInfoMap[type].conflict_name;
		resolver->conflict_resolver_name = ConflictResolverNames[def_resolver];

		/* Append to the response list */
		res = lappend(res, resolver);
	}

	return res;
}

/*
 * Update the Subscription's conflict resolvers in pg_subscription_conflict
 * system catalog for the given conflict types.
 */
void
UpdateSubConflictResolvers(List *conflict_resolvers, Oid subid)
{
	Datum		values[Natts_pg_subscription_conflict];
	bool		nulls[Natts_pg_subscription_conflict];
	bool		replaces[Natts_pg_subscription_conflict];
	HeapTuple	oldtup;
	HeapTuple	newtup = NULL;
	Relation	pg_subscription_conflict;
	char	   *cur_conflict_res;
	Datum		datum;

	/* Prepare to update a tuple */
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));
	memset(values, 0, sizeof(values));

	pg_subscription_conflict = table_open(SubscriptionConflictId, RowExclusiveLock);

	foreach_ptr(ConflictTypeResolver, conftyperesolver, conflict_resolvers)
	{
		/* Set up subid and conflict_type to search in cache */
		values[Anum_pg_subscription_conflict_confsubid - 1] = ObjectIdGetDatum(subid);
		values[Anum_pg_subscription_conflict_conftype - 1] =
			CStringGetTextDatum(conftyperesolver->conflict_type_name);

		oldtup = SearchSysCache2(SUBSCRIPTIONCONFLMAP,
								 values[Anum_pg_subscription_conflict_confsubid - 1],
								 values[Anum_pg_subscription_conflict_conftype - 1]);

		if (!HeapTupleIsValid(oldtup))
			elog(ERROR, "cache lookup failed for table conflict %s for subid %u",
				 conftyperesolver->conflict_type_name, subid);

		datum = SysCacheGetAttrNotNull(SUBSCRIPTIONCONFLMAP,
									   oldtup, Anum_pg_subscription_conflict_confres);
		cur_conflict_res = TextDatumGetCString(datum);

		/*
		 * Update system catalog only if the new resolver is not same as the
		 * existing one.
		 */
		if (pg_strcasecmp(cur_conflict_res,
						  conftyperesolver->conflict_resolver_name) != 0)
		{
			/* Update the new resolver */
			values[Anum_pg_subscription_conflict_confres - 1] =
				CStringGetTextDatum(conftyperesolver->conflict_resolver_name);
			replaces[Anum_pg_subscription_conflict_confres - 1] = true;

			newtup = heap_modify_tuple(oldtup,
									   RelationGetDescr(pg_subscription_conflict),
									   values, nulls, replaces);
			CatalogTupleUpdate(pg_subscription_conflict,
							   &oldtup->t_self, newtup);
			heap_freetuple(newtup);
		}

		ReleaseSysCache(oldtup);
	}

	table_close(pg_subscription_conflict, RowExclusiveLock);
}

/*
 * Reset the conflict resolver for this conflict type to its default setting.
 */
void
ResetSubConflictResolver(Oid subid, char *conflict_type)
{
	ConflictType idx;
	ConflictTypeResolver *conflictResolver = NULL;
	List	   *conflictresolver_list = NIL;

	/* Validate the conflict type and get the index */
	idx = ValidateConflictType(conflict_type);
	conflictResolver = palloc(sizeof(ConflictTypeResolver));
	conflictResolver->conflict_type_name = conflict_type;

	/* Get the default resolver for this conflict_type */
	conflictResolver->conflict_resolver_name =
		ConflictResolverNames[ConflictInfoMap[idx].default_resolver];

	/* Create a list of conflict resolvers and update in catalog */
	conflictresolver_list = lappend(conflictresolver_list, conflictResolver);
	UpdateSubConflictResolvers(conflictresolver_list, subid);

}

/*
 * Set Conflict Resolvers on the subscription
 */
void
SetSubConflictResolvers(Oid subId, List *conflict_resolvers)
{
	Relation	pg_subscription_conflict;
	Datum		values[Natts_pg_subscription_conflict];
	bool		nulls[Natts_pg_subscription_conflict];
	HeapTuple	newtup = NULL;
	Oid			conflict_oid;

	pg_subscription_conflict = table_open(SubscriptionConflictId, RowExclusiveLock);

	/* Prepare to update a tuple */
	memset(nulls, false, sizeof(nulls));

	/* Iterate over the list of resolvers */
	foreach_ptr(ConflictTypeResolver, conftyperesolver, conflict_resolvers)
	{
		values[Anum_pg_subscription_conflict_confsubid - 1] =
			ObjectIdGetDatum(subId);
		values[Anum_pg_subscription_conflict_conftype - 1] =
			CStringGetTextDatum(conftyperesolver->conflict_type_name);
		values[Anum_pg_subscription_conflict_confres - 1] =
			CStringGetTextDatum(conftyperesolver->conflict_resolver_name);

		/* Get a new oid and update the tuple into catalog */
		conflict_oid = GetNewOidWithIndex(pg_subscription_conflict,
										  SubscriptionConflictOidIndexId,
										  Anum_pg_subscription_conflict_oid);
		values[Anum_pg_subscription_conflict_oid - 1] =
			ObjectIdGetDatum(conflict_oid);
		newtup = heap_form_tuple(RelationGetDescr(pg_subscription_conflict),
								 values, nulls);
		CatalogTupleInsert(pg_subscription_conflict, newtup);
		heap_freetuple(newtup);
	}

	table_close(pg_subscription_conflict, RowExclusiveLock);
}

/*
 * Remove the subscription conflict resolvers for the subscription id
 */
void
RemoveSubConflictResolvers(Oid subid)
{
	Relation	rel;
	HeapTuple	tup;
	TableScanDesc scan;
	ScanKeyData skey[1];

	rel = table_open(SubscriptionConflictId, RowExclusiveLock);

	/*
	 * Search using the subid to return all conflict resolvers for this
	 * subscription.
	 */
	ScanKeyInit(&skey[0],
				Anum_pg_subscription_conflict_confsubid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = table_beginscan_catalog(rel, 1, skey);

	/* Iterate through the tuples and delete them */
	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
		CatalogTupleDelete(rel, &tup->t_self);

	table_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Find the resolver for the given conflict type and subscription.
 *
 * Set 'apply_remote' to true if remote tuple should be applied,
 * false otherwise.
 */
ConflictResolver
GetConflictResolver(Oid subid, ConflictType type, Relation localrel,
					LogicalRepTupleData *newtup, bool *apply_remote)
{
	ConflictResolver resolver;

	resolver = get_conflict_resolver_internal(type, subid);

	switch (resolver)
	{
		case CR_APPLY_REMOTE:
			*apply_remote = true;
			break;
		case CR_APPLY_OR_SKIP:
			if (can_create_full_tuple(localrel, newtup))
				*apply_remote = true;
			else
				*apply_remote = false;
			break;
		case CR_APPLY_OR_ERROR:
			if (can_create_full_tuple(localrel, newtup))
				*apply_remote = true;
			else
				*apply_remote = false;
			break;
		case CR_KEEP_LOCAL:
		case CR_SKIP:
		case CR_ERROR:
			*apply_remote = false;
			break;
		default:
			elog(ERROR, "Conflict %s is detected! Unrecogonized conflict resolution method",
				 ConflictInfoMap[type].conflict_name);
	}

	return resolver;
}
