/*-------------------------------------------------------------------------
 *
 * sessionvariable.c
 *	  session variable creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/sessionvariable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "catalog/pg_variable.h"
#include "commands/session_variable.h"
#include "executor/executor.h"
#include "executor/svariableReceiver.h"
#include "funcapi.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "storage/itemptr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * Values of session variables are stored in local memory, in
 * sessionvars hash table in binary format and the life cycle of
 * session variable can be longer than transaction. Then we
 * need to solve following issues:
 *
 * - We need to purge memory when variable is dropped (in current
 *  transaction in current session or by other sessions (other
 *  users). There is a request to protect content against
 *  possibly aborted the DROP VARIABLE command. So the purging should
 *  not be executed immediately. It should be postponed until the end
 *  of the transaction.
 *
 * - The session variable can be dropped explicitly (by DROP VARIABLE)
 *  command or implicitly (by using ON COMMIT DROP clause). The
 *  purging memory at transaction end can be requested implicitly
 *  too (by using the ON TRANSACTION END clause).
 *
 * - We need to ensure that the returned value is valid. The stored
 *  value can be invalid from different reasons:
 *
 *  a) format change - we doesn't support change of format for
 *   session variable, but format can be changed by ALTER TYPE
 *   operation or by owner's extension update. We store fingerprint
 *   of any type used by session variables, and before any
 *   read we check if this fingerprint is still valid (see
 *   SVariableType related operations). Built-in non composite
 *   types are trusty every time.
 *
 *   For other cases we can check xmin, and if xmin is different,
 *   then we check the name. For types from extension we
 *   can check xmin of pg_extension related record (and if it
 *   is different we can check extension's name and extension's
 *   version). For domains we should to force cast to domain type
 *   to reply all constraints before returning the value.
 *
 *  b) the variable can be dropped - we don't purge memory
 *   when the variable is dropped immediately, so theoretically
 *   we can return dropped value. The protection against this
 *   issue is based on invisibility of dropped session variables
 *   in system catalogue. The DROP commands invalidates related
 *   cached plans too, so it should work with prepared statements.
 *   Theoretically there is possible access to value of dropped
 *   variable by using known oid, but we don't try to solve this
 *   issue now (it is possible only when session variables API
 *   is used from extensions). The implementation watches sinval
 *   messages, and in message processing check the consistency
 *   of stored values in hash table against system catalogue.
 *   This check should be done after end of any transaction, but
 *   because for this check we need not aborted state, the check
 *   can be postponed to next transaction (when the variable
 *   will be used). Unfortunately, this check can be postponed
 *   to the future and between there can be oid overflow. We
 *   cannot to detect oid overflow, and then theoretically there is
 *   the possibility to find in catalogue a variable with same
 *   oid like oid of dropped value. But as protection against
 *   returning invalid value we can (and we do) check the used
 *   type, and when the type is not valid, then we don't allow
 *   returns any value. We cannot to check name or schema, because
 *   we suppport ALTER RENAME and ALTER SET SCHEMA. So, although
 *   there is an risk to return value of dropped variable (instead
 *   of new calculated default expr or NULL), there should not
 *   be risk of crash due to invalid format.
 *
 * The values of session variables are stored in hash table
 * sessionvars, the fingerprints of used types are stored in
 * hash table sessionvars_types. We maintain four queues
 * (implemented as list of variable oid, list of RecheckVariableRequests
 * (requests are created in reaction on sinval) and lists
 * of actions). List of actions are used whe we need to calculate
 * the final transaction state of an entry's creator's
 * subtransaction. List of session variables are used, when we
 * don't need to do this calculation (we know, so. every values
 * of session variables created with clauses ON COMMIT DROP or
 * ON TRANSACTION END RESET will be purged from memory), but
 * ON COMMIT DROP action or ON COMMIT RESET action can be done only
 * when related sub transaction is committed (we don't want to lost
 * value after aborted DROP VARIABLE command, we don't want to delete
 * record from system catalogue after aborted CREATE VARIABLE command,
 * or we don't want to drop record from system catalog implicitly
 * when TEMP ON COMMIT DROP variable was successfully dropped).
 * Recheck of session's variable existence should be executed
 * only when some operation over session's variable is executed
 * first time in transaction or at the end of transaction. It
 * should not be executed more times inside transaction.
 *
 * Although the variable's reset doesn't need full delete from
 * sessionvars hash table, we do. It is the most simple solution,
 * and it reduces possible state's space (and reduces sessionvars
 * bloating).
 *
 * There are two different ways to do final access to session
 * variables: buffered (indirect) or direct. Buffered access is used
 * in queries, where we have to ensure an stability of passed values
 * (then the session variable has same behaviour like external query
 * parameters, what is consistent with using PL/pgSQL's variables in
 * embedded queries in PL/pgSQL).
 *
 * This is implemented by using an aux buffer (an array) that holds a
 * copy of values of used (in query) session variables. In the final
 * end, the values from this array are passed as constant (EEOP_CONST).
 *
 * Direct access is used by simple expression evaluation (PLpgSQL).
 * In this case we don't need to ensure the stability of passed
 * values, and maintaining the buffer with copies of values of session
 * variables can be useless overhead. In this case we just read the
 * value of the session variable directly (EEOP_PARAM_VARIABLE). This
 * strategy removes the necessity to modify related PL/pgSQL code to
 * support session variables (the reading of session variables is
 * fully transparent for PL/pgSQL).
 */
struct SVariableTypeDataField;

typedef struct SVaribleTypeData
{
	Oid			typid;
	int64		gennum;			/* generation number helps with change detection */
	LocalTransactionId verified_lxid; /* lxid of transaction when the typ fingerprint
									   * was created or verified */
	int16		typlen;
	bool		typbyval;
	bool		is_domain;
	bool		is_rowtype;
	char	   *typname;

	struct SVaribleTypeData *base_type;
	int64		base_type_gennum;
	void	   *domain_check_extra;
	LocalTransactionId domain_check_extra_lxid;

	TransactionId	typ_xmin;	/* xmin of related tuple of pg_type */
	ItemPointerData typ_tid;

	Oid			extid;			/* OID of owner extension if exists */
	TransactionId ext_xmin;		/* xmin of related tuple of pg_extension */
	ItemPointerData ext_tid;
	char	   *extname;
	char	   *extversion;

	int			natts;			/* number of attributies of composite type */
	struct SvariableTypeDataField *attrs;		/* array of attributies */
} SVariableTypeData;

typedef SVariableTypeData * SVariableType;

typedef struct SvariableTypeDataField
{
	SVariableType svartype;
	int64		gennum;
} SvariableTypeDataField;

typedef enum SVariableXActAction
{
	SVAR_ON_COMMIT_DROP,		/* used for ON COMMIT DROP */
	SVAR_ON_COMMIT_RESET,		/* used for DROP VARIABLE */
} SVariableXActAction;

typedef struct SVariableXActActionItem
{
	Oid			varid;			/* varid of session variable */

	/*
	 * creating_subid is the ID of the creating subxact. If the action was
	 * unregistered during the current transaction, deleting_subid is the ID of
	 * the deleting subxact, otherwise InvalidSubTransactionId.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
}  SVariableXActActionItem;

/* Both lists hold fields of SVariableXActActionItem type */
static List *xact_on_commit_drop_actions = NIL;
static List *xact_on_commit_reset_actions = NIL;

/*
 * the ON COMMIT DROP and ON TRANSACTION END RESET variables
 * are purged from memory every time.
 */
static List *xact_reset_varids = NIL;

/*
 * When the session variable is dropped we need to purge memory. The
 * session variable can be dropped by current session, but it can be
 * dropped by other's sessions too, so we have to watc sinval message.
 * But because we don't want to purge memory immediately, we need to
 * hold list of possibly dropped session variables and at the end of
 * transaction, we check session variables from this list against system
 * catalogue. This check can be postponed into next transaction if
 * current transactions is in aborted state. When the check is postponed
 * to the next transaction, then have to be executed just once time, and
 * the fields of list created in current transaction have to be ignored
 * (the sinval message can come first before sync_sessionvars_all call,
 * and then we have to skip items created by current transaction, because
 * we want to variables when the related operation (that emmits sinval)
 * is in final state (at the end of related transaction).
 * Saved lxid in synced_lxid is safeguard against repeated useles
 * recheck inside one transaction.
 */
typedef struct RecheckVariableRequest
{
	Oid			varid;
	LocalTransactionId lxid;
} RecheckVariableRequest;

static List *xact_recheck_requests = NIL;

static LocalTransactionId synced_lxid = InvalidLocalTransactionId;

typedef struct SVariableData
{
	Oid			varid;			/* pg_variable OID of this sequence (hash key) */

	char	   *name;		/* for debug purposes */
	char	   *nsname;		/* for debug purposes */

	SVariableType svartype;
	int64		gennum;

	bool		isnull;
	bool		freeval;
	Datum		value;

	bool		is_not_null;	/* don't allow null values */
	bool		is_immutable;	/* true when variable is immutable */
	bool		has_defexpr;	/* true when variable has a default value */

	bool		is_valid;		/* true when variable was successfully
								 * initialized */

	uint32		hashvalue;

	bool		eox_reset;		/* true, when lifecycle is limitted by transaction */
}			SVariableData;

typedef SVariableData * SVariable;

static HTAB *sessionvars = NULL;		/* hash table for session variables */
static HTAB *sessionvars_types = NULL;	/* hash table for type fingerprints of session
										 * variables */

static MemoryContext SVariableMemoryContext = NULL;

static bool first_time = true;


static SVariableType get_svariabletype(Oid typid);
static int64 get_svariable_valid_type_gennum(SVariableType svt);

static void register_session_variable_xact_action(Oid varid, SVariableXActAction action);
static void unregister_session_variable_xact_action(Oid varid, SVariableXActAction action);

/*
 * Returns human readable name of SVariableXActAction value.
 */
static const char *
SvariableXActActionName(SVariableXActAction action)
{
	switch (action)
	{
		case SVAR_ON_COMMIT_DROP:
			return "ON COMMIT DROP";
		case SVAR_ON_COMMIT_RESET:
			return "ON COMMIT RESET";
		default:
			elog(ERROR, "unknown SVariableXActAction action %d",
						action);
	}
}

/*
 * In this case we know, so fast comparing fails.
 */
static bool
svariabletypes_equals(SVariableType svt1, SVariableType svt2)
{
	Assert(svt1->typid == svt2->typid);
	Assert(svt1 != svt2);

	/*
	 * for trustworthy check we need to know base type, extension,
	 * or composite fields. Just typlen, and typbyval is not enough
	 * trustworthy, because (-1, false) is very common. In this case
	 * we can check only the name (for other cases we don't check the
	 * name, because the name can be altered).
	 */
	if (!(OidIsValid(svt1->extid) ||
		  svt1->base_type ||
		  svt1->is_rowtype))
	{
		if (strcmp(svt1->typname, svt2->typname) != 0)
			return false;
	}

	if (svt1->typlen != svt2->typlen)
		return false;

	if (svt1->typbyval != svt2->typbyval)
		return false;

	if (svt1->is_domain != svt2->is_domain)
		return false;

	if (svt1->is_rowtype != svt2->is_rowtype)
		return false;

	if (svt1->is_domain)
	{
		if (svt1->base_type->typid != svt2->base_type->typid)
			return false;

		if (svt1->base_type_gennum != svt2->base_type_gennum)
			return false;
	}

	if (OidIsValid(svt1->extid))
	{
		if (svt1->extid != svt2->extid)
			return false;

		if (strcmp(svt1->extname, svt2->extname) != 0)
			return false;

		if (strcmp(svt1->extversion, svt2->extversion) != 0)
			return false;
	}

	if (svt1->natts > 0 || svt2->natts > 0)
	{
		int		i;

		if (svt1->natts != svt2->natts)
			return false;

		for (i = 0; i < svt1->natts; i++)
		{
			if (svt1->attrs[i].svartype->typid != svt2->attrs[i].svartype->typid)
				return false;

			if (svt1->attrs[i].gennum != svt2->attrs[i].gennum)
				return false;
		}
	}

	return true;
}

static void
svariabletype_free(SVariableType svt)
{
	pfree(svt->typname);

	if (OidIsValid(svt->extid))
	{
		pfree(svt->extname);
		pfree(svt->extversion);
	}

	if (svt->natts > 0)
		pfree(svt->attrs);
}

/*
 * Update fields used for fast check
 */
static void
svariabletype_refresh(SVariableType svt1, SVariableType svt2)
{
	svt1->typ_xmin = svt2->typ_xmin;
	svt1->typ_tid = svt2->typ_tid;

	svt1->ext_xmin = svt2->ext_xmin;
	svt1->ext_tid = svt2->ext_tid;
}

/*
 * Update all fields and increase generation number
 */
static void
svariabletype_update(SVariableType svt1, SVariableType svt2)
{
	int		gennum = svt1->gennum;

	svariabletype_free(svt1);

	memcpy(svt1, svt2, sizeof(SVariableTypeData));

	svt1->gennum = gennum + 1;
}

/*
 * When type owner's extension is not changed, then we can belive
 * so type is still valid. For this check we need to hold few
 * information about extension in memory. We can do fast check
 * based on xmin, tid or slow check on oid, name and version.
 */
static void
svariabletype_assign_extension(SVariableType svt, Oid extid)
{
	Relation	pg_extension;
	ScanKeyData entry[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Form_pg_extension ext;
	bool		isnull;
	Datum		datum;
	MemoryContext oldcxt;

	Assert(OidIsValid(extid));

	/* There's no syscache for pg_extension, so do it the hard way */
	pg_extension = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(extid));

	scan = systable_beginscan(pg_extension,
							  ExtensionOidIndexId, true,
							  NULL, 1, entry);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("extension with OID %u does not exist", svt->extid)));

	ext = (Form_pg_extension) GETSTRUCT(tuple);
	svt->extid = extid;
	svt->ext_xmin = HeapTupleHeaderGetRawXmin(tuple->t_data);
	svt->ext_tid = tuple->t_self;

	datum = heap_getattr(tuple, Anum_pg_extension_extversion,
						 RelationGetDescr(pg_extension), &isnull);

	if (isnull)
		elog(ERROR, "extversion is null");

	oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

	svt->extversion = TextDatumGetCString(datum);
	svt->extname = pstrdup(NameStr(ext->extname));

	MemoryContextSwitchTo(oldcxt);

	systable_endscan(scan);
	table_close(pg_extension, AccessShareLock);
}

static bool
svariabletype_verify_ext_fast(SVariableType svt)
{
	Relation	pg_extension;
	ScanKeyData entry[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	bool		result = true;

	Assert(OidIsValid(svt->extid));

	/* There's no syscache for pg_extension, so do it the hard way */
	pg_extension = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(svt->extid));

	scan = systable_beginscan(pg_extension,
							  ExtensionOidIndexId, true,
							  NULL, 1, entry);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		if (svt->ext_xmin != HeapTupleHeaderGetRawXmin(tuple->t_data) ||
			!ItemPointerEquals(&svt->ext_tid, &tuple->t_self))
			result = false;
	}
	else
		result = false;

	systable_endscan(scan);
	table_close(pg_extension, AccessShareLock);

	return result;
}

/*
 * We hold data like typlen, typbyval for usual purposes. More
 * we hold xmin, tid, extid like type's fingerprint. This is
 * used later for type verification stored value of session variable.
 */
static void
svariabletype_init(SVariableType svt, HeapTuple tuple, Oid typid)
{
	Form_pg_type typ;
	MemoryContext oldcxt;

	memset(svt, 0, sizeof(SVariableTypeData));

	svt->typid = typid;
	svt->gennum = 1;

	typ = (Form_pg_type) GETSTRUCT(tuple);

	/* save basic attributtes */
	svt->typlen = typ->typlen;
	svt->typbyval = typ->typbyval;

	/* save info about type */
	svt->typ_xmin = HeapTupleHeaderGetRawXmin(tuple->t_data);
	svt->typ_tid = tuple->t_self;

	oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

	svt->typname = pstrdup(NameStr(typ->typname));

	MemoryContextSwitchTo(oldcxt);

	if (typ->typtype == TYPTYPE_DOMAIN)
	{
		Oid			basetypid;

		svt->is_domain = true;
		basetypid = getBaseType(typid);

		svt->base_type = get_svariabletype(basetypid);
		svt->base_type_gennum = get_svariable_valid_type_gennum(svt->base_type);
		svt->is_rowtype = svt->base_type->is_rowtype;
	}
	else
	{
		svt->is_domain = false;
		svt->is_rowtype = typ->typtype == TYPTYPE_COMPOSITE;
	}

	/*
	 * Store fingerprints of fields of composite types.
	 * Probably buildin types should not be changed, but
	 * just be safe, and store fingerprints for all composite
	 * types (including buildin composite types).
	*/
	if (svt->is_rowtype)
	{
		Oid			rowtypid;
		TupleDesc	tupdesc;
		int			i;
		int			natts = 0;

		if (svt->is_domain)
			rowtypid = svt->base_type->typid;
		else
			rowtypid = svt->typid;

		tupdesc = lookup_rowtype_tupdesc(rowtypid, -1);

		svt->attrs = MemoryContextAlloc(SVariableMemoryContext,
											 tupdesc->natts * sizeof(SvariableTypeDataField));

		for (i = 0; i < tupdesc->natts; i++)
		{
			SVariableType field_svartype;
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (!attr->attisdropped)
			{
				field_svartype = get_svariabletype(attr->atttypid);

				svt->attrs[natts].svartype = field_svartype;
				svt->attrs[natts++].gennum = get_svariable_valid_type_gennum(field_svartype);
			}
		}

		svt->natts = natts;

		ReleaseTupleDesc(tupdesc);
	}

	svt->domain_check_extra_lxid = InvalidLocalTransactionId;

	svt->verified_lxid = MyProc->lxid;

	/* try to find related extension */
	svt->extid = getExtensionOfObject(TypeRelationId, typid);

	if (OidIsValid(svt->extid))
		svariabletype_assign_extension(svt, svt->extid);
}

/*
 * the field check can ignore dropped fields
 */
static bool
svartype_verify_composite_fast(SVariableType svt)
{
	bool	result = true;
	TupleDesc	tupdesc;
	int		i;
	int		attrn = 0;

	Assert(svt);
	Assert(svt->is_rowtype);

	tupdesc = lookup_rowtype_tupdesc_noerror(svt->typid, -1, true);
	if (!tupdesc)
		return false;

	/* only not dropped attributies are stored */
	for (i = 0; i < svt->natts; i++)
	{
		Form_pg_attribute attr = NULL;

		/* skip dropped attributies */
		while (attrn < tupdesc->natts)
		{
			attr = TupleDescAttr(tupdesc, attrn++);

			if (!attr->attisdropped)
				break;
		}

		if (attr && !attr->attisdropped)
		{
			SVariableType field_svt = svt->attrs[i].svartype;
			int64		field_gennum = svt->attrs[i].gennum;

			if (field_svt->typid != attr->atttypid)
			{
				result = false;
				break;
			}

			if (field_gennum != field_svt->gennum)
			{
				result = false;
				break;
			}

			if (field_gennum != get_svariable_valid_type_gennum(field_svt))
			{
				result = false;
				break;
			}
		}
		else
		{
			result = false;
			break;
		}
	}

	/* now only dropped columns can be allowed */
	while (attrn < tupdesc->natts)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attrn++);

		if (!attr->attisdropped)
		{
			result = false;
			break;
		}
	}

	ReleaseTupleDesc(tupdesc);

	return result;
}

/*
 * Check type fingerprint and if it is not valid, then does an update, and
 * increase generation number. We can trust just to buildin types. Composite
 * types are checked recusively until we iterarate to buildin types. External
 * types are valid if related record is without change, or version string is
 * equal. Although the record can be untouched, we need to check extension record,
 * because format can be changed by extension updade.
 */
static int64
get_svariable_valid_type_gennum(SVariableType svt)
{
	HeapTuple	tuple;
	bool		fast_check = true;

	Assert(svt);

	/* Buildin scalar objects are trustworthy */
	if (svt->typid < FirstNormalObjectId && !svt->is_rowtype)
		return svt->gennum;

	/* don't repeat check in one transaction */
	if (svt->verified_lxid == MyProc->lxid)
		return svt->gennum;

	/*
	 * First we check the type record. If it was not changed, then
	 * we can trust to stored fingerprint. In this case we can do
	 * fast check of extension (because the format of type can be
	 * changed when extension was updated). When fast check of type
	 * fails, we have to run slow check.
	 */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(svt->typid));
	if (!(svt->typ_xmin == HeapTupleHeaderGetRawXmin(tuple->t_data) &&
		  ItemPointerEquals(&svt->typ_tid, &tuple->t_self)))
		fast_check = false;

	if (fast_check && OidIsValid(svt->extid))
	{
		if (!svariabletype_verify_ext_fast(svt))
			fast_check = false;
	}

	/* When type or extension records are up to date, check base type */
	if (fast_check && svt->is_domain)
	{
		if (svt->base_type_gennum != svt->base_type->gennum)
		{
			fast_check = false;
		}
		else if (get_svariable_valid_type_gennum(svt->base_type) !=
			  svt->base_type_gennum)
		{
			fast_check = false;
		}
	}

	if (fast_check && svt->is_rowtype)
	{
		if (!svartype_verify_composite_fast(svt))
			fast_check = false;
	}

	if (!fast_check)
	{
		SVariableTypeData nsvtd;

		/*
		 * Slow check. We create new SVariableType value. Compare it with
		 * previous value, and if it is different, then we replace old by
		 * new and we increase gennum
		 */
		svariabletype_init(&nsvtd, tuple, svt->typid);

		if (svariabletypes_equals(svt, &nsvtd))
		{
			svariabletype_refresh(svt, &nsvtd);
			svariabletype_free(&nsvtd);
		}
		else
			svariabletype_update(svt, &nsvtd);
	}

	ReleaseSysCache(tuple);

	svt->verified_lxid = MyProc->lxid;

	return svt->gennum;
}

static SVariableType
get_svariabletype(Oid typid)
{
	SVariableType svt;
	bool		found;

	Assert(sessionvars_types);

	svt = (SVariableType) hash_search(sessionvars_types, &typid,
										   HASH_ENTER, &found);

	if (!found)
	{
		HeapTuple	tuple;

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);

		svariabletype_init(svt, tuple, typid);

		ReleaseSysCache(tuple);
	}

	return svt;
}

/*
 * Returns true, when type of stored value is still valid
 */
static bool
session_variable_use_valid_type(SVariable svar)
{
	Assert(svar);
	Assert(svar->svartype);

	/*
	 * when referenced type is not valid of obsolete, the
	 * value is stored in maybe not up the data format.
	 */
	if (svar->gennum != svar->svartype->gennum)
		return false;

	/* enforce type verification, get fresh generation number */
	if (svar->gennum != get_svariable_valid_type_gennum(svar->svartype))
		return false;

	return true;
}

/*
 * Releases stored data from session variable, but preserve the hash entry
 * in sessionvars.
 */
static void
free_session_variable_value(SVariable svar)
{
	if (svar->freeval)
		pfree(DatumGetPointer(svar->value));

	/* Clean current value */
	svar->value = (Datum) 0;
	svar->isnull = true;
	svar->freeval = false;

	/*
	 * We can mark this session variable as valid when
	 * it has not default expression, and when null is
	 * allowed. When it has defexpr, then the content
	 * will be valid after an assignment or defexp evaluation.
	 */
	svar->is_valid = !svar->has_defexpr && !svar->is_not_null;
}

/*
 * Release the variable defined by varid from sessionvars
 * hashtab.
 */
static void
remove_session_variable(SVariable svar)
{
	free_session_variable_value(svar);

	elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) is removing from memory",
				 svar->nsname, svar->name,
				 svar->varid);

	if (hash_search(sessionvars,
					(void *) &svar->varid,
					HASH_REMOVE,
					NULL) == NULL)
		elog(DEBUG1, "hash table corrupted");
}

/*
 * Release the variable defined by varid from sessionvars
 * hashtab.
 */
static void
remove_session_variable_by_id(Oid varid)
{
	SVariable svar;
	bool		found;

	if (!sessionvars)
		return;

	svar = (SVariable) hash_search(sessionvars, &varid,
										HASH_FIND, &found);
	if (found)
		remove_session_variable(svar);
}

/*
 * Callback function for session variable invalidation.
 */
static void
pg_variable_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	SVariable svar;

	/*
	 * There is no guarantee of sessionvars being initialized, even when
	 * receiving an invalidation callback, as DISCARD [ ALL | VARIABLES ]
	 * destroys the hash table entirely.
	 */
	if (!sessionvars)
		return;

	/*
	 * When the hashvalue is not specified, then we have to recheck all
	 * currently used session variables. Since we can't guarantee the exact
	 * session variable from its hashValue, we have to iterate over all
	 * items of hash table.
	 */
	hash_seq_init(&status, sessionvars);

	while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
	{
		if (hashvalue == 0 || svar->hashvalue == hashvalue)
		{
			/*
			 * We don't need to execute recheck for variables,
			 * that will be surelly purged from memory at eox
			 * time.
			 */
			if (!svar->eox_reset)
			{
				RecheckVariableRequest *req = NULL;
				ListCell *l;

				/*
				 * The recheck request should be postponed to the end of current
				 * transaction if there are request recheck from previous
				 * transaction. This ensure so we don't purge memory too
				 * early, when we process sinval from previous aborted
				 * transaction. Possible test case:
				 *
				 * LET var = X;
				 * BEGIN DROP VAR var; ROOLBACK;
				 * BEGIN DROP VAR var ROLLBACK;
				 * SELECT var; -- should be X
				 *
				 * Without postponing, the call of sync_sessionvars_all
				 * in second transaction purge (badly) memory, because
				 * sinval message from prrevious aborted transaction is
				 * processed and in this time related record from pg_variable
				 * is removed. So when we find request recheck from prev
				 * transaction, we just update lxid.
				 */
				foreach (l, xact_recheck_requests)
				{
					req = (RecheckVariableRequest *) lfirst(l);
					if (req->varid == svar->varid)
					{
						if (req->lxid != MyProc->lxid)
						{
							req->lxid = MyProc->lxid;

							elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) should be rechecked (forced by sinval)"
										 " (found recheck request from previous transaction)",
									 svar->nsname, svar->name,
									 svar->varid);

							break;
						}
					}
				}

				if (!req)
				{
					MemoryContext oldcxt;

					oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

					req = (RecheckVariableRequest *) palloc(sizeof(RecheckVariableRequest));

					req->varid = svar->varid;
					req->lxid = MyProc->lxid;

					elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) should be rechecked (forced by sinval)",
								 svar->nsname, svar->name,
								 svar->varid);

					xact_recheck_requests = lappend(xact_recheck_requests, req);

					MemoryContextSwitchTo(oldcxt);
				}
			}
		}

		/*
		 * although it there is low probability, we have to iterate
		 * over all actively used session variables, because hashvalue
		 * is not unique identifier.
		 */
	}
}

/*
 * When we need to recheck all session variables, then
 * the most effective method is seq scan over hash tab.
 * We need to check synchronization request before any
 * read to be sure so returned data are valid.
 */
static void
sync_sessionvars_all()
{
	SVariable svar;
	ListCell   *l;

	if (synced_lxid == MyProc->lxid)
		return;

	if (!xact_recheck_requests)
		return;

	/*
	 * sessionvars is null after DISCARD VARIABLES.
	 * When we are sure, so there are not any
	 * active session variable in this session.
	 */
	if (!sessionvars)
	{
		list_free_deep(xact_recheck_requests);
		xact_recheck_requests = NIL;
		return;
	}

	/*
	 * This routine is called before any reading. So the
	 * session should be in transaction state. This is required
	 * for access to system catalog.
	 */
	Assert(IsTransactionState());

	foreach(l, xact_recheck_requests)
	{
		RecheckVariableRequest *req;
		bool	found;

		req = (RecheckVariableRequest *) lfirst(l);

		if (req->lxid != MyProc->lxid)
		{
			svar = (SVariable) hash_search(sessionvars, &req->varid,
											HASH_FIND, &found);

			if (found)
			{
				HeapTuple	tp;

				tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(req->varid));

				if (HeapTupleIsValid(tp))
					ReleaseSysCache(tp);
				else
					remove_session_variable(svar);
			}

			xact_recheck_requests = foreach_delete_current(xact_recheck_requests, l);
			pfree(req);
		}
	}

	/* Don't repeat this check in this transaction more time */
	synced_lxid = MyProc->lxid;
}

/*
 * Create the hash table for storing session variables
 */
static void
create_sessionvars_hashtables(void)
{
	HASHCTL		vars_ctl;
	HASHCTL		types_ctl;

	/* set callbacks */
	if (first_time)
	{
		/* Read sinval messages */
		CacheRegisterSyscacheCallback(VARIABLEOID,
									  pg_variable_cache_callback,
									  (Datum) 0);
		/* needs its own long lived memory context */
		SVariableMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "session variables",
								  ALLOCSET_START_SMALL_SIZES);

		first_time = false;
	}

	Assert(SVariableMemoryContext);

	memset(&vars_ctl, 0, sizeof(vars_ctl));
	vars_ctl.keysize = sizeof(Oid);
	vars_ctl.entrysize = sizeof(SVariableData);
	vars_ctl.hcxt = SVariableMemoryContext;

	Assert(sessionvars == NULL);

	sessionvars = hash_create("Session variables", 64, &vars_ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	memset(&types_ctl, 0, sizeof(types_ctl));
	types_ctl.keysize = sizeof(Oid);
	types_ctl.entrysize = sizeof(SVariableTypeData);
	types_ctl.hcxt = SVariableMemoryContext;

	Assert(sessionvars_types == NULL);

	sessionvars_types = hash_create("Session variable types", 64, &types_ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Assign some content to the session variable. It's copied to
 * SVariableMemoryContext if necessary.
 *
 * init_mode is true, when the value of session variable is being initialized
 * by default expression or by null. Only in this moment we can allow to
 * modify immutable variables with default expression.
 */
static void
set_session_variable(SVariable svar, Datum value,
					 bool isnull, Oid typid,
					 bool init_mode)
{
	MemoryContext oldcxt;
	Datum		newval = value;

	/* Don't allow assignment of null to NOT NULL variable */
	if (isnull && svar->is_not_null)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	Assert(svar->svartype);

	if (svar->svartype->typid != typid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type \"%s\" of assigned value is different than type \"%s\" of session variable \"%s.%s\"",
						format_type_be(typid),
						format_type_be(svar->svartype->typid),
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	svar->gennum = get_svariable_valid_type_gennum(svar->svartype);

	/*
	 * Don't allow updating of immutable session variable that has assigned
	 * not null value or has default expression (and then the value should be
	 * result of default expression always). Don't do this check, when variable
	 * is being initialized.
	 */
	if (!init_mode &&
		(svar->is_immutable && (svar->is_valid || svar->has_defexpr)))
		ereport(ERROR,
				(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
				 errmsg("session variable \"%s.%s\" is declared IMMUTABLE",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	/* copy value to session persistent context */
	oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);
	if (!isnull)
		newval = datumCopy(value,
						   svar->svartype->typbyval,
						   svar->svartype->typlen);

	MemoryContextSwitchTo(oldcxt);

	free_session_variable_value(svar);

	svar->value = newval;
	svar->isnull = isnull;
	svar->freeval = newval != value;
	svar->is_valid = true;
}

/*
 * Initialize svar from var
 * svar - SVariable - holds value
 * var  - Variable - holds metadata
 */
static void
init_session_variable(SVariable svar, Variable *var)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	Assert(OidIsValid(var->oid));

	svar->varid = var->oid;

	svar->name = pstrdup(var->name);
	svar->nsname = get_namespace_name(var->namespace);

	svar->svartype = get_svariabletype(var->typid);
	svar->gennum = get_svariable_valid_type_gennum(svar->svartype);

	svar->value = (Datum) 0;
	svar->isnull = true;
	svar->freeval = false;

	svar->is_not_null = var->is_not_null;
	svar->is_immutable = var->is_immutable;
	svar->has_defexpr = var->has_defexpr;

	svar->hashvalue = GetSysCacheHashValue1(VARIABLEOID,
											ObjectIdGetDatum(var->oid));

	/* the value of variable is not known yet */
	svar->is_valid = false;

	svar->eox_reset = var->eoxaction == VARIABLE_EOX_RESET ||
					  var->eoxaction == VARIABLE_EOX_DROP;

	if (svar->eox_reset)
	{

		MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

		xact_reset_varids = lappend_oid(xact_reset_varids, var->oid);

		MemoryContextSwitchTo(oldcxt);
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Search the given session variable in the hash table. If it doesn't
 * exist, then insert it (and calculate defexpr if it exists).
 *
 * Caller is responsible for doing permission checks.
 *
 * As side effect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
static SVariable
prepare_variable_for_reading(Oid varid)
{
	SVariable svar;
	Variable	var;
	bool		found;

	var.oid = InvalidOid;

	if (!sessionvars)
		create_sessionvars_hashtables();

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	/* Ensure so all entries in sessionvars hash table are valid */
	sync_sessionvars_all();

	svar = (SVariable) hash_search(sessionvars, &varid,
										HASH_ENTER, &found);

	/* Return content if it is available and valid */
	if (!found || !svar->is_valid)
	{
		/* We need to load defexpr. */
		initVariable(&var, varid, false);

		if (!found)
		{
			init_session_variable(svar, &var);

			elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new entry in memory (emitted by READ)",
						 svar->nsname, svar->name,
						 varid);
		}

		/* Raise an error when we cannot initialize variable correctly */
		if (var.is_not_null && !var.defexpr)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
							get_namespace_name(get_session_variable_namespace(varid)),
							get_session_variable_name(varid)),
					 errdetail("The session variable was not initialized yet.")));

		if (svar->has_defexpr)
		{
			Datum		value = (Datum) 0;
			bool		isnull;
			EState	   *estate = NULL;
			Expr	   *defexpr;
			ExprState  *defexprs;
			MemoryContext oldcxt;

			/* Prepare default expr */
			estate = CreateExecutorState();

			oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

			defexpr = expression_planner((Expr *) var.defexpr);
			defexprs = ExecInitExpr(defexpr, NULL);
			value = ExecEvalExprSwitchContext(defexprs,
											  GetPerTupleExprContext(estate),
											  &isnull);


			/* Store result before releasing Executor memory */
			set_session_variable(svar, value, isnull, svar->svartype->typid, true);

			MemoryContextSwitchTo(oldcxt);

			FreeExecutorState(estate);
		}
		else
			set_session_variable(svar, (Datum) 0, true, svar->svartype->typid, true);
	}

	/*
	 * Check if stored value has still valid type. Now, we just to
	 * raise error. Future versions can try to convert stored to
	 * current binary format instead (composite types).
	 */
	if (!session_variable_use_valid_type(svar))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("format of stored value of \"%s.%s\" session variable can be outdated",
						get_namespace_name(get_session_variable_namespace(varid)),
						get_session_variable_name(varid))));

	/*
	 * Although the value of domain type should be valid (it is
	 * checked when it is assigned to session variable), we have to
	 * check related constraints anytime. It can be more expensive
	 * than in PL/pgSQL. PL/pgSQL forces domain checks when value
	 * is assigned to the variable or when value is returned from
	 * function. Fortunately, domain types manage cache of constraints by
	 * self.
	 */
	if (svar->svartype->is_domain)
	{
		MemoryContext oldcxt = CurrentMemoryContext;

		/*
		 * Store domain_check extra in CurTransactionContext. When we are
		 * in other transaction, the domain_check_extra cache is not valid.
		 */
		if (svar->svartype->domain_check_extra_lxid != MyProc->lxid)
			svar->svartype->domain_check_extra = NULL;

		domain_check(svar->value,
					 svar->isnull,
					 svar->svartype->typid,
					 &svar->svartype->domain_check_extra,
					 CurTransactionContext);

		svar->svartype->domain_check_extra_lxid = MyProc->lxid;

		MemoryContextSwitchTo(oldcxt);
	}

	return svar;
}

/*
 * Store the given value in an SVariable, and cache it if not already present.
 *
 * Caller is responsible for doing permission checks.
 * We try not to break the previous value, if something is wrong.
 *
 * As side effect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
void
SetSessionVariable(Oid varid, Datum value, bool isNull, Oid typid)
{
	SVariable svar;
	bool		found;

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	if (!sessionvars)
		create_sessionvars_hashtables();

	/* Ensure so all entries in sessionvars hash table are valid */
	sync_sessionvars_all();

	svar = (SVariable) hash_search(sessionvars, &varid,
										HASH_ENTER, &found);

	if (!found)
	{
		Variable	var;

		/* We don't need to know defexpr here */
		initVariable(&var, varid, true);
		init_session_variable(svar, &var);

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new entry in memory (emitted by WRITE)",
					 svar->nsname, svar->name,
					 varid);
	}

	set_session_variable(svar, value, isNull, typid, false);
}

/*
 * Wrapper around SetSessionVariable after checking for correct permission.
 */
void
SetSessionVariableWithSecurityCheck(Oid varid, Datum value, bool isNull, Oid typid)
{
	AclResult	aclresult;

	/*
	 * Is possible to write to session variable?
	 */
	aclresult = pg_variable_aclcheck(varid, GetUserId(), ACL_WRITE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	SetSessionVariable(varid, value, isNull, typid);
}

/*
 * Returns a copy of value of the session variable specified by varid
 * Caller is responsible for doing permission checks.
 */
Datum
CopySessionVariable(Oid varid, bool *isNull, Oid *typid)
{
	SVariable svar;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL && svar->is_valid);

	*isNull = svar->isnull;
	*typid = svar->svartype->typid;

	if (!svar->isnull)
		return datumCopy(svar->value,
						 svar->svartype->typbyval,
						 svar->svartype->typlen);

	return (Datum) 0;
}

/*
 * Returns a copy of value of the session variable specified by varid
 * with check of expected type. Like previous function, the caller
 * is responsible for doing permission checks.
 */
Datum
CopySessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid)
{
	SVariable svar;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL && svar->is_valid);

	if (expected_typid != svar->svartype->typid)
		elog(ERROR, "type of variable \"%s.%s\" is different than expected",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid));

	*isNull = svar->isnull;

	if (!svar->isnull)
		return datumCopy(svar->value,
						 svar->svartype->typbyval,
						 svar->svartype->typlen);

	return (Datum) 0;
}

/*
 * Returns a value of session variable identified by varid with
 * check of expected type. Like previous function, the called
 * is reposible for doing permission check.
 */
Datum
GetSessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid)
{
	SVariable svar;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL && svar->is_valid);

	if (expected_typid != svar->svartype->typid)
		elog(ERROR, "type of variable \"%s.%s\" is different than expected",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid));

	*isNull = svar->isnull;

	if (svar->isnull)
		return (Datum) 0;

	return svar->value;
}

/*
 * Routines used for manipulation with session variables from
 * SQL level
 */

/*
 * Creates new variable - entry in pg_catalog.pg_variable table
 *
 * Used by CREATE VARIABLE command
 */
ObjectAddress
DefineSessionVariable(ParseState *pstate, CreateSessionVarStmt *stmt)
{
	Oid			namespaceid;
	AclResult	aclresult;
	Oid			typid;
	int32		typmod;
	Oid			varowner = GetUserId();
	Oid			collation;
	Oid			typcollation;
	ObjectAddress variable;

	Node	   *cooked_default = NULL;

	/*
	 * Check consistency of arguments
	 */
	if (stmt->eoxaction == VARIABLE_EOX_DROP
		&& stmt->variable->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("ON COMMIT DROP can only be used on temporary variables")));

	if (stmt->is_not_null && stmt->is_immutable && !stmt->defexpr)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("IMMUTABLE NOT NULL variable requires default expression")));

	namespaceid =
		RangeVarGetAndCheckCreationNamespace(stmt->variable, NoLock, NULL);

	typenameTypeIdAndMod(pstate, stmt->typeName, &typid, &typmod);
	typcollation = get_typcollation(typid);

	aclresult = pg_type_aclcheck(typid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typid);

	if (stmt->collClause)
		collation = LookupCollation(pstate,
									stmt->collClause->collname,
									stmt->collClause->location);
	else
		collation = typcollation;;

	/* Complain if COLLATE is applied to an uncollatable type */
	if (OidIsValid(collation) && !OidIsValid(typcollation))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("collations are not supported by type %s",
						format_type_be(typid)),
				 parser_errposition(pstate, stmt->collClause->location)));

	if (stmt->defexpr)
	{
		cooked_default = transformExpr(pstate, stmt->defexpr,
									   EXPR_KIND_VARIABLE_DEFAULT);

		cooked_default = coerce_to_specific_type(pstate,
												 cooked_default, typid, "DEFAULT");
		assign_expr_collations(pstate, cooked_default);
	}

	variable = VariableCreate(stmt->variable->relname,
							  namespaceid,
							  typid,
							  typmod,
							  varowner,
							  collation,
							  cooked_default,
							  stmt->eoxaction,
							  stmt->is_not_null,
							  stmt->if_not_exists,
							  stmt->is_immutable);

	elog(DEBUG1, "record for session variable \"%s\" (oid:%d) was created in pg_variable",
				stmt->variable->relname, variable.objectId);

	/*
	 * For temporary variables, we need to create a new end of xact action to
	 * ensure deletion from catalog.
	 */
	if (stmt->eoxaction == VARIABLE_EOX_DROP)
	{
		Assert(isTempNamespace(namespaceid));

		register_session_variable_xact_action(variable.objectId,
											  SVAR_ON_COMMIT_DROP);
	}

	/*
	 * We must bump the command counter to make the newly-created variable
	 * tuple visible for any other operations.
	 */
	CommandCounterIncrement();

	return variable;
}

/*
 * Drop variable by OID. This routine can be called by directly
 * command DROP VARIABLE or indirectly for TEMP ON COMMIT DROP
 * variables.
 *
 * In first case we want to postpone memory purging to commit or
 * abort time. In second case, the menory will be purged by
 * processing xact_reset_varids list. In second case, the calling
 * of unregistration of SVAR_ON_COMMIT_DROP action is useless, but
 * we are not able to detect which case is executed.
 */
void
RemoveSessionVariable(Oid varid)
{
	SVariable svar;
	bool		found;
	Relation	rel;
	HeapTuple	tup;

	rel = table_open(VariableRelationId, RowExclusiveLock);

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	if (message_level_is_interesting(DEBUG1))
	{
		Form_pg_variable varform;

		varform = (Form_pg_variable) GETSTRUCT(tup);

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) is deleted from pg_variable",
					 get_namespace_name(varform->varnamespace),
					 NameStr(varform->varname),
					 varid);
	}

	CatalogTupleDelete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);

	/*
	 * We removed entry from catalog already, we must not do it
	 * again at end of xact time
	 */
	unregister_session_variable_xact_action(varid, SVAR_ON_COMMIT_DROP);

	if (sessionvars)
	{
		svar = (SVariable) hash_search(sessionvars, &varid,
											HASH_FIND, &found);

		/*
		 * For variables that are not purged by default we need to
		 * register SVAR_ON_COMMIT_RESET action. This action can
		 * be reverted by ABORT of DROP VARIABLE command.
		 */
		if (found && !svar->eox_reset)
		{
			/*
			 * And we want to enforce variable clearning when this transaction or
			 * subtransaction will be committed (we don't need to wait for
			 * sinval message). The cleaning action for one session variable
			 * can be repeated in the action list without causing any problem,
			 * so we don't need to ensure uniqueness. We need a different action
			 * from RESET, because RESET is executed on any transaction end,
			 * but we want to execute cleaning only when the current transaction
			 * will be committed.
			 */
			register_session_variable_xact_action(varid, SVAR_ON_COMMIT_RESET);
		}
	}
}

/*
 * Fast drop of the complete content of all session variables hash table.
 * This is code for DISCARD VARIABLES command. This command
 * cannot be run inside transaction, so we don't need to handle
 * end of transaction actions.
 */
void
ResetSessionVariables(void)
{
	/* Destroy hash table and reset related memory context */
	if (sessionvars)
	{
		hash_destroy(sessionvars);
		sessionvars = NULL;

		hash_destroy(sessionvars_types);
		sessionvars_types = NULL;
	}

	/* Release memory allocated by session variables */
	if (SVariableMemoryContext != NULL)
		MemoryContextReset(SVariableMemoryContext);

	/*
	 * There are not any session variables left, so simply trim
	 * both xact action lists.
	 */
	list_free_deep(xact_on_commit_drop_actions);
	xact_on_commit_drop_actions = NIL;

	list_free_deep(xact_on_commit_reset_actions);
	xact_on_commit_reset_actions = NIL;

	/* We should clean xact_reset_varids */
	list_free(xact_reset_varids);
	xact_reset_varids = NIL;

	/* we should clean xact_recheck_requests */
	list_free_deep(xact_recheck_requests);
	xact_recheck_requests = NIL;
}

/*
 * Assign result of evaluated expression to session variable
 */
void
ExecuteLetStmt(ParseState *pstate,
			   LetStmt *stmt,
			   ParamListInfo params,
			   QueryEnvironment *queryEnv,
			   QueryCompletion *qc)
{
	Query	   *query = castNode(Query, stmt->query);
	List	   *rewritten;
	DestReceiver *dest;
	AclResult	aclresult;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Oid			varid = query->resultVariable;

	Assert(OidIsValid(varid));

	/*
	 * Is it allowed to write to session variable?
	 */
	aclresult = pg_variable_aclcheck(varid, GetUserId(), ACL_WRITE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	/* Create dest receiver for LET */
	dest = CreateDestReceiver(DestVariable);
	SetVariableDestReceiverParams(dest, varid);

	/* run rewriter - can be used for replacement of DEFAULT node */
	query = copyObject(query);

	rewritten = QueryRewrite(query);

	Assert(list_length(rewritten) == 1);

	query = linitial_node(Query, rewritten);
	Assert(query->commandType == CMD_SELECT);

	/* plan the query */
	plan = pg_plan_query(query, pstate->p_sourcetext,
						 CURSOR_OPT_PARALLEL_OK, params);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only
	 * matter if the planner executed an allegedly-stable function that
	 * changed the database contents, but let's do it anyway to be
	 * parallel to the EXPLAIN code path.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan to completion */
	ExecutorRun(queryDesc, ForwardScanDirection, 2L, true);

	/* save the rowcount if we're given a qc to fill */
	if (qc)
		SetQueryCompletion(qc, CMDTAG_LET, queryDesc->estate->es_processed);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();
}

/*
 * Registration of actions to be executed on session variables at transaction
 * end time. We want to drop temporary session variables with clause ON COMMIT
 * DROP, or we want to clean (reset) local memory allocated by
 * values of dropped session variables.
 */

/*
 * Register a session variable xact action.
 */
static void
register_session_variable_xact_action(Oid varid,
									  SVariableXActAction action)
{
	SVariableXActActionItem *xact_ai;
	MemoryContext oldcxt;

	elog(DEBUG1, "SVariableXActAction \"%s\" is registered for session variable (oid:%u)",
						SvariableXActActionName(action), varid);

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	xact_ai = (SVariableXActActionItem *)
							palloc(sizeof(SVariableXActActionItem));

	xact_ai->varid = varid;

	xact_ai->creating_subid = GetCurrentSubTransactionId();
	xact_ai->deleting_subid = InvalidSubTransactionId;

	if (action == SVAR_ON_COMMIT_DROP)
		xact_on_commit_drop_actions = lcons(xact_ai, xact_on_commit_drop_actions);
	else
		xact_on_commit_reset_actions = lcons(xact_ai, xact_on_commit_reset_actions);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Unregister an action on a given session variable from action list. In this
 * moment, the action is just marked as deleted by setting deleting_subid. The
 * calling even might be rollbacked, in which case we should not lose this
 * action.
 */
static void
unregister_session_variable_xact_action(Oid varid,
									SVariableXActAction action)
{
	ListCell   *l;

	Assert(action == SVAR_ON_COMMIT_DROP);

	elog(DEBUG1, "SVariableXActAction \"%s\" is unregistered for session variable (oid:%u)",
						SvariableXActActionName(action), varid);

	foreach(l, xact_on_commit_drop_actions)
	{
		SVariableXActActionItem *xact_ai =
					(SVariableXActActionItem *) lfirst(l);

		if (xact_ai->varid == varid)
			xact_ai->deleting_subid = GetCurrentSubTransactionId();
	}
}

/*
 * Perform ON TRANSACTION END RESET or ON COMMIT DROP
 * and COMMIT/ROLLBACK of transaction session variables.
 */
void
AtPreEOXact_SessionVariable_on_xact_actions(bool isCommit)
{
	ListCell   *l;

	/*
	 * Clean memory for all eox_reset variables. Do it
	 * first, it reduces enhancing action lists about
	 * RECHECK action.
	 */
	foreach(l, xact_reset_varids)
	{
		remove_session_variable_by_id(lfirst_oid(l));
	}

	/* We can clean xact_reset_varids */
	list_free(xact_reset_varids);
	xact_reset_varids = NIL;

	if (isCommit && xact_on_commit_drop_actions)
	{
		foreach(l, xact_on_commit_drop_actions)
		{
			SVariableXActActionItem *xact_ai =
								(SVariableXActActionItem *) lfirst(l);

			/* Iterate only over entries that are still pending */
			if (xact_ai->deleting_subid == InvalidSubTransactionId)
			{
				ObjectAddress object;

				/*
				 * ON COMMIT DROP is allowed only for temp session
				 * variables. So we should explicitly delete only when
				 * current transaction was committed. When it's rollback,
				 * then session variable is removed automatically.
				 */

				object.classId = VariableRelationId;
				object.objectId = xact_ai->varid;
				object.objectSubId = 0;

				/*
				 * Since this is an automatic drop, rather than one
				 * directly initiated by the user, we pass the
				 * PERFORM_DELETION_INTERNAL flag.
				 */
				elog(DEBUG1, "session variable (oid:%u) will be deleted (forced by SVAR_ON_COMMIT_DROP action)",
							  xact_ai->varid);

				performDeletion(&object, DROP_CASCADE,
								PERFORM_DELETION_INTERNAL |
								PERFORM_DELETION_QUIETLY);
			}
		}
	}

	/*
	 * Any drop action left is an entry that was unregistered and not
	 * rollbacked, so we can simply remove them.
	 */
	list_free_deep(xact_on_commit_drop_actions);
	xact_on_commit_drop_actions = NIL;

	if (isCommit && xact_recheck_requests)
	{
		Assert(sessionvars);

		foreach(l, xact_recheck_requests)
		{
			SVariable	svar;
			bool		found;
			RecheckVariableRequest *req;

			req = (RecheckVariableRequest *) lfirst(l);

			svar = (SVariable) hash_search(sessionvars, &req->varid,
										   HASH_FIND, &found);

			if (found)
			{
				HeapTuple	tp;

				tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));

				if (HeapTupleIsValid(tp))
					ReleaseSysCache(tp);
				else
					remove_session_variable(svar);
			}
		}

		list_free_deep(xact_recheck_requests);
		xact_recheck_requests = NIL;
	}

	if (isCommit && xact_on_commit_reset_actions)
	{
		foreach(l, xact_on_commit_reset_actions)
		{
			SVariableXActActionItem *xact_ai =
								(SVariableXActActionItem *) lfirst(l);

			/*
			 * When we process DROP VARIABLE statement, we create
			 * SVAR_ON_COMMIT_RESET xact action. We want to process
			 * this action only when related transaction is commited
			 * (when DROP VARIABLE statement sucessfully processed).
			 * We want to preserve variable content, when the transaction
			 * with DROP VARAIBLE statement was reverted.
			 */
			if (xact_ai->deleting_subid == InvalidSubTransactionId)
				remove_session_variable_by_id(xact_ai->varid);
		}
	}

	list_free_deep(xact_on_commit_reset_actions);
	xact_on_commit_reset_actions = NIL;
}

/*
 * Post-subcommit or post-subabort cleanup of xact action list.
 *
 * During subabort, we can immediately remove entries created during this
 * subtransaction. During subcommit, just transfer entries marked during
 * this subtransaction as being the parent's responsibility.
 */
void
AtEOSubXact_SessionVariable_on_xact_actions(bool isCommit, SubTransactionId mySubid,
											SubTransactionId parentSubid)
{
	ListCell   *cur_item;

	foreach(cur_item, xact_on_commit_drop_actions)
	{
		SVariableXActActionItem *xact_ai =
								  (SVariableXActActionItem *) lfirst(cur_item);

		if (!isCommit && xact_ai->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			xact_on_commit_drop_actions = foreach_delete_current(xact_on_commit_drop_actions, cur_item);
			pfree(xact_ai);
		}
		else
		{
			/* cur_item must be preserved */
			if (xact_ai->creating_subid == mySubid)
				xact_ai->creating_subid = parentSubid;
			if (xact_ai->deleting_subid == mySubid)
				xact_ai->deleting_subid = isCommit ? parentSubid : InvalidSubTransactionId;
		}
	}

	/*
	 * Reset and recheck actions - cleaning memory should be done every time
	 * (when the variable with short life cycle was used) and then
	 * cannot be removed from xact action list.
	 */
	foreach(cur_item, xact_on_commit_reset_actions)
	{
		SVariableXActActionItem *xact_ai =
								  (SVariableXActActionItem *) lfirst(cur_item);

		if (!isCommit && xact_ai->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			xact_on_commit_reset_actions =
							foreach_delete_current(xact_on_commit_reset_actions, cur_item);
			pfree(xact_ai);
		}
		else
		{
			/* cur_item must be preserved */
			if (xact_ai->creating_subid == mySubid)
				xact_ai->creating_subid = parentSubid;
			if (xact_ai->deleting_subid == mySubid)
				xact_ai->deleting_subid = isCommit ? parentSubid : InvalidSubTransactionId;
		}
	}
}

/*
 * pg_debug_show_used_session_variables - designed for testing
 *
 * returns content of session vars
 */
Datum
pg_debug_show_used_session_variables(PG_FUNCTION_ARGS)
{
#define NUM_PG_DEBUG_SHOW_USED_SESSION_VARIABLES_ATTS 10

	SetSingleFuncCall(fcinfo, 0);

	if (sessionvars)
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		HASH_SEQ_STATUS status;
		SVariable svar;

		/* Ensure so all entries in sessionvars hash table are valid */
		sync_sessionvars_all();

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			Datum		values[NUM_PG_DEBUG_SHOW_USED_SESSION_VARIABLES_ATTS];
			bool		nulls[NUM_PG_DEBUG_SHOW_USED_SESSION_VARIABLES_ATTS];
			HeapTuple	tp;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[0] = ObjectIdGetDatum(svar->varid);

			/*
			 * Sessionvars can hold data of variables removed from catalogue,
			 * (and not purged) and then namespacename and name cannot be read
			 * from catalogue.
			 */
			values[1] = PointerGetDatum(cstring_to_text(svar->nsname));
			values[2] = PointerGetDatum(cstring_to_text(svar->name));
			values[3] = ObjectIdGetDatum(svar->svartype->typid);
			values[4] = PointerGetDatum(cstring_to_text(svar->svartype->typname));

			/* check if session variable is visible in system catalogue */
			tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));

			if (HeapTupleIsValid(tp))
			{
				ReleaseSysCache(tp);

				values[5] = BoolGetDatum(false);
				values[6] = BoolGetDatum(svar->is_valid);
				values[7] = BoolGetDatum(session_variable_use_valid_type(svar));
				values[8] = BoolGetDatum(pg_variable_aclcheck(svar->varid, GetUserId(), ACL_READ) == ACLCHECK_OK);
				values[9] = BoolGetDatum(pg_variable_aclcheck(svar->varid, GetUserId(), ACL_WRITE) == ACLCHECK_OK);
			}
			else
			{
				values[5] = BoolGetDatum(true);
				nulls[6] = true;
				nulls[7] = true;
				nulls[8] = true;
				nulls[9] = true;
			}

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	return (Datum) 0;
}
