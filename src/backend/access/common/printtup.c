/*-------------------------------------------------------------------------
 *
 * printtup.c
 *	  Routines to print out tuples to the destination (both frontend
 *	  clients and standalone backends are supported here).
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/printtup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/printtup.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/pg_colenckey.h"
#include "catalog/pg_colenckeydata.h"
#include "catalog/pg_colmasterkey.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "tcop/pquery.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static void printtup_startup(DestReceiver *self, int operation,
							 TupleDesc typeinfo);
static bool printtup(TupleTableSlot *slot, DestReceiver *self);
static void printtup_shutdown(DestReceiver *self);
static void printtup_destroy(DestReceiver *self);

/* ----------------------------------------------------------------
 *		printtup / debugtup support
 * ----------------------------------------------------------------
 */

/* ----------------
 *		Private state for a printtup destination object
 *
 * NOTE: finfo is the lookup info for either typoutput or typsend, whichever
 * we are using for this column.
 * ----------------
 */
typedef struct
{								/* Per-attribute information */
	Oid			typoutput;		/* Oid for the type's text output fn */
	Oid			typsend;		/* Oid for the type's binary output fn */
	bool		typisvarlena;	/* is it varlena (ie possibly toastable)? */
	int16		format;			/* format code for this column */
	FmgrInfo	finfo;			/* Precomputed call info for output fn */
} PrinttupAttrInfo;

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Portal		portal;			/* the Portal we are printing from */
	bool		sendDescrip;	/* send RowDescription at startup? */
	TupleDesc	attrinfo;		/* The attr info we are set up for */
	int			nattrs;
	PrinttupAttrInfo *myinfo;	/* Cached info about each attr */
	StringInfoData buf;			/* output buffer (*not* in tmpcontext) */
	MemoryContext tmpcontext;	/* Memory context for per-row workspace */
} DR_printtup;

/* ----------------
 *		Initialize: create a DestReceiver for printtup
 * ----------------
 */
DestReceiver *
printtup_create_DR(CommandDest dest)
{
	DR_printtup *self = (DR_printtup *) palloc0(sizeof(DR_printtup));

	self->pub.receiveSlot = printtup;	/* might get changed later */
	self->pub.rStartup = printtup_startup;
	self->pub.rShutdown = printtup_shutdown;
	self->pub.rDestroy = printtup_destroy;
	self->pub.mydest = dest;

	/*
	 * Send T message automatically if DestRemote, but not if
	 * DestRemoteExecute
	 */
	self->sendDescrip = (dest == DestRemote);

	self->attrinfo = NULL;
	self->nattrs = 0;
	self->myinfo = NULL;
	self->buf.data = NULL;
	self->tmpcontext = NULL;

	return (DestReceiver *) self;
}

/*
 * Set parameters for a DestRemote (or DestRemoteExecute) receiver
 */
void
SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)
{
	DR_printtup *myState = (DR_printtup *) self;

	Assert(myState->pub.mydest == DestRemote ||
		   myState->pub.mydest == DestRemoteExecute);

	myState->portal = portal;
}

static void
printtup_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_printtup *myState = (DR_printtup *) self;
	Portal		portal = myState->portal;

	/*
	 * Create I/O buffer to be used for all messages.  This cannot be inside
	 * tmpcontext, since we want to re-use it across rows.
	 */
	initStringInfo(&myState->buf);

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.
	 */
	myState->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
												"printtup",
												ALLOCSET_DEFAULT_SIZES);

	/*
	 * If we are supposed to emit row descriptions, then send the tuple
	 * descriptor of the tuples.
	 */
	if (myState->sendDescrip)
		SendRowDescriptionMessage(&myState->buf,
								  typeinfo,
								  FetchPortalTargetList(portal),
								  portal->formats);

	/* ----------------
	 * We could set up the derived attr info at this time, but we postpone it
	 * until the first call of printtup, for 2 reasons:
	 * 1. We don't waste time (compared to the old way) if there are no
	 *	  tuples at all to output.
	 * 2. Checking in printtup allows us to handle the case that the tuples
	 *	  change type midway through (although this probably can't happen in
	 *	  the current executor).
	 * ----------------
	 */
}

/*
 * Send ColumnMasterKey message, unless it's already been sent in this session
 * for this key.
 */
List	   *cmk_sent = NIL;

static void
cmk_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	list_free(cmk_sent);
	cmk_sent = NIL;
}

static void
MaybeSendColumnMasterKeyMessage(Oid cmkid)
{
	HeapTuple	tuple;
	Form_pg_colmasterkey cmkform;
	Datum		datum;
	bool		isnull;
	StringInfoData buf;
	static bool registered_inval = false;
	MemoryContext oldcontext;

	if (list_member_oid(cmk_sent, cmkid))
		return;

	tuple = SearchSysCache1(CMKOID, ObjectIdGetDatum(cmkid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for column master key %u", cmkid);
	cmkform = (Form_pg_colmasterkey) GETSTRUCT(tuple);

	pq_beginmessage(&buf, 'y'); /* ColumnMasterKey */
	pq_sendint32(&buf, cmkform->oid);
	pq_sendstring(&buf, NameStr(cmkform->cmkname));
	datum = SysCacheGetAttr(CMKOID, tuple, Anum_pg_colmasterkey_cmkrealm, &isnull);
	Assert(!isnull);
	pq_sendstring(&buf, TextDatumGetCString(datum));
	pq_endmessage(&buf);

	ReleaseSysCache(tuple);

	if (!registered_inval)
	{
		CacheRegisterSyscacheCallback(CMKOID, cmk_change_cb, (Datum) 0);
		registered_inval = true;
	}

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	cmk_sent = lappend_oid(cmk_sent, cmkid);
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Send ColumnEncryptionKey message, unless it's already been sent in this
 * session for this key.
 */
List	   *cek_sent = NIL;

static void
cek_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	list_free(cek_sent);
	cek_sent = NIL;
}

void
MaybeSendColumnEncryptionKeyMessage(Oid attcek)
{
	HeapTuple	tuple;
	ScanKeyData skey[1];
	SysScanDesc sd;
	Relation	rel;
	bool		found = false;
	static bool registered_inval = false;
	MemoryContext oldcontext;

	if (list_member_oid(cek_sent, attcek))
		return;

	ScanKeyInit(&skey[0],
				Anum_pg_colenckeydata_ckdcekid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attcek));
	rel = table_open(ColumnEncKeyDataRelationId, AccessShareLock);
	sd = systable_beginscan(rel, ColumnEncKeyCekidCmkidIndexId, true, NULL, 1, skey);

	while ((tuple = systable_getnext(sd)))
	{
		Form_pg_colenckeydata ckdform = (Form_pg_colenckeydata) GETSTRUCT(tuple);
		Datum		datum;
		bool		isnull;
		bytea	   *ba;
		StringInfoData buf;

		MaybeSendColumnMasterKeyMessage(ckdform->ckdcmkid);

		datum = heap_getattr(tuple, Anum_pg_colenckeydata_ckdencval, RelationGetDescr(rel), &isnull);
		Assert(!isnull);
		ba = pg_detoast_datum_packed((bytea *) DatumGetPointer(datum));

		pq_beginmessage(&buf, 'Y'); /* ColumnEncryptionKey */
		pq_sendint32(&buf, ckdform->ckdcekid);
		pq_sendint32(&buf, ckdform->ckdcmkid);
		pq_sendint16(&buf, ckdform->ckdcmkalg);
		pq_sendint32(&buf, VARSIZE_ANY_EXHDR(ba));
		pq_sendbytes(&buf, VARDATA_ANY(ba), VARSIZE_ANY_EXHDR(ba));
		pq_endmessage(&buf);

		found = true;
	}

	if (!found)
		elog(ERROR, "lookup failed for column encryption key data %u", attcek);

	systable_endscan(sd);
	table_close(rel, NoLock);

	if (!registered_inval)
	{
		CacheRegisterSyscacheCallback(CEKDATAOID, cek_change_cb, (Datum) 0);
		registered_inval = true;
	}

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	cek_sent = lappend_oid(cek_sent, attcek);
	MemoryContextSwitchTo(oldcontext);
}

/*
 * SendRowDescriptionMessage --- send a RowDescription message to the frontend
 *
 * Notes: the TupleDesc has typically been manufactured by ExecTypeFromTL()
 * or some similar function; it does not contain a full set of fields.
 * The targetlist will be NIL when executing a utility function that does
 * not have a plan.  If the targetlist isn't NIL then it is a Query node's
 * targetlist; it is up to us to ignore resjunk columns in it.  The formats[]
 * array pointer might be NULL (if we are doing Describe on a prepared stmt);
 * send zeroes for the format codes in that case.
 */
void
SendRowDescriptionMessage(StringInfo buf, TupleDesc typeinfo,
						  List *targetlist, int16 *formats)
{
	int			natts = typeinfo->natts;
	int			i;
	ListCell   *tlist_item = list_head(targetlist);

	/* tuple descriptor message type */
	pq_beginmessage_reuse(buf, 'T');
	/* # of attrs in tuples */
	pq_sendint16(buf, natts);

	/*
	 * Preallocate memory for the entire message to be sent. That allows to
	 * use the significantly faster inline pqformat.h functions and to avoid
	 * reallocations.
	 *
	 * Have to overestimate the size of the column-names, to account for
	 * character set overhead.
	 */
	enlargeStringInfo(buf, (NAMEDATALEN * MAX_CONVERSION_GROWTH /* attname */
							+ sizeof(Oid)	/* resorigtbl */
							+ sizeof(AttrNumber)	/* resorigcol */
							+ sizeof(Oid)	/* atttypid */
							+ sizeof(int16) /* attlen */
							+ sizeof(int32) /* attypmod */
							+ sizeof(int16) /* format */
							) * natts);

	for (i = 0; i < natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(typeinfo, i);
		Oid			atttypid = att->atttypid;
		int32		atttypmod = att->atttypmod;
		Oid			resorigtbl;
		AttrNumber	resorigcol;
		int16		format;
		Oid			attcekid = InvalidOid;
		int16		attencalg = 0;

		/*
		 * If column is a domain, send the base type and typmod instead.
		 * Lookup before sending any ints, for efficiency.
		 */
		atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);

		/* Do we have a non-resjunk tlist item? */
		while (tlist_item &&
			   ((TargetEntry *) lfirst(tlist_item))->resjunk)
			tlist_item = lnext(targetlist, tlist_item);
		if (tlist_item)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);

			resorigtbl = tle->resorigtbl;
			resorigcol = tle->resorigcol;
			tlist_item = lnext(targetlist, tlist_item);
		}
		else
		{
			/* No info available, so send zeroes */
			resorigtbl = 0;
			resorigcol = 0;
		}

		if (formats)
			format = formats[i];
		else
			format = 0;

		if (get_typtype(atttypid) == TYPTYPE_ENCRYPTED)
		{
			HeapTuple	tp;
			Form_pg_attribute orig_att;

			tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(resorigtbl), Int16GetDatum(resorigcol));
			if (!HeapTupleIsValid(tp))
				elog(ERROR, "cache lookup failed for attribute %d of relation %u", resorigcol, resorigtbl);
			orig_att = (Form_pg_attribute) GETSTRUCT(tp);
			MaybeSendColumnEncryptionKeyMessage(orig_att->attcek);
			atttypid = orig_att->attrealtypid;
			attcekid = orig_att->attcek;
			attencalg = orig_att->attencalg;
			ReleaseSysCache(tp);
		}

		pq_writestring(buf, NameStr(att->attname));
		pq_writeint32(buf, resorigtbl);
		pq_writeint16(buf, resorigcol);
		pq_writeint32(buf, atttypid);
		pq_writeint16(buf, att->attlen);
		pq_writeint32(buf, atttypmod);
		pq_writeint16(buf, format);
		pq_writeint32(buf, attcekid);
		pq_writeint16(buf, attencalg);
	}

	pq_endmessage_reuse(buf);
}

/*
 * Get the lookup info that printtup() needs
 */
static void
printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
{
	int16	   *formats = myState->portal->formats;
	int			i;

	/* get rid of any old data */
	if (myState->myinfo)
		pfree(myState->myinfo);
	myState->myinfo = NULL;

	myState->attrinfo = typeinfo;
	myState->nattrs = numAttrs;
	if (numAttrs <= 0)
		return;

	myState->myinfo = (PrinttupAttrInfo *)
		palloc0(numAttrs * sizeof(PrinttupAttrInfo));

	for (i = 0; i < numAttrs; i++)
	{
		PrinttupAttrInfo *thisState = myState->myinfo + i;
		int16		format = (formats ? formats[i] : 0);
		Form_pg_attribute attr = TupleDescAttr(typeinfo, i);

		thisState->format = format;
		if (format == 0)
		{
			getTypeOutputInfo(attr->atttypid,
							  &thisState->typoutput,
							  &thisState->typisvarlena);
			fmgr_info(thisState->typoutput, &thisState->finfo);
		}
		else if (format == 1)
		{
			getTypeBinaryOutputInfo(attr->atttypid,
									&thisState->typsend,
									&thisState->typisvarlena);
			fmgr_info(thisState->typsend, &thisState->finfo);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported format code: %d", format)));
	}
}

/* ----------------
 *		printtup --- send a tuple to the client
 * ----------------
 */
static bool
printtup(TupleTableSlot *slot, DestReceiver *self)
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	DR_printtup *myState = (DR_printtup *) self;
	MemoryContext oldcontext;
	StringInfo	buf = &myState->buf;
	int			natts = typeinfo->natts;
	int			i;

	/* Set or update my derived attribute info, if needed */
	if (myState->attrinfo != typeinfo || myState->nattrs != natts)
		printtup_prepare_info(myState, typeinfo, natts);

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/* Switch into per-row context so we can recover memory below */
	oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

	/*
	 * Prepare a DataRow message (note buffer is in per-row context)
	 */
	pq_beginmessage_reuse(buf, 'D');

	pq_sendint16(buf, natts);

	/*
	 * send the attributes of this tuple
	 */
	for (i = 0; i < natts; ++i)
	{
		PrinttupAttrInfo *thisState = myState->myinfo + i;
		Datum		attr = slot->tts_values[i];

		if (slot->tts_isnull[i])
		{
			pq_sendint32(buf, -1);
			continue;
		}

		/*
		 * Here we catch undefined bytes in datums that are returned to the
		 * client without hitting disk; see comments at the related check in
		 * PageAddItem().  This test is most useful for uncompressed,
		 * non-external datums, but we're quite likely to see such here when
		 * testing new C functions.
		 */
		if (thisState->typisvarlena)
			VALGRIND_CHECK_MEM_IS_DEFINED(DatumGetPointer(attr),
										  VARSIZE_ANY(attr));

		if (thisState->format == 0)
		{
			/* Text output */
			char	   *outputstr;

			outputstr = OutputFunctionCall(&thisState->finfo, attr);
			pq_sendcountedtext(buf, outputstr, strlen(outputstr), false);
		}
		else
		{
			/* Binary output */
			bytea	   *outputbytes;

			outputbytes = SendFunctionCall(&thisState->finfo, attr);
			pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
			pq_sendbytes(buf, VARDATA(outputbytes),
						 VARSIZE(outputbytes) - VARHDRSZ);
		}
	}

	pq_endmessage_reuse(buf);

	/* Return to caller's context, and flush row's temporary memory */
	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(myState->tmpcontext);

	return true;
}

/* ----------------
 *		printtup_shutdown
 * ----------------
 */
static void
printtup_shutdown(DestReceiver *self)
{
	DR_printtup *myState = (DR_printtup *) self;

	if (myState->myinfo)
		pfree(myState->myinfo);
	myState->myinfo = NULL;

	myState->attrinfo = NULL;

	if (myState->buf.data)
		pfree(myState->buf.data);
	myState->buf.data = NULL;

	if (myState->tmpcontext)
		MemoryContextDelete(myState->tmpcontext);
	myState->tmpcontext = NULL;
}

/* ----------------
 *		printtup_destroy
 * ----------------
 */
static void
printtup_destroy(DestReceiver *self)
{
	pfree(self);
}

/* ----------------
 *		printatt
 * ----------------
 */
static void
printatt(unsigned attributeId,
		 Form_pg_attribute attributeP,
		 char *value)
{
	printf("\t%2d: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n",
		   attributeId,
		   NameStr(attributeP->attname),
		   value != NULL ? " = \"" : "",
		   value != NULL ? value : "",
		   value != NULL ? "\"" : "",
		   (unsigned int) (attributeP->atttypid),
		   attributeP->attlen,
		   attributeP->atttypmod,
		   attributeP->attbyval ? 't' : 'f');
}

/* ----------------
 *		debugStartup - prepare to print tuples for an interactive backend
 * ----------------
 */
void
debugStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	int			natts = typeinfo->natts;
	int			i;

	/*
	 * show the return type of the tuples
	 */
	for (i = 0; i < natts; ++i)
		printatt((unsigned) i + 1, TupleDescAttr(typeinfo, i), NULL);
	printf("\t----\n");
}

/* ----------------
 *		debugtup - print one tuple for an interactive backend
 * ----------------
 */
bool
debugtup(TupleTableSlot *slot, DestReceiver *self)
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	int			natts = typeinfo->natts;
	int			i;
	Datum		attr;
	char	   *value;
	bool		isnull;
	Oid			typoutput;
	bool		typisvarlena;

	for (i = 0; i < natts; ++i)
	{
		attr = slot_getattr(slot, i + 1, &isnull);
		if (isnull)
			continue;
		getTypeOutputInfo(TupleDescAttr(typeinfo, i)->atttypid,
						  &typoutput, &typisvarlena);

		value = OidOutputFunctionCall(typoutput, attr);

		printatt((unsigned) i + 1, TupleDescAttr(typeinfo, i), value);
	}
	printf("\t----\n");

	return true;
}
