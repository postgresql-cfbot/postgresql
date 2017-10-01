/*
 * spgistfuncs.c
 *		Functions to investigate the content of SP-GiST indexes
 *
 * Source code is based on Gevel module available at
 * (http://www.sai.msu.su/~megera/oddmuse/index.cgi/Gevel)
 * Originally developed by:
 *  - Oleg Bartunov <oleg@sai.msu.su>, Moscow, Moscow University, Russia
 *  - Teodor Sigaev <teodor@sigaev.ru>, Moscow, Moscow University, Russia
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pageinspect/spgistfuncs.c
 */

#include "postgres.h"

#include "access/gist_private.h"
#include "access/spgist_private.h"
#include "access/relscan.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/varlena.h"

typedef struct SPGistPrintStackElem
{
	ItemPointerData iptr;
	int16		nlabel;
	int			level;
}			SPGistPrintStackElem;

typedef struct SPGistPrint
{
	SpGistState state;
	Relation	index;
	Datum		dvalues[8 /* see CreateTemplateTupleDesc call */ ];
	bool		nulls[8 /* see CreateTemplateTupleDesc call */ ];
	List	   *stack;
}			SPGistPrint;

#define IsIndex(r) ((r)->rd_rel->relkind == RELKIND_INDEX)
#define IsSpGist(r) ((r)->rd_rel->relam == SPGIST_AM_OID)

static void
pushSPGistPrint(FuncCallContext *funcctx, SPGistPrint * prst, ItemPointer ip,
				int level)
{
	MemoryContext oldcontext;
	SPGistPrintStackElem *e;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	e = palloc(sizeof(*e));
	e->iptr = *ip;
	e->nlabel = 0;
	e->level = level;
	prst->stack = lcons(e, prst->stack);

	MemoryContextSwitchTo(oldcontext);
}

static void
close_spgist_print(SPGistPrint * prst)
{
	index_close(prst->index, AccessExclusiveLock);
}

PG_FUNCTION_INFO_V1(spgist_stats);
PG_FUNCTION_INFO_V1(spgist_print);

/*
 * spgist_stats
 *		Show statistics about SP-GiST tree.
 *
 * Usage:
 *		SELECT spgist_stats('spgist_idx');
 */
Datum
spgist_stats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Relation	index;
	BlockNumber blkno;
	BlockNumber totalPages = 0,
				innerPages = 0,
				leafPages = 0,
				emptyPages = 0,
				deletedPages = 0;
	double		usedSpace = 0.0,
				usedLeafSpace = 0.0,
				usedInnerSpace = 0.0;
	char		res[1024];
	int			bufferSize = -1;
	int64		innerTuples = 0,
				leafTuples = 0,
				nAllTheSame = 0,
				nLeafPlaceholder = 0,
				nInnerPlaceholder = 0,
				nLeafRedirect = 0,
				nInnerRedirect = 0;

	relvar = makeRangeVarFromNameList(textToQualifiedNameList(name));
	index = relation_openrv(relvar, AccessExclusiveLock);

	if (!IsIndex(index) || !IsSpGist(index))
		elog(ERROR, "relation \"%s\" is not an SPGiST index",
			 RelationGetRelationName(index));

	totalPages = RelationGetNumberOfBlocks(index);

	for (blkno = SPGIST_ROOT_BLKNO; blkno < totalPages; blkno++)
	{
		Buffer		buffer;
		Page		page;
		int			pageFree;

		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);

		if (PageIsNew(page) || SpGistPageIsDeleted(page))
		{
			deletedPages++;
			UnlockReleaseBuffer(buffer);
			continue;
		}

		if (SpGistPageIsLeaf(page))
		{
			leafPages++;
			leafTuples += PageGetMaxOffsetNumber(page);
			nLeafPlaceholder += SpGistPageGetOpaque(page)->nPlaceholder;
			nLeafRedirect += SpGistPageGetOpaque(page)->nRedirection;
		}
		else
		{
			int			i,
						max;

			innerPages++;
			max = PageGetMaxOffsetNumber(page);
			innerTuples += max;
			nInnerPlaceholder += SpGistPageGetOpaque(page)->nPlaceholder;
			nInnerRedirect += SpGistPageGetOpaque(page)->nRedirection;
			for (i = FirstOffsetNumber; i <= max; i++)
			{
				SpGistInnerTuple it;

				it = (SpGistInnerTuple) PageGetItem(page,
													PageGetItemId(page, i));
				if (it->allTheSame)
					nAllTheSame++;
			}
		}

		if (bufferSize < 0)
			bufferSize = BufferGetPageSize(buffer)
				- MAXALIGN(sizeof(SpGistPageOpaqueData))
				- SizeOfPageHeaderData;

		pageFree = PageGetExactFreeSpace(page);

		usedSpace += bufferSize - pageFree;
		if (SpGistPageIsLeaf(page))
			usedLeafSpace += bufferSize - pageFree;
		else
			usedInnerSpace += bufferSize - pageFree;

		if (pageFree == bufferSize)
			emptyPages++;

		UnlockReleaseBuffer(buffer);
	}

	index_close(index, AccessExclusiveLock);

	totalPages--;				/* discount metapage */

	snprintf(res, sizeof(res),
			 "totalPages:        %u\n"
			 "deletedPages:      %u\n"
			 "innerPages:        %u\n"
			 "leafPages:         %u\n"
			 "emptyPages:        %u\n"
			 "usedSpace:         %.2f kbytes\n"
			 "usedInnerSpace:    %.2f kbytes\n"
			 "usedLeafSpace:     %.2f kbytes\n"
			 "freeSpace:         %.2f kbytes\n"
			 "fillRatio:         %.2f%%\n"
			 "leafTuples:        " INT64_FORMAT "\n"
			 "innerTuples:       " INT64_FORMAT "\n"
			 "innerAllTheSame:   " INT64_FORMAT "\n"
			 "leafPlaceholders:  " INT64_FORMAT "\n"
			 "innerPlaceholders: " INT64_FORMAT "\n"
			 "leafRedirects:     " INT64_FORMAT "\n"
			 "innerRedirects:    " INT64_FORMAT,
			 totalPages, deletedPages, innerPages, leafPages, emptyPages,
			 usedSpace / 1024.0,
			 usedInnerSpace / 1024.0,
			 usedLeafSpace / 1024.0,
			 (((double) bufferSize) * ((double) totalPages) - usedSpace) / 1024,
			 100.0 * (usedSpace / (((double) bufferSize) * ((double) totalPages))),
			 leafTuples, innerTuples, nAllTheSame,
			 nLeafPlaceholder, nInnerPlaceholder,
			 nLeafRedirect, nInnerRedirect);

	PG_RETURN_TEXT_P(CStringGetTextDatum(res));
}

/*
 * spgist_print
 *		Prints objects stored in GiST tree, works only if objects in index have
 *		textual representation (type_out functions should be implemented for the
 *		given object type).
 */
Datum
spgist_print(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	SPGistPrint *prst;
	SPGistPrintStackElem *s;
	HeapTuple	htuple;
	Datum		result;
	MemoryContext oldcontext;

	if (SRF_IS_FIRSTCALL())
	{
		text	   *name = PG_GETARG_TEXT_P(0);
		RangeVar   *relvar;
		Relation	index;
		ItemPointerData ipd;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		relvar = makeRangeVarFromNameList(textToQualifiedNameList(name));
		index = relation_openrv(relvar, AccessExclusiveLock);

		if (!IsIndex(index) || !IsSpGist(index))
			elog(ERROR, "relation \"%s\" is not an SPGiST index",
				 RelationGetRelationName(index));

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		prst = palloc(sizeof(*prst));

		prst->index = index;
		initSpGistState(&prst->state, index);

		tupdesc = CreateTemplateTupleDesc(3 /* types */ + 1 /* level */ +
										  1 /* nlabel */ + 2 /* tids */ +
										  1, false);
		TupleDescInitEntry(tupdesc, 1, "tid", TIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, 2, "allthesame", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, 3, "node", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 4, "level", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, 5, "tid_pointer", TIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, 6, "prefix",
						   (prst->state.attPrefixType.type == VOIDOID) ? INT4OID
						   : prst->state.attPrefixType.type,
						   -1, 0);
		TupleDescInitEntry(tupdesc, 7, "label",
						   (prst->state.attLabelType.type == VOIDOID) ? INT4OID
						   : prst->state.attLabelType.type,
						   -1, 0);
		TupleDescInitEntry(tupdesc, 8, "leaf",
						   (prst->state.attType.type == VOIDOID) ? INT4OID
						   : prst->state.attType.type,
						   -1, 0);

		funcctx->slot = TupleDescGetSlot(tupdesc);
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		funcctx->user_fctx = (void *) prst;

		MemoryContextSwitchTo(oldcontext);

		ItemPointerSet(&ipd, SPGIST_ROOT_BLKNO, FirstOffsetNumber);
		prst->stack = NIL;
		pushSPGistPrint(funcctx, prst, &ipd, 1);

		PG_FREE_IF_COPY(name, 0);
	}

	funcctx = SRF_PERCALL_SETUP();
	prst = (SPGistPrint *) (funcctx->user_fctx);

next:
	for (;;)
	{
		if (prst->stack == NIL)
		{
			close_spgist_print(prst);
			SRF_RETURN_DONE(funcctx);
		}

		CHECK_FOR_INTERRUPTS();

		s = (SPGistPrintStackElem *) linitial(prst->stack);
		prst->stack = list_delete_first(prst->stack);

		if (ItemPointerIsValid(&s->iptr))
			break;
		free(s);
	}

	do
	{
		Buffer		buffer;
		Page		page;
		SpGistDeadTuple dtuple;
		ItemPointer tid;

		buffer = ReadBuffer(prst->index, ItemPointerGetBlockNumber(&s->iptr));
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		if (ItemPointerGetOffsetNumber(&s->iptr) > PageGetMaxOffsetNumber(page))
		{
			UnlockReleaseBuffer(buffer);
			pfree(s);
			goto next;
		}

		dtuple = (SpGistDeadTuple) PageGetItem(page,
											   PageGetItemId(page,
															 ItemPointerGetOffsetNumber(&s->iptr)));

		if (dtuple->tupstate != SPGIST_LIVE)
		{
			UnlockReleaseBuffer(buffer);
			pfree(s);
			goto next;
		}

		if (SpGistPageIsLeaf(page))
		{
			SpGistLeafTuple leafTuple = (SpGistLeafTuple) dtuple;

			tid = palloc(sizeof(ItemPointerData));
			*tid = s->iptr;
			prst->dvalues[0] = PointerGetDatum(tid);
			prst->nulls[0] = false;
			prst->nulls[1] = true;
			prst->nulls[2] = true;
			prst->dvalues[3] = s->level;
			prst->nulls[3] = false;
			prst->nulls[4] = true;
			prst->nulls[5] = true;
			prst->nulls[6] = true;
			prst->dvalues[7] = datumCopy(SGLTDATUM(leafTuple, &prst->state),
										 prst->state.attType.attbyval,
										 prst->state.attType.attlen);
			prst->nulls[7] = false;
		}
		else
		{
			SpGistInnerTuple innerTuple = (SpGistInnerTuple) dtuple;
			int			i;
			SpGistNodeTuple node;

			SGITITERATE(innerTuple, i, node)
			{
				if (ItemPointerIsValid(&node->t_tid))
				{
					if (i >= s->nlabel)
						break;
				}
			}

			if (i >= innerTuple->nNodes)
			{
				UnlockReleaseBuffer(buffer);
				pfree(s);
				goto next;

			}

			tid = palloc(sizeof(ItemPointerData));
			*tid = s->iptr;
			prst->dvalues[0] = PointerGetDatum(tid);
			prst->nulls[0] = false;
			prst->dvalues[1] = innerTuple->allTheSame;
			prst->nulls[1] = false;
			prst->dvalues[2] = Int32GetDatum(s->nlabel);
			prst->nulls[2] = false;
			prst->dvalues[3] = s->level;
			prst->nulls[3] = false;
			tid = palloc(sizeof(ItemPointerData));
			*tid = node->t_tid;
			prst->dvalues[4] = PointerGetDatum(tid);
			prst->nulls[4] = false;
			if (innerTuple->prefixSize > 0)
			{
				prst->dvalues[5] = datumCopy(SGITDATUM(innerTuple, &prst->state),
											 prst->state.attPrefixType.attbyval,
											 prst->state.attPrefixType.attlen);
				prst->nulls[5] = false;
			}
			else
				prst->nulls[5] = true;
			if (!IndexTupleHasNulls(node))
			{
				prst->dvalues[6] = datumCopy(SGNTDATUM(node, &prst->state),
											 prst->state.attLabelType.attbyval,
											 prst->state.attLabelType.attlen);
				prst->nulls[6] = false;
			}
			else
				prst->nulls[6] = true;
			prst->nulls[7] = true;

			pushSPGistPrint(funcctx, prst, &node->t_tid, s->level + 1);
			s->nlabel = i + 1;
			oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
			prst->stack = lcons(s, prst->stack);
			MemoryContextSwitchTo(oldcontext);
			s = NULL;
		}

		UnlockReleaseBuffer(buffer);
		if (s)
			pfree(s);
	} while (0);

	htuple = heap_form_tuple(funcctx->attinmeta->tupdesc,
							 prst->dvalues,
							 prst->nulls);
	result = TupleGetDatum(funcctx->slot, htuple);

	SRF_RETURN_NEXT(funcctx, result);
}
