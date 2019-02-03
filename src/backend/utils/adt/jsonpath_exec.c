/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.c
 *	 Routines for SQL/JSON path execution.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/jsonpath.h"
#include "utils/varlena.h"

/* Standard error message for SQL/JSON errors */
#define ERRMSG_JSON_ARRAY_NOT_FOUND			"SQL/JSON array not found"
#define ERRMSG_JSON_OBJECT_NOT_FOUND		"SQL/JSON object not found"
#define ERRMSG_JSON_MEMBER_NOT_FOUND		"SQL/JSON member not found"
#define ERRMSG_JSON_NUMBER_NOT_FOUND		"SQL/JSON number not found"
#define ERRMSG_JSON_SCALAR_REQUIRED			"SQL/JSON scalar required"
#define ERRMSG_SINGLETON_JSON_ITEM_REQUIRED	"singleton SQL/JSON item required"
#define ERRMSG_NON_NUMERIC_JSON_ITEM		"non-numeric SQL/JSON item"
#define ERRMSG_INVALID_JSON_SUBSCRIPT		"invalid SQL/JSON subscript"
#define ERRMSG_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION	\
	"invalid argument for SQL/JSON datetime function"

/*
 * Represents "base object" and it's "id" for .keyvalue() evaluation.
 */
typedef struct JsonBaseObjectInfo
{
	JsonbContainer *jbc;
	int			id;
} JsonBaseObjectInfo;

/*
 * Special data structure representing stack of current items.  We use it
 * instead of regular list in order to evade extra memory allocation.  These
 * items are always allocated in local variables.
 */
typedef struct JsonItemStackEntry
{
	JsonbValue *item;
	struct JsonItemStackEntry *parent;
} JsonItemStackEntry;

typedef JsonItemStackEntry *JsonItemStack;

/*
 * Context of jsonpath execution.
 */
typedef struct JsonPathExecContext
{
	List	   *vars;			/* variables to substitute into jsonpath */
	JsonbValue *root;			/* for $ evaluation */
	JsonItemStack stack;		/* for @ evaluation */
	JsonBaseObjectInfo baseObject;	/* "base object" for .keyvalue()
									 * evaluation */
	int			lastGeneratedObjectId;	/* "id" counter for .keyvalue()
										 * evaluation */
	int			innermostArraySize; /* for LAST array index evaluation */
	bool		laxMode;		/* true for "lax" mode, false for "strict"
								 * mode */
	bool		ignoreStructuralErrors; /* with "true" structural errors such
										 * as absence of required json item or
										 * unexpected json item type are
										 * ignored */
	bool		throwErrors;	/* with "false" all suppressible errors are
								 * suppressed */
} JsonPathExecContext;

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#define jspStrictAbsenseOfErrors(cxt)	(!(cxt)->laxMode)
#define jspAutoUnwrap(cxt)				((cxt)->laxMode)
#define jspAutoWrap(cxt)				((cxt)->laxMode)
#define jspIgnoreStructuralErrors(cxt)	((cxt)->ignoreStructuralErrors)
#define jspThrowErrors(cxt)				((cxt)->throwErrors)

#define ThrowJsonPathError(code, detail) \
	ereport(ERROR, \
			 (errcode(ERRCODE_ ## code), \
			  errmsg(ERRMSG_ ## code), \
			  errdetail detail))

#define ThrowJsonPathErrorHint(code, detail, hint) \
	ereport(ERROR, \
			 (errcode(ERRCODE_ ## code), \
			  errmsg(ERRMSG_ ## code), \
			  errdetail detail, \
			  errhint hint))

#define ReturnJsonPathError(cxt, code, detail)	do { \
	if (jspThrowErrors(cxt)) \
		ThrowJsonPathError(code, detail); \
	else \
		return jperError; \
} while (0)

#define ReturnJsonPathErrorHint(cxt, code, detail, hint)	do { \
	if (jspThrowErrors(cxt)) \
		ThrowJsonPathErrorHint(code, detail, hint); \
	else \
		return jperError; \
} while (0)

typedef struct JsonValueListIterator
{
	ListCell   *lcell;
} JsonValueListIterator;

#define JsonValueListIteratorEnd ((ListCell *) -1)

static inline JsonPathExecResult recursiveExecute(JsonPathExecContext *cxt,
				 JsonPathItem *jsp, JsonbValue *jb,
				 JsonValueList *found);

static JsonPathExecResult recursiveExecuteUnwrapArray(JsonPathExecContext *cxt,
							JsonPathItem *jsp, JsonbValue *jb,
							JsonValueList *found);


static inline void
JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv)
{
	if (jvl->singleton)
	{
		jvl->list = list_make2(jvl->singleton, jbv);
		jvl->singleton = NULL;
	}
	else if (!jvl->list)
		jvl->singleton = jbv;
	else
		jvl->list = lappend(jvl->list, jbv);
}

static inline int
JsonValueListLength(const JsonValueList *jvl)
{
	return jvl->singleton ? 1 : list_length(jvl->list);
}

static inline bool
JsonValueListIsEmpty(JsonValueList *jvl)
{
	return !jvl->singleton && list_length(jvl->list) <= 0;
}

static inline JsonbValue *
JsonValueListHead(JsonValueList *jvl)
{
	return jvl->singleton ? jvl->singleton : linitial(jvl->list);
}

static inline List *
JsonValueListGetList(JsonValueList *jvl)
{
	if (jvl->singleton)
		return list_make1(jvl->singleton);

	return jvl->list;
}

/*
 * Get the next item from the sequence advancing iterator.
 */
static inline JsonbValue *
JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)
{
	if (it->lcell == JsonValueListIteratorEnd)
		return NULL;

	if (it->lcell)
		it->lcell = lnext(it->lcell);
	else
	{
		if (jvl->singleton)
		{
			it->lcell = JsonValueListIteratorEnd;
			return jvl->singleton;
		}

		it->lcell = list_head(jvl->list);
	}

	if (!it->lcell)
	{
		it->lcell = JsonValueListIteratorEnd;
		return NULL;
	}

	return lfirst(it->lcell);
}

/*
 * Initialize a binary JsonbValue with the given jsonb container.
 */
static inline JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = &jb->root;
	jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

	return jbv;
}

/*
 * Returns jbv* type of of JsonbValue. Note, it never returns jbvBinary as is.
 */
static inline int
JsonbType(JsonbValue *jb)
{
	int			type = jb->type;

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = (void *) jb->val.binary.data;

		/* Scalars should be always extracted during jsonpath execution. */
		Assert(!JsonContainerIsScalar(jbc));

		if (JsonContainerIsObject(jbc))
			type = jbvObject;
		else if (JsonContainerIsArray(jbc))
			type = jbvArray;
		else
			elog(ERROR, "invalid jsonb container type: 0x%08x", jbc->header);
	}

	return type;
}

static inline void
pushJsonItem(JsonItemStack *stack, JsonItemStackEntry *entry,
			 JsonbValue *item)
{
	entry->item = item;
	entry->parent = *stack;
	*stack = entry;
}

static inline void
popJsonItem(JsonItemStack *stack)
{
	*stack = (*stack)->parent;
}

static inline JsonbValue *
getScalar(JsonbValue *scalar, JsonbValue *buf, int type)
{
	/* Scalars should be always extracted during jsonpath execution. */
	Assert(scalar->type != jbvBinary ||
		   !JsonContainerIsScalar(scalar->val.binary.data));

	return scalar->type == type ? scalar : NULL;
}

/* Construct a JSON array from the item list */
static inline JsonbValue *
wrapItemsInArray(const JsonValueList *items)
{
	JsonbParseState *ps = NULL;
	JsonValueListIterator it = {0};
	JsonbValue *jbv;

	pushJsonbValue(&ps, WJB_BEGIN_ARRAY, NULL);

	while ((jbv = JsonValueListNext(items, &it)))
		pushJsonbValue(&ps, WJB_ELEM, jbv);

	return pushJsonbValue(&ps, WJB_END_ARRAY, NULL);
}

/********************Execute functions for JsonPath**************************/

/*
 * Find value of jsonpath variable in a list of passing params
 */
static int
computeJsonPathVariable(JsonPathItem *variable, List *vars, JsonbValue *value)
{
	ListCell   *cell;
	JsonPathVariable *var = NULL;
	bool		isNull;
	Datum		computedValue;
	char	   *varName;
	int			varNameLength;
	int			varId = 1;

	Assert(variable->type == jpiVariable);
	varName = jspGetString(variable, &varNameLength);

	foreach(cell, vars)
	{
		var = (JsonPathVariable *) lfirst(cell);

		if (varNameLength == VARSIZE_ANY_EXHDR(var->varName) &&
			!strncmp(varName, VARDATA_ANY(var->varName), varNameLength))
			break;

		var = NULL;
		varId++;
	}

	if (var == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cannot find jsonpath variable '%s'",
						pnstrdup(varName, varNameLength))));

	computedValue = var->cb(var->cb_arg, &isNull);

	if (isNull)
	{
		value->type = jbvNull;
		return varId;
	}

	switch (var->typid)
	{
		case BOOLOID:
			value->type = jbvBool;
			value->val.boolean = DatumGetBool(computedValue);
			break;
		case NUMERICOID:
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(computedValue);
			break;
			break;
		case TEXTOID:
		case VARCHAROID:
			value->type = jbvString;
			value->val.string.val = VARDATA_ANY(computedValue);
			value->val.string.len = VARSIZE_ANY_EXHDR(computedValue);
			break;
		case INTERNALOID:		/* raw JsonbValue */
			*value = *(JsonbValue *) DatumGetPointer(computedValue);
			Assert(IsAJsonbScalar(value) ||
				   (value->type == jbvBinary &&
					!JsonContainerIsScalar(value->val.binary.data)));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid parameter value"),
					 errdetail("Only bool, numeric and text types could be"
							   "casted to supported jsonpath types.")));
	}

	return varId;
}

/*
 * Convert jsonpath's scalar or variable node to actual jsonb value.
 *
 * If node is a variable then its id returned, otherwise 0 returned.
 */
static int
computeJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item,
					JsonbValue *value)
{
	switch (item->type)
	{
		case jpiNull:
			value->type = jbvNull;
			break;
		case jpiBool:
			value->type = jbvBool;
			value->val.boolean = jspGetBool(item);
			break;
		case jpiNumeric:
			value->type = jbvNumeric;
			value->val.numeric = jspGetNumeric(item);
			break;
		case jpiString:
			value->type = jbvString;
			value->val.string.val = jspGetString(item,
												 &value->val.string.len);
			break;
		case jpiVariable:
			return computeJsonPathVariable(item, cxt->vars, value);
		default:
			elog(ERROR, "unexpected jsonpath item type");
	}

	return 0;
}


/*
 * Get the type name of a SQL/JSON item.
 */
static const char *
JsonbTypeName(JsonbValue *jb)
{
	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = jb->val.binary.data;

		Assert(!JsonContainerIsScalar(jbc));

		if (JsonContainerIsArray(jbc))
			return "array";
		else if (JsonContainerIsObject(jbc))
			return "object";
		else
			elog(ERROR, "invalid jsonb container type: 0x%08x", jbc->header);
	}

	switch (jb->type)
	{
		case jbvNumeric:
			return "number";
		case jbvString:
			return "string";
		case jbvBool:
			return "boolean";
		case jbvNull:
			return "null";
		case jbvDatetime:
			switch (jb->val.datetime.typid)
			{
				case DATEOID:
					return "date";
				case TIMEOID:
					return "time without time zone";
				case TIMETZOID:
					return "time with time zone";
				case TIMESTAMPOID:
					return "timestamp without time zone";
				case TIMESTAMPTZOID:
					return "timestamp with time zone";
				default:
					elog(ERROR, "unrecognized jsonb value datetime "
						 "type oid %d",
						 jb->val.datetime.typid);
			}
			return "unknown";
		default:
			elog(ERROR, "unrecognized jsonb value type: %d", jb->type);
			return "unknown";
	}
}

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
static int
JsonbArraySize(JsonbValue *jb)
{
	Assert(jb->type != jbvArray);

	if (jb->type == jbvBinary)
	{
		JsonbContainer *jbc = jb->val.binary.data;

		if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
			return JsonContainerSize(jbc);
	}

	return -1;
}

/*
 * Compare two numerics.
 */
static int
compareNumeric(Numeric a, Numeric b)
{
	return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
											 PointerGetDatum(a),
											 PointerGetDatum(b)
											 ));
}

/*
 * Cross-type comparison of two datetime SQL/JSON items.  If items are
 * uncomparable, 'error' flag is set.
 */
static int
compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2, bool *error)
{
	PGFunction cmpfunc = NULL;

	switch (typid1)
	{
		case DATEOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = date_cmp;

					break;

				case TIMESTAMPOID:
					cmpfunc = date_cmp_timestamp;

					break;

				case TIMESTAMPTZOID:
					cmpfunc = date_cmp_timestamptz;

					break;

				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMEOID:
			switch (typid2)
			{
				case TIMEOID:
					cmpfunc = time_cmp;

					break;

				case TIMETZOID:
					val1 = DirectFunctionCall1(time_timetz, val1);
					cmpfunc = timetz_cmp;

					break;

				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMETZOID:
			switch (typid2)
			{
				case TIMEOID:
					val2 = DirectFunctionCall1(time_timetz, val2);
					cmpfunc = timetz_cmp;

					break;

				case TIMETZOID:
					cmpfunc = timetz_cmp;

					break;

				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMESTAMPOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = timestamp_cmp_date;

					break;

				case TIMESTAMPOID:
					cmpfunc = timestamp_cmp;

					break;

				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp_timestamptz;

					break;

				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		case TIMESTAMPTZOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = timestamptz_cmp_date;

					break;

				case TIMESTAMPOID:
					cmpfunc = timestamptz_cmp_timestamp;

					break;

				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp;

					break;

				case TIMEOID:
				case TIMETZOID:
					*error = true;
					return 0;
			}
			break;

		default:
			elog(ERROR, "unrecognized SQL/JSON datetime type oid: %d",
				 typid1);
	}

	if (!cmpfunc)
		elog(ERROR, "unrecognized SQL/JSON datetime type oid: %d",
			 typid2);

	*error = false;

	return DatumGetInt32(DirectFunctionCall2(cmpfunc, val1, val2));
}

/*
 * Compare two SQL/JSON items using comparison operation 'op'.
 */
static JsonPathBool
compareItems(int32 op, JsonbValue *jb1, JsonbValue *jb2)
{
	int			cmp;
	bool		res;

	if (jb1->type != jb2->type)
	{
		if (jb1->type == jbvNull || jb2->type == jbvNull)

			/*
			 * Equality and order comparison of nulls to non-nulls returns
			 * always false, but inequality comparison returns true.
			 */
			return op == jpiNotEqual ? jpbTrue : jpbFalse;

		/* Non-null items of different types are not comparable. */
		return jpbUnknown;
	}

	switch (jb1->type)
	{
		case jbvNull:
			cmp = 0;
			break;
		case jbvBool:
			cmp = jb1->val.boolean == jb2->val.boolean ? 0 :
				jb1->val.boolean ? 1 : -1;
			break;
		case jbvNumeric:
			cmp = compareNumeric(jb1->val.numeric, jb2->val.numeric);
			break;
		case jbvString:
			cmp = varstr_cmp(jb1->val.string.val, jb1->val.string.len,
							 jb2->val.string.val, jb2->val.string.len,
							 DEFAULT_COLLATION_OID);
			break;
		case jbvDatetime:
			{
				bool		error;

				cmp = compareDatetime(jb1->val.datetime.value,
									  jb1->val.datetime.typid,
									  jb2->val.datetime.value,
									  jb2->val.datetime.typid,
									  &error);

				if (error)
					return jpbUnknown;
			}
			break;

		case jbvBinary:
		case jbvArray:
		case jbvObject:
			return jpbUnknown;	/* non-scalars are not comparable */

		default:
			elog(ERROR, "invalid jsonb value type %d", jb1->type);
	}

	switch (op)
	{
		case jpiEqual:
			res = (cmp == 0);
			break;
		case jpiNotEqual:
			res = (cmp != 0);
			break;
		case jpiLess:
			res = (cmp < 0);
			break;
		case jpiGreater:
			res = (cmp > 0);
			break;
		case jpiLessOrEqual:
			res = (cmp <= 0);
			break;
		case jpiGreaterOrEqual:
			res = (cmp >= 0);
			break;
		default:
			elog(ERROR, "unrecognized jsonpath operation: %d", op);
			return jpbUnknown;
	}

	return res ? jpbTrue : jpbFalse;
}

/* Comparison predicate callback. */
static inline JsonPathBool
executeComparison(JsonPathItem *cmp, JsonbValue *lv, JsonbValue *rv, void *p)
{
	return compareItems(cmp->type, lv, rv);
}

static JsonbValue *
copyJsonbValue(JsonbValue *src)
{
	JsonbValue *dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
}

/*
 * Execute next jsonpath item if it does exist.
 */
static inline JsonPathExecResult
recursiveExecuteNext(JsonPathExecContext *cxt,
					 JsonPathItem *cur, JsonPathItem *next,
					 JsonbValue *v, JsonValueList *found, bool copy)
{
	JsonPathItem elem;
	bool		hasNext;

	if (!cur)
		hasNext = next != NULL;
	else if (next)
		hasNext = jspHasNext(cur);
	else
	{
		next = &elem;
		hasNext = jspGetNext(cur, next);
	}

	if (hasNext)
		return recursiveExecute(cxt, next, v, found);

	if (found)
		JsonValueListAppend(found, copy ? copyJsonbValue(v) : v);

	return jperOk;
}

/*
 * Execute jsonpath expression and automatically unwrap each array item from
 * the resulting sequence in lax mode.
 */
static inline JsonPathExecResult
recursiveExecuteAndUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
						  JsonbValue *jb, bool unwrap, JsonValueList *found)
{
	if (unwrap && jspAutoUnwrap(cxt))
	{
		JsonValueList seq = {0};
		JsonValueListIterator it = {0};
		JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &seq);
		JsonbValue *item;

		if (jperIsError(res))
			return res;

		while ((item = JsonValueListNext(&seq, &it)))
		{
			Assert(item->type != jbvArray);

			if (item->type == jbvBinary &&
				JsonContainerIsArray(item->val.binary.data))
			{
				JsonbValue	elem;
				JsonbIterator *it = JsonbIteratorInit(item->val.binary.data);
				JsonbIteratorToken tok;

				while ((tok = JsonbIteratorNext(&it, &elem, true)) != WJB_DONE)
				{
					if (tok == WJB_ELEM)
						JsonValueListAppend(found, copyJsonbValue(&elem));
				}
			}
			else
				JsonValueListAppend(found, item);
		}

		return jperOk;
	}

	return recursiveExecute(cxt, jsp, jb, found);
}

static inline JsonPathExecResult
recursiveExecuteAndUnwrapNoThrow(JsonPathExecContext *cxt, JsonPathItem *jsp,
								 JsonbValue *jb, bool unwrap,
								 JsonValueList *found)
{
	JsonPathExecResult res;
	bool		throwErrors = cxt->throwErrors;

	cxt->throwErrors = false;
	res = recursiveExecuteAndUnwrap(cxt, jsp, jb, unwrap, found);
	cxt->throwErrors = throwErrors;

	return res;
}

typedef JsonPathBool (*JsonPathPredicateCallback) (JsonPathItem *jsp,
												   JsonbValue *larg,
												   JsonbValue *rarg,
												   void *param);

/*
 * Execute unary or binary predicate.
 *
 * Predicates have existence semantics, because their operands are item
 * sequences.  Pairs of items from the left and right operand's sequences are
 * checked.  TRUE returned only if any pair satisfying the condition is found.
 * In strict mode, even if the desired pair has already been found, all pairs
 * still need to be examined to check the absence of errors.  If any error
 * occurs, UNKNOWN (analogous to SQL NULL) is returned.
 */
static inline JsonPathBool
executePredicate(JsonPathExecContext *cxt, JsonPathItem *pred,
				 JsonPathItem *larg, JsonPathItem *rarg, JsonbValue *jb,
				 bool unwrapRightArg, JsonPathPredicateCallback exec,
				 void *param)
{
	JsonPathExecResult res;
	JsonValueListIterator lseqit = {0};
	JsonValueList lseq = {0};
	JsonValueList rseq = {0};
	JsonbValue *lval;
	bool		error = false;
	bool		found = false;

	/* Left argument is always auto-unwrapped. */
	res = recursiveExecuteAndUnwrapNoThrow(cxt, larg, jb, true, &lseq);
	if (jperIsError(res))
		return jpbUnknown;

	if (rarg)
	{
		/* Right argument is conditionally auto-unwrapped. */
		res = recursiveExecuteAndUnwrapNoThrow(cxt, rarg, jb, unwrapRightArg,
											   &rseq);
		if (jperIsError(res))
			return jpbUnknown;
	}

	while ((lval = JsonValueListNext(&lseq, &lseqit)))
	{
		JsonValueListIterator rseqit;
		JsonbValue *rval;
		int			i = 0;

		if (rarg)
			memset(&rseqit, 0, sizeof(rseqit));
		else
			rval = NULL;

		/* Loop only if we have right arg sequence. */
		while (rarg ? !!(rval = JsonValueListNext(&rseq, &rseqit)) : !i++)
		{
			JsonPathBool res = exec(pred, lval, rval, param);

			if (res == jpbUnknown)
			{
				if (jspStrictAbsenseOfErrors(cxt))
					return jpbUnknown;

				error = true;
			}
			else if (res == jpbTrue)
			{
				if (!jspStrictAbsenseOfErrors(cxt))
					return jpbTrue;

				found = true;
			}
		}
	}

	if (found)					/* possible only in strict mode */
		return jpbTrue;

	if (error)					/* possible only in lax mode */
		return jpbUnknown;

	return jpbFalse;
}

/*
 * Execute binary arithmetic expression on singleton numeric operands.
 * Array operands are automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeBinaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
						JsonbValue *jb, PGFunction func, JsonValueList *found)
{
	MemoryContext mcxt = CurrentMemoryContext;
	JsonPathExecResult jper;
	JsonPathItem elem;
	JsonValueList lseq = {0};
	JsonValueList rseq = {0};
	JsonbValue *lval;
	JsonbValue *rval;
	JsonbValue	lvalbuf;
	JsonbValue	rvalbuf;
	Datum		ldatum;
	Datum		rdatum;
	Datum		res;

	jspGetLeftArg(jsp, &elem);

	/*
	 * XXX: By standard only operands of multiplicative expressions are
	 * unwrapped.  We extend it to other binary arithmetics expressions too.
	 */
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, true, &lseq);
	if (jperIsError(jper))
		return jper;

	jspGetRightArg(jsp, &elem);

	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, true, &rseq);
	if (jperIsError(jper))
		return jper;

	if (JsonValueListLength(&lseq) != 1 ||
		!(lval = getScalar(JsonValueListHead(&lseq), &lvalbuf, jbvNumeric)))
		ReturnJsonPathError(cxt, SINGLETON_JSON_ITEM_REQUIRED,
							("left operand of binary jsonpath operator %s "
							 "is not a singleton numeric value",
							 jspOperationName(jsp->type)));

	if (JsonValueListLength(&rseq) != 1 ||
		!(rval = getScalar(JsonValueListHead(&rseq), &rvalbuf, jbvNumeric)))
		ReturnJsonPathError(cxt, SINGLETON_JSON_ITEM_REQUIRED,
							("right operand of binary jsonpath operator %s "
							 "is not a singleton numeric value",
							 jspOperationName(jsp->type)));

	ldatum = NumericGetDatum(lval->val.numeric);
	rdatum = NumericGetDatum(rval->val.numeric);

	/*
	 * It is safe to use here PG_TRY/PG_CATCH without subtransaction because
	 * no function called inside performs data modification.
	 */
	PG_TRY();
	{
		res = DirectFunctionCall2(func, ldatum, rdatum);
	}
	PG_CATCH();
	{
		int			errcode = geterrcode();

		if (jspThrowErrors(cxt) ||
			ERRCODE_TO_CATEGORY(errcode) != ERRCODE_DATA_EXCEPTION)
			PG_RE_THROW();

		MemoryContextSwitchTo(mcxt);
		FlushErrorState();

		return jperError;
	}
	PG_END_TRY();

	if (!jspGetNext(jsp, &elem) && !found)
		return jperOk;

	lval = palloc(sizeof(*lval));
	lval->type = jbvNumeric;
	lval->val.numeric = DatumGetNumeric(res);

	return recursiveExecuteNext(cxt, jsp, &elem, lval, found, false);
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, PGFunction func, JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathExecResult jper2;
	JsonPathItem elem;
	JsonValueList seq = {0};
	JsonValueListIterator it = {0};
	JsonbValue *val;
	bool		hasNext;

	jspGetArg(jsp, &elem);
	jper = recursiveExecuteAndUnwrap(cxt, &elem, jb, true, &seq);

	if (jperIsError(jper))
		return jper;

	jper = jperNotFound;

	hasNext = jspGetNext(jsp, &elem);

	while ((val = JsonValueListNext(&seq, &it)))
	{
		if ((val = getScalar(val, val, jbvNumeric)))
		{
			if (!found && !hasNext)
				return jperOk;
		}
		else
		{
			if (!found && !hasNext)
				continue;		/* skip non-numerics processing */

			ReturnJsonPathError(cxt, JSON_NUMBER_NOT_FOUND,
								("operand of unary jsonpath operator %s "
								 "is not a numeric value",
								 jspOperationName(jsp->type)));
		}

		if (func)
			val->val.numeric =
				DatumGetNumeric(DirectFunctionCall1(func,
													NumericGetDatum(val->val.numeric)));

		jper2 = recursiveExecuteNext(cxt, jsp, &elem, val, found, false);

		if (jperIsError(jper2))
			return jper2;

		if (jper2 == jperOk)
		{
			if (!found)
				return jperOk;
			jper = jperOk;
		}
	}

	return jper;
}

/*
 * implements jpiAny node (** operator)
 */
static JsonPathExecResult
recursiveAny(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			 JsonValueList *found, uint32 level, uint32 first, uint32 last)
{
	JsonPathExecResult res = jperNotFound;
	JsonbIterator *it;
	int32		r;
	JsonbValue	v;

	check_stack_depth();

	if (level > last)
		return res;

	it = JsonbIteratorInit(jb->val.binary.data);

	/*
	 * Recursively iterate over jsonb objects/arrays
	 */
	while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			r = JsonbIteratorNext(&it, &v, true);
			Assert(r == WJB_VALUE);
		}

		if (r == WJB_VALUE || r == WJB_ELEM)
		{

			if (level >= first ||
				(first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
				 v.type != jbvBinary))	/* leaves only requested */
			{
				/* check expression */
				bool		savedIgnoreStructuralErrors;

				savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
				cxt->ignoreStructuralErrors = true;
				res = recursiveExecuteNext(cxt, NULL, jsp, &v, found, true);
				cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;

				if (jperIsError(res))
					break;

				if (res == jperOk && !found)
					break;
			}

			if (level < last && v.type == jbvBinary)
			{
				res = recursiveAny(cxt, jsp, &v, found, level + 1,
								   first, last);

				if (jperIsError(res))
					break;

				if (res == jperOk && found == NULL)
					break;
			}
		}
	}

	return res;
}

/*
 * Execute array subscript expression and convert resulting numeric item to
 * the integer type with truncation.
 */
static JsonPathExecResult
getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			  int32 *index)
{
	JsonbValue *jbv;
	JsonValueList found = {0};
	JsonbValue	tmp;
	JsonPathExecResult res = recursiveExecute(cxt, jsp, jb, &found);

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&found) != 1 ||
		!(jbv = getScalar(JsonValueListHead(&found), &tmp, jbvNumeric)))
		ReturnJsonPathError(cxt, INVALID_JSON_SUBSCRIPT,
							("jsonpath array subscript is not a "
							 "singleton numeric value"));

	*index = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											   DirectFunctionCall2(numeric_trunc,
																   NumericGetDatum(jbv->val.numeric),
																   Int32GetDatum(0))));

	return jperOk;
}

/*
 * STARTS_WITH predicate callback.
 *
 * Check if the 'whole' string starts from 'initial' string.
 */
static inline JsonPathBool
executeStartsWith(JsonPathItem *jsp, JsonbValue *whole, JsonbValue *initial,
				  void *param)
{
	JsonbValue	wholeBuf;
	JsonbValue	initialBuf;

	if (!(whole = getScalar(whole, &wholeBuf, jbvString)))
		return jpbUnknown;		/* error */

	if (!(initial = getScalar(initial, &initialBuf, jbvString)))
		return jpbUnknown;		/* error */

	if (whole->val.string.len >= initial->val.string.len &&
		!memcmp(whole->val.string.val,
				initial->val.string.val,
				initial->val.string.len))
		return jpbTrue;

	return jpbFalse;
}

/* Context for LIKE_REGEX execution. */
typedef struct JsonLikeRegexContext
{
	text	   *regex;
	int			cflags;
} JsonLikeRegexContext;

/*
 * LIKE_REGEX predicate callback.
 *
 * Check if the string matches regex pattern.
 */
static JsonPathBool
executeLikeRegex(JsonPathItem *jsp, JsonbValue *str, JsonbValue *rarg,
				 void *param)
{
	JsonLikeRegexContext *cxt = param;
	JsonbValue	strbuf;

	if (!(str = getScalar(str, &strbuf, jbvString)))
		return jpbUnknown;

	/* Cache regex text and converted flags. */
	if (!cxt->regex)
	{
		uint32		flags = jsp->content.like_regex.flags;

		cxt->regex =
			cstring_to_text_with_len(jsp->content.like_regex.pattern,
									 jsp->content.like_regex.patternlen);

		/* Convert regex flags. */
		cxt->cflags = REG_ADVANCED;

		if (flags & JSP_REGEX_ICASE)
			cxt->cflags |= REG_ICASE;
		if (flags & JSP_REGEX_MLINE)
			cxt->cflags |= REG_NEWLINE;
		if (flags & JSP_REGEX_SLINE)
			cxt->cflags &= ~REG_NEWLINE;
		if (flags & JSP_REGEX_WSPACE)
			cxt->cflags |= REG_EXPANDED;
	}

	if (RE_compile_and_execute(cxt->regex, str->val.string.val,
							   str->val.string.len,
							   cxt->cflags, DEFAULT_COLLATION_OID, 0, NULL))
		return jpbTrue;

	return jpbFalse;
}

static JsonPathExecResult
executeNumericItemMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
						 JsonbValue *jb, bool unwrap, PGFunction func,
						 JsonValueList *found)
{
	JsonPathItem next;
	JsonbValue	jbvbuf;
	Datum		datum;

	if (unwrap && JsonbType(jb) == jbvArray)
		return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

	if (!(jb = getScalar(jb, &jbvbuf, jbvNumeric)))
		ReturnJsonPathError(cxt, NON_NUMERIC_JSON_ITEM,
							("jsonpath item method .%s() is applied to "
							 "not a numeric value",
							 jspOperationName(jsp->type)));

	datum = NumericGetDatum(jb->val.numeric);
	datum = DirectFunctionCall1(func, datum);

	if (!jspGetNext(jsp, &next) && !found)
		return jperOk;

	jb = palloc(sizeof(*jb));
	jb->type = jbvNumeric;
	jb->val.numeric = DatumGetNumeric(datum);

	return recursiveExecuteNext(cxt, jsp, &next, jb, found, false);
}

/*
 * Try to parse datetime text with the specified datetime template and
 * default time-zone 'tzname'.
 * Returns 'value' datum, its type 'typid' and 'typmod'.
 * Datetime error is rethrown with SQL/JSON errcode if 'throwErrors' is true.
 */
static bool
tryToParseDatetime(text *fmt, text *datetime, char *tzname, bool strict,
				   Datum *value, Oid *typid, int32 *typmod, int *tz,
				   bool throwErrors)
{
	MemoryContext mcxt = CurrentMemoryContext;
	bool		ok = false;

	/*
	 * It is safe to use here PG_TRY/PG_CATCH without subtransaction because
	 * no function called inside performs data modification.
	 */
	PG_TRY();
	{
		*value = to_datetime(datetime, fmt, tzname, strict,
							 typid, typmod, tz);
		ok = true;
	}
	PG_CATCH();
	{
		if (ERRCODE_TO_CATEGORY(geterrcode()) != ERRCODE_DATA_EXCEPTION)
			PG_RE_THROW();

		if (throwErrors)
		{
			/*
			 * Save original datetime error message, details and hint, just
			 * replace errcode with a SQL/JSON one.
			 */
			errcode(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION);
			PG_RE_THROW();
		}

		MemoryContextSwitchTo(mcxt);
		FlushErrorState();
	}
	PG_END_TRY();

	return ok;
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next jsonpath.
 */
static inline JsonPathExecResult
appendBoolResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
				 JsonValueList *found, JsonPathBool res)
{
	JsonPathItem next;
	JsonbValue	jbv;

	if (!jspGetNext(jsp, &next) && !found)
		return jperOk;			/* found singleton boolean value */

	if (res == jpbUnknown)
	{
		jbv.type = jbvNull;
	}
	else
	{
		jbv.type = jbvBool;
		jbv.val.boolean = res == jpbTrue;
	}

	return recursiveExecuteNext(cxt, jsp, &next, &jbv, found, true);
}

/* Execute boolean-valued jsonpath expression. */
static inline JsonPathBool
recursiveExecuteBool(JsonPathExecContext *cxt, JsonPathItem *jsp,
					 JsonbValue *jb, bool canHaveNext)
{
	JsonPathItem larg;
	JsonPathItem rarg;
	JsonPathBool res;
	JsonPathBool res2;

	if (!canHaveNext && jspHasNext(jsp))
		elog(ERROR, "boolean jsonpath item cannot have next item");

	switch (jsp->type)
	{
		case jpiAnd:
			jspGetLeftArg(jsp, &larg);
			res = recursiveExecuteBool(cxt, &larg, jb, false);

			if (res == jpbFalse)
				return jpbFalse;

			/*
			 * SQL/JSON says that we should check second arg in case of
			 * jperError
			 */

			jspGetRightArg(jsp, &rarg);
			res2 = recursiveExecuteBool(cxt, &rarg, jb, false);

			return res2 == jpbTrue ? res : res2;

		case jpiOr:
			jspGetLeftArg(jsp, &larg);
			res = recursiveExecuteBool(cxt, &larg, jb, false);

			if (res == jpbTrue)
				return jpbTrue;

			jspGetRightArg(jsp, &rarg);
			res2 = recursiveExecuteBool(cxt, &rarg, jb, false);

			return res2 == jpbFalse ? res : res2;

		case jpiNot:
			jspGetArg(jsp, &larg);

			res = recursiveExecuteBool(cxt, &larg, jb, false);

			if (res == jpbUnknown)
				return jpbUnknown;

			return res == jpbTrue ? jpbFalse : jpbTrue;

		case jpiIsUnknown:
			jspGetArg(jsp, &larg);
			res = recursiveExecuteBool(cxt, &larg, jb, false);
			return res == jpbUnknown ? jpbTrue : jpbFalse;

		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			jspGetLeftArg(jsp, &larg);
			jspGetRightArg(jsp, &rarg);
			return executePredicate(cxt, jsp, &larg, &rarg, jb, true,
									executeComparison, NULL);

		case jpiStartsWith:		/* 'whole STARTS WITH initial' */
			jspGetLeftArg(jsp, &larg);	/* 'whole' */
			jspGetRightArg(jsp, &rarg); /* 'initial' */
			return executePredicate(cxt, jsp, &larg, &rarg, jb, false,
									executeStartsWith, NULL);

		case jpiLikeRegex:		/* 'expr LIKE_REGEX pattern FLAGS flags' */
			{
				/*
				 * 'expr' is a sequence-returning expression.  'pattern' is a
				 * regex string literal.  SQL/JSON standard requires XQuery
				 * regexes, but we use Postgres regexes here.  'flags' is a
				 * string literal converted to integer flags at compile-time.
				 */
				JsonLikeRegexContext lrcxt = {0};

				jspInitByBuffer(&larg, jsp->base,
								jsp->content.like_regex.expr);

				return executePredicate(cxt, jsp, &larg, NULL, jb, false,
										executeLikeRegex, &lrcxt);
			}

		case jpiExists:
			jspGetArg(jsp, &larg);

			if (jspStrictAbsenseOfErrors(cxt))
			{
				/*
				 * In strict mode we must get a complete list of values to
				 * check that there are no errors at all.
				 */
				JsonValueList vals = {0};
				JsonPathExecResult res =
				recursiveExecuteAndUnwrapNoThrow(cxt, &larg, jb, false, &vals);

				if (jperIsError(res))
					return jpbUnknown;

				return JsonValueListIsEmpty(&vals) ? jpbFalse : jpbTrue;
			}
			else
			{
				JsonPathExecResult res =
				recursiveExecuteAndUnwrapNoThrow(cxt, &larg, jb, false, NULL);

				if (jperIsError(res))
					return jpbUnknown;

				return res == jperOk ? jpbTrue : jpbFalse;
			}

		default:
			elog(ERROR, "invalid boolean jsonpath item type: %d", jsp->type);
			return jpbUnknown;
	}
}

/*
 * Execute nested (filters etc.) boolean expression pushing current SQL/JSON
 * item onto the stack.
 */
static inline JsonPathBool
recursiveExecuteBoolNested(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb)
{
	JsonItemStackEntry current;
	JsonPathBool res;

	pushJsonItem(&cxt->stack, &current, jb);
	res = recursiveExecuteBool(cxt, jsp, jb, false);
	popJsonItem(&cxt->stack);

	return res;
}

/* Save base object and its id needed for the execution of .keyvalue(). */
static inline JsonBaseObjectInfo
setBaseObject(JsonPathExecContext *cxt, JsonbValue *jbv, int32 id)
{
	JsonBaseObjectInfo baseObject = cxt->baseObject;

	cxt->baseObject.jbc = jbv->type != jbvBinary ? NULL :
		(JsonbContainer *) jbv->val.binary.data;
	cxt->baseObject.id = id;

	return baseObject;
}

/*
 * Main jsonpath executor function: walks on jsonpath structure, finds
 * relevant parts of jsonb and evaluates expressions over them.
 * When 'unwrap' is true current SQL/JSON item is unwrapped if it is an array.
 */
static JsonPathExecResult
recursiveExecuteUnwrap(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, bool unwrap, JsonValueList *found)
{
	JsonPathItem elem;
	JsonPathExecResult res = jperNotFound;
	JsonBaseObjectInfo baseObject;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch (jsp->type)
	{
			/* all boolean item types: */
		case jpiAnd:
		case jpiOr:
		case jpiNot:
		case jpiIsUnknown:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiExists:
		case jpiStartsWith:
		case jpiLikeRegex:
			{
				JsonPathBool st = recursiveExecuteBool(cxt, jsp, jb, true);

				res = appendBoolResult(cxt, jsp, found, st);
				break;
			}

		case jpiKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbValue *v;
				JsonbValue	key;

				key.type = jbvString;
				key.val.string.val = jspGetString(jsp, &key.val.string.len);

				v = findJsonbValueFromContainer(jb->val.binary.data,
												JB_FOBJECT, &key);

				if (v != NULL)
				{
					res = recursiveExecuteNext(cxt, jsp, NULL,
											   v, found, false);

					/* free value if it was not added to found list */
					if (jspHasNext(jsp) || !found)
						pfree(v);
				}
				else if (!jspIgnoreStructuralErrors(cxt))
				{
					StringInfoData keybuf;
					char	   *keystr;

					Assert(found);

					if (!jspThrowErrors(cxt))
						return jperError;

					initStringInfo(&keybuf);

					keystr = pnstrdup(key.val.string.val, key.val.string.len);
					escape_json(&keybuf, keystr);

					ThrowJsonPathError(JSON_MEMBER_NOT_FOUND,
									   ("JSON object does not contain key %s",
										keybuf.data));
				}
			}
			else if (unwrap && JsonbType(jb) == jbvArray)
				return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				ReturnJsonPathError(cxt, JSON_MEMBER_NOT_FOUND,
									("jsonpath member accessor is applied to "
									 "not an object"));
			}
			break;

		case jpiRoot:
			jb = cxt->root;
			baseObject = setBaseObject(cxt, jb, 0);
			res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
			cxt->baseObject = baseObject;
			break;

		case jpiCurrent:
			res = recursiveExecuteNext(cxt, jsp, NULL, cxt->stack->item,
									   found, true);
			break;

		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				bool		hasNext = jspGetNext(jsp, &elem);

				if (jb->type == jbvBinary)
				{
					JsonbValue	v;
					JsonbIterator *it;
					JsonbIteratorToken r;

					it = JsonbIteratorInit(jb->val.binary.data);

					while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
					{
						if (r == WJB_ELEM)
						{
							if (!hasNext && !found)
								return jperOk;

							res = recursiveExecuteNext(cxt, jsp, &elem,
													   &v, found, true);

							if (jperIsError(res))
								break;

							if (res == jperOk && !found)
								break;
						}
					}
				}
				else
				{
					Assert(jb->type != jbvArray);
					elog(ERROR, "invalid jsonb array value type: %d",
						 jb->type);
				}
			}
			else if (jspAutoWrap(cxt))
				res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
			else if (!jspIgnoreStructuralErrors(cxt))
				ReturnJsonPathError(cxt, JSON_ARRAY_NOT_FOUND,
									("jsonpath wildcard array accessor is "
									 "applied to not an array"));
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray || jspAutoWrap(cxt))
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = JsonbArraySize(jb);
				bool		singleton = size < 0;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (singleton)
					size = 1;

				cxt->innermostArraySize = size; /* for LAST evaluation */

				for (i = 0; i < jsp->content.array.nelems; i++)
				{
					JsonPathItem from;
					JsonPathItem to;
					int32		index;
					int32		index_from;
					int32		index_to;
					bool		range = jspGetArraySubscript(jsp, &from,
															 &to, i);

					res = getArrayIndex(cxt, &from, jb, &index_from);

					if (jperIsError(res))
						break;

					if (range)
					{
						res = getArrayIndex(cxt, &to, jb, &index_to);

						if (jperIsError(res))
							break;
					}
					else
						index_to = index_from;

					if (!jspIgnoreStructuralErrors(cxt) &&
						(index_from < 0 ||
						 index_from > index_to ||
						 index_to >= size))
						ReturnJsonPathError(cxt, INVALID_JSON_SUBSCRIPT,
											("jsonpath array subscript is "
											 "out of bounds"));

					if (index_from < 0)
						index_from = 0;

					if (index_to >= size)
						index_to = size - 1;

					res = jperNotFound;

					for (index = index_from; index <= index_to; index++)
					{
						JsonbValue *v;
						bool		copy;

						if (singleton)
						{
							v = jb;
							copy = true;
						}
						else
						{
							v = getIthJsonbValueFromContainer(jb->val.binary.data,
															  (uint32) index);

							if (v == NULL)
								continue;

							copy = false;
						}

						if (!hasNext && !found)
							return jperOk;

						res = recursiveExecuteNext(cxt, jsp, &elem, v, found,
												   copy);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;
			}
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				ReturnJsonPathError(cxt, JSON_ARRAY_NOT_FOUND,
									("jsonpath array accessor is applied to "
									 "not an array"));
			}
			break;

		case jpiLast:
			{
				JsonbValue	tmpjbv;
				JsonbValue *lastjbv;
				int			last;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (cxt->innermostArraySize < 0)
					elog(ERROR, "evaluating jsonpath LAST outside of "
						 "array subscript");

				if (!hasNext && !found)
				{
					res = jperOk;
					break;
				}

				last = cxt->innermostArraySize - 1;

				lastjbv = hasNext ? &tmpjbv : palloc(sizeof(*lastjbv));

				lastjbv->type = jbvNumeric;
				lastjbv->val.numeric =
					DatumGetNumeric(DirectFunctionCall1(int4_numeric,
														Int32GetDatum(last)));

				res = recursiveExecuteNext(cxt, jsp, &elem,
										   lastjbv, found, hasNext);
			}
			break;

		case jpiAnyKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonbIterator *it;
				int32		r;
				JsonbValue	v;
				bool		hasNext = jspGetNext(jsp, &elem);

				it = JsonbIteratorInit(jb->val.binary.data);

				while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
				{
					if (r == WJB_VALUE)
					{
						if (!hasNext && !found)
							return jperOk;

						res = recursiveExecuteNext(cxt, jsp, &elem,
												   &v, found, true);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
			}
			else if (unwrap && JsonbType(jb) == jbvArray)
				return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				ReturnJsonPathError(cxt, JSON_OBJECT_NOT_FOUND,
									("jsonpath wildcard member accessor is "
									 "applied to not an object"));
			}
			break;

		case jpiAdd:
			return executeBinaryArithmExpr(cxt, jsp, jb, numeric_add, found);

		case jpiSub:
			return executeBinaryArithmExpr(cxt, jsp, jb, numeric_sub, found);

		case jpiMul:
			return executeBinaryArithmExpr(cxt, jsp, jb, numeric_mul, found);

		case jpiDiv:
			return executeBinaryArithmExpr(cxt, jsp, jb, numeric_div, found);

		case jpiMod:
			return executeBinaryArithmExpr(cxt, jsp, jb, numeric_mod, found);

		case jpiPlus:
			return executeUnaryArithmExpr(cxt, jsp, jb, NULL, found);

		case jpiMinus:
			return executeUnaryArithmExpr(cxt, jsp, jb, numeric_uminus,
										  found);

		case jpiFilter:
			{
				JsonPathBool st;

				if (unwrap && JsonbType(jb) == jbvArray)
					return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

				jspGetArg(jsp, &elem);
				st = recursiveExecuteBoolNested(cxt, &elem, jb);
				if (st != jpbTrue)
					res = jperNotFound;
				else
					res = recursiveExecuteNext(cxt, jsp, NULL,
											   jb, found, true);
				break;
			}

		case jpiAny:
			{
				bool		hasNext = jspGetNext(jsp, &elem);

				/* first try without any intermediate steps */
				if (jsp->content.anybounds.first == 0)
				{
					bool		savedIgnoreStructuralErrors;

					savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
					cxt->ignoreStructuralErrors = true;
					res = recursiveExecuteNext(cxt, jsp, &elem,
											   jb, found, true);
					cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;

					if (res == jperOk && !found)
						break;
				}

				if (jb->type == jbvBinary)
					res = recursiveAny(cxt, hasNext ? &elem : NULL, jb, found,
									   1,
									   jsp->content.anybounds.first,
									   jsp->content.anybounds.last);
				break;
			}

		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			{
				JsonbValue	vbuf;
				JsonbValue *v;
				bool		hasNext = jspGetNext(jsp, &elem);
				int			id;

				if (!hasNext && !found)
				{
					res = jperOk;	/* skip evaluation */
					break;
				}

				v = hasNext ? &vbuf : palloc(sizeof(*v));

				id = computeJsonPathItem(cxt, jsp, v);

				baseObject = setBaseObject(cxt, v, id);
				res = recursiveExecuteNext(cxt, jsp, &elem,
										   v, found, hasNext);
				cxt->baseObject = baseObject;
			}
			break;

		case jpiType:
			{
				JsonbValue *jbv = palloc(sizeof(*jbv));

				jbv->type = jbvString;
				jbv->val.string.val = pstrdup(JsonbTypeName(jb));
				jbv->val.string.len = strlen(jbv->val.string.val);

				res = recursiveExecuteNext(cxt, jsp, NULL, jbv,
										   found, false);
			}
			break;

		case jpiSize:
			{
				int			size = JsonbArraySize(jb);

				if (size < 0)
				{
					if (!jspAutoWrap(cxt))
					{
						if (!jspIgnoreStructuralErrors(cxt))
							ReturnJsonPathError(cxt, JSON_ARRAY_NOT_FOUND,
												("jsonpath item method .%s() "
												 "is applied to not an array",
												 jspOperationName(jsp->type)));
						break;
					}

					size = 1;
				}

				jb = palloc(sizeof(*jb));

				jb->type = jbvNumeric;
				jb->val.numeric =
					DatumGetNumeric(DirectFunctionCall1(int4_numeric,
														Int32GetDatum(size)));

				res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, false);
			}
			break;

		case jpiAbs:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_abs,
											found);

		case jpiFloor:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_floor,
											found);

		case jpiCeiling:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_ceil,
											found);

		case jpiDouble:
			{
				JsonbValue	jbv;
				MemoryContext mcxt = CurrentMemoryContext;

				if (unwrap && JsonbType(jb) == jbvArray)
					return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

				/*
				 * It is safe to use here PG_TRY/PG_CATCH without
				 * subtransaction because no function called inside performs
				 * data modification.
				 */
				PG_TRY();
				{
					if (jb->type == jbvNumeric)
					{
						/* only check success of numeric to double cast */
						DirectFunctionCall1(numeric_float8,
											NumericGetDatum(jb->val.numeric));
						res = jperOk;
					}
					else if (jb->type == jbvString)
					{
						/* cast string as double */
						char	   *str = pnstrdup(jb->val.string.val,
												   jb->val.string.len);
						Datum		val = DirectFunctionCall1(float8in,
															  CStringGetDatum(str));

						pfree(str);

						jb = &jbv;
						jb->type = jbvNumeric;
						jb->val.numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric,
																			  val));
						res = jperOk;
					}
				}
				PG_CATCH();
				{
					if (ERRCODE_TO_CATEGORY(geterrcode()) !=
						ERRCODE_DATA_EXCEPTION)
						PG_RE_THROW();

					FlushErrorState();
					MemoryContextSwitchTo(mcxt);
					ReturnJsonPathError(cxt, NON_NUMERIC_JSON_ITEM,
										("jsonpath item method .%s() is "
										 "applied to not a numeric value",
										 jspOperationName(jsp->type)));
				}
				PG_END_TRY();

				if (res == jperNotFound)
					ReturnJsonPathError(cxt, NON_NUMERIC_JSON_ITEM,
										("jsonpath item method .%s() is "
										 "applied to neither a string nor "
										 "numeric value",
										 jspOperationName(jsp->type)));

				res = recursiveExecuteNext(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiDatetime:
			{
				JsonbValue	jbvbuf;
				Datum		value;
				text	   *datetime;
				Oid			typid;
				int32		typmod = -1;
				int			tz;
				bool		hasNext;

				if (unwrap && JsonbType(jb) == jbvArray)
					return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

				if (!(jb = getScalar(jb, &jbvbuf, jbvString)))
					ReturnJsonPathError(cxt,
										INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION,
										("jsonpath item method .%s() is "
										 "applied to not a string",
										 jspOperationName(jsp->type)));

				datetime = cstring_to_text_with_len(jb->val.string.val,
													jb->val.string.len);

				if (jsp->content.args.left)
				{
					text	   *template;
					char	   *template_str;
					int			template_len;
					char	   *tzname = NULL;

					jspGetLeftArg(jsp, &elem);

					if (elem.type != jpiString)
						elog(ERROR, "invalid jsonpath item type for "
							 ".datetime() argument");

					template_str = jspGetString(&elem, &template_len);

					if (jsp->content.args.right)
					{
						JsonValueList tzlist = {0};
						JsonPathExecResult tzres;
						JsonbValue *tzjbv;

						jspGetRightArg(jsp, &elem);
						tzres = recursiveExecute(cxt, &elem, jb, &tzlist);
						if (jperIsError(tzres))
							return tzres;

						if (JsonValueListLength(&tzlist) != 1 ||
							(tzjbv = JsonValueListHead(&tzlist))->type != jbvString)
							ReturnJsonPathError(cxt,
												INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION,
												("timezone argument of "
												 "jsonpath item method .%s() "
												 "is not a singleton string",
												 jspOperationName(jsp->type)));

						tzname = pnstrdup(tzjbv->val.string.val,
										  tzjbv->val.string.len);
					}

					template = cstring_to_text_with_len(template_str,
														template_len);

					if (tryToParseDatetime(template, datetime, tzname, false,
										   &value, &typid, &typmod,
										   &tz, jspThrowErrors(cxt)))
						res = jperOk;
					else
						res = jperError;

					if (tzname)
						pfree(tzname);
				}
				else
				{
					/* Try to recognize one of ISO formats. */
					static const char *fmt_str[] =
					{
						"yyyy-mm-dd HH24:MI:SS TZH:TZM",
						"yyyy-mm-dd HH24:MI:SS TZH",
						"yyyy-mm-dd HH24:MI:SS",
						"yyyy-mm-dd",
						"HH24:MI:SS TZH:TZM",
						"HH24:MI:SS TZH",
						"HH24:MI:SS"
					};

					/* cache for format texts */
					static text *fmt_txt[lengthof(fmt_str)] = {0};
					int			i;

					for (i = 0; i < lengthof(fmt_str); i++)
					{
						if (!fmt_txt[i])
						{
							MemoryContext oldcxt =
							MemoryContextSwitchTo(TopMemoryContext);

							fmt_txt[i] = cstring_to_text(fmt_str[i]);
							MemoryContextSwitchTo(oldcxt);
						}

						if (tryToParseDatetime(fmt_txt[i], datetime, NULL,
											   true, &value, &typid, &typmod,
											   &tz, false))
						{
							res = jperOk;
							break;
						}
					}

					if (res == jperNotFound)
						ReturnJsonPathErrorHint(cxt,
												INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION,
												("unrecognized datetime format"),
												("use datetime template "
												 "argument for explicit "
												 "format specification"));
				}

				pfree(datetime);

				if (jperIsError(res))
					break;

				hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
					break;

				jb = hasNext ? &jbvbuf : palloc(sizeof(*jb));

				jb->type = jbvDatetime;
				jb->val.datetime.value = value;
				jb->val.datetime.typid = typid;
				jb->val.datetime.typmod = typmod;
				jb->val.datetime.tz = tz;

				res = recursiveExecuteNext(cxt, jsp, &elem, jb,
										   found, hasNext);
			}
			break;

		case jpiKeyValue:
			if (unwrap && JsonbType(jb) == jbvArray)
				return recursiveExecuteUnwrapArray(cxt, jsp, jb, found);

			/*
			 * .keyvalue() method returns a sequence of object's key-value
			 * pairs in the following format: '{ "key": key, "value": value,
			 * "id": id }'.
			 *
			 * "id" field is an object identifier which is constructed from
			 * the two parts: base object id and its binary offset in base
			 * object's jsonb: id = 10000000000 * base_object_id +
			 * obj_offset_in_base_object
			 *
			 * 10000000000 (10^10) -- is a first round decimal number greater
			 * than 2^32 (maximal offset in jsonb).  Decimal multiplier is
			 * used here to improve the readability of identifiers.
			 *
			 * Base object is usually a root object of the path: context item
			 * '$' or path variable '$var', literals can't produce objects for
			 * now.  But if the path contains generated objects (.keyvalue()
			 * itself, for example), then they become base object for the
			 * subsequent .keyvalue().
			 *
			 * Id of '$' is 0. Id of '$var' is its ordinal (positive) number
			 * in the list of variables (see computeJsonPathVariable()). Ids
			 * for generated objects are assigned using global counter
			 * JsonPathExecContext.lastGeneratedObjectId starting from
			 * (number_of_vars + 1).
			 */

			if (JsonbType(jb) != jbvObject || jb->type != jbvBinary)
			{
				ReturnJsonPathError(cxt, JSON_OBJECT_NOT_FOUND,
									("jsonpath item method .%s() is applied "
									 "to not an object",
									 jspOperationName(jsp->type)));
			}
			else
			{
				int32		r;
				JsonbValue	key;
				JsonbValue	val;
				JsonbValue	idval;
				JsonbValue	obj;
				JsonbValue	keystr;
				JsonbValue	valstr;
				JsonbValue	idstr;
				JsonbIterator *it;
				JsonbParseState *ps = NULL;
				int64		id;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (!JsonContainerSize(jb->val.binary.data))
					return jperNotFound;	/* no key-value pairs */

				/* make template object */
				obj.type = jbvBinary;

				keystr.type = jbvString;
				keystr.val.string.val = "key";
				keystr.val.string.len = 3;

				valstr.type = jbvString;
				valstr.val.string.val = "value";
				valstr.val.string.len = 5;

				idstr.type = jbvString;
				idstr.val.string.val = "id";
				idstr.val.string.len = 2;

				/*
				 * construct object id from its base object and offset inside
				 * that
				 */
				id = jb->type != jbvBinary ? 0 :
					(int64) ((char *) jb->val.binary.data -
							 (char *) cxt->baseObject.jbc);
				id += (int64) cxt->baseObject.id * INT64CONST(10000000000);

				idval.type = jbvNumeric;
				idval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
																		Int64GetDatum(id)));

				it = JsonbIteratorInit(jb->val.binary.data);

				while ((r = JsonbIteratorNext(&it, &key, true)) != WJB_DONE)
				{
					if (r == WJB_KEY)
					{
						Jsonb	   *jsonb;
						JsonbValue *keyval;

						res = jperOk;

						if (!hasNext && !found)
							break;

						r = JsonbIteratorNext(&it, &val, true);
						Assert(r == WJB_VALUE);

						pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

						pushJsonbValue(&ps, WJB_KEY, &keystr);
						pushJsonbValue(&ps, WJB_VALUE, &key);

						pushJsonbValue(&ps, WJB_KEY, &valstr);
						pushJsonbValue(&ps, WJB_VALUE, &val);

						pushJsonbValue(&ps, WJB_KEY, &idstr);
						pushJsonbValue(&ps, WJB_VALUE, &idval);

						keyval = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

						jsonb = JsonbValueToJsonb(keyval);

						JsonbInitBinary(&obj, jsonb);

						baseObject = setBaseObject(cxt, &obj,
												   cxt->lastGeneratedObjectId++);

						res = recursiveExecuteNext(cxt, jsp, &elem, &obj,
												   found, true);

						cxt->baseObject = baseObject;

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}
				}
			}
			break;

		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
static JsonPathExecResult
recursiveExecuteUnwrapArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
							JsonbValue *jb, JsonValueList *found)
{
	JsonPathExecResult res = jperNotFound;

	if (jb->type == jbvBinary)
	{
		JsonbValue	v;
		JsonbIterator *it;
		JsonbIteratorToken tok;

		it = JsonbIteratorInit(jb->val.binary.data);

		while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
		{
			if (tok == WJB_ELEM)
			{
				res = recursiveExecuteUnwrap(cxt, jsp, &v, false, found);
				if (jperIsError(res))
					break;
				if (res == jperOk && !found)
					break;
			}
		}
	}
	else
	{
		Assert(jb->type != jbvArray);
		elog(ERROR, "invalid jsonb array value type: %d", jb->type);
	}

	return res;
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
static inline JsonPathExecResult
recursiveExecute(JsonPathExecContext *cxt, JsonPathItem *jsp,
				 JsonbValue *jb, JsonValueList *found)
{
	return recursiveExecuteUnwrap(cxt, jsp, jb, jspAutoUnwrap(cxt), found);
}


/*
 * Interface to jsonpath executor
 *
 * 'path' - jsonpath to be executed
 * 'vars' - variables to be substituted to jsonpath
 * 'json' - target document for jsonpath evaluation
 * 'throwErrors' - whether we should throw suppressible errors
 * 'result' - list to store result items into
 *
 * Returns an error happens during processing or NULL on no error.
 *
 * Note, jsonb and jsonpath values should be avaliable and untoasted during
 * work because JsonPathItem, JsonbValue and result item could have pointers
 * into input values.  If caller needs to just check if document matches
 * jsonpath, then it doesn't provide a result arg.  In this case executor
 * works till first positive result and does not check the rest if possible.
 * In other case it tries to find all the satisfied result items.
 */
static JsonPathExecResult
executeJsonPath(JsonPath *path, List *vars, Jsonb *json, bool throwErrors,
				JsonValueList *result)
{
	JsonPathExecContext cxt;
	JsonPathExecResult res;
	JsonPathItem jsp;
	JsonbValue	jbv;
	JsonItemStackEntry root;

	jspInit(&jsp, path);

	if (JB_ROOT_IS_SCALAR(json))
	{
		bool		res PG_USED_FOR_ASSERTS_ONLY;

		res = JsonbExtractScalar(&json->root, &jbv);
		Assert(res);
	}
	else
		JsonbInitBinary(&jbv, json);

	cxt.vars = vars;
	cxt.laxMode = (path->header & JSONPATH_LAX) != 0;
	cxt.ignoreStructuralErrors = cxt.laxMode;
	cxt.root = &jbv;
	cxt.stack = NULL;
	cxt.baseObject.jbc = NULL;
	cxt.baseObject.id = 0;
	cxt.lastGeneratedObjectId = list_length(vars) + 1;
	cxt.innermostArraySize = -1;
	cxt.throwErrors = throwErrors;

	pushJsonItem(&cxt.stack, &root, cxt.root);

	if (jspStrictAbsenseOfErrors(&cxt) && !result)
	{
		/*
		 * In strict mode we must get a complete list of values to check that
		 * there are no errors at all.
		 */
		JsonValueList vals = {0};

		Assert(!throwErrors);

		res = recursiveExecute(&cxt, &jsp, &jbv, &vals);

		if (jperIsError(res))
			return res;

		return JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
	}

	res = recursiveExecute(&cxt, &jsp, &jbv, result);

	Assert(!throwErrors || !jperIsError(res));

	return res;
}

static Datum
returnDatum(void *arg, bool *isNull)
{
	*isNull = false;
	return PointerGetDatum(arg);
}

static Datum
returnNull(void *arg, bool *isNull)
{
	*isNull = true;
	return Int32GetDatum(0);
}

/*
 * Converts jsonb object into list of vars for executor.
 */
static List *
makePassingVars(Jsonb *jb)
{
	JsonbValue	v;
	JsonbIterator *it;
	int32		r;
	List	   *vars = NIL;

	it = JsonbIteratorInit(&jb->root);

	r = JsonbIteratorNext(&it, &v, true);

	if (r != WJB_BEGIN_OBJECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("jsonb containing jsonpath variables"
						"is not an object")));

	while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			JsonPathVariable *jpv = palloc0(sizeof(*jpv));

			jpv->varName = cstring_to_text_with_len(v.val.string.val,
													v.val.string.len);

			JsonbIteratorNext(&it, &v, true);

			/* Datums are copied from jsonb into the current memory context. */
			jpv->cb = returnDatum;

			switch (v.type)
			{
				case jbvBool:
					jpv->typid = BOOLOID;
					jpv->cb_arg = DatumGetPointer(BoolGetDatum(v.val.boolean));
					break;
				case jbvNull:
					jpv->cb = returnNull;
					break;
				case jbvString:
					jpv->typid = TEXTOID;
					jpv->cb_arg = cstring_to_text_with_len(v.val.string.val,
														   v.val.string.len);
					break;
				case jbvNumeric:
					jpv->typid = NUMERICOID;
					jpv->cb_arg =
						DatumGetPointer(datumCopy(NumericGetDatum(v.val.numeric),
												  false, -1));
					break;
				case jbvBinary:
					/* copy jsonb container into current memory context */
					v.val.binary.data = memcpy(palloc(v.val.binary.len),
											   v.val.binary.data,
											   v.val.binary.len);
					jpv->typid = INTERNALOID;	/* raw jsonb value */
					jpv->cb_arg = copyJsonbValue(&v);
					break;
				default:
					elog(ERROR, "invalid jsonb value type: %d", v.type);
			}

			vars = lappend(vars, jpv);
		}
	}

	return vars;
}

/*
 * jsonb_path_exists
 *		Returns true if jsonpath returns at least one item for the specified
 *		jsonb value.  This function and jsonb_path_match() are used to
 *		implement @? and @@ operators, which in turn are intended to have an
 *		index support.  Thus, it's desirable to make it easier to achieve
 *		consistency between index scan results and sequential scan results.
 *		So, we throw as less errors as possible.  Regarding this function,
 *		such behavior also matches behavior of JSON_EXISTS() clause of
 *		SQL/JSON.  Regarding jsonb_path_match(), this function doesn't have
 *		an analogy in SQL/JSON, so we define its behavior on our own.
 */
Datum
jsonb_path_exists(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonPathExecResult res;
	List	   *vars = NIL;

	if (PG_NARGS() == 3)
		vars = makePassingVars(PG_GETARG_JSONB_P(2));

	res = executeJsonPath(jp, vars, jb, false, NULL);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (jperIsError(res))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(res == jperOk);
}

/*
 * jsonb_path_exists_novars
 *		Implements the 2-argument version of jsonb_path_exists
 */
Datum
jsonb_path_exists_novars(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonb_path_exists(fcinfo);
}

/*
 * jsonb_path_match
 *		Returns jsonpath predicate result item for the specified jsonb value.
 *		See jsonb_path_exists() comment for details regarding error handling.
 */
Datum
jsonb_path_match(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonbValue *jbv;
	JsonValueList found = {0};
	List	   *vars;

	if (PG_NARGS() == 3)
		vars = makePassingVars(PG_GETARG_JSONB_P(2));
	else
		vars = NULL;

	(void) executeJsonPath(jp, vars, jb, false, &found);

	if (JsonValueListLength(&found) < 1)
		PG_RETURN_NULL();

	jbv = JsonValueListHead(&found);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (jbv->type != jbvBool)
		PG_RETURN_NULL();

	PG_RETURN_BOOL(jbv->val.boolean);
}

/*
 * jsonb_path_match_novars
 *		Implements the 2-argument version of jsonb_path_match
 */
Datum
jsonb_path_match_novars(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonb_path_match(fcinfo);
}

/*
 * jsonb_path_query
 *		Executes jsonpath for given jsonb document and returns result as
 *		rowset.
 */
Datum
jsonb_path_query(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	List	   *found;
	JsonbValue *v;
	ListCell   *c;

	if (SRF_IS_FIRSTCALL())
	{
		JsonPath   *jp;
		Jsonb	   *jb;
		MemoryContext oldcontext;
		List	   *vars = NIL;
		JsonValueList found = {0};

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		jb = PG_GETARG_JSONB_P_COPY(0);
		jp = PG_GETARG_JSONPATH_P_COPY(1);
		if (PG_NARGS() == 3)
			vars = makePassingVars(PG_GETARG_JSONB_P(2));

		(void) executeJsonPath(jp, vars, jb, true, &found);

		funcctx->user_fctx = JsonValueListGetList(&found);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	found = funcctx->user_fctx;

	c = list_head(found);

	if (c == NULL)
		SRF_RETURN_DONE(funcctx);

	v = lfirst(c);
	funcctx->user_fctx = list_delete_first(found);

	SRF_RETURN_NEXT(funcctx, JsonbPGetDatum(JsonbValueToJsonb(v)));
}

/*
 * jsonb_path_query_novars
 *		Implements the 2-argument version of jsonb_path_query
 */
Datum
jsonb_path_query_novars(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonb_path_query(fcinfo);
}

/*
 * jsonb_path_query_array
 *		Executes jsonpath for given jsonb document and returns result as
 *		jsonb array.
 */
Datum
jsonb_path_query_array(FunctionCallInfo fcinfo)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonValueList found = {0};
	List	   *vars;

	if (PG_NARGS() == 3)
		vars = makePassingVars(PG_GETARG_JSONB_P(2));
	else
		vars = NIL;

	(void) executeJsonPath(jp, vars, jb, true, &found);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(wrapItemsInArray(&found)));
}

/*
 * jsonb_path_query_array_novars
 *		Implements the 2-argument version of jsonb_path_query_array
 */
Datum
jsonb_path_query_array_novars(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonb_path_query_array(fcinfo);
}

/*
 * jsonb_path_query_first
 *		Executes jsonpath for given jsonb document and returns first result
 *		item.  If there are no items, NULL returned.
 */
Datum
jsonb_path_query_first(FunctionCallInfo fcinfo)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonPath   *jp = PG_GETARG_JSONPATH_P(1);
	JsonValueList found = {0};
	List	   *vars;

	if (PG_NARGS() == 3)
		vars = makePassingVars(PG_GETARG_JSONB_P(2));
	else
		vars = NIL;

	(void) executeJsonPath(jp, vars, jb, true, &found);

	if (JsonValueListLength(&found) >= 1)
		PG_RETURN_JSONB_P(JsonbValueToJsonb(JsonValueListHead(&found)));
	else
		PG_RETURN_NULL();
}

/*
 * jsonb_path_query_first_novars
 *		Implements the 2-argument version of jsonb_path_query_first
 */
Datum
jsonb_path_query_first_novars(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonb_path_query_first(fcinfo);
}
