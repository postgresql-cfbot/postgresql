/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.c
 *	 Routines for SQL/JSON path execution.
 *
 * Jsonpath is executed in the global context stored in JsonPathExecContext,
 * which is passed to almost every function involved into execution.  Entry
 * point for jsonpath execution is executeJsonPath() function, which
 * initializes execution context including initial JsonPathItem and JsonbValue,
 * flags, stack for calculation of @ in filters.
 *
 * The result of jsonpath query execution is enum JsonPathExecResult and
 * if succeeded sequence of JsonbValue, written to JsonValueList *found, which
 * is passed through the jsonpath items.  When found == NULL, we're inside
 * exists-query and we're interested only in whether result is empty.  In this
 * case execution is stopped once first result item is found, and the only
 * execution result is JsonPathExecResult.  The values of JsonPathExecResult
 * are following:
 * - jperOk			-- result sequence is not empty
 * - jperNotFound	-- result sequence is empty
 * - jperError		-- error occurred during execution
 *
 * Jsonpath is executed recursively (see executeItem()) starting form the
 * first path item (which in turn might be, for instance, an arithmetic
 * expression evaluated separately).  On each step single JsonbValue obtained
 * from previous path item is processed.  The result of processing is a
 * sequence of JsonbValue (probably empty), which is passed to the next path
 * item one by one.  When there is no next path item, then JsonbValue is added
 * to the 'found' list.  When found == NULL, then execution functions just
 * return jperOk (see executeNextItem()).
 *
 * Many of jsonpath operations require automatic unwrapping of arrays in lax
 * mode.  So, if input value is array, then corresponding operation is
 * processed not on array itself, but on all of its members one by one.
 * executeItemOptUnwrapTarget() function have 'unwrap' argument, which indicates
 * whether unwrapping of array is needed.  When unwrap == true, each of array
 * members is passed to executeItemOptUnwrapTarget() again but with unwrap == false
 * in order to evade subsequent array unwrapping.
 *
 * All boolean expressions (predicates) are evaluated by executeBoolItem()
 * function, which returns tri-state JsonPathBool.  When error is occurred
 * during predicate execution, it returns jpbUnknown.  According to standard
 * predicates can be only inside filters.  But we support their usage as
 * jsonpath expression.  This helps us to implement @@ operator.  In this case
 * resulting JsonPathBool is transformed into jsonb bool or null.
 *
 * Arithmetic and boolean expression are evaluated recursively from expression
 * tree top down to the leaves.  Therefore, for binary arithmetic expressions
 * we calculate operands first.  Then we check that results are numeric
 * singleton lists, calculate the result and pass it to the next path item.
 *
 * Copyright (c) 2019-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "executor/execExpr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/datum.h"
#include "utils/float.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonpath.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"


typedef union Jsonx
{
	Jsonb		jb;
	Json		js;
} Jsonx;

#define DatumGetJsonxP(datum, isJsonb) \
	((isJsonb) ? (Jsonx *) DatumGetJsonbP(datum) : (Jsonx *) DatumGetJsonP(datum))

typedef JsonbContainer JsonxContainer;

typedef struct JsonxIterator
{
	bool		isJsonb;
	union
	{
		JsonbIterator *jb;
		JsonIterator *js;
	}			it;
} JsonxIterator;

/*
 * Represents "base object" and it's "id" for .keyvalue() evaluation.
 */
typedef struct JsonBaseObjectInfo
{
	JsonxContainer *jbc;
	int			id;
} JsonBaseObjectInfo;

/*
 * Special data structure representing stack of current items.  We use it
 * instead of regular list in order to evade extra memory allocation.  These
 * items are always allocated in local variables.
 */
typedef struct JsonItemStackEntry
{
	JsonItem   *item;
	struct JsonItemStackEntry *parent;
} JsonItemStackEntry;

typedef JsonItemStackEntry *JsonItemStack;

typedef int (*JsonPathVarCallback) (void *vars, bool isJsonb,
									char *varName, int varNameLen,
									JsonItem *val, JsonbValue *baseObject);

/*
 * Context of jsonpath execution.
 */
typedef struct JsonPathExecContext
{
	void	   *vars;			/* variables to substitute into jsonpath */
	JsonPathVarCallback getVar;
	JsonItem   *root;			/* for $ evaluation */
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
	bool		useTz;
	bool		isJsonb;
} JsonPathExecContext;

/* Context for LIKE_REGEX execution. */
typedef struct JsonLikeRegexContext
{
	text	   *regex;
	int			cflags;
} JsonLikeRegexContext;

/* Result of jsonpath predicate evaluation */
typedef enum JsonPathBool
{
	jpbFalse = 0,
	jpbTrue = 1,
	jpbUnknown = 2
} JsonPathBool;

/* Result of jsonpath expression evaluation */
typedef enum JsonPathExecResult
{
	jperOk = 0,
	jperNotFound = 1,
	jperError = 2
} JsonPathExecResult;

#define jperIsError(jper)			((jper) == jperError)

/*
 * List of SQL/JSON items with shortcut for single-value list.
 */
typedef struct JsonValueList
{
	JsonItem   *head;
	JsonItem   *tail;
	int			length;
} JsonValueList;

typedef struct JsonValueListIterator
{
	JsonItem   *next;
} JsonValueListIterator;

/*
 * Context for execution of
 * jsonb_path_*(jsonb, jsonpath [, vars jsonb, silent boolean]) user functions.
 */
typedef struct JsonPathUserFuncContext
{
	FunctionCallInfo fcinfo;
	void	   *js;				/* first jsonb function argument */
	Json	   *json;
	JsonPath   *jp;				/* second jsonpath function argument */
	void	   *vars;			/* third vars function argument */
	JsonValueList found;		/* resulting item list */
	bool		silent;			/* error suppression flag */
} JsonPathUserFuncContext;

/* Structures for JSON_TABLE execution  */
typedef struct JsonTableScanState JsonTableScanState;
typedef struct JsonTableJoinState JsonTableJoinState;

struct JsonTableScanState
{
	JsonTableScanState *parent;
	JsonTableJoinState *nested;
	MemoryContext mcxt;
	JsonPath   *path;
	List	   *args;
	JsonValueList found;
	JsonValueListIterator iter;
	Datum		current;
	int			ordinal;
	bool		currentIsNull;
	bool		outerJoin;
	bool		errorOnError;
	bool		advanceNested;
	bool		reset;
};

struct JsonTableJoinState
{
	union
	{
		struct
		{
			JsonTableJoinState *left;
			JsonTableJoinState *right;
			bool		cross;
			bool		advanceRight;
		}			join;
		JsonTableScanState scan;
	}			u;
	bool		is_join;
};

/* random number to identify JsonTableContext */
#define JSON_TABLE_CONTEXT_MAGIC	418352867

typedef struct JsonTableContext
{
	int			magic;
	struct
	{
		ExprState  *expr;
		JsonTableScanState *scan;
	}		   *colexprs;
	JsonTableScanState root;
	bool		empty;
	bool		isJsonb;
} JsonTableContext;

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#define jspStrictAbsenseOfErrors(cxt)	(!(cxt)->laxMode)
#define jspAutoUnwrap(cxt)				((cxt)->laxMode)
#define jspAutoWrap(cxt)				((cxt)->laxMode)
#define jspIgnoreStructuralErrors(cxt)	((cxt)->ignoreStructuralErrors)
#define jspThrowErrors(cxt)				((cxt)->throwErrors)

/* Convenience macro: return or throw error depending on context */
#define RETURN_ERROR(throw_error) \
do { \
	if (jspThrowErrors(cxt)) \
		throw_error; \
	else \
		return jperError; \
} while (0)

typedef JsonPathBool (*JsonPathPredicateCallback) (JsonPathItem *jsp,
												   JsonItem *larg,
												   JsonItem *rarg,
												   void *param);
typedef Numeric (*BinaryArithmFunc) (Numeric num1, Numeric num2, bool *error);

typedef JsonbValue *(*JsonBuilderFunc) (JsonbParseState **,
										JsonbIteratorToken,
										JsonbValue *);

static void freeUserFuncContext(JsonPathUserFuncContext *cxt);
static JsonPathExecResult executeUserFunc(FunctionCallInfo fcinfo,
				JsonPathUserFuncContext *cxt, bool isJsonb, bool copy, bool useTz);

static JsonPathExecResult executeJsonPath(JsonPath *path, void *vars,
										  JsonPathVarCallback getVar,
										  Jsonx *json, bool isJsonb,
										  bool throwErrors,
										  JsonValueList *result, bool useTz);
static JsonPathExecResult executeItem(JsonPathExecContext *cxt,
									  JsonPathItem *jsp, JsonItem *jb,
									  JsonValueList *found);
static JsonPathExecResult executeItemOptUnwrapTarget(JsonPathExecContext *cxt,
													 JsonPathItem *jsp,
													 JsonItem *jb,
													 JsonValueList *found,
													 bool unwrap);
static JsonPathExecResult executeItemUnwrapTargetArray(JsonPathExecContext *cxt,
													   JsonPathItem *jsp,
													   JsonItem *jb,
													   JsonValueList *found,
													   bool unwrapElements);
static JsonPathExecResult executeNextItem(JsonPathExecContext *cxt,
										  JsonPathItem *cur, JsonPathItem *next,
										  JsonItem *v, JsonValueList *found,
										  bool copy);
static JsonPathExecResult executeItemOptUnwrapResult(JsonPathExecContext *cxt,
													 JsonPathItem *jsp,
													 JsonItem *jb, bool unwrap,
													 JsonValueList *found);
static JsonPathExecResult executeItemOptUnwrapResultNoThrow(JsonPathExecContext *cxt,
															JsonPathItem *jsp,
															JsonItem *jb,
															bool unwrap,
															JsonValueList *found);
static JsonPathBool executeBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
									JsonItem *jb, bool canHaveNext);
static JsonPathBool executeNestedBoolItem(JsonPathExecContext *cxt,
										  JsonPathItem *jsp, JsonItem *jb);
static JsonPathExecResult executeAnyItem(JsonPathExecContext *cxt,
										 JsonPathItem *jsp, JsonbContainer *jbc, JsonValueList *found,
										 uint32 level, uint32 first, uint32 last,
										 bool ignoreStructuralErrors, bool unwrapNext);
static JsonPathBool executePredicate(JsonPathExecContext *cxt,
									 JsonPathItem *pred, JsonPathItem *larg,
									 JsonPathItem *rarg, JsonItem *jb,
									 bool unwrapRightArg,
									 JsonPathPredicateCallback exec,
									 void *param);
static JsonPathExecResult executeBinaryArithmExpr(JsonPathExecContext *cxt,
												  JsonPathItem *jsp,
												  JsonItem *jb,
												  BinaryArithmFunc func,
												  JsonValueList *found);
static JsonPathExecResult executeUnaryArithmExpr(JsonPathExecContext *cxt,
												 JsonPathItem *jsp,
												 JsonItem *jb, PGFunction func,
												 JsonValueList *found);
static JsonPathBool executeStartsWith(JsonPathItem *jsp, JsonItem *whole,
									  JsonItem *initial, void *param);
static JsonPathBool executeLikeRegex(JsonPathItem *jsp, JsonItem *str,
									 JsonItem *rarg, void *param);
static JsonPathExecResult executeNumericItemMethod(JsonPathExecContext *cxt,
												   JsonPathItem *jsp,
												   JsonItem *jb, bool unwrap,
												   PGFunction func,
												   JsonValueList *found);
static JsonPathExecResult executeDateTimeMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
												JsonItem *jb, JsonValueList *found);
static JsonPathExecResult executeKeyValueMethod(JsonPathExecContext *cxt,
												JsonPathItem *jsp, JsonItem *jb,
												JsonValueList *found);
static JsonPathExecResult appendBoolResult(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonValueList *found, JsonPathBool res);
static void getJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item,
							JsonItem *value);
static void getJsonPathVariable(JsonPathExecContext *cxt,
					JsonPathItem *variable, JsonItem *value);
static int getJsonPathVariableFromJsonx(void *varsJsonb, bool isJsonb,
										char *varName, int varNameLen,
										JsonItem *val, JsonbValue *baseObject);
static int	JsonxArraySize(JsonItem *jb, bool isJsonb);
static JsonPathBool executeComparison(JsonPathItem *cmp, JsonItem *lv,
									  JsonItem *rv, void *p);
static JsonPathBool compareItems(int32 op, JsonItem *jb1, JsonItem *jb2,
								 bool useTz);
static int	compareNumeric(Numeric a, Numeric b);

static void JsonItemInitNull(JsonItem *item);
static void JsonItemInitBool(JsonItem *item, bool val);
static void JsonItemInitNumeric(JsonItem *item, Numeric val);
#define JsonItemInitNumericDatum(item, val) \
		JsonItemInitNumeric(item, DatumGetNumeric(val))
static void JsonItemInitString(JsonItem *item, char *str, int len);
static void JsonItemInitDatetime(JsonItem *item, Datum val, Oid typid,
					 int32 typmod, int tz);

static JsonItem *copyJsonItem(JsonItem *src);
static JsonItem *JsonbValueToJsonItem(JsonbValue *jbv, JsonItem *jsi);
static JsonbValue *JsonItemToJsonbValue(JsonItem *jsi, JsonbValue *jbv);
static const char *JsonItemTypeName(JsonItem *jsi);
static JsonPathExecResult getArrayIndex(JsonPathExecContext *cxt,
										JsonPathItem *jsp, JsonItem *jb,
										int32 *index);
static JsonBaseObjectInfo setBaseObject(JsonPathExecContext *cxt,
										JsonItem *jsi, int32 id);
static void JsonValueListClear(JsonValueList *jvl);
static void JsonValueListAppend(JsonValueList *jvl, JsonItem *jbv);
static int	JsonValueListLength(const JsonValueList *jvl);
static bool JsonValueListIsEmpty(JsonValueList *jvl);
static JsonItem *JsonValueListHead(JsonValueList *jvl);
static List *JsonValueListGetList(JsonValueList *jvl);
static void JsonValueListInitIterator(const JsonValueList *jvl,
									  JsonValueListIterator *it);
static JsonItem *JsonValueListNext(const JsonValueList *jvl,
								   JsonValueListIterator *it);
static int	JsonbType(JsonItem *jb);
static JsonbValue *JsonbInitBinary(JsonbValue *jbv, Jsonb *jb);
static inline JsonbValue *JsonInitBinary(JsonbValue *jbv, Json *js);
static JsonItem *getScalar(JsonItem *scalar, enum jbvType type);
static JsonbValue *wrapItemsInArray(const JsonValueList *items, bool isJsonb);
static text *JsonItemUnquoteText(JsonItem *jsi, bool isJsonb);

static JsonItem *getJsonObjectKey(JsonItem *jb, char *keystr, int keylen,
								  bool isJsonb, JsonItem *val);
static JsonItem *getJsonArrayElement(JsonItem *jb, uint32 index, bool isJsonb,
									 JsonItem *elem);

static void JsonxIteratorInit(JsonxIterator *it, JsonxContainer *jxc,
				  bool isJsonb);
static JsonbIteratorToken JsonxIteratorNext(JsonxIterator *it, JsonbValue *jbv,
				  bool skipNested);
static JsonbValue *JsonItemToJsonbValue(JsonItem *jsi, JsonbValue *jbv);
static Jsonx *JsonbValueToJsonx(JsonbValue *jbv, bool isJsonb);

static int	compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2,
							bool useTz, bool *have_error);

static void pushJsonItem(JsonItemStack *stack,
			 JsonItemStackEntry *entry, JsonItem *item);
static void popJsonItem(JsonItemStack *stack);

static JsonTableJoinState *JsonTableInitPlanState(JsonTableContext *cxt,
									Node *plan, JsonTableScanState *parent);
static bool JsonTableNextRow(JsonTableScanState *scan, bool isJsonb);


/****************** User interface to JsonPath executor ********************/

/*
 * json[b]_path_exists
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
static Datum
jsonx_path_exists_internal(FunctionCallInfo fcinfo, bool isJsonb, bool tz)
{
	JsonPathExecResult res = executeUserFunc(fcinfo, NULL, isJsonb, false, tz);

	if (jperIsError(res))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(res == jperOk);
}

Datum
jsonb_path_exists(PG_FUNCTION_ARGS)
{
	return jsonx_path_exists_internal(fcinfo, true, false);
}

Datum
json_path_exists(PG_FUNCTION_ARGS)
{
	return jsonx_path_exists_internal(fcinfo, false, false);
}

Datum
jsonb_path_exists_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_exists_internal(fcinfo, true, true);
}

Datum
json_path_exists_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_exists_internal(fcinfo, false, true);
}

/*
 * json[b]_path_exists_opr
 *		Implementation of operator "json[b] @? jsonpath" (2-argument version of
 *		json[b]_path_exists()).
 */
Datum
jsonb_path_exists_opr(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonx_path_exists_internal(fcinfo, true, false);
}

Datum
json_path_exists_opr(PG_FUNCTION_ARGS)
{
	return jsonx_path_exists_internal(fcinfo, false, false);
}

/*
 * json[b]_path_match
 *		Returns jsonpath predicate result item for the specified jsonb value.
 *		See jsonb_path_exists() comment for details regarding error handling.
 */
static Datum
jsonx_path_match_internal(FunctionCallInfo fcinfo, bool isJsonb, bool tz)
{
	JsonPathUserFuncContext cxt;

	(void) executeUserFunc(fcinfo, &cxt, isJsonb, false, tz);

	freeUserFuncContext(&cxt);

	if (JsonValueListLength(&cxt.found) == 1)
	{
		JsonItem   *res = JsonValueListHead(&cxt.found);

		if (JsonItemIsBool(res))
			PG_RETURN_BOOL(JsonItemBool(res));

		if (JsonItemIsNull(res))
			PG_RETURN_NULL();
	}

	if (!cxt.silent)
		ereport(ERROR,
				(errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
				 errmsg("single boolean result is expected")));

	PG_RETURN_NULL();
}

Datum
jsonb_path_match(PG_FUNCTION_ARGS)
{
	return jsonx_path_match_internal(fcinfo, true, false);
}

Datum
json_path_match(PG_FUNCTION_ARGS)
{
	return jsonx_path_match_internal(fcinfo, false, false);
}

Datum
jsonb_path_match_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_match_internal(fcinfo, true, true);
}

Datum
json_path_match_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_match_internal(fcinfo, false, true);
}

/*
 * json[b]_path_match_opr
 *		Implementation of operator "json[b] @@ jsonpath" (2-argument version of
 *		json[b]_path_match()).
 */
Datum
jsonb_path_match_opr(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonx_path_match_internal(fcinfo, true, false);
}

Datum
json_path_match_opr(PG_FUNCTION_ARGS)
{
	/* just call the other one -- it can handle both cases */
	return jsonx_path_match_internal(fcinfo, false, false);
}

/*
 * json[b]_path_query
 *		Executes jsonpath for given jsonb document and returns result as
 *		rowset.
 */
static Datum
jsonx_path_query_internal(FunctionCallInfo fcinfo, bool isJsonb, bool tz)
{
	FuncCallContext *funcctx;
	List	   *found;
	JsonItem   *v;
	ListCell   *c;
	Datum		res;

	if (SRF_IS_FIRSTCALL())
	{
		JsonPathUserFuncContext jspcxt;
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* jsonb and jsonpath arguments are copied into SRF context. */
		(void) executeUserFunc(fcinfo, &jspcxt, isJsonb, true, tz);

		/*
		 * Don't free jspcxt because items in jspcxt.found can reference
		 * untoasted copies of jsonb and jsonpath arguments.
		 */

		funcctx->user_fctx = JsonValueListGetList(&jspcxt.found);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	found = funcctx->user_fctx;

	c = list_head(found);

	if (c == NULL)
		SRF_RETURN_DONE(funcctx);

	v = lfirst(c);
	funcctx->user_fctx = list_delete_first(found);

	res = isJsonb ?
		JsonbPGetDatum(JsonItemToJsonb(v)) :
		JsonPGetDatum(JsonItemToJson(v));

	SRF_RETURN_NEXT(funcctx, res);
}

Datum
jsonb_path_query(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_internal(fcinfo, true, false);
}

Datum
json_path_query(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_internal(fcinfo, false, false);
}

Datum
jsonb_path_query_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_internal(fcinfo, true, true);
}

Datum
json_path_query_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_internal(fcinfo, false, true);
}

/*
 * json[b]_path_query_array
 *		Executes jsonpath for given jsonb document and returns result as
 *		jsonb array.
 */
static Datum
jsonx_path_query_array_internal(FunctionCallInfo fcinfo, bool isJsonb, bool tz)
{
	JsonPathUserFuncContext cxt;
	Datum		res;

	(void) executeUserFunc(fcinfo, &cxt, isJsonb, false, tz);

	res = JsonbValueToJsonxDatum(wrapItemsInArray(&cxt.found, isJsonb), isJsonb);

	freeUserFuncContext(&cxt);

	PG_RETURN_DATUM(res);
}

Datum
jsonb_path_query_array(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_array_internal(fcinfo, true, false);
}

Datum
json_path_query_array(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_array_internal(fcinfo, false, false);
}

Datum
jsonb_path_query_array_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_array_internal(fcinfo, true, true);
}

Datum
json_path_query_array_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_array_internal(fcinfo, false, true);
}

/*
 * json[b]_path_query_first
 *		Executes jsonpath for given jsonb document and returns first result
 *		item.  If there are no items, NULL returned.
 */
static Datum
jsonx_path_query_first_internal(FunctionCallInfo fcinfo, bool isJsonb, bool tz)
{
	JsonPathUserFuncContext cxt;
	Datum		res;

	(void) executeUserFunc(fcinfo, &cxt, isJsonb, false, tz);

	if (JsonValueListLength(&cxt.found) >= 1)
		res = JsonItemToJsonxDatum(JsonValueListHead(&cxt.found), isJsonb);
	else
		res = (Datum) 0;

	freeUserFuncContext(&cxt);

	if (res)
		PG_RETURN_DATUM(res);
	else
		PG_RETURN_NULL();
}

Datum
jsonb_path_query_first(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_internal(fcinfo, true, false);
}

Datum
json_path_query_first(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_internal(fcinfo, false, false);
}

Datum
jsonb_path_query_first_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_internal(fcinfo, true, true);
}

Datum
json_path_query_first_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_internal(fcinfo, false, true);
}

/*
 * json[b]_path_query_first_text
 *		Executes jsonpath for given jsonb document and returns first result
 *		item as text.  If there are no items, NULL returned.
 */
static Datum
jsonx_path_query_first_text_internal(FunctionCallInfo fcinfo, bool isJsonb, bool tz)
{
	JsonPathUserFuncContext cxt;
	text	   *txt;

	(void) executeUserFunc(fcinfo, &cxt, isJsonb, false, tz);

	if (JsonValueListLength(&cxt.found) >= 1)
		txt = JsonItemUnquoteText(JsonValueListHead(&cxt.found), isJsonb);
	else
		txt = NULL;

	freeUserFuncContext(&cxt);

	if (txt)
		PG_RETURN_TEXT_P(txt);
	else
		PG_RETURN_NULL();
}

Datum
jsonb_path_query_first_text(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_text_internal(fcinfo, true, false);
}

Datum
json_path_query_first_text(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_text_internal(fcinfo, false, false);
}

Datum
jsonb_path_query_first_text_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_text_internal(fcinfo, true, true);
}

Datum
json_path_query_first_text_tz(PG_FUNCTION_ARGS)
{
	return jsonx_path_query_first_text_internal(fcinfo, false, true);
}

/* Free untoasted copies of jsonb and jsonpath arguments. */
static void
freeUserFuncContext(JsonPathUserFuncContext *cxt)
{
	FunctionCallInfo fcinfo = cxt->fcinfo;

	PG_FREE_IF_COPY(cxt->js, 0);
	PG_FREE_IF_COPY(cxt->jp, 1);
	if (cxt->vars)
		PG_FREE_IF_COPY(cxt->vars, 2);
	if (cxt->json)
		pfree(cxt->json);
}

/*
 * Common code for jsonb_path_*(jsonb, jsonpath [, vars jsonb, silent bool])
 * user functions.
 *
 * 'copy' flag enables copying of first three arguments into the current memory
 * context.
 */
static JsonPathExecResult
executeUserFunc(FunctionCallInfo fcinfo, JsonPathUserFuncContext *cxt,
				bool isJsonb, bool copy, bool useTz)
{
	Datum		js_toasted = PG_GETARG_DATUM(0);
	struct varlena *js_detoasted = copy ?
		PG_DETOAST_DATUM(js_toasted) :
		PG_DETOAST_DATUM_COPY(js_toasted);
	Jsonx	   *js = DatumGetJsonxP(js_detoasted, isJsonb);
	JsonPath   *jp = copy ? PG_GETARG_JSONPATH_P_COPY(1) : PG_GETARG_JSONPATH_P(1);
	struct varlena *vars_detoasted = NULL;
	Jsonx	   *vars = NULL;
	bool		silent = true;
	JsonPathExecResult res;

	if (PG_NARGS() == 4)
	{
		Datum		vars_toasted = PG_GETARG_DATUM(2);

		vars_detoasted = copy ?
			PG_DETOAST_DATUM(vars_toasted) :
			PG_DETOAST_DATUM_COPY(vars_toasted);

		vars = DatumGetJsonxP(vars_detoasted, isJsonb);

		silent = PG_GETARG_BOOL(3);
	}

	if (cxt)
	{
		cxt->fcinfo = fcinfo;
		cxt->js = js_detoasted;
		cxt->jp = jp;
		cxt->vars = vars_detoasted;
		cxt->json = isJsonb ? NULL : &js->js;
		cxt->silent = silent;
		memset(&cxt->found, 0, sizeof(cxt->found));
	}

	res = executeJsonPath(jp, vars, getJsonPathVariableFromJsonx,
						  js, isJsonb, !silent, cxt ? &cxt->found : NULL, useTz);

	if (!cxt && !copy)
	{
		PG_FREE_IF_COPY(js_detoasted, 0);
		PG_FREE_IF_COPY(jp, 1);

		if (vars_detoasted)
			PG_FREE_IF_COPY(vars_detoasted, 2);

		if (!isJsonb)
		{
			pfree(js);
			if (vars)
				pfree(vars);
		}
	}

	return res;
}

/********************Execute functions for JsonPath**************************/

/*
 * Interface to jsonpath executor
 *
 * 'path' - jsonpath to be executed
 * 'vars' - variables to be substituted to jsonpath
 * 'json' - target document for jsonpath evaluation
 * 'throwErrors' - whether we should throw suppressible errors
 * 'result' - list to store result items into
 *
 * Returns an error if a recoverable error happens during processing, or NULL
 * on no error.
 *
 * Note, jsonb and jsonpath values should be available and untoasted during
 * work because JsonPathItem, JsonbValue and result item could have pointers
 * into input values.  If caller needs to just check if document matches
 * jsonpath, then it doesn't provide a result arg.  In this case executor
 * works till first positive result and does not check the rest if possible.
 * In other case it tries to find all the satisfied result items.
 */
static JsonPathExecResult
executeJsonPath(JsonPath *path, void *vars, JsonPathVarCallback getVar,
				Jsonx *json, bool isJsonb, bool throwErrors,
				JsonValueList *result, bool useTz)
{
	JsonPathExecContext cxt;
	JsonPathExecResult res;
	JsonPathItem jsp;
	JsonItem	jsi;
	JsonbValue *jbv = JsonItemJbv(&jsi);
	JsonItemStackEntry root;

	jspInit(&jsp, path);

	if (isJsonb)
	{
		if (!JsonbExtractScalar(&json->jb.root, jbv))
			JsonbInitBinary(jbv, &json->jb);
	}
	else
	{
		if (!JsonExtractScalar(&json->js.root, jbv))
			JsonInitBinary(jbv, &json->js);
	}

	cxt.vars = vars;
	cxt.getVar = getVar;
	cxt.laxMode = (path->header & JSONPATH_LAX) != 0;
	cxt.ignoreStructuralErrors = cxt.laxMode;
	cxt.root = &jsi;
	cxt.stack = NULL;
	cxt.baseObject.jbc = NULL;
	cxt.baseObject.id = 0;
	/* 1 + number of base objects in vars */
	cxt.lastGeneratedObjectId = 1 + getVar(vars, isJsonb, NULL, 0, NULL, NULL);
	cxt.innermostArraySize = -1;
	cxt.throwErrors = throwErrors;
	cxt.useTz = useTz;
	cxt.isJsonb = isJsonb;

	pushJsonItem(&cxt.stack, &root, cxt.root);

	if (jspStrictAbsenseOfErrors(&cxt) && !result)
	{
		/*
		 * In strict mode we must get a complete list of values to check that
		 * there are no errors at all.
		 */
		JsonValueList vals = {0};

		res = executeItem(&cxt, &jsp, &jsi, &vals);

		if (jperIsError(res))
			return res;

		return JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
	}

	res = executeItem(&cxt, &jsp, &jsi, result);

	Assert(!throwErrors || !jperIsError(res));

	return res;
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
static JsonPathExecResult
executeItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
			JsonItem *jb, JsonValueList *found)
{
	return executeItemOptUnwrapTarget(cxt, jsp, jb, found, jspAutoUnwrap(cxt));
}

/*
 * Main jsonpath executor function: walks on jsonpath structure, finds
 * relevant parts of jsonb and evaluates expressions over them.
 * When 'unwrap' is true current SQL/JSON item is unwrapped if it is an array.
 */
static JsonPathExecResult
executeItemOptUnwrapTarget(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonItem *jb, JsonValueList *found, bool unwrap)
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
				JsonPathBool st = executeBoolItem(cxt, jsp, jb, true);

				res = appendBoolResult(cxt, jsp, found, st);
				break;
			}

		case jpiKey:
			if (JsonbType(jb) == jbvObject)
			{
				JsonItem	val;
				int			keylen;
				char	   *key = jspGetString(jsp, &keylen);

				jb = getJsonObjectKey(jb, key, keylen, cxt->isJsonb, &val);

				if (jb != NULL)
				{
					res = executeNextItem(cxt, jsp, NULL, jb, found, true);
				}
				else if (!jspIgnoreStructuralErrors(cxt))
				{
					Assert(found);

					if (!jspThrowErrors(cxt))
						return jperError;

					ereport(ERROR,
							(errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND),
							 errmsg("JSON object does not contain key \"%s\"",
									pnstrdup(key, keylen))));
				}
			}
			else if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND),
									  errmsg("jsonpath member accessor can only be applied to an object"))));
			}
			break;

		case jpiRoot:
			jb = cxt->root;
			baseObject = setBaseObject(cxt, jb, 0);
			res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			cxt->baseObject = baseObject;
			break;

		case jpiCurrent:
			res = executeNextItem(cxt, jsp, NULL, cxt->stack->item,
								  found, true);
			break;

		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				bool		hasNext = jspGetNext(jsp, &elem);

				res = executeItemUnwrapTargetArray(cxt, hasNext ? &elem : NULL,
												   jb, found, jspAutoUnwrap(cxt));
			}
			else if (jspAutoWrap(cxt))
				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			else if (!jspIgnoreStructuralErrors(cxt))
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
									  errmsg("jsonpath wildcard array accessor can only be applied to an array"))));
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray || jspAutoWrap(cxt))
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = JsonxArraySize(jb, cxt->isJsonb);
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
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
											  errmsg("jsonpath array subscript is out of bounds"))));

					if (index_from < 0)
						index_from = 0;

					if (index_to >= size)
						index_to = size - 1;

					res = jperNotFound;

					for (index = index_from; index <= index_to; index++)
					{
						JsonItem	jsibuf;
						JsonItem   *jsi;

						if (singleton)
						{
							jsi = jb;
						}
						else
						{
							jsi = getJsonArrayElement(jb, (uint32) index,
													  cxt->isJsonb, &jsibuf);

							if (jsi == NULL)
								continue;
						}

						if (!hasNext && !found)
							return jperOk;

						res = executeNextItem(cxt, jsp, &elem, jsi, found,
											  true);

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
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
									  errmsg("jsonpath array accessor can only be applied to an array"))));
			}
			break;

		case jpiLast:
			{
				JsonItem	tmpjsi;
				JsonItem   *lastjsi;
				int			last;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (cxt->innermostArraySize < 0)
					elog(ERROR, "evaluating jsonpath LAST outside of array subscript");

				if (!hasNext && !found)
				{
					res = jperOk;
					break;
				}

				last = cxt->innermostArraySize - 1;

				lastjsi = hasNext ? &tmpjsi : palloc(sizeof(*lastjsi));

				JsonItemInitNumericDatum(lastjsi,
										 DirectFunctionCall1(int4_numeric,
															 Int32GetDatum(last)));

				res = executeNextItem(cxt, jsp, &elem, lastjsi, found, hasNext);
			}
			break;

		case jpiAnyKey:
			if (JsonbType(jb) == jbvObject)
			{
				bool		hasNext = jspGetNext(jsp, &elem);

				if (!JsonItemIsBinary(jb))
					elog(ERROR, "invalid jsonb object type: %d",
						 JsonItemGetType(jb));

				return executeAnyItem
					(cxt, hasNext ? &elem : NULL,
					 JsonItemBinary(jb).data, found, 1, 1, 1,
					 false, jspAutoUnwrap(cxt));
			}
			else if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND),
									  errmsg("jsonpath wildcard member accessor can only be applied to an object"))));
			}
			break;

		case jpiAdd:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_add_opt_error, found);

		case jpiSub:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_sub_opt_error, found);

		case jpiMul:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_mul_opt_error, found);

		case jpiDiv:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_div_opt_error, found);

		case jpiMod:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_mod_opt_error, found);

		case jpiPlus:
			return executeUnaryArithmExpr(cxt, jsp, jb, NULL, found);

		case jpiMinus:
			return executeUnaryArithmExpr(cxt, jsp, jb, numeric_uminus,
										  found);

		case jpiFilter:
			{
				JsonPathBool st;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				jspGetArg(jsp, &elem);
				st = executeNestedBoolItem(cxt, &elem, jb);
				if (st != jpbTrue)
					res = jperNotFound;
				else
					res = executeNextItem(cxt, jsp, NULL,
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
					res = executeNextItem(cxt, jsp, &elem,
										  jb, found, true);
					cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;

					if (res == jperOk && !found)
						break;
				}

				if (JsonItemIsBinary(jb))
					res = executeAnyItem
						(cxt, hasNext ? &elem : NULL,
						 JsonItemBinary(jb).data, found,
						 1,
						 jsp->content.anybounds.first,
						 jsp->content.anybounds.last,
						 true, jspAutoUnwrap(cxt));
				break;
			}

		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			{
				JsonItem	vbuf;
				JsonItem   *v;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
				{
					res = jperOk;	/* skip evaluation */
					break;
				}
				v = hasNext ? &vbuf : palloc(sizeof(*v));

				baseObject = cxt->baseObject;
				getJsonPathItem(cxt, jsp, v);

				res = executeNextItem(cxt, jsp, &elem,
									  v, found, hasNext);
				cxt->baseObject = baseObject;
			}
			break;

		case jpiType:
			{
				JsonItem	jsi;
				const char *typname = JsonItemTypeName(jb);

				JsonItemInitString(&jsi, pstrdup(typname), strlen(typname));

				res = executeNextItem(cxt, jsp, NULL, &jsi, found, true);
			}
			break;

		case jpiSize:
			{
				int			size = JsonxArraySize(jb, cxt->isJsonb);

				if (size < 0)
				{
					if (!jspAutoWrap(cxt))
					{
						if (!jspIgnoreStructuralErrors(cxt))
							RETURN_ERROR(ereport(ERROR,
												 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
												  errmsg("jsonpath item method .%s() can only be applied to an array",
														 jspOperationName(jsp->type)))));
						break;
					}

					size = 1;
				}

				jb = palloc(sizeof(*jb));

				JsonItemInitNumericDatum(jb, DirectFunctionCall1(int4_numeric,
																 Int32GetDatum(size)));

				res = executeNextItem(cxt, jsp, NULL, jb, found, false);
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
				JsonItem	jsi;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				if (JsonItemIsNumeric(jb))
				{
					char	   *tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
																		  JsonItemNumericDatum(jb)));
					bool		have_error = false;

					(void) float8in_internal_opt_error(tmp,
													   NULL,
													   "double precision",
													   tmp,
													   &have_error);

					if (have_error)
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("jsonpath item method .%s() can only be applied to a numeric value",
													 jspOperationName(jsp->type)))));
					res = jperOk;
				}
				else if (JsonItemIsString(jb))
				{
					/* cast string as double */
					double		val;
					char	   *tmp = pnstrdup(JsonItemString(jb).val,
											   JsonItemString(jb).len);
					bool		have_error = false;

					val = float8in_internal_opt_error(tmp,
													  NULL,
													  "double precision",
													  tmp,
													  &have_error);

					if (have_error || isinf(val))
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("jsonpath item method .%s() can only be applied to a numeric value",
													 jspOperationName(jsp->type)))));

					jb = &jsi;
					JsonItemInitNumericDatum(jb, DirectFunctionCall1(float8_numeric,
																	 Float8GetDatum(val)));
					res = jperOk;
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
												 jspOperationName(jsp->type)))));

				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiDatetime:
			if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

			return executeDateTimeMethod(cxt, jsp, jb, found);

		case jpiKeyValue:
			if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

			return executeKeyValueMethod(cxt, jsp, jb, found);

		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
	}

	return res;
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
static JsonPathExecResult
executeItemUnwrapTargetArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
							 JsonItem *jb, JsonValueList *found,
							 bool unwrapElements)
{
	if (!JsonItemIsBinary(jb))
	{
		Assert(!JsonItemIsArray(jb));
		elog(ERROR, "invalid jsonb array value type: %d", JsonItemGetType(jb));
	}

	return executeAnyItem
		(cxt, jsp, JsonItemBinary(jb).data, found, 1, 1, 1,
		 false, unwrapElements);
}

/*
 * Execute next jsonpath item if exists.  Otherwise put "v" to the "found"
 * list if provided.
 */
static JsonPathExecResult
executeNextItem(JsonPathExecContext *cxt,
				JsonPathItem *cur, JsonPathItem *next,
				JsonItem *v, JsonValueList *found, bool copy)
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
		return executeItem(cxt, next, v, found);

	if (found)
		JsonValueListAppend(found, copy ? copyJsonItem(v) : v);

	return jperOk;
}

/*
 * Same as executeItem(), but when "unwrap == true" automatically unwraps
 * each array item from the resulting sequence in lax mode.
 */
static JsonPathExecResult
executeItemOptUnwrapResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonItem *jb, bool unwrap,
						   JsonValueList *found)
{
	if (unwrap && jspAutoUnwrap(cxt))
	{
		JsonValueList seq = {0};
		JsonValueListIterator it;
		JsonPathExecResult res = executeItem(cxt, jsp, jb, &seq);
		JsonItem   *item;
		int			count;

		if (jperIsError(res))
			return res;

		count = JsonValueListLength(&seq);

		if (!count)
			return jperNotFound;

		/* Optimize copying of singleton item into empty list */
		if (count == 1 &&
			JsonbType((item = JsonValueListHead(&seq))) != jbvArray)
		{
			if (JsonValueListIsEmpty(found))
				*found = seq;
			else
				JsonValueListAppend(found, item);

			return jperOk;
		}

		JsonValueListInitIterator(&seq, &it);
		while ((item = JsonValueListNext(&seq, &it)))
		{
			Assert(!JsonItemIsArray(item));

			if (JsonbType(item) == jbvArray)
				executeItemUnwrapTargetArray(cxt, NULL, item, found, false);
			else
				JsonValueListAppend(found, item);
		}

		return jperOk;
	}

	return executeItem(cxt, jsp, jb, found);
}

/*
 * Same as executeItemOptUnwrapResult(), but with error suppression.
 */
static JsonPathExecResult
executeItemOptUnwrapResultNoThrow(JsonPathExecContext *cxt,
								  JsonPathItem *jsp,
								  JsonItem *jb, bool unwrap,
								  JsonValueList *found)
{
	JsonPathExecResult res;
	bool		throwErrors = cxt->throwErrors;

	cxt->throwErrors = false;
	res = executeItemOptUnwrapResult(cxt, jsp, jb, unwrap, found);
	cxt->throwErrors = throwErrors;

	return res;
}

/* Execute boolean-valued jsonpath expression. */
static JsonPathBool
executeBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
				JsonItem *jb, bool canHaveNext)
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
			res = executeBoolItem(cxt, &larg, jb, false);

			if (res == jpbFalse)
				return jpbFalse;

			/*
			 * SQL/JSON says that we should check second arg in case of
			 * jperError
			 */

			jspGetRightArg(jsp, &rarg);
			res2 = executeBoolItem(cxt, &rarg, jb, false);

			return res2 == jpbTrue ? res : res2;

		case jpiOr:
			jspGetLeftArg(jsp, &larg);
			res = executeBoolItem(cxt, &larg, jb, false);

			if (res == jpbTrue)
				return jpbTrue;

			jspGetRightArg(jsp, &rarg);
			res2 = executeBoolItem(cxt, &rarg, jb, false);

			return res2 == jpbFalse ? res : res2;

		case jpiNot:
			jspGetArg(jsp, &larg);

			res = executeBoolItem(cxt, &larg, jb, false);

			if (res == jpbUnknown)
				return jpbUnknown;

			return res == jpbTrue ? jpbFalse : jpbTrue;

		case jpiIsUnknown:
			jspGetArg(jsp, &larg);
			res = executeBoolItem(cxt, &larg, jb, false);
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
									executeComparison, cxt);

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
				executeItemOptUnwrapResultNoThrow(cxt, &larg, jb,
												  false, &vals);

				if (jperIsError(res))
					return jpbUnknown;

				return JsonValueListIsEmpty(&vals) ? jpbFalse : jpbTrue;
			}
			else
			{
				JsonPathExecResult res =
				executeItemOptUnwrapResultNoThrow(cxt, &larg, jb,
												  false, NULL);

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
static JsonPathBool
executeNestedBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
					  JsonItem *jb)
{
	JsonItemStackEntry current;
	JsonPathBool res;

	pushJsonItem(&cxt->stack, &current, jb);
	res = executeBoolItem(cxt, jsp, jb, false);
	popJsonItem(&cxt->stack);

	return res;
}

/*
 * Implementation of several jsonpath nodes:
 *  - jpiAny (.** accessor),
 *  - jpiAnyKey (.* accessor),
 *  - jpiAnyArray ([*] accessor)
 */
static JsonPathExecResult
executeAnyItem(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbContainer *jbc,
			   JsonValueList *found, uint32 level, uint32 first, uint32 last,
			   bool ignoreStructuralErrors, bool unwrapNext)
{
	JsonPathExecResult res = jperNotFound;
	JsonxIterator it;
	int32		r;
	JsonItem	v;

	check_stack_depth();

	if (level > last)
		return res;


	JsonxIteratorInit(&it, jbc, cxt->isJsonb);

	/*
	 * Recursively iterate over jsonb objects/arrays
	 */
	while ((r = JsonxIteratorNext(&it, JsonItemJbv(&v), true)) != WJB_DONE)
	{
		if (r == WJB_KEY)
		{
			r = JsonxIteratorNext(&it, JsonItemJbv(&v), true);
			Assert(r == WJB_VALUE);
		}

		if (r == WJB_VALUE || r == WJB_ELEM)
		{

			if (level >= first ||
				(first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
				 !JsonItemIsBinary(&v)))	/* leaves only requested */
			{
				/* check expression */
				if (jsp)
				{
					if (ignoreStructuralErrors)
					{
						bool		savedIgnoreStructuralErrors;

						savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
						cxt->ignoreStructuralErrors = true;
						res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);
						cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;
					}
					else
						res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}
				else if (found)
					JsonValueListAppend(found, copyJsonItem(&v));
				else
					return jperOk;
			}

			if (level < last && JsonItemIsBinary(&v))
			{
				res = executeAnyItem
					(cxt, jsp, JsonItemBinary(&v).data, found,
					 level + 1, first, last,
					 ignoreStructuralErrors, unwrapNext);

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
 * Execute unary or binary predicate.
 *
 * Predicates have existence semantics, because their operands are item
 * sequences.  Pairs of items from the left and right operand's sequences are
 * checked.  TRUE returned only if any pair satisfying the condition is found.
 * In strict mode, even if the desired pair has already been found, all pairs
 * still need to be examined to check the absence of errors.  If any error
 * occurs, UNKNOWN (analogous to SQL NULL) is returned.
 */
static JsonPathBool
executePredicate(JsonPathExecContext *cxt, JsonPathItem *pred,
				 JsonPathItem *larg, JsonPathItem *rarg, JsonItem *jb,
				 bool unwrapRightArg, JsonPathPredicateCallback exec,
				 void *param)
{
	JsonPathExecResult res;
	JsonValueListIterator lseqit;
	JsonValueList lseq = {0};
	JsonValueList rseq = {0};
	JsonItem *lval;
	bool		error = false;
	bool		found = false;

	/* Left argument is always auto-unwrapped. */
	res = executeItemOptUnwrapResultNoThrow(cxt, larg, jb, true, &lseq);
	if (jperIsError(res))
		return jpbUnknown;

	if (rarg)
	{
		/* Right argument is conditionally auto-unwrapped. */
		res = executeItemOptUnwrapResultNoThrow(cxt, rarg, jb,
												unwrapRightArg, &rseq);
		if (jperIsError(res))
			return jpbUnknown;
	}

	JsonValueListInitIterator(&lseq, &lseqit);
	while ((lval = JsonValueListNext(&lseq, &lseqit)))
	{
		JsonValueListIterator rseqit;
		JsonItem   *rval;
		bool		first = true;

		JsonValueListInitIterator(&rseq, &rseqit);
		if (rarg)
			rval = JsonValueListNext(&rseq, &rseqit);
		else
			rval = NULL;

		/* Loop over right arg sequence or do single pass otherwise */
		while (rarg ? (rval != NULL) : first)
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

			first = false;
			if (rarg)
				rval = JsonValueListNext(&rseq, &rseqit);
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
						JsonItem *jb, BinaryArithmFunc func,
						JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathItem elem;
	JsonValueList lseq = {0};
	JsonValueList rseq = {0};
	JsonItem   *lval;
	JsonItem   *rval;
	Numeric		res;

	jspGetLeftArg(jsp, &elem);

	/*
	 * XXX: By standard only operands of multiplicative expressions are
	 * unwrapped.  We extend it to other binary arithmetic expressions too.
	 */
	jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &lseq);
	if (jperIsError(jper))
		return jper;

	jspGetRightArg(jsp, &elem);

	jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &rseq);
	if (jperIsError(jper))
		return jper;

	if (JsonValueListLength(&lseq) != 1 ||
		!(lval = getScalar(JsonValueListHead(&lseq), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
							  errmsg("left operand of jsonpath operator %s is not a single numeric value",
									 jspOperationName(jsp->type)))));

	if (JsonValueListLength(&rseq) != 1 ||
		!(rval = getScalar(JsonValueListHead(&rseq), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
							  errmsg("right operand of jsonpath operator %s is not a single numeric value",
									 jspOperationName(jsp->type)))));

	if (jspThrowErrors(cxt))
	{
		res = func(JsonItemNumeric(lval), JsonItemNumeric(rval), NULL);
	}
	else
	{
		bool		error = false;

		res = func(JsonItemNumeric(lval), JsonItemNumeric(rval), &error);

		if (error)
			return jperError;
	}

	if (!jspGetNext(jsp, &elem) && !found)
		return jperOk;

	lval = palloc(sizeof(*lval));
	JsonItemInitNumeric(lval, res);

	return executeNextItem(cxt, jsp, &elem, lval, found, false);
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonItem *jb, PGFunction func, JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathExecResult jper2;
	JsonPathItem elem;
	JsonValueList seq = {0};
	JsonValueListIterator it;
	JsonItem   *val;
	bool		hasNext;

	jspGetArg(jsp, &elem);
	jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &seq);

	if (jperIsError(jper))
		return jper;

	jper = jperNotFound;

	hasNext = jspGetNext(jsp, &elem);

	JsonValueListInitIterator(&seq, &it);
	while ((val = JsonValueListNext(&seq, &it)))
	{
		if ((val = getScalar(val, jbvNumeric)))
		{
			if (!found && !hasNext)
				return jperOk;
		}
		else
		{
			if (!found && !hasNext)
				continue;		/* skip non-numerics processing */

			RETURN_ERROR(ereport(ERROR,
								 (errcode(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND),
								  errmsg("operand of unary jsonpath operator %s is not a numeric value",
										 jspOperationName(jsp->type)))));
		}

		if (func)
			JsonItemNumeric(val) =
				DatumGetNumeric(DirectFunctionCall1(func,
													NumericGetDatum(JsonItemNumeric(val))));

		jper2 = executeNextItem(cxt, jsp, &elem, val, found, false);

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
 * STARTS_WITH predicate callback.
 *
 * Check if the 'whole' string starts from 'initial' string.
 */
static JsonPathBool
executeStartsWith(JsonPathItem *jsp, JsonItem *whole, JsonItem *initial,
				  void *param)
{
	if (!(whole = getScalar(whole, jbvString)))
		return jpbUnknown;		/* error */

	if (!(initial = getScalar(initial, jbvString)))
		return jpbUnknown;		/* error */

	if (JsonItemString(whole).len >= JsonItemString(initial).len &&
		!memcmp(JsonItemString(whole).val,
				JsonItemString(initial).val,
				JsonItemString(initial).len))
		return jpbTrue;

	return jpbFalse;
}

/*
 * LIKE_REGEX predicate callback.
 *
 * Check if the string matches regex pattern.
 */
static JsonPathBool
executeLikeRegex(JsonPathItem *jsp, JsonItem *str, JsonItem *rarg,
				 void *param)
{
	JsonLikeRegexContext *cxt = param;

	if (!(str = getScalar(str, jbvString)))
		return jpbUnknown;

	/* Cache regex text and converted flags. */
	if (!cxt->regex)
	{
		cxt->regex =
			cstring_to_text_with_len(jsp->content.like_regex.pattern,
									 jsp->content.like_regex.patternlen);
		cxt->cflags = jspConvertRegexFlags(jsp->content.like_regex.flags);
	}

	if (RE_compile_and_execute(cxt->regex, JsonItemString(str).val,
							   JsonItemString(str).len,
							   cxt->cflags, DEFAULT_COLLATION_OID, 0, NULL))
		return jpbTrue;

	return jpbFalse;
}

/*
 * Execute numeric item methods (.abs(), .floor(), .ceil()) using the specified
 * user function 'func'.
 */
static JsonPathExecResult
executeNumericItemMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
						 JsonItem *jb, bool unwrap, PGFunction func,
						 JsonValueList *found)
{
	JsonPathItem next;
	Datum		datum;

	if (unwrap && JsonbType(jb) == jbvArray)
		return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

	if (!(jb = getScalar(jb, jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
							  errmsg("jsonpath item method .%s() can only be applied to a numeric value",
									 jspOperationName(jsp->type)))));

	datum = DirectFunctionCall1(func, JsonItemNumericDatum(jb));

	if (!jspGetNext(jsp, &next) && !found)
		return jperOk;

	jb = palloc(sizeof(*jb));
	JsonItemInitNumericDatum(jb, datum);

	return executeNextItem(cxt, jsp, &next, jb, found, false);
}

/*
 * Implementation of the .datetime() method.
 *
 * Converts a string into a date/time value. The actual type is determined at run time.
 * If an argument is provided, this argument is used as a template string.
 * Otherwise, the first fitting ISO format is selected.
 */
static JsonPathExecResult
executeDateTimeMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
					  JsonItem *jb, JsonValueList *found)
{
	JsonItem	jbvbuf;
	Datum		value;
	text	   *datetime;
	Oid			typid;
	int32		typmod = -1;
	int			tz = 0;
	bool		hasNext;
	JsonPathExecResult res = jperNotFound;
	JsonPathItem elem;

	if (!(jb = getScalar(jb, jbvString)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION),
							  errmsg("jsonpath item method .%s() can only be applied to a string",
									 jspOperationName(jsp->type)))));

	datetime = cstring_to_text_with_len(JsonItemString(jb).val,
										JsonItemString(jb).len);

	if (jsp->content.arg)
	{
		text	   *template;
		char	   *template_str;
		int			template_len;
		bool		have_error = false;

		jspGetArg(jsp, &elem);

		if (elem.type != jpiString)
			elog(ERROR, "invalid jsonpath item type for .datetime() argument");

		template_str = jspGetString(&elem, &template_len);

		template = cstring_to_text_with_len(template_str,
											template_len);

		value = parse_datetime(datetime, template, true,
							   &typid, &typmod, &tz,
							   jspThrowErrors(cxt) ? NULL : &have_error);

		if (have_error)
			res = jperError;
		else
			res = jperOk;
	}
	else
	{
		/*
		 * According to SQL/JSON standard enumerate ISO formats for: date,
		 * timetz, time, timestamptz, timestamp.
		 */
		static const char *fmt_str[] =
		{
			"yyyy-mm-dd",
			"HH24:MI:SS TZH:TZM",
			"HH24:MI:SS TZH",
			"HH24:MI:SS",
			"yyyy-mm-dd HH24:MI:SS TZH:TZM",
			"yyyy-mm-dd HH24:MI:SS TZH",
			"yyyy-mm-dd HH24:MI:SS"
		};

		/* cache for format texts */
		static text *fmt_txt[lengthof(fmt_str)] = {0};
		int			i;

		/* loop until datetime format fits */
		for (i = 0; i < lengthof(fmt_str); i++)
		{
			bool		have_error = false;

			if (!fmt_txt[i])
			{
				MemoryContext oldcxt =
				MemoryContextSwitchTo(TopMemoryContext);

				fmt_txt[i] = cstring_to_text(fmt_str[i]);
				MemoryContextSwitchTo(oldcxt);
			}

			value = parse_datetime(datetime, fmt_txt[i], true,
								   &typid, &typmod, &tz,
								   &have_error);

			if (!have_error)
			{
				res = jperOk;
				break;
			}
		}

		if (res == jperNotFound)
			RETURN_ERROR(ereport(ERROR,
								 (errcode(ERRCODE_INVALID_ARGUMENT_FOR_JSON_DATETIME_FUNCTION),
								  errmsg("datetime format is not unrecognized"),
								  errhint("use datetime template argument for explicit format specification"))));
	}

	pfree(datetime);

	if (jperIsError(res))
		return res;

	hasNext = jspGetNext(jsp, &elem);

	if (!hasNext && !found)
		return res;

	jb = hasNext ? &jbvbuf : palloc(sizeof(*jb));

	JsonItemInitDatetime(jb, value, typid, typmod, tz);

	return executeNextItem(cxt, jsp, &elem, jb, found, hasNext);
}

/*
 * Implementation of .keyvalue() method.
 *
 * .keyvalue() method returns a sequence of object's key-value pairs in the
 * following format: '{ "key": key, "value": value, "id": id }'.
 *
 * "id" field is an object identifier which is constructed from the two parts:
 * base object id and its binary offset in base object's jsonb:
 * id = 10000000000 * base_object_id + obj_offset_in_base_object
 *
 * 10000000000 (10^10) -- is a first round decimal number greater than 2^32
 * (maximal offset in jsonb).  Decimal multiplier is used here to improve the
 * readability of identifiers.
 *
 * Base object is usually a root object of the path: context item '$' or path
 * variable '$var', literals can't produce objects for now.  But if the path
 * contains generated objects (.keyvalue() itself, for example), then they
 * become base object for the subsequent .keyvalue().
 *
 * Id of '$' is 0. Id of '$var' is its ordinal (positive) number in the list
 * of variables (see getJsonPathVariable()).  Ids for generated objects
 * are assigned using global counter JsonPathExecContext.lastGeneratedObjectId.
 */
static JsonPathExecResult
executeKeyValueMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
					  JsonItem *jb, JsonValueList *found)
{
	JsonPathExecResult res = jperNotFound;
	JsonPathItem next;
	JsonbContainer *jbc;
	JsonbValue	key;
	JsonbValue	val;
	JsonbValue	idval;
	JsonbValue	keystr;
	JsonbValue	valstr;
	JsonbValue	idstr;
	JsonxIterator it;
	JsonbIteratorToken tok;
	JsonBuilderFunc push;
	int64		id;
	bool		hasNext;

	if (JsonbType(jb) != jbvObject || !JsonItemIsBinary(jb))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND),
							  errmsg("jsonpath item method .%s() can only be applied to an object",
									 jspOperationName(jsp->type)))));

	jbc = JsonItemBinary(jb).data;

	if (!JsonContainerSize(jbc))
		return jperNotFound;	/* no key-value pairs */

	hasNext = jspGetNext(jsp, &next);

	keystr.type = jbvString;
	keystr.val.string.val = "key";
	keystr.val.string.len = 3;

	valstr.type = jbvString;
	valstr.val.string.val = "value";
	valstr.val.string.len = 5;

	idstr.type = jbvString;
	idstr.val.string.val = "id";
	idstr.val.string.len = 2;

	/* construct object id from its base object and offset inside that */
	id = cxt->isJsonb ?
		(int64) ((char *)(JsonContainer *) jbc -
				 (char *)(JsonContainer *) cxt->baseObject.jbc) :
		(int64) (((JsonContainer *) jbc)->data -
				 ((JsonContainer *) cxt->baseObject.jbc)->data);

	id += (int64) cxt->baseObject.id * INT64CONST(10000000000);

	idval.type = jbvNumeric;
	idval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
															Int64GetDatum(id)));

	push = cxt->isJsonb ? pushJsonbValue : pushJsonValue;

	JsonxIteratorInit(&it, jbc, cxt->isJsonb);

	while ((tok = JsonxIteratorNext(&it, &key, true)) != WJB_DONE)
	{
		JsonBaseObjectInfo baseObject;
		JsonItem	obj;
		JsonbParseState *ps;
		JsonbValue *keyval;
		Jsonx	   *jsonx;

		if (tok != WJB_KEY)
			continue;

		res = jperOk;

		if (!hasNext && !found)
			break;

		tok = JsonxIteratorNext(&it, &val, true);
		Assert(tok == WJB_VALUE);

		ps = NULL;
		push(&ps, WJB_BEGIN_OBJECT, NULL);

		pushJsonbValue(&ps, WJB_KEY, &keystr);
		pushJsonbValue(&ps, WJB_VALUE, &key);

		pushJsonbValue(&ps, WJB_KEY, &valstr);
		push(&ps, WJB_VALUE, &val);

		pushJsonbValue(&ps, WJB_KEY, &idstr);
		pushJsonbValue(&ps, WJB_VALUE, &idval);

		keyval = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

		jsonx = JsonbValueToJsonx(keyval, cxt->isJsonb);

		if (cxt->isJsonb)
			JsonbInitBinary(JsonItemJbv(&obj), &jsonx->jb);
		else
			JsonInitBinary(JsonItemJbv(&obj), &jsonx->js);

		baseObject = setBaseObject(cxt, &obj, cxt->lastGeneratedObjectId++);

		res = executeNextItem(cxt, jsp, &next, &obj, found, true);

		cxt->baseObject = baseObject;

		if (jperIsError(res))
			return res;

		if (res == jperOk && !found)
			break;
	}

	return res;
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next jsonpath.
 */
static JsonPathExecResult
appendBoolResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
				 JsonValueList *found, JsonPathBool res)
{
	JsonPathItem next;
	JsonItem	jsi;

	if (!jspGetNext(jsp, &next) && !found)
		return jperOk;			/* found singleton boolean value */

	if (res == jpbUnknown)
		JsonItemInitNull(&jsi);
	else
		JsonItemInitBool(&jsi, res == jpbTrue);

	return executeNextItem(cxt, jsp, &next, &jsi, found, true);
}

/*
 * Convert jsonpath's scalar or variable node to actual jsonb value.
 *
 * If node is a variable then its id returned, otherwise 0 returned.
 */
static void
getJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item,
				JsonItem *value)
{
	switch (item->type)
	{
		case jpiNull:
			JsonItemInitNull(value);
			break;
		case jpiBool:
			JsonItemInitBool(value, jspGetBool(item));
			break;
		case jpiNumeric:
			JsonItemInitNumeric(value, jspGetNumeric(item));
			break;
		case jpiString:
			{
				int			len;
				char	   *str = jspGetString(item, &len);

				JsonItemInitString(value, str, len);
				break;
			}
		case jpiVariable:
			getJsonPathVariable(cxt, item, value);
			return;
		default:
			elog(ERROR, "unexpected jsonpath item type");
	}
}

/*
 * Get the value of variable passed to jsonpath executor
 */
static void
getJsonPathVariable(JsonPathExecContext *cxt, JsonPathItem *variable,
					JsonItem *value)
{
	char	   *varName;
	int			varNameLength;
	JsonItem	baseObject;
	int			baseObjectId;

	Assert(variable->type == jpiVariable);
	varName = jspGetString(variable, &varNameLength);

	if (!cxt->vars ||
		(baseObjectId = cxt->getVar(cxt->vars, cxt->isJsonb,
									varName, varNameLength,
									value, JsonItemJbv(&baseObject))) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("could not find jsonpath variable \"%s\"",
						pnstrdup(varName, varNameLength))));

	if (baseObjectId > 0)
		setBaseObject(cxt, &baseObject, baseObjectId);
}

static int
getJsonPathVariableFromJsonx(void *varsJsonx, bool isJsonb,
							 char *varName, int varNameLength,
							 JsonItem *value, JsonbValue *baseObject)
{
	Jsonx	   *vars = varsJsonx;
	JsonbValue *val;
	JsonbValue	key;

	if (!varName)
	{
		if (vars &&
			!(isJsonb ?
			  JsonContainerIsObject(&vars->jb.root) :
			  JsonContainerIsObject(&vars->js.root)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"vars\" argument is not an object"),
					 errdetail("Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.")));

		return vars ? 1 : 0;	/* count of base objects */
	}

	if (!vars)
		return -1;

	key.type = jbvString;
	key.val.string.val = varName;
	key.val.string.len = varNameLength;

	if (isJsonb)
	{
		Jsonb	   *jb = &vars->jb;

		val = findJsonbValueFromContainer(&jb->root, JB_FOBJECT, &key);

		if (!val)
			return -1;

		JsonbInitBinary(baseObject, jb);
	}
	else
	{
		Json	   *js = &vars->js;

		val = findJsonValueFromContainer(&js->root, JB_FOBJECT, &key);

		if (!val)
			return -1;

		JsonInitBinary(baseObject, js);
	}

	*JsonItemJbv(value) = *val;
	pfree(val);

	return 1;
}

/**************** Support functions for JsonPath execution *****************/

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
static int
JsonxArraySize(JsonItem *jb, bool isJsonb)
{
	Assert(!JsonItemIsArray(jb));

	if (JsonItemIsBinary(jb))
	{
		if (isJsonb)
		{
			JsonbContainer *jbc = JsonItemBinary(jb).data;

			if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
				return JsonContainerSize(jbc);
		}
		else
		{
			JsonContainer *jc = (JsonContainer *) JsonItemBinary(jb).data;

			if (JsonContainerIsArray(jc) && !JsonContainerIsScalar(jc))
				return JsonTextContainerSize(jc);
		}
	}

	return -1;
}

/* Comparison predicate callback. */
static JsonPathBool
executeComparison(JsonPathItem *cmp, JsonItem *lv, JsonItem *rv, void *p)
{
	JsonPathExecContext *cxt = (JsonPathExecContext *) p;

	return compareItems(cmp->type, lv, rv, cxt->useTz);
}

/*
 * Perform per-byte comparison of two strings.
 */
static int
binaryCompareStrings(const char *s1, int len1,
					 const char *s2, int len2)
{
	int			cmp;

	cmp = memcmp(s1, s2, Min(len1, len2));

	if (cmp != 0)
		return cmp;

	if (len1 == len2)
		return 0;

	return len1 < len2 ? -1 : 1;
}

/*
 * Compare two strings in the current server encoding using Unicode codepoint
 * collation.
 */
static int
compareStrings(const char *mbstr1, int mblen1,
			   const char *mbstr2, int mblen2)
{
	if (GetDatabaseEncoding() == PG_SQL_ASCII ||
		GetDatabaseEncoding() == PG_UTF8)
	{
		/*
		 * It's known property of UTF-8 strings that their per-byte comparison
		 * result matches codepoints comparison result.  ASCII can be
		 * considered as special case of UTF-8.
		 */
		return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
	}
	else
	{
		char	   *utf8str1,
				   *utf8str2;
		int			cmp,
					utf8len1,
					utf8len2;

		/*
		 * We have to convert other encodings to UTF-8 first, then compare.
		 * Input strings may be not null-terminated and pg_server_to_any() may
		 * return them "as is".  So, use strlen() only if there is real
		 * conversion.
		 */
		utf8str1 = pg_server_to_any(mbstr1, mblen1, PG_UTF8);
		utf8str2 = pg_server_to_any(mbstr2, mblen2, PG_UTF8);
		utf8len1 = (mbstr1 == utf8str1) ? mblen1 : strlen(utf8str1);
		utf8len2 = (mbstr2 == utf8str2) ? mblen2 : strlen(utf8str2);

		cmp = binaryCompareStrings(utf8str1, utf8len1, utf8str2, utf8len2);

		/*
		 * If pg_server_to_any() did no real conversion, then we actually
		 * compared original strings.  So, we already done.
		 */
		if (mbstr1 == utf8str1 && mbstr2 == utf8str2)
			return cmp;

		/* Free memory if needed */
		if (mbstr1 != utf8str1)
			pfree(utf8str1);
		if (mbstr2 != utf8str2)
			pfree(utf8str2);

		/*
		 * When all Unicode codepoints are equal, return result of binary
		 * comparison.  In some edge cases, same characters may have different
		 * representations in encoding.  Then our behavior could diverge from
		 * standard.  However, that allow us to do simple binary comparison
		 * for "==" operator, which is performance critical in typical cases.
		 * In future to implement strict standard conformance, we can do
		 * normalization of input JSON strings.
		 */
		if (cmp == 0)
			return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
		else
			return cmp;
	}
}

/*
 * Compare two SQL/JSON items using comparison operation 'op'.
 */
static JsonPathBool
compareItems(int32 op, JsonItem *jsi1, JsonItem *jsi2, bool useTz)
{
	JsonbValue *jb1 = JsonItemJbv(jsi1);
	JsonbValue *jb2 = JsonItemJbv(jsi2);
	int			cmp;
	bool		res;

	if (JsonItemGetType(jsi1) != JsonItemGetType(jsi2))
	{
		if (JsonItemIsNull(jsi1) || JsonItemIsNull(jsi2))

			/*
			 * Equality and order comparison of nulls to non-nulls returns
			 * always false, but inequality comparison returns true.
			 */
			return op == jpiNotEqual ? jpbTrue : jpbFalse;

		/* Non-null items of different types are not comparable. */
		return jpbUnknown;
	}

	switch (JsonItemGetType(jsi1))
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
			if (op == jpiEqual)
				return jb1->val.string.len != jb2->val.string.len ||
					memcmp(jb1->val.string.val,
						   jb2->val.string.val,
						   jb1->val.string.len) ? jpbFalse : jpbTrue;

			cmp = compareStrings(jb1->val.string.val, jb1->val.string.len,
								 jb2->val.string.val, jb2->val.string.len);
			break;
		case jsiDatetime:
			{
				bool		cast_error;

				cmp = compareDatetime(JsonItemDatetime(jsi1).value,
									  JsonItemDatetime(jsi1).typid,
									  JsonItemDatetime(jsi2).value,
									  JsonItemDatetime(jsi2).typid,
									  useTz,
									  &cast_error);

				if (cast_error)
					return jpbUnknown;
			}
			break;

		case jbvBinary:
		case jbvArray:
		case jbvObject:
			return jpbUnknown;	/* non-scalars are not comparable */

		default:
			elog(ERROR, "invalid jsonb value type %d", JsonItemGetType(jsi1));
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

/* Compare two numerics */
static int
compareNumeric(Numeric a, Numeric b)
{
	return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
											 NumericGetDatum(a),
											 NumericGetDatum(b)));
}

static JsonItem *
copyJsonItem(JsonItem *src)
{
	JsonItem *dst = palloc(sizeof(*dst));

	*dst = *src;

	return dst;
}

static JsonItem *
JsonbValueToJsonItem(JsonbValue *jbv, JsonItem *jsi)
{
	*JsonItemJbv(jsi) = *jbv;
	return jsi;
}

static JsonbValue *
JsonItemToJsonbValue(JsonItem *jsi, JsonbValue *jbv)
{
	switch (JsonItemGetType(jsi))
	{
		case jsiDatetime:
			jbv->type = jbvString;
			jbv->val.string.val = JsonEncodeDateTime(NULL,
													 JsonItemDatetime(jsi).value,
													 JsonItemDatetime(jsi).typid,
													 &JsonItemDatetime(jsi).tz);
			jbv->val.string.len = strlen(jbv->val.string.val);
			return jbv;

		default:
			return JsonItemJbv(jsi);
	}
}

Jsonb *
JsonItemToJsonb(JsonItem *jsi)
{
	JsonbValue	jbv;

	return JsonbValueToJsonb(JsonItemToJsonbValue(jsi, &jbv));
}

static const char *
JsonItemTypeName(JsonItem *jsi)
{
	switch (JsonItemGetType(jsi))
	{
		case jsiDatetime:
			switch (JsonItemDatetime(jsi).typid)
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
					elog(ERROR, "unrecognized jsonb value datetime type: %d",
						 JsonItemDatetime(jsi).typid);
					return "unknown";
			}

		default:
			return JsonbTypeName(JsonItemJbv(jsi));
	}
}

/*
 * Execute array subscript expression and convert resulting numeric item to
 * the integer type with truncation.
 */
static JsonPathExecResult
getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonItem *jb,
			  int32 *index)
{
	JsonItem   *jbv;
	JsonValueList found = {0};
	JsonPathExecResult res = executeItem(cxt, jsp, jb, &found);
	Datum		numeric_index;
	bool		have_error = false;

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&found) != 1 ||
		!(jbv = getScalar(JsonValueListHead(&found), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
							  errmsg("jsonpath array subscript is not a single numeric value"))));

	numeric_index = DirectFunctionCall2(numeric_trunc,
										NumericGetDatum(JsonItemNumeric(jbv)),
										Int32GetDatum(0));

	*index = numeric_int4_opt_error(DatumGetNumeric(numeric_index),
									&have_error);

	if (have_error)
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
							  errmsg("jsonpath array subscript is out of integer range"))));

	return jperOk;
}

/* Save base object and its id needed for the execution of .keyvalue(). */
static JsonBaseObjectInfo
setBaseObject(JsonPathExecContext *cxt, JsonItem *jbv, int32 id)
{
	JsonBaseObjectInfo baseObject = cxt->baseObject;

	cxt->baseObject.jbc = !JsonItemIsBinary(jbv) ? NULL :
		(JsonbContainer *) JsonItemBinary(jbv).data;
	cxt->baseObject.id = id;

	return baseObject;
}

static void
JsonValueListClear(JsonValueList *jvl)
{
	jvl->head = NULL;
	jvl->tail = NULL;
	jvl->length = 0;
}

static void
JsonValueListAppend(JsonValueList *jvl, JsonItem *jsi)
{
	jsi->next = NULL;

	if (jvl->tail)
	{
		jvl->tail->next = jsi;
		jvl->tail = jsi;
	}
	else
	{
		Assert(!jvl->head);
		jvl->head = jvl->tail = jsi;
	}

	jvl->length++;
}

static int
JsonValueListLength(const JsonValueList *jvl)
{
	return jvl->length;
}

static bool
JsonValueListIsEmpty(JsonValueList *jvl)
{
	return !jvl->length;
}

static JsonItem *
JsonValueListHead(JsonValueList *jvl)
{
	return jvl->head;
}

static List *
JsonValueListGetList(JsonValueList *jvl)
{
	List	   *list = NIL;
	JsonItem   *jsi;

	for (jsi = jvl->head; jsi; jsi = jsi->next)
		list = lappend(list, jsi);

	return list;
}

static void
JsonValueListInitIterator(const JsonValueList *jvl, JsonValueListIterator *it)
{
	it->next = jvl->head;
}

/*
 * Get the next item from the sequence advancing iterator.
 */
static JsonItem *
JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)
{
	JsonItem   *result = it->next;

	if (result)
		it->next = result->next;

	return result;
}

/*
 * Initialize a binary JsonbValue with the given jsonb container.
 */
static JsonbValue *
JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = &jb->root;
	jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

	return jbv;
}

/*
 * Initialize a binary JsonbValue with the given json container.
 */
static inline JsonbValue *
JsonInitBinary(JsonbValue *jbv, Json *js)
{
	jbv->type = jbvBinary;
	jbv->val.binary.data = (void *) &js->root;
	jbv->val.binary.len = js->root.len;

	return jbv;
}

/*
 * Returns jbv* type of of JsonbValue. Note, it never returns jbvBinary as is.
 */
static int
JsonbType(JsonItem *jb)
{
	int			type = JsonItemGetType(jb);

	if (type == jbvBinary)
	{
		JsonbContainer *jbc = (void *) JsonItemBinary(jb).data;

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

/*
 * Convert jsonb to a C-string stripping quotes from scalar strings.
 */
static char *
JsonbValueUnquote(JsonbValue *jbv, int *len, bool isJsonb)
{
	switch (jbv->type)
	{
		case jbvString:
			*len = jbv->val.string.len;
			return jbv->val.string.val;

		case jbvBool:
			*len = jbv->val.boolean ? 4 : 5;
			return jbv->val.boolean ? "true" : "false";

		case jbvNumeric:
			*len = -1;
			return DatumGetCString(DirectFunctionCall1(numeric_out,
													   PointerGetDatum(jbv->val.numeric)));

		case jbvNull:
			*len = 4;
			return "null";

		case jbvBinary:
			{
				JsonbValue	jbvbuf;

				if (isJsonb ?
					JsonbExtractScalar(jbv->val.binary.data, &jbvbuf) :
					JsonExtractScalar((JsonContainer *) jbv->val.binary.data, &jbvbuf))
					return JsonbValueUnquote(&jbvbuf, len, isJsonb);

				*len = -1;
				return isJsonb ?
					JsonbToCString(NULL, jbv->val.binary.data, jbv->val.binary.len) :
					JsonToCString(NULL, (JsonContainer *) jbv->val.binary.data, jbv->val.binary.len);
			}

		default:
			elog(ERROR, "unexpected jsonb value type: %d", jbv->type);
			return NULL;
	}
}

static char *
JsonItemUnquote(JsonItem *jsi, int *len, bool isJsonb)
{
	switch (JsonItemGetType(jsi))
	{
		case jsiDatetime:
			*len = -1;
			return JsonEncodeDateTime(NULL,
									  JsonItemDatetime(jsi).value,
									  JsonItemDatetime(jsi).typid,
									  &JsonItemDatetime(jsi).tz);

		default:
			return JsonbValueUnquote(JsonItemJbv(jsi), len, isJsonb);
	}
}

static text *
JsonItemUnquoteText(JsonItem *jsi, bool isJsonb)
{
	int			len;
	char	   *str = JsonItemUnquote(jsi, &len, isJsonb);

	if (len < 0)
		return cstring_to_text(str);
	else
		return cstring_to_text_with_len(str, len);
}

static JsonItem *
getJsonObjectKey(JsonItem *jsi, char *keystr, int keylen, bool isJsonb,
				 JsonItem *res)
{
	JsonbContainer *jbc = JsonItemBinary(jsi).data;
	JsonbValue *val;
	JsonbValue	key;

	key.type = jbvString;
	key.val.string.val = keystr;
	key.val.string.len = keylen;

	val = isJsonb ?
		findJsonbValueFromContainer(jbc, JB_FOBJECT, &key) :
		findJsonValueFromContainer((JsonContainer *) jbc, JB_FOBJECT, &key);

	return val ? JsonbValueToJsonItem(val, res) : NULL;
}

static JsonItem *
getJsonArrayElement(JsonItem *jb, uint32 index, bool isJsonb, JsonItem *res)
{
	JsonbContainer *jbc = JsonItemBinary(jb).data;
	JsonbValue *elem = isJsonb ?
		getIthJsonbValueFromContainer(jbc, index) :
		getIthJsonValueFromContainer((JsonContainer *) jbc, index);

	return elem ? JsonbValueToJsonItem(elem, res) : NULL;
}

static inline void
JsonxIteratorInit(JsonxIterator *it, JsonxContainer *jxc, bool isJsonb)
{
	it->isJsonb = isJsonb;
	if (isJsonb)
		it->it.jb = JsonbIteratorInit((JsonbContainer *) jxc);
	else
		it->it.js = JsonIteratorInit((JsonContainer *) jxc);
}

static JsonbIteratorToken
JsonxIteratorNext(JsonxIterator *it, JsonbValue *jbv, bool skipNested)
{
	return it->isJsonb ?
		JsonbIteratorNext(&it->it.jb, jbv, skipNested) :
		JsonIteratorNext(&it->it.js, jbv, skipNested);
}

Json *
JsonItemToJson(JsonItem *jsi)
{
	JsonbValue	jbv;

	return JsonbValueToJson(JsonItemToJsonbValue(jsi, &jbv));
}

static Jsonx *
JsonbValueToJsonx(JsonbValue *jbv, bool isJsonb)
{
	return isJsonb ?
		(Jsonx *) JsonbValueToJsonb(jbv) :
		(Jsonx *) JsonbValueToJson(jbv);
}

Datum
JsonbValueToJsonxDatum(JsonbValue *jbv, bool isJsonb)
{
	return isJsonb ?
		JsonbPGetDatum(JsonbValueToJsonb(jbv)) :
		JsonPGetDatum(JsonbValueToJson(jbv));
}

Datum
JsonItemToJsonxDatum(JsonItem *jsi, bool isJsonb)
{
	JsonbValue	jbv;

	return JsonbValueToJsonxDatum(JsonItemToJsonbValue(jsi, &jbv), isJsonb);
}

/* Get scalar of given type or NULL on type mismatch */
static JsonItem *
getScalar(JsonItem *scalar, enum jbvType type)
{
	/* Scalars should be always extracted during jsonpath execution. */
	Assert(!JsonItemIsBinary(scalar) ||
		   !JsonContainerIsScalar(JsonItemBinary(scalar).data));

	return JsonItemGetType(scalar) == type ? scalar : NULL;
}

/* Construct a JSON array from the item list */
static JsonbValue *
wrapItemsInArray(const JsonValueList *items, bool isJsonb)
{
	JsonbParseState *ps = NULL;
	JsonValueListIterator it;
	JsonItem   *jsi;
	JsonbValue	jbv;
	JsonBuilderFunc push = isJsonb ? pushJsonbValue : pushJsonValue;

	push(&ps, WJB_BEGIN_ARRAY, NULL);

	JsonValueListInitIterator(items, &it);

	while ((jsi = JsonValueListNext(items, &it)))
		push(&ps, WJB_ELEM, JsonItemToJsonbValue(jsi, &jbv));

	return push(&ps, WJB_END_ARRAY, NULL);
}

static void
pushJsonItem(JsonItemStack *stack, JsonItemStackEntry *entry, JsonItem *item)
{
	entry->item = item;
	entry->parent = *stack;
	*stack = entry;
}

static void
popJsonItem(JsonItemStack *stack)
{
	*stack = (*stack)->parent;
}

/* Check if the timezone required for casting from type1 to type2 is used */
static void
checkTimezoneIsUsedForCast(bool useTz, const char *type1, const char *type2)
{
	if (!useTz)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot convert value from %s to %s without timezone usage",
						type1, type2),
				 errhint("Use *_tz() function for timezone support.")));
}

/* Convert time datum to timetz datum */
static Datum
castTimeToTimeTz(Datum time, bool useTz)
{
	checkTimezoneIsUsedForCast(useTz, "time", "timetz");

	return DirectFunctionCall1(time_timetz, time);
}

/*---
 * Compares 'ts1' and 'ts2' timestamp, assuming that ts1 might be overflowed
 * during cast from another datatype.
 *
 * 'overflow1' specifies overflow of 'ts1' value:
 *  0 - no overflow,
 * -1 - exceed lower boundary,
 *  1 - exceed upper boundary.
 */
static int
cmpTimestampWithOverflow(Timestamp ts1, int overflow1, Timestamp ts2)
{
	/*
	 * All the timestamps we deal with in jsonpath are produced by
	 * to_datetime() method.  So, they should be valid.
	 */
	Assert(IS_VALID_TIMESTAMP(ts2));

	/*
	 * Timestamp, which exceed lower (upper) bound, is always lower (higher)
	 * than any valid timestamp except minus (plus) infinity.
	 */
	if (overflow1)
	{
		if (overflow1 < 0)
		{
			if (TIMESTAMP_IS_NOBEGIN(ts2))
				return 1;
			else
				return -1;
		}
		if (overflow1 > 0)
		{
			if (TIMESTAMP_IS_NOEND(ts2))
				return -1;
			else
				return 1;
		}
	}

	return timestamp_cmp_internal(ts1, ts2);
}

/*
 * Compare date to timestamptz without throwing overflow error during cast.
 */
static int
cmpDateToTimestamp(DateADT date1, Timestamp ts2, bool useTz)
{
	TimestampTz ts1;
	int			overflow = 0;

	ts1 = date2timestamp_opt_overflow(date1, &overflow);

	return cmpTimestampWithOverflow(ts1, overflow, ts2);
}

/*
 * Compare date to timestamptz without throwing overflow error during cast.
 */
static int
cmpDateToTimestampTz(DateADT date1, TimestampTz tstz2, bool useTz)
{
	TimestampTz tstz1;
	int			overflow = 0;

	checkTimezoneIsUsedForCast(useTz, "date", "timestamptz");

	tstz1 = date2timestamptz_opt_overflow(date1, &overflow);

	return cmpTimestampWithOverflow(tstz1, overflow, tstz2);
}

/*
 * Compare timestamp to timestamptz without throwing overflow error during cast.
 */
static int
cmpTimestampToTimestampTz(Timestamp ts1, TimestampTz tstz2, bool useTz)
{
	TimestampTz tstz1;
	int			overflow = 0;

	checkTimezoneIsUsedForCast(useTz, "timestamp", "timestamptz");

	tstz1 = timestamp2timestamptz_opt_overflow(ts1, &overflow);

	return cmpTimestampWithOverflow(tstz1, overflow, tstz2);
}

/*
 * Cross-type comparison of two datetime SQL/JSON items.  If items are
 * uncomparable *cast_error flag is set, otherwise *cast_error is unset.
 * If the cast requires timezone and it is not used, then explicit error is thrown.
 */
static int
compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2,
				bool useTz, bool *cast_error)
{
	PGFunction cmpfunc;

	*cast_error = false;

	switch (typid1)
	{
		case DATEOID:
			switch (typid2)
			{
				case DATEOID:
					cmpfunc = date_cmp;

					break;

				case TIMESTAMPOID:
					return cmpDateToTimestamp(DatumGetDateADT(val1),
											  DatumGetTimestamp(val2),
											  useTz);

				case TIMESTAMPTZOID:
					return cmpDateToTimestampTz(DatumGetDateADT(val1),
												DatumGetTimestampTz(val2),
												useTz);

				case TIMEOID:
				case TIMETZOID:
					*cast_error = true; /* uncomparable types */
					return 0;

				default:
					elog(ERROR, "unrecognized SQL/JSON datetime type oid: %u",
						 typid2);
			}
			break;

		case TIMEOID:
			switch (typid2)
			{
				case TIMEOID:
					cmpfunc = time_cmp;

					break;

				case TIMETZOID:
					val1 = castTimeToTimeTz(val1, useTz);
					cmpfunc = timetz_cmp;

					break;

				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*cast_error = true; /* uncomparable types */
					return 0;

				default:
					elog(ERROR, "unrecognized SQL/JSON datetime type oid: %u",
						 typid2);
			}
			break;

		case TIMETZOID:
			switch (typid2)
			{
				case TIMEOID:
					val2 = castTimeToTimeTz(val2, useTz);
					cmpfunc = timetz_cmp;

					break;

				case TIMETZOID:
					cmpfunc = timetz_cmp;

					break;

				case DATEOID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					*cast_error = true; /* uncomparable types */
					return 0;

				default:
					elog(ERROR, "unrecognized SQL/JSON datetime type oid: %u",
						 typid2);
			}
			break;

		case TIMESTAMPOID:
			switch (typid2)
			{
				case DATEOID:
					return -cmpDateToTimestamp(DatumGetDateADT(val2),
											   DatumGetTimestamp(val1),
											   useTz);

				case TIMESTAMPOID:
					cmpfunc = timestamp_cmp;

					break;

				case TIMESTAMPTZOID:
					return cmpTimestampToTimestampTz(DatumGetTimestamp(val1),
													 DatumGetTimestampTz(val2),
													 useTz);

				case TIMEOID:
				case TIMETZOID:
					*cast_error = true; /* uncomparable types */
					return 0;

				default:
					elog(ERROR, "unrecognized SQL/JSON datetime type oid: %u",
						 typid2);
			}
			break;

		case TIMESTAMPTZOID:
			switch (typid2)
			{
				case DATEOID:
					return -cmpDateToTimestampTz(DatumGetDateADT(val2),
												 DatumGetTimestampTz(val1),
												 useTz);

				case TIMESTAMPOID:
					return -cmpTimestampToTimestampTz(DatumGetTimestamp(val2),
													  DatumGetTimestampTz(val1),
													  useTz);

				case TIMESTAMPTZOID:
					cmpfunc = timestamp_cmp;

					break;

				case TIMEOID:
				case TIMETZOID:
					*cast_error = true; /* uncomparable types */
					return 0;

				default:
					elog(ERROR, "unrecognized SQL/JSON datetime type oid: %u",
						 typid2);
			}
			break;

		default:
			elog(ERROR, "unrecognized SQL/JSON datetime type oid: %u", typid1);
	}

	if (*cast_error)
		return 0;				/* cast error */

	return DatumGetInt32(DirectFunctionCall2(cmpfunc, val1, val2));
}

static void
JsonItemInitNull(JsonItem *item)
{
	item->val.type = jbvNull;
}

static void
JsonItemInitBool(JsonItem *item, bool val)
{
	item->val.type = jbvBool;
	JsonItemBool(item) = val;
}

static void
JsonItemInitNumeric(JsonItem *item, Numeric val)
{
	item->val.type = jbvNumeric;
	JsonItemNumeric(item) = val;
}

#define JsonItemInitNumericDatum(item, val) JsonItemInitNumeric(item, DatumGetNumeric(val))

static void
JsonItemInitString(JsonItem *item, char *str, int len)
{
	item->val.type = jbvString;
	JsonItemString(item).val = str;
	JsonItemString(item).len = len;
}

static void
JsonItemInitDatetime(JsonItem *item, Datum val, Oid typid, int32 typmod, int tz)
{
	item->val.type = jsiDatetime;
	JsonItemDatetime(item).value = val;
	JsonItemDatetime(item).typid = typid;
	JsonItemDatetime(item).typmod = typmod;
	JsonItemDatetime(item).tz = tz;
}

/********************Interface to pgsql's executor***************************/

bool
JsonPathExists(Datum jb, JsonPath *jp, List *vars, bool isJsonb,
			   bool *error)
{
	Jsonx	   *js = DatumGetJsonxP(jb, isJsonb);
	JsonPathExecResult res = executeJsonPath(jp, vars, EvalJsonPathVar,
											 js, isJsonb, !error, NULL, false /* FIXME */);

	Assert(error || !jperIsError(res));

	if (error && jperIsError(res))
		*error = true;

	return res == jperOk;
}

Datum
JsonPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper, bool *empty,
			  bool *error, List *vars, bool isJsonb)
{
	Jsonx	   *js = DatumGetJsonxP(jb, isJsonb);
	JsonItem   *first;
	bool		wrap;
	JsonValueList found = {0};
	JsonPathExecResult res PG_USED_FOR_ASSERTS_ONLY;
	int			count;

	res = executeJsonPath(jp, vars, EvalJsonPathVar, js, isJsonb, !error, &found,
						  false /* FIXME */);

	Assert(error || !jperIsError(res));

	if (error && jperIsError(res))
	{
		*error = true;
		*empty = false;
		return (Datum) 0;
	}

	count = JsonValueListLength(&found);

	first = count ? JsonValueListHead(&found) : NULL;

	if (!first)
		wrap = false;
	else if (wrapper == JSW_NONE)
		wrap = false;
	else if (wrapper == JSW_UNCONDITIONAL)
		wrap = true;
	else if (wrapper == JSW_CONDITIONAL)
		wrap = count > 1 ||
			JsonItemIsScalar(first) ||
			(JsonItemIsBinary(first) &&
			 JsonContainerIsScalar(JsonItemBinary(first).data));
	else
	{
		elog(ERROR, "unrecognized json wrapper %d", wrapper);
		wrap = false;
	}

	if (wrap)
	{
		JsonbValue *arr = wrapItemsInArray(&found, isJsonb);

		return JsonbValueToJsonxDatum(arr, isJsonb);
	}

	if (count > 1)
	{
		if (error)
		{
			*error = true;
			return (Datum) 0;
		}

		ereport(ERROR,
				(errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
				 errmsg("JSON path expression in JSON_QUERY should return "
						"singleton item without wrapper"),
				 errhint("use WITH WRAPPER clause to wrap SQL/JSON item "
						 "sequence into array")));
	}

	if (first)
		return JsonItemToJsonxDatum(first, isJsonb);

	*empty = true;
	return PointerGetDatum(NULL);
}

JsonItem *
JsonPathValue(Datum jb, JsonPath *jp, bool *empty, bool *error, List *vars,
			  bool isJsonb)
{
	Jsonx	   *js = DatumGetJsonxP(jb, isJsonb);
	JsonItem   *res;
	JsonValueList found = { 0 };
	JsonPathExecResult jper PG_USED_FOR_ASSERTS_ONLY;
	int			count;

	jper = executeJsonPath(jp, vars, EvalJsonPathVar, js, isJsonb, !error,
						   &found, false /* FIXME */);

	Assert(error || !jperIsError(jper));

	if (error && jperIsError(jper))
	{
		*error = true;
		*empty = false;
		return NULL;
	}

	count = JsonValueListLength(&found);

	*empty = !count;

	if (*empty)
		return NULL;

	if (count > 1)
	{
		if (error)
		{
			*error = true;
			return NULL;
		}

		ereport(ERROR,
				(errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
				 errmsg("JSON path expression in JSON_VALUE should return "
						"singleton scalar item")));
	}

	res = JsonValueListHead(&found);

	if (JsonItemIsBinary(res) &&
		JsonContainerIsScalar(JsonItemBinary(res).data))
	{
		if (isJsonb)
			JsonbExtractScalar(JsonItemBinary(res).data, JsonItemJbv(res));
		else
			JsonExtractScalar((JsonContainer *) JsonItemBinary(res).data, JsonItemJbv(res));
	}

	if (!JsonItemIsScalar(res))
	{
		if (error)
		{
			*error = true;
			return NULL;
		}

		ereport(ERROR,
				(errcode(ERRCODE_SQL_JSON_SCALAR_REQUIRED),
				 errmsg("JSON path expression in JSON_VALUE should return "
						"singleton scalar item")));
	}

	if (JsonItemIsNull(res))
		return NULL;

	return res;
}

void
JsonItemFromDatum(Datum val, Oid typid, int32 typmod, JsonItem *res,
				  bool isJsonb)
{
	switch (typid)
	{
		case BOOLOID:
			JsonItemInitBool(res, DatumGetBool(val));
			break;
		case NUMERICOID:
			JsonItemInitNumericDatum(res, val);
			break;
		case INT2OID:
			JsonItemInitNumericDatum(res, DirectFunctionCall1(int2_numeric, val));
			break;
		case INT4OID:
			JsonItemInitNumericDatum(res, DirectFunctionCall1(int4_numeric, val));
			break;
		case INT8OID:
			JsonItemInitNumericDatum(res, DirectFunctionCall1(int8_numeric, val));
			break;
		case FLOAT4OID:
			JsonItemInitNumericDatum(res, DirectFunctionCall1(float4_numeric, val));
			break;
		case FLOAT8OID:
			JsonItemInitNumericDatum(res, DirectFunctionCall1(float8_numeric, val));
			break;
		case TEXTOID:
		case VARCHAROID:
			JsonItemInitString(res, VARDATA_ANY(val), VARSIZE_ANY_EXHDR(val));
			break;
		case DATEOID:
		case TIMEOID:
		case TIMETZOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			JsonItemInitDatetime(res, val, typid, typmod, 0);
			break;
		case JSONBOID:
			{
				JsonbValue *jbv = JsonItemJbv(res);
				Jsonb	   *jb = DatumGetJsonbP(val);

				if (JsonContainerIsScalar(&jb->root))
				{
					bool		res PG_USED_FOR_ASSERTS_ONLY;

					res = JsonbExtractScalar(&jb->root, jbv);
					Assert(res);
				}
				else if (isJsonb)
				{
					JsonbInitBinary(jbv, jb);
				}
				else
				{
					StringInfoData buf;
					text	   *txt;
					Json	   *js;

					initStringInfo(&buf);
					JsonbToCString(&buf, &jb->root, VARSIZE(jb));
					txt = cstring_to_text_with_len(buf.data, buf.len);
					pfree(buf.data);

					js = JsonCreate(txt);

					JsonInitBinary(jbv, js);
				}
				break;
			}
		case JSONOID:
			{
				JsonbValue *jbv = JsonItemJbv(res);
				Json	   *js = DatumGetJsonP(val);

				if (JsonContainerIsScalar(&js->root))
				{
					bool		res PG_USED_FOR_ASSERTS_ONLY;

					res = JsonExtractScalar(&js->root, jbv);
					Assert(res);
				}
				else if (isJsonb)
				{
					text	   *txt = DatumGetTextP(val);
					char	   *str = text_to_cstring(txt);
					Jsonb	   *jb =
						DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(str)));

					pfree(str);

					JsonbInitBinary(jbv, jb);
				}
				else
				{
					JsonInitBinary(jbv, js);
				}
				break;
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only bool, numeric and text types could be "
							"casted to supported jsonpath types.")));
	}
}

/************************ JSON_TABLE functions ***************************/

/*
 * Returns private data from executor state. Ensure validity by check with
 * MAGIC number.
 */
static inline JsonTableContext *
GetJsonTableContext(TableFuncScanState *state, const char *fname)
{
	JsonTableContext *result;

	if (!IsA(state, TableFuncScanState))
		elog(ERROR, "%s called with invalid TableFuncScanState", fname);
	result = (JsonTableContext *) state->opaque;
	if (result->magic != JSON_TABLE_CONTEXT_MAGIC)
		elog(ERROR, "%s called with invalid TableFuncScanState", fname);

	return result;
}

/* Recursively initialize JSON_TABLE scan state */
static void
JsonTableInitScanState(JsonTableContext *cxt, JsonTableScanState *scan,
					   JsonTableParentNode *node, JsonTableScanState *parent,
					   List *args, MemoryContext mcxt)
{
	int			i;

	scan->parent = parent;
	scan->outerJoin = node->outerJoin;
	scan->errorOnError = node->errorOnError;
	scan->path = DatumGetJsonPathP(node->path->constvalue);
	scan->args = args;
	scan->mcxt = AllocSetContextCreate(mcxt, "JsonTableContext",
									   ALLOCSET_DEFAULT_SIZES);
	scan->nested = node->child ?
		JsonTableInitPlanState(cxt, node->child, scan) : NULL;
	scan->current = PointerGetDatum(NULL);
	scan->currentIsNull = true;

	for (i = node->colMin; i <= node->colMax; i++)
		cxt->colexprs[i].scan = scan;
}

/* Recursively initialize JSON_TABLE scan state */
static JsonTableJoinState *
JsonTableInitPlanState(JsonTableContext *cxt, Node *plan,
					   JsonTableScanState *parent)
{
	JsonTableJoinState *state = palloc0(sizeof(*state));

	if (IsA(plan, JsonTableSiblingNode))
	{
		JsonTableSiblingNode *join = castNode(JsonTableSiblingNode, plan);

		state->is_join = true;
		state->u.join.cross = join->cross;
		state->u.join.left = JsonTableInitPlanState(cxt, join->larg, parent);
		state->u.join.right = JsonTableInitPlanState(cxt, join->rarg, parent);
	}
	else
	{
		JsonTableParentNode *node = castNode(JsonTableParentNode, plan);

		state->is_join = false;

		JsonTableInitScanState(cxt, &state->u.scan, node, parent,
							   parent->args, parent->mcxt);
	}

	return state;
}

/*
 * JsonxTableInitOpaque
 *		Fill in TableFuncScanState->opaque for JsonTable processor
 */
static void
JsonxTableInitOpaque(TableFuncScanState *state, int natts, bool isJsonb)
{
	JsonTableContext *cxt;
	PlanState  *ps = &state->ss.ps;
	TableFuncScan  *tfs = castNode(TableFuncScan, ps->plan);
	TableFunc  *tf = tfs->tablefunc;
	JsonExpr   *ci = castNode(JsonExpr, tf->docexpr);
	JsonTableParentNode *root = castNode(JsonTableParentNode, tf->plan);
	List	   *args = NIL;
	ListCell   *lc;
	int			i;

	cxt = palloc0(sizeof(JsonTableContext));
	cxt->magic = JSON_TABLE_CONTEXT_MAGIC;
	cxt->isJsonb = isJsonb;

	if (ci->passing.values)
	{
		ListCell   *exprlc;
		ListCell   *namelc;

		forboth(exprlc, ci->passing.values,
				namelc, ci->passing.names)
		{
			Expr	   *expr = (Expr *) lfirst(exprlc);
			Value	   *name = (Value *) lfirst(namelc);
			JsonPathVariableEvalContext *var = palloc(sizeof(*var));

			var->name = pstrdup(name->val.str);
			var->typid = exprType((Node *) expr);
			var->typmod = exprTypmod((Node *) expr);
			var->estate = ExecInitExpr(expr, ps);
			var->econtext = ps->ps_ExprContext;
			var->mcxt = CurrentMemoryContext;
			var->evaluated = false;
			var->value = (Datum) 0;
			var->isnull = true;

			args = lappend(args, var);
		}
	}

	cxt->colexprs = palloc(sizeof(*cxt->colexprs) *
						   list_length(tf->colvalexprs));

	JsonTableInitScanState(cxt, &cxt->root, root, NULL, args,
						   CurrentMemoryContext);

	i = 0;

	foreach(lc, tf->colvalexprs)
	{
		Expr	   *expr = lfirst(lc);

		cxt->colexprs[i].expr =
			ExecInitExprWithCaseValue(expr, ps,
									  &cxt->colexprs[i].scan->current,
									  &cxt->colexprs[i].scan->currentIsNull);

		i++;
	}

	state->opaque = cxt;
}

static void
JsonbTableInitOpaque(TableFuncScanState *state, int natts)
{
	JsonxTableInitOpaque(state, natts, true);
}

static void
JsonTableInitOpaque(TableFuncScanState *state, int natts)
{
	JsonxTableInitOpaque(state, natts, false);
}

/* Reset scan iterator to the beginning of the item list */
static void
JsonTableRescan(JsonTableScanState *scan)
{
	JsonValueListInitIterator(&scan->found, &scan->iter);
	scan->current = PointerGetDatum(NULL);
	scan->currentIsNull = true;
	scan->advanceNested = false;
	scan->ordinal = 0;
}

/* Reset context item of a scan, execute JSON path and reset a scan */
static void
JsonTableResetContextItem(JsonTableScanState *scan, Datum item, bool isJsonb)
{
	MemoryContext oldcxt;
	JsonPathExecResult res;
	Jsonx	   *js = DatumGetJsonxP(item, isJsonb);

	JsonValueListClear(&scan->found);

	MemoryContextResetOnly(scan->mcxt);

	oldcxt = MemoryContextSwitchTo(scan->mcxt);

	res = executeJsonPath(scan->path, scan->args, EvalJsonPathVar, js, isJsonb,
						  scan->errorOnError, &scan->found, false /* FIXME */);

	MemoryContextSwitchTo(oldcxt);

	if (jperIsError(res))
	{
		Assert(!scan->errorOnError);
		JsonValueListClear(&scan->found);	/* EMPTY ON ERROR case */
	}

	JsonTableRescan(scan);
}

/*
 * JsonTableSetDocument
 *		Install the input document
 */
static void
JsonTableSetDocument(TableFuncScanState *state, Datum value)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableSetDocument");

	JsonTableResetContextItem(&cxt->root, value, cxt->isJsonb);
}

/* Recursively reset scan and its child nodes */
static void
JsonTableRescanRecursive(JsonTableJoinState *state)
{
	if (state->is_join)
	{
		JsonTableRescanRecursive(state->u.join.left);
		JsonTableRescanRecursive(state->u.join.right);
		state->u.join.advanceRight = false;
	}
	else
	{
		JsonTableRescan(&state->u.scan);
		if (state->u.scan.nested)
			JsonTableRescanRecursive(state->u.scan.nested);
	}
}

/*
 * Fetch next row from a cross/union joined scan.
 *
 * Returned false at the end of a scan, true otherwise.
 */
static bool
JsonTableNextJoinRow(JsonTableJoinState *state, bool isJsonb)
{
	if (!state->is_join)
		return JsonTableNextRow(&state->u.scan, isJsonb);

	if (state->u.join.advanceRight)
	{
		/* fetch next inner row */
		if (JsonTableNextJoinRow(state->u.join.right, isJsonb))
			return true;

		/* inner rows are exhausted */
		if (state->u.join.cross)
			state->u.join.advanceRight = false;	/* next outer row */
		else
			return false;	/* end of scan */
	}

	while (!state->u.join.advanceRight)
	{
		/* fetch next outer row */
		bool		left = JsonTableNextJoinRow(state->u.join.left, isJsonb);

		if (state->u.join.cross)
		{
			if (!left)
				return false;	/* end of scan */

			JsonTableRescanRecursive(state->u.join.right);

			if (!JsonTableNextJoinRow(state->u.join.right, isJsonb))
				continue;	/* next outer row */

			state->u.join.advanceRight = true;	/* next inner row */
		}
		else if (!left)
		{
			if (!JsonTableNextJoinRow(state->u.join.right, isJsonb))
				return false;	/* end of scan */

			state->u.join.advanceRight = true;	/* next inner row */
		}

		break;
	}

	return true;
}

/* Recursively set 'reset' flag of scan and its child nodes */
static void
JsonTableJoinReset(JsonTableJoinState *state)
{
	if (state->is_join)
	{
		JsonTableJoinReset(state->u.join.left);
		JsonTableJoinReset(state->u.join.right);
		state->u.join.advanceRight = false;
	}
	else
	{
		state->u.scan.reset = true;
		state->u.scan.advanceNested = false;

		if (state->u.scan.nested)
			JsonTableJoinReset(state->u.scan.nested);
	}
}

/*
 * Fetch next row from a simple scan with outer/inner joined nested subscans.
 *
 * Returned false at the end of a scan, true otherwise.
 */
static bool
JsonTableNextRow(JsonTableScanState *scan, bool isJsonb)
{
	/* reset context item if requested */
	if (scan->reset)
	{
		Assert(!scan->parent->currentIsNull);
		JsonTableResetContextItem(scan, scan->parent->current, isJsonb);
		scan->reset = false;
	}

	if (scan->advanceNested)
	{
		/* fetch next nested row */
		scan->advanceNested = JsonTableNextJoinRow(scan->nested, isJsonb);

		if (scan->advanceNested)
			return true;
	}

	for (;;)
	{
		/* fetch next row */
		JsonItem   *jbv = JsonValueListNext(&scan->found, &scan->iter);
		MemoryContext oldcxt;

		if (!jbv)
		{
			scan->current = PointerGetDatum(NULL);
			scan->currentIsNull = true;
			return false;	/* end of scan */
		}

		/* set current row item */
		oldcxt = MemoryContextSwitchTo(scan->mcxt);
		scan->current = JsonItemToJsonxDatum(jbv, isJsonb);
		scan->currentIsNull = false;
		MemoryContextSwitchTo(oldcxt);

		scan->ordinal++;

		if (!scan->nested)
			break;

		JsonTableJoinReset(scan->nested);

		scan->advanceNested = JsonTableNextJoinRow(scan->nested, isJsonb);

		if (scan->advanceNested || scan->outerJoin)
			break;

		/* state->ordinal--; */	/* skip current outer row, reset counter */
	}

	return true;
}

/*
 * JsonTableFetchRow
 *		Prepare the next "current" tuple for upcoming GetValue calls.
 *		Returns FALSE if the row-filter expression returned no more rows.
 */
static bool
JsonTableFetchRow(TableFuncScanState *state)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableFetchRow");

	if (cxt->empty)
		return false;

	return JsonTableNextRow(&cxt->root, cxt->isJsonb);
}

/*
 * JsonTableGetValue
 *		Return the value for column number 'colnum' for the current row.
 *
 * This leaks memory, so be sure to reset often the context in which it's
 * called.
 */
static Datum
JsonTableGetValue(TableFuncScanState *state, int colnum,
				  Oid typid, int32 typmod, bool *isnull)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableGetValue");
	ExprContext *econtext = state->ss.ps.ps_ExprContext;
	ExprState  *estate = cxt->colexprs[colnum].expr;
	JsonTableScanState *scan = cxt->colexprs[colnum].scan;
	Datum		result;

	if (scan->currentIsNull) /* NULL from outer/union join */
	{
		result = (Datum) 0;
		*isnull = true;
	}
	else if (estate)	/* regular column */
	{
		result = ExecEvalExpr(estate, econtext, isnull);
	}
	else
	{
		result = Int32GetDatum(scan->ordinal);	/* ordinality column */
		*isnull = false;
	}

	return result;
}

/*
 * JsonTableDestroyOpaque
 */
static void
JsonTableDestroyOpaque(TableFuncScanState *state)
{
	JsonTableContext *cxt = GetJsonTableContext(state, "JsonTableDestroyOpaque");

	/* not valid anymore */
	cxt->magic = 0;

	state->opaque = NULL;
}

const TableFuncRoutine JsonbTableRoutine =
{
	JsonbTableInitOpaque,
	JsonTableSetDocument,
	NULL,
	NULL,
	NULL,
	JsonTableFetchRow,
	JsonTableGetValue,
	JsonTableDestroyOpaque
};

const TableFuncRoutine JsonTableRoutine =
{
	JsonTableInitOpaque,
	JsonTableSetDocument,
	NULL,
	NULL,
	NULL,
	JsonTableFetchRow,
	JsonTableGetValue,
	JsonTableDestroyOpaque
};
