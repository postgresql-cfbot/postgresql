/*--------------------------------------------------------------------------
 *
 * test_logging.c
 *		Test logging functions
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_logging/test_logging.c
 *
 * -------------------------------------------------------------------------
 */

#include <unistd.h>
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/jsonb.h"

PG_MODULE_MAGIC;

static emit_log_hook_type prev_log_hook = NULL;

void		_PG_init(void);
void		_PG_fini(void);

static void move_tags_to_detail(ErrorData *edata);

static char *
jsonb_value_to_tagvalue(JsonbValue *v)
{
	switch (v->type)
	{
		case jbvNull:
			return "";
		case jbvString:
			return pnstrdup(v->val.string.val, v->val.string.len);
		default:
			elog(ERROR, "jsonb type not allowed here: %d", (int) v->type);
	}
}

static void
jsonb_tags_to_key_value_text_pairs(Jsonb *tags, List **keys, List **values)
{
	JsonbValue	v;
	JsonbIterator *it;
	JsonbIteratorToken r;
	bool		skipNested = false;

	it = JsonbIteratorInit(&tags->root);
	*keys = *values = NIL;
	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;
		if (r == WJB_KEY)
		{
			*keys = lappend(*keys, pnstrdup(v.val.string.val, v.val.string.len));
			r = JsonbIteratorNext(&it, &v, skipNested);
			Assert(r != WJB_DONE);
			*values = lappend(*values, jsonb_value_to_tagvalue(&v));
		}
	}
}

PG_FUNCTION_INFO_V1(test_logging);
Datum
test_logging(PG_FUNCTION_ARGS)
{
	int			level = PG_GETARG_INT32(0);
	char	   *message = "";
	List	   *keys = NIL,
			   *values = NIL;
	ListCell   *lk,
			   *lv;

	if (!PG_ARGISNULL(1))
	{
		message = text_to_cstring(PG_GETARG_TEXT_PP(1));
	}
	if (!PG_ARGISNULL(2))
	{
		jsonb_tags_to_key_value_text_pairs(PG_GETARG_JSONB_P(2), &keys, &values);
	}
	errstart(level, TEXTDOMAIN);
	errmsg("%s", message);
	forboth(lk, keys, lv, values)
	{
		errtag(lfirst(lk), "%s", (char *) lfirst(lv));
	}
	errfinish(__FILE__, __LINE__, PG_FUNCNAME_MACRO);

	PG_RETURN_VOID();
}

void
move_tags_to_detail(ErrorData *edata)
{
	StringInfoData buf;
	ListCell   *lc;

	initStringInfo(&buf);
	foreach(lc, edata->tags)
	{
		ErrorTag   *tag = (ErrorTag *) lfirst(lc);

		appendStringInfo(&buf, "%s: %s ", tag->tagname, tag->tagvalue);
	}
	errdetail("NB TAGS: %d TAGS: %s", list_length(edata->tags), buf.data);
	pfree(buf.data);
}

void
_PG_init(void)
{
	prev_log_hook = emit_log_hook;
	emit_log_hook = move_tags_to_detail;
}
