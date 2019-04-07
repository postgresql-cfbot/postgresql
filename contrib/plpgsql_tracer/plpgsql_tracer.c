/*
 * contrib/plpgsql_tracer/plpgsql_tracer.c
 */
#include "postgres.h"
#include "plpgsql.h"
#include "fmgr.h"

#include "nodes/bitmapset.h"

PG_MODULE_MAGIC;


#define PLPGSQL_TRACER_MAGIC		73071522

typedef struct plpgsql_tracer_data
{
	int				magic;
	Bitmapset	   *stmtids;
} plpgsql_tracer_data;


static void collect_stmtid(PLpgSQL_stmt *stmt, plpgsql_tracer_data *data, bool trace);

static void plpgsql_tracer_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void plpgsql_tracer_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

static PLpgSQL_plugin plugin_funcs = { plpgsql_tracer_func_init,
									   NULL,
									   NULL,
									   plpgsql_tracer_stmt_beg,
									   NULL,
									   NULL,
									   NULL};

/*
 * Collect traced statement id from list of statements.
 */
static void
collect_stmtid_list(List *stmts,
					plpgsql_tracer_data *data,
					bool trace)
{
	ListCell	   *lc;

	foreach(lc, stmts)
	{
		collect_stmtid((PLpgSQL_stmt *) lfirst(lc), data, trace);
	}
}


/*
 * It is iterate over all plpgsql statements and collect stmtid of statements
 * inside blocks marked by PRAGMA trace.
 */
static void
collect_stmtid(PLpgSQL_stmt *stmt,
			   plpgsql_tracer_data *data,
			   bool trace)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;
				ListCell	   *lc;

				foreach (lc, stmt_block->pragmas)
				{
					PLpgSQL_pragma *pragma = (PLpgSQL_pragma *) lfirst(lc);

					if (strcmp(pragma->name, "plpgsql_tracer") == 0)
					{
						ListCell	   *larg;
						char		   *value = NULL;
						int				count = 0;

						/* this pragma requires just on/off parameter */
						foreach(larg, pragma->args)
						{
							PLpgSQL_pragma_arg	   *arg = (PLpgSQL_pragma_arg *) lfirst(larg);

							if (count++ > 0)
							{
								elog(WARNING, "PRAGMA plpgsql_tracer has only one parameter");
								break;
							}

							if (arg->argname)
							{
								elog(WARNING, "PRAGMA plpgsql_tracer hasn't named parameters");
								break;
							}

							if (arg->type == PLPGSQL_PRAGMA_ARG_IDENT)
								value = arg->val.ident;
							else if (arg->type == PLPGSQL_PRAGMA_ARG_SCONST)
								value = arg->val.str;
							else
							{
								elog(WARNING, "unsupported type of PRAGMA plpgsql_tracer");
								break;
							}

							if (value)
							{
								if ((strcmp(value, "on") == 0) ||
									(strcmp(value, "true") == 0) ||
									(strcmp(value, "t") == 0))
									trace = true;
								else if ((strcmp(value, "off") == 0) ||
										 (strcmp(value, "false") == 0) ||
										 (strcmp(value, "f") == 0))
									trace = false;
								else
								{
									elog(WARNING, "unknown value of PRAGMA plpgsql_tracer parameter");
									break;
								}
							}
						}

						if (count < 1)
							elog(WARNING, "missing argument of PRAGMA plpgsql_tracer");
					}
				}

				collect_stmtid_list(stmt_block->body, data, trace);
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;
				ListCell		*lc;

				collect_stmtid_list(stmt_if->then_body, data, trace);
				foreach(lc, stmt_if->elsif_list)
				{
					collect_stmtid_list(((PLpgSQL_if_elsif *) lfirst(lc))->stmts,
										data,
										trace);
				}
				collect_stmtid_list(stmt_if->else_body, data, trace);
			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
				ListCell		 *lc;

				foreach(lc, stmt_case->case_when_list)
				{
					collect_stmtid_list(((PLpgSQL_case_when *) lfirst(lc))->stmts,
										data,
										trace);
				}
				collect_stmtid_list(stmt_case->else_stmts, data, trace);
			}
			break;

		case PLPGSQL_STMT_LOOP:
			collect_stmtid_list(((PLpgSQL_stmt_while *) stmt)->body, data, trace);
			break;

		case PLPGSQL_STMT_FORI:
			collect_stmtid_list(((PLpgSQL_stmt_fori *) stmt)->body, data, trace);
			break;

		case PLPGSQL_STMT_FORS:
			collect_stmtid_list(((PLpgSQL_stmt_fors *) stmt)->body, data, trace);
			break;

		case PLPGSQL_STMT_FORC:
			collect_stmtid_list(((PLpgSQL_stmt_forc *) stmt)->body, data, trace);
			break;

		case PLPGSQL_STMT_DYNFORS:
			collect_stmtid_list(((PLpgSQL_stmt_dynfors *) stmt)->body, data, trace);
			break;

		case PLPGSQL_STMT_FOREACH_A:
			collect_stmtid_list(((PLpgSQL_stmt_foreach_a *) stmt)->body, data, trace);
			break;

		default:
			/* do nothing */
			break;
	}

	if (trace && stmt->cmd_type != PLPGSQL_STMT_BLOCK)
		data->stmtids = bms_add_member(data->stmtids, stmt->stmtid);
}

/*
 * Define functions for PL debug API
 */
static void
plpgsql_tracer_func_init(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	plpgsql_tracer_data	   *tdata;

	tdata = palloc0(sizeof(plpgsql_tracer_data));
	tdata->magic = PLPGSQL_TRACER_MAGIC;

	estate->plugin_info = tdata;

	collect_stmtid((PLpgSQL_stmt *) func->action, tdata, false);
}

static void
plpgsql_tracer_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	plpgsql_tracer_data		*tdata = (plpgsql_tracer_data *) estate->plugin_info;

	if (tdata->magic == PLPGSQL_TRACER_MAGIC)
	{
		/* now, its our data */
		if (bms_is_member(stmt->stmtid, tdata->stmtids))
		{
			elog(NOTICE,
				 "plpgsql_tracer: %4d: %s",
				 stmt->lineno,
				 plpgsql_stmt_typename(stmt));
		}
	}
}

/*
 * Module initialization
 *
 * setup PLpgSQL_plugin API
 */
void
_PG_init(void)
{
	PLpgSQL_plugin **var_ptr;
	static bool	inited = false;

	if (inited)
		return;

	var_ptr = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
	*var_ptr = &plugin_funcs;

	inited = true;
}
