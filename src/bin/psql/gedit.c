/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/bin/psql/gedit.c
 */

#include <sys/stat.h>
#include <time.h>
#include <utime.h>

#include "postgres_fe.h"
#include "catalog/pg_type_d.h"
#include "command.h"
#include "common.h"
#include "common/jsonapi.h"
#include "common/logging.h"
#include "common/string.h"
#include "lib/ilist.h"
#include "pqexpbuffer.h"
#include "psqlscanslash.h"
#include "settings.h"

#include "gedit.h"

/*
 * Process a single option for \gedit
 */
static bool
process_command_gedit_option(char *option, char *valptr)
{
	bool success = false;

	if (strcmp(option, "table") == 0)
	{
		if (valptr && *valptr)
		{
			if (pset.gedit_table)
				free(pset.gedit_table);
			pset.gedit_table = strdup(valptr);
			success = true;
		} else
			pg_log_error("\\gedit: missing table name in table option");
	}
	else if (strcmp(option, "key") == 0)
	{
		if (valptr && *valptr)
		{
			char *p;
			int nkeycols = 1;
			char *saveptr = NULL;
			int i = 0;
			char *tok;

			/* count number of tokens. Currently, this assumes no commas in table names and no quoting */
			for (p = valptr; *p; p++)
			{
				if (*p == ',')
					nkeycols++;
			}

			Assert(! pset.gedit_key_columns);
			pset.gedit_key_columns = calloc(nkeycols + 1, sizeof(char *));
			if (! pset.gedit_key_columns)
			{
				pg_log_error("\\gedit: out of memory");
				return false;
			}

			do {
				tok = strtok_r(saveptr ? NULL : valptr, ",", &saveptr);
				if (tok)
					pset.gedit_key_columns[i++] = strdup(tok);
			} while (tok);
			pset.gedit_key_columns[i] = NULL; /* NULL-terminate array */

			success = true;

		} else
			pg_log_error("\\gedit: missing column names in key option");
	}
	else if (strcmp(option, "x") == 0 || strcmp(option, "expanded") == 0 || strcmp(option, "vertical") == 0)
	{
		success = do_pset("expanded", valptr, &pset.popt, true);
	}
	else
		pg_log_error("\\gedit: unknown option \"%s\"", option);

	return success;
}

/*
 * Process parenthesized options for \gedit
 */
bool
process_command_gedit_options(PsqlScanState scan_state, bool active_branch)
{
	bool		success = true;
	bool		first_option = true;
	bool		found_r_paren = false;

	do
	{
		char	   *option;
		size_t		optlen;

		option = psql_scan_slash_option(scan_state,
										OT_NORMAL, NULL, false);
		if (!option)
		{
			if (active_branch && !first_option)
			{
				pg_log_error("\\gedit: missing right parenthesis in options");
				success = false;
			}
			break;
		}

		/* Skip over '(' in first option */
		if (first_option)
		{
			char *tmp;

			if (option[0] != '(')
			{
				pg_log_error("\\gedit: missing left parenthesis in options");
				success = false;
				break;
			}

			tmp = strdup(option + 1);
			free(option);
			option = tmp;
			first_option = false;
		}

		/* Check for terminating right paren, and remove it from string */
		optlen = strlen(option);
		if (optlen > 0 && option[optlen - 1] == ')')
		{
			option[--optlen] = '\0';
			found_r_paren = true;
		}

		/* If there was anything besides parentheses, parse/execute it */
		if (optlen > 0)
		{
			/* We can have either "name" or "name=value" */
			char	   *valptr = strchr(option, '=');

			if (valptr)
				*valptr++ = '\0';
			if (active_branch)
				success &= process_command_gedit_option(option, valptr);
		}

		free(option);
	} while (!found_r_paren);

	return success;
}

/*
 * Extract table name from query
 */
char *
gedit_table_name(PQExpBuffer query_buf)
{
	char *query = strdup(query_buf->data);
	char *tok;
	char *saveptr = NULL;
	char *res = NULL;

	do {
		tok = strtok_r(saveptr ? NULL : query, " \t", &saveptr);
		if (tok && (strcasecmp(tok, "FROM") == 0 || strcasecmp(tok, "TABLE") == 0))
		{
			if ((res = strtok_r(NULL, " \t", &saveptr))) /* next word or NULL */
			{
				int l = strlen(res);
				Assert (l > 0);

				res = strdup(res);
				if (res[l-1] == ';') /* strip trailing ; */
					res[l-1] = '\0';
			}
			break;
		}
	} while (tok != NULL);

	free(query);

	return res;
}

/*
 * Append one array-quoted value to a buffer.
 */
static void
PQExpBufferAppendArrayValue(PQExpBuffer buf, const char *value)
{
	const char *p;
	appendPQExpBufferChar(buf, '"');
	for (p = value; *p; p++)
	{
		if (*p == '"')
			appendPQExpBufferStr(buf, "\\\"");
		else if (*p == '\\')
			appendPQExpBufferStr(buf, "\\\\");
		else
			appendPQExpBufferChar(buf, *p);
	}
	appendPQExpBufferChar(buf, '"');
}

/*
 * Describe a query, and return the list of columns as an array literal.
 */
static bool
gedit_get_query_columns(PGconn *conn, const char *query, PQExpBuffer query_columns)
{
	PGresult	*result = PQprepare(conn, "", query, 0, NULL);
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		pg_log_error("%s", PQerrorMessage(pset.db));
		PQclear(result);
		return false;
	}
	PQclear(result);

	result = PQdescribePrepared(pset.db, "");
	if (PQresultStatus(result) == PGRES_COMMAND_OK)
	{
		appendPQExpBufferChar(query_columns, '{');
		if (PQnfields(result) > 0)
		{
			int i;
			for (i = 0; i < PQnfields(result); i++)
			{
				char *name;
				name = PQfname(result, i);
				if (i > 0)
					appendPQExpBufferChar(query_columns, ',');
				PQExpBufferAppendArrayValue(query_columns, name);
			}
		}
		appendPQExpBufferChar(query_columns, '}');
	}

	return true;
}

/*
 * Given a query and a table, get a table key that is contained in the query
 * columns.
 */
char **
gedit_table_key_columns(PGconn *conn, const char *query, const char *table)
{
	PQExpBuffer query_columns = createPQExpBuffer();
	char		key_query[] =
		"WITH keys AS (SELECT array_agg(attname ORDER BY ordinality) keycols, indisprimary "
		"FROM pg_index, unnest(indkey) with ordinality u(keycol) "
		"JOIN pg_attribute ON keycol = attnum "
		"WHERE indrelid = $1::regclass AND attrelid = $1::regclass AND (indisprimary or indisunique) "
		"GROUP BY indexrelid), "
		"key AS (SELECT keycols FROM keys WHERE keycols <@ $2 " /* all keys contained in the query columns */
		"ORDER BY indisprimary DESC LIMIT 1) " /* prefer primary key over unique indexes */
		"SELECT unnest(keycols) FROM key;";
	Oid			types[] = { NAMEOID, NAMEARRAYOID };
	const char *values[] = { table, NULL };
	char	   **gedit_key_columns = NULL;
	PGresult   *res;

	if (! gedit_get_query_columns(pset.db, query, query_columns))
	{
		destroyPQExpBuffer(query_columns);
		return NULL;
	}
	values[1] = query_columns->data;
	res = PQexecParams(conn, key_query, 2, types, values, NULL, NULL, 0);
	destroyPQExpBuffer(query_columns);

	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		int nkeycols = PQntuples(res);
		if (nkeycols > 0)
		{
			int i;
			gedit_key_columns = calloc(nkeycols + 1, sizeof(char *));
			if (! gedit_key_columns)
			{
				pg_log_error("\\gedit: Out of memory.");
				return NULL;
			}

			for (i = 0; i < nkeycols; i++)
			{
				gedit_key_columns[i] = strdup(PQgetvalue(res, i, 0));
			}
		}
		else
		{
			pg_log_error("\\gedit: no key of table \"%s\" is contained in the returned query columns.", table);
			pg_log_error_hint("Select more columns or manually specify a key using \\gedit (key=...)");
		}
	}
	else if (PQresultStatus(res) == PGRES_FATAL_ERROR)
	{
		char *val = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (strcmp(val, "42P01") == 0)
			pg_log_error("\\gedit: table \"%s\" does not exist", table);
		else
			pg_log_error("\\gedit: error while retrieving key columns of table \"%s\": %s",
					table, PQerrorMessage(pset.db));
	}
	PQclear(res);

	return gedit_key_columns;
}

/*
 * Check if a query contains the given columns.
 */
bool
gedit_check_key_columns(PGconn *conn, const char *query, char **key_columns)
{
	char **key_column;
	PGresult	*result = PQprepare(conn, "", query, 0, NULL);

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		pg_log_error("\\gedit: %s", PQerrorMessage(conn));
		PQclear(result);
		return false;
	}
	PQclear(result);

	result = PQdescribePrepared(conn, "");
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		pg_log_error("\\gedit: %s", PQerrorMessage(conn));
		PQclear(result);
		return false;
	}

	if (PQnfields(result) == 0)
	{
		pg_log_error("\\gedit: query has no columns");
		PQclear(result);
		return false;
	}

	for (key_column = key_columns; *key_column; key_column++)
	{
		bool found = false;
		int i;
		for (i = 0; i < PQnfields(result); i++)
		{
			if (strcmp(*key_column, PQfname(result, i)) == 0)
			{
				found = true;
				break;
			}
		}

		if (! found)
		{
			pg_log_error("\\gedit: key column \"%s\" not found in query", *key_column);
			PQclear(result);
			return false;
		}
	}

	PQclear(result);
	return true;
}

/* JSON parsing */

typedef struct
{
	char	   *name;
	char	   *value;
	slist_node	cell_node;
} Cell;

static void
cell_free (Cell *cell1)
{
	if (cell1->name)
		free(cell1->name);
	if (cell1->value)
		free(cell1->value);
	free(cell1);
}

typedef struct
{
	slist_head	cells;
	slist_node	row_node;
} Row;

static void
row_free (Row *row1)
{
	slist_mutable_iter cell_iter1;

	slist_foreach_modify(cell_iter1, &row1->cells)
	{
		Cell *cell1 = slist_container(Cell, cell_node, cell_iter1.cur);
		cell_free(cell1);
	}

	free(row1);
}

static void
data_free (slist_head *data)
{
	slist_mutable_iter row_iter1;

	slist_foreach_modify(row_iter1, data)
	{
		Row *row1 = slist_container(Row, row_node, row_iter1.cur);
		row_free(row1);
	}
}

typedef enum
{
	GEDIT_JSON_TOP,
	GEDIT_JSON_ARRAY,
	GEDIT_JSON_OBJECT,
} gedit_context_type;

typedef struct
{
	gedit_context_type	context;
	char	   *fieldname;
	slist_head *data;
	Row		   *current_row;
	Cell	   *current_cell;
} gedit_parse_state;

static JsonParseErrorType
gedit_array_start_action(void *state)
{
	gedit_parse_state *pstate = state;

	if (pstate->context != GEDIT_JSON_TOP)
	{
		pg_log_error("\\gedit: arrays are only permitted as the top-level syntax element");
		return JSON_SEM_ACTION_FAILED;
	}
	pstate->context = GEDIT_JSON_ARRAY;

	return JSON_SUCCESS;
}

static JsonParseErrorType
gedit_object_start_action(void *state)
{
	gedit_parse_state *pstate = state;
	Row *row = calloc(sizeof(Row), 1);

	if (pstate->context != GEDIT_JSON_ARRAY)
	{
		pg_log_error("\\gedit: objects are only permitted inside the top-level array");
		return JSON_SEM_ACTION_FAILED;
	}
	pstate->context = GEDIT_JSON_OBJECT;

	if(slist_is_empty(pstate->data))
		slist_push_head(pstate->data, &row->row_node);
	else
		slist_insert_after(&pstate->current_row->row_node, &row->row_node);
	pstate->current_row = row;

	return JSON_SUCCESS;
}

static JsonParseErrorType
gedit_object_end_action(void *state)
{
	gedit_parse_state *pstate = state;
	pstate->context = GEDIT_JSON_ARRAY;
	return JSON_SUCCESS;
}

static JsonParseErrorType
gedit_object_field_start_action (void *state, char *fname, bool isnull)
{
	gedit_parse_state *pstate = state;
	Cell *cell = calloc(sizeof(Cell), 1);

	cell->name = fname;

	if(slist_is_empty(&pstate->current_row->cells))
		slist_push_head(&pstate->current_row->cells, &cell->cell_node);
	else
		slist_insert_after(&pstate->current_cell->cell_node, &cell->cell_node);
	pstate->current_cell = cell;

	return JSON_SUCCESS;
}

static JsonParseErrorType
gedit_scalar_action(void *state, char *token, JsonTokenType tokentype)
{
	gedit_parse_state *pstate = state;
	if (pstate->context != GEDIT_JSON_OBJECT)
	{
		pfree(token);
		pg_log_error("\\gedit: scalars are only permitted inside objects");
		return JSON_SEM_ACTION_FAILED;
	}

	/* when token "null", leave value NULL */
	if (tokentype == JSON_TOKEN_NULL)
	{
		pfree(token);
		pstate->current_cell->value = NULL;
	}
	else
		pstate->current_cell->value = token;

	return JSON_SUCCESS;
}

static bool
read_json_file(const char *fname, int *line_number, slist_head *data)
{
	bool		success = false;
	FILE	   *stream = NULL;
	char		line[1024];
	PQExpBuffer jsondata = createPQExpBuffer();

	gedit_parse_state parse_state = {
		.data = data
	};
	JsonSemAction geditSemAction = {
		.semstate = &parse_state,
		.array_start = gedit_array_start_action,
		.object_start = gedit_object_start_action,
		.object_end = gedit_object_end_action,
		.object_field_start = gedit_object_field_start_action,
		.scalar = gedit_scalar_action,
	};
	JsonLexContext jsonlexer;
	JsonParseErrorType parseresult;

	if (PQExpBufferBroken(jsondata))
	{
		pg_log_error("PQresultErrorField: out of memory");
		return false;
	}

	if (!(stream = fopen(fname, PG_BINARY_R)))
	{
		pg_log_error("%s: %m", fname);
		goto error;
	}

	while (fgets(line, sizeof(line), stream) != NULL)
		appendPQExpBufferStr(jsondata, line);

	if (ferror(stream))
	{
		pg_log_error("%s: %m", fname);
		goto error;
	}

	makeJsonLexContextCstringLen(&jsonlexer, jsondata->data, jsondata->len, pset.encoding, true);
	parseresult = pg_parse_json(&jsonlexer, &geditSemAction);
	if (line_number)
		*line_number = jsonlexer.line_number;

	success = (parseresult == JSON_SUCCESS);

error:
	destroyPQExpBuffer(jsondata);
	if (stream)
		fclose(stream);

	return success;
}

/* main code */

/* NULL-aware variant of strcmp() */
static bool
str_distinct(char *a, char *b)
{
	if ((a == NULL) && (b == NULL))
		return false;
	if ((a == NULL) || (b == NULL))
		return true;
	return strcmp(a, b) != 0;
}

/*
 * Check if value consists entirely of alphanumeric chars and the first
 * character is not a digit.
 */
static bool
identifier_needs_no_quoting(const char *value)
{
	return strspn(value, "abcdefghijklmnopqrstuvwxyz_0123456789") == strlen(value) &&
		   strspn(value, "0123456789") == 0;
}

/* NULL-aware variant of quote_literal */
static void
quote_literal_or_null(PQExpBuffer query_buf, char *value, bool as_ident)
{
	char *p;

	if (!value)
	{
		appendPQExpBufferStr(query_buf, "NULL");
		return;
	}

	if (as_ident)
	{
		if (identifier_needs_no_quoting(value)) /* skip quoting of simple names */
			appendPQExpBufferStr(query_buf, value);
		else
		{
			p = PQescapeIdentifier(pset.db, value, strlen(value));
			appendPQExpBufferStr(query_buf, p);
			free(p);
		}
	}
	else
	{
		p = PQescapeLiteral(pset.db, value, strlen(value));
		appendPQExpBufferStr(query_buf, p);
		free(p);
	}
}

static Row *
find_matching_row(Row *row1, slist_head *data2, PQExpBuffer where_clause)
{
	slist_mutable_iter row_iter2;

	slist_foreach_modify(row_iter2, data2)
	{
		Row *row2 = slist_container(Row, row_node, row_iter2.cur);
		char **key_column;

		for (key_column = pset.gedit_key_columns; *key_column; key_column++)
		{
			slist_iter cell_iter1;
			Cell *cell1;

			slist_foreach(cell_iter1, &row1->cells)
			{
				slist_iter cell_iter2;
				cell1 = slist_container(Cell, cell_node, cell_iter1.cur);

				if (strcmp(*key_column, cell1->name) != 0)
					continue; /* look at next column */

				slist_foreach(cell_iter2, &row2->cells)
				{
					Cell *cell2 = slist_container(Cell, cell_node, cell_iter2.cur);
					if (strcmp(*key_column, cell2->name) != 0)
						continue; /* look at next column */

					if (str_distinct(cell1->value, cell2->value) != 0)
						goto next_row; /* key column value doesn't match, look at next row */

					/* matching column found */
					if (key_column == pset.gedit_key_columns) /* first key column */
						printfPQExpBuffer(where_clause, " WHERE ");
					else
						appendPQExpBufferStr(where_clause, " AND ");
					quote_literal_or_null(where_clause, cell1->name, true);
					appendPQExpBufferStr(where_clause, " = ");
					quote_literal_or_null(where_clause, cell1->value, false);

					goto next_key; /* look at next key column */
				}
				/* key column not found in edited result; ignore row*/
				goto next_row;
			}
			/* should not happen, report error and move on */
			pg_log_error("\\gedit: key column \"%s\" not found in original result", *key_column);
			return NULL;

next_key:
		}

		/* all columns matched */
		slist_delete_current(&row_iter2); /* caller needs to free row */
		appendPQExpBufferStr(where_clause, ";");
		return row2;

next_row:
	}

	return NULL;
}

static void
generate_where_clause(PQExpBuffer query_buf, Row *row1)
{
	char **key_column;

	appendPQExpBuffer(query_buf, " WHERE ");

	for (key_column = pset.gedit_key_columns; *key_column; key_column++)
	{
		slist_iter cell_iter1;

		slist_foreach(cell_iter1, &row1->cells)
		{
			Cell *cell1 = slist_container(Cell, cell_node, cell_iter1.cur);

			if (strcmp(*key_column, cell1->name) == 0)
			{
				if (key_column != pset.gedit_key_columns) /* not the first key column */
					appendPQExpBufferStr(query_buf, " AND ");
				quote_literal_or_null(query_buf, cell1->name, true);
				appendPQExpBufferStr(query_buf, " = ");
				quote_literal_or_null(query_buf, cell1->value, false);
			}
		}
	}

	appendPQExpBufferStr(query_buf, ";");
}

#define APPEND_NEWLINE \
	if (cmd_started) \
		appendPQExpBufferChar(query_buf, '\n'); \
	cmd_started = true

#define APPEND_COMMA \
	if (list_started) \
		appendPQExpBufferStr(query_buf, ", "); \
	list_started = true

static bool
generate_update_commands(PQExpBuffer query_buf, slist_head *data1, slist_head *data2)
{
	slist_iter row_iter1, row_iter2;
	bool		cmd_started = false;
	PQExpBuffer	where_clause = createPQExpBuffer();

	/* look for differing rows, produce UPDATE and DELETE statements */
	slist_foreach(row_iter1, data1)
	{
		Row *row1 = slist_container(Row, row_node, row_iter1.cur);
		Row *row2 = find_matching_row(row1, data2, where_clause);
		slist_mutable_iter cell_iter1, cell_iter2;
		bool		list_started = false;

		/* no matching row was found, DELETE original row */
		if (!row2)
		{
			APPEND_NEWLINE;
			appendPQExpBuffer(query_buf, "DELETE FROM %s", pset.gedit_table);
			generate_where_clause(query_buf, row1);
			continue;
		}

		/* loop over both rows */
		slist_foreach_modify(cell_iter1, &row1->cells)
		{
			Cell *cell1 = slist_container(Cell, cell_node, cell_iter1.cur);

			slist_foreach_modify(cell_iter2, &row2->cells)
			{
				Cell *cell2 = slist_container(Cell, cell_node, cell_iter2.cur);

				if (strcmp(cell1->name, cell2->name) != 0)
					continue; /* cell names do not match */

				if (str_distinct(cell1->value, cell2->value))
				{
					if (!list_started)
					{
						APPEND_NEWLINE;
						appendPQExpBuffer(query_buf, "UPDATE %s SET ", pset.gedit_table);
					}
					APPEND_COMMA;
					quote_literal_or_null(query_buf, cell2->name, true);
					appendPQExpBufferStr(query_buf, " = ");
					quote_literal_or_null(query_buf, cell2->value, false);
				}

				/* no need to look again at these cells, delete them */
				slist_delete_current(&cell_iter1);
				cell_free(cell1);
				slist_delete_current(&cell_iter2);
				cell_free(cell2);

				break;
			}
		}

		/* any cells left in row2 were manually added, include them in the UPDATE */
		slist_foreach_modify(cell_iter2, &row2->cells)
		{
			Cell *cell2 = slist_container(Cell, cell_node, cell_iter2.cur);
			if (list_started)
				appendPQExpBufferStr(query_buf, ",");
			else
			{
				APPEND_NEWLINE;
				appendPQExpBuffer(query_buf, "UPDATE %s SET", pset.gedit_table);
				list_started = true;
			}
			appendPQExpBufferStr(query_buf, " ");
			quote_literal_or_null(query_buf, cell2->name, true);
			appendPQExpBufferStr(query_buf, " = ");
			quote_literal_or_null(query_buf, cell2->value, false);

			slist_delete_current(&cell_iter2);
			cell_free(cell2);
		}
		/*
		 * Any cells left in row1 were deleted while editing, ignore them.
		 * (We could set them NULL, but let's better require the user do that
		 * explicitly.)
		 */

		if (list_started)
			appendPQExpBufferStr(query_buf, where_clause->data);

		row_free(row2);
	}

	/* all rows left in data2 are newly added, produce INSERT statements */
	slist_foreach(row_iter2, data2)
	{
		Row *row2 = slist_container(Row, row_node, row_iter2.cur);
		slist_iter	cell_iter2;
		bool		list_started = false;

		APPEND_NEWLINE;
		appendPQExpBuffer(query_buf, "INSERT INTO %s (", pset.gedit_table);

		slist_foreach(cell_iter2, &row2->cells)
		{
			Cell *cell2 = slist_container(Cell, cell_node, cell_iter2.cur);
			APPEND_COMMA;
			quote_literal_or_null(query_buf, cell2->name, true);
		}

		appendPQExpBuffer(query_buf, ") VALUES (");
		list_started = false;

		slist_foreach(cell_iter2, &row2->cells)
		{
			Cell *cell2 = slist_container(Cell, cell_node, cell_iter2.cur);
			APPEND_COMMA;
			quote_literal_or_null(query_buf, cell2->value, false);
		}

		appendPQExpBuffer(query_buf, ");");
	}

	if (cmd_started)
		printf("%s\n", query_buf->data);

	destroyPQExpBuffer(where_clause);
	return cmd_started;
}

/*
 * gedit_edit_and_build_query -- handler for \gedit
 *
 * This function reads the query result in JSON format and calls an editor. If
 * the file was edited, it reads the file contents again, and puts the diff
 * into the query buffer as INSERT/UPDATE/DELETE commands.
 */
backslashResult
gedit_edit_and_build_query(const char *fname, PQExpBuffer query_buf)
{
	bool		error = false;
	bool		edited = false;
	int			line_number = 0;
	slist_head	data1 = {0};
	slist_head	data2 = {0};
	backslashResult result = PSQL_CMD_SKIP_LINE;

	if (!read_json_file(fname, NULL, &data1))
	{
		pg_log_error("%s: invalid json data even before editing", fname);
		error = true;
	}

	while (!error)
	{
		char	   *retry;
		bool		do_retry;

		/* call editor */
		edited = false;
		error = !editFileWithCheck(fname, line_number, &edited);

		if (!edited)
			break;
		if (error)
		{
			pg_log_error("%s: %m", fname);
			continue;
		}
		if (read_json_file(fname, &line_number, &data2))
			break;

		/* loop until edit produces valid json */
		retry = simple_prompt(_("Edit again? [y] "), true);
		if (!retry)
		{
			pg_log_error("simple_prompt: out of memory");
			error = true;
			break;
		}
		do_retry = (*retry == '\0' || *retry == 'y');
		free(retry);
		if (!do_retry)
		{
			error = true;
			break;
		}
	}

	/* remove temp file */
	if (remove(fname) == -1)
	{
		pg_log_error("%s: %m", fname);
		error = true;
	}

	if (error)
		result = PSQL_CMD_ERROR;
	else if (edited)
	{
		/* generate command to run from diffing data1 and data2 */
		if (generate_update_commands(query_buf, &data1, &data2))
			result = PSQL_CMD_NEWEDIT;
	}
	if (result == PSQL_CMD_SKIP_LINE) /* either file wasn't edited, or editing didn't change data */
		pg_log_info("\\gedit: no changes");

	if (data1.head.next)
		data_free(&data1);
	if (data2.head.next)
		data_free(&data2);

	return result;
}


