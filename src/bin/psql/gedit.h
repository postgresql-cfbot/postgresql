/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/bin/psql/gedit.h
 */
#ifndef GEDIT_H
#define GEDIT_H

extern bool process_command_gedit_options(PsqlScanState scan_state, bool active_branch);
extern char *gedit_table_name(PQExpBuffer query_buf);
extern char **gedit_table_key_columns(PGconn *conn, const char *query, const char *table);
extern bool gedit_check_key_columns(PGconn *conn, const char *query, char **key_columns);
extern backslashResult gedit_edit_and_build_query(const char *filename_arg, PQExpBuffer query_buf);

#endif
