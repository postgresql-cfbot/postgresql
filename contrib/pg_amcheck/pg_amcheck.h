/*-------------------------------------------------------------------------
 *
 * pg_amcheck.h
 *		Detects corruption within database relations.
 *
 * Copyright (c) 2020-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_amcheck/pg_amcheck.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AMCHECK_H
#define PG_AMCHECK_H

#include "fe_utils/simple_list.h"
#include "fe_utils/string_utils.h"
#include "libpq-fe.h"
#include "pqexpbuffer.h"		/* pgrminclude ignore */

/* amcheck options controlled by user flags */
typedef struct amcheckOptions
{
	bool		alldb;
	bool		echo;
	bool		quiet;
	bool		verbose;
	bool		dependents;
	bool		no_indexes;
	bool		exclude_toast;
	bool		reconcile_toast;
	bool		on_error_stop;
	bool		parent_check;
	bool		rootdescend;
	bool		heapallindexed;
	const char *skip;
	int			jobs;			/* >= 0 indicates user specified the parallel
								 * degree, otherwise -1 */
	long		startblock;
	long		endblock;
} amcheckOptions;

/* names of database objects to include or exclude controlled by user flags */
typedef struct amcheckObjects
{
	SimpleStringList databases;
	SimpleStringList schemas;
	SimpleStringList tables;
	SimpleStringList indexes;
	SimpleStringList exclude_databases;
	SimpleStringList exclude_schemas;
	SimpleStringList exclude_tables;
	SimpleStringList exclude_indexes;
} amcheckObjects;

/*
 * We cannot launch the same amcheck function for all checked objects.  For
 * btree indexes, we must use either bt_index_check() or
 * bt_index_parent_check().  For heap relations, we must use verify_heapam().
 * We silently ignore all other object types.
 *
 * The following CheckType enum and corresponding ct_filter array track which
 * which kinds of relations get which treatment.
 */
typedef enum
{
	CT_TABLE = 0,
	CT_BTREE
} CheckType;

/*
 * This struct is used for filtering relations in pg_catalog.pg_class to just
 * those of a given CheckType.  The relam field should equal pg_class.relam,
 * and the pg_class.relkind should be contained in the relkinds comma separated
 * list.
 *
 * The 'typname' field is not strictly for filtering, but for printing messages
 * about relations that matched the filter.
 */
typedef struct
{
	Oid			relam;
	const char *relkinds;
	const char *typname;
} CheckTypeFilter;

/* Constants taken from pg_catalog/pg_am.dat */
#define HEAP_TABLE_AM_OID 2
#define BTREE_AM_OID 403

static void check_each_database(ConnParams *cparams,
								const amcheckObjects *objects,
								const amcheckOptions *checkopts,
								const char *progname);

static void check_one_database(const ConnParams *cparams,
							   const amcheckObjects *objects,
							   const amcheckOptions *checkopts,
							   const char *progname);
static void prepare_table_command(PQExpBuffer sql,
								  const amcheckOptions *checkopts, Oid reloid);

static void prepare_btree_command(PQExpBuffer sql,
								  const amcheckOptions *checkopts, Oid reloid);
static void run_command(PGconn *conn, const char *sql,
						const amcheckOptions *checkopts, Oid reloid,
						const char *typ);

static PGresult *VerifyHeapamSlotHandler(PGresult *res, PGconn *conn,
										 void *context);

static PGresult *VerifyBtreeSlotHandler(PGresult *res, PGconn *conn,
										void *context);

static void help(const char *progname);


static void get_db_regexes_from_fqrps(SimpleStringList *regexes,
									  const SimpleStringList *patterns);

static void get_db_regexes_from_patterns(SimpleStringList *regexes,
										 const SimpleStringList *patterns);

static void dbname_select(PGconn *conn, PQExpBuffer sql,
						  const SimpleStringList *regexes, bool alldb);

static void target_select(PGconn *conn, PQExpBuffer sql,
						  const amcheckObjects *objects,
						  const amcheckOptions *options, const char *progname,
						  bool inclusive);

#endif							/* PG_AMCHECK_H */
