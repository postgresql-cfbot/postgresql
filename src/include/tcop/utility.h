/*-------------------------------------------------------------------------
 *
 * utility.h
 *	  prototypes for utility.c.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/utility.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UTILITY_H
#define UTILITY_H

#include "tcop/tcopprot.h"

typedef enum
{
	PROCESS_UTILITY_TOPLEVEL,	/* toplevel interactive command */
	PROCESS_UTILITY_QUERY,		/* a complete query, but not toplevel */
	PROCESS_UTILITY_QUERY_NONATOMIC,	/* a complete query, nonatomic
										 * execution context */
	PROCESS_UTILITY_SUBCOMMAND	/* a portion of a query */
} ProcessUtilityContext;

/* ----------
 * The sql statement types for pg_stat_sql
 * ----------
 */
typedef enum StatSqlType
{
	T_Stat_INSERT,
	T_Stat_DELETE,
	T_Stat_UPDATE,
	T_Stat_SELECT,
	T_Stat_BEGIN,
	T_Stat_START_TRANSACTION,
	T_Stat_COMMIT,
	T_Stat_ROLLBACK,
	T_Stat_SAVEPOINT,
	T_Stat_RELEASE,
	T_Stat_PREPARE_TRANSACTION,
	T_Stat_COMMIT_PREPARED,
	T_Stat_ROLLBACK_PREPARED,
	T_Stat_DECLARE_CURSOR,
	T_Stat_CLOSE_CURSOR_ALL,
	T_Stat_CLOSE_CURSOR,
	T_Stat_MOVE,
	T_Stat_FETCH,
	T_Stat_CREATE_DOMAIN,
	T_Stat_CREATE_SCHEMA,
	T_Stat_CREATE_TABLE,
	T_Stat_CREATE_TABLESPACE,
	T_Stat_DROP_TABLESPACE,
	T_Stat_ALTER_TABLESPACE,
	T_Stat_CREATE_EXTENSION,
	T_Stat_ALTER_EXTENSION,
	T_Stat_CREATE_FOREIGN_DATA_WRAPPER,
	T_Stat_ALTER_FOREIGN_DATA_WRAPPER,
	T_Stat_CREATE_SERVER,
	T_Stat_ALTER_SERVER,
	T_Stat_CREATE_USER_MAPPING,
	T_Stat_ALTER_USER_MAPPING,
	T_Stat_DROP_USER_MAPPING,
	T_Stat_CREATE_FOREIGN_TABLE,
	T_Stat_IMPORT_FOREIGN_SCHEMA,
	T_Stat_DROP_TABLE,
	T_Stat_DROP_SEQUENCE,
	T_Stat_DROP_VIEW,
	T_Stat_DROP_MATERIALIZED_VIEW,
	T_Stat_DROP_INDEX,
	T_Stat_DROP_TYPE,
	T_Stat_DROP_DOMAIN,
	T_Stat_DROP_COLLATION,
	T_Stat_DROP_CONVERSION,
	T_Stat_DROP_SCHEMA,
	T_Stat_DROP_TEXT_SEARCH_PARSER,
	T_Stat_DROP_TEXT_SEARCH_DICTIONARY,
	T_Stat_DROP_TEXT_SEARCH_TEMPLATE,
	T_Stat_DROP_DROP_TEXT_SEARCH_CONFIGURATION,
	T_Stat_DROP_DROP_FOREIGN_TABLE,
	T_Stat_DROP_EXTENSION,
	T_Stat_DROP_FUNCTION,
	T_Stat_DROP_PROCEDURE,
	T_Stat_DROP_ROUTINE,
	T_Stat_DROP_AGGREGATE,
	T_Stat_DROP_OPERATOR,
	T_Stat_DROP_LANGUAGE,
	T_Stat_DROP_CAST,
	T_Stat_DROP_TRIGGER,
	T_Stat_DROP_EVENT_TRIGGER,
	T_Stat_DROP_RULE,
	T_Stat_DROP_FOREIGN_DATA_WRAPPER,
	T_Stat_DROP_SERVER,
	T_Stat_DROP_OPERATOR_CLASS,
	T_Stat_DROP_OPERATOR_FAMILY,
	T_Stat_DROP_POLICY,
	T_Stat_DROP_TRANSFORM,
	T_Stat_DROP_ACCESS_METHOD,
	T_Stat_DROP_PUBLICATION,
	T_Stat_DROP_STATISTICS,
	T_Stat_TRUNCATE_TABLE,
	T_Stat_COMMENT,
	T_Stat_SECURITY_LABEL,
	T_Stat_COPY,
	T_Stat_ALTER_AGGREGATE,
	T_Stat_ALTER_TYPE,
	T_Stat_ALTER_CAST,
	T_Stat_ALTER_COLLATION,
	T_Stat_ALTER_TABLE,
	T_Stat_ALTER_CONVERSION,
	T_Stat_ALTER_DATABASE,
	T_Stat_ALTER_DOMAIN,
	T_Stat_ALTER_FOREIGN_TABLE,
	T_Stat_ALTER_FUNCTION,
	T_Stat_ALTER_INDEX,
	T_Stat_ALTER_LANGUAGE,
	T_Stat_ALTER_LARGE_OBJECT,
	T_Stat_ALTER_OPERATOR_CLASS,
	T_Stat_ALTER_OPERATOR,
	T_Stat_ALTER_OPERATOR_FAMILY,
	T_Stat_ALTER_POLICY,
	T_Stat_ALTER_PROCEDURE,
	T_Stat_ALTER_ROLE,
	T_Stat_ALTER_ROUTINE,
	T_Stat_ALTER_RULE,
	T_Stat_ALTER_SCHEMA,
	T_Stat_ALTER_SEQUENCE,
	T_Stat_ALTER_TRIGGER,
	T_Stat_ALTER_EVENT_TRIGGER,
	T_Stat_ALTER_TEXT_SEARCH_CONFIGURATION,
	T_Stat_ALTER_TEXT_SEARCH_DICTIONARY,
	T_Stat_ALTER_TEXT_SEARCH_PARSER,
	T_Stat_ALTER_TEXT_SEARCH_TEMPLATE,
	T_Stat_ALTER_VIEW,
	T_Stat_ALTER_MATERIALIZED_VIEW,
	T_Stat_ALTER_PUBLICATION,
	T_Stat_ALTER_SUBSCRIPTION,
	T_Stat_ALTER_STATISTICS,
	T_Stat_GRANT,
	T_Stat_REVOKE,
	T_Stat_GRANT_ROLE,
	T_Stat_REVOKE_ROLE,
	T_Stat_ALTER_DEFAULT_PRIVILEGES,
	T_Stat_CREATE_AGGREGATE,
	T_Stat_CREATE_OPERATOR,
	T_Stat_CREATE_TYPE,
	T_Stat_CREATE_TEXT_SEARCH_PARSER,
	T_Stat_CREATE_TEXT_SEARCH_DICTIONARY,
	T_Stat_CREATE_TEXT_SEARCH_TEMPLATE,
	T_Stat_CREATE_TEXT_SEARCH_CONFIGURATION,
	T_Stat_CREATE_COLLATION,
	T_Stat_CREATE_ACCESS_METHOD,
	T_Stat_CREATE_VIEW,
	T_Stat_CREATE_PROCEDURE,
	T_Stat_CREATE_FUNCTION,
	T_Stat_CREATE_INDEX,
	T_Stat_CREATE_RULE,
	T_Stat_CREATE_SEQUENCE,
	T_Stat_DO,
	T_Stat_CREATE_DATABASE,
	T_Stat_DROP_DATABASE,
	T_Stat_NOTIFY,
	T_Stat_LISTEN,
	T_Stat_UNLISTEN,
	T_Stat_LOAD,
	T_Stat_CALL,
	T_Stat_CLUSTER,
	T_Stat_VACUUM,
	T_Stat_ANALYZE,
	T_Stat_EXPLAIN,
	T_Stat_SELECT_INTO,
	T_Stat_CREATE_TABLE_AS,
	T_Stat_CREATE_MATERIALIZED_VIEW,
	T_Stat_REFRESH_MATERIALIZED_VIEW,
	T_Stat_ALTER_SYSTEM,
	T_Stat_SET,
	T_Stat_RESET,
	T_Stat_SHOW,
	T_Stat_DISCARD_ALL,
	T_Stat_DISCARD_PLANS,
	T_Stat_DISCARD_TEMP,
	T_Stat_DISCARD_SEQUENCES,
	T_Stat_CREATE_TRANSFORM,
	T_Stat_CREATE_TRIGGER,
	T_Stat_CREATE_EVENT_TRIGGER,
	T_Stat_CREATE_LANGUAGE,
	T_Stat_CREATE_ROLE,
	T_Stat_DROP_ROLE,
	T_Stat_DROP_OWNED,
	T_Stat_REASSIGN_OWNED,
	T_Stat_LOCK_TABLE,
	T_Stat_SET_CONSTRAINTS,
	T_Stat_CHECKPOINT,
	T_Stat_REINDEX,
	T_Stat_CREATE_CONVERSION,
	T_Stat_CREATE_CAST,
	T_Stat_CREATE_OPERATOR_CLASS,
	T_Stat_CREATE_OPERATOR_FAMILY,
	T_Stat_CREATE_POLICY,
	T_Stat_CREATE_PUBLICATION,
	T_Stat_CREATE_SUBSCRIPTION,
	T_Stat_DROP_SUBSCRIPTION,
	T_Stat_PREPARE,
	T_Stat_EXECUTE,
	T_Stat_CREATE_STATISTICS,
	T_Stat_DEALLOCATE_ALL,
	T_Stat_DEALLOCATE,
	T_Stat_SELECT_FOR_KEY_SHARE,
	T_Stat_SELECT_FOR_SHARE,
	T_Stat_SELECT_FOR_NO_KEY_UPDATE,
	T_Stat_SELECT_FOR_UPDATE,

	T_Stat_UNKNOWN

} StatSqlType;

#define PGSTAT_SQLSTMT_SIZE (T_Stat_UNKNOWN + 1)

extern const char* g_str_sql_type[];


/* Hook for plugins to get control in ProcessUtility() */
typedef void (*ProcessUtility_hook_type) (PlannedStmt *pstmt,
										  const char *queryString, ProcessUtilityContext context,
										  ParamListInfo params,
										  QueryEnvironment *queryEnv,
										  DestReceiver *dest, char *completionTag);
extern PGDLLIMPORT ProcessUtility_hook_type ProcessUtility_hook;

extern void ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						   ProcessUtilityContext context, ParamListInfo params,
						   QueryEnvironment *queryEnv,
						   DestReceiver *dest, char *completionTag);
extern void standard_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									ProcessUtilityContext context, ParamListInfo params,
									QueryEnvironment *queryEnv,
									DestReceiver *dest, char *completionTag);

extern bool UtilityReturnsTuples(Node *parsetree);

extern TupleDesc UtilityTupleDescriptor(Node *parsetree);

extern Query *UtilityContainsQuery(Node *parsetree);

extern const char *CreateCommandTag(Node *parsetree);

extern StatSqlType CreateCommandType(Node *parsetree);

extern const char* CreateCommandTagType(Node *parsetree, StatSqlType* pESqlType);

extern LogStmtLevel GetCommandLogLevel(Node *parsetree);

extern bool CommandIsReadOnly(PlannedStmt *pstmt);

#endif							/* UTILITY_H */
