/*-------------------------------------------------------------------------
 *
 * ddltrigger.c
 *	  Logical DDL messages.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/ddltrigger.c
 *
 * NOTES
 *
 * Deparse the ddl command and log it.
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_class.h"
#include "commands/event_trigger.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "replication/ddlmessage.h"
#include "tcop/ddl_deparse.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"

extern EventTriggerQueryState *currentEventTriggerState;

/*
 * Deparse the ddl command and log it prior to
 * execution. Currently only used for DROP TABLE command
 * so that catalog can be accessed before being deleted.
 * This is to check if the table is part of the publication
 * or not.
 */
Datum
publication_deparse_ddl_command_start(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;
	char	   *command = psprintf("Drop table command start");
	DropStmt   *stmt;
	ListCell   *cell1;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	trigdata = (EventTriggerData *) fcinfo->context;
	stmt = (DropStmt *) trigdata->parsetree;

	/* Extract the relid from the parse tree */
	foreach(cell1, stmt->objects)
	{
		char		relpersist;
		Node	   *object = lfirst(cell1);
		ObjectAddress address;
		Relation	relation = NULL;

		address = get_object_address(stmt->removeType,
									 object,
									 &relation,
									 AccessExclusiveLock,
									 true);

		/* Object does not exist, nothing to do */
		if (!relation)
			continue;

		relpersist = get_rel_persistence(address.objectId);

		/*
		 * Do not generate wal log for commands whose target table is a
		 * temporary or unlogged table.
		 *
		 * XXX We may generate wal logs for unlogged tables in the future so
		 * that unlogged tables can also be created and altered on the
		 * subscriber side. This makes it possible to directly replay the SET
		 * LOGGED command and the incoming rewrite message without creating a
		 * new table.
		 */
		if (relpersist == RELPERSISTENCE_PERMANENT)
		{
			DeparsedCommandType cmdtype;

			if (stmt->removeType == OBJECT_TABLE)
				cmdtype = DCT_TableDropStart;
			else
				cmdtype = DCT_ObjectDropStart;

			LogLogicalDDLMessage("deparse", address.objectId, cmdtype,
								 command, strlen(command) + 1);
		}

		if (relation)
			table_close(relation, NoLock);
	}
	return PointerGetDatum(NULL);
}

/*
 * publication_deparse_table_rewrite
 *
 * Deparse the ddl table rewrite command and log it.
 */
Datum
publication_deparse_table_rewrite(PG_FUNCTION_ARGS)
{
	char		relpersist;
	CollectedCommand *cmd;
	char	   *json_string;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	cmd = currentEventTriggerState->currentCommand;

	Assert(cmd && cmd->d.alterTable.rewrite);

	relpersist = get_rel_persistence(cmd->d.alterTable.objectId);

	/*
	 * Do not generate wal log for commands whose target table is a temporary
	 * or unlogged table.
	 *
	 * XXX We may generate wal logs for unlogged tables in the future so that
	 * unlogged tables can also be created and altered on the subscriber side.
	 * This makes it possible to directly replay the SET LOGGED command and the
	 * incoming rewrite message without creating a new table.
	 */
	if (relpersist == RELPERSISTENCE_PERMANENT)
	{
		/* Deparse the DDL command and WAL log it to allow decoding of the same. */
		json_string = deparse_utility_command(cmd, true, false);

		if (json_string != NULL)
			LogLogicalDDLMessage("deparse", cmd->d.alterTable.objectId, DCT_TableAlter,
								 json_string, strlen(json_string) + 1);
	}

	return PointerGetDatum(NULL);
}

/*
 * Deparse the ddl command and log it. This function
 * is called after the execution of the command but before the
 * transaction commits.
 */
Datum
publication_deparse_ddl_command_end(PG_FUNCTION_ARGS)
{
	ListCell   *lc;
	slist_iter	iter;
	DeparsedCommandType type;
	Oid			relid;
	char		relkind;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	foreach(lc, currentEventTriggerState->commandList)
	{
		char		relpersist = RELPERSISTENCE_PERMANENT;
		CollectedCommand *cmd = lfirst(lc);
		char	   *json_string;

		/* Rewrite DDL has been handled in table_rewrite trigger */
		if (cmd->d.alterTable.rewrite)
		{
			RenameStmt *renameStmt = (RenameStmt *) cmd->parsetree;

			if (renameStmt && renameStmt->relationType != OBJECT_TYPE &&
				renameStmt->relationType != OBJECT_TABLE)
				continue;
		}

		if (cmd->type == SCT_Simple &&
			!OidIsValid(cmd->d.simple.address.objectId))
			continue;

		if (cmd->type == SCT_AlterTable)
		{
			relid = cmd->d.alterTable.objectId;
			type = DCT_TableAlter;
		}
		else
		{
			/* Only SCT_Simple for now */
			relid = cmd->d.simple.address.objectId;
			type = DCT_SimpleCmd;
		}

		relkind = get_rel_relkind(relid);
		if (relkind)
			relpersist = get_rel_persistence(relid);

		/*
		 * Do not generate wal log for commands whose target table is a
		 * temporary or unlogged table.
		 *
		 * XXX We may generate wal logs for unlogged tables in the future so
		 * that unlogged tables can also be created and altered on the
		 * subscriber side. This makes it possible to directly replay the SET
		 * LOGGED command and the incoming rewrite message without creating a
		 * new table.
		 */
		if (relpersist == RELPERSISTENCE_PERMANENT)
		{
			/*
			 * Deparse the DDL command and WAL log it to allow decoding of the
			 * same.
			 */
			json_string = deparse_utility_command(cmd, true, false);

			if (json_string != NULL)
				LogLogicalDDLMessage("deparse", relid, type, json_string,
									 strlen(json_string) + 1);
		}
	}

	slist_foreach(iter, &(currentEventTriggerState->SQLDropList))
	{
		SQLDropObject *obj;
		DropStmt   *stmt;
		EventTriggerData *trigdata;
		char	   *command;
		DeparsedCommandType cmdtype;
		const char *tmptype;
		ObjectClass	objclass;
		ObjectAddress objaddr;

		trigdata = (EventTriggerData *) fcinfo->context;
		stmt = (DropStmt *) trigdata->parsetree;

		obj = slist_container(SQLDropObject, next, iter.cur);
		objaddr = obj->address;
		objclass = getObjectClass(&objaddr);

		if (strcmp(obj->objecttype, "table") == 0)
			cmdtype = DCT_TableDropEnd;
		else if (objclass == OCLASS_SCHEMA ||
				 objclass == OCLASS_OPERATOR ||
				 objclass == OCLASS_OPCLASS ||
				 objclass == OCLASS_OPFAMILY ||
				 objclass == OCLASS_CAST ||
				 objclass == OCLASS_TYPE ||
				 objclass == OCLASS_TRIGGER ||
				 objclass == OCLASS_CONVERSION ||
				 objclass == OCLASS_POLICY ||
				 objclass == OCLASS_REWRITE ||
				 objclass == OCLASS_EXTENSION ||
				 objclass == OCLASS_FDW ||
				 objclass == OCLASS_TSCONFIG ||
				 objclass == OCLASS_TSDICT ||
				 objclass == OCLASS_TSTEMPLATE ||
				 objclass == OCLASS_TSPARSER ||
				 objclass == OCLASS_TRANSFORM ||
				 objclass == OCLASS_FOREIGN_SERVER ||
				 objclass == OCLASS_COLLATION ||
				 objclass == OCLASS_USER_MAPPING ||
				 objclass == OCLASS_LANGUAGE ||
				 objclass == OCLASS_STATISTIC_EXT ||
				 objclass == OCLASS_AM ||
				 objclass == OCLASS_PUBLICATION ||
				 objclass == OCLASS_PUBLICATION_REL ||
				 objclass == OCLASS_PUBLICATION_NAMESPACE ||
				 strcmp(obj->objecttype, "foreign table") == 0 ||
				 strcmp(obj->objecttype, "index") == 0 ||
				 strcmp(obj->objecttype, "sequence") == 0 ||
				 strcmp(obj->objecttype, "view") == 0 ||
				 strcmp(obj->objecttype, "function") == 0 ||
				 strcmp(obj->objecttype, "materialized view") == 0 ||
				 strcmp(obj->objecttype, "procedure") == 0 ||
				 strcmp(obj->objecttype, "routine") == 0 ||
				 strcmp(obj->objecttype, "aggregate") == 0)
			cmdtype = DCT_ObjectDropEnd;
		else
			continue;

		/* Change foreign-data wrapper to foreign data wrapper */
		if (strncmp(obj->objecttype, "foreign-data wrapper", 20) == 0)
		{
			tmptype = pstrdup("foreign data wrapper");
			command = deparse_drop_command(obj->objidentity, tmptype,
										   stmt->behavior);
		}

		/* Change statistics object to statistics */
		else if (strncmp(obj->objecttype, "statistics object",
						 strlen("statistics object")) == 0)
		{
			tmptype = pstrdup("statistics");
			command = deparse_drop_command(obj->objidentity, tmptype,
										   stmt->behavior);
		}

		/*
		 * Object identity needs to be modified to make the drop work
		 *
		 * FROM: <role> on server <servername> TO  : for <role> server
		 * <servername>
		 *
		 */
		else if (strncmp(obj->objecttype, "user mapping", 12) == 0)
		{
			char	   *on_server;

			tmptype = palloc(strlen(obj->objidentity) + 2);
			on_server = strstr(obj->objidentity, "on server");

			sprintf((char *) tmptype, "for ");
			strncat((char *) tmptype, obj->objidentity, on_server - obj->objidentity);
			strcat((char *) tmptype, on_server + 3);
			command = deparse_drop_command(tmptype, obj->objecttype,
										   stmt->behavior);
		}
		else if (strncmp(obj->objecttype, "publication namespace",
						 strlen("publication namespace")) == 0 ||
				 strncmp(obj->objecttype, "publication relation",
						 strlen("publication relation")) == 0)
			command = deparse_AlterPublicationDropStmt(obj);
		else
			command = deparse_drop_command(obj->objidentity, obj->objecttype,
										   stmt->behavior);

		if (command)
			LogLogicalDDLMessage("deparse", obj->address.objectId, cmdtype,
								 command, strlen(command) + 1);
	}

	return PointerGetDatum(NULL);
}


/*
 * publication_deparse_table_init_write
 *
 * Deparse the ddl table create command and log it.
 */
Datum
publication_deparse_table_init_write(PG_FUNCTION_ARGS)
{
	char		relpersist;
	CollectedCommand *cmd;
	char	   *json_string;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	cmd = currentEventTriggerState->currentCommand;
	Assert(cmd);

	relpersist = get_rel_persistence(cmd->d.simple.address.objectId);

	/*
	 * Do not generate wal log for commands whose target table is a temporary
	 * table.
	 *
	 * We will generate wal logs for unlogged tables so that unlogged tables
	 * can also be created and altered on the subscriber side. This makes it
	 * possible to directly replay the SET LOGGED command and the incoming
	 * rewrite message without creating a new table.
	 */
	if (relpersist != RELPERSISTENCE_PERMANENT)
		return PointerGetDatum(NULL);

	/* Deparse the DDL command and WAL log it to allow decoding of the same. */
	json_string = deparse_utility_command(cmd, true, false);

	if (json_string != NULL)
		LogLogicalDDLMessage("deparse", cmd->d.simple.address.objectId, DCT_SimpleCmd,
							 json_string, strlen(json_string) + 1);

	return PointerGetDatum(NULL);
}
