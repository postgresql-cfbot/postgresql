/* ----------
 * backend_progress.h
 *	  Command progress reporting definition.
 *
 * Note that this file provides the infrastructure for storing a single
 * backend's command progress counters, without ascribing meaning to the
 * individual fields. See commands/progress.h and system_views.sql for that.
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_progress.h
 * ----------
 */
#ifndef BACKEND_PROGRESS_H
#define BACKEND_PROGRESS_H


/* ----------
 * Command type for progress reporting purposes
 * ----------
 */
typedef enum ProgressCommandType
{
	PROGRESS_COMMAND_INVALID,
	PROGRESS_COMMAND_VACUUM,
	PROGRESS_COMMAND_ANALYZE,
	PROGRESS_COMMAND_CLUSTER,
	PROGRESS_COMMAND_CREATE_INDEX,
	PROGRESS_COMMAND_BASEBACKUP,
	PROGRESS_COMMAND_COPY,
} ProgressCommandType;


#define PGSTAT_NUM_PROGRESS_PARAM	20

/*
 * Any command which wishes can advertise that it is running by setting
 * command, command_target, and param[].  command_target should be the OID of
 * the relation which the command targets (we assume there's just one, as this
 * is meant for utility commands), but the meaning of each element in the
 * param array is command-specific.
 */
typedef struct PgBackendProgress
{
	ProgressCommandType command;
	Oid			command_target;
	int64		param[PGSTAT_NUM_PROGRESS_PARAM];
} PgBackendProgress;

extern void pgstat_progress_start_command(ProgressCommandType cmdtype,
										  Oid relid);
extern void pgstat_progress_update_param(int index, int64 val);
extern void pgstat_progress_incr_param(int index, int64 incr);
extern void pgstat_progress_parallel_incr_param(int index, int64 incr);
extern void pgstat_progress_update_multi_param(int nparam, const int *index,
											   const int64 *val);
extern void pgstat_progress_end_command(void);


#endif							/* BACKEND_PROGRESS_H */
