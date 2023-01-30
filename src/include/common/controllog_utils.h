/*
 * controllog_utils.h
 *		Common code for pg_control_log output
 *
 *	Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/include/common/controllog_utils.h
 */
#ifndef COMMON_CONTROLLOG_UTILS_H
#define COMMON_CONTROLLOG_UTILS_H

#include "catalog/pg_controllog.h"

extern OperationLogBuffer * get_operation_log(const char *DataDir, bool *crc_ok_p);
extern OperationLogBuffer * get_empty_operation_log_buffer(void);
extern void update_operation_log(const char *DataDir, OperationLogBuffer * log_buffer);

extern void put_operation_log_element(const char *DataDir, ol_type_enum ol_type);
extern void put_operation_log_element_version(const char *DataDir, ol_type_enum ol_type,
											  PgNumEdition edition, uint32 version_num);
extern uint16 get_operation_log_count(OperationLogBuffer * log_buffer);
extern OperationLogData * get_operation_log_element(OperationLogBuffer * log_buffer,
													uint16 num);
extern const char *get_operation_log_type_name(ol_type_enum ol_type);

#endif							/* COMMON_CONTROLLOG_UTILS_H */
