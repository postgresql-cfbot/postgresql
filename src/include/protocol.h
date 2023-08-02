/*-------------------------------------------------------------------------
 *
 * protocol.h
 *	  Exports from postmaster/postmaster.c.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * src/include/protocol.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PROTOCOL_H
#define _PROTOCOL_H

#define BIND_REQUEST                'B'
#define CLOSE_REQUEST               'C'
#define DESCRIBE_REQUEST            'D'
#define EXECUTE_REQUEST             'E'
#define FUNCTION_CALL_REQUEST       'F'
#define FLUSH_DATA_REQUEST          'H'
#define BACKEND_KEY_DATA            'K'
#define PARSE_REQUEST               'P'
#define AUTHENTICATION_REQUEST      'R'
#define SYNC_DATA_REQUEST           'S'
#define SIMPLE_QUERY                'Q'
#define TERMINATE_REQUEST           'X'
#define COPY_FAIL                   'f'
#define COPY_DONE                   'c'
#define COPY_DATA                   'd'
#define COPY_PROGRESS               'p'
#define DESCRIBE_PREPARED           'S'
#define DESCRIBE_PORTAL             'P'

/*
Responses
*/
#define PARSE_COMPLETE_RESPONSE '1'
#define BIND_COMPLETE_RESPONSE  '2'
#define CLOSE_COMPLETE_RESPONSE '3'
#define NOTIFY_RESPONSE         'A'
#define COMMAND_COMPLETE        'C'
#define DATA_ROW_RESPONSE       'D'
#define ERROR_RESPONSE          'E'
#define COPY_IN_RESPONSE        'G'
#define COPY_OUT_RESPONSE       'H'
#define EMPTY_QUERY_RESPONSE    'I'
#define NOTICE_RESPONSE         'N'
#define PARALLEL_PROGRESS_RESPONSE 'P'
#define FUNCTION_CALL_RESPONSE  'V'
#define PARAMETER_STATUS_RESPONSE 'S'
#define ROW_DESCRIPTION_RESPONSE 'T'
#define COPY_BOTH_RESPONSE      'W'
#define READY_FOR_QUERY         'Z'
#define NO_DATA_RESPONSE        'n'
#define PASSWORD_RESPONSE       'p'
#define GSS_RESPONSE            'p'
#define PORTAL_SUSPENDED_RESPONSE 's'
#define PARAMETER_DESCRIPTION_RESPONSE 't'
#define NEGOTIATE_PROTOCOL      'v'
#endif