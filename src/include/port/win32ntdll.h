/*-------------------------------------------------------------------------
 *
 * win32ntdll.h
 *	  Dynamically loaded Windows NT functions.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/win32ntdll.h
 *
 *-------------------------------------------------------------------------
 */

typedef NTSTATUS (__stdcall *RtlGetLastNtStatus_t)(void);

extern RtlGetLastNtStatus_t pg_RtlGetLastNtStatus;

extern int initialize_ntdll(void);
