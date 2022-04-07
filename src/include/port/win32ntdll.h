/*-------------------------------------------------------------------------
 *
 * win32ntdll.h
 *	  Dynamically loaded Windows NT functions.
 *
 * Portions Copyright (c) 2021-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/win32ntdll.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef WIN32NTDLL_H
#define WIN32NTDLL_H

/*
 * Because this includes NT headers that normally conflict with Win32 headers,
 * any translation unit that includes it should #define UMDF_USING_NTSTATUS
 * before including <windows.h>.
 */

#include <ntstatus.h>
#include <winternl.h>

#ifndef FLUSH_FLAGS_FILE_DATA_SYNC_ONLY
#define FLUSH_FLAGS_FILE_DATA_SYNC_ONLY 0x4
#endif

typedef NTSTATUS (__stdcall *RtlGetLastNtStatus_t) (void);
typedef ULONG (__stdcall *RtlNtStatusToDosError_t) (NTSTATUS);
typedef NTSTATUS (__stdcall *NtFlushBuffersFileEx_t) (HANDLE, ULONG, PVOID, ULONG, PIO_STATUS_BLOCK);

extern RtlGetLastNtStatus_t pg_RtlGetLastNtStatus;
extern RtlNtStatusToDosError_t pg_RtlNtStatusToDosError;
extern NtFlushBuffersFileEx_t pg_NtFlushBuffersFileEx;

extern int	initialize_ntdll(void);

#endif							/* WIN32NTDLL_H */
