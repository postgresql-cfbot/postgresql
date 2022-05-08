/*-------------------------------------------------------------------------
 *
 * dirmod.c
 *	  directory handling functions
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	This includes replacement versions of functions that work on
 *	Win32 (NT4 and newer).
 *
 * IDENTIFICATION
 *	  src/port/dirmod.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

/* Don't modify declarations in system headers */
#if defined(WIN32) || defined(__CYGWIN__)
#undef rename
#undef unlink
#endif

#include <unistd.h>
#include <sys/stat.h>

#if defined(WIN32) || defined(__CYGWIN__)
#ifndef __CYGWIN__
#include <winioctl.h>
#else
#include <windows.h>
#include <w32api/winioctl.h>
#endif
#endif


#if defined(WIN32) && !defined(__CYGWIN__) && defined(_WIN32_WINNT_WIN10) && _WIN32_WINNT >= _WIN32_WINNT_WIN10

#include <winternl.h>

/*
 * Checks Windows version using RtlGetVersion
 * Version 1809 (Build 17763) is required for SetFileInformationByHandle
 * function with FILE_RENAME_FLAG_POSIX_SEMANTICS flag
*/
typedef NTSYSAPI(NTAPI* PFN_RTLGETVERSION)
(OUT PRTL_OSVERSIONINFOEXW lpVersionInformation);

static int	windowsPosixSemanticsUsable = -1;

static bool
is_windows_posix_semantics_usable()
{
	HMODULE		ntdll;

	PFN_RTLGETVERSION _RtlGetVersion = NULL;
	OSVERSIONINFOEXW info;

	if (windowsPosixSemanticsUsable >= 0)
		return (windowsPosixSemanticsUsable > 0);

	ntdll = LoadLibraryEx("ntdll.dll", NULL, 0);
	if (ntdll == NULL)
	{
		DWORD		err = GetLastError();

		_dosmaperr(err);
		return false;
	}

	_RtlGetVersion = (PFN_RTLGETVERSION)(pg_funcptr_t)
		GetProcAddress(ntdll, "RtlGetVersion");
	if (_RtlGetVersion == NULL)
	{
		DWORD		err = GetLastError();

		FreeLibrary(ntdll);
		_dosmaperr(err);
		return false;
	}
	info.dwOSVersionInfoSize = sizeof(OSVERSIONINFOEXW);
	if (!NT_SUCCESS(_RtlGetVersion(&info)))
	{
		DWORD		err = GetLastError();

		FreeLibrary(ntdll);
		_dosmaperr(err);
		return false;
	}

	if (info.dwMajorVersion >= 10 && info.dwBuildNumber >= 17763)
		windowsPosixSemanticsUsable = 1;
	else
		windowsPosixSemanticsUsable = 0;
	FreeLibrary(ntdll);

	return (windowsPosixSemanticsUsable > 0);
}

typedef struct _FILE_RENAME_INFO_EXT {
	FILE_RENAME_INFO fri;
	WCHAR extra_space[MAX_PATH];
} FILE_RENAME_INFO_EXT;

/*
 * pgrename_windows_posix_semantics  - uses SetFileInformationByHandle function
 * with FILE_RENAME_FLAG_POSIX_SEMANTICS flag for atomic rename file
 * working only on Windows 10 (1809) or later and  _WIN32_WINNT must be >= _WIN32_WINNT_WIN10
 */
static int
pgrename_windows_posix_semantics(const char *from, const char *to)
{
	int err;
	FILE_RENAME_INFO_EXT rename_info;
	PFILE_RENAME_INFO prename_info;
	HANDLE f_handle;
	wchar_t from_w[MAX_PATH];

	prename_info = (PFILE_RENAME_INFO)&rename_info;

	if (MultiByteToWideChar(CP_ACP, 0, (LPCCH)from, -1, (LPWSTR)from_w, MAX_PATH) == 0) {
		err = GetLastError();
		_dosmaperr(err);
		return -1;
	}
	if (MultiByteToWideChar(CP_ACP, 0, (LPCCH)to, -1, (LPWSTR)prename_info->FileName, MAX_PATH) == 0) {
		err = GetLastError();
		_dosmaperr(err);
		return -1;
	}
	/*
	 * To open a directory using CreateFile, specify the FILE_FLAG_BACKUP_SEMANTICS.
	 * We use the FILE_FLAG_OPEN_REPARSE_POINT flag
	 * If FILE_FLAG_OPEN_REPARSE_POINT is not specified: If an existing file is opened and it is
	 * a symbolic link, the handle returned is a handle to the target.
	 */
	f_handle = CreateFileW(from_w,
		GENERIC_READ | GENERIC_WRITE | DELETE,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
		NULL);


	if (f_handle == INVALID_HANDLE_VALUE)
	{
		err = GetLastError();

		_dosmaperr(err);

		return -1;
	}

	
	prename_info->ReplaceIfExists = TRUE;
	prename_info->Flags = FILE_RENAME_FLAG_POSIX_SEMANTICS | FILE_RENAME_FLAG_REPLACE_IF_EXISTS;

	prename_info->RootDirectory = NULL;
	prename_info->FileNameLength = wcslen(prename_info->FileName);
	
	if (!SetFileInformationByHandle(f_handle, FileRenameInfoEx, prename_info, sizeof(FILE_RENAME_INFO_EXT)))
	{
		err = GetLastError();

		_dosmaperr(err);
		CloseHandle(f_handle);
		return -1;
	}

	CloseHandle(f_handle);
	return 0;

}

/*
 *	pgunlink_windows_posix_semantics function
 */
#define FILE_DISPOSITION_DELETE 0x00000001
#define FILE_DISPOSITION_POSIX_SEMANTICS 0x00000002

typedef struct _FILE_DISPOSITION_INFORMATION_EX {
	ULONG Flags;
} FILE_DISPOSITION_INFORMATION_EX, *PFILE_DISPOSITION_INFORMATION_EX;

/*
 * pgunlink_windows_posix_semantics  - uses SetFileInformationByHandle function
 * with FILE_DISPOSITION_POSIX_SEMANTICS flag for delete file
 * working only on Windows 10 (1809) or later and  _WIN32_WINNT must be >= _WIN32_WINNT_WIN10
 */
int
pgunlink_windows_posix_semantics(const char *path)
{
	int err;
	HANDLE hFile;
	FILE_DISPOSITION_INFO_EX fdi;

	hFile = CreateFile(path, DELETE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, 0, OPEN_EXISTING,
		FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);


	if (hFile == INVALID_HANDLE_VALUE)
	{
		err = GetLastError();
		_dosmaperr(err);
		return -1;
	}

	fdi.Flags = FILE_DISPOSITION_DELETE | FILE_DISPOSITION_POSIX_SEMANTICS;

	if (!SetFileInformationByHandle(hFile, FileDispositionInfoEx, &fdi, sizeof(fdi)))
	{
		err = GetLastError();

		_dosmaperr(err);
		CloseHandle(hFile);
		return -1;
	}

	CloseHandle(hFile);

	return 0;

}


#endif 							/* #if defined(WIN32) && !defined(__CYGWIN__) && defined(_WIN32_WINNT_WIN10) && _WIN32_WINNT >= _WIN32_WINNT_WIN10 */


#if defined(WIN32) || defined(__CYGWIN__)

/*
 *	pgrename
 */
int
pgrename(const char *from, const char *to)
{
	int			loops = 0;

	/*
	 * Calls pgrename_windows_posix_semantics on Windows 10 and later when
	 * _WIN32_WINNT >= _WIN32_WINNT_WIN10.
	 */
#if defined(_WIN32_WINNT_WIN10) \
			&& _WIN32_WINNT >= _WIN32_WINNT_WIN10 \
			&& !defined(__CYGWIN__)
	if (is_windows_posix_semantics_usable())
	{
		if (pgrename_windows_posix_semantics(from, to) == 0)
			return 0;
	}
#endif


	/*
	 * We need to loop because even though PostgreSQL uses flags that allow
	 * rename while the file is open, other applications might have the file
	 * open without those flags.  However, we won't wait indefinitely for
	 * someone else to close the file, as the caller might be holding locks
	 * and blocking other backends.
	 */
#if defined(WIN32) && !defined(__CYGWIN__)
	while (!MoveFileEx(from, to, MOVEFILE_REPLACE_EXISTING))
#else
	while (rename(from, to) < 0)
#endif
	{
#if defined(WIN32) && !defined(__CYGWIN__)
		DWORD		err = GetLastError();

		_dosmaperr(err);

		/*
		 * Modern NT-based Windows versions return ERROR_SHARING_VIOLATION if
		 * another process has the file open without FILE_SHARE_DELETE.
		 * ERROR_LOCK_VIOLATION has also been seen with some anti-virus
		 * software. This used to check for just ERROR_ACCESS_DENIED, so
		 * presumably you can get that too with some OS versions. We don't
		 * expect real permission errors where we currently use rename().
		 */
		if (err != ERROR_ACCESS_DENIED &&
			err != ERROR_SHARING_VIOLATION &&
			err != ERROR_LOCK_VIOLATION)
			return -1;
#else
		if (errno != EACCES)
			return -1;
#endif

		if (++loops > 100)		/* time out after 10 sec */
			return -1;
		pg_usleep(100000);		/* us */
	}
	return 0;
}


/*
 *	pgunlink
 */
int
pgunlink(const char *path)
{
	int			loops = 0;
	/*
	* Calls pgunlink_windows_posix_semantics on Windows 10 and later when _WIN32_WINNT >= _WIN32_WINNT_WIN10.
	*/
#if defined(_WIN32_WINNT_WIN10) && _WIN32_WINNT >= _WIN32_WINNT_WIN10 && !defined(__CYGWIN__)
	if (is_windows_posix_semantics_usable())
	{
		if (pgunlink_windows_posix_semantics(path) == 0)
			return 0;
	}
#endif

	/*
	 * We need to loop because even though PostgreSQL uses flags that allow
	 * unlink while the file is open, other applications might have the file
	 * open without those flags.  However, we won't wait indefinitely for
	 * someone else to close the file, as the caller might be holding locks
	 * and blocking other backends.
	 */
	while (unlink(path))
	{
		if (errno != EACCES)
			return -1;
		if (++loops > 100)		/* time out after 10 sec */
			return -1;
		pg_usleep(100000);		/* us */
	}
	return 0;
}

/* We undefined these above; now redefine for possible use below */
#define rename(from, to)		pgrename(from, to)
#define unlink(path)			pgunlink(path)
#endif							/* defined(WIN32) || defined(__CYGWIN__) */


#if defined(WIN32) && !defined(__CYGWIN__)	/* Cygwin has its own symlinks */

/*
 *	pgsymlink support:
 *
 *	This struct is a replacement for REPARSE_DATA_BUFFER which is defined in VC6 winnt.h
 *	but omitted in later SDK functions.
 *	We only need the SymbolicLinkReparseBuffer part of the original struct's union.
 */
typedef struct
{
	DWORD		ReparseTag;
	WORD		ReparseDataLength;
	WORD		Reserved;
	/* SymbolicLinkReparseBuffer */
	WORD		SubstituteNameOffset;
	WORD		SubstituteNameLength;
	WORD		PrintNameOffset;
	WORD		PrintNameLength;
	WCHAR		PathBuffer[FLEXIBLE_ARRAY_MEMBER];
} REPARSE_JUNCTION_DATA_BUFFER;

#define REPARSE_JUNCTION_DATA_BUFFER_HEADER_SIZE   \
		FIELD_OFFSET(REPARSE_JUNCTION_DATA_BUFFER, SubstituteNameOffset)


/*
 *	pgsymlink - uses Win32 junction points
 *
 *	For reference:	http://www.codeproject.com/KB/winsdk/junctionpoints.aspx
 */
int
pgsymlink(const char *oldpath, const char *newpath)
{
	HANDLE		dirhandle;
	DWORD		len;
	char		buffer[MAX_PATH * sizeof(WCHAR) + offsetof(REPARSE_JUNCTION_DATA_BUFFER, PathBuffer)];
	char		nativeTarget[MAX_PATH];
	char	   *p = nativeTarget;
	REPARSE_JUNCTION_DATA_BUFFER *reparseBuf = (REPARSE_JUNCTION_DATA_BUFFER *) buffer;

	CreateDirectory(newpath, 0);
	dirhandle = CreateFile(newpath, GENERIC_READ | GENERIC_WRITE,
						   0, 0, OPEN_EXISTING,
						   FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, 0);

	if (dirhandle == INVALID_HANDLE_VALUE)
		return -1;

	/* make sure we have an unparsed native win32 path */
	if (memcmp("\\??\\", oldpath, 4) != 0)
		snprintf(nativeTarget, sizeof(nativeTarget), "\\??\\%s", oldpath);
	else
		strlcpy(nativeTarget, oldpath, sizeof(nativeTarget));

	while ((p = strchr(p, '/')) != NULL)
		*p++ = '\\';

	len = strlen(nativeTarget) * sizeof(WCHAR);
	reparseBuf->ReparseTag = IO_REPARSE_TAG_MOUNT_POINT;
	reparseBuf->ReparseDataLength = len + 12;
	reparseBuf->Reserved = 0;
	reparseBuf->SubstituteNameOffset = 0;
	reparseBuf->SubstituteNameLength = len;
	reparseBuf->PrintNameOffset = len + sizeof(WCHAR);
	reparseBuf->PrintNameLength = 0;
	MultiByteToWideChar(CP_ACP, 0, nativeTarget, -1,
						reparseBuf->PathBuffer, MAX_PATH);

	/*
	 * FSCTL_SET_REPARSE_POINT is coded differently depending on SDK version;
	 * we use our own definition
	 */
	if (!DeviceIoControl(dirhandle,
						 CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 41, METHOD_BUFFERED, FILE_ANY_ACCESS),
						 reparseBuf,
						 reparseBuf->ReparseDataLength + REPARSE_JUNCTION_DATA_BUFFER_HEADER_SIZE,
						 0, 0, &len, 0))
	{
		LPSTR		msg;

		errno = 0;
		FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
					  FORMAT_MESSAGE_IGNORE_INSERTS |
					  FORMAT_MESSAGE_FROM_SYSTEM,
					  NULL, GetLastError(),
					  MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
					  (LPSTR) &msg, 0, NULL);
#ifndef FRONTEND
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not set junction for \"%s\": %s",
						nativeTarget, msg)));
#else
		fprintf(stderr, _("could not set junction for \"%s\": %s\n"),
				nativeTarget, msg);
#endif
		LocalFree(msg);

		CloseHandle(dirhandle);
		RemoveDirectory(newpath);
		return -1;
	}

	CloseHandle(dirhandle);

	return 0;
}

/*
 *	pgreadlink - uses Win32 junction points
 */
int
pgreadlink(const char *path, char *buf, size_t size)
{
	DWORD		attr;
	HANDLE		h;
	char		buffer[MAX_PATH * sizeof(WCHAR) + offsetof(REPARSE_JUNCTION_DATA_BUFFER, PathBuffer)];
	REPARSE_JUNCTION_DATA_BUFFER *reparseBuf = (REPARSE_JUNCTION_DATA_BUFFER *) buffer;
	DWORD		len;
	int			r;

	attr = GetFileAttributes(path);
	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		_dosmaperr(GetLastError());
		return -1;
	}
	if ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == 0)
	{
		errno = EINVAL;
		return -1;
	}

	h = CreateFile(path,
				   GENERIC_READ,
				   FILE_SHARE_READ | FILE_SHARE_WRITE,
				   NULL,
				   OPEN_EXISTING,
				   FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
				   0);
	if (h == INVALID_HANDLE_VALUE)
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	if (!DeviceIoControl(h,
						 FSCTL_GET_REPARSE_POINT,
						 NULL,
						 0,
						 (LPVOID) reparseBuf,
						 sizeof(buffer),
						 &len,
						 NULL))
	{
		LPSTR		msg;

		errno = 0;
		FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
					  FORMAT_MESSAGE_IGNORE_INSERTS |
					  FORMAT_MESSAGE_FROM_SYSTEM,
					  NULL, GetLastError(),
					  MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
					  (LPSTR) &msg, 0, NULL);
#ifndef FRONTEND
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not get junction for \"%s\": %s",
						path, msg)));
#else
		fprintf(stderr, _("could not get junction for \"%s\": %s\n"),
				path, msg);
#endif
		LocalFree(msg);
		CloseHandle(h);
		errno = EINVAL;
		return -1;
	}
	CloseHandle(h);

	/* Got it, let's get some results from this */
	if (reparseBuf->ReparseTag != IO_REPARSE_TAG_MOUNT_POINT)
	{
		errno = EINVAL;
		return -1;
	}

	r = WideCharToMultiByte(CP_ACP, 0,
							reparseBuf->PathBuffer, -1,
							buf,
							size,
							NULL, NULL);

	if (r <= 0)
	{
		errno = EINVAL;
		return -1;
	}

	/*
	 * If the path starts with "\??\", which it will do in most (all?) cases,
	 * strip those out.
	 */
	if (r > 4 && strncmp(buf, "\\??\\", 4) == 0)
	{
		memmove(buf, buf + 4, strlen(buf + 4) + 1);
		r -= 4;
	}
	return r;
}

/*
 * Assumes the file exists, so will return false if it doesn't
 * (since a nonexistent file is not a junction)
 */
bool
pgwin32_is_junction(const char *path)
{
	DWORD		attr = GetFileAttributes(path);

	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		_dosmaperr(GetLastError());
		return false;
	}
	return ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == FILE_ATTRIBUTE_REPARSE_POINT);
}
#endif							/* defined(WIN32) && !defined(__CYGWIN__) */
