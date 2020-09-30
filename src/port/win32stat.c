/*-------------------------------------------------------------------------
 *
 * win32stat.c
 *	  Replacements for <sys/stat.h> functions using GetFileInformationByHandle
 *    on Win32.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/win32stat.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32

#include "c.h"
#include <windows.h>

/*
 * In order to support MinGW and MSVC2013 we use NtQueryInformationFile as an
 * alternative for GetFileInformationByHandleEx. It is loaded from the ntdll
 * library.
 */
#if _WIN32_WINNT < 0x0600
#include <winternl.h>
#if !defined(__MINGW32__) && !defined(__MINGW64__)
/* MinGW includes this in <winternl.h>, but is missing in MSVC */
typedef struct _FILE_STANDARD_INFORMATION
{
	LARGE_INTEGER AllocationSize;
	LARGE_INTEGER EndOfFile;
	ULONG         NumberOfLinks;
	BOOLEAN       DeletePending;
	BOOLEAN       Directory;
} FILE_STANDARD_INFORMATION, *PFILE_STANDARD_INFORMATION;
#define FileStandardInformation 5
#endif /* !defined(__MINGW32__) && !defined(__MINGW64__) */

typedef NTSTATUS (NTAPI *PFN_NTQUERYINFORMATIONFILE)
(
	IN HANDLE FileHandle,
	OUT PIO_STATUS_BLOCK IoStatusBlock,
	OUT PVOID FileInformation,
	IN ULONG Length,
	IN FILE_INFORMATION_CLASS FileInformationClass
);

static PFN_NTQUERYINFORMATIONFILE _NtQueryInformationFile = NULL;

/*
 * Load DLL file just once regardless of how many functions we load/call in it.
 */
static HMODULE ntdll = NULL;

static void
LoadNtdll(void)
{
	if (ntdll != NULL)
		return;
	ntdll = LoadLibraryEx("ntdll.dll", NULL, 0);
}
#endif /* _WIN32_WINNT < 0x0600 */

static const unsigned __int64 EpochShift = UINT64CONST(116444736000000000);

/*
 * Converts a FILETIME struct into a 64 bit time_t.
 */
static __time64_t
filetime_to_time(FILETIME const *ft)
{
	ULARGE_INTEGER unified_ft = {0};

	unified_ft.LowPart  = ft->dwLowDateTime;
	unified_ft.HighPart = ft->dwHighDateTime;

	if (unified_ft.QuadPart < EpochShift)
		return -1;

	unified_ft.QuadPart -= EpochShift;
	unified_ft.QuadPart /= 10 * 1000 * 1000;

	return unified_ft.QuadPart;
}

/*
 * Converts WIN32 file attributes to unix mode.
 * Only owner permissions are set.
 */
static unsigned short
fileattr_to_unixmode(int attr)
{
	unsigned short uxmode = 0;

	uxmode |= (unsigned short)((attr & FILE_ATTRIBUTE_DIRECTORY) ?
			 (_S_IFDIR) : (_S_IFREG));

	uxmode |= (unsigned short)(attr & FILE_ATTRIBUTE_READONLY) ?
			  (_S_IREAD) : (_S_IREAD | _S_IWRITE);

	/* there is no need to simulate _S_IEXEC using CMD's PATHEXT extensions */
	uxmode |= _S_IEXEC;

	return uxmode;
}

/*
 * Converts WIN32 file infomation to struct stat.
 * GetFileInformationByHandle minimum supported version: Windows XP and
 * Windows Server 2003.
 */
static int
fileinfo_to_stat(HANDLE hFile, struct stat *buf)
{
	BY_HANDLE_FILE_INFORMATION fiData;

	if (!GetFileInformationByHandle(hFile, &fiData))
	{
		 _dosmaperr(GetLastError());
		return -1;
	}

	memset(buf, 0, sizeof(*buf));
	if (fiData.ftLastWriteTime.dwLowDateTime ||
		fiData.ftLastWriteTime.dwHighDateTime)
		buf->st_mtime = filetime_to_time(&fiData.ftLastWriteTime);

	if (fiData.ftLastAccessTime.dwLowDateTime ||
		fiData.ftLastAccessTime.dwHighDateTime)
		buf->st_atime = filetime_to_time(&fiData.ftLastAccessTime);
	else
		buf->st_atime = buf->st_mtime;

	if (fiData.ftCreationTime.dwLowDateTime ||
		fiData.ftCreationTime.dwHighDateTime)
		buf->st_ctime = filetime_to_time(&fiData.ftCreationTime);
	else
		buf->st_ctime = buf->st_mtime;

	buf->st_mode  = fileattr_to_unixmode(fiData.dwFileAttributes);
	buf->st_nlink = fiData.nNumberOfLinks;

	buf->st_size = ((__int64) fiData.nFileSizeHigh) << 32 |
				   (__int64)(fiData.nFileSizeLow);
	return 0;
}

/*
 * We must use a handle so lstat() returns the information of the target file.
 * As a reliable test for ERROR_DELETE_PENDING we use NtQueryInformationFile
 * from Windows 2000 or GetFileInformationByHandleEx from Server 2008 / Vista.
 */
int
_pgstat64(const char *name, struct stat *buf)
{
	SECURITY_ATTRIBUTES sa;
	HANDLE hFile;
	int		ret;
#if _WIN32_WINNT < 0x0600
	IO_STATUS_BLOCK ioStatus;
	FILE_STANDARD_INFORMATION standardInfo;
#else
	FILE_STANDARD_INFO standardInfo;
#endif

	if (name == NULL || buf == NULL)
	{
		errno = EINVAL;
		return -1;
	}
	/* fast not exists */
	if (GetFileAttributes(name) == INVALID_FILE_ATTRIBUTES)
	{
		errno = ENOENT;
		return -1;
	}

	/* get a file handle as lightweight as we can */
	sa.nLength = sizeof(SECURITY_ATTRIBUTES);
	sa.bInheritHandle = TRUE;
	sa.lpSecurityDescriptor = NULL;
	hFile = CreateFile(name, GENERIC_READ,
					   (FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE),
					   &sa, OPEN_EXISTING,
					   (FILE_FLAG_NO_BUFFERING | FILE_FLAG_BACKUP_SEMANTICS |
					   FILE_FLAG_OVERLAPPED), NULL);
	if (hFile == INVALID_HANDLE_VALUE)
	{
		CloseHandle(hFile);
		errno = ENOENT;
		return -1;
	}

	memset(&standardInfo, 0, sizeof(standardInfo));
#if _WIN32_WINNT < 0x0600
	if (_NtQueryInformationFile == NULL)
	{
		LoadNtdll();
		if(ntdll == NULL)
		{
			_dosmaperr(GetLastError());
			CloseHandle(hFile);
			return -1;
		}

		_NtQueryInformationFile = (PFN_NTQUERYINFORMATIONFILE)
			GetProcAddress(ntdll, "NtQueryInformationFile");
		if (_NtQueryInformationFile == NULL)
		{
			_dosmaperr(GetLastError());
			CloseHandle(hFile);
			return -1;
		}
	}

	if (!NT_SUCCESS(_NtQueryInformationFile(hFile, &ioStatus, &standardInfo,
		sizeof(standardInfo), FileStandardInformation)))
#else
	if (!GetFileInformationByHandleEx(hFile, FileStandardInfo, &standardInfo,
		sizeof(standardInfo)))
#endif /* _WIN32_WINNT < 0x0600 */
	{
		 _dosmaperr(GetLastError());
		CloseHandle(hFile);
		return -1;
	}
	if (standardInfo.DeletePending)
	{
		/*
		 * File has been deleted, but is not gone from the filesystem yet.
		 * This can happen when some process with FILE_SHARE_DELETE has it
		 * open and it will be fully removed once that handle is closed.
		 * Meanwhile, we can't open it, so indicate that the file just
		 * doesn't exist.
		 */
		CloseHandle(hFile);
		errno = ENOENT;
		return -1;
	}

	ret = fileinfo_to_stat(hFile, buf);
	CloseHandle(hFile);
	return ret;
}

/*
 * Since we already have a file handle there is no need to check for
 * ERROR_DELETE_PENDING.
 */
int
_pgfstat64(int fileno, struct stat *buf)
{
	HANDLE hFile = (HANDLE)_get_osfhandle(fileno);

	if (hFile == INVALID_HANDLE_VALUE || buf == NULL)
	{
		errno = EINVAL;
		return -1;
	}

	return fileinfo_to_stat(hFile, buf);
}

#endif /* WIN32 */

