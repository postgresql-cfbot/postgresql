/*-------------------------------------------------------------------------
 *
 * win32_stat.c
 *	  Replacements for <sys/stat.h> functions using GetFileAttributesEx on
 *    Win32.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/win32_stat.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32

#include "c.h"
#include <tchar.h>
#if defined(__CYGWIN__)
#include <w32api/windows.h>
#else
#include <windows.h>
#endif

static const unsigned __int64 EpochShift = UINT64CONST(116444736000000000);

/*
 * Converts a FILETIME struct into a 64 bit time_t
 */
static __time64_t
filetime_to_time(FILETIME const * ft)
{
	ULARGE_INTEGER unified_ft;

	unified_ft.LowPart  = ft->dwLowDateTime;
	unified_ft.HighPart = ft->dwHighDateTime;

	if (unified_ft.QuadPart < EpochShift)
		return -1;

	unified_ft.QuadPart -= EpochShift;
	unified_ft.QuadPart /= 10 * 1000 * 1000;

	return ((__int64) unified_ft.HighPart) << 32 |
		   (__int64) unified_ft.LowPart;
}

/*
 * Converts WIN32 file attributes to unix mode
 */
static unsigned short
fileattr_to_unixmode(int attr, const _TCHAR * name)
{
	unsigned short uxmode;
	const _TCHAR * p;

	uxmode = (unsigned short)((attr & FILE_ATTRIBUTE_DIRECTORY) ?
			 (_S_IFDIR | _S_IEXEC) : (_S_IFREG));

	uxmode |= (unsigned short)(attr & FILE_ATTRIBUTE_READONLY) ?
			  (_S_IREAD) : (_S_IREAD | _S_IWRITE);

	if (p = _tcsrchr(name, _T('.')))
	{
		if (! _tcsicmp(p, _T(".exe")) ||
			! _tcsicmp(p, _T(".cmd")) ||
			! _tcsicmp(p, _T(".bat")) ||
			! _tcsicmp(p, _T(".com")))
			uxmode |= _S_IEXEC;
	}

	uxmode |= (uxmode & 0700) >> 3;
	uxmode |= (uxmode & 0700) >> 6;

	return uxmode;
}

/*
 * GetFileAttributesEx minimum supported version: Windows XP and
 * Windows Server 2003
 */
int
_pgstat64(char const *name, struct stat *buf)
{
	WIN32_FILE_ATTRIBUTE_DATA faData;

	if (NULL == name || NULL == buf)
	{
		errno = EINVAL;
		return -1;
	}

	if (!GetFileAttributesEx(name, GetFileExInfoStandard, &faData))
	{
		DWORD		error = GetLastError();
		if (error== ERROR_DELETE_PENDING)
		{
			/*
			 * File has been deleted, but is not gone from the filesystem yet.
			 * This can happen when some process with FILE_SHARE_DELETE has it
			 * open and it will be fully removed once that handle is closed.
			 * Meanwhile, we can't open it, so indicate that the file just
			 * doesn't exist.
			 */
			errno = ENOENT;
		}
		_dosmaperr(error);
		return -1;
	}

	if (faData.ftLastWriteTime.dwLowDateTime ||
		faData.ftLastWriteTime.dwHighDateTime)
		buf->st_mtime = filetime_to_time(&faData.ftLastWriteTime);

	if (faData.ftLastAccessTime.dwLowDateTime ||
		faData.ftLastAccessTime.dwHighDateTime)
		buf->st_atime = filetime_to_time(&faData.ftLastAccessTime);
	else
		buf->st_atime = buf->st_mtime;

	if (faData.ftCreationTime.dwLowDateTime ||
		faData.ftCreationTime.dwHighDateTime)
		buf->st_ctime = filetime_to_time(&faData.ftCreationTime);
	else
		buf->st_ctime = buf->st_mtime;

	buf->st_mode  = fileattr_to_unixmode(faData.dwFileAttributes, name);
	buf->st_nlink = 1;

	buf->st_size = ((__int64) faData.nFileSizeHigh) << 32 |
				   (__int64)(faData.nFileSizeLow);

	return 0;
}

/*
 * Wrapper for _pgstat64
 * GetFinalPathNameByHandle minimum supported version: Windows Vista and
 * Windows Server 2008
 */
int
_pgfstat64(int fileno, struct stat *buf)
{
	const char filepath[_MAX_PATH];

	if(GetFinalPathNameByHandle((HANDLE)_get_osfhandle(fileno),
								filepath, _MAX_PATH, VOLUME_NAME_DOS))
		return _pgstat64(filepath, buf);
	return -1;
}

#endif /* WIN32 */
