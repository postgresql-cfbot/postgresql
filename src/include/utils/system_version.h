/*-------------------------------------------------------------------------
 * system_version.h
 *	  Definitions related to system versions reporting
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * src/include/utils/system_version.h
 * ----------
 */

#ifndef SYSTEM_VERSION_H
#define SYSTEM_VERSION_H

#include <gnu/libc-version.h>
#include <link.h>

#define MAX_SYSTEM_VERSIONS 100

typedef enum VersionType
{
	CompileTime,
	RunTime,
} VersionType;

/*
 * Callback to return version string of a system component.
 * The version might be not available, what is indicated via the argument.
 */
typedef const char* (*SystemVersionCB) (bool *available);

typedef struct SystemVersion
{
	char			name[NAMEDATALEN]; 	/* Unique component name, used as a key
										 * for versions HTAB */
	VersionType 	type;
	SystemVersionCB	callback; 			/* Callback to fetch the version string */
} SystemVersion;

void add_system_version(const char* name, SystemVersionCB cb, VersionType type);
extern void register_core_versions(void);

const char* core_get_version(bool *available);
const char* core_get_arch(bool *available);
const char* core_get_compiler(bool *available);

const char* icu_get_version(bool *available);
const char* glibc_get_version(bool *available);

#endif							/* SYSTEM_VERSION_H */
