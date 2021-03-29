/*-------------------------------------------------------------------------
 *
 * fdwxact_launcher.h
 *	  PostgreSQL foreign transaction launcher definitions
 *
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact_launcher.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FDWXACT_LAUNCHER_H
#define FDWXACT_LAUNCHER_H

#include "access/fdwxact.h"

extern void FdwXactLauncherRegister(void);
extern void FdwXactLauncherMain(Datum main_arg);
extern void FdwXactRequestToLaunchResolver(void);
extern void FdwXactLaunchOrWakeupResolver(void);
extern Size FdwXactLauncherShmemSize(void);
extern void FdwXactLauncherShmemInit(void);
extern bool IsFdwXactLauncher(void);


#endif							/* FDWXACT_LAUNCHER_H */
