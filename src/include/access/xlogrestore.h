#ifndef XLOGRESTORE_H
#define XLOGRESTORE_H

#include "postgres.h"

extern int	max_restore_command_workers;

extern Size RestoreCommandShmemSize(void);
extern void RestoreCommandShmemInit(void);
extern bool RestoreCommandXLog(char *path, const char *xlogfname,
							   const char *recovername,
							   const off_t expectedSize,
							   bool cleanupEnabled);
extern void RestoreCommandWorkerMain(Datum main_arg) pg_attribute_noreturn();

#endif							/* XLOGRESTORE_H */
