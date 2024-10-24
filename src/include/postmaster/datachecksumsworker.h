/*-------------------------------------------------------------------------
 *
 * datachecksumsworker.h
 *	  header file for checksum helper background worker
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/datachecksumsworker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATACHECKSUMSWORKER_H
#define DATACHECKSUMSWORKER_H

/* Shared memory */
extern Size DataChecksumsWorkerShmemSize(void);
extern void DataChecksumsWorkerShmemInit(void);

/* Start the background processes for enabling or disabling checksums */
void		StartDataChecksumsWorkerLauncher(bool enable_checksums,
											 int cost_delay,
											 int cost_limit,
											 bool fast);

/* Background worker entrypoints */
void		DataChecksumsWorkerLauncherMain(Datum arg);
void		DataChecksumsWorkerMain(Datum arg);

#endif							/* DATACHECKSUMSWORKER_H */
