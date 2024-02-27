/*-------------------------------------------------------------------------
 * injection_point.h
 *	  Definitions related to injection points.
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * src/include/utils/injection_point.h
 *-------------------------------------------------------------------------
 */

#ifndef INJECTION_POINT_H
#define INJECTION_POINT_H

/*
 * Injections points require --enable-injection-points.
 */
#ifdef USE_INJECTION_POINTS
#define INJECTION_POINT(name) InjectionPointRun(name)
#define INJECTION_POINT_PREPARE(name) InjectionPointPrepare(name)
#define INJECTION_POINT_RUN_PREPARED() InjectionPointRunPrepared()
#else
#define INJECTION_POINT(name) ((void) name)
#define INJECTION_POINT_PREPARE(name) ((void) name)
#define INJECTION_POINT_RUN_PREPARED()
#endif

/*
 * Typedef for callback function launched by an injection point.
 */
typedef void (*InjectionPointCallback) (const char *name);

extern Size InjectionPointShmemSize(void);
extern void InjectionPointShmemInit(void);

extern void InjectionPointAttach(const char *name,
								 const char *library,
								 const char *function);
extern void InjectionPointRun(const char *name);
extern void InjectionPointDetach(const char *name);
extern bool InjectionPointIsAttach(const char *name);
extern void InjectionPointPrepare(const char *name);
extern void InjectionPointRunPrepared(void);

#endif							/* INJECTION_POINT_H */
