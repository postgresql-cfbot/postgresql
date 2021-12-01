/*-------------------------------------------------------------------------
 *
 * walwriter.h
 *	  Exports from postmaster/walwriter.c.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/include/postmaster/walwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALWRITER_H
#define _WALWRITER_H

/* GUC options */
extern int	WalWriterDelay pg_attribute_hidden();
extern int	WalWriterFlushAfter pg_attribute_hidden();

extern void WalWriterMain(void) pg_attribute_noreturn();

#endif							/* _WALWRITER_H */
