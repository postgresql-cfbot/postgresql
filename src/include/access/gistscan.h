/*-------------------------------------------------------------------------
 *
 * gistscan.h
 *	  routines defined in access/gist/gistscan.c
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/gistscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GISTSCAN_H
#define GISTSCAN_H

#include "access/amapi.h"

extern IndexScanDesc gistbeginscan(Relation r, int nkeys, int norderbys);
extern void gistrescan(IndexScanDesc scan, const ScanKeyData *key, int nkeys,
					   const ScanKeyData *orderbys, int norderbys);
extern void gistendscan(IndexScanDesc scan);

#endif							/* GISTSCAN_H */
