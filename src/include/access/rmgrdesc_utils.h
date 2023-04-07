/*-------------------------------------------------------------------------
 *
 * rmgrdesc_utils.h
 *	  helper utilities for rmgrdesc
 *
 * Copyright (c) 2023 PostgreSQL Global Development Group
 *
 * src/include/access/rmgrdesc_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RMGRDESC_UTILS_H_
#define RMGRDESC_UTILS_H_

#include "storage/off.h"
#include "access/heapam_xlog.h"

extern void array_desc(StringInfo buf, void *array, size_t elem_size, int count,
					   void (*elem_desc) (StringInfo buf, void *elem, void *data),
					   void *data);
extern void offset_elem_desc(StringInfo buf, void *offset, void *data);
extern void redirect_elem_desc(StringInfo buf, void *offset, void *data);
extern void relid_desc(StringInfo buf, void *relid, void *data);

#endif							/* RMGRDESC_UTILS_H */
