/*-------------------------------------------------------------------------
 * logicalfuncs.h
 *	   PostgreSQL WAL to logical transformation support functions
 *
 * Copyright (c) 2012-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALFUNCS_H
#define LOGICALFUNCS_H

#include "replication/logical.h"

extern bool logical_read_local_xlog_page(LogicalDecodingContext *ctx);

#endif
