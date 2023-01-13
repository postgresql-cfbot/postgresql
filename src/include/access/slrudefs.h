/*-------------------------------------------------------------------------
 *
 * slrudefs.h
 *		macros for accessing contents of "slru" pages 
 *
 *
 * Copyright (c) 2021-2022, PostgreSQL Global Development Group
 *
 * src/include/access/slrudefs.h
 *
 *--------------------------------------------------------------------------
 */

#define SLRU_PAGES_PER_SEGMENT 32

#define MULTIXACT_MEMBER_ENTRY_SIZE 20 

#define MULTIXACT_OFFSET_ENTRY_SIZE 8

