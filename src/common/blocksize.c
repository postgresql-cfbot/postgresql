/*-------------------------------------------------------------------------
 *
 * blocksize.c
 *		This file contains methods to calculate blocksize-related variables
 *
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/blocksize.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "common/blocksize_int.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/nbtree_int.h"
#include "common/controldata_utils.h"
#include "storage/large_object.h"

/* These variables are effectively constants, but are initialized by BlockSizeInit() */

Size ReservedPageSize;

Size ClusterMaxTIDsPerBTreePage = 0;
Size ClusterLargeObjectBlockSize = 0;
Size ClusterMaxHeapTupleSize = 0;
Size ClusterMaxHeapTuplesPerPage = 0;
Size ClusterMaxIndexTuplesPerPage = 0;
Size ClusterToastMaxChunkSize = 0;

/*
 * This routine will calculate and cache the necessary constants. This should
 * be called once very very early in the process (as soon as the native block
 * size is known, so after reading ControlFile, or using BlockSizeInitControl).
 */

static bool initialized = false;

void
BlockSizeInit(Size rawblocksize, Size reservedsize)
{
	Assert(rawblocksize == BLCKSZ);
	Assert(IsValidReservedPageSize(reservedsize));

	if (initialized)
		return;

	ReservedPageSize = reservedsize;
	ClusterMaxTIDsPerBTreePage = CalcMaxTIDsPerBTreePage(PageUsableSpace);
	ClusterLargeObjectBlockSize = LOBLKSIZE;		 /* TODO: calculate? */
	ClusterMaxHeapTupleSize = CalcMaxHeapTupleSize(PageUsableSpace);
	ClusterMaxHeapTuplesPerPage = CalcMaxHeapTuplesPerPage(PageUsableSpace);
	ClusterMaxIndexTuplesPerPage = CalcMaxIndexTuplesPerPage(PageUsableSpace);
	ClusterToastMaxChunkSize = CalcToastMaxChunkSize(PageUsableSpace);

	initialized = true;
}

/*
 * Init the BlockSize using values from the given control file pointer.  If
 * this is nil, then load and use the control file pointed to by the pgdata
 * path and perform said operations.
 */
void
BlockSizeInitControl(ControlFileData *control, const char *pgdata)
{
	bool crc_ok = true;

	Assert(pgdata);

	if (!control)
		control = get_controlfile(pgdata, &crc_ok);

	Assert(crc_ok);

	if (control)
	{
		BlockSizeInit(control->blcksz, control->reserved_page_size);
		return;
	}

	/* panic */
}
