/*-------------------------------------------------------------------------
 *
 * dynamic_shared_buffers.c
 *	  Coordination state and helpers for resizing shared_buffers at runtime.
 *
 * The dynamic shared buffer (DSB) machinery lets shared_buffers grow and
 * shrink while the cluster is running.
 *
 * See pgxn/neon/README.md ("Dynamic shared buffer") for the full design and
 * the resize protocol.
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/dynamic_shared_buffers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/dynamic_shared_buffers.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/spin.h"

DynamicSharedBuffersControl *DSBCtrl = NULL;

/*
 * AcquireNBuffersLock
 *
 * Take AccessNBuffersLock in shared mode and return the current high water
 * mark. Callers iterate up to that bound. The lock keeps the resize
 * coordinator (which acquires the lock exclusively) waiting until we drop it.
 */
int
AcquireNBuffersLock(void)
{
	if (!enable_dynamic_shared_buffers)
		return GetHighNBuffers();

	if (DSBCtrl != NULL)
		LWLockAcquire(&DSBCtrl->AccessNBuffersLock, LW_SHARED);
	return GetHighNBuffers();
}

void
ReleaseNBuffersLock(void)
{
	if (!enable_dynamic_shared_buffers)
		return;
	if (DSBCtrl != NULL)
		LWLockRelease(&DSBCtrl->AccessNBuffersLock);
}

/*
 * Try to claim coordinator status for a buffer-pool resize.
 *
 * Returns true if we are now the coordinator, or false if another backend
 * is performing a resize.
 */
bool
ClaimResizeCoordinator(void)
{
	bool		claimed = false;

	Assert(DSBCtrl != NULL);

	SpinLockAcquire(&DSBCtrl->coordinator_lock);
	if (!DSBCtrl->resize_in_progress)
	{
		DSBCtrl->resize_in_progress = true;
		DSBCtrl->coordinator_pid = MyProcPid;
		claimed = true;
	}
	SpinLockRelease(&DSBCtrl->coordinator_lock);

	return claimed;
}

/*
 * Release the coordinator slot acquired by ClaimResizeCoordinator().
 */
void
ReleaseResizeCoordinator(void)
{
	Assert(DSBCtrl != NULL);

	SpinLockAcquire(&DSBCtrl->coordinator_lock);
	Assert(DSBCtrl->resize_in_progress);
	Assert(DSBCtrl->coordinator_pid == MyProcPid);
	DSBCtrl->resize_in_progress = false;
	DSBCtrl->coordinator_pid = InvalidPid;
	SpinLockRelease(&DSBCtrl->coordinator_lock);
}

/*
 * DSBControlInit
 *
 * Allocate and initialize the DynamicSharedBuffersControl structure in shared
 * memory. Must be called before BufferManagerShmemInit so that DSBCtrl is
 * available when the buffer pool is set up.
 */
void
DSBControlInit(void)
{
	bool		foundDSBCtrl;

	DSBCtrl = (DynamicSharedBuffersControl *)
		ShmemInitStruct("DSB Control", sizeof(DynamicSharedBuffersControl),
						&foundDSBCtrl);

	if (!foundDSBCtrl)
	{
		pg_atomic_init_u32(&DSBCtrl->lowNBuffers, NBuffersGUC);
		pg_atomic_init_u32(&DSBCtrl->highNBuffers, NBuffersGUC);

		SpinLockInit(&DSBCtrl->coordinator_lock);
		DSBCtrl->resize_in_progress = false;
		DSBCtrl->coordinator_pid = InvalidPid;

		LWLockInitialize(&DSBCtrl->AccessNBuffersLock,
						 LWTRANCHE_ACCESS_NBUFFERS);
	}
}
