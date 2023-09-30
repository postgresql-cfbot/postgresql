/*
 * Implement a pool of workers that can be used to perform work in parallel.
 * This version is derived from the postgres test_shm_mq module, but it is
 * intended to be more general purpose. Besides being useful for running
 * tests, it could eventually replace the current implementation of parallel query.
 * For now, it is just a piece of infrastructure for running parallel tests.
 *
 * The worker pool is created by the owner process. It creates a shared memory
 * segment and a set of background workers. Each background worker is provided
 * with three message queues:
 *    - inQ:  used to send messages to the worker
 *    - outQ: used to reply to the owner
 *    - errQ: used to send error exceptions back to the owner.
 * Note: errQ is not yet implemented.
 *
 * The flow of control for the owner is:
 *    pool = createWorkerPool(nWorkers, inSize, outSize, libName, procName)
 *    repeat
 *       sendToWorker(pool, workerIdx, msg, len)
 *       recvFromWorker(pool, workerIdx, &msg, &len)
 *    until done
 *    freeWorkerPool(pool)
 *
 * The flow of control for the worker is:
 *    workerInit(arg)
 *    repeat
 *        workerRecv(&msg, &len)
 *        workerSend(msg, len)
 *    until done
 *    workerExit()
 *
 * If the workers don't exit on their own, they will be terminated when
 * the owner calls freeWorkerPool.
 *
 * Currently, the worker entry point is passed as the name of a procedure
 * and the name of a shared library.
 * Future updates may allow the worker to be specified as function pointer
 * in the main postgres executable.
 */

#ifndef WORKER_POOL_H
#define WORKER_POOL_H
#include <unistd.h>
#include "postgres.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/shm_mq.h"
#include "storage/s_lock.h"

typedef struct WorkerPool WorkerPool;
typedef shm_mq_handle *MemQue;

/*
 * Procedures called from the worker pool owner
 */
WorkerPool *createWorkerPool(int nWorkers, int inSize, int outSize, char *libName, char *procName);
shm_mq_result sendToWorker(WorkerPool *pool, int workerIdx, void *msg, Size len);
shm_mq_result recvFromWorker(WorkerPool *pool, int workerIdx, void **msg, Size *len);
void freeWorkerPool(WorkerPool *pool);

/*
 * Procedures called from the worker
 */
void workerInit(Datum arg);
shm_mq_result  workerRecv(void **msg, Size *msgSize);
shm_mq_result  workerSend(void *msg, Size msgSize);
void workerExit(int code);

#endif //WORKER_POOL_H
