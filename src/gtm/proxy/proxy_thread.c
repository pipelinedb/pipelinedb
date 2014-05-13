/*-------------------------------------------------------------------------
 *
 * proxy_thread.c
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include <pthread.h>
#include "gtm/gtm_proxy.h"
#include "gtm/memutils.h"
#include "gtm/libpq.h"

static void *GTMProxy_ThreadMainWrapper(void *argp);
static void GTMProxy_ThreadCleanup(void *argp);

GTMProxy_Threads	GTMProxyThreadsData;
GTMProxy_Threads *GTMProxyThreads = &GTMProxyThreadsData;

#define GTM_PROXY_MIN_THREADS 32			/* Provision for minimum threads */
#define GTM_PROXY_MAX_THREADS 1024		/* Max threads allowed in the GTMProxy */
#define GTMProxyThreadsFull	(GTMProxyThreads->gt_thread_count == GTMProxyThreads->gt_array_size)

extern int GTMProxyWorkerThreads;
extern GTMProxy_ThreadInfo **Proxy_ThreadInfo;

/*
 * Add the given thrinfo structure to the global array, expanding it if
 * necessary
 */
int
GTMProxy_ThreadAdd(GTMProxy_ThreadInfo *thrinfo)
{
	int ii;

	GTM_RWLockAcquire(&GTMProxyThreads->gt_lock, GTM_LOCKMODE_WRITE);

	if (GTMProxyThreadsFull)
	{
		GTMProxy_ThreadInfo **threads;
		uint32 newsize;

		/*
		 * TODO Optimize lock management by not holding any locks during memory
		 * allocation
		 */
		if (GTMProxyThreads->gt_array_size == GTM_PROXY_MAX_THREADS)
			elog(ERROR, "Too many threads active");

		if (GTMProxyThreads->gt_array_size == 0)
			newsize = GTM_PROXY_MIN_THREADS;
		else
		{
			/*
			 * We ran out of the array size. Just double the size, bound by the
			 * upper limit
			 */
			newsize = GTMProxyThreads->gt_array_size * 2;
		}

		/* Can't have more than GTM_PROXY_MAX_THREADS */
		if (newsize > GTM_PROXY_MAX_THREADS)
			newsize = GTM_PROXY_MAX_THREADS;

		if (GTMProxyThreads->gt_threads == NULL)
			threads = (GTMProxy_ThreadInfo **)palloc0(sizeof (GTMProxy_ThreadInfo *) * newsize);
		else
		{
			void *old_ptr = GTMProxyThreads->gt_threads;
			threads = (GTMProxy_ThreadInfo **)palloc0(sizeof (GTMProxy_ThreadInfo *) * newsize);
			memcpy(threads, old_ptr,
					GTMProxyThreads->gt_array_size * sizeof (GTMProxy_ThreadInfo *));
			pfree(old_ptr);
		}

		GTMProxyThreads->gt_threads = threads;
		GTMProxyThreads->gt_array_size = newsize;
	}

	/*
	 * Now that we have free entries in the array, find a free slot and add the
	 * thrinfo pointer to it.
	 *
	 * TODO Optimize this later by tracking few free slots and reusing them.
	 * The free slots can be updated when a thread exits and reused when a new
	 * thread is added to the pool.
	 */
	for (ii = 0; ii < GTMProxyThreads->gt_array_size; ii++)
	{
		if (GTMProxyThreads->gt_threads[ii] == NULL)
		{
			GTMProxyThreads->gt_threads[ii] = thrinfo;
			GTMProxyThreads->gt_thread_count++;
			break;
		}
	}
	GTM_RWLockRelease(&GTMProxyThreads->gt_lock);

	/*
	 * Track the slot information in the thrinfo. This is useful to quickly
	 * find the slot given the thrinfo structure.
	 */
	thrinfo->thr_localid = ii;
	return ii;
}

int
GTMProxy_ThreadRemove(GTMProxy_ThreadInfo *thrinfo)
{
	/*
	 * XXX To be implemeneted
	 */
	return 0;
}

/*
 * Create a new thread and assign the given connection to it.
 *
 * This function is responsible for setting up the various memory contextes for
 * the thread as well as registering this thread with the Thread Manager.
 *
 * Upon successful creation, the thread will start running the given
 * "startroutine". The thread information is returned to the calling process.
 */
GTMProxy_ThreadInfo *
GTMProxy_ThreadCreate(void *(* startroutine)(void *), int idx)
{
	GTMProxy_ThreadInfo *thrinfo;
	int err;

	/*
	 * We are still running in the context of the main thread. So the
	 * allocation below would last as long as the main thread exists or the
	 * memory is explicitely freed.
	 */
	thrinfo = (GTMProxy_ThreadInfo *)palloc0(sizeof (GTMProxy_ThreadInfo));

	GTM_MutexLockInit(&thrinfo->thr_lock);
	GTM_CVInit(&thrinfo->thr_cv);

	/* Initialize mapping to be unassigned. */
	memset(thrinfo->thr_conid2idx, 0xff, sizeof(thrinfo->thr_conid2idx));

	/*
	 * Initialize communication area with SIGUSR2 signal handler (reconnect)
	 */
	Proxy_ThreadInfo[idx] = thrinfo;

	/*
	 * The thread status is set to GTM_PROXY_THREAD_STARTING and will be changed by
	 * the thread itself when it actually starts executing
	 */
	thrinfo->thr_status = GTM_PROXY_THREAD_STARTING;

	/*
	 * Install the ThreadInfo structure in the global array. We do this before
	 * starting the thread
	 */
	if (GTMProxy_ThreadAdd(thrinfo) == -1)
		elog(ERROR, "Error starting a new thread");

	/*
	 * Set up memory contextes before actually starting the threads
	 *
	 * The TopThreadContext is a child of TopMemoryContext and it will last as
	 * long as the main process or this thread lives
	 *
	 * Thread context is not shared between other threads
	 */
	thrinfo->thr_thread_context = AllocSetContextCreate(TopMemoryContext,
														"TopMemoryContext",
														ALLOCSET_DEFAULT_MINSIZE,
														ALLOCSET_DEFAULT_INITSIZE,
														ALLOCSET_DEFAULT_MAXSIZE,
														false);

	/*
	 * Since the thread is not yes started, TopMemoryContext still points to
	 * the context of the calling thread
	 */
	thrinfo->thr_parent_context = TopMemoryContext;

	/*
	 * Each thread gets its own ErrorContext and its a child of ErrorContext of
	 * the main process
	 *
	 * This is a thread-specific context and is not shared between other
	 * threads
	 */
	thrinfo->thr_error_context = AllocSetContextCreate(ErrorContext,
													   "ErrorContext",
													   8 * 1024,
													   8 * 1024,
													   8 * 1024,
													   false);

	thrinfo->thr_startroutine = startroutine;

	/*
	 * Now start the thread. The thread will start executing the given
	 * "startroutine". The thrinfo structure is also passed to the thread. Any
	 * additional parameters should be passed via the thrinfo strcuture.
	 *
	 * Return the thrinfo structure to the caller
	 */
	if ((err = pthread_create(&thrinfo->thr_id, NULL, GTMProxy_ThreadMainWrapper,
							 thrinfo)))
		ereport(ERROR,
				(err,
				 errmsg("Failed to create a new thread: error %d", err)));

	return thrinfo;
}

/*
 * Exit the current thread
 */
void
GTMProxy_ThreadExit(void)
{
	/* XXX To be implemented */
}

int
GTMProxy_ThreadJoin(GTMProxy_ThreadInfo *thrinfo)
{
	int error;
	void *data;

	error = pthread_join(thrinfo->thr_id, &data);

	return error;
}

/*
 * Get thread information for the given thread, identified by the
 * thread_id
 */
GTMProxy_ThreadInfo *
GTMProxy_GetThreadInfo(GTM_ThreadID thrid)
{

	return NULL;
}

/*
 * Cleanup routine for the thread
 */
static void
GTMProxy_ThreadCleanup(void *argp)
{
	GTMProxy_ThreadInfo *thrinfo = (GTMProxy_ThreadInfo *)argp;

	elog(LOG, "Cleaning up thread state");

	/*
	 * TODO Close the open connection.
	 */
	StreamClose(thrinfo->thr_conn->con_port->sock);

	/*
	 * Switch to the memory context of the main process so that we can free up
	 * our memory contextes easily.
	 *
	 * XXX We don't setup cleanup handlers for the main process. So this
	 * routine would never be called for the main process/thread
	 */
	MemoryContextSwitchTo(thrinfo->thr_parent_context);

	MemoryContextDelete(thrinfo->thr_message_context);
	thrinfo->thr_message_context = NULL;

	MemoryContextDelete(thrinfo->thr_error_context);
	thrinfo->thr_error_context = NULL;

	MemoryContextDelete(thrinfo->thr_thread_context);
	thrinfo->thr_thread_context = NULL;

	/*
	 * TODO Now cleanup the thrinfo structure itself and remove it from the global
	 * array.
	 */


	/*
	 * Reset the thread-specific information. This should be done only after we
	 * are sure that memory contextes are not required
	 *
	 * Note: elog calls need memory contextes, so no elog calls beyond this
	 * point.
	 */
	SetMyThreadInfo(NULL);

	return;
}

/*
 * A wrapper around the start routine of the thread. This helps us doing any
 * initialization and setting up cleanup handlers before the main routine is
 * started
 */
void *
GTMProxy_ThreadMainWrapper(void *argp)
{
	GTMProxy_ThreadInfo *thrinfo = (GTMProxy_ThreadInfo *)argp;

	pthread_detach(thrinfo->thr_id);

	SetMyThreadInfo(thrinfo);
	MemoryContextSwitchTo(TopMemoryContext);

	pthread_cleanup_push(GTMProxy_ThreadCleanup, thrinfo);
	thrinfo->thr_startroutine(thrinfo);
	pthread_cleanup_pop(1);

	return thrinfo;
}

/*
 * Add the given connection info structure to a thread which is selected by a
 * round-robin manner. The caller is responsible for only accepting the
 * connection. Other things including the authentication is done by the worker
 * thread when it finds a new entry in the connection list.
 *
 * Return the reference to the GTMProxy_ThreadInfo structure of the thread
 * which will be serving this connection
 */
GTMProxy_ThreadInfo *
GTMProxy_ThreadAddConnection(GTMProxy_ConnectionInfo *conninfo)
{
	int con_id = -1, con_idx = 0;
	GTMProxy_ThreadInfo *thrinfo = NULL;

	/*
	 * Get the next thread in the queue
	 */
	GTM_RWLockAcquire(&GTMProxyThreads->gt_lock, GTM_LOCKMODE_WRITE);

	/*
	 * Always start with thread 1 because thread 0 is the main thread
	 */
	if (GTMProxyThreads->gt_next_worker == 0)
		GTMProxyThreads->gt_next_worker = 1;

	thrinfo = GTMProxyThreads->gt_threads[GTMProxyThreads->gt_next_worker];

	/*
	 * Set the next worker thread before releasing the lock
	 */
	GTMProxyThreads->gt_next_worker++;
	if (GTMProxyThreads->gt_next_worker == GTMProxyThreads->gt_thread_count)
	   GTMProxyThreads->gt_next_worker = 1;

	GTM_RWLockRelease(&GTMProxyThreads->gt_lock);

	/*
	 * Lock the threadninfo structure to safely add the new connection to the
	 * thread structure. The thread will see the connection when it queries the
	 * socket descriptor in the next cycle
	 */
	GTM_MutexLockAcquire(&thrinfo->thr_lock);

	if (thrinfo->thr_conn_count >= GTM_PROXY_MAX_CONNECTIONS)
	{
		GTM_MutexLockRelease(&thrinfo->thr_lock);
		elog(ERROR, "Too many connections");
	}

	/* Find unassigned connection id in this worker thread. */
	for (con_id = 0; con_id < GTM_PROXY_MAX_CONNECTIONS; con_id++)
		if (thrinfo->thr_conid2idx[con_id] == -1)
			break;

	if (con_id >= GTM_PROXY_MAX_CONNECTIONS) {
		GTM_MutexLockRelease(&thrinfo->thr_lock);
		elog(ERROR, "Unassigned connection id not found.");
	}

	/*
	 * Save the array slotid in the conninfo structure. We send this to the GTM
	 * server as an identifier which the GTM server sends us back in the
	 * response. We use that information to route the response back to the
	 * approrpiate connection
	 */
	con_idx = thrinfo->thr_conn_count++;
	conninfo->con_id = con_id;
	thrinfo->thr_conid2idx[con_id] = con_idx;
	thrinfo->thr_all_conns[con_idx] = conninfo;
	elog(DEBUG5, "Assigned a connection id to new connection: id = %d, index = %d", con_id, con_idx);

	/*
	 * Now increment the seqno since a new connection is added to the array.
	 * Before we do the next poll(), the fd array will be forced to be
	 * reconstructed.
	 */
   	thrinfo->thr_seqno++;

	/*
	 * Signal the worker thread if its waiting for connections to be added to
	 * its Q
	 *
	 * XXX May be we can first check the condition that this is the first
	 * connection in the array and also use signal instead of a bcast since
	 * only one thread is waiting on the cv.
	 */
	GTM_CVBcast(&thrinfo->thr_cv);
	GTM_MutexLockRelease(&thrinfo->thr_lock);

	return thrinfo;
}

/*
 * Remove the connection from the array and compact the array
 */
int
GTMProxy_ThreadRemoveConnection(GTMProxy_ThreadInfo *thrinfo, GTMProxy_ConnectionInfo *conninfo)
{
	int ii;

	/*
	 * Lock the threadninfo structure to safely remove the connection from the
	 * thread structure.
	 */
	GTM_MutexLockAcquire(&thrinfo->thr_lock);

	for (ii = 0; ii < thrinfo->thr_conn_count; ii++)
	{
		if (thrinfo->thr_all_conns[ii] == conninfo)
			break;
	}

	if (ii >= thrinfo->thr_conn_count)
	{
		GTM_MutexLockRelease(&thrinfo->thr_lock);
		elog(ERROR, "No such connection");
	}

	/*
	 * Reset command backup info
	 */
	thrinfo->thr_any_backup[ii] = FALSE;
	thrinfo->thr_qtype[ii] = 0;
	resetStringInfo(&(thrinfo->thr_inBufData[ii]));

	/* Release connection id */
	if (conninfo->con_id != InvalidGTMProxyConnID)
	{
		thrinfo->thr_conid2idx[conninfo->con_id] = -1;
		elog(DEBUG5, "Released connection id %d", conninfo->con_id);
	}

	/*
	 * If this is the last entry in the array ? If not, then copy the last
	 * entry in this slot and mark the last slot an empty
	 */
	if ((ii + 1) < thrinfo->thr_conn_count)
	{
		GTMProxy_ConnectionInfo *ci_moved;
		int last_idx;

		/* Pick up last slot */
		last_idx = thrinfo->thr_conn_count - 1;
		ci_moved = thrinfo->thr_all_conns[last_idx];
		
		/* Copy the last entry in this slot */
		thrinfo->thr_all_conns[ii] = ci_moved;

		/* Mark the last slot free */
		thrinfo->thr_all_conns[last_idx] = NULL;

		/* Adjust the mapping to reflect the current slot in the array */
		if (ci_moved->con_id != InvalidGTMProxyConnID)
			thrinfo->thr_conid2idx[ci_moved->con_id] = ii;
	}
	else
	{
		/* This is the last entry in the array. Just mark it free */
		thrinfo->thr_all_conns[ii] = NULL;
	}

	thrinfo->thr_conn_count--;

	/*
	 * Increment the seqno to ensure that the next time before we poll, the fd
	 * array is reconstructed.
	 */
	thrinfo->thr_seqno++;
	GTM_MutexLockRelease(&thrinfo->thr_lock);

	return 0;
}
