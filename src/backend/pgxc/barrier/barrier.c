/*-------------------------------------------------------------------------
 *
 * barrier.c
 *
 *	  Barrier handling for PITR
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/gtm.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "pgxc/barrier.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"
#include "storage/lwlock.h"
#include "tcop/dest.h"

static const char *generate_barrier_id(const char *id);
static PGXCNodeAllHandles *PrepareBarrier(const char *id);
static void ExecuteBarrier(const char *id);
static void EndBarrier(PGXCNodeAllHandles *handles, const char *id);

/*
 * Prepare ourselves for an incoming BARRIER. We must disable all new 2PC
 * commits and let the ongoing commits to finish. We then remember the
 * barrier id (so that it can be matched with the final END message) and
 * tell the driving Coordinator to proceed with the next step.
 *
 * A simple way to implement this is to grab a lock in an exclusive mode
 * while all other backend starting a 2PC will grab the lock in shared
 * mode. So as long as we hold the exclusive lock, no other backend start a
 * new 2PC and there can not be any 2PC in-progress. This technique would
 * rely on assumption that an exclusive lock requester is not starved by
 * share lock requesters.
 *
 * Note: To ensure that the 2PC are not blocked for a long time, we should
 * set a timeout. The lock should be release after the timeout and the
 * barrier should be canceled.
 */
void
ProcessCreateBarrierPrepare(const char *id)
{
	StringInfoData buf;

	if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The CREATE BARRIER PREPARE message is expected to "
						"arrive at a Coordinator from another Coordinator")));

	LWLockAcquire(BarrierLock, LW_EXCLUSIVE);

	pq_beginmessage(&buf, 'b');
	pq_sendstring(&buf, id);
	pq_endmessage(&buf);
	pq_flush();

	/*
	 * TODO Start a timer to terminate the pending barrier after a specified
	 * timeout
	 */
}

/*
 * Mark the completion of an on-going barrier. We must have remembered the
 * barrier ID when we received the CREATE BARRIER PREPARE command
 */
void
ProcessCreateBarrierEnd(const char *id)
{
	StringInfoData buf;

	if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The CREATE BARRIER END message is expected to "
						"arrive at a Coordinator from another Coordinator")));

	LWLockRelease(BarrierLock);

	pq_beginmessage(&buf, 'b');
	pq_sendstring(&buf, id);
	pq_endmessage(&buf);
	pq_flush();

	/*
	 * TODO Stop the timer
	 */
}

/*
 * Execute the CREATE BARRIER command. Write a BARRIER WAL record and flush the
 * WAL buffers to disk before returning to the caller. Writing the WAL record
 * does not guarantee successful completion of the barrier command.
 */
void
ProcessCreateBarrierExecute(const char *id)
{
	StringInfoData buf;

	if (!IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The CREATE BARRIER EXECUTE message is expected to "
						"arrive from a Coordinator")));
	{
		XLogRecData rdata[1];
		XLogRecPtr recptr;

		rdata[0].data = (char *) id;
		rdata[0].len = strlen(id) + 1;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = NULL;

		recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_CREATE, rdata);
		XLogFlush(recptr);
	}

	pq_beginmessage(&buf, 'b');
	pq_sendstring(&buf, id);
	pq_endmessage(&buf);
	pq_flush();
}

static const char *
generate_barrier_id(const char *id)
{
	char genid[1024];
	TimestampTz ts;

	/*
	 * If the caller can passed a NULL value, generate an id which is
	 * guaranteed to be unique across the cluster. We use a combination of
	 * the Coordinator node id and current timestamp.
	 */

	if (id)
	   return id;

	ts = GetCurrentTimestamp();
#ifdef HAVE_INT64_TIMESTAMP
	sprintf(genid, "%s_"INT64_FORMAT, PGXCNodeName, ts);
#else
	sprintf(genid, "%s_%.0f", PGXCNodeName, ts);
#endif
	return pstrdup(genid);
}

static PGXCNodeAllHandles *
SendBarrierPrepareRequest(List *coords, const char *id)
{
	PGXCNodeAllHandles *coord_handles;
	int conn;
	int msglen;
	int barrier_idlen;

	coord_handles = get_handles(NIL, coords, true);

	for (conn = 0; conn < coord_handles->co_conn_count; conn++)
	{
		PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

		/* Invalid connection state, return error */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send CREATE BARRIER PREPARE request "
						 	"to the node")));

		barrier_idlen = strlen(id) + 1;

		msglen = 4; /* for the length itself */
		msglen += barrier_idlen;
		msglen += 1; /* for barrier command itself */

		/* msgType + msgLen */
		if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Out of memory")));
		}

		handle->outBuffer[handle->outEnd++] = 'b';
		msglen = htonl(msglen);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;

		handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_PREPARE;

		memcpy(handle->outBuffer + handle->outEnd, id, barrier_idlen);
		handle->outEnd += barrier_idlen;

		handle->state = DN_CONNECTION_STATE_QUERY;

		pgxc_node_flush(handle);
	}

	return coord_handles;
}

static void
CheckBarrierCommandStatus(PGXCNodeAllHandles *conn_handles, const char *id,
						  const char *command)
{
	int conn;
	int count = conn_handles->co_conn_count + conn_handles->dn_conn_count;

	elog(DEBUG1, "Check CREATE BARRIER <%s> %s command status", id, command);

	for (conn = 0; conn < count; conn++)
	{
		PGXCNodeHandle *handle;

		if (conn < conn_handles->co_conn_count)
			handle = conn_handles->coord_handles[conn];
		else
			handle = conn_handles->datanode_handles[conn - conn_handles->co_conn_count];

		if (pgxc_node_receive(1, &handle, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to receive response from the remote side")));

		if (handle_response(handle, NULL) != RESPONSE_BARRIER_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("CREATE BARRIER PREPARE command failed "
						 	"with error %s", handle->error)));
	}

	elog(DEBUG1, "Successfully completed CREATE BARRIER <%s> %s command on "
				 "all nodes", id, command);
}

static void
SendBarrierEndRequest(PGXCNodeAllHandles *coord_handles, const char *id)
{
	int conn;
	int msglen;
	int barrier_idlen;

	elog(DEBUG1, "Sending CREATE BARRIER <%s> END command to all Coordinators", id);

	for (conn = 0; conn < coord_handles->co_conn_count; conn++)
	{
		PGXCNodeHandle *handle = coord_handles->coord_handles[conn];

		/* Invalid connection state, return error */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send CREATE BARRIER PREPARE request "
						 	"to the node")));

		barrier_idlen = strlen(id) + 1;

		msglen = 4; /* for the length itself */
		msglen += barrier_idlen;
		msglen += 1; /* for barrier command itself */

		/* msgType + msgLen */
		if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Out of memory")));
		}

		handle->outBuffer[handle->outEnd++] = 'b';
		msglen = htonl(msglen);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;

		handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_END;

		memcpy(handle->outBuffer + handle->outEnd, id, barrier_idlen);
		handle->outEnd += barrier_idlen;

		handle->state = DN_CONNECTION_STATE_QUERY;
		pgxc_node_flush(handle);
	}

}

/*
 * Prepare all Coordinators for barrier. During this step all the Coordinators
 * are informed to suspend any new 2PC transactions. The Coordinators should
 * disable new 2PC transactions and then wait for the existing transactions to
 * complete. Once all "in-flight" 2PC transactions are over, the Coordinators
 * respond back.
 *
 * That completes the first step in barrier generation
 *
 * Any errors will be reported via ereport.
 */
static PGXCNodeAllHandles *
PrepareBarrier(const char *id)
{
	PGXCNodeAllHandles *coord_handles;

	elog(DEBUG1, "Preparing Coordinators for BARRIER");

	/*
	 * Send a CREATE BARRIER PREPARE message to all the Coordinators. We should
	 * send an asynchronous request so that we can disable local commits and
	 * then wait for the remote Coordinators to finish the work
	 */
	coord_handles = SendBarrierPrepareRequest(GetAllCoordNodes(), id);

	/*
	 * Disable local commits
	 */
	LWLockAcquire(BarrierLock, LW_EXCLUSIVE);

	elog(DEBUG2, "Disabled 2PC commits originating at the driving Coordinator");

	/*
	 * TODO Start a timer to cancel the barrier request in case of a timeout
	 */

	/*
	 * Local in-flight commits are now over. Check status of the remote
	 * Coordinators
	 */
	CheckBarrierCommandStatus(coord_handles, id, "PREPARE");

	return coord_handles;
}

/*
 * Execute the barrier command on all the components, including Datanodes and
 * Coordinators.
 */
static void
ExecuteBarrier(const char *id)
{
	List *barrierDataNodeList = GetAllDataNodes();
	List *barrierCoordList = GetAllCoordNodes();
	PGXCNodeAllHandles *conn_handles;
	int conn;
	int msglen;
	int barrier_idlen;

	conn_handles = get_handles(barrierDataNodeList, barrierCoordList, false);

	elog(DEBUG1, "Sending CREATE BARRIER <%s> EXECUTE message to "
				 "Datanodes and Coordinator", id);
	/*
	 * Send a CREATE BARRIER request to all the Datanodes and the Coordinators
	 */
	for (conn = 0; conn < conn_handles->co_conn_count + conn_handles->dn_conn_count; conn++)
	{
		PGXCNodeHandle *handle;

		if (conn < conn_handles->co_conn_count)
			handle = conn_handles->coord_handles[conn];
		else
			handle = conn_handles->datanode_handles[conn - conn_handles->co_conn_count];

		/* Invalid connection state, return error */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send CREATE BARRIER EXECUTE request "
						 	"to the node")));

		barrier_idlen = strlen(id) + 1;

		msglen = 4; /* for the length itself */
		msglen += barrier_idlen;
		msglen += 1; /* for barrier command itself */

		/* msgType + msgLen */
		if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Out of memory")));
		}

		handle->outBuffer[handle->outEnd++] = 'b';
		msglen = htonl(msglen);
		memcpy(handle->outBuffer + handle->outEnd, &msglen, 4);
		handle->outEnd += 4;

		handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_EXECUTE;

		memcpy(handle->outBuffer + handle->outEnd, id, barrier_idlen);
		handle->outEnd += barrier_idlen;

		handle->state = DN_CONNECTION_STATE_QUERY;
		pgxc_node_flush(handle);
	}

	CheckBarrierCommandStatus(conn_handles, id, "EXECUTE");

	pfree_pgxc_all_handles(conn_handles);

	/*
	 * Also WAL log the BARRIER locally and flush the WAL buffers to disk
	 */
	{
		XLogRecData rdata[1];
		XLogRecPtr recptr;

		rdata[0].data = (char *) id;
		rdata[0].len = strlen(id) + 1;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = NULL;

		recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_CREATE, rdata);
		XLogFlush(recptr);
	}
}

/*
 * Resume 2PC commits on the local as well as remote Coordinators.
 */
static void
EndBarrier(PGXCNodeAllHandles *prepared_handles, const char *id)
{
	/* Resume 2PC locally */
	LWLockRelease(BarrierLock);

	SendBarrierEndRequest(prepared_handles, id);

	CheckBarrierCommandStatus(prepared_handles, id, "END");
}

void
RequestBarrier(const char *id, char *completionTag)
{
	PGXCNodeAllHandles *prepared_handles;
	const char *barrier_id;

	elog(DEBUG1, "CREATE BARRIER request received");
	/*
	 * Ensure that we are a Coordinator and the request is not from another
	 * coordinator
	 */
	if (!IS_PGXC_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("CREATE BARRIER command must be sent to a Coordinator")));

	if (IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("CREATE BARRIER command is not expected from another Coordinator")));

	/*
	 * Get a barrier id if the user has not supplied it
	 */
	barrier_id = generate_barrier_id(id);

	elog(DEBUG1, "CREATE BARRIER <%s>", barrier_id);

	/*
	 * Step One. Prepare all Coordinators for upcoming barrier request
	 */
	prepared_handles = PrepareBarrier(barrier_id);

	/*
	 * Step two. Issue BARRIER command to all involved components, including
	 * Coordinators and Datanodes
	 */
	ExecuteBarrier(barrier_id);

	/*
	 * Step three. Inform Coordinators about a successfully completed barrier
	 */
	EndBarrier(prepared_handles, barrier_id);
	/* Finally report the barrier to GTM to backup its restart point */
	ReportBarrierGTM((char *)barrier_id);

	/* Free the handles */
	pfree_pgxc_all_handles(prepared_handles);

	if (completionTag)
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "BARRIER %s", barrier_id);
}
