/*-------------------------------------------------------------------------
 *
 * gtm_standby.c
 *		Functionalities of GTM Standby
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_standby.c
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_standby.h"

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_utils.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/register.h"
#include "gtm/standby_utils.h"
#include "gtm/stringinfo.h"

GTM_Conn *GTM_ActiveConn = NULL;
static char standbyHostName[NI_MAXHOST];
static char standbyNodeName[NI_MAXHOST];
static int standbyPortNumber;
static char *standbyDataDir;

static GTM_Conn * gtm_standby_connect_to_standby_int(int *report_needed);
static GTM_Conn *gtm_standby_connectToActiveGTM(void);

extern char *NodeName;		/* Defined in main.c */

int
gtm_standby_start_startup(void)
{
	GTM_ActiveConn = gtm_standby_connectToActiveGTM();
	if (GTM_ActiveConn == NULL)
	{
		elog(DEBUG3, "Error in connection");
		return 0;
	}
	elog(LOG, "Connection established to the GTM active.");

	/* Initialize standby lock */
	Recovery_InitStandbyLock();

	return 1;
}

int
gtm_standby_finish_startup(void)
{
	elog(INFO, "Closing a startup connection...");

	GTMPQfinish(GTM_ActiveConn);
	GTM_ActiveConn = NULL;

	elog(INFO, "A startup connection closed.");
	return 1;
}

int
gtm_standby_restore_next_gxid(void)
{
	GlobalTransactionId next_gxid = InvalidGlobalTransactionId;

	next_gxid = get_next_gxid(GTM_ActiveConn);
	GTM_RestoreTxnInfo(NULL, next_gxid);

	elog(INFO, "Restoring the next GXID done.");
	return 1;
}

int
gtm_standby_restore_sequence(void)
{
	GTM_SeqInfo *seq_list;
	int num_seq;
	int i;

	/*
	 * Restore sequence data.
	 */
	num_seq = get_sequence_list(GTM_ActiveConn, &seq_list);

	for (i = 0; i < num_seq; i++)
	{
		GTM_SeqRestore(seq_list[i].gs_key,
					   seq_list[i].gs_increment_by,
					   seq_list[i].gs_min_value,
					   seq_list[i].gs_max_value,
					   seq_list[i].gs_init_value,
					   seq_list[i].gs_value,
					   seq_list[i].gs_state,
					   seq_list[i].gs_cycle,
					   seq_list[i].gs_called);
	}

	elog(INFO, "Restoring sequences done.");
	return 1;
}

int
gtm_standby_restore_gxid(void)
{
	int num_txn;
	GTM_Transactions txn;
	int i;

	/*
	 * Restore gxid data.
	 */
	num_txn = get_txn_gxid_list(GTM_ActiveConn, &txn);

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	GTMTransactions.gt_txn_count = txn.gt_txn_count;
	GTMTransactions.gt_gtm_state = txn.gt_gtm_state;
	GTMTransactions.gt_nextXid = txn.gt_nextXid;
	GTMTransactions.gt_oldestXid = txn.gt_oldestXid;
	GTMTransactions.gt_xidVacLimit = txn.gt_xidVacLimit;
	GTMTransactions.gt_xidWarnLimit = txn.gt_xidWarnLimit;
	GTMTransactions.gt_xidStopLimit = txn.gt_xidStopLimit;
	GTMTransactions.gt_xidWrapLimit = txn.gt_xidWrapLimit;
	GTMTransactions.gt_latestCompletedXid = txn.gt_latestCompletedXid;
	GTMTransactions.gt_recent_global_xmin = txn.gt_recent_global_xmin;
	GTMTransactions.gt_lastslot = txn.gt_lastslot;

	for (i = 0; i < num_txn; i++)
	{
		GTMTransactions.gt_transactions_array[i].gti_handle = txn.gt_transactions_array[i].gti_handle;
		GTMTransactions.gt_transactions_array[i].gti_thread_id = txn.gt_transactions_array[i].gti_thread_id;
		GTMTransactions.gt_transactions_array[i].gti_in_use = txn.gt_transactions_array[i].gti_in_use;
		GTMTransactions.gt_transactions_array[i].gti_gxid = txn.gt_transactions_array[i].gti_gxid;
		GTMTransactions.gt_transactions_array[i].gti_state = txn.gt_transactions_array[i].gti_state;
		GTMTransactions.gt_transactions_array[i].gti_coordname = txn.gt_transactions_array[i].gti_coordname;
		GTMTransactions.gt_transactions_array[i].gti_xmin = txn.gt_transactions_array[i].gti_xmin;
		GTMTransactions.gt_transactions_array[i].gti_isolevel = txn.gt_transactions_array[i].gti_isolevel;
		GTMTransactions.gt_transactions_array[i].gti_readonly = txn.gt_transactions_array[i].gti_readonly;
		GTMTransactions.gt_transactions_array[i].gti_backend_id = txn.gt_transactions_array[i].gti_backend_id;

		if (txn.gt_transactions_array[i].nodestring == NULL )
			GTMTransactions.gt_transactions_array[i].nodestring = NULL;
		else
			GTMTransactions.gt_transactions_array[i].nodestring = txn.gt_transactions_array[i].nodestring;

		/* GID */
		if (txn.gt_transactions_array[i].gti_gid == NULL )
			GTMTransactions.gt_transactions_array[i].gti_gid = NULL;
		else
			GTMTransactions.gt_transactions_array[i].gti_gid = txn.gt_transactions_array[i].gti_gid;

		/* copy GTM_SnapshotData */
		GTMTransactions.gt_transactions_array[i].gti_current_snapshot.sn_xmin =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xmin;
		GTMTransactions.gt_transactions_array[i].gti_current_snapshot.sn_xmax =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xmax;
		GTMTransactions.gt_transactions_array[i].gti_current_snapshot.sn_recent_global_xmin =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_recent_global_xmin;
		GTMTransactions.gt_transactions_array[i].gti_current_snapshot.sn_xcnt =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xcnt;
		GTMTransactions.gt_transactions_array[i].gti_current_snapshot.sn_xip =
						txn.gt_transactions_array[i].gti_current_snapshot.sn_xip;
		/* end of copying GTM_SnapshotData */

		GTMTransactions.gt_transactions_array[i].gti_snapshot_set =
						txn.gt_transactions_array[i].gti_snapshot_set;
		GTMTransactions.gt_transactions_array[i].gti_vacuum =
						txn.gt_transactions_array[i].gti_vacuum;

		/*
		 * Is this correct? Is GTM_TXN_COMMITTED transaction categorized as "open"?
		 */
		if (GTMTransactions.gt_transactions_array[i].gti_state != GTM_TXN_ABORTED)
		{
			GTMTransactions.gt_open_transactions =
					gtm_lappend(GTMTransactions.gt_open_transactions,
							&GTMTransactions.gt_transactions_array[i]);
		}
	}

	dump_transactions_elog(&GTMTransactions, num_txn);

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	elog(INFO, "Restoring %d gxid(s) done.", num_txn);
	return 1;
}

int
gtm_standby_restore_node(void)
{
	GTM_PGXCNodeInfo *data;
	int rc, i;
	int num_node;

	elog(INFO, "Copying node information from the GTM active...");

	data = (GTM_PGXCNodeInfo *) malloc(sizeof(GTM_PGXCNodeInfo) * 128);
	memset(data, 0, sizeof(GTM_PGXCNodeInfo) * 128);

	rc = get_node_list(GTM_ActiveConn, data, 128);
	if (rc < 0)
	{
		elog(DEBUG3, "get_node_list() failed.");
		rc = 0;
		goto finished;
	}

	num_node = rc;

	for (i = 0; i < num_node; i++)
	{
		elog(DEBUG1, "get_node_list: nodetype=%d, nodename=%s, datafolder=%s",
			 data[i].type, data[i].nodename, data[i].datafolder);
		if (Recovery_PGXCNodeRegister(data[i].type, data[i].nodename, data[i].port,
					 data[i].proxyname, data[i].status,
					 data[i].ipaddress, data[i].datafolder, true,
					 -1 /* dummy socket */) != 0)
		{
			rc = 0;
			goto finished;
		}
	}

	elog(INFO, "Copying node information from GTM active done.");

finished:
	free(data);
	return rc;
}

/*
 * Register myself to the GTM (active) as a "disconnected" node.
 *
 * This status would be updated later after restoring completion.
 * See gtm_standby_update_self().
 *
 * Returns 1 on success, 0 on failure.
 */
int
gtm_standby_register_self(const char *node_name, int port, const char *datadir)
{
	int rc;

	elog(INFO, "Registering standby-GTM status...");

	node_get_local_addr(GTM_ActiveConn, standbyHostName, sizeof(standbyNodeName), &rc);
	if (rc != 0)
		return 0;

	memset(standbyNodeName, 0, NI_MAXHOST);
	strncpy(standbyNodeName, node_name, NI_MAXHOST - 1);
	standbyPortNumber = port;
	standbyDataDir= (char *)datadir;

	rc = node_register_internal(GTM_ActiveConn, GTM_NODE_GTM, standbyHostName, standbyPortNumber,
								standbyNodeName, standbyDataDir, NODE_DISCONNECTED);
	if (rc < 0)
	{
		elog(LOG, "Failed to register a standby-GTM status.");
		return 0;
	}

	elog(INFO, "Registering standby-GTM done.");

	return 1;
}

/*
 * Update my node status from "disconnected" to "connected" in GTM by myself.
 *
 * Returns 1 on success, 0 on failure.
 */
int
gtm_standby_activate_self(void)
{
	int rc;

	elog(DEBUG1, "Updating the standby-GTM status to \"CONNECTED\"...");

	rc = node_unregister(GTM_ActiveConn, GTM_NODE_GTM, standbyNodeName);
	if (rc < 0)
	{
		elog(LOG, "Failed to unregister old standby-GTM status.");
		return 0;
	}

	rc = node_register_internal(GTM_ActiveConn, GTM_NODE_GTM, standbyHostName, standbyPortNumber,
								standbyNodeName, standbyDataDir, NODE_CONNECTED);

	if (rc < 0)
	{
		elog(LOG, "Failed to register a new standby-GTM status.");
		return 0;
	}

	elog(INFO, "Updating the standby-GTM status done.");

	return 1;
}


/*
 * Find "one" GTM standby node info.
 *
 * Returns a pointer to GTM_PGXCNodeInfo on success,
 * or returns NULL on failure.
 */
GTM_PGXCNodeInfo *
find_standby_node_info(void)
{
	GTM_PGXCNodeInfo *node[1024];
	size_t n;
	int i;

	n = pgxcnode_find_by_type(GTM_NODE_GTM, node, 1024);

	for (i = 0 ; i < n ; i++)
	{
		elog(DEBUG1, "pgxcnode_find_by_type: nodename=%s, type=%d, ipaddress=%s, port=%d, status=%d",
			 node[i]->nodename,
			 node[i]->type,
			 node[i]->ipaddress,
			 node[i]->port,
			 node[i]->status);

		if ( (strcmp(standbyNodeName, node[i]->nodename) != 0) &&
			node[i]->status == NODE_CONNECTED)
			return node[i];
	}

	return NULL;
}


/*
 * Make a connection to the GTM standby node when getting connected
 * from the client.
 *
 * Returns a pointer to a GTM_Conn object on success, or NULL on failure.
 */
GTM_Conn *
gtm_standby_connect_to_standby(void)
{
	GTM_Conn *conn;
	int report;

	conn = gtm_standby_connect_to_standby_int(&report);

	return conn;
}

static GTM_Conn *
gtm_standby_connect_to_standby_int(int *report_needed)
{
	GTM_Conn *standby = NULL;
	GTM_PGXCNodeInfo *n;
	char conn_string[1024];

	*report_needed = 0;

	if (Recovery_IsStandby())
		return NULL;

	n = find_standby_node_info();

	if (!n)
	{
		elog(LOG, "Any GTM standby node not found in registered node(s).");
		return NULL;
	}

	elog(INFO, "GTM standby is active. Going to connect.");
	*report_needed = 1;

	snprintf(conn_string, sizeof(conn_string),
		 "host=%s port=%d node_name=%s remote_type=4",
			 n->ipaddress, n->port, NodeName);

	standby = PQconnectGTM(conn_string);

	if ( !standby )
	{
	 	elog(LOG, "Failed to establish a connection with GTM standby. - %p", n);
		return NULL;
	}

	elog(DEBUG1, "Connection established with GTM standby. - %p", n);

	return standby;
}

void
gtm_standby_disconnect_from_standby(GTM_Conn *conn)
{
	if (Recovery_IsStandby())
		return;

	GTMPQfinish(conn);
}


GTM_Conn *
gtm_standby_reconnect_to_standby(GTM_Conn *old_conn, int retry_max)
{
	GTM_Conn *newconn = NULL;
	int report;
	int i;

	if (Recovery_IsStandby())
		return NULL;

	if (old_conn != NULL)
		gtm_standby_disconnect_from_standby(old_conn);

	for (i = 0; i < retry_max; i++)
	{
		elog(DEBUG1, "gtm_standby_reconnect_to_standby(): going to re-connect. retry=%d", i);

		newconn = gtm_standby_connect_to_standby_int(&report);
		if (newconn != NULL)
			break;

		elog(DEBUG1, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=%d", i);
	}

	return newconn;
}


#define GTM_STANDBY_RETRY_MAX 3

bool
gtm_standby_check_communication_error(int *retry_count, GTM_Conn *oldconn)
{
	GTM_ThreadInfo *my_threadInfo = GetMyThreadInfo;

	/*
	 * This function may be called without result from standby.
	 */
	if (my_threadInfo->thr_conn->standby->result
		&& my_threadInfo->thr_conn->standby->result->gr_status == GTM_RESULT_COMM_ERROR)
	{
		if (*retry_count == 0)
		{
			(*retry_count)++;

			GetMyThreadInfo->thr_conn->standby =
					gtm_standby_reconnect_to_standby(GetMyThreadInfo->thr_conn->standby,
													 GTM_STANDBY_RETRY_MAX);

			if (GetMyThreadInfo->thr_conn->standby)
				return true;
		}

		elog(LOG, "communication error with standby.");
	}
	return false;
}

int
gtm_standby_begin_backup(void)
{
	int rc = set_begin_end_backup(GTM_ActiveConn, true);
	return (rc ? 0 : 1);
}

int
gtm_standby_end_backup(void)
{
	int rc = set_begin_end_backup(GTM_ActiveConn, false);
	return (rc ? 0 : 1);
}

extern char *NodeName;		/* Defined in main.c */

void
gtm_standby_finishActiveConn(void)
{
	GTM_ActiveConn = gtm_standby_connectToActiveGTM();
	if (GTM_ActiveConn == NULL)
	{
		elog(DEBUG3, "Error in connection");
		return;
	}
	elog(LOG, "Connection established to the GTM active.");

	/* Unregister self from Active-GTM */
	node_unregister(GTM_ActiveConn, GTM_NODE_GTM, NodeName);
	/* Disconnect form Active */
	GTMPQfinish(GTM_ActiveConn);
}

static GTM_Conn *
gtm_standby_connectToActiveGTM(void)
{
	char connect_string[1024];
	int active_port = Recovery_StandbyGetActivePort();
	char *active_address = Recovery_StandbyGetActiveAddress();

	/* Need to connect to Active-GTM again here */
	elog(LOG, "Connecting the GTM active on %s:%d...", active_address, active_port);

	sprintf(connect_string, "host=%s port=%d node_name=%s remote_type=%d",
			active_address, active_port, NodeName, GTM_NODE_GTM);

	return PQconnectGTM(connect_string);
}

void
ProcessGTMBeginBackup(Port *myport, StringInfo message)
{
	int ii;
	GTM_ThreadInfo *my_threadinfo;
	StringInfoData buf;

	pq_getmsgend(message);
	my_threadinfo = GetMyThreadInfo;

	for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
	{
		if (GTMThreads->gt_threads[ii] && GTMThreads->gt_threads[ii] != my_threadinfo)
			GTM_RWLockAcquire(&GTMThreads->gt_threads[ii]->thr_lock, GTM_LOCKMODE_WRITE);
	}
	my_threadinfo->thr_status = GTM_THREAD_BACKUP;
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, BEGIN_BACKUP_RESULT, 4);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
}

void
ProcessGTMEndBackup(Port *myport, StringInfo message)
{
	int ii;
	GTM_ThreadInfo *my_threadinfo;
	StringInfoData buf;

	pq_getmsgend(message);
	my_threadinfo = GetMyThreadInfo;

	for (ii = 0; ii < GTMThreads->gt_array_size; ii++)
	{
		if (GTMThreads->gt_threads[ii] && GTMThreads->gt_threads[ii] != my_threadinfo)
			GTM_RWLockRelease(&GTMThreads->gt_threads[ii]->thr_lock);
	}
	my_threadinfo->thr_status = GTM_THREAD_RUNNING;
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, END_BACKUP_RESULT, 4);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
}
