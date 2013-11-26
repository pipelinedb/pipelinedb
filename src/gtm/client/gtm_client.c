/*-------------------------------------------------------------------------
 *
 * gtm-client.c
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
/* Time in seconds to wait for a response from GTM */
/* We should consider making this a GUC */
#ifndef CLIENT_GTM_TIMEOUT
#ifdef  GTM_DEBUG
#define CLIENT_GTM_TIMEOUT 3600
#else
#define CLIENT_GTM_TIMEOUT 20
#endif
#endif

#include <time.h>

#include "gtm/gtm_c.h"

#include "gtm/gtm_ip.h"
#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"

#include "gtm/gtm_client.h"
#include "gtm/gtm_msg.h"
#include "gtm/gtm_serialize.h"
#include "gtm/register.h"
#include "gtm/assert.h"

extern bool Backup_synchronously;

void GTM_FreeResult(GTM_Result *result, GTM_PGXCNodeType remote_type);

static GTM_Result *makeEmptyResultIfIsNull(GTM_Result *oldres);
static int commit_prepared_transaction_internal(GTM_Conn *conn,
												GlobalTransactionId gxid, GlobalTransactionId prepared_gxid,
												bool is_backup);
static int prepare_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup);
static int abort_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup);
static int abort_transaction_multi_internal(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
											int *txn_count_out, int *status_out, bool is_backup);
static int open_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
								  GTM_Sequence minval, GTM_Sequence maxval,
								  GTM_Sequence startval, bool cycle, bool is_backup);
static int get_next_internal(GTM_Conn *conn, GTM_SequenceKey key,
									  GTM_Sequence *result, bool is_backup);
static int set_val_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool iscalled, bool is_backup);
static int reset_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, bool is_backup);
static int commit_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup);
static int close_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, bool is_backup);
static int rename_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey, bool is_backup);
static int alter_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
								   GTM_Sequence minval, GTM_Sequence maxval,
								   GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart, bool is_backup);
static int node_register_worker(GTM_Conn *conn, GTM_PGXCNodeType type, const char *host, GTM_PGXCNodePort port,
								char *node_name, char *datafolder, GTM_PGXCNodeStatus status, bool is_backup);
static int node_unregister_worker(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name, bool is_backup);
static int report_barrier_internal(GTM_Conn *conn, char *barrier_id, bool is_backup);
/*
 * Make an empty result if old one is null.
 */
static GTM_Result *
makeEmptyResultIfIsNull(GTM_Result *oldres)
{
	GTM_Result *res = NULL;

	if (oldres == NULL)
	{
		res = (GTM_Result *) malloc(sizeof(GTM_Result));
		memset(res, 0, sizeof(GTM_Result));
	}
	else
		return oldres;

	return res;
}

/*
 * Connection Management API
 */
GTM_Conn *
connect_gtm(const char *connect_string)
{
	return PQconnectGTM(connect_string);
}

void
disconnect_gtm(GTM_Conn *conn)
{
	GTMPQfinish(conn);
}

/*
 * begin_replication_initial_sync() acquires several locks to prepare
 * for copying internal transaction, xid and sequence information
 * to the standby node at its startup.
 *
 * returns 1 on success, 0 on failure.
 */
int
begin_replication_initial_sync(GTM_Conn *conn)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_NODE_BEGIN_REPLICATION_INIT, sizeof (GTM_MessageType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
		Assert(res->gr_type == NODE_BEGIN_REPLICATION_INIT_RESULT);
	else
		return 0;

	return 1;

receive_failed:
send_failed:
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return 0;
}

/*
 * end_replication_initial_sync() releases several locks
 * after copying internal transaction, xid and sequence information
 * to the standby node at its startup.
 *
 * returns 1 on success, 0 on failure.
 */
int
end_replication_initial_sync(GTM_Conn *conn)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
	    gtmpqPutInt(MSG_NODE_END_REPLICATION_INIT, sizeof (GTM_MessageType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
		Assert(res->gr_type == NODE_END_REPLICATION_INIT_RESULT);

	return 1;

receive_failed:
send_failed:
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return 0;
}

/*
 * get_node_list()
 *
 * returns a number of nodes on success, -1 on failure.
 */
size_t
get_node_list(GTM_Conn *conn, GTM_PGXCNodeInfo *data, size_t maxlen)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	size_t num_node;
	int i;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
	    gtmpqPutInt(MSG_NODE_LIST, sizeof (GTM_MessageType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	/*
	 * Do something here.
	 */
	num_node = res->gr_resdata.grd_node_list.num_node;

	fprintf(stderr, "get_node_list: num_node=%ld\n", num_node);

	for (i = 0; i < num_node; i++)
	{
		memcpy(&data[i], res->gr_resdata.grd_node_list.nodeinfo[i], sizeof(GTM_PGXCNodeInfo));
	}

	if (res->gr_status == GTM_RESULT_OK)
		Assert(res->gr_type == NODE_LIST_RESULT);

	return num_node;

receive_failed:
send_failed:
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * get_next_gxid()
 *
 * returns the next gxid on success, InvalidGlobalTransactionId on failure.
 */
GlobalTransactionId
get_next_gxid(GTM_Conn *conn)
{
	GTM_Result *res = NULL;
	GlobalTransactionId next_gxid;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
	    gtmpqPutInt(MSG_TXN_GET_NEXT_GXID, sizeof (GTM_MessageType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	fprintf(stderr, "GTMPQgetResult() done.\n");
	fflush(stderr);

	next_gxid = res->gr_resdata.grd_next_gxid;

	if (res->gr_status == GTM_RESULT_OK)
		Assert(res->gr_type == TXN_GET_NEXT_GXID_RESULT);

	/* FIXME: should be a number of gxids */
	return next_gxid;

receive_failed:
send_failed:
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return InvalidGlobalTransactionId;
}

/*
 * get_txn_gxid_list()
 *
 * returns a number of gxid on success, -1 on failure.
 */
uint32
get_txn_gxid_list(GTM_Conn *conn, GTM_Transactions *txn)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	int txn_count;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
	    gtmpqPutInt(MSG_TXN_GXID_LIST, sizeof (GTM_MessageType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
		Assert(res->gr_type == TXN_GXID_LIST_RESULT);

	txn_count = gtm_deserialize_transactions(txn,
						 res->gr_resdata.grd_txn_gid_list.ptr,
						 res->gr_resdata.grd_txn_gid_list.len);

	return txn_count;

receive_failed:
send_failed:
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * get_sequence_list()
 *
 * returns a number of sequences on success, -1 on failure.
 * Returned seq_list is pointing to GTM_Result structure, the data should be
 * copied before the next call to getResult.
 */
size_t
get_sequence_list(GTM_Conn *conn, GTM_SeqInfo **seq_list)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
	    gtmpqPutInt(MSG_SEQUENCE_LIST, sizeof (GTM_MessageType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
		Assert(res->gr_type == SEQUENCE_LIST_RESULT);

	*seq_list = res->gr_resdata.grd_seq_list.seq;

	return res->gr_resdata.grd_seq_list.seq_count;

receive_failed:
send_failed:
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * Transaction Management API
 */

int
bkup_begin_transaction(GTM_Conn *conn, GTM_TransactionHandle txn, GTM_IsolationLevel isolevel,
					   bool read_only, GTM_Timestamp timestamp)
{
	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_BKUP_TXN_BEGIN, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(txn, sizeof(GTM_TransactionHandle), conn) ||
		gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
		gtmpqPutc(read_only, conn) ||
		gtmpqPutnchar((char *)&timestamp, sizeof(GTM_Timestamp), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	return 0;

send_failed:
	return -1;

}

int
bkup_begin_transaction_gxid(GTM_Conn *conn, GTM_TransactionHandle txn, GlobalTransactionId gxid,
							GTM_IsolationLevel isolevel, bool read_only, GTM_Timestamp timestamp)
{
	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_BKUP_TXN_BEGIN_GETGXID, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(txn, sizeof(GTM_TransactionHandle), conn) ||
		gtmpqPutInt(gxid, sizeof(GlobalTransactionId), conn) ||
		gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
		gtmpqPutc(read_only, conn) ||
		gtmpqPutnchar((char *)&timestamp, sizeof(GTM_Timestamp), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	return 0;

send_failed:
	return -1;
}

GlobalTransactionId
begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel, GTM_Timestamp *timestamp)
{
	bool txn_read_only = false;
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_TXN_BEGIN_GETGXID, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
		gtmpqPutc(txn_read_only, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		if (timestamp)
			*timestamp = res->gr_resdata.grd_gxid_tp.timestamp;

		return res->gr_resdata.grd_gxid_tp.gxid;
	}
	else
		return InvalidGlobalTransactionId;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return InvalidGlobalTransactionId;
}


int
bkup_begin_transaction_autovacuum(GTM_Conn *conn, GTM_TransactionHandle txn, GlobalTransactionId gxid,
								  GTM_IsolationLevel isolevel)
{
	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(txn, sizeof(GTM_TransactionHandle), conn) ||
		gtmpqPutInt(gxid, sizeof(GlobalTransactionId), conn) ||
		gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	return 0;

send_failed:
	return -1;
}
/*
 * Transaction Management API
 * Begin a transaction for an autovacuum worker process
 */
GlobalTransactionId
begin_transaction_autovacuum(GTM_Conn *conn, GTM_IsolationLevel isolevel)
{
	bool txn_read_only = false;
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_TXN_BEGIN_GETGXID_AUTOVACUUM, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
		gtmpqPutc(txn_read_only, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
		return res->gr_resdata.grd_gxid;
	else
		return InvalidGlobalTransactionId;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return InvalidGlobalTransactionId;
}

int
bkup_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
	return commit_transaction_internal(conn, gxid, true);
}


int
commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
	return commit_transaction_internal(conn, gxid, false);
}


static int
commit_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_TXN_COMMIT : MSG_TXN_COMMIT, sizeof (GTM_MessageType), conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		if (res->gr_status == GTM_RESULT_OK)
		{
			Assert(res->gr_type == TXN_COMMIT_RESULT);
			Assert(res->gr_resdata.grd_gxid == gxid);
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid)
{
	return commit_prepared_transaction_internal(conn, gxid, prepared_gxid, false);
}

int
bkup_commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid)
{
	return commit_prepared_transaction_internal(conn, gxid, prepared_gxid, true);
}

static int
commit_prepared_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	/* Start the message */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_TXN_COMMIT_PREPARED : MSG_TXN_COMMIT_PREPARED, sizeof (GTM_MessageType), conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&prepared_gxid, sizeof (GlobalTransactionId), conn))
		goto send_failed;

	/* Finish the message */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backends gets it */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		if (res->gr_status == GTM_RESULT_OK)
		{
			Assert(res->gr_type == TXN_COMMIT_PREPARED_RESULT);
			Assert(res->gr_resdata.grd_gxid == gxid);
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

send_failed:
receive_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
	return abort_transaction_internal(conn, gxid, false);
}

int
bkup_abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
	return abort_transaction_internal(conn, gxid, true);
}

static int
abort_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_TXN_ROLLBACK : MSG_TXN_ROLLBACK, sizeof (GTM_MessageType), conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		if (res->gr_status == GTM_RESULT_OK)
		{
			Assert(res->gr_type == TXN_ROLLBACK_RESULT);
			Assert(res->gr_resdata.grd_gxid == gxid);
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;

}

int
backup_start_prepared_transaction(GTM_Conn *conn, GTM_TransactionHandle txn, char *gid,
								  char *nodestring)
{
	Assert(nodestring && gid && conn);

	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_BKUP_TXN_START_PREPARED, sizeof(GTM_MessageType), conn) ||
		gtmpqPutc(false, conn) ||
		gtmpqPutInt(txn, sizeof(GTM_TransactionHandle), conn) ||
		gtmpqPutInt(strlen(gid), sizeof(GTM_StrLen), conn) ||
		gtmpqPutnchar(gid, strlen(gid), conn) ||
		gtmpqPutInt(strlen(nodestring), sizeof(GTM_StrLen), conn) ||
		gtmpqPutnchar(nodestring, strlen(nodestring), conn))
		goto send_failed;

	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	return GTM_RESULT_OK;

send_failed:
	return -1;
}

int
start_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
						   char *nodestring)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	Assert(nodestring);

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_TXN_START_PREPARED, sizeof (GTM_MessageType), conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn) ||
		/* Send also GID for an explicit prepared transaction */
		gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
		gtmpqPutnchar((char *) gid, strlen(gid), conn) ||
		gtmpqPutInt(strlen(nodestring), sizeof (GTM_StrLen), conn) ||
		gtmpqPutnchar((char *) nodestring, strlen(nodestring), conn))
		goto send_failed;


	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		Assert(res->gr_type == TXN_START_PREPARED_RESULT);
		Assert(res->gr_resdata.grd_gxid == gxid);
	}

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
	return prepare_transaction_internal(conn, gxid, false);
}

int
bkup_prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid)
{
	return prepare_transaction_internal(conn, gxid, true);
}

static int
prepare_transaction_internal(GTM_Conn *conn, GlobalTransactionId gxid, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_TXN_PREPARE : MSG_TXN_PREPARE, sizeof (GTM_MessageType), conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		if (res->gr_status == GTM_RESULT_OK)
		{
			Assert(res->gr_type == TXN_PREPARE_RESULT);
			Assert(res->gr_resdata.grd_gxid == gxid);
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
get_gid_data(GTM_Conn *conn,
			 GTM_IsolationLevel isolevel,
			 char *gid,
			 GlobalTransactionId *gxid,
			 GlobalTransactionId *prepared_gxid,
			 char **nodestring)
{
	bool txn_read_only = false;
	GTM_Result *res = NULL;
	time_t finish_time;

	/* Start the message */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_TXN_GET_GID_DATA, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(isolevel, sizeof (GTM_IsolationLevel), conn) ||
		gtmpqPutc(txn_read_only, conn) ||
		/* Send also GID for an explicit prepared transaction */
		gtmpqPutInt(strlen(gid), sizeof (GTM_StrLen), conn) ||
		gtmpqPutnchar((char *) gid, strlen(gid), conn))
		goto send_failed;

	/* Finish the message */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		*gxid = res->gr_resdata.grd_txn_get_gid_data.gxid;
		*prepared_gxid = res->gr_resdata.grd_txn_get_gid_data.prepared_gxid;
		*nodestring = res->gr_resdata.grd_txn_get_gid_data.nodestring;
	}

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * Snapshot Management API
 */
GTM_SnapshotData *
get_snapshot(GTM_Conn *conn, GlobalTransactionId gxid, bool canbe_grouped)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(MSG_SNAPSHOT_GET, sizeof (GTM_MessageType), conn) ||
		gtmpqPutc(canbe_grouped, conn) ||
		gtmpqPutc(true, conn) ||
		gtmpqPutnchar((char *)&gxid, sizeof (GlobalTransactionId), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		Assert(res->gr_type == SNAPSHOT_GET_RESULT);
		Assert(res->gr_resdata.grd_txn_snap_multi.gxid == gxid);
		return &(res->gr_snapshot);
	}
	else
		return NULL;


receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return NULL;
}

/*
 * Sequence Management API
 */
int
open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
			  GTM_Sequence minval, GTM_Sequence maxval,
			  GTM_Sequence startval, bool cycle)
{
	return open_sequence_internal(conn, key, increment, minval, maxval, startval, cycle, false);
}

int
bkup_open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				   GTM_Sequence minval, GTM_Sequence maxval,
				   GTM_Sequence startval, bool cycle)
{
	return open_sequence_internal(conn, key, increment, minval, maxval, startval, cycle, true);
}

static int
open_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
					   GTM_Sequence minval, GTM_Sequence maxval,
					   GTM_Sequence startval, bool cycle, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_INIT : MSG_SEQUENCE_INIT, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
		gtmpqPutnchar((char *)&increment, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&minval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&maxval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&startval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutc(cycle, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
			   GTM_Sequence minval, GTM_Sequence maxval,
			   GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart)
{
	return alter_sequence_internal(conn, key, increment, minval, maxval, startval, lastval, cycle, is_restart, false);
}

int
bkup_alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
					GTM_Sequence minval, GTM_Sequence maxval,
					GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart)
{
	return alter_sequence_internal(conn, key, increment, minval, maxval, startval, lastval, cycle, is_restart, true);
}

static int
alter_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
						GTM_Sequence minval, GTM_Sequence maxval,
						GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_ALTER : MSG_SEQUENCE_ALTER, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
		gtmpqPutnchar((char *)&increment, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&minval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&maxval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&startval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutnchar((char *)&lastval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutc(cycle, conn) ||
		gtmpqPutc(is_restart, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
close_sequence(GTM_Conn *conn, GTM_SequenceKey key)
{
	return close_sequence_internal(conn, key, false);
}

int
bkup_close_sequence(GTM_Conn *conn, GTM_SequenceKey key)
{
	return close_sequence_internal(conn, key, true);
}

static int
close_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_CLOSE : MSG_SEQUENCE_CLOSE, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
		gtmpqPutnchar((char *)&key->gsk_type, sizeof(GTM_SequenceKeyType), conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
rename_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey)
{
	return rename_sequence_internal(conn, key, newkey, false);
}

int
bkup_rename_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey)
{
	return rename_sequence_internal(conn, key, newkey, true);
}

static int
rename_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_RENAME : MSG_SEQUENCE_RENAME, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn)||
		gtmpqPutInt(newkey->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(newkey->gsk_key, newkey->gsk_keylen, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
set_val(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool iscalled)
{
	return set_val_internal(conn, key, nextval, iscalled, false);
}

int
bkup_set_val(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool iscalled)
{
	return set_val_internal(conn, key, nextval, iscalled, true);
}

static int
set_val_internal(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool iscalled, bool is_backup)
{
	GTM_Result *res = NULL;
    time_t finish_time;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_SET_VAL : MSG_SEQUENCE_SET_VAL, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn) ||
		gtmpqPutnchar((char *)&nextval, sizeof (GTM_Sequence), conn) ||
		gtmpqPutc(iscalled, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
get_next(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence *result)
{
	return get_next_internal(conn, key, result, false);
}

int
bkup_get_next(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence *result)
{
	return get_next_internal(conn, key, result, true);
}

static int
get_next_internal(GTM_Conn *conn, GTM_SequenceKey key,
				  GTM_Sequence *result, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_GET_NEXT : MSG_SEQUENCE_GET_NEXT, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		if (res->gr_status == GTM_RESULT_OK)
			*result = res->gr_resdata.grd_seq.seqval;
		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
reset_sequence(GTM_Conn *conn, GTM_SequenceKey key)
{
	return reset_sequence_internal(conn, key, false);
}

int
bkup_reset_sequence(GTM_Conn *conn, GTM_SequenceKey key)
{
	return reset_sequence_internal(conn, key, true);
}

static int
reset_sequence_internal(GTM_Conn *conn, GTM_SequenceKey key, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	 /* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_SEQUENCE_RESET : MSG_SEQUENCE_RESET, sizeof (GTM_MessageType), conn) ||
		gtmpqPutInt(key->gsk_keylen, 4, conn) ||
		gtmpqPutnchar(key->gsk_key, key->gsk_keylen, conn))
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * rc would be 0 on success, non-zero on gtm_getnameinfo_all() failure.
 */
char *
node_get_local_addr(GTM_Conn *conn, char *buf, size_t buflen, int *rc)
{
	char local_host[NI_MAXHOST];
	char local_port[NI_MAXSERV];

	*rc = 0;

	memset(local_host, 0, sizeof(local_host));
	memset(local_port, 0, sizeof(local_port));
	memset(buf, 0, buflen);

	if (conn->remote_type != GTM_NODE_GTM_PROXY)
	{
		if (gtm_getnameinfo_all(&conn->laddr.addr, conn->laddr.salen,
					local_host, sizeof(local_host),
					local_port, sizeof(local_port),
					NI_NUMERICSERV))
		{
			*rc = gtm_getnameinfo_all(&conn->laddr.addr, conn->laddr.salen,
							  local_host, sizeof(local_host),
							  local_port, sizeof(local_port),
							  NI_NUMERICHOST | NI_NUMERICSERV);
		}
	}

	if (local_host[0] != '\0')
		strncpy(buf, local_host, buflen);

	return buf;
}

/*
 * Register a Node on GTM
 * Seen from a Node viewpoint, we do not know if we are directly connected to GTM
 * or go through a proxy, so register 0 as proxy number.
 * This number is modified at proxy level automatically.
 *
 * node_register() returns 0 on success, -1 on failure.
 *
 * is_backup indicates the message should be *_BKUP_* message
 */
int node_register(GTM_Conn *conn,
			GTM_PGXCNodeType type,
			GTM_PGXCNodePort port,
			char *node_name,
			char *datafolder)
{
	char host[1024];
	int rc;

	node_get_local_addr(conn, host, sizeof(host), &rc);
	if (rc != 0)
	{
		return -1;
	}

	return node_register_worker(conn, type, host, port, node_name, datafolder, NODE_CONNECTED, false);
}

int node_register_internal(GTM_Conn *conn,
						   GTM_PGXCNodeType type,
						   const char *host,
						   GTM_PGXCNodePort port,
						   char *node_name,
						   char *datafolder,
						   GTM_PGXCNodeStatus status)
{
	return node_register_worker(conn, type, host, port, node_name, datafolder, status, false);
}

int bkup_node_register(GTM_Conn *conn,
					   GTM_PGXCNodeType type,
					   GTM_PGXCNodePort port,
					   char *node_name,
					   char *datafolder)
{
	char host[1024];
	int rc;

	node_get_local_addr(conn, host, sizeof(host), &rc);
	if (rc != 0)
	{
		return -1;
	}

	return node_register_worker(conn, type, host, port, node_name, datafolder, NODE_CONNECTED, true);
}

int bkup_node_register_internal(GTM_Conn *conn,
								GTM_PGXCNodeType type,
								const char *host,
								GTM_PGXCNodePort port,
								char *node_name,
								char *datafolder,
								GTM_PGXCNodeStatus status)
{
	return node_register_worker(conn, type, host, port, node_name, datafolder, status, true);
}

static int node_register_worker(GTM_Conn *conn,
								GTM_PGXCNodeType type,
								const char *host,
								GTM_PGXCNodePort port,
								char *node_name,
								char *datafolder,
								GTM_PGXCNodeStatus status,
								bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	char proxy_name[] = "";

	/*
	 * We should be very careful about the format of the message.
	 * Host name and its length is needed only when registering
	 * GTM Proxy.
	 * In other case, they must not be included in the message.
	 * PGXCTODO: FIXME How would this work in the new scenario
	 * Fix that for GTM and GTM-proxy
	 */
	if (gtmpqPutMsgStart('C', true, conn) ||
		/* Message Type */
		gtmpqPutInt(is_backup? MSG_BKUP_NODE_REGISTER : MSG_NODE_REGISTER, sizeof (GTM_MessageType), conn) ||
		/* Node Type to Register */
		gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), conn) ||
		/* Node name length */
		gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
		/* Node name (var-len) */
		gtmpqPutnchar(node_name, strlen(node_name), conn) ||
		/* Host name length */
		gtmpqPutInt(strlen(host), sizeof (GTM_StrLen), conn) ||
		/* Host name (var-len) */
		gtmpqPutnchar(host, strlen(host), conn) ||
		/* Port number */
		gtmpqPutnchar((char *)&port, sizeof(GTM_PGXCNodePort), conn) ||
		/* Proxy name length (zero if connected to GTM directly) */
		gtmpqPutInt(strlen(proxy_name), sizeof (GTM_StrLen), conn) ||
		/* Proxy name (var-len) */
		gtmpqPutnchar(proxy_name, strlen(proxy_name), conn) ||
		/* Proxy ID (zero if connected to GTM directly) */
		/* Data Folder length */
		gtmpqPutInt(strlen(datafolder), sizeof (GTM_StrLen), conn) ||
		/* Data Folder (var-len) */
		gtmpqPutnchar(datafolder, strlen(datafolder), conn) ||
		/* Node Status */
		gtmpqPutInt(status, sizeof(GTM_PGXCNodeStatus), conn))
	{
		goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
	{
		goto send_failed;
	}

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
	{
		goto send_failed;
	}

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
		{
			goto receive_failed;
		}

		if ((res = GTMPQgetResult(conn)) == NULL)
		{
			goto receive_failed;
		}

		/* Check on node type and node name */
		if (res->gr_status == GTM_RESULT_OK)
		{
			Assert(res->gr_resdata.grd_node.type == type);
			Assert((strcmp(res->gr_resdata.grd_node.node_name,node_name) == 0));
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name)
{
	return node_unregister_worker(conn, type, node_name, false);
}

int bkup_node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name)
{
	return node_unregister_worker(conn, type, node_name, true);
}

static int node_unregister_worker(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	if (gtmpqPutMsgStart('C', true, conn) ||
		gtmpqPutInt(is_backup ? MSG_BKUP_NODE_UNREGISTER : MSG_NODE_UNREGISTER, sizeof (GTM_MessageType), conn) ||
		gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), conn) ||
		/* Node name length */
		gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
		/* Node name (var-len) */
		gtmpqPutnchar(node_name, strlen(node_name), conn) )
		goto send_failed;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		/* Check on node type and node name */
		if (res->gr_status == GTM_RESULT_OK)
		{
			Assert(res->gr_resdata.grd_node.type == type);
			Assert( (strcmp(res->gr_resdata.grd_node.node_name, node_name) == 0) );
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}


void
GTM_FreeResult(GTM_Result *result, GTM_PGXCNodeType remote_type)
{
	if (result == NULL)
		return;
	gtmpqFreeResultData(result, remote_type);
	free(result);
}

int
backend_disconnect(GTM_Conn *conn, bool is_postmaster, GTM_PGXCNodeType type, char *node_name)
{
	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn) ||
	    gtmpqPutInt(MSG_BACKEND_DISCONNECT, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutc(is_postmaster, conn))
		goto send_failed;

	/*
	 * Then send node type and node name if backend is a postmaster to
	 * disconnect the correct node.
	 */
	if (is_postmaster)
	{
		if (gtmpqPutnchar((char *)&type, sizeof(GTM_PGXCNodeType), conn) ||
			/* Node name length */
			gtmpqPutInt(strlen(node_name), sizeof (GTM_StrLen), conn) ||
			/* Node name (var-len) */
			gtmpqPutnchar(node_name, strlen(node_name), conn))
			goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	return 1;

send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
begin_transaction_multi(GTM_Conn *conn, int txn_count, GTM_IsolationLevel *txn_isolation_level,
			bool *txn_read_only, GTMProxy_ConnID *txn_connid,
			int *txn_count_out, GlobalTransactionId *gxid_out, GTM_Timestamp *ts_out)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	int i;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
		goto send_failed;

	if (gtmpqPutInt(MSG_TXN_BEGIN_GETGXID_MULTI, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutInt(txn_count, sizeof(int), conn))
		goto send_failed;

	for (i = 0; i < txn_count; i++)
	{
		gtmpqPutInt(txn_isolation_level[i], sizeof(int), conn);
		gtmpqPutc(txn_read_only[i], conn);
		gtmpqPutInt(txn_connid[i], sizeof(int), conn);
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
		memcpy(gxid_out, &res->gr_resdata.grd_txn_get_multi.start_gxid, sizeof(GlobalTransactionId));
		memcpy(ts_out, &res->gr_resdata.grd_txn_get_multi.timestamp, sizeof(GTM_Timestamp));
	}

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}


int
bkup_begin_transaction_multi(GTM_Conn *conn, int txn_count,
							 GTM_TransactionHandle *txn, GlobalTransactionId start_gxid, GTM_IsolationLevel *isolevel,
							 bool *read_only, GTMProxy_ConnID *txn_connid)
{
	int ii;
	GlobalTransactionId gxid = start_gxid;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
		goto send_failed;

	if (gtmpqPutInt(MSG_BKUP_TXN_BEGIN_GETGXID_MULTI, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutInt(txn_count, sizeof(int), conn))
		goto send_failed;

	for (ii = 0; ii < txn_count; ii++, gxid++)
	{
		if (gxid == InvalidGlobalTransactionId)
			gxid = FirstNormalGlobalTransactionId;
		if (gtmpqPutInt(txn[ii], sizeof(GTM_TransactionHandle), conn) ||
			gtmpqPutInt(gxid, sizeof(GlobalTransactionId), conn) ||
			gtmpqPutInt(isolevel[ii], sizeof(GTM_IsolationLevel), conn) ||
			gtmpqPutc(read_only[ii], conn) ||
			gtmpqPutInt(txn_connid[ii], sizeof(GTMProxy_ConnID), conn))
			goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	return 0;

send_failed:
	return -1;
}

int
bkup_commit_transaction_multi(GTM_Conn *conn, int txn_count, GTM_TransactionHandle *txn)
{
	int ii;

	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
		goto send_failed;

	if (gtmpqPutInt(MSG_BKUP_TXN_COMMIT_MULTI, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutInt(txn_count, sizeof(int), conn))
		goto send_failed;

	for (ii = 0; ii < txn_count; ii++)
	{
		if (gtmpqPutc(false, conn) ||
		    gtmpqPutnchar((char *)&txn[ii],
				  sizeof (GTM_TransactionHandle), conn))
			  goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	return GTM_RESULT_OK;

send_failed:
	return -1;
}


int
commit_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
			 int *txn_count_out, int *status_out)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	int i;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
		goto send_failed;

	if (gtmpqPutInt(MSG_TXN_COMMIT_MULTI, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutInt(txn_count, sizeof(int), conn))
		goto send_failed;

	for (i = 0; i < txn_count; i++)
	{
		if (gtmpqPutc(true, conn) ||
		    gtmpqPutnchar((char *)&gxid[i],
				  sizeof (GlobalTransactionId), conn))
			  goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
		memcpy(status_out, &res->gr_resdata.grd_txn_rc_multi.status, sizeof(int) * (*txn_count_out));
	}

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
						int *txn_count_out, int *status_out)
{
	return abort_transaction_multi_internal(conn, txn_count, gxid, txn_count_out, status_out, false);
}

int
bkup_abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid)
{
	int txn_count_out;
	int status_out[GTM_MAX_GLOBAL_TRANSACTIONS];

	return abort_transaction_multi_internal(conn, txn_count, gxid, &txn_count_out, status_out, true);
}

static int
abort_transaction_multi_internal(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
								 int *txn_count_out, int *status_out, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	int i;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
		goto send_failed;

	if (gtmpqPutInt(is_backup ? MSG_BKUP_TXN_ROLLBACK_MULTI : MSG_TXN_ROLLBACK_MULTI, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutInt(txn_count, sizeof(int), conn))
		goto send_failed;

	for (i = 0; i < txn_count; i++)
	{
		if (gtmpqPutc(true, conn) ||
		    gtmpqPutnchar((char *)&gxid[i],
				  sizeof (GlobalTransactionId), conn))
			  goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		if (res->gr_status == GTM_RESULT_OK)
		{
			memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
			memcpy(status_out, &res->gr_resdata.grd_txn_rc_multi.status, sizeof(int) * (*txn_count_out));
		}

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

int
snapshot_get_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
		   int *txn_count_out, int *status_out,
		   GlobalTransactionId *xmin_out, GlobalTransactionId *xmax_out,
		   GlobalTransactionId *recent_global_xmin_out, int32 *xcnt_out)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	int i;

	/* Start the message. */
	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: no proxy header */
		goto send_failed;

	if (gtmpqPutInt(MSG_SNAPSHOT_GET_MULTI, sizeof (GTM_MessageType), conn) ||
	    gtmpqPutInt(txn_count, sizeof(int), conn))
		goto send_failed;

	for (i = 0; i < txn_count; i++)
	{
		if (gtmpqPutc(true, conn) ||
		    gtmpqPutnchar((char *)&gxid[i],
				  sizeof (GlobalTransactionId), conn))
			  goto send_failed;
	}

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	if (res->gr_status == GTM_RESULT_OK)
	{
		memcpy(txn_count_out, &res->gr_resdata.grd_txn_get_multi.txn_count, sizeof(int));
		memcpy(status_out, &res->gr_resdata.grd_txn_rc_multi.status, sizeof(int) * (*txn_count_out));
		memcpy(xmin_out, &res->gr_snapshot.sn_xmin, sizeof(GlobalTransactionId));
		memcpy(xmax_out, &res->gr_snapshot.sn_xmax, sizeof(GlobalTransactionId));
		memcpy(recent_global_xmin_out, &res->gr_snapshot.sn_recent_global_xmin, sizeof(GlobalTransactionId));
		memcpy(xcnt_out, &res->gr_snapshot.sn_xcnt, sizeof(int32));
	}

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * Barrier
 */

int
report_barrier(GTM_Conn *conn, char *barrier_id)
{
	return(report_barrier_internal(conn, barrier_id, false));
}

int
bkup_report_barrier(GTM_Conn *conn, char *barrier_id)
{
	return(report_barrier_internal(conn, barrier_id, true));

}

static int
report_barrier_internal(GTM_Conn *conn, char *barrier_id, bool is_backup)
{
	GTM_Result *res = NULL;
	time_t finish_time;
	int barrier_id_len = strlen(barrier_id) + 1;
	

	/* Send the message */
	if (gtmpqPutMsgStart('C', true, conn)) /* FIXME: not proxy header --> proxy shold handle this separately */
		goto send_failed;
	if (gtmpqPutInt(is_backup ? MSG_BKUP_BARRIER : MSG_BARRIER, sizeof(GTM_MessageType), conn) ||
		gtmpqPutInt(barrier_id_len, sizeof(int), conn) ||
		gtmpqPutnchar(barrier_id, barrier_id_len, conn))
		goto send_failed;
	/* Flush the message */
	if (gtmpqPutMsgEnd(conn))
		goto send_failed;
	/* Flush to ensure backend gets it */
	if (gtmpqFlush(conn))
		goto send_failed;

	/* Handle the response */
	if (!is_backup)
	{
		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
		if (gtmpqWaitTimed(true, false, conn, finish_time) ||
			gtmpqReadData(conn) < 0)
			goto receive_failed;

		if ((res = GTMPQgetResult(conn)) == NULL)
			goto receive_failed;

		return res->gr_status;
	}
	return GTM_RESULT_OK;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}


/*
 * Backup to Standby
 */

int
set_begin_end_backup(GTM_Conn *conn, bool begin)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	if (gtmpqPutMsgStart('C', true, conn))
		goto send_failed;

	if(gtmpqPutInt(begin ? MSG_BEGIN_BACKUP : MSG_END_BACKUP,
				   sizeof(GTM_MessageType), conn))
		goto send_failed;

	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	if (gtmpqFlush(conn))
		goto send_failed;

	finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}

/*
 * Sync with standby
 */
int
gtm_sync_standby(GTM_Conn *conn)
{
	GTM_Result *res = NULL;
	time_t finish_time;

	if (gtmpqPutMsgStart('C', true, conn))
		goto send_failed;

	if (gtmpqPutInt(MSG_SYNC_STANDBY, sizeof(GTM_MessageType), conn))
		goto send_failed;

	if (gtmpqPutMsgEnd(conn))
		goto send_failed;

	if (gtmpqFlush(conn))
		goto send_failed;

		finish_time = time(NULL) + CLIENT_GTM_TIMEOUT;
	if (gtmpqWaitTimed(true, false, conn, finish_time) ||
		gtmpqReadData(conn) < 0)
		goto receive_failed;

	if ((res = GTMPQgetResult(conn)) == NULL)
		goto receive_failed;

	return res->gr_status;

receive_failed:
send_failed:
	conn->result = makeEmptyResultIfIsNull(conn->result);
	conn->result->gr_status = GTM_RESULT_COMM_ERROR;
	return -1;
}
