/*-------------------------------------------------------------------------
 *
 * gtm_client.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_CLIENT_H
#define GTM_CLIENT_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_msg.h"
#include "gtm/register.h"
#include "gtm/libpq-fe.h"

typedef union GTM_ResultData
{
	GTM_TransactionHandle		grd_txnhandle;	/* TXN_BEGIN */

	struct
	{
		GlobalTransactionId		gxid;
		GTM_Timestamp			timestamp;
	} grd_gxid_tp;								/* TXN_BEGIN_GETGXID */

	GlobalTransactionId			grd_gxid;		/* TXN_PREPARE
												 * TXN_START_PREPARED
												 * TXN_COMMIT
												 * TXN_COMMIT_PREPARED
												 * TXN_ROLLBACK
												 */

	GlobalTransactionId			grd_next_gxid;

	struct
	{
		GTM_TransactionHandle	txnhandle;
		GlobalTransactionId		gxid;
	} grd_txn;									/* TXN_GET_GXID */

	GTM_SequenceKeyData			grd_seqkey;		/* SEQUENCE_INIT
												 * SEQUENCE_RESET
												 * SEQUENCE_CLOSE */
	struct
	{
		GTM_SequenceKeyData		seqkey;
		GTM_Sequence			seqval;
	} grd_seq;									/* SEQUENCE_GET_NEXT */

	struct
	{
		int						seq_count;
		GTM_SeqInfo			   *seq;
	} grd_seq_list;								/* SEQUENCE_GET_LIST */

	struct
	{
		int				txn_count; 				/* TXN_BEGIN_GETGXID_MULTI */
		GlobalTransactionId		start_gxid;
		GTM_Timestamp			timestamp;
	} grd_txn_get_multi;

	struct
	{
		int				txn_count;				/* TXN_COMMIT_MULTI */
		int				status[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_txn_rc_multi;

	struct
	{
		GTM_TransactionHandle	txnhandle;		/* SNAPSHOT_GXID_GET */
		GlobalTransactionId		gxid;			/* SNAPSHOT_GET */
		int						txn_count;		/* SNAPSHOT_GET_MULTI */
		int						status[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_txn_snap_multi;

	struct
	{
		GlobalTransactionId		gxid;
		GlobalTransactionId		prepared_gxid;
		int				nodelen;
		char			*nodestring;
	} grd_txn_get_gid_data;					/* TXN_GET_GID_DATA_RESULT */

	struct
	{
		char				*ptr;
		size_t				len;
	} grd_txn_gid_list;						/* TXN_GXID_LIST_RESULT */

	struct
	{
		GTM_PGXCNodeType	type;			/* NODE_REGISTER */
		size_t				len;
		char				*node_name;		/* NODE_UNREGISTER */
	} grd_node;

	struct
	{
		int				num_node;
		GTM_PGXCNodeInfo		*nodeinfo[MAX_NODES];
	} grd_node_list;

	/*
	 * TODO
	 * 	TXN_GET_STATUS
	 * 	TXN_GET_ALL_PREPARED
	 */
} GTM_ResultData;

#define GTM_RESULT_COMM_ERROR (-2) /* Communication error */
#define GTM_RESULT_ERROR      (-1)
#define GTM_RESULT_OK         (0)
/*
 * This error is used ion the case where allocated buffer is not large
 * enough to store the errors. It may happen of an allocation failed
 * so it's status is considered as unknown.
 */
#define GTM_RESULT_UNKNOWN    (1)

typedef struct GTM_Result
{
	GTM_ResultType		gr_type;
	int					gr_msglen;
	int					gr_status;
	GTM_ProxyMsgHeader	gr_proxyhdr;
	GTM_ResultData		gr_resdata;
	/*
	 * We keep these two items outside the union to avoid repeated malloc/free
	 * of the xip array. If these items are pushed inside the union, they may
	 * get overwritten by other members in the union
	 */
	int					gr_xip_size;
	GTM_SnapshotData	gr_snapshot;

	/*
	 * Similarly, keep the buffer for proxying data outside the union
	 */
	char		*gr_proxy_data;
	int			gr_proxy_datalen;
} GTM_Result;

/*
 * Connection Management API
 */
GTM_Conn *connect_gtm(const char *connect_string);
void disconnect_gtm(GTM_Conn *conn);

int begin_replication_initial_sync(GTM_Conn *);
int end_replication_initial_sync(GTM_Conn *);

size_t get_node_list(GTM_Conn *, GTM_PGXCNodeInfo *, size_t);
GlobalTransactionId get_next_gxid(GTM_Conn *);
uint32 get_txn_gxid_list(GTM_Conn *, GTM_Transactions *);
size_t get_sequence_list(GTM_Conn *, GTM_SeqInfo **);

/*
 * Transaction Management API
 */
GlobalTransactionId begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel, GTM_Timestamp *timestamp);
int bkup_begin_transaction(GTM_Conn *conn, GTM_TransactionHandle txn, GTM_IsolationLevel isolevel,
						   bool read_only, GTM_Timestamp timestamp);
int bkup_begin_transaction_gxid(GTM_Conn *conn, GTM_TransactionHandle txn, GlobalTransactionId gxid,
								GTM_IsolationLevel isolevel, bool read_only, GTM_Timestamp timestamp);

GlobalTransactionId begin_transaction_autovacuum(GTM_Conn *conn, GTM_IsolationLevel isolevel);
int bkup_begin_transaction_autovacuum(GTM_Conn *conn, GTM_TransactionHandle txn, GlobalTransactionId gxid,
									  GTM_IsolationLevel isolevel);
int commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int bkup_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid);
int bkup_commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid);
int abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int bkup_abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int start_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
							   char *nodestring);
int backup_start_prepared_transaction(GTM_Conn *conn, GTM_TransactionHandle txn, char *gid,
									  char *nodestring);
int prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int bkup_prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int get_gid_data(GTM_Conn *conn, GTM_IsolationLevel isolevel, char *gid,
				 GlobalTransactionId *gxid,
				 GlobalTransactionId *prepared_gxid,
				 char **nodestring);

/*
 * Multiple Transaction Management API
 */
int
begin_transaction_multi(GTM_Conn *conn, int txn_count, GTM_IsolationLevel *txn_isolation_level,
			bool *txn_read_only, GTMProxy_ConnID *txn_connid,
			int *txn_count_out, GlobalTransactionId *gxid_out, GTM_Timestamp *ts_out);
int
bkup_begin_transaction_multi(GTM_Conn *conn, int txn_count,
							 GTM_TransactionHandle *txn, GlobalTransactionId start_gxid, GTM_IsolationLevel *isolevel,
							 bool *read_only, GTMProxy_ConnID *txn_connid);
int
commit_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
						 int *txn_count_out, int *status_out);
int
bkup_commit_transaction_multi(GTM_Conn *conn, int txn_count, GTM_TransactionHandle *txn);
int
abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
			int *txn_count_out, int *status_out);
int
bkup_abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid);
int
snapshot_get_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
		   int *txn_count_out, int *status_out,
		   GlobalTransactionId *xmin_out, GlobalTransactionId *xmax_out,
		   GlobalTransactionId *recent_global_xmin_out, int32 *xcnt_out);

/*
 * Snapshot Management API
 */
GTM_SnapshotData *get_snapshot(GTM_Conn *conn, GlobalTransactionId gxid,
		bool canbe_grouped);

/*
 * Node Registering management API
 */
int node_register(GTM_Conn *conn,
				  GTM_PGXCNodeType type,
				  GTM_PGXCNodePort port,
				  char *node_name,
				  char *datafolder);
int bkup_node_register(GTM_Conn *conn,
					   GTM_PGXCNodeType type,
					   GTM_PGXCNodePort port,
					   char *node_name,
					   char *datafolder);
int node_register(GTM_Conn *conn, GTM_PGXCNodeType type, GTM_PGXCNodePort port,	char *node_name, char *datafolder);
int node_register_internal(GTM_Conn *conn, GTM_PGXCNodeType type, const char *host,	GTM_PGXCNodePort port, char *node_name,
						   char *datafolder, GTM_PGXCNodeStatus status);
int bkup_node_register(GTM_Conn *conn, GTM_PGXCNodeType type, GTM_PGXCNodePort port, char *node_name, char *datafolder);
int bkup_node_register_internal(GTM_Conn *conn, GTM_PGXCNodeType type, const char *host, GTM_PGXCNodePort port,
								char *node_name, char *datafolder, GTM_PGXCNodeStatus status);

int node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char *node_name);
int bkup_node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name);
int backend_disconnect(GTM_Conn *conn, bool is_postmaster, GTM_PGXCNodeType type, char *node_name);
char *node_get_local_addr(GTM_Conn *conn, char *buf, size_t buflen, int *rc);

/*
 * Sequence Management API
 */
int open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				  GTM_Sequence minval, GTM_Sequence maxval,
				  GTM_Sequence startval, bool cycle);
int bkup_open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
					   GTM_Sequence minval, GTM_Sequence maxval,
					   GTM_Sequence startval, bool cycle);
int alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				   GTM_Sequence minval, GTM_Sequence maxval,
				   GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart);
int bkup_alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
						GTM_Sequence minval, GTM_Sequence maxval,
						GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart);
int close_sequence(GTM_Conn *conn, GTM_SequenceKey key);
int bkup_close_sequence(GTM_Conn *conn, GTM_SequenceKey key);
int rename_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey);
int bkup_rename_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey);
int get_next(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence *result);
int bkup_get_next(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence *result);
int set_val(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool is_called);
int bkup_set_val(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence nextval, bool is_called);
int reset_sequence(GTM_Conn *conn, GTM_SequenceKey key);
int bkup_reset_sequence(GTM_Conn *conn, GTM_SequenceKey key);

/*
 * Barrier
 */
int report_barrier(GTM_Conn *conn, char *barier_id);
int bkup_report_barrier(GTM_Conn *conn, char *barrier_id);

/*
 * GTM-Standby
 */
int set_begin_end_backup(GTM_Conn *conn, bool begin);
int gtm_sync_standby(GTM_Conn *conn);


#endif
