/*-------------------------------------------------------------------------
 *
 * pgxcnode.h
 *
 *	  Utility functions to communicate to Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxcnode.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCNODE_H
#define PGXCNODE_H
#include "postgres.h"
#include "gtm/gtm_c.h"
#include "utils/timestamp.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include <unistd.h>

#define NO_SOCKET -1

#define PGXCNODE_CANCEL_DELAY 10	/* Cancel delay duration in millisecond */

/* Connection to Datanode maintained by Pool Manager */
typedef struct PGconn NODE_CONNECTION;
typedef struct PGcancel NODE_CANCEL;

/* Helper structure to access Datanode from Session */
typedef enum
{
	DN_CONNECTION_STATE_IDLE,			/* idle, ready for query */
	DN_CONNECTION_STATE_QUERY,			/* query is sent, response expected */
	DN_CONNECTION_STATE_ERROR_FATAL,	/* fatal error */
	DN_CONNECTION_STATE_COPY_IN,
	DN_CONNECTION_STATE_COPY_OUT
}	DNConnectionState;

typedef enum
{
	HANDLE_IDLE,
	HANDLE_ERROR,
	HANDLE_DEFAULT
}	PGXCNode_HandleRequested;

/*
 * Enumeration for two purposes
 * 1. To indicate to the HandleCommandComplete function whether response checking is required or not
 * 2. To enable HandleCommandComplete function to indicate whether the response was a ROLLBACK or not
 * Response checking is required in case of PREPARE TRANSACTION and should not be done for the rest
 * of the cases for performance reasons, hence we have an option to ignore response checking.
 * The problem with PREPARE TRANSACTION is that it can result in a ROLLBACK response
 * yet Coordinator would think it got done on all nodes.
 * If we ignore ROLLBACK response then we would try to COMMIT a transaction that
 * never got prepared, which in an incorrect behavior.
 */
typedef enum
{
	RESP_ROLLBACK_IGNORE,			/* Ignore response checking */
	RESP_ROLLBACK_CHECK,			/* Check whether response was ROLLBACK */
	RESP_ROLLBACK_RECEIVED,			/* Response is ROLLBACK */
	RESP_ROLLBACK_NOT_RECEIVED		/* Response is NOT ROLLBACK */
}RESP_ROLLBACK;


#define DN_CONNECTION_STATE_ERROR(dnconn) \
		((dnconn)->state == DN_CONNECTION_STATE_ERROR_FATAL \
			|| (dnconn)->transaction_status == 'E')

#define HAS_MESSAGE_BUFFERED(conn) \
		((conn)->inCursor + 4 < (conn)->inEnd \
			&& (conn)->inCursor + ntohl(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1))) < (conn)->inEnd)

struct pgxc_node_handle
{
	Oid			nodeoid;

	/* fd of the connection */
	int		sock;
	/* Connection state */
	char		transaction_status;
	DNConnectionState state;
	struct RemoteQueryState *combiner;
#ifdef DN_CONNECTION_DEBUG
	bool		have_row_desc;
#endif
	char		*error;
	/* Output buffer */
	char		*outBuffer;
	size_t		outSize;
	size_t		outEnd;
	/* Input buffer */
	char		*inBuffer;
	size_t		inSize;
	size_t		inStart;
	size_t		inEnd;
	size_t		inCursor;

	/*
	 * Have a variable to enable/disable response checking and
	 * if enable then read the result of response checking
	 *
	 * For details see comments of RESP_ROLLBACK
	 */
	RESP_ROLLBACK	ck_resp_rollback;
};
typedef struct pgxc_node_handle PGXCNodeHandle;

/* Structure used to get all the handles involved in a transaction */
typedef struct
{
	PGXCNodeHandle	   *primary_handle;	/* Primary connection to PGXC node */
	int					dn_conn_count;	/* number of Datanode Handles including primary handle */
	PGXCNodeHandle	  **datanode_handles;	/* an array of Datanode handles */
	int					co_conn_count;	/* number of Coordinator handles */
	PGXCNodeHandle	  **coord_handles;	/* an array of Coordinator handles */
} PGXCNodeAllHandles;

extern void InitMultinodeExecutor(bool is_force);

/* Open/close connection routines (invoked from Pool Manager) */
extern char *PGXCNodeConnStr(char *host, int port, char *dbname, char *user,
	                         char *pgoptions, char *remote_type);
extern NODE_CONNECTION *PGXCNodeConnect(char *connstr);
extern int PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command);
extern void PGXCNodeClose(NODE_CONNECTION * conn);
extern int PGXCNodeConnected(NODE_CONNECTION * conn);
extern int PGXCNodeConnClean(NODE_CONNECTION * conn);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);

/* Look at information cached in node handles */
extern int PGXCNodeGetNodeId(Oid nodeoid, char node_type);
extern Oid PGXCNodeGetNodeOid(int nodeid, char node_type);
extern int PGXCNodeGetNodeIdFromName(char *node_name, char node_type);

extern PGXCNodeAllHandles *get_handles(List *datanodelist, List *coordlist, bool is_query_coord_only);
extern void pfree_pgxc_all_handles(PGXCNodeAllHandles *handles);

extern void release_handles(void);
extern void cancel_query(void);
extern void clear_all_data(void);


extern int get_transaction_nodes(PGXCNodeHandle ** connections,
								  char client_conn_type,
								  PGXCNode_HandleRequested type_requested);
extern char* collect_pgxcnode_names(char *nodestring, int conn_count, PGXCNodeHandle ** connections, char client_conn_type);
extern char* collect_localnode_name(char *nodestring);
extern int	get_active_nodes(PGXCNodeHandle ** connections);

extern int	ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);
extern int	ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);

extern int	pgxc_node_send_query(PGXCNodeHandle * handle, const char *query);
extern int	pgxc_node_send_describe(PGXCNodeHandle * handle, bool is_statement,
						const char *name);
extern int	pgxc_node_send_execute(PGXCNodeHandle * handle, const char *portal, int fetch);
extern int	pgxc_node_send_close(PGXCNodeHandle * handle, bool is_statement,
					 const char *name);
extern int	pgxc_node_send_sync(PGXCNodeHandle * handle);
extern int	pgxc_node_send_bind(PGXCNodeHandle * handle, const char *portal,
								const char *statement, int paramlen, char *params);
extern int	pgxc_node_send_parse(PGXCNodeHandle * handle, const char* statement,
								 const char *query, short num_params, Oid *param_types);
extern int	pgxc_node_send_flush(PGXCNodeHandle * handle);
extern int	pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query,
							  const char *statement, const char *portal,
							  int num_params, Oid *param_types,
							  int paramlen, char *params,
							  bool send_describe, int fetch_size);
extern int	pgxc_node_send_gxid(PGXCNodeHandle * handle, GlobalTransactionId gxid);
extern int	pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid);
extern int	pgxc_node_send_snapshot(PGXCNodeHandle * handle, Snapshot snapshot);
extern int	pgxc_node_send_timestamp(PGXCNodeHandle * handle, TimestampTz timestamp);

extern bool	pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout);
extern int	pgxc_node_read_data(PGXCNodeHandle * conn, bool close_if_error);
extern int	pgxc_node_is_data_enqueued(PGXCNodeHandle *conn);

extern int	send_some(PGXCNodeHandle * handle, int len);
extern int	pgxc_node_flush(PGXCNodeHandle *handle);
extern void	pgxc_node_flush_read(PGXCNodeHandle *handle);

extern int	pgxc_all_handles_send_gxid(PGXCNodeAllHandles *pgxc_handles, GlobalTransactionId gxid, bool stop_at_error);
extern int	pgxc_all_handles_send_query(PGXCNodeAllHandles *pgxc_handles, const char *buffer, bool stop_at_error);

extern char get_message(PGXCNodeHandle *conn, int *len, char **msg);

extern void add_error_message(PGXCNodeHandle * handle, const char *message);

extern Datum pgxc_execute_on_nodes(int numnodes, Oid *nodelist, char *query);

#endif /* PGXCNODE_H */
