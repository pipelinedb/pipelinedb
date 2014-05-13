/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execRemote.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/xc_maintenance_mode.h"

/* Enforce the use of two-phase commit when temporary objects are used */
bool EnforceTwoPhaseCommit = true;

/*
 * non-FQS UPDATE & DELETE to a replicated table without any primary key or
 * unique key should be prohibited (true) or allowed (false)
 */
bool RequirePKeyForRepTab = true;

/*
 * Max to begin flushing data to datanodes, max to stop flushing data to datanodes.
 */
#define MAX_SIZE_TO_FORCE_FLUSH (2^10 * 64 * 2)
#define MAX_SIZE_TO_STOP_FLUSH (2^10 * 64)

#define END_QUERY_TIMEOUT	20
#define ROLLBACK_RESP_LEN	9

typedef enum RemoteXactNodeStatus
{
	RXACT_NODE_NONE,					/* Initial state */
	RXACT_NODE_PREPARE_SENT,			/* PREPARE request sent */
	RXACT_NODE_PREPARE_FAILED,		/* PREPARE failed on the node */
	RXACT_NODE_PREPARED,				/* PREPARED successfully on the node */
	RXACT_NODE_COMMIT_SENT,			/* COMMIT sent successfully */
	RXACT_NODE_COMMIT_FAILED,		/* failed to COMMIT on the node */
	RXACT_NODE_COMMITTED,			/* COMMITTed successfully on the node */
	RXACT_NODE_ABORT_SENT,			/* ABORT sent successfully */
	RXACT_NODE_ABORT_FAILED,			/* failed to ABORT on the node */
	RXACT_NODE_ABORTED				/* ABORTed successfully on the node */
} RemoteXactNodeStatus;

typedef enum RemoteXactStatus
{
	RXACT_NONE,				/* Initial state */
	RXACT_PREPARE_FAILED,	/* PREPARE failed */
	RXACT_PREPARED,			/* PREPARED succeeded on all nodes */
	RXACT_COMMIT_FAILED,		/* COMMIT failed on all the nodes */
	RXACT_PART_COMMITTED,	/* COMMIT failed on some and succeeded on other nodes */
	RXACT_COMMITTED,			/* COMMIT succeeded on all the nodes */
	RXACT_ABORT_FAILED,		/* ABORT failed on all the nodes */
	RXACT_PART_ABORTED,		/* ABORT failed on some and succeeded on other nodes */
	RXACT_ABORTED			/* ABORT succeeded on all the nodes */
} RemoteXactStatus;

typedef struct RemoteXactState
{
	/* Current status of the remote 2PC */
	RemoteXactStatus		status;

	/*
	 * Information about all the nodes involved in the transaction. We track
	 * the number of writers and readers. The first numWriteRemoteNodes entries
	 * in the remoteNodeHandles and remoteNodeStatus correspond to the writer
	 * connections and rest correspond to the reader connections.
	 */
	int 					numWriteRemoteNodes;
	int 					numReadRemoteNodes;
	int						maxRemoteNodes;
	PGXCNodeHandle			**remoteNodeHandles;
	RemoteXactNodeStatus	*remoteNodeStatus;

	GlobalTransactionId		commitXid;

	bool					preparedLocalNode;

	char					prepareGID[256]; /* GID used for internal 2PC */
} RemoteXactState;

static RemoteXactState remoteXactState;

#ifdef PGXC
typedef struct
{
	xact_callback function;
	void *fparams;
} abort_callback_type;
#endif

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024


/*
 * List of PGXCNodeHandle to track readers and writers involved in the
 * current transaction
 */
static List *XactWriteNodes;
static List *XactReadNodes;
static char *preparedNodes;

/*
 * Flag to track if a temporary object is accessed by the current transaction
 */
static bool temp_object_included = false;

#ifdef PGXC
static abort_callback_type dbcleanup_info = { NULL, NULL };
#endif

static int	pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid, bool need_tran_block,
				bool readOnly, char node_type);
static PGXCNodeAllHandles * get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type);

static void close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor);

static int pgxc_get_transaction_nodes(PGXCNodeHandle *connections[], int size, bool writeOnly);
static int pgxc_get_connections(PGXCNodeHandle *connections[], int size, List *connlist);

static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
					RemoteQueryState *remotestate, Snapshot snapshot);
static TupleTableSlot * RemoteQueryNext(ScanState *node);
static bool RemoteQueryRecheck(RemoteQueryState *node, TupleTableSlot *slot);


static char *generate_begin_command(void);
static bool pgxc_node_remote_prepare(char *prepareGID);
static void pgxc_node_remote_commit(void);
static void pgxc_node_remote_abort(void);
static char *pgxc_node_get_nodelist(bool localNode);

static void ExecClearTempObjectIncluded(void);
static void init_RemoteXactState(bool preparedLocalNode);
static void clear_RemoteXactState(void);
static void pgxc_node_report_error(RemoteQueryState *combiner);
static bool IsReturningDMLOnReplicatedTable(RemoteQuery *rq);
static void SetDataRowForIntParams(JunkFilter *junkfilter,
					   TupleTableSlot *sourceSlot, TupleTableSlot *newSlot,
					   RemoteQueryState *rq_state);
static void pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype);
static void pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno, Oid valtype, StringInfo buf);
static void pgxc_rq_fire_bstriggers(RemoteQueryState *node);
static void pgxc_rq_fire_astriggers(RemoteQueryState *node);

static int flushPGXCNodeHandleData(PGXCNodeHandle *handle);

/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
static RemoteQueryState *
CreateResponseCombiner(int node_count, CombineType combine_type)
{
	RemoteQueryState *combiner;

	/* ResponseComber is a typedef for pointer to ResponseCombinerData */
	combiner = makeNode(RemoteQueryState);
	if (combiner == NULL)
	{
		/* Out of memory */
		return combiner;
	}

	combiner->node_count = node_count;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->combine_type = combine_type;
	combiner->command_complete_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->tuple_desc = NULL;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->query_Done = false;
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->remoteCopyType = REMOTE_COPY_NONE;
	combiner->copy_file = NULL;
	combiner->rqs_cmd_id = FirstCommandId;
	combiner->rqs_processed = 0;

	return combiner;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		}
		else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
static TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
	TupleDesc 	result;
	int 		i, nattr;
	uint16		n16;

	/* get number of attributes */
	memcpy(&n16, msg_body, 2);
	nattr = ntohs(n16);
	msg_body += 2;

	result = CreateTemplateTupleDesc(nattr, false);

	/* decode attributes */
	for (i = 1; i <= nattr; i++)
	{
		AttrNumber	attnum;
		char		*attname;
		char		*typname;
		Oid 		oidtypeid;
		int32 		typemode, typmod;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* type name */
		typname = msg_body;
		msg_body += strlen(typname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type OID, ignored */
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&typemode, msg_body, 4);
		typmod = ntohl(typemode);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		/* Get the OID type and mode type from typename */
		parseTypeString(typname, &oidtypeid, NULL);

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a Datanode connection
 */
static void
HandleCopyOutComplete(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'c' message, current request type %d", combiner->request_type)));
	/* Just do nothing, close message is managed by the Coordinator */
	combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void
HandleCommandComplete(RemoteQueryState *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{
	int 			digits = 0;
	bool			non_fqs_dml;

	/* Is this a DML query that is not FQSed ? */
	non_fqs_dml = (combiner->ss.ps.plan &&
					((RemoteQuery*)combiner->ss.ps.plan)->rq_params_internal);
	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	/* Extract rowcount */
	if (combiner->combine_type != COMBINE_TYPE_NONE)
	{
		uint64	rowcount;
		digits = parse_row_count(msg_body, len, &rowcount);
		if (digits > 0)
		{
			/*
			 * PGXC TODO: Need to completely remove the dependency on whether
			 * it's an FQS or non-FQS DML query. For this, command_complete_count
			 * needs to be better handled. Currently this field is being updated
			 * for each iteration of FetchTuple by re-using the same combiner
			 * for each iteration, whereas it seems it should be updated only
			 * for each node execution, not for each tuple fetched.
			 */

			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->command_complete_count)
				{
					/* For FQS, check if there is a consistency issue with replicated table. */
					if (rowcount != combiner->rqs_processed && !non_fqs_dml)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned"
										"different results from the Datanodes")));
				}
				/* Always update the row count. We have initialized it to 0 */
				combiner->rqs_processed = rowcount;
			}
			else
				combiner->rqs_processed += rowcount;

			/*
			 * This rowcount will be used to increment estate->es_processed
			 * either in ExecInsert/Update/Delete for non-FQS query, or will
			 * used in RemoteQueryNext() for FQS query.
			 */
		}
		else
			combiner->combine_type = COMBINE_TYPE_NONE;
	}

	/* If response checking is enable only then do further processing */

	if (conn->ck_resp_rollback == RESP_ROLLBACK_CHECK)
	{
		conn->ck_resp_rollback = RESP_ROLLBACK_NOT_RECEIVED;
		if (len == ROLLBACK_RESP_LEN)	/* No need to do string comparison otherwise */
		{
			if (strcmp(msg_body, "ROLLBACK") == 0)
				conn->ck_resp_rollback = RESP_ROLLBACK_RECEIVED;
		}
	}

	combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool
HandleRowDescription(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'T' message, current request type %d", combiner->request_type)));
	}
	/* Increment counter and check if it was first */
	if (combiner->description_count++ == 0)
	{
		combiner->tuple_desc = create_tuple_desc(msg_body, len);
		return true;
	}
	return false;
}


#ifdef NOT_USED
/*
 * Handle ParameterStatus ('S') message from a Datanode connection (SET command)
 */
static void
HandleParameterStatus(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'S' message, current request type %d", combiner->request_type)));
	}
	/* Proxy last */
	if (++combiner->description_count == combiner->node_count)
	{
		pq_putmessage('S', msg_body, len);
	}
}
#endif

/*
 * Handle CopyInResponse ('G') message from a Datanode connection
 */
static void
HandleCopyIn(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_IN;
	if (combiner->request_type != REQUEST_TYPE_COPY_IN)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'G' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an G message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a Datanode connection
 */
static void
HandleCopyOut(RemoteQueryState *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'H' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an H message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a Datanode connection
 */
static void
HandleCopyDataRow(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	/* Inconsistent responses */
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'd' message, current request type %d", combiner->request_type)));

	/* count the row */
	combiner->processed++;

	/* Output remote COPY operation to correct location */
	switch (combiner->remoteCopyType)
	{
		case REMOTE_COPY_FILE:
			/* Write data directly to file */
			fwrite(msg_body, 1, len, combiner->copy_file);
			break;
		case REMOTE_COPY_STDOUT:
			/* Send back data to client */
			pq_putmessage('d', msg_body, len);
			break;
		case REMOTE_COPY_TUPLESTORE:
			{
				Datum  *values;
				bool   *nulls;
				TupleDesc   tupdesc = combiner->tuple_desc;
				int i, dropped;
				Form_pg_attribute *attr = tupdesc->attrs;
				FmgrInfo *in_functions;
				Oid *typioparams;
				char **fields;

				values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
				nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
				in_functions = (FmgrInfo *) palloc(tupdesc->natts * sizeof(FmgrInfo));
				typioparams = (Oid *) palloc(tupdesc->natts * sizeof(Oid));

				/* Calculate the Oids of input functions */
				for (i = 0; i < tupdesc->natts; i++)
				{
					Oid         in_func_oid;

					/* Do not need any information for dropped attributes */
					if (attr[i]->attisdropped)
						continue;

					getTypeInputInfo(attr[i]->atttypid,
									 &in_func_oid, &typioparams[i]);
					fmgr_info(in_func_oid, &in_functions[i]);
				}

				/*
				 * Convert message into an array of fields.
				 * Last \n is not included in converted message.
				 */
				fields = CopyOps_RawDataToArrayField(tupdesc, msg_body, len - 1);

				/* Fill in the array values */
				dropped = 0;
				for (i = 0; i < tupdesc->natts; i++)
				{
					char	*string = fields[i - dropped];
					/* Do not need any information for dropped attributes */
					if (attr[i]->attisdropped)
					{
						dropped++;
						nulls[i] = true; /* Consider dropped parameter as NULL */
						continue;
					}

					/* Find value */
					values[i] = InputFunctionCall(&in_functions[i],
												  string,
												  typioparams[i],
												  attr[i]->atttypmod);
					/* Setup value with NULL flag if necessary */
					if (string == NULL)
						nulls[i] = true;
					else
						nulls[i] = false;
				}

				/* Then insert the values into tuplestore */
				tuplestore_putvalues(combiner->tuplestorestate,
									 combiner->tuple_desc,
									 values,
									 nulls);

				/* Clean up everything */
				if (*fields)
					pfree(*fields);
				pfree(fields);
				pfree(values);
				pfree(nulls);
				pfree(in_functions);
				pfree(typioparams);
			}
			break;
		case REMOTE_COPY_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if buffer can accept more data rows.
 * Caller must stop reading if function returns false
 */
static void
HandleDataRow(RemoteQueryState *combiner, char *msg_body, size_t len, Oid nodeoid)
{
	/* We expect previous message is consumed */
	Assert(combiner->currentRow.msg == NULL);

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'D' message, current request type %d", combiner->request_type)));
	}

	/*
	 * If we got an error already ignore incoming data rows from other nodes
	 * Still we want to continue reading until get CommandComplete
	 */
	if (combiner->errorMessage)
		return;

	/*
	 * We are copying message because it points into connection buffer, and
	 * will be overwritten on next socket read
	 */
	combiner->currentRow.msg = (char *) palloc(len);
	memcpy(combiner->currentRow.msg, msg_body, len);
	combiner->currentRow.msglen = len;
	combiner->currentRow.msgnode = nodeoid;
}

/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static void
HandleError(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	/* parse error message */
	char *code = NULL;
	char *message = NULL;
	char *detail = NULL;
	int   offset = 0;

	/*
	 * Scan until point to terminating \0
	 */
	while (offset + 1 < len)
	{
		/* pointer to the field message */
		char *str = msg_body + offset + 1;

		switch (msg_body[offset])
		{
			case 'C':	/* code */
				code = str;
				break;
			case 'M':	/* message */
				message = str;
				break;
			case 'D':	/* details */
				detail = str;
				break;

			/* Fields not yet in use */
			case 'S':	/* severity */
			case 'R':	/* routine */
			case 'H':	/* hint */
			case 'P':	/* position string */
			case 'p':	/* position int */
			case 'q':	/* int query */
			case 'W':	/* where */
			case 'F':	/* file */
			case 'L':	/* line */
			default:
				break;
		}

		/* code, message and \0 */
		offset += strlen(str) + 2;
	}

	/*
	 * We may have special handling for some errors, default handling is to
	 * throw out error with the same message. We can not ereport immediately
	 * because we should read from this and other connections until
	 * ReadyForQuery is received, so we just store the error message.
	 * If multiple connections return errors only first one is reported.
	 */
	if (!combiner->errorMessage)
	{
		combiner->errorMessage = pstrdup(message);
		/* Error Code is exactly 5 significant bytes */
		if (code)
			memcpy(combiner->errorCode, code, 5);
	}

	if (!combiner->errorDetail && detail != NULL)
	{
		combiner->errorDetail = pstrdup(detail);
	}

	/*
	 * If Datanode have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_complete_count++;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters:
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine,
						const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;

	if (msg_body == NULL)
		return;

	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;

		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}

}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void
HandleDatanodeCommandId(RemoteQueryState *combiner, char *msg_body, size_t len)
{
	uint32		n32;
	CommandId	cid;

	Assert(msg_body != NULL);
	Assert(len >= 2);

	/* Get the command Id */
	memcpy(&n32, &msg_body[0], 4);
	cid = ntohl(n32);

	/* If received command Id is higher than current one, set it to a new value */
	if (cid > GetReceivedCommandId())
		SetReceivedCommandId(cid);
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
static bool
validate_combiner(RemoteQueryState *combiner)
{
	/* There was error message while combining */
	if (combiner->errorMessage)
		return false;
	/* Check if state is defined */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		return false;

	/* Check all nodes completed */
	if ((combiner->request_type == REQUEST_TYPE_COMMAND
	        || combiner->request_type == REQUEST_TYPE_QUERY)
	        && combiner->command_complete_count != combiner->node_count)
		return false;

	/* Check count of description responses */
	if (combiner->request_type == REQUEST_TYPE_QUERY
	        && combiner->description_count != combiner->node_count)
		return false;

	/* Check count of copy-in responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_IN
	        && combiner->copy_in_count != combiner->node_count)
		return false;

	/* Check count of copy-out responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_OUT
	        && combiner->copy_out_count != combiner->node_count)
		return false;

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
static void
CloseCombiner(RemoteQueryState *combiner)
{
	if (combiner)
	{
		if (combiner->connections)
			pfree(combiner->connections);
		if (combiner->tuple_desc)
		{
			/*
			 * In the case of a remote COPY with tuplestore, combiner is not
			 * responsible from freeing the tuple store. This is done at an upper
			 * level once data redistribution is completed.
			 */
			if (combiner->remoteCopyType != REMOTE_COPY_TUPLESTORE)
				FreeTupleDesc(combiner->tuple_desc);
		}
		if (combiner->errorMessage)
			pfree(combiner->errorMessage);
		if (combiner->errorDetail)
			pfree(combiner->errorDetail);
		if (combiner->cursor_connections)
			pfree(combiner->cursor_connections);
		if (combiner->tapenodes)
			pfree(combiner->tapenodes);
		pfree(combiner);
	}
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
static bool
ValidateAndCloseCombiner(RemoteQueryState *combiner)
{
	bool		valid = validate_combiner(combiner);

	CloseCombiner(combiner);

	return valid;
}

/*
 * It is possible if multiple steps share the same Datanode connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all Datanode responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn)
{
	RemoteQueryState *combiner = conn->combiner;
	MemoryContext oldcontext;

	if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
		return;

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ss_ScanTupleSlot->tts_mcxt);

	/* Verify the connection is in use by the combiner */
	combiner->current_conn = 0;
	while (combiner->current_conn < combiner->conn_count)
	{
		if (combiner->connections[combiner->current_conn] == conn)
			break;
		combiner->current_conn++;
	}
	Assert(combiner->current_conn < combiner->conn_count);

	/*
	 * Buffer data rows until Datanode return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (conn->state == DN_CONNECTION_STATE_QUERY)
	{
		int res;

		/* Move to buffer currentRow (received from the Datanode) */
		if (combiner->currentRow.msg)
		{
			RemoteDataRow dataRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData));
			*dataRow = combiner->currentRow;
			combiner->currentRow.msg = NULL;
			combiner->currentRow.msglen = 0;
			combiner->currentRow.msgnode = 0;
			combiner->rowBuffer = lappend(combiner->rowBuffer, dataRow);
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */
		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
			{
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				add_error_message(conn, "Failed to fetch from Datanode");
			}
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/* Remove current connection, move last in-place, adjust current_conn */
			if (combiner->current_conn < --combiner->conn_count)
				combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
			else
				combiner->current_conn = 0;
		}
		/*
		 * Before output RESPONSE_COMPLETE or PORTAL_SUSPENDED handle_response()
		 * changes connection state to DN_CONNECTION_STATE_IDLE, breaking the
		 * loop. We do not need to do anything specific in case of
		 * PORTAL_SUSPENDED so skiping "else if" block for that case
		 */
	}
	MemoryContextSwitchTo(oldcontext);
	conn->combiner = NULL;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
static void
CopyDataRowTupleToSlot(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	char 		*msg;
	MemoryContext	oldcontext;
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
	msg = (char *)palloc(combiner->currentRow.msglen);
	memcpy(msg, combiner->currentRow.msg, combiner->currentRow.msglen);
	ExecStoreDataRowTuple(msg, combiner->currentRow.msglen,
							combiner->currentRow.msgnode, slot, true);
	pfree(combiner->currentRow.msg);
	combiner->currentRow.msg = NULL;
	combiner->currentRow.msglen = 0;
	combiner->currentRow.msgnode = 0;
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Get next data row from the combiner's buffer into provided slot
 * Just clear slot and return false if buffer is empty, that means end of result
 * set is reached
 */
bool
FetchTuple(RemoteQueryState *combiner, TupleTableSlot *slot)
{
	bool have_tuple = false;

	/* If we have message in the buffer, consume it */
	if (combiner->currentRow.msg)
	{
		CopyDataRowTupleToSlot(combiner, slot);
		have_tuple = true;
	}

	/*
	 * Note: If we are fetching not sorted results we can not have both
	 * currentRow and buffered rows. When connection is buffered currentRow
	 * is moved to buffer, and then it is cleaned after buffering is
	 * completed. Afterwards rows will be taken from the buffer bypassing
	 * currentRow until buffer is empty, and only after that data are read
	 * from a connection.
	 * PGXCTODO: the message should be allocated in the same memory context as
	 * that of the slot. Are we sure of that in the call to
	 * ExecStoreDataRowTuple below? If one fixes this memory issue, please
	 * consider using CopyDataRowTupleToSlot() for the same.
	 */
	if (list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
		ExecStoreDataRowTuple(dataRow->msg, dataRow->msglen, dataRow->msgnode,
								slot, true);
		pfree(dataRow);
		return true;
	}

	while (combiner->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

		/* Going to use a connection, buffer it if needed */
		if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != NULL
				&& conn->combiner != combiner)
			BufferConnection(conn);

		/*
		 * If current connection is idle it means portal on the Datanode is
		 * suspended. If we have a tuple do not hurry to request more rows,
		 * leave connection clean for other RemoteQueries.
		 * If we do not have, request more and try to get it
		 */
		if (conn->state == DN_CONNECTION_STATE_IDLE)
		{
			/*
			 * If we have tuple to return do not hurry to request more, keep
			 * connection clean
			 */
			if (have_tuple)
				return true;
			else
			{
				if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from Datanode")));
				if (pgxc_node_send_sync(conn) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from Datanode")));
				if (pgxc_node_receive(1, &conn, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to fetch from Datanode")));
				conn->combiner = combiner;
			}
		}

		/* read messages */
		res = handle_response(conn, combiner);
		if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to fetch from Datanode")));
			continue;
		}
		else if (res == RESPONSE_SUSPENDED)
		{
			/* Make next connection current */
			if (++combiner->current_conn >= combiner->conn_count)
				combiner->current_conn = 0;
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/* Remove current connection, move last in-place, adjust current_conn */
			if (combiner->current_conn < --combiner->conn_count)
				combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
			else
				combiner->current_conn = 0;
		}
		else if (res == RESPONSE_DATAROW && have_tuple)
		{
			/*
			 * We already have a tuple and received another one, leave it till
			 * next fetch
			 */
			return true;
		}

		/* If we have message in the buffer, consume it */
		if (combiner->currentRow.msg)
		{
			CopyDataRowTupleToSlot(combiner, slot);
			have_tuple = true;
		}
	}

	/* report end of data to the caller */
	if (!have_tuple)
		ExecClearTuple(slot);

	return have_tuple;
}


/*
 * Handle responses from the Datanode connections
 */
static int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
						 struct timeval * timeout, RemoteQueryState *combiner)
{
	int			count = conn_count;
	PGXCNodeHandle *to_receive[conn_count];

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from Datanode connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * Other safegaurds exist to avoid this, however.
	 */
	while (count > 0)
	{
		int i = 0;

		if (pgxc_node_receive(count, to_receive, timeout))
			return EOF;
		while (i < count)
		{
			int result =  handle_response(to_receive[i], combiner);
			switch (result)
			{
				case RESPONSE_EOF: /* have something to read, keep receiving */
					i++;
					break;
				case RESPONSE_COMPLETE:
				case RESPONSE_COPY:
					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
						to_receive[i] = to_receive[count];
					break;
				default:
					/* Inconsistent responses */
					add_error_message(to_receive[i], "Unexpected response from the Datanodes");
					elog(ERROR, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
					/* Stop tracking and move last connection in place */
					count--;
					if (i < count)
						to_receive[i] = to_receive[count];
			}
		}
	}
	pgxc_node_report_error(combiner);

	return 0;
}

/*
 * Read next message from the connection and update the combiner accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_COMPLETE - done with the connection
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response(PGXCNodeHandle * conn, RemoteQueryState *combiner)
{
	char		*msg;
	int		msg_len;
	char		msg_type;
	bool		suspended = false;

	for (;;)
	{
		Assert(conn->state != DN_CONNECTION_STATE_IDLE);

		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return RESPONSE_COMPLETE;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		Assert(conn->combiner == combiner || conn->combiner == NULL);

		/* TODO handle other possible responses */
		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				return RESPONSE_EOF;
			case 'c':			/* CopyToCommandComplete */
				HandleCopyOutComplete(combiner);
				break;
			case 'C':			/* CommandComplete */
				HandleCommandComplete(combiner, msg, msg_len, conn);
				break;
			case 'T':			/* RowDescription */
#ifdef DN_CONNECTION_DEBUG
				Assert(!conn->have_row_desc);
				conn->have_row_desc = true;
#endif
				if (HandleRowDescription(combiner, msg, msg_len))
					return RESPONSE_TUPDESC;
				break;
			case 'D':			/* DataRow */
#ifdef DN_CONNECTION_DEBUG
				Assert(conn->have_row_desc);
#endif
				HandleDataRow(combiner, msg, msg_len, conn->nodeoid);
				return RESPONSE_DATAROW;
			case 's':			/* PortalSuspended */
				suspended = true;
				break;
			case '1': /* ParseComplete */
			case '2': /* BindComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				/* simple notifications, continue reading */
				break;
			case 'G': /* CopyInResponse */
				conn->state = DN_CONNECTION_STATE_COPY_IN;
				HandleCopyIn(combiner);
				/* Done, return to caller to let it know the data can be passed in */
				return RESPONSE_COPY;
			case 'H': /* CopyOutResponse */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyOut(combiner);
				return RESPONSE_COPY;
			case 'd': /* CopyOutDataRow */
				conn->state = DN_CONNECTION_STATE_COPY_OUT;
				HandleCopyDataRow(combiner, msg, msg_len);
				break;
			case 'E':			/* ErrorResponse */
				HandleError(combiner, msg, msg_len);
				add_error_message(conn, combiner->errorMessage);
				/*
				 * Do not return with an error, we still need to consume Z,
				 * ready-for-query
				 */
				break;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
			case 'S':			/* SetCommandComplete */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
			{
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED Coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				int result = suspended ? RESPONSE_SUSPENDED : RESPONSE_COMPLETE;
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				return result;
			}
			case 'M':			/* Command Id */
				HandleDatanodeCommandId(combiner, msg, msg_len);
				break;
			case 'b':
				conn->state = DN_CONNECTION_STATE_IDLE;
				return RESPONSE_BARRIER_OK;
			case 'I':			/* EmptyQuery */
			default:
				/* sync lost? */
				elog(WARNING, "Received unsupported message type: %c", msg_type);
				conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
				/* stop reading */
				return RESPONSE_COMPLETE;
		}
	}
	/* never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}


/*
 * Has the Datanode sent Ready For Query
 */

bool
is_data_node_ready(PGXCNodeHandle * conn)
{
	char		*msg;
	int		msg_len;
	char		msg_type;

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return true;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return false;

		msg_type = get_message(conn, &msg_len, &msg);
		switch (msg_type)
		{
			case 's':			/* PortalSuspended */
				break;

			case 'Z':			/* ReadyForQuery */
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED Coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				conn->transaction_status = msg[0];
				conn->state = DN_CONNECTION_STATE_IDLE;
				conn->combiner = NULL;
				return true;
		}
	}
	/* never happen, but keep compiler quiet */
	return false;
}

/*
 * Construct a BEGIN TRANSACTION command after taking into account the
 * current options. The returned string is not palloced and is valid only until
 * the next call to the function.
 */
static char *
generate_begin_command(void)
{
	static char begin_cmd[1024];
	const char *read_only;
	const char *isolation_level;

	/*
	 * First get the READ ONLY status because the next call to GetConfigOption
	 * will overwrite the return buffer
	 */
	if (strcmp(GetConfigOption("transaction_read_only", false, false), "on") == 0)
		read_only = "READ ONLY";
	else
		read_only = "READ WRITE";

	/* Now get the isolation_level for the transaction */
	isolation_level = GetConfigOption("transaction_isolation", false, false);
	if (strcmp(isolation_level, "default") == 0)
		isolation_level = GetConfigOption("default_transaction_isolation", false, false);

	/* Finally build a START TRANSACTION command */
	sprintf(begin_cmd, "START TRANSACTION ISOLATION LEVEL %s %s", isolation_level, read_only);

	return begin_cmd;
}

/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses.
 * Also send the GXID for the transaction.
 */
static int
pgxc_node_begin(int conn_count, PGXCNodeHandle **connections,
				GlobalTransactionId gxid, bool need_tran_block,
				bool readOnly, char node_type)
{
	int			i;
	struct timeval *timeout = NULL;
	RemoteQueryState *combiner;
	TimestampTz timestamp = GetCurrentGTMStartTimestamp();
	PGXCNodeHandle *new_connections[conn_count];
	int new_count = 0;
	int 			con[conn_count];
	int				j = 0;

	/*
	 * If no remote connections, we don't have anything to do
	 */
	if (conn_count == 0)
		return 0;

	for (i = 0; i < conn_count; i++)
	{
		/*
		 * If the node is already a participant in the transaction, skip it
		 */
		if (list_member(XactReadNodes, connections[i]) ||
				list_member(XactWriteNodes, connections[i]))
		{
			/*
			 * If we are doing a write operation, we may need to shift the node
			 * to the write-list. RegisterTransactionNodes does that for us
			 */
			if (!readOnly)
				RegisterTransactionNodes(1, (void **)&connections[i], true);
			continue;
		}

		/*
		 * PGXC TODO - A connection should not be in DN_CONNECTION_STATE_QUERY
		 * state when we are about to send a BEGIN TRANSACTION command to the
		 * node. We should consider changing the following to an assert and fix
		 * any bugs reported
		 */
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);

		/* Send GXID and check for errors */
		if (GlobalTransactionIdIsValid(gxid) && pgxc_node_send_gxid(connections[i], gxid))
			return EOF;

		/* Send timestamp and check for errors */
		if (GlobalTimestampIsValid(timestamp) && pgxc_node_send_timestamp(connections[i], timestamp))
			return EOF;

		/* Send BEGIN */
		if (need_tran_block)
		{
			/* Send the BEGIN TRANSACTION command and check for errors */
			if (pgxc_node_send_query(connections[i], generate_begin_command()))
				return EOF;

			con[j++] = PGXCNodeGetNodeId(connections[i]->nodeoid, node_type);
			/*
			 * Register the node as a participant in the transaction. The
			 * caller should tell us if the node may do any write activitiy
			 *
			 * XXX This is a bit tricky since it would be difficult to know if
			 * statement has any side effect on the Datanode. So a SELECT
			 * statement may invoke a function on the Datanode which may end up
			 * modifying the data at the Datanode. We can possibly rely on the
			 * function qualification to decide if a statement is a read-only or a
			 * read-write statement.
			 */
			RegisterTransactionNodes(1, (void **)&connections[i], !readOnly);
			new_connections[new_count++] = connections[i];
		}
	}

	/*
	 * If we did not send a BEGIN command to any node, we are done. Otherwise,
	 * we need to check for any errors and report them
	 */
	if (new_count == 0)
		return 0;

	combiner = CreateResponseCombiner(new_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	if (pgxc_node_receive_responses(new_count, new_connections, timeout, combiner))
		return EOF;

	/* Verify status */
	if (!ValidateAndCloseCombiner(combiner))
		return EOF;

	/*
	 * Ask pooler to send commands (if any) to nodes involved in transaction to alter the
	 * behavior of current transaction. This fires all transaction level commands before
	 * issuing any DDL, DML or SELECT within the current transaction block.
	 */
	if (GetCurrentLocalParamStatus())
	{
		int res;
		if (node_type == PGXC_NODE_DATANODE)
			res = PoolManagerSendLocalCommand(j, con, 0, NULL);
		else
			res = PoolManagerSendLocalCommand(0, NULL, j, con);

		if (res != 0)
			return EOF;
	}

	/* No problem, let's get going */
	return 0;
}

/*
 * Prepare all remote nodes involved in this transaction. The local node is
 * handled separately and prepared first in xact.c. If there is any error
 * during this phase, it will be reported via ereport() and the transaction
 * will be aborted on the local as well as remote nodes
 *
 * prepareGID is created and passed from xact.c
 */
static bool
pgxc_node_remote_prepare(char *prepareGID)
{
	int         	result = 0;
	int				write_conn_count = remoteXactState.numWriteRemoteNodes;
	char			prepare_cmd[256];
	int				i;
	PGXCNodeHandle	**connections = remoteXactState.remoteNodeHandles;
	RemoteQueryState *combiner = NULL;

	/*
	 * If there is NO write activity or the caller does not want us to run a
	 * 2PC protocol, we don't need to do anything special
	 */
	if ((write_conn_count == 0) || (prepareGID == NULL))
		return false;

	SetSendCommandId(false);

	/* Save the prepareGID in the global state information */
	sprintf(remoteXactState.prepareGID, "%s", prepareGID);

	/* Generate the PREPARE TRANSACTION command */
	sprintf(prepare_cmd, "PREPARE TRANSACTION '%s'", remoteXactState.prepareGID);

	for (i = 0; i < write_conn_count; i++)
	{
		/*
		 * PGXCTODO - We should actually make sure that the connection state is
		 * IDLE when we reach here. The executor should have guaranteed that
		 * before the transaction gets to the commit point. For now, consume
		 * the pending data on the connection
		 */
		if (connections[i]->state != DN_CONNECTION_STATE_IDLE)
			BufferConnection(connections[i]);

		/* Clean the previous errors, if any */
		connections[i]->error = NULL;

		/*
		 * Now we are ready to PREPARE the transaction. Any error at this point
		 * can be safely ereport-ed and the transaction will be aborted.
		 */
		if (pgxc_node_send_query(connections[i], prepare_cmd))
		{
			remoteXactState.remoteNodeStatus[i] = RXACT_NODE_PREPARE_FAILED;
			remoteXactState.status = RXACT_PREPARE_FAILED;
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to send PREPARE TRANSACTION command to "
						"the node %u", connections[i]->nodeoid)));
		}
		else
		{
			remoteXactState.remoteNodeStatus[i] = RXACT_NODE_PREPARE_SENT;
			/* Let the HandleCommandComplete know response checking is enable */
			connections[i]->ck_resp_rollback = RESP_ROLLBACK_CHECK;
		}
	}

	/*
	 * Receive and check for any errors. In case of errors, we don't bail out
	 * just yet. We first go through the list of connections and look for
	 * errors on each connection. This is important to ensure that we run
	 * an appropriate ROLLBACK command later on (prepared transactions must be
	 * rolled back with ROLLBACK PREPARED commands).
	 *
	 * PGXCTODO - There doesn't seem to be a solid mechanism to track errors on
	 * individual connections. The transaction_status field doesn't get set
	 * every time there is an error on the connection. The combiner mechanism is
	 * good for parallel proessing, but I think we should have a leak-proof
	 * mechanism to track connection status
	 */
	if (write_conn_count)
	{
		combiner = CreateResponseCombiner(write_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(write_conn_count, connections, NULL, combiner);
		if (result || !validate_combiner(combiner))
			result = EOF;
		else
		{
			CloseCombiner(combiner);
			combiner = NULL;
		}

		for (i = 0; i < write_conn_count; i++)
		{
			if (remoteXactState.remoteNodeStatus[i] == RXACT_NODE_PREPARE_SENT)
			{
				if (connections[i]->error)
				{
					remoteXactState.remoteNodeStatus[i] = RXACT_NODE_PREPARE_FAILED;
					remoteXactState.status = RXACT_PREPARE_FAILED;
				}
				else
				{
					/* Did we receive ROLLBACK in response to PREPARE TRANSCATION? */
					if (connections[i]->ck_resp_rollback == RESP_ROLLBACK_RECEIVED)
					{
						/* If yes, it means PREPARE TRANSACTION failed */
						remoteXactState.remoteNodeStatus[i] = RXACT_NODE_PREPARE_FAILED;
						remoteXactState.status = RXACT_PREPARE_FAILED;
						result = 0;
					}
					else
					{
						remoteXactState.remoteNodeStatus[i] = RXACT_NODE_PREPARED;
					}
				}
			}
		}
	}

	/*
	 * If we failed to PREPARE on one or more nodes, report an error and let
	 * the normal abort processing take charge of aborting the transaction
	 */
	if (result)
	{
		remoteXactState.status = RXACT_PREPARE_FAILED;
		if (combiner)
			pgxc_node_report_error(combiner);
		else
			elog(ERROR, "failed to PREPARE transaction on one or more nodes");
	}

	if (remoteXactState.status == RXACT_PREPARE_FAILED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to PREPARE the transaction on one or more nodes")));

	/* Everything went OK. */
	remoteXactState.status = RXACT_PREPARED;
	return result;
}

/*
 * Commit a running or a previously PREPARED transaction on the remote nodes.
 * The local transaction is handled separately in xact.c
 *
 * Once a COMMIT command is sent to any node, the transaction must be finally
 * be committed. But we still report errors via ereport and let
 * AbortTransaction take care of handling partly committed transactions.
 *
 * For 2PC transactions: If local node is involved in the transaction, its
 * already prepared locally and we are in a context of a different transaction
 * (we call it auxulliary transaction) already. So AbortTransaction will
 * actually abort the auxilliary transaction, which is OK. OTOH if the local
 * node is not involved in the main transaction, then we don't care much if its
 * rolled back on the local node as part of abort processing.
 *
 * When 2PC is not used for reasons such as because transaction has accessed
 * some temporary objects, we are already exposed to the risk of committing it
 * one node and aborting on some other node. So such cases need not get more
 * attentions.
 */
static void
pgxc_node_remote_commit(void)
{
	int				result = 0;
	char			commitPrepCmd[256];
	char			commitCmd[256];
	int				write_conn_count = remoteXactState.numWriteRemoteNodes;
	int				read_conn_count = remoteXactState.numReadRemoteNodes;
	PGXCNodeHandle	**connections = remoteXactState.remoteNodeHandles;
	PGXCNodeHandle  *new_connections[write_conn_count + read_conn_count];
	int				new_conn_count = 0;
	int				i;
	RemoteQueryState *combiner = NULL;

	/*
	 * We must handle reader and writer connections both since the transaction
	 * must be closed even on a read-only node
	 */
	if (read_conn_count + write_conn_count == 0)
		return;

	SetSendCommandId(false);

	/*
	 * Barrier:
	 *
	 * We should acquire the BarrierLock in SHARE mode here to ensure that
	 * there are no in-progress barrier at this point. This mechanism would
	 * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
	 * requester
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);

	/*
	 * The readers can be committed with a simple COMMIT command. We still need
	 * this to close the transaction block
	 */
	sprintf(commitCmd, "COMMIT TRANSACTION");

	/*
	 * If we are running 2PC, construct a COMMIT command to commit the prepared
	 * transactions
	 */
	if (remoteXactState.status == RXACT_PREPARED)
	{
		sprintf(commitPrepCmd, "COMMIT PREPARED '%s'", remoteXactState.prepareGID);
		/*
		 * If the local node is involved in the transaction, we would have
		 * already prepared it and started a new transaction. We can use the
		 * GXID of the new transaction to run the COMMIT PREPARED commands.
		 * So get an auxilliary GXID only if the local node is not involved
		 */

		if (!GlobalTransactionIdIsValid(remoteXactState.commitXid))
			remoteXactState.commitXid = GetAuxilliaryTransactionId();
	}

	/*
	 * First send GXID if necessary. If there is an error at this stage, the
	 * transaction can be aborted safely because we haven't yet sent COMMIT
	 * command to any participant
	 */
	for (i = 0; i < write_conn_count + read_conn_count; i++)
	{
		if (remoteXactState.remoteNodeStatus[i] == RXACT_NODE_PREPARED)
		{
			Assert(GlobalTransactionIdIsValid(remoteXactState.commitXid));
			if (pgxc_node_send_gxid(connections[i], remoteXactState.commitXid))
			{
				remoteXactState.status = RXACT_COMMIT_FAILED;
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send GXID for COMMIT PREPARED "
							 "command")));
			}
		}
	}

	/*
	 * Now send the COMMIT command to all the participants
	 */
	for (i = 0; i < write_conn_count + read_conn_count; i++)
	{
		const char *command;

		Assert(remoteXactState.remoteNodeStatus[i] == RXACT_NODE_PREPARED ||
			   remoteXactState.remoteNodeStatus[i] == RXACT_NODE_NONE);

		if (remoteXactState.remoteNodeStatus[i] == RXACT_NODE_PREPARED)
			command = commitPrepCmd;
		else
			command = commitCmd;

		/* Clean the previous errors, if any */
		connections[i]->error = NULL;

		if (pgxc_node_send_query(connections[i], command))
		{
			remoteXactState.remoteNodeStatus[i] = RXACT_NODE_COMMIT_FAILED;
			remoteXactState.status = RXACT_COMMIT_FAILED;

			/*
			 * If the error occurred on the first connection, we still have
			 * chance to abort the whole transaction. We prefer that because
			 * that reduces the need for any manual intervention at least until
			 * we have a automatic mechanism to resolve in-doubt transactions
			 *
			 * XXX We can ideally check for first writer connection, but keep
			 * it simple for now
			 */
			if (i == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send COMMIT command to node")));
			else
				add_error_message(connections[i], "failed to send COMMIT "
						"command to node");
		}
		else
		{
			remoteXactState.remoteNodeStatus[i] = RXACT_NODE_COMMIT_SENT;
			new_connections[new_conn_count++] = connections[i];
		}
	}

	/*
	 * Release the BarrierLock.
	 */
	LWLockRelease(BarrierLock);

	if (new_conn_count)
	{
		combiner = CreateResponseCombiner(new_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(new_conn_count, new_connections, NULL, combiner);
		if (result || !validate_combiner(combiner))
			result = EOF;
		else
		{
			CloseCombiner(combiner);
			combiner = NULL;
		}
		/*
		 * Even if the command failed on some node, don't throw an error just
		 * yet. That gives a chance to look for individual connection status
		 * and record appropriate information for later recovery
		 *
		 * XXX A node once prepared must be able to either COMMIT or ABORT. So a
		 * COMMIT can fail only because of either communication error or because
		 * the node went down. Even if one node commits, the transaction must be
		 * eventually committed on all the nodes.
		 */

		/* At this point, we must be in one the following state */
		Assert(remoteXactState.status == RXACT_COMMIT_FAILED ||
				remoteXactState.status == RXACT_PREPARED ||
				remoteXactState.status == RXACT_NONE);

		/*
		 * Go through every connection and check if COMMIT succeeded or failed on
		 * that connection. If the COMMIT has failed on one node, but succeeded on
		 * some other, such transactions need special attention (by the
		 * administrator for now)
		 */
		for (i = 0; i < write_conn_count + read_conn_count; i++)
		{
			if (remoteXactState.remoteNodeStatus[i] == RXACT_NODE_COMMIT_SENT)
			{
				if (connections[i]->error)
				{
					remoteXactState.remoteNodeStatus[i] = RXACT_NODE_COMMIT_FAILED;
					if (remoteXactState.status != RXACT_PART_COMMITTED)
						remoteXactState.status = RXACT_COMMIT_FAILED;
				}
				else
				{
					remoteXactState.remoteNodeStatus[i] = RXACT_NODE_COMMITTED;
					if (remoteXactState.status == RXACT_COMMIT_FAILED)
						remoteXactState.status = RXACT_PART_COMMITTED;
				}
			}
		}
	}

	if (result)
	{
		if (combiner)
			pgxc_node_report_error(combiner);
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to COMMIT the transaction on one or more nodes")));
	}

	if (remoteXactState.status == RXACT_COMMIT_FAILED ||
		remoteXactState.status == RXACT_PART_COMMITTED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to COMMIT the transaction on one or more nodes")));

	remoteXactState.status = RXACT_COMMITTED;
}

/*
 * Abort the current transaction on the local and remote nodes. If the
 * transaction is prepared on the remote node, we send a ROLLBACK PREPARED
 * command, otherwise a ROLLBACK command is sent.
 *
 * Note that if the local node was involved and prepared successfully, we are
 * running in a separate transaction context right now
 */
static void
pgxc_node_remote_abort(void)
{
	int				result = 0;
	char			*rollbackCmd = "ROLLBACK TRANSACTION";
	char			rollbackPrepCmd[256];
	int				write_conn_count = remoteXactState.numWriteRemoteNodes;
	int				read_conn_count = remoteXactState.numReadRemoteNodes;
	int				i;
	PGXCNodeHandle	**connections = remoteXactState.remoteNodeHandles;
	PGXCNodeHandle  *new_connections[remoteXactState.numWriteRemoteNodes + remoteXactState.numReadRemoteNodes];
	int				new_conn_count = 0;
	RemoteQueryState *combiner = NULL;

	SetSendCommandId(false);

	/* Send COMMIT/ROLLBACK PREPARED TRANSACTION to the remote nodes */
	for (i = 0; i < write_conn_count + read_conn_count; i++)
	{
		RemoteXactNodeStatus status = remoteXactState.remoteNodeStatus[i];

		/* Clean the previous errors, if any */
		connections[i]->error = NULL;

		if ((status == RXACT_NODE_PREPARED) ||
			(status == RXACT_NODE_PREPARE_SENT))
		{
			sprintf(rollbackPrepCmd, "ROLLBACK PREPARED '%s'", remoteXactState.prepareGID);

			if (!GlobalTransactionIdIsValid(remoteXactState.commitXid))
				remoteXactState.commitXid = GetAuxilliaryTransactionId();

			if (pgxc_node_send_gxid(connections[i], remoteXactState.commitXid))
			{
				add_error_message(connections[i], "failed to send GXID for "
						"ROLLBACK PREPARED command");
				remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
				remoteXactState.status = RXACT_ABORT_FAILED;

			}
			else if (pgxc_node_send_query(connections[i], rollbackPrepCmd))
			{
				add_error_message(connections[i], "failed to send ROLLBACK PREPARED "
						"TRANSACTION command to node");
				remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
				remoteXactState.status = RXACT_ABORT_FAILED;
			}
			else
			{
				remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORT_SENT;
				new_connections[new_conn_count++] = connections[i];
			}
		}
		else
		{
			if (pgxc_node_send_query(connections[i], rollbackCmd))
			{
				add_error_message(connections[i], "failed to send ROLLBACK "
						"TRANSACTION command to node");
				remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
				remoteXactState.status = RXACT_ABORT_FAILED;
			}
			else
			{
				remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORT_SENT;
				new_connections[new_conn_count++] = connections[i];
			}
		}
	}

	if (new_conn_count)
	{
		combiner = CreateResponseCombiner(new_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(new_conn_count, new_connections, NULL, combiner);
		if (result || !validate_combiner(combiner))
			result = EOF;
		else
		{
			CloseCombiner(combiner);
			combiner = NULL;
		}

		for (i = 0; i < write_conn_count + read_conn_count; i++)
		{
			if (remoteXactState.remoteNodeStatus[i] == RXACT_NODE_ABORT_SENT)
			{
				if (connections[i]->error)
				{
					remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
					if (remoteXactState.status != RXACT_PART_ABORTED)
						remoteXactState.status = RXACT_ABORT_FAILED;
					elog(LOG, "Failed to ABORT at node %d\nDetail: %s",
							connections[i]->nodeoid, connections[i]->error);
				}
				else
				{
					remoteXactState.remoteNodeStatus[i] = RXACT_NODE_ABORTED;
					if (remoteXactState.status == RXACT_ABORT_FAILED)
						remoteXactState.status = RXACT_PART_ABORTED;
				}
			}
		}
	}

	if (result)
	{
		if (combiner)
			pgxc_node_report_error(combiner);
		else
			elog(LOG, "Failed to ABORT an implicitly PREPARED "
					"transaction - result %d", result);
	}

	/*
	 * Don't ereport because we might already been abort processing and any
	  * error at this point can lead to infinite recursion
	 *
	 * XXX How do we handle errors reported by internal functions used to
	 * communicate with remote nodes ?
	 */
	if (remoteXactState.status == RXACT_ABORT_FAILED ||
		remoteXactState.status == RXACT_PART_ABORTED)
		elog(LOG, "Failed to ABORT an implicitly PREPARED transaction "
				"status - %d", remoteXactState.status);
	else
		remoteXactState.status = RXACT_ABORTED;

	return;
}

/*
 * Begin COPY command
 * The copy_connections array must have room for requested number of items.
 * The function can not deal with mix of coordinators and datanodes.
 */
PGXCNodeHandle**
pgxcNodeCopyBegin(const char *query, List *nodelist, Snapshot snapshot, char node_type)
{
	int i;
	int default_node_count = (node_type == PGXC_NODE_DATANODE) ?
									NumDataNodes : NumCoords;
	int conn_count = list_length(nodelist) == 0 ? default_node_count : list_length(nodelist);
	struct timeval *timeout = NULL;
	PGXCNodeAllHandles *pgxc_handles;
	PGXCNodeHandle **connections;
	PGXCNodeHandle **copy_connections;
	ListCell *nodeitem;
	bool need_tran_block;
	GlobalTransactionId gxid;
	RemoteQueryState *combiner;

	Assert(node_type == PGXC_NODE_DATANODE || node_type == PGXC_NODE_COORDINATOR);

	if (conn_count == 0)
		return NULL;

	/* Get needed Datanode connections */
	if (node_type == PGXC_NODE_DATANODE)
	{
		pgxc_handles = get_handles(nodelist, NULL, false);
		connections = pgxc_handles->datanode_handles;
	}
	else
	{
		pgxc_handles = get_handles(NULL, nodelist, true);
		connections = pgxc_handles->coord_handles;
	}

	if (!connections)
		return NULL;

	/*
	 * If more than one nodes are involved or if we are already in a
	 * transaction block, we must the remote statements in a transaction block
	 */
	need_tran_block = (conn_count > 1) || (TransactionBlockStatusCode() == 'T');

	elog(DEBUG1, "conn_count = %d, need_tran_block = %s", conn_count,
			need_tran_block ? "true" : "false");

	/*
	 * We need to be able quickly find a connection handle for specified node number,
	 * So store connections in an array where index is node-1.
	 * Unused items in the array should be NULL
	 */
	copy_connections = (PGXCNodeHandle **) palloc0(default_node_count * sizeof(PGXCNodeHandle *));
	i = 0;
	foreach(nodeitem, nodelist)
		copy_connections[lfirst_int(nodeitem)] = connections[i++];

	gxid = GetCurrentTransactionId();

	if (!GlobalTransactionIdIsValid(gxid))
	{
		pfree_pgxc_all_handles(pgxc_handles);
		pfree(copy_connections);
		return NULL;
	}

	/* Start transaction on connections where it is not started */
	if (pgxc_node_begin(conn_count, connections, gxid, need_tran_block, false, node_type))
	{
		pfree_pgxc_all_handles(pgxc_handles);
		pfree(copy_connections);
		return NULL;
	}

	/* Send query to nodes */
	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);

		if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
		if (pgxc_node_send_query(connections[i], query) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree_pgxc_all_handles(pgxc_handles);
			pfree(copy_connections);
			return NULL;
		}
	}

	/*
	 * We are expecting CopyIn response, but do not want to send it to client,
	 * caller should take care about this, because here we do not know if
	 * client runs console or file copy
	 */
	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	if (pgxc_node_receive_responses(conn_count, connections, timeout, combiner)
			|| !ValidateAndCloseCombiner(combiner))
	{
		pgxcNodeCopyFinish(connections, -1, COMBINE_TYPE_NONE, PGXC_NODE_DATANODE);
		pfree(connections);
		pfree(copy_connections);
		return NULL;
	}
	pfree(connections);
	return copy_connections;
}

/*
 * Send a data row to the specified nodes
 */
int
DataNodeCopyIn(char *data_row, int len, ExecNodes *exec_nodes, PGXCNodeHandle** copy_connections)
{
	PGXCNodeHandle *primary_handle = NULL;
	ListCell *nodeitem;
	/* size + data row + \n */
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	if (exec_nodes->primarynodelist)
	{
		primary_handle = copy_connections[lfirst_int(list_head(exec_nodes->primarynodelist))];
	}

	if (primary_handle)
	{
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = primary_handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if (bytes_needed > COPY_BUFFER_SIZE)
			{
				/* First look if Datanode has sent a error message */
				int read_status = pgxc_node_read_data(primary_handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(primary_handle, "failed to read data from Datanode");
					return EOF;
				}

				if (primary_handle->inStart < primary_handle->inEnd)
				{
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					handle_response(primary_handle, combiner);
					if (!ValidateAndCloseCombiner(combiner))
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(primary_handle))
					return EOF;

				if (send_some(primary_handle, primary_handle->outEnd) < 0)
				{
					add_error_message(primary_handle, "failed to send data to Datanode");
					return EOF;
				}
				else if (primary_handle->outEnd > MAX_SIZE_TO_FORCE_FLUSH)
				{
					int rv;

					if ((rv = flushPGXCNodeHandleData(primary_handle)) != 0)
						return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, primary_handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			primary_handle->outBuffer[primary_handle->outEnd++] = 'd';
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, &nLen, 4);
			primary_handle->outEnd += 4;
			memcpy(primary_handle->outBuffer + primary_handle->outEnd, data_row, len);
			primary_handle->outEnd += len;
			primary_handle->outBuffer[primary_handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(primary_handle, "Invalid Datanode connection");
			return EOF;
		}
	}

	foreach(nodeitem, exec_nodes->nodeList)
	{
		PGXCNodeHandle *handle = copy_connections[lfirst_int(nodeitem)];
		if (handle && handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if ((primary_handle && bytes_needed > PRIMARY_NODE_WRITEAHEAD)
					|| (!primary_handle && bytes_needed > COPY_BUFFER_SIZE))
			{
				int to_send = handle->outEnd;

				/* First look if Datanode has sent a error message */
				int read_status = pgxc_node_read_data(handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(handle, "failed to read data from Datanode");
					return EOF;
				}

				if (handle->inStart < handle->inEnd)
				{
					RemoteQueryState *combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
					handle_response(handle, combiner);
					if (!ValidateAndCloseCombiner(combiner))
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(handle))
					return EOF;

				/*
				 * Allow primary node to write out data before others.
				 * If primary node was blocked it would not accept copy data.
				 * So buffer at least PRIMARY_NODE_WRITEAHEAD at the other nodes.
				 * If primary node is blocked and is buffering, other buffers will
				 * grow accordingly.
				 */
				if (primary_handle)
				{
					if (primary_handle->outEnd + PRIMARY_NODE_WRITEAHEAD < handle->outEnd)
						to_send = handle->outEnd - primary_handle->outEnd - PRIMARY_NODE_WRITEAHEAD;
					else
						to_send = 0;
				}

				/*
				 * Try to send down buffered data if we have
				 */
				if (to_send && send_some(handle, to_send) < 0)
				{
					add_error_message(handle, "failed to send data to Datanode");
					return EOF;
				}
				else if (handle->outEnd > MAX_SIZE_TO_FORCE_FLUSH)
				{
					int rv;

					if ((rv = flushPGXCNodeHandleData(handle)) != 0)
						return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, data_row, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid Datanode connection");
			return EOF;
		}
	}
	return 0;
}

uint64
DataNodeCopyOut(ExecNodes *exec_nodes,
				PGXCNodeHandle** copy_connections,
				TupleDesc tupleDesc,
				FILE* copy_file,
				Tuplestorestate *store,
				RemoteCopyType remoteCopyType)
{
	RemoteQueryState *combiner;
	int 		conn_count = list_length(exec_nodes->nodeList) == 0 ? NumDataNodes : list_length(exec_nodes->nodeList);
	ListCell	*nodeitem;
	uint64		processed;

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_SUM);
	combiner->processed = 0;
	combiner->remoteCopyType = remoteCopyType;

	/*
	 * If there is an existing file where to copy data,
	 * pass it to combiner when remote COPY output is sent back to file.
	 */
	if (copy_file && remoteCopyType == REMOTE_COPY_FILE)
		combiner->copy_file = copy_file;
	if (store && remoteCopyType == REMOTE_COPY_TUPLESTORE)
	{
		combiner->tuplestorestate = store;
		combiner->tuple_desc = tupleDesc;
	}

	foreach(nodeitem, exec_nodes->nodeList)
	{
		PGXCNodeHandle *handle = copy_connections[lfirst_int(nodeitem)];
		int read_status = 0;

		Assert(handle && handle->state == DN_CONNECTION_STATE_COPY_OUT);

		/*
		 * H message has been consumed, continue to manage data row messages.
		 * Continue to read as long as there is data.
		 */
		while (read_status >= 0 && handle->state == DN_CONNECTION_STATE_COPY_OUT)
		{
			if (handle_response(handle,combiner) == RESPONSE_EOF)
			{
				/* read some extra-data */
				read_status = pgxc_node_read_data(handle, true);
				if (read_status < 0)
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on datanode connection")));
				else
					/*
					 * Set proper connection status - handle_response
					 * has changed it to DN_CONNECTION_STATE_QUERY
					 */
					handle->state = DN_CONNECTION_STATE_COPY_OUT;
			}
			/* There is no more data that can be read from connection */
		}
	}

	processed = combiner->processed;

	if (!ValidateAndCloseCombiner(combiner))
	{
		if (!PersistentConnections)
			release_handles();
		pfree(copy_connections);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes when combining, request type %d", combiner->request_type)));
	}

	return processed;
}

/*
 * Finish copy process on all connections
 */
void
pgxcNodeCopyFinish(PGXCNodeHandle** copy_connections, int primary_dn_index,
					CombineType combine_type, char node_type)
{
	int		i;
	RemoteQueryState *combiner = NULL;
	bool 		error = false;
	struct timeval *timeout = NULL; /* wait forever */
	int		default_conn_count = (node_type == PGXC_NODE_DATANODE) ? NumDataNodes : NumCoords;
	PGXCNodeHandle **connections = palloc0(sizeof(PGXCNodeHandle *) * default_conn_count);
	PGXCNodeHandle *primary_handle = NULL;
	int 		conn_count = 0;

	Assert(node_type == PGXC_NODE_DATANODE || node_type == PGXC_NODE_COORDINATOR);

	for (i = 0; i < default_conn_count; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		if (i == primary_dn_index)
			primary_handle = handle;
		else
			connections[conn_count++] = handle;
	}

	if (primary_handle)
	{
		error = true;
		if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN || primary_handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(primary_handle, false);

		combiner = CreateResponseCombiner(conn_count + 1, combine_type);
		error = (pgxc_node_receive_responses(1, &primary_handle, timeout, combiner) != 0) || error;
	}

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];

		error = true;
		if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(handle, false);
	}

	if (!combiner)
		combiner = CreateResponseCombiner(conn_count, combine_type);
	error = (pgxc_node_receive_responses(conn_count, connections, timeout, combiner) != 0) || error;

	if (!ValidateAndCloseCombiner(combiner) || error)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Error while running COPY")));
}

/*
 * End copy process on a connection
 */
bool
DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error)
{
	int 		nLen = htonl(4);

	if (handle == NULL)
		return true;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
		return true;

	if (is_error)
		handle->outBuffer[handle->outEnd++] = 'f';
	else
		handle->outBuffer[handle->outEnd++] = 'c';

	memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
	handle->outEnd += 4;

	/* We need response right away, so send immediately */
	if (pgxc_node_flush(handle) < 0)
		return true;

	return false;
}

RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;
	TupleDesc			scan_type;

	/* RemoteQuery node is the leaf node in the plan tree, just like seqscan */
	Assert(innerPlan(node) == NULL);
	Assert(outerPlan(node) == NULL);

	remotestate = CreateResponseCombiner(0, node->combine_type);
	remotestate->ss.ps.plan = (Plan *) node;
	remotestate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialisation
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &remotestate->ss.ps);

	/* Initialise child expressions */
	remotestate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) remotestate);
	remotestate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) remotestate);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK)));

	/* Extract the eflags bits that are relevant for tuplestorestate */
	remotestate->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD));

	/* We anyways have to support REWIND for ReScan */
	remotestate->eflags |= EXEC_FLAG_REWIND;

	remotestate->eof_underlying = false;
	remotestate->tuplestorestate = NULL;

	ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
	ExecInitScanTupleSlot(estate, &remotestate->ss);
	scan_type = ExecTypeFromTL(node->base_tlist, false);
	ExecAssignScanType(&remotestate->ss, scan_type);

	remotestate->ss.ps.ps_TupFromTlist = false;

	/*
	 * If there are parameters supplied, get them into a form to be sent to the
	 * Datanodes with bind message. We should not have had done this before.
	 */
	SetDataRowForExtParams(estate->es_param_list_info, remotestate);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&remotestate->ss.ps);
	ExecAssignScanProjectionInfo(&remotestate->ss);

	if (node->rq_save_command_id)
	{
		/* Save command id to be used in some special cases */
		remotestate->rqs_cmd_id = GetCurrentCommandId(false);
	}

	return remotestate;
}

/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type)
{
	List 	   *nodelist = NIL;
	List 	   *primarynode = NIL;
	List	   *coordlist = NIL;
	PGXCNodeHandle *primaryconnection;
	int			co_conn_count, dn_conn_count;
	bool		is_query_coord_only = false;
	PGXCNodeAllHandles *pgxc_handles = NULL;

	/*
	 * If query is launched only on Coordinators, we have to inform get_handles
	 * not to ask for Datanode connections even if list of Datanodes is NIL.
	 */
	if (exec_type == EXEC_ON_COORDS)
		is_query_coord_only = true;

	if (exec_nodes)
	{
		if (exec_nodes->en_expr)
		{
			/* execution time determining of target Datanodes */
			bool isnull;
			ExprState *estate = ExecInitExpr(exec_nodes->en_expr,
											 (PlanState *) planstate);
			Datum partvalue = ExecEvalExpr(estate,
										   planstate->ss.ps.ps_ExprContext,
										   &isnull,
										   NULL);
			RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
			/* PGXCTODO what is the type of partvalue here */
			ExecNodes *nodes = GetRelationNodes(rel_loc_info,
												partvalue,
												isnull,
												exprType((Node *) exec_nodes->en_expr),
												exec_nodes->accesstype);
			/*
			 * en_expr is set by pgxc_set_en_expr only for distributed
			 * relations while planning DMLs, hence a select for update
			 * on a replicated table here is an assertion
			 */
			Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
						IsRelationReplicated(rel_loc_info)));

			if (nodes)
			{
				nodelist = nodes->nodeList;
				primarynode = nodes->primarynodelist;
				pfree(nodes);
			}
			FreeRelationLocInfo(rel_loc_info);
		}
		else if (OidIsValid(exec_nodes->en_relid))
		{
			RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
			ExecNodes *nodes = GetRelationNodes(rel_loc_info, 0, true, InvalidOid, exec_nodes->accesstype);

			/*
			 * en_relid is set only for DMLs, hence a select for update on a
			 * replicated table here is an assertion
			 */
			Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
						IsRelationReplicated(rel_loc_info)));

			/* Use the obtained list for given table */
			if (nodes)
				nodelist = nodes->nodeList;

			/*
			 * Special handling for ROUND ROBIN distributed tables. The target
			 * node must be determined at the execution time
			 */
			if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes)
			{
				nodelist = nodes->nodeList;
				primarynode = nodes->primarynodelist;
			}
			else if (nodes)
			{
				if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				{
					nodelist = exec_nodes->nodeList;
					primarynode = exec_nodes->primarynodelist;
				}
			}

			if (nodes)
				pfree(nodes);
			FreeRelationLocInfo(rel_loc_info);
		}
		else
		{
			if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				nodelist = exec_nodes->nodeList;
			else if (exec_type == EXEC_ON_COORDS)
				coordlist = exec_nodes->nodeList;

			primarynode = exec_nodes->primarynodelist;
		}
	}

	/* Set node list and DN number */
	if (list_length(nodelist) == 0 &&
		(exec_type == EXEC_ON_ALL_NODES ||
		 exec_type == EXEC_ON_DATANODES))
	{
		/* Primary connection is included in this number of connections if it exists */
		dn_conn_count = NumDataNodes;
	}
	else
	{
		if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
		{
			if (primarynode)
				dn_conn_count = list_length(nodelist) + 1;
			else
				dn_conn_count = list_length(nodelist);
		}
		else
			dn_conn_count = 0;
	}

	/* Set Coordinator list and Coordinator number */
	if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
		(list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
	{
		coordlist = GetAllCoordNodes();
		co_conn_count = list_length(coordlist);
	}
	else
	{
		if (exec_type == EXEC_ON_COORDS)
			co_conn_count = list_length(coordlist);
		else
			co_conn_count = 0;
	}

	/* Get other connections (non-primary) */
	pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only);
	if (!pgxc_handles)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not obtain connection from pool")));

	/* Get connection for primary node, if used */
	if (primarynode)
	{
		/* Let's assume primary connection is always a Datanode connection for the moment */
		PGXCNodeAllHandles *pgxc_conn_res;
		pgxc_conn_res = get_handles(primarynode, NULL, false);

		/* primary connection is unique */
		primaryconnection = pgxc_conn_res->datanode_handles[0];

		pfree(pgxc_conn_res);

		if (!primaryconnection)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not obtain connection from pool")));
		pgxc_handles->primary_handle = primaryconnection;
	}

	/* Depending on the execution type, we still need to save the initial node counts */
	pgxc_handles->dn_conn_count = dn_conn_count;
	pgxc_handles->co_conn_count = co_conn_count;

	return pgxc_handles;
}

static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection,
									RemoteQueryState *remotestate,
									Snapshot snapshot)
{
	CommandId	cid;
	RemoteQuery	*step = (RemoteQuery *) remotestate->ss.ps.plan;
	if (connection->state == DN_CONNECTION_STATE_QUERY)
		BufferConnection(connection);

	/*
	 * Scan descriptor would be valid and would contain a valid snapshot
	 * in cases when we need to send out of order command id to data node
	 * e.g. in case of a fetch
	 */

	if (remotestate->cursor != NULL &&
	    remotestate->cursor[0] != '\0' &&
	    remotestate->ss.ss_currentScanDesc != NULL &&
	    remotestate->ss.ss_currentScanDesc->rs_snapshot != NULL)
		cid = remotestate->ss.ss_currentScanDesc->rs_snapshot->curcid;
	else
	{
		/*
		 * An insert into a child by selecting form its parent gets translated
		 * into a multi-statement transaction in which first we select from parent
		 * and then insert into child, then select form child and insert into child.
		 * The select from child should not see the just inserted rows.
		 * The command id of the select from child is therefore set to
		 * the command id of the insert-select query saved earlier.
		 * Similarly a WITH query that updates a table in main query
		 * and inserts a row in the same table in the WITH query
		 * needs to make sure that the row inserted by the WITH query does
		 * not get updated by the main query.
		 */
		if (step->exec_nodes->accesstype == RELATION_ACCESS_READ && step->rq_save_command_id)
			cid = remotestate->rqs_cmd_id;
		else
			cid = GetCurrentCommandId(false);
	}

	if (pgxc_node_send_cmd_id(connection, cid) < 0 )
		return false;

	if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
		return false;
	if (step->statement || step->cursor || remotestate->rqs_num_params)
	{
		/* need to use Extended Query Protocol */
		int	fetch = 0;
		bool	prepared = false;
		bool	send_desc = false;

		if (step->base_tlist != NULL ||
		    step->exec_nodes->accesstype == RELATION_ACCESS_READ ||
		    step->has_row_marks)
			send_desc = true;

		/* if prepared statement is referenced see if it is already exist */
		if (step->statement)
			prepared = ActivateDatanodeStatementOnNode(step->statement,
													   PGXCNodeGetNodeId(connection->nodeoid,
																		 PGXC_NODE_DATANODE));
		/*
		 * execute and fetch rows only if they will be consumed
		 * immediately by the sorter
		 */
		if (step->cursor)
			fetch = 1;

		if (pgxc_node_send_query_extended(connection,
							prepared ? NULL : step->sql_statement,
							step->statement,
							step->cursor,
							remotestate->rqs_num_params,
							remotestate->rqs_param_types,
							remotestate->paramval_len,
							remotestate->paramval_data,
							send_desc,
							fetch) != 0)
			return false;
	}
	else
	{
		if (pgxc_node_send_query(connection, step->sql_statement) != 0)
			return false;
	}
	return true;
}


/*
 * IsReturningDMLOnReplicatedTable
 *
 * This function returns true if the passed RemoteQuery
 * 1. Operates on a table that is replicated
 * 2. Represents a DML
 * 3. Has a RETURNING clause in it
 *
 * If the passed RemoteQuery has a non null base_tlist
 * means that DML has a RETURNING clause.
 */

static bool
IsReturningDMLOnReplicatedTable(RemoteQuery *rq)
{
	if (IsExecNodesReplicated(rq->exec_nodes) &&
		rq->base_tlist != NULL &&	/* Means DML has RETURNING */
		(rq->exec_nodes->accesstype == RELATION_ACCESS_UPDATE ||
		rq->exec_nodes->accesstype == RELATION_ACCESS_INSERT))
		return true;

	return false;
}

void
do_query(RemoteQueryState *node)
{
	RemoteQuery		*step = (RemoteQuery *) node->ss.ps.plan;
	TupleTableSlot		*scanslot = node->ss.ss_ScanTupleSlot;
	bool			force_autocommit = step->force_autocommit;
	bool			is_read_only = step->read_only;
	GlobalTransactionId	gxid = InvalidGlobalTransactionId;
	Snapshot		snapshot = GetActiveSnapshot();
	PGXCNodeHandle		**connections = NULL;
	PGXCNodeHandle		*primaryconnection = NULL;
	int			i;
	int			regular_conn_count = 0;
	bool			need_tran_block;
	PGXCNodeAllHandles	*pgxc_connections;

	/*
	 * A Postgres-XC node cannot run transactions while in recovery as
	 * this operation needs transaction IDs. This is more a safety guard than anything else.
	 */
	if (RecoveryInProgress())
		elog(ERROR, "cannot run transaction to remote nodes during recovery");

	/*
	 * Remember if the remote query is accessing a temp object
	 *
	 * !! PGXC TODO Check if the is_temp flag is propogated correctly when a
	 * remote join is reduced
	 */
	if (step->is_temp)
		ExecSetTempObjectIncluded();

	/*
	 * Consider a test case
	 *
	 * create table rf(a int, b int) distributed by replication;
	 * insert into rf values(1,2),(3,4) returning ctid;
	 *
	 * While inserting the first row do_query works fine, receives the returned
	 * row from the first connection and returns it. In this iteration the other
	 * datanodes also had returned rows but they have not yet been read from the
	 * network buffers. On Next Iteration do_query does not enter the data
	 * receiving loop because it finds that node->connections is not null.
	 * It is therefore required to set node->connections to null here.
	 */
	if (node->conn_count == 0)
		node->connections = NULL;

	/*
	 * Get connections for Datanodes only, utilities and DDLs
	 * are launched in ExecRemoteUtility
	 */
	pgxc_connections = get_exec_connections(node, step->exec_nodes, step->exec_type);

	if (step->exec_type == EXEC_ON_DATANODES)
	{
		connections = pgxc_connections->datanode_handles;
		regular_conn_count = pgxc_connections->dn_conn_count;
	}
	else if (step->exec_type == EXEC_ON_COORDS)
	{
		connections = pgxc_connections->coord_handles;
		regular_conn_count = pgxc_connections->co_conn_count;
	}

	primaryconnection = pgxc_connections->primary_handle;

	/* Primary connection is counted separately */
	if (primaryconnection)
		regular_conn_count--;

	pfree(pgxc_connections);

	/*
	 * We save only regular connections, at the time we exit the function
	 * we finish with the primary connection and deal only with regular
	 * connections on subsequent invocations
	 */
	node->node_count = regular_conn_count;

	if (force_autocommit || is_read_only)
		need_tran_block = false;
	else
		need_tran_block = true;
	/*
	 * XXX We are forcing a transaction block for non-read-only every remote query. We can
	 * get smarter here and avoid a transaction block if all of the following
	 * conditions are true:
	 *
	 * 	- there is only one writer node involved in the transaction (including
	 * 	the local node)
	 * 	- the statement being executed on the remote writer node is a single
	 * 	step statement. IOW, Coordinator must not send multiple queries to the
	 * 	remote node.
	 *
	 * 	Once we have leak-proof mechanism to enforce these constraints, we
	 * 	should relax the transaction block requirement.
	 *
	   need_tran_block = (!is_read_only && total_conn_count > 1) ||
	   					 (TransactionBlockStatusCode() == 'T');
	 */

	elog(DEBUG1, "has primary = %s, regular_conn_count = %d, "
				 "need_tran_block = %s", primaryconnection ? "true" : "false",
				 regular_conn_count, need_tran_block ? "true" : "false");

	gxid = GetCurrentTransactionId();

	if (!GlobalTransactionIdIsValid(gxid))
	{
		if (primaryconnection)
			pfree(primaryconnection);
		pfree(connections);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));
	}

	/* See if we have a primary node, execute on it first before the others */
	if (primaryconnection)
	{
		if (pgxc_node_begin(1, &primaryconnection, gxid, need_tran_block,
					is_read_only, PGXC_NODE_DATANODE))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on primary Datanode.")));

		if (!pgxc_start_command_on_connection(primaryconnection, node, snapshot))
		{
			pfree(connections);
			pfree(primaryconnection);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to Datanodes")));
		}
		Assert(node->combine_type == COMBINE_TYPE_SAME);

		/* Make sure the command is completed on the primary node */
		while (true)
		{
			int res;
			if (pgxc_node_receive(1, &primaryconnection, NULL))
				break;

			res = handle_response(primaryconnection, node);
			if (res == RESPONSE_COMPLETE)
				break;
			else if (res == RESPONSE_TUPDESC)
			{
				ExecSetSlotDescriptor(scanslot, node->tuple_desc);
				/*
				 * Now tuple table slot is responsible for freeing the
				 * descriptor
				 */
				node->tuple_desc = NULL;
				/*
				 * RemoteQuery node doesn't support backward scan, so
				 * randomAccess is false, neither we want this tuple store
				 * persist across transactions.
				 */
				node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
				tuplestore_set_eflags(node->tuplestorestate, node->eflags);
			}
			else if (res == RESPONSE_DATAROW)
			{
				pfree(node->currentRow.msg);
				node->currentRow.msg = NULL;
				node->currentRow.msglen = 0;
				node->currentRow.msgnode = 0;
				continue;
			}
			else if (res == RESPONSE_EOF)
				continue;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unexpected response from Datanode")));
		}
		/* report error if any */
		pgxc_node_report_error(node);
	}

	for (i = 0; i < regular_conn_count; i++)
	{
		if (pgxc_node_begin(1, &connections[i], gxid, need_tran_block,
					is_read_only, PGXC_NODE_DATANODE))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on Datanodes.")));

		if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
		{
			pfree(connections);
			if (primaryconnection)
				pfree(primaryconnection);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to Datanodes")));
		}
		connections[i]->combiner = node;
	}

	if (step->cursor)
	{
		node->cursor_count = regular_conn_count;
		node->cursor_connections = (PGXCNodeHandle **) palloc(regular_conn_count * sizeof(PGXCNodeHandle *));
		memcpy(node->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle *));
	}

	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	while (regular_conn_count > 0 && node->connections == NULL)
	{
		int i = 0;

		if (pgxc_node_receive(regular_conn_count, connections, NULL))
		{
			pfree(connections);
			if (primaryconnection)
				pfree(primaryconnection);
			if (node->cursor_connections)
				pfree(node->cursor_connections);

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to read response from Datanodes")));
		}
		/*
		 * Handle input from the Datanodes.
		 * If we got a RESPONSE_DATAROW we can break handling to wrap
		 * it into a tuple and return. Handling will be continued upon
		 * subsequent invocations.
		 * If we got 0, we exclude connection from the list. We do not
		 * expect more input from it. In case of non-SELECT query we quit
		 * the loop when all nodes finish their work and send ReadyForQuery
		 * with empty connections array.
		 * If we got EOF, move to the next connection, will receive more
		 * data on the next iteration.
		 */
		while (i < regular_conn_count)
		{
			int res = handle_response(connections[i], node);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (i < --regular_conn_count)
					connections[i] = connections[regular_conn_count];
			}
			else if (res == RESPONSE_TUPDESC)
			{
				ExecSetSlotDescriptor(scanslot, node->tuple_desc);
				/*
				 * Now tuple table slot is responsible for freeing the
				 * descriptor
				 */
				node->tuple_desc = NULL;
				/*
				 * RemoteQuery node doesn't support backward scan, so
				 * randomAccess is false, neither we want this tuple store
				 * persist across transactions.
				 */
				node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
				tuplestore_set_eflags(node->tuplestorestate, node->eflags);
			}
			else if (res == RESPONSE_DATAROW)
			{
				/*
				 * Got first data row, quit the loop
				 */
				node->connections = connections;
				node->conn_count = regular_conn_count;
				node->current_conn = i;
				break;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unexpected response from Datanode")));
		}
		/* report error if any */
		pgxc_node_report_error(node);
	}

	if (node->cursor_count)
	{
		node->conn_count = node->cursor_count;
		memcpy(connections, node->cursor_connections, node->cursor_count * sizeof(PGXCNodeHandle *));
		node->connections = connections;
	}
}

/*
 * ExecRemoteQuery
 * Wrapper around the main RemoteQueryNext() function. This
 * wrapper provides materialization of the result returned by
 * RemoteQueryNext
 */

TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	return ExecScan(&(node->ss),
					(ExecScanAccessMtd) RemoteQueryNext,
					(ExecScanRecheckMtd) RemoteQueryRecheck);
}

/*
 * RemoteQueryRecheck -- remote query routine to recheck a tuple in EvalPlanQual
 */
static bool
RemoteQueryRecheck(RemoteQueryState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, RemoteQueryScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}
/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the Datanodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
static TupleTableSlot *
RemoteQueryNext(ScanState *scan_node)
{
	RemoteQueryState *node = (RemoteQueryState *)scan_node;
	TupleTableSlot *scanslot = scan_node->ss_ScanTupleSlot;
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/*
	 * Initialize tuples processed to 0, to make sure we don't re-use the
	 * values from the earlier iteration of RemoteQueryNext(). For an FQS'ed
	 * DML returning query, it may not get updated for subsequent calls.
	 * because there won't be a HandleCommandComplete() call to update this
	 * field.
	 */
	node->rqs_processed = 0;

	if (!node->query_Done)
	{
		/* Fire BEFORE STATEMENT triggers just before the query execution */
		pgxc_rq_fire_bstriggers(node);
		do_query(node);
		node->query_Done = true;
	}

	if (node->update_cursor)
	{
		PGXCNodeAllHandles *all_dn_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
		close_node_cursors(all_dn_handles->datanode_handles,
						  all_dn_handles->dn_conn_count,
						  node->update_cursor);
		pfree(node->update_cursor);
		node->update_cursor = NULL;
		pfree_pgxc_all_handles(all_dn_handles);
	}
	else if(node->tuplestorestate)
	{
		/*
		 * If we are not at the end of the tuplestore, try
		 * to fetch a tuple from tuplestore.
		 */
		Tuplestorestate *tuplestorestate = node->tuplestorestate;
		bool eof_tuplestore = tuplestore_ateof(tuplestorestate);

		/*
		 * If we can fetch another tuple from the tuplestore, return it.
		 */
		if (!eof_tuplestore)
		{
			/* RemoteQuery node doesn't support backward scans */
			if(!tuplestore_gettupleslot(tuplestorestate, true, false, scanslot))
				eof_tuplestore = true;
		}

		/*
		 * Consider a test case
		 *
		 * create table ta1 (v1 int, v2 int);
		 * insert into ta1 values(1,2),(2,3),(3,4);
		 *
		 * create table ta2 (v1 int, v2 int);
		 * insert into ta2 values(1,2),(2,3),(3,4);
		 *
		 * select t1.ctid, t2.ctid,* from ta1 t1, ta2 t2
		 * where t2.v2<=3 order by t1.v1;
		 *          ctid  | ctid  | v1 | v2 | v1 | v2
		 *         -------+-------+----+----+----+----
		 * Row_1    (0,1) | (0,1) |  1 |  2 |  1 |  2
		 * Row_2    (0,1) | (0,2) |  1 |  2 |  2 |  3
		 * Row_3    (0,2) | (0,1) |  2 |  3 |  1 |  2
		 * Row_4    (0,2) | (0,2) |  2 |  3 |  2 |  3
		 * Row_5    (0,1) | (0,1) |  3 |  4 |  1 |  2
		 * Row_6    (0,1) | (0,2) |  3 |  4 |  2 |  3
		 *         (6 rows)
		 *
		 * Note that in the resulting join, we are getting one row of ta1 twice,
		 * as shown by the ctid's in the results. Now consider this update
		 *
		 * update ta1 t1 set v2=t1.v2+10 from ta2 t2
		 * where t2.v2<=3 returning t1.ctid,t1.v1 t1_v1, t1.v2 t1_v2;
		 *
		 * The first iteration of the update runs for Row_1, succeeds and
		 * updates its ctid to say (0,3). In the second iteration for Row_2,
		 * since the ctid of the row has already changed, fails to update any
		 * row and hence do_query does not return any tuple. The FetchTuple
		 * call in RemoteQueryNext hence fails and eof_underlying is set to true.
		 * However in the third iteration for Row_3, the update succeeds and
		 * returns a row, but since the eof_underlying is already set to true,
		 * the RemoteQueryNext does not bother calling FetchTuple, we therefore
		 * do not get more than one row returned as a result of the update
		 * returning query. It is therefore required in RemoteQueryNext to call
		 * FetchTuple in case do_query has copied a row in node->currentRow.msg.
		 * Also we have to reset the eof_underlying flag every time
		 * FetchTuple succeeds to clear any previously set status.
		 */
		if (eof_tuplestore &&
			(!node->eof_underlying ||
			(node->currentRow.msg != NULL)))
		{
			/*
			 * If tuplestore has reached its end but the underlying RemoteQueryNext() hasn't
			 * finished yet, try to fetch another row.
			 */
			if (FetchTuple(node, scanslot))
			{
				/* See comments a couple of lines above */
				node->eof_underlying = false;
				/*
				 * Append a copy of the returned tuple to tuplestore.  NOTE: because
				 * the tuplestore is certainly in EOF state, its read position will
				 * move forward over the added tuple.  This is what we want.
				 */
				if (tuplestorestate && !TupIsNull(scanslot))
					tuplestore_puttupleslot(tuplestorestate, scanslot);
			}
			else
				node->eof_underlying = true;
		}

		if (eof_tuplestore && node->eof_underlying)
			ExecClearTuple(scanslot);
	}
	else
		ExecClearTuple(scanslot);

	/* report error if any */
	pgxc_node_report_error(node);

	/*
	 * Now we know the query is successful. Fire AFTER STATEMENT triggers. Make
	 * sure this is the last iteration of the query. If an FQS query has
	 * RETURNING clause, this function can be called multiple times until we
	 * return NULL.
	 */
	if (TupIsNull(scanslot))
		pgxc_rq_fire_astriggers(node);

	/*
	 * If it's an FQSed DML query for which command tag is to be set,
	 * then update estate->es_processed. For other queries, the standard
	 * executer takes care of it; namely, in ExecModifyTable for DML queries
	 * and ExecutePlan for SELECT queries.
	 */
	if (rq->remote_query->canSetTag &&
		!rq->rq_params_internal &&
		(rq->remote_query->commandType == CMD_INSERT ||
		 rq->remote_query->commandType == CMD_UPDATE ||
		 rq->remote_query->commandType == CMD_DELETE))
		estate->es_processed += node->rqs_processed;

	return scanslot;
}

/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ListCell *lc;

	/* clean up the buffer */
	foreach(lc, node->rowBuffer)
	{
		RemoteDataRow dataRow = (RemoteDataRow) lfirst(lc);
		pfree(dataRow->msg);
	}
	list_free_deep(node->rowBuffer);

	node->current_conn = 0;
	while (node->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = node->connections[node->current_conn];

		/* throw away message */
		if (node->currentRow.msg)
		{
			pfree(node->currentRow.msg);
			node->currentRow.msg = NULL;
		}

		if (conn == NULL)
		{
			node->conn_count--;
			continue;
		}

		/* no data is expected */
		if (conn->state == DN_CONNECTION_STATE_IDLE ||
				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			if (node->current_conn < --node->conn_count)
				node->connections[node->current_conn] = node->connections[node->conn_count];
			continue;
		}
		res = handle_response(conn, node);
		if (res == RESPONSE_EOF)
		{
			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT;
			timeout.tv_usec = 0;

			if (pgxc_node_receive(1, &conn, &timeout))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from Datanodes when ending query")));
		}
	}

	if (node->tuplestorestate != NULL)
		ExecClearTuple(node->ss.ss_ScanTupleSlot);
	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * If there are active cursors close them
	 */
	if (node->cursor || node->update_cursor)
	{
		PGXCNodeAllHandles *all_handles = NULL;
		PGXCNodeHandle    **cur_handles;
		bool bFree = false;
		int nCount;
		int i;

		cur_handles = node->cursor_connections;
		nCount = node->cursor_count;

		for(i=0;i<node->cursor_count;i++)
		{
			if (node->cursor_connections == NULL || node->cursor_connections[i]->sock == -1)
			{
				bFree = true;
				all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
				cur_handles = all_handles->datanode_handles;
				nCount = all_handles->dn_conn_count;
				break;
			}
		}

		if (node->cursor)
		{
			close_node_cursors(cur_handles, nCount, node->cursor);
			pfree(node->cursor);
			node->cursor = NULL;
		}

		if (node->update_cursor)
		{
			close_node_cursors(cur_handles, nCount, node->update_cursor);
			pfree(node->update_cursor);
			node->update_cursor = NULL;
		}

		if (bFree)
			pfree_pgxc_all_handles(all_handles);
	}

	/*
	 * Clean up parameters if they were set
	 */
	if (node->paramval_data)
	{
		pfree(node->paramval_data);
		node->paramval_data = NULL;
		node->paramval_len = 0;
	}

	/* Free the param types if they are newly allocated */
	if (node->rqs_param_types &&
	    node->rqs_param_types != ((RemoteQuery*)node->ss.ps.plan)->rq_param_types)
	{
		pfree(node->rqs_param_types);
		node->rqs_param_types = NULL;
		node->rqs_num_params = 0;
	}

	if (node->ss.ss_currentRelation)
		ExecCloseScanRelation(node->ss.ss_currentRelation);

	CloseCombiner(node);
}

static void
close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor)
{
	int i;
	RemoteQueryState *combiner;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], false, cursor) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		if (pgxc_node_send_sync(connections[i]) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				// Unexpected response, ignore?
			}
		}
	}

	ValidateAndCloseCombiner(combiner);
}


/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = 0;
	RemoteQuery *node = (RemoteQuery*) rq_state->ss.ps.plan;

	/* If there are no parameters, there is no data to BIND. */
	if (!paraminfo)
		return;

	/*
	 * If this query has been generated internally as a part of two-step DML
	 * statement, it uses only the internal parameters for input values taken
	 * from the source data, and it never uses external parameters. So even if
	 * parameters were being set externally, they won't be present in this
	 * statement (they might be present in the source data query). In such
	 * case where parameters refer to the values returned by SELECT query, the
	 * parameter data and parameter types would be set in SetDataRowForIntParams().
	 */
	if (node->rq_params_internal)
		return;

	Assert(!rq_state->paramval_data);

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	for (i = 0; i < paraminfo->numParams; i++)
	{
		ParamExternData *param;

		param = &paraminfo->params[i];

		if (!OidIsValid(param->ptype) && paraminfo->paramFetch != NULL)
			(*paraminfo->paramFetch) (paraminfo, i + 1);

		/*
		 * This is the last parameter found as useful, so we need
		 * to include all the previous ones to keep silent the remote
		 * nodes. All the parameters prior to the last usable having no
		 * type available will be considered as NULL entries.
		 */
		if (OidIsValid(param->ptype))
			real_num_params = i + 1;
	}

	/*
	 * If there are no parameters available, simply leave.
	 * This is possible in the case of a query called through SPI
	 * and using no parameters.
	 */
	if (real_num_params == 0)
	{
		rq_state->paramval_data = NULL;
		rq_state->paramval_len = 0;
		return;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < real_num_params; i++)
	{
		ParamExternData *param = &paraminfo->params[i];
		uint32 n32;

		/*
		 * Parameters with no types are considered as NULL and treated as integer
		 * The same trick is used for dropped columns for remote DML generation.
		 */
		if (param->isnull || !OidIsValid(param->ptype))
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;

			/* Get info needed to output the value */
			getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
		}
	}


	/*
	 * If parameter types are not already set, infer them from
	 * the paraminfo.
	 */
	if (node->rq_num_params > 0)
	{
		/*
		 * Use the already known param types for BIND. Parameter types
		 * can be already known when the same plan is executed multiple
		 * times.
		 */
		if (node->rq_num_params != real_num_params)
			elog(ERROR, "Number of user-supplied parameters do not match "
						"the number of remote parameters");
		rq_state->rqs_num_params = node->rq_num_params;
		rq_state->rqs_param_types = node->rq_param_types;
	}
	else
	{
		rq_state->rqs_num_params = real_num_params;
		rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
		for (i = 0; i < real_num_params; i++)
			rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;
}


/* ----------------------------------------------------------------
 *		ExecRemoteQueryReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt)
{
	/*
	 * If the materialized store is not empty, just rewind the stored output.
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (!node->tuplestorestate)
		return;

	tuplestore_rescan(node->tuplestorestate);
}


/*
 * Execute utility statement on multiple Datanodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void
ExecRemoteUtility(RemoteQuery *node)
{
	RemoteQueryState *remotestate;
	bool		force_autocommit = node->force_autocommit;
	RemoteQueryExecType exec_type = node->exec_type;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot snapshot = GetActiveSnapshot();
	PGXCNodeAllHandles *pgxc_connections;
	int			co_conn_count;
	int			dn_conn_count;
	bool		need_tran_block;
	ExecDirectType		exec_direct_type = node->exec_direct_type;
	int			i;

	if (!force_autocommit)
		RegisterTransactionLocalNode(true);

	/*
	 * It is possible to invoke create table with inheritance on
	 * temporary objects. Remember that we might have accessed a temp object
	 */
	if (node->is_temp)
		ExecSetTempObjectIncluded();

	remotestate = CreateResponseCombiner(0, node->combine_type);

	pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type);

	dn_conn_count = pgxc_connections->dn_conn_count;
	co_conn_count = pgxc_connections->co_conn_count;

	if (force_autocommit)
		need_tran_block = false;
	else
		need_tran_block = true;

	/* Commands launched through EXECUTE DIRECT do not need start a transaction */
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_tran_block = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

	gxid = GetCurrentTransactionId();
	if (!GlobalTransactionIdIsValid(gxid))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));

	if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
	{
		if (pgxc_node_begin(dn_conn_count, pgxc_connections->datanode_handles,
					gxid, need_tran_block, false, PGXC_NODE_DATANODE))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on Datanodes")));
		for (i = 0; i < dn_conn_count; i++)
		{
			PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

			if (conn->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(conn);
			if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to Datanodes")));
			}
			if (pgxc_node_send_query(conn, node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to Datanodes")));
			}
		}
	}

	if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)
	{
		if (pgxc_node_begin(co_conn_count, pgxc_connections->coord_handles,
					gxid, need_tran_block, false, PGXC_NODE_COORDINATOR))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on coordinators")));
		/* Now send it to Coordinators if necessary */
		for (i = 0; i < co_conn_count; i++)
		{
			if (snapshot && pgxc_node_send_snapshot(pgxc_connections->coord_handles[i], snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to coordinators")));
			}
			if (pgxc_node_send_query(pgxc_connections->coord_handles[i], node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to coordinators")));
			}
		}
	}

	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_DATANODES)
	{
		while (dn_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL))
				break;
			/*
			 * Handle input from the Datanodes.
			 * We do not expect Datanodes returning tuples when running utility
			 * command.
			 * If we got EOF, move to the next connection, will receive more
			 * data on the next iteration.
			 */
			while (i < dn_conn_count)
			{
				PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];
				int res = handle_response(conn, remotestate);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --dn_conn_count)
						pgxc_connections->datanode_handles[i] =
							pgxc_connections->datanode_handles[dn_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from Datanode")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from Datanode")));
				}
			}
		}
	}

	/* Make the same for Coordinators */
	if (exec_type == EXEC_ON_ALL_NODES ||
		exec_type == EXEC_ON_COORDS)
	{
		while (co_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(co_conn_count, pgxc_connections->coord_handles, NULL))
				break;

			while (i < co_conn_count)
			{
				int res = handle_response(pgxc_connections->coord_handles[i], remotestate);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					if (i < --co_conn_count)
						pgxc_connections->coord_handles[i] =
							 pgxc_connections->coord_handles[co_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
			}
		}
	}
	/*
	 * We have processed all responses from nodes and if we have
	 * error message pending we can report it. All connections should be in
	 * consistent state now and so they can be released to the pool after ROLLBACK.
	 */
	pgxc_node_report_error(remotestate);
}


/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Clean up prepared transactions before releasing connections */
	DropAllPreparedStatements();

	/* Release Datanode connections */
	release_handles();

	/* Disconnect from Pooler */
	PoolManagerDisconnect();

	/* Close connection with GTM */
	CloseGTM();
}

static int
pgxc_get_connections(PGXCNodeHandle *connections[], int size, List *connlist)
{
	ListCell *lc;
	int count = 0;

	foreach(lc, connlist)
	{
		PGXCNodeHandle *conn = (PGXCNodeHandle *) lfirst(lc);
		Assert (count < size);
		connections[count++] = conn;
	}
	return count;
}
/*
 * Get all connections for which we have an open transaction,
 * for both Datanodes and Coordinators
 */
static int
pgxc_get_transaction_nodes(PGXCNodeHandle *connections[], int size, bool write)
{
	return pgxc_get_connections(connections, size, write ? XactWriteNodes : XactReadNodes);
}

void
ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle	  **connections;
	RemoteQueryState   *combiner;
	int					conn_count;
	int 				i;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

	/* get needed Datanode connections */
	all_handles = get_handles(nodelist, NIL, false);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
		{
			/*
			 * statements are not affected by statement end, so consider
			 * unclosed statement on the Datanode as a fatal issue and
			 * force connection is discarded
			 */
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statemrnt")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
	}

	combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
		{
			for (i = 0; i <= conn_count; i++)
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
			}
		}
	}

	ValidateAndCloseCombiner(combiner);
	pfree_pgxc_all_handles(all_handles);
}

/*
 * DataNodeCopyInBinaryForAll
 *
 * In a COPY TO, send to all Datanodes PG_HEADER for a COPY TO in binary mode.
 */
int DataNodeCopyInBinaryForAll(char *msg_buf, int len, PGXCNodeHandle** copy_connections)
{
	int 		i;
	int 		conn_count = 0;
	PGXCNodeHandle *connections[NumDataNodes];
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	for (i = 0; i < NumDataNodes; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];

		if (!handle)
			continue;

		connections[conn_count++] = handle;
	}

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];
		if (handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, msg_buf, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid Datanode connection");
			return EOF;
		}
	}

	return 0;
}

/*
 * ExecSetTempObjectIncluded
 *
 * Remember that we have accessed a temporary object.
 */
void
ExecSetTempObjectIncluded(void)
{
	temp_object_included = true;
}

/*
 * ExecClearTempObjectIncluded
 *
 * Forget about temporary objects
 */
static void
ExecClearTempObjectIncluded(void)
{
	temp_object_included = false;
}

/* ExecIsTempObjectIncluded
 *
 * Check if a temporary object has been accessed
 */
bool
ExecIsTempObjectIncluded(void)
{
	return temp_object_included;
}

/*
 * ExecProcNodeDMLInXC
 *
 * This function is used by ExecInsert/Update/Delete to execute the
 * Insert/Update/Delete on the datanode using RemoteQuery plan.
 *
 * In XC, a non-FQSed UPDATE/DELETE is planned as a two step process
 * The first step selects the ctid & node id of the row to be modified and the
 * second step creates a parameterized query that is supposed to take the data
 * row returned by the lower plan node as the parameters to modify the affected
 * row. In case of an INSERT however the first step is used to get the new
 * column values to be inserted in the target table and the second step uses
 * those values as parameters of the INSERT query.
 *
 * We use extended query protocol to avoid repeated planning of the query and
 * pass the column values(in case of an INSERT) and ctid & xc_node_id
 * (in case of UPDATE/DELETE) as parameters while executing the query.
 *
 * Parameters:
 * resultRemoteRel:  The RemoteQueryState containing DML statement to be
 *					 executed
 * sourceDataSlot: The tuple returned by the first step (described above)
 *					 to be used as parameters in the second step.
 * newDataSlot: This has all the junk attributes stripped off from
 *				sourceDataSlot, plus BEFORE triggers may have modified the
 *				originally fetched data values. In other words, this has
 *				the final values that are to be sent to datanode through BIND.
 *
 * Returns the result of RETURNING clause if any
 */
TupleTableSlot *
ExecProcNodeDMLInXC(EState *estate,
	                TupleTableSlot *sourceDataSlot,
	                TupleTableSlot *newDataSlot)
{
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
	RemoteQueryState *resultRemoteRel = (RemoteQueryState *) estate->es_result_remoterel;
	ExprContext	*econtext = resultRemoteRel->ss.ps.ps_ExprContext;
	TupleTableSlot	*returningResultSlot = NULL;	/* RETURNING clause result */
	TupleTableSlot	*temp_slot;
	bool			dml_returning_on_replicated = false;
	RemoteQuery		*step = (RemoteQuery *) resultRemoteRel->ss.ps.plan;

	/*
	 * If the tuple returned by the previous step was null,
	 * simply return null tuple, no need to execute the DML
	 */
	if (TupIsNull(sourceDataSlot))
		return NULL;

	/*
	 * The current implementation of DMLs with RETURNING when run on replicated
	 * tables returns row from one of the datanodes. In order to achieve this
	 * ExecProcNode is repeatedly called saving one tuple and rejecting the rest.
	 * Do we have a DML on replicated table with RETURNING?
	 */
	dml_returning_on_replicated = IsReturningDMLOnReplicatedTable(step);

	/*
	 * Use data row returned by the previous step as parameter for
	 * the DML to be executed in this step.
	 */
	SetDataRowForIntParams(resultRelInfo->ri_junkFilter,
	                       sourceDataSlot, newDataSlot, resultRemoteRel);

	/*
	 * do_query calls get_exec_connections to determine target nodes
	 * at execution time. The function get_exec_connections can decide
	 * to evaluate en_expr to determine the target nodes. To evaluate en_expr,
	 * ExecEvalVar is called which picks up values from ecxt_scantuple if Var
	 * does not refer either OUTER or INNER varno. Hence we should copy the
	 * tuple returned by previous step in ecxt_scantuple if econtext is set.
	 * The econtext is set only when en_expr is set for execution time
	 * determination of the target nodes.
	 */

	if (econtext)
		econtext->ecxt_scantuple = newDataSlot;


	/*
	 * This loop would be required to reject tuples received from datanodes
	 * when a DML with RETURNING is run on a replicated table otherwise it
	 * would run once.
	 * PGXC_TODO: This approach is error prone if the DML statement constructed
	 * by the planner is such that it updates more than one row (even in case of
	 * non-replicated data). Fix it.
	 */
	do
	{
		temp_slot = ExecProcNode((PlanState *)resultRemoteRel);
		if (!TupIsNull(temp_slot))
		{
			/* Have we already copied the returned tuple? */
			if (returningResultSlot == NULL)
			{
				/* Copy the received tuple to be returned later */
				returningResultSlot = MakeSingleTupleTableSlot(temp_slot->tts_tupleDescriptor);
				returningResultSlot = ExecCopySlot(returningResultSlot, temp_slot);
			}
			/* Clear the received tuple, the copy required has already been saved */
			ExecClearTuple(temp_slot);
		}
		else
		{
			/* Null tuple received, so break the loop */
			ExecClearTuple(temp_slot);
			break;
		}
	} while (dml_returning_on_replicated);

	/*
	 * A DML can impact more than one row, e.g. an update without any where
	 * clause on a table with more than one row. We need to make sure that
	 * RemoteQueryNext calls do_query for each affected row, hence we reset
	 * the flag here and finish the DML being executed only when we return
	 * NULL from ExecModifyTable
	 */
	resultRemoteRel->query_Done = false;

	return returningResultSlot;
}

void
RegisterTransactionNodes(int count, void **connections, bool write)
{
	int i;
	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	for (i = 0; i < count; i++)
	{
		/*
		 * Add the node to either read or write participants. If a node is
		 * already in the write participant's list, don't add it to the read
		 * participant's list. OTOH if a node is currently in the read
		 * participant's list, but we are now initiating a write operation on
		 * the node, move it to the write participant's list
		 */
		if (write)
		{
			XactWriteNodes = list_append_unique(XactWriteNodes, connections[i]);
			XactReadNodes = list_delete(XactReadNodes, connections[i]);
		}
		else
		{
			if (!list_member(XactWriteNodes, connections[i]))
				XactReadNodes = list_append_unique(XactReadNodes, connections[i]);
		}
	}

	MemoryContextSwitchTo(oldcontext);
}

void
ForgetTransactionNodes(void)
{
	list_free(XactReadNodes);
	XactReadNodes = NIL;

	list_free(XactWriteNodes);
	XactWriteNodes = NIL;
}

/*
 * Clear per transaction remote information
 */
void
AtEOXact_Remote(void)
{
	ExecClearTempObjectIncluded();
	ForgetTransactionNodes();
	clear_RemoteXactState();
}

/*
 * Do pre-commit processing for remote nodes which includes Datanodes and
 * Coordinators. If more than one nodes are involved in the transaction write
 * activity, then we must run 2PC. For 2PC, we do the following steps:
 *
 *  1. PREPARE the transaction locally if the local node is involved in the
 *     transaction. If local node is not involved, skip this step and go to the
 *     next step
 *  2. PREPARE the transaction on all the remote nodes. If any node fails to
 *     PREPARE, directly go to step 6
 *  3. Now that all the involved nodes are PREPAREd, we can commit the
 *     transaction. We first inform the GTM that the transaction is fully
 *     PREPARED and also supply the list of the nodes involved in the
 *     transaction
 *  4. COMMIT PREPARED the transaction on all the remotes nodes and then
 *     finally COMMIT PREPARED on the local node if its involved in the
 *     transaction and start a new transaction so that normal commit processing
 *     works unchanged. Go to step 5.
 *  5. Return and let the normal commit processing resume
 *  6. Abort by ereporting the error and let normal abort-processing take
 *     charge.
 */
void
PreCommit_Remote(char *prepareGID, bool preparedLocalNode)
{
	if (!preparedLocalNode)
		PrePrepare_Remote(prepareGID, preparedLocalNode, false);

	/*
	 * OK, everything went fine. At least one remote node is in PREPARED state
	 * and the transaction is successfully prepared on all the involved nodes.
	 * Now we are ready to commit the transaction. We need a new GXID to send
	 * down the remote nodes to execute the forthcoming COMMIT PREPARED
	 * command. So grab one from the GTM and track it. It will be closed along
	 * with the main transaction at the end.
	 */
	pgxc_node_remote_commit();

	/*
	 * If the transaction is not committed successfully on all the involved
	 * nodes, it will remain in PREPARED state on those nodes. Such transaction
	 * should be be reported as live in the snapshots. So we must not close the
	 * transaction on the GTM. We just record the state of the transaction in
	 * the GTM and flag a warning for applications to take care of such
	 * in-doubt transactions
	 */
	if (remoteXactState.status == RXACT_PART_COMMITTED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to commit the transaction on one or more nodes")));


	Assert(remoteXactState.status == RXACT_COMMITTED ||
		   remoteXactState.status == RXACT_NONE);

	clear_RemoteXactState();

	/*
	 * The transaction is now successfully committed on all the remote nodes.
	 * (XXX How about the local node ?). It can now be cleaned up from the GTM
	 * as well
	 */
	if (!PersistentConnections)
		release_handles();
}

/*
 * Do abort processing for the transaction. We must abort the transaction on
 * all the involved nodes. If a node has already prepared a transaction, we run
 * ROLLBACK PREPARED command on the node. Otherwise, a simple ROLLBACK command
 * is sufficient.
 *
 * We must guard against the case when a transaction is prepared succefully on
 * all the nodes and some error occurs after we send a COMMIT PREPARED message
 * to at lease one node. Such a transaction must not be aborted to preserve
 * global consistency. We handle this case by recording the nodes involved in
 * the transaction at the GTM and keep the transaction open at the GTM so that
 * its reported as "in-progress" on all the nodes until resolved
 */
bool
PreAbort_Remote(void)
{
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		cancel_query();
		clear_all_data();
	}

	if (remoteXactState.status == RXACT_COMMITTED)
		return false;

	if (remoteXactState.status == RXACT_PART_COMMITTED)
	{
		/*
		 * In this case transaction is partially committed, pick up the list of nodes
		 * prepared and not committed and register them on GTM as if it is an explicit 2PC.
		 * This permits to keep the transaction alive in snapshot and other transaction
		 * don't have any side effects with partially committed transactions
		 */
		char	*nodestring = NULL;

		/*
		 * Get the list of nodes in prepared state; such nodes have not
		 * committed successfully
		 */
		nodestring = pgxc_node_get_nodelist(remoteXactState.preparedLocalNode);
		Assert(nodestring);

		/* Save the node list and gid on GTM. */
		StartPreparedTranGTM(GetTopGlobalTransactionId(),
				remoteXactState.prepareGID,
				nodestring);

		/* Finish to prepare the transaction. */
		PrepareTranGTM(GetTopGlobalTransactionId());
		clear_RemoteXactState();
		return false;
	}
	else
	{
		/*
		 * The transaction is neither part or fully committed. We can safely
		 * abort such transaction
		 */
		if (remoteXactState.status == RXACT_NONE)
			init_RemoteXactState(false);

		pgxc_node_remote_abort();
	}

	clear_RemoteXactState();

	if (!PersistentConnections)
		release_handles();

	return true;
}

char *
PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit)
{
	init_RemoteXactState(false);
	/*
	 * PREPARE the transaction on all nodes including remote nodes as well as
	 * local node. Any errors will be reported via ereport and the transaction
	 * will be aborted accordingly.
	 */
	pgxc_node_remote_prepare(prepareGID);

	if (preparedNodes)
		pfree(preparedNodes);
	preparedNodes = NULL;

	if (!implicit)
		preparedNodes = pgxc_node_get_nodelist(true);

	return preparedNodes;
}

void
PostPrepare_Remote(char *prepareGID, char *nodestring, bool implicit)
{
	remoteXactState.preparedLocalNode = true;

	/*
	 * If this is an explicit PREPARE request by the client, we must also save
	 * the list of nodes involved in this transaction on the GTM for later use
	 */
	if (!implicit)
	{
		/* Save the node list and gid on GTM. */
		StartPreparedTranGTM(GetTopGlobalTransactionId(),
				prepareGID,
				nodestring);

		/* Finish to prepare the transaction. */
		PrepareTranGTM(GetTopGlobalTransactionId());
		clear_RemoteXactState();
	}

	/* Now forget the transaction nodes */
	ForgetTransactionNodes();
}

/*
 * Return the list of nodes where the prepared transaction is not yet committed
 */
static char *
pgxc_node_get_nodelist(bool localNode)
{
	int i;
	char *nodestring = NULL, *nodename;

	for (i = 0; i < remoteXactState.numWriteRemoteNodes; i++)
	{
		RemoteXactNodeStatus status = remoteXactState.remoteNodeStatus[i];
		PGXCNodeHandle *conn = remoteXactState.remoteNodeHandles[i];

		if (status != RXACT_NODE_COMMITTED)
		{
			nodename = get_pgxc_nodename(conn->nodeoid);
			if (!nodestring)
			{
				nodestring = (char *) MemoryContextAlloc(TopMemoryContext, strlen(nodename) + 1);
				sprintf(nodestring, "%s", nodename);
			}
			else
			{
				nodestring = (char *) repalloc(nodestring,
											   strlen(nodename) + strlen(nodestring) + 2);
				sprintf(nodestring, "%s,%s", nodestring, nodename);
			}
		}
	}

	/* Case of a single Coordinator */
	if (localNode && PGXCNodeId >= 0)
	{
		if (!nodestring)
		{
			nodestring = (char *) MemoryContextAlloc(TopMemoryContext, strlen(PGXCNodeName) + 1);
			sprintf(nodestring, "%s", PGXCNodeName);
		}
		else
		{
			nodestring = (char *) repalloc(nodestring,
					strlen(PGXCNodeName) + strlen(nodestring) + 2);
			sprintf(nodestring, "%s,%s", nodestring, PGXCNodeName);
		}
	}

	return nodestring;
}

bool
IsTwoPhaseCommitRequired(bool localWrite)
{
	if ((list_length(XactWriteNodes) > 1) ||
		((list_length(XactWriteNodes) == 1) && localWrite))
	{
		if (ExecIsTempObjectIncluded())
		{
			elog(DEBUG1, "Transaction accessed temporary objects - "
					"2PC will not be used and that can lead to data inconsistencies "
					"in case of failures");
			return false;
		}
		return true;
	}
	else
		return false;
}

static void
clear_RemoteXactState(void)
{
	/* Clear the previous state */
	remoteXactState.numWriteRemoteNodes = 0;
	remoteXactState.numReadRemoteNodes = 0;
	remoteXactState.status = RXACT_NONE;
	remoteXactState.commitXid = InvalidGlobalTransactionId;
	remoteXactState.prepareGID[0] = '\0';

	if ((remoteXactState.remoteNodeHandles == NULL) ||
		(remoteXactState.maxRemoteNodes < (NumDataNodes + NumCoords)))
	{
		if (!remoteXactState.remoteNodeHandles)
			remoteXactState.remoteNodeHandles = (PGXCNodeHandle **)
				malloc(sizeof(PGXCNodeHandle *) * (MaxDataNodes + MaxCoords));
		else
			remoteXactState.remoteNodeHandles = (PGXCNodeHandle **)
				realloc(remoteXactState.remoteNodeHandles,
						sizeof(PGXCNodeHandle *) * (NumDataNodes + NumCoords));
		if (!remoteXactState.remoteNodeStatus)
			remoteXactState.remoteNodeStatus = (RemoteXactNodeStatus *)
				malloc(sizeof(RemoteXactNodeStatus) * (MaxDataNodes + MaxCoords));
		else
			remoteXactState.remoteNodeStatus = (RemoteXactNodeStatus *)
				realloc (remoteXactState.remoteNodeStatus,
						sizeof(RemoteXactNodeStatus) * (NumDataNodes + NumCoords));

		remoteXactState.maxRemoteNodes = NumDataNodes + NumCoords;
	}

	if (remoteXactState.remoteNodeHandles)
		memset(remoteXactState.remoteNodeHandles, 0,
				sizeof (PGXCNodeHandle *) * (NumDataNodes + NumCoords));
	if (remoteXactState.remoteNodeStatus)
		memset(remoteXactState.remoteNodeStatus, 0,
				sizeof (RemoteXactNodeStatus) * (NumDataNodes + NumCoords));
}

static void
init_RemoteXactState(bool preparedLocalNode)
{
	int write_conn_count, read_conn_count;
	PGXCNodeHandle	  **connections;

	clear_RemoteXactState();

	remoteXactState.preparedLocalNode = preparedLocalNode;
	connections = remoteXactState.remoteNodeHandles;

	Assert(connections);

	/*
	 * First get information about all the nodes involved in this transaction
	 */
	write_conn_count = pgxc_get_transaction_nodes(connections,
			NumDataNodes + NumCoords, true);
	remoteXactState.numWriteRemoteNodes = write_conn_count;

	read_conn_count = pgxc_get_transaction_nodes(connections + write_conn_count,
			NumDataNodes + NumCoords - write_conn_count, false);
	remoteXactState.numReadRemoteNodes = read_conn_count;

}

bool
FinishRemotePreparedTransaction(char *prepareGID, bool commit)
{
	char					*nodename, *nodestring;
	List					*nodelist = NIL, *coordlist = NIL;
	GlobalTransactionId		gxid, prepare_gxid;
	PGXCNodeAllHandles 		*pgxc_handles;
	bool					prepared_local = false;
	int						i;

	/*
	 * Please note that with xc_maintenance_mode = on, COMMIT/ROLLBACK PREPARED will not
	 * propagate to remote nodes. Only GTM status is cleaned up.
	 */
	if (xc_maintenance_mode)
	{
		if (commit)
		{
			pgxc_node_remote_commit();
			CommitPreparedTranGTM(prepare_gxid, gxid);
		}
		else
		{
			pgxc_node_remote_abort();
			RollbackTranGTM(prepare_gxid);
			RollbackTranGTM(gxid);
		}
		return false;
	}

	/*
	 * Get the list of nodes involved in this transaction.
	 *
	 * This function returns the GXID of the prepared transaction. It also
	 * returns a fresh GXID which can be used for running COMMIT PREPARED
	 * commands on the remote nodes. Both these GXIDs can then be either
	 * committed or aborted together.
	 *
	 * XXX While I understand that we get the prepared and a new GXID with a
	 * single call, it doesn't look nicer and create confusion. We should
	 * probably split them into two parts. This is used only for explicit 2PC
	 * which should not be very common in XC
	 */
	if (GetGIDDataGTM(prepareGID, &gxid, &prepare_gxid, &nodestring) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						prepareGID)));

	/*
	 * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
	 * remote nodes and also finish the transaction locally is required
	 */
	nodename = strtok(nodestring, ",");
	while (nodename != NULL)
	{
		Oid		nodeoid;
		int		nodeIndex;
		char	nodetype;

		nodeoid = get_pgxc_nodeoid(nodename);

		if (!OidIsValid(nodeoid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							nodename)));

		/* Get node type and index */
		nodetype = get_pgxc_nodetype(nodeoid);
		nodeIndex = PGXCNodeGetNodeId(nodeoid, get_pgxc_nodetype(nodeoid));

		/* Check if node is requested is the self-node or not */
		if (nodetype == PGXC_NODE_COORDINATOR)
		{
			if (nodeIndex == PGXCNodeId - 1)
				prepared_local = true;
			else
				coordlist = lappend_int(coordlist, nodeIndex);
		}
		else
			nodelist = lappend_int(nodelist, nodeIndex);

		nodename = strtok(NULL, ",");
	}

	/*
	 * Now get handles for all the involved Datanodes and the Coordinators
	 */
	pgxc_handles = get_handles(nodelist, coordlist, false);

	/*
	 * Send GXID (as received above) to the remote nodes.
	 if (pgxc_node_begin(pgxc_handles->dn_conn_count,
	 pgxc_handles->datanode_handles,
	 gxid, false, false))
	 ereport(ERROR,
	 (errcode(ERRCODE_INTERNAL_ERROR),
	 errmsg("Could not begin transaction on Datanodes")));
	*/
	RegisterTransactionNodes(pgxc_handles->dn_conn_count,
							 (void **) pgxc_handles->datanode_handles, true);

	/*
	  if (pgxc_node_begin(pgxc_handles->co_conn_count,
	  pgxc_handles->coord_handles,
	  gxid, false, false))
	  ereport(ERROR,
	  (errcode(ERRCODE_INTERNAL_ERROR),
	  errmsg("Could not begin transaction on coordinators")));
	*/
	RegisterTransactionNodes(pgxc_handles->co_conn_count,
							 (void **) pgxc_handles->coord_handles, true);

	/*
	 * Initialize the remoteXactState so that we can use the APIs to take care
	 * of commit/abort.
	 */
	init_RemoteXactState(prepared_local);
	remoteXactState.commitXid = gxid;

	/*
	 * At this point, most of the things are set up in remoteXactState except
	 * the state information for all the involved nodes. Force that now and we
	 * are ready to call the commit/abort API
	 */
	strcpy(remoteXactState.prepareGID, prepareGID);
	for (i = 0; i < remoteXactState.numWriteRemoteNodes; i++)
		remoteXactState.remoteNodeStatus[i] = RXACT_NODE_PREPARED;
	remoteXactState.status = RXACT_PREPARED;

	if (commit)
	{
		pgxc_node_remote_commit();
		CommitPreparedTranGTM(prepare_gxid, gxid);
	}
	else
	{
		pgxc_node_remote_abort();
		RollbackTranGTM(prepare_gxid);
		RollbackTranGTM(gxid);
	}

	/*
	 * The following is also only for usual operation.  With xc_maintenance_mode = on,
	 * no remote operation will be done here and no post-operation work is needed.
	 */
	clear_RemoteXactState();
	ForgetTransactionNodes();

	return prepared_local;
}

/*
 * pgxc_node_report_error
 * Throw error from Datanode if any.
 */
static void
pgxc_node_report_error(RemoteQueryState *combiner)
{
	/* If no combiner, nothing to do */
	if (!combiner)
		return;
	if (combiner->errorMessage)
	{
		char *code = combiner->errorCode;
		if (combiner->errorDetail != NULL)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					errmsg("%s", combiner->errorMessage), errdetail("%s", combiner->errorDetail) ));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					errmsg("%s", combiner->errorMessage)));
	}
}


/*
 * get_success_nodes:
 * Currently called to print a user-friendly message about
 * which nodes the query failed.
 * Gets all the nodes where no 'E' (error) messages were received; i.e. where the
 * query ran successfully.
 */
static ExecNodes *
get_success_nodes(int node_count, PGXCNodeHandle **handles, char node_type, StringInfo failednodes)
{
	ExecNodes *success_nodes = NULL;
	int i;

	for (i = 0; i < node_count; i++)
	{
		PGXCNodeHandle *handle = handles[i];
		int nodenum = PGXCNodeGetNodeId(handle->nodeoid, node_type);

		if (!handle->error)
		{
			if (!success_nodes)
				success_nodes = makeNode(ExecNodes);
			success_nodes->nodeList = lappend_int(success_nodes->nodeList, nodenum);
		}
		else
		{
			if (failednodes->len == 0)
				appendStringInfo(failednodes, "Error message received from nodes:");
			appendStringInfo(failednodes, " %s#%d",
				(node_type == PGXC_NODE_COORDINATOR ? "coordinator" : "datanode"),
				nodenum + 1);
		}
	}
	return success_nodes;
}

/*
 * pgxc_all_success_nodes: Uses get_success_nodes() to collect the
 * user-friendly message from coordinator as well as datanode.
 */
void
pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg)
{
	PGXCNodeAllHandles *connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES);
	StringInfoData failednodes;
	initStringInfo(&failednodes);

	*d_nodes = get_success_nodes(connections->dn_conn_count,
	                             connections->datanode_handles,
								 PGXC_NODE_DATANODE,
								 &failednodes);

	*c_nodes = get_success_nodes(connections->co_conn_count,
	                             connections->coord_handles,
								 PGXC_NODE_COORDINATOR,
								 &failednodes);

	if (failednodes.len == 0)
		*failednodes_msg = NULL;
	else
		*failednodes_msg = failednodes.data;
}


/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size)
{
	void *fparams;

	fparams = MemoryContextAlloc(TopMemoryContext, paraminfo_size);
	memcpy(fparams, paraminfo, paraminfo_size);

	dbcleanup_info.function = function;
	dbcleanup_info.fparams = fparams;
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void AtEOXact_DBCleanup(bool isCommit)
{
	if (dbcleanup_info.function)
		(*dbcleanup_info.function)(isCommit, dbcleanup_info.fparams);

	/*
	 * Just reset the callbackinfo. We anyway don't want this to be called again,
	 * until explicitly set.
	 */
	dbcleanup_info.function = NULL;
	if (dbcleanup_info.fparams)
	{
		pfree(dbcleanup_info.fparams);
		dbcleanup_info.fparams = NULL;
	}
}


/*
 * SetDataRowForIntParams: Form a BIND data row for internal parameters.
 * This function is called when the data for the parameters of remote
 * statement resides in some plan slot of an internally generated remote
 * statement rather than from some extern params supplied by the caller of the
 * query. Currently DML is the only case where we generate a query with
 * internal parameters.
 * The parameter data is constructed from the slot data, and stored in
 * RemoteQueryState.paramval_data.
 * At the same time, remote parameter types are inferred from the slot
 * tuple descriptor, and stored in RemoteQueryState.rqs_param_types.
 * On subsequent calls, these param types are re-used.
 * The data to be BOUND consists of table column data to be inserted/updated
 * and the ctid/nodeid values to be supplied for the WHERE clause of the
 * query. The data values are present in dataSlot whereas the ctid/nodeid
 * are available in sourceSlot as junk attributes.
 * sourceSlot is used only to retrieve ctid/nodeid, so it does not get
 * used for INSERTs, although it will never be NULL.
 * The slots themselves are undisturbed.
 */
static void
SetDataRowForIntParams(JunkFilter *junkfilter,
					   TupleTableSlot *sourceSlot, TupleTableSlot *dataSlot,
					   RemoteQueryState *rq_state)
{
	StringInfoData	buf;
	uint16			numparams = 0;
	RemoteQuery		*step = (RemoteQuery *) rq_state->ss.ps.plan;

	Assert(sourceSlot);

	/* Calculate the total number of parameters */
	if (step->rq_max_param_num > 0)
		numparams = step->rq_max_param_num;
	else if (dataSlot)
		numparams = dataSlot->tts_tupleDescriptor->natts;
	/* Add number of junk attributes */
	if (junkfilter)
	{
		if (junkfilter->jf_junkAttNo)
			numparams++;
		if (junkfilter->jf_xc_node_id)
			numparams++;
	}

	/*
	 * Infer param types from the slot tupledesc and junk attributes. But we
	 * have to do it only the first time: the interal parameters remain the same
	 * while processing all the source data rows because the data slot tupdesc
	 * never changes. Even though we can determine the internal param types
	 * during planning, we want to do it here: we don't want to set the param
	 * types and param data at two different places. Doing them together here
	 * helps us to make sure that the param types are in sync with the param
	 * data.
	 */

	/*
	 * We know the numparams, now initialize the param types if not already
	 * done. Once set, this will be re-used for each source data row.
	 */
	if (rq_state->rqs_num_params == 0)
	{
		int	attindex = 0;

		rq_state->rqs_num_params = numparams;
		rq_state->rqs_param_types =
			(Oid *) palloc(sizeof(Oid) * rq_state->rqs_num_params);

		if (dataSlot) /* We have table attributes to bind */
		{
			TupleDesc tdesc = dataSlot->tts_tupleDescriptor;
			int numatts = tdesc->natts;

			if (step->rq_max_param_num > 0)
				numatts = step->rq_max_param_num;

			for (attindex = 0; attindex < numatts; attindex++)
			{
				rq_state->rqs_param_types[attindex] =
					tdesc->attrs[attindex]->atttypid;
			}
		}
		if (junkfilter) /* Param types for specific junk attributes if present */
		{
			/* jf_junkAttNo always contains ctid */
			if (AttributeNumberIsValid(junkfilter->jf_junkAttNo))
				rq_state->rqs_param_types[attindex] = TIDOID;

			if (AttributeNumberIsValid(junkfilter->jf_xc_node_id))
				rq_state->rqs_param_types[attindex + 1] = INT4OID;
		}
	}
	else
	{
		Assert(rq_state->rqs_num_params == numparams);
	}

	/*
	 * If we already have the data row, just copy that, and we are done. One
	 * scenario where we can have the data row is for INSERT ... SELECT.
	 * Effectively, in this case, we just re-use the data row from SELECT as-is
	 * for BIND row of INSERT. But just make sure all of the data required to
	 * bind is available in the slot. If there are junk attributes to be added
	 * in the BIND row, we cannot re-use the data row as-is.
	 */
	if (!junkfilter && dataSlot && dataSlot->tts_dataRow)
	{
		rq_state->paramval_data = (char *)palloc(dataSlot->tts_dataLen);
		memcpy(rq_state->paramval_data, dataSlot->tts_dataRow, dataSlot->tts_dataLen);
		rq_state->paramval_len = dataSlot->tts_dataLen;
		return;
	}

	initStringInfo(&buf);

	{
		uint16 params_nbo = htons(numparams); /* Network byte order */
		appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
	}

	if (dataSlot)
	{
		TupleDesc	 	tdesc = dataSlot->tts_tupleDescriptor;
		int				attindex;
		int				numatts = tdesc->natts;

		/* Append the data attributes */

		if (step->rq_max_param_num > 0)
			numatts = step->rq_max_param_num;

		/* ensure we have all values */
		slot_getallattrs(dataSlot);
		for (attindex = 0; attindex < numatts; attindex++)
		{
			uint32 n32;
			Assert(attindex < numparams);

			if (dataSlot->tts_isnull[attindex])
			{
				n32 = htonl(-1);
				appendBinaryStringInfo(&buf, (char *) &n32, 4);
			}
			else
				pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], tdesc->attrs[attindex]->atttypid);

		}
	}

	/*
	 * From the source data, fetch the junk attribute values to be appended in
	 * the end of the data buffer. The junk attribute vals like ctid and
	 * xc_node_id are used in the WHERE clause parameters.
	 * These attributes would not be present for INSERT.
	 */
	if (junkfilter)
	{
		/* First one - jf_junkAttNo - always reprsents ctid */
		pgxc_append_param_junkval(sourceSlot, junkfilter->jf_junkAttNo,
								  TIDOID, &buf);
		pgxc_append_param_junkval(sourceSlot, junkfilter->jf_xc_node_id,
								  INT4OID, &buf);
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;

}


/*
 * pgxc_append_param_junkval:
 * Append into the data row the parameter whose value cooresponds to the junk
 * attributes in the source slot, namely ctid or node_id.
 */
static void
pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno,
						  Oid valtype, StringInfo buf)
{
	bool isNull;

	if (slot && attno != InvalidAttrNumber)
	{
		/* Junk attribute positions are saved by ExecFindJunkAttribute() */
		Datum val = ExecGetJunkAttribute(slot, attno, &isNull);
		/* shouldn't ever get a null result... */
		if (isNull)
			elog(ERROR, "NULL junk attribute");

		pgxc_append_param_val(buf, val, valtype);
	}
}

/*
 * pgxc_append_param_val:
 * Append the parameter value for the SET clauses of the UPDATE statement.
 * These values are the table attribute values from the dataSlot.
 */
static void
pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype)
{
	/* Convert Datum to string */
	char *pstring;
	int len;
	uint32 n32;
	Oid		typOutput;
	bool	typIsVarlena;

	/* Get info needed to output the value */
	getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
	/*
	 * If we have a toasted datum, forcibly detoast it here to avoid
	 * memory leakage inside the type's output routine.
	 */
	if (typIsVarlena)
		val = PointerGetDatum(PG_DETOAST_DATUM(val));

	pstring = OidOutputFunctionCall(typOutput, val);

	/* copy data to the buffer */
	len = strlen(pstring);
	n32 = htonl(len);
	appendBinaryStringInfo(buf, (char *) &n32, 4);
	appendBinaryStringInfo(buf, pstring, len);
}

/*
 * pgxc_rq_fire_bstriggers:
 * BEFORE STATEMENT triggers to be fired for a user-supplied DML query.
 * For non-FQS query, we internally generate remote DML query to be executed
 * for each row to be processed. But we do not want to explicitly fire triggers
 * for such a query; ExecModifyTable does that for us. It is the FQS DML query
 * where we need to explicitly fire statement triggers on coordinator. We
 * cannot run stmt triggers on datanode. While we can fire stmt trigger on
 * datanode versus coordinator based on the function shippability, we cannot
 * do the same for FQS query. The datanode has no knowledge that the trigger
 * being fired is due to a non-FQS query or an FQS query. Even though it can
 * find that all the triggers are shippable, it won't know whether the stmt
 * itself has been FQSed. Even though all triggers were shippable, the stmt
 * might have been planned on coordinator due to some other non-shippable
 * clauses. So the idea here is to *always* fire stmt triggers on coordinator.
 * Note that this does not prevent the query itself from being FQSed. This is
 * because we separately fire stmt triggers on coordinator.
 */
static void
pgxc_rq_fire_bstriggers(RemoteQueryState *node)
{
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/* If it's not an internally generated query, fire BS triggers */
	if (!rq->rq_params_internal && estate->es_result_relations)
	{
		Assert(rq->remote_query);
		switch (rq->remote_query->commandType)
		{
			case CMD_INSERT:
				ExecBSInsertTriggers(estate, estate->es_result_relations);
				break;
			case CMD_UPDATE:
				ExecBSUpdateTriggers(estate, estate->es_result_relations);
				break;
			case CMD_DELETE:
				ExecBSDeleteTriggers(estate, estate->es_result_relations);
				break;
			default:
				break;
		}
	}
}

/*
 * pgxc_rq_fire_astriggers:
 * AFTER STATEMENT triggers to be fired for a user-supplied DML query.
 * See comments in pgxc_rq_fire_astriggers()
 */
static void
pgxc_rq_fire_astriggers(RemoteQueryState *node)
{
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/* If it's not an internally generated query, fire AS triggers */
	if (!rq->rq_params_internal && estate->es_result_relations)
	{
		Assert(rq->remote_query);
		switch (rq->remote_query->commandType)
		{
			case CMD_INSERT:
				ExecASInsertTriggers(estate, estate->es_result_relations);
				break;
			case CMD_UPDATE:
				ExecASUpdateTriggers(estate, estate->es_result_relations);
				break;
			case CMD_DELETE:
				ExecASDeleteTriggers(estate, estate->es_result_relations);
				break;
			default:
				break;
		}
	}
}

/*
 * Flush PGXCNodeHandle cash to the coordinator until the amount of remaining data
 * becomes lower than the threshold.
 *
 * If datanode is too slow to handle data sent, retry to flush some of the buffered
 * data.
 */
static int flushPGXCNodeHandleData(PGXCNodeHandle *handle)
{
	int retry_no = 0;
	int wait_microsec = 0;
	size_t remaining;

	while (handle->outEnd > MAX_SIZE_TO_STOP_FLUSH)
	{
		remaining = handle->outEnd;
		if (send_some(handle, handle->outEnd) <0)
		{
			add_error_message(handle, "failed to send data to Datanode");
			return EOF;
		}
		if (remaining == handle->outEnd)
		{
			/* No data sent */
			retry_no++;
			wait_microsec = retry_no < 5 ? 0 : (retry_no < 35 ? 2^(retry_no / 5) : 128) * 1000;
			if (wait_microsec)
				pg_usleep(wait_microsec);
			continue;
		}
		else
		{
			/* Some data sent */
			retry_no = 0;
			wait_microsec = 0;
			continue;
		}
	}
	return 0;
}
