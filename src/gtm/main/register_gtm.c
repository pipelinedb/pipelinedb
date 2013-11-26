/*-------------------------------------------------------------------------
 *
 * register.c
 *  PGXC Node Register on GTM and GTM Proxy, node registering functions
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

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/stringinfo.h"
#include "gtm/register.h"

#include "gtm/gtm_ip.h"

static void finishStandbyConn(GTM_ThreadInfo *thrinfo);
extern bool Backup_synchronously;

/*
 * Process MSG_NODE_REGISTER/MSG_BKUP_NODE_REGISTER message.
 *
 * is_backup indicates the message is MSG_BKUP_NODE_REGISTER.
 */
void
ProcessPGXCNodeRegister(Port *myport, StringInfo message, bool is_backup)
{
	GTM_PGXCNodeType	type;
	GTM_PGXCNodePort	port;
	char			remote_host[NI_MAXHOST];
	char			datafolder[NI_MAXHOST];
	char			node_name[NI_MAXHOST];
	char			proxyname[NI_MAXHOST];
	char			*ipaddress;
	MemoryContext		oldContext;
	int			len;
	StringInfoData		buf;
	GTM_PGXCNodeStatus	status;

	/* Read Node Type */
	memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
			sizeof (GTM_PGXCNodeType));

	 /* Read Node name */
	len = pq_getmsgint(message, sizeof (int));
	if (len >= NI_MAXHOST)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Invalid name length.")));

	memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
	node_name[len] = '\0';

	 /* Read Host name */
	len = pq_getmsgint(message, sizeof (int));
	memcpy(remote_host, (char *)pq_getmsgbytes(message, len), len);
	remote_host[len] = '\0';
	ipaddress = remote_host;

	/* Read Port Number */
	memcpy(&port, pq_getmsgbytes(message, sizeof (GTM_PGXCNodePort)),
			sizeof (GTM_PGXCNodePort));

	/* Read Proxy name (empty string if no proxy used) */
	len = pq_getmsgint(message, sizeof (GTM_StrLen));
	if (len >= NI_MAXHOST)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Invalid proxy name length.")));
	memcpy(proxyname, (char *)pq_getmsgbytes(message, len), len);
	proxyname[len] = '\0';

	/*
	 * Finish by reading Data Folder (length and then string)
	 */
	len = pq_getmsgint(message, sizeof (GTM_StrLen));

	memcpy(datafolder, (char *)pq_getmsgbytes(message, len), len);
	datafolder[len] = '\0';

	elog(DEBUG1,
		 "ProcessPGXCNodeRegister: ipaddress = \"%s\", node name = \"%s\", proxy name = \"%s\", "
		 "datafolder \"%s\"",
		 ipaddress, node_name, proxyname, datafolder);

	status = pq_getmsgint(message, sizeof (GTM_PGXCNodeStatus));

	if ((type!=GTM_NODE_GTM_PROXY) &&
		(type!=GTM_NODE_GTM_PROXY_POSTMASTER) &&
		(type!=GTM_NODE_COORDINATOR) &&
		(type!=GTM_NODE_DATANODE) &&
		(type!=GTM_NODE_GTM) &&
		(type!=GTM_NODE_DEFAULT))
		ereport(ERROR,
				(EINVAL,
				 errmsg("Unknown node type.")));

	elog(DEBUG1, "Node type = %d", type);

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	/*
	 * We don't check if the this is not in standby mode to allow
	 * cascaded standby.
	 */
	if (type == GTM_NODE_GTM)
	{
		elog(DEBUG1, "Registering GTM (Standby).  Unregister this first.");
		/*
		 * There's another standby.   May be failed one.
		 * Clean this up.  This means that we allow
		 * only one standby at the same time.
		 *
		 * This helps to give up failed standby and connect
		 * new one, regardless how they stopped.
		 *
		 * Be sure that all ther threads are locked by other
		 * means, typically by receiving MSG_BEGIN_BACKUP.
		 *
		 * First try to unregister GTM which is now connected.  We don't care
		 * if it failed.
		 */
		Recovery_PGXCNodeUnregister(type, node_name, false, -1);
		/*
		 * Then disconnect the connections to the standby from each thread.
		 * Please note that we assume only one standby is allowed at the same time.
		 * Cascade standby may be allowed.
		 */
		GTM_DoForAllOtherThreads(finishStandbyConn);

		GTMThreads->gt_standby_ready = true;
	}

	if (Recovery_PGXCNodeRegister(type, node_name, port,
								  proxyname, NODE_CONNECTED,
								  ipaddress, datafolder, false, myport->sock))
	{
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to Register node")));
	}

	/*
	 * We don't check if the this is not in standby mode to allow
	 * cascaded standby.
	 */
	if (type == GTM_NODE_GTM)
		GTMThreads->gt_standby_ready = true;

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	if (!is_backup)
	{
		/*
		 * Backup first
		 */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int _rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;
			GTM_PGXCNodeInfo *standbynode;

			elog(DEBUG1, "calling node_register_internal() for standby GTM %p.",
				 GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = bkup_node_register_internal(GetMyThreadInfo->thr_conn->standby,
											  type,
											  ipaddress,
											  port,
											  node_name,
											  datafolder,
											  status);

			elog(DEBUG1, "node_register_internal() returns rc %d.", _rc);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Now check if there're other standby registered. */
			standbynode = find_standby_node_info();
			if (!standbynode)
				GTMThreads->gt_standby_ready = false;

			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

		}
		/*
		 * Then, send a SUCCESS message back to the client
		 */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, NODE_REGISTER_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&type, sizeof(GTM_PGXCNodeType));
		/* Node name length */
		pq_sendint(&buf, strlen(node_name), 4);
		/* Node name (var-len) */
		pq_sendbytes(&buf, node_name, strlen(node_name));
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}
	}
}


/*
 * Process MSG_NODE_UNREGISTER/MSG_BKUP_NODE_UNREGISTER
 *
 * is_backup indiccates MSG_BKUP_NODE_UNREGISTER
 */
void
ProcessPGXCNodeUnregister(Port *myport, StringInfo message, bool is_backup)
{
	GTM_PGXCNodeType	type;
	MemoryContext		oldContext;
	StringInfoData		buf;
	int			len;
	char			node_name[NI_MAXHOST];

	/* Read Node Type and number */
	memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)),
			sizeof (GTM_PGXCNodeType));

	 /* Read Node name */
	len = pq_getmsgint(message, sizeof (int));
	if (len >= NI_MAXHOST)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Invalid node name length")));
	memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
	node_name[len] = '\0';

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeUnregister(type, node_name, false, myport->sock))
	{
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to Unregister node")));
	}

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);


	if (!is_backup)
	{
		/*
		 * Backup first
		 */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int _rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling node_unregister() for standby GTM %p.",
				 GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = bkup_node_unregister(GetMyThreadInfo->thr_conn->standby,
									   type,
									   node_name);


			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "node_unregister() returns rc %d.", _rc);
		}
		/*
		 * Send a SUCCESS message back to the client
		 */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, NODE_UNREGISTER_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&type, sizeof(GTM_PGXCNodeType));
		/* Node name length */
		pq_sendint(&buf, strlen(node_name), 4);
		/* Node name (var-len) */
		pq_sendbytes(&buf, node_name, strlen(node_name));

		pq_endmessage(myport, &buf);

		/* Flush standby before flush to the client */
		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}
	}
}

/*
 * Process MSG_NODE_LIST
 */
void
ProcessPGXCNodeList(Port *myport, StringInfo message)
{
	MemoryContext		oldContext;
	StringInfoData		buf;
	int			num_node = 13;
	int i;

	GTM_PGXCNodeInfo *data[MAX_NODES];
	char *s_data[MAX_NODES];
	size_t s_datalen[MAX_NODES];

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	memset(data, 0, sizeof(GTM_PGXCNodeInfo *) * MAX_NODES);
	memset(s_data, 0, sizeof(char *) * MAX_NODES);

	num_node = pgxcnode_get_all(data, MAX_NODES);

	for (i = 0; i < num_node; i++)
	{
		size_t s_len;

		s_len = gtm_get_pgxcnodeinfo_size(data[i]);

		/*
		 * Allocate memory blocks for serialized GTM_PGXCNodeInfo data.
		 */
		s_data[i] = (char *)malloc(s_len+1);
		memset(s_data[i], 0, s_len+1);

		s_datalen[i] = gtm_serialize_pgxcnodeinfo(data[i], s_data[i], s_len+1);

		elog(DEBUG1, "gtm_get_pgxcnodeinfo_size: s_len=%ld, s_datalen=%ld", s_len, s_datalen[i]);
	}

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	/*
	 * Send a SUCCESS message back to the client
	 */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, NODE_LIST_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendint(&buf, num_node, sizeof(int));   /* number of nodes */

	/*
	 * Send pairs of GTM_PGXCNodeInfo size and serialized GTM_PGXCNodeInfo body.
	 */
	for (i = 0; i < num_node; i++)
	{
		pq_sendint(&buf, s_datalen[i], sizeof(int));
		pq_sendbytes(&buf, s_data[i], s_datalen[i]);
	}

	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
		pq_flush(myport);

	/*
	 * Release memory blocks for the serialized data.
	 */
	for (i = 0; i < num_node; i++)
	{
		free(s_data[i]);
	}

	elog(DEBUG1, "ProcessPGXCNodeList() ok.");
}

static void
finishStandbyConn(GTM_ThreadInfo *thrinfo)
{
	if ((thrinfo->thr_conn != NULL) && (thrinfo->thr_conn->standby != NULL))
	{
		GTMPQfinish(thrinfo->thr_conn->standby);
		thrinfo->thr_conn->standby = NULL;
	}
}
