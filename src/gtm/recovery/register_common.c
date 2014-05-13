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

#define GTM_NODE_FILE			"register.node"
#define NODE_HASH_TABLE_SIZE	16
#define GTM_NODE_FILE_MAX_PATH	1024

typedef struct GTM_NodeInfoHashBucket
{
	gtm_List        *nhb_list;
	GTM_RWLock  nhb_lock;
} GTM_PGXCNodeInfoHashBucket;

static char GTMPGXCNodeFile[GTM_NODE_FILE_MAX_PATH];

/* Lock access of record file when necessary */
static GTM_RWLock RegisterFileLock;

static int NodeRegisterMagic = 0xeaeaeaea;
static int NodeUnregisterMagic = 0xebebebeb;
static int NodeEndMagic = 0xefefefef;

static GTM_PGXCNodeInfoHashBucket GTM_PGXCNodes[NODE_HASH_TABLE_SIZE];

static GTM_PGXCNodeInfo *pgxcnode_find_info(GTM_PGXCNodeType type, char *node_name);
static uint32 pgxcnode_gethash(char *nodename);
static int pgxcnode_remove_info(GTM_PGXCNodeInfo *node);
static int pgxcnode_add_info(GTM_PGXCNodeInfo *node);
static char *pgxcnode_copy_char(const char *str);

#define pgxcnode_type_equal(type1,type2) (type1 == type2)
#define pgxcnode_port_equal(port1,port2) (port1 == port2)

size_t
pgxcnode_get_all(GTM_PGXCNodeInfo **data, size_t maxlen)
{
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	int node = 0;
	int i;

	for (i = 0; i < NODE_HASH_TABLE_SIZE; i++)
	{
		bucket = &GTM_PGXCNodes[i];

		GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, bucket->nhb_list)
		{
			GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

			curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
			if (curr_nodeinfo != NULL)
			{
				data[node] = curr_nodeinfo;
				node++;
			}

			if (node == maxlen)
				break;
		}

		GTM_RWLockRelease(&bucket->nhb_lock);
	}

	return node;
}

size_t
pgxcnode_find_by_type(GTM_PGXCNodeType type, GTM_PGXCNodeInfo **data, size_t maxlen)
{
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	int node = 0;
	int i;

	for (i = 0; i < NODE_HASH_TABLE_SIZE; i++)
	{
		bucket = &GTM_PGXCNodes[i];

		GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, bucket->nhb_list)
		{
			GTM_PGXCNodeInfo *cur = NULL;

			cur = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);

			if (cur != NULL && cur->type == type)
			{
				data[node] = cur;
				elog(DEBUG1, "pgxcnode_find_by_type: cur=%p, ipaddress=%s", cur, cur->ipaddress);
				node++;
			}

			if (node == maxlen)
				break;
		}

		GTM_RWLockRelease(&bucket->nhb_lock);
	}

	return node;
}

/*
 * Find the pgxcnode info structure for the given node type and number key.
 */
static GTM_PGXCNodeInfo *
pgxcnode_find_info(GTM_PGXCNodeType type, char *node_name)
{
	uint32 hash = pgxcnode_gethash(node_name);
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	GTM_PGXCNodeInfo *curr_nodeinfo = NULL;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

	gtm_foreach(elem, bucket->nhb_list)
	{
		curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
		if (pgxcnode_type_equal(curr_nodeinfo->type, type) &&
			(strcmp(curr_nodeinfo->nodename, node_name) == 0))
			break;
		curr_nodeinfo = NULL;
	}

	GTM_RWLockRelease(&bucket->nhb_lock);

	return curr_nodeinfo;
}

/*
 * Get the Hash Key depending on the node name
 * We do not except to have hundreds of nodes yet,
 * This function could be replaced by a better one
 * such as a double hash function indexed on type and Node Name
 */
static uint32
pgxcnode_gethash(char *nodename)
{
	int			i;
	int			length;
	int			value;
	uint32			hash = 0;

	if (nodename == NULL || nodename == '\0')
	{
		return 0;
	}

	length = strlen(nodename);

	value = 0x238F13AF * length;

	for (i = 0; i < length; i++)
	{
		value = value + ((nodename[i] << i * 5 % 24) & 0x7fffffff);
	}

	hash = (1103515243 * value + 12345) % 65537 & 0x00000FFF;

	return (hash % NODE_HASH_TABLE_SIZE);
}

/*
 * Remove a PGXC Node Info structure from the global hash table
 */
static int
pgxcnode_remove_info(GTM_PGXCNodeInfo *nodeinfo)
{
	uint32 hash = pgxcnode_gethash(nodeinfo->nodename);
	GTM_PGXCNodeInfoHashBucket   *bucket;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_WRITE);
	GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

	bucket->nhb_list = gtm_list_delete(bucket->nhb_list, nodeinfo);

	GTM_RWLockRelease(&nodeinfo->node_lock);
	GTM_RWLockRelease(&bucket->nhb_lock);

    return 0;
}

/*
 * Add a PGXC Node info structure to the global hash table
 */
static int
pgxcnode_add_info(GTM_PGXCNodeInfo *nodeinfo)
{
	uint32 hash = pgxcnode_gethash(nodeinfo->nodename);
	GTM_PGXCNodeInfoHashBucket   *bucket;
	gtm_ListCell *elem;

	bucket = &GTM_PGXCNodes[hash];

	GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_WRITE);

	gtm_foreach(elem, bucket->nhb_list)
	{
		GTM_PGXCNodeInfo *curr_nodeinfo = NULL;
		curr_nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);

		/* GTM Proxy are always registered as they do not have Identification numbers yet */
		if (pgxcnode_type_equal(curr_nodeinfo->type, nodeinfo->type) &&
			(strcmp(curr_nodeinfo->nodename, nodeinfo->nodename) == 0))
		{
			if (curr_nodeinfo->status == NODE_CONNECTED)
			{
				GTM_RWLockRelease(&bucket->nhb_lock);
				ereport(LOG,
						(EEXIST,
						 errmsg("Node with the given ID number already exists")));
				return EEXIST;
			}
			else
			{
				/*
				 * Node has been disconnected abruptly.
				 * And we are sure that disconnections are not done by other node
				 * trying to use the same ID.
				 * So check if its data (port, datafolder and remote IP) has changed
				 * and modify it.
				 */
				if (!pgxcnode_port_equal(curr_nodeinfo->port, nodeinfo->port))
					curr_nodeinfo->port = nodeinfo->port;

				if (strlen(curr_nodeinfo->datafolder) == strlen(nodeinfo->datafolder))
				{
					if (memcpy(curr_nodeinfo->datafolder,
							   nodeinfo->datafolder,
							   strlen(nodeinfo->datafolder)) != 0)
					{
						pfree(curr_nodeinfo->ipaddress);
						curr_nodeinfo->ipaddress = nodeinfo->ipaddress;
					}
				}

				if (strlen(curr_nodeinfo->ipaddress) == strlen(nodeinfo->ipaddress))
				{
					if (memcpy(curr_nodeinfo->datafolder,
							   nodeinfo->datafolder,
							   strlen(nodeinfo->datafolder)) != 0)
					{
						pfree(curr_nodeinfo->datafolder);
						curr_nodeinfo->datafolder = nodeinfo->datafolder;
					}
				}

				/* Reconnect a disconnected node */
				curr_nodeinfo->status = NODE_CONNECTED;

				/* Set socket number with the new one */
				curr_nodeinfo->socket = nodeinfo->socket;
				GTM_RWLockRelease(&bucket->nhb_lock);
				return 0;
			}
		}
	}

	/*
	 * Safe to add the structure to the list
	 */
	bucket->nhb_list = gtm_lappend(bucket->nhb_list, nodeinfo);
	GTM_RWLockRelease(&bucket->nhb_lock);

    return 0;
}

/*
 * Makes a copy of given string in TopMostMemoryContext
 */
static char *
pgxcnode_copy_char(const char *str)
{
	char *retstr = NULL;

	/*
	 * We must use the TopMostMemoryContext because the node information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	retstr = (char *) MemoryContextAlloc(TopMostMemoryContext,
										 strlen(str) + 1);

	if (retstr == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	memcpy(retstr, str, strlen(str));
	retstr[strlen(str)] = '\0';

	return retstr;
}

/*
 * Unregister the given node
 */
int
Recovery_PGXCNodeUnregister(GTM_PGXCNodeType type, char *node_name, bool in_recovery, int socket)
{
	GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, node_name);

	if (nodeinfo != NULL)
	{
		/*
		 * Unregistration has to be made by the same connection as the one used for registration
		 * or the one that reconnected the node.
		 */
		pgxcnode_remove_info(nodeinfo);

		/* Add a record to file on disk saying that this node has been unregistered correctly */
		if (!in_recovery)
			Recovery_RecordRegisterInfo(nodeinfo, false);

		pfree(nodeinfo->nodename);
		pfree(nodeinfo->ipaddress);
		pfree(nodeinfo->datafolder);
		pfree(nodeinfo);
	}
	else
		return EINVAL;

	return 0;
}

int
Recovery_PGXCNodeRegister(GTM_PGXCNodeType	type,
						  char			*nodename,
						  GTM_PGXCNodePort	port,
						  char			*proxyname,
						  GTM_PGXCNodeStatus	status,
						  char			*ipaddress,
						  char			*datafolder,
						  bool			in_recovery,
						  int			socket)
{
	GTM_PGXCNodeInfo *nodeinfo = NULL;
	int errcode = 0;

	nodeinfo = (GTM_PGXCNodeInfo *) palloc(sizeof (GTM_PGXCNodeInfo));

	if (nodeinfo == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	GTM_RWLockInit(&nodeinfo->node_lock);

	/* Fill in structure */
	nodeinfo->type = type;
	if (nodename)
		nodeinfo->nodename = pgxcnode_copy_char(nodename);
	nodeinfo->port = port;
	if (proxyname)
		nodeinfo->proxyname = pgxcnode_copy_char(proxyname);
	if (datafolder)
		nodeinfo->datafolder = pgxcnode_copy_char(datafolder);
	if (ipaddress)
		nodeinfo->ipaddress = pgxcnode_copy_char(ipaddress);
	nodeinfo->status = status;
	nodeinfo->socket = socket;

	elog(DEBUG1, "Recovery_PGXCNodeRegister Request info: type=%d, nodename=%s, port=%d," \
			  "datafolder=%s, ipaddress=%s, status=%d",
			  type, nodename, port, datafolder, ipaddress, status);
	elog(DEBUG1, "Recovery_PGXCNodeRegister Node info: type=%d, nodename=%s, port=%d, "\
			  "datafolder=%s, ipaddress=%s, status=%d",
			  nodeinfo->type, nodeinfo->nodename, nodeinfo->port,
			  nodeinfo->datafolder, nodeinfo->ipaddress, nodeinfo->status);

	/* Add PGXC Node Info to the global hash table */
	errcode = pgxcnode_add_info(nodeinfo);

	/*
	 * Add a Record to file disk saying that this node
	 * with given data has been correctly registered
	 */
	if (!in_recovery && errcode == 0)
		Recovery_RecordRegisterInfo(nodeinfo, true);

	return errcode;
}



/*
 * Called at GTM shutdown, rewrite on disk register information
 * and write only data of nodes currently registered.
 */
void
Recovery_SaveRegisterInfo(void)
{
#define Write(x, y, z) do{if (write(x, y, z) != (z)) elog(WARNING, "Write error in Recovery_SaveRegisterInfo");}while(0)
	GTM_PGXCNodeInfoHashBucket *bucket;
	gtm_ListCell *elem;
	GTM_PGXCNodeInfo *nodeinfo = NULL;
	int hash, ctlfd;
	char filebkp[GTM_NODE_FILE_MAX_PATH];

	GTM_RWLockAcquire(&RegisterFileLock, GTM_LOCKMODE_WRITE);

	/* Create a backup file in case their is a problem during file writing */
	sprintf(filebkp, "%s.bkp", GTMPGXCNodeFile);

	ctlfd = open(filebkp, O_WRONLY | O_CREAT | O_TRUNC,
				 S_IRUSR | S_IWUSR);

	if (ctlfd < 0)
	{
		GTM_RWLockRelease(&RegisterFileLock);
		return;
	}

	for (hash = 0; hash < NODE_HASH_TABLE_SIZE; hash++)
	{
		bucket = &GTM_PGXCNodes[hash];

		GTM_RWLockAcquire(&bucket->nhb_lock, GTM_LOCKMODE_READ);

		/* Write one by one information about registered nodes */
		gtm_foreach(elem, bucket->nhb_list)
		{
			int len;

			nodeinfo = (GTM_PGXCNodeInfo *) gtm_lfirst(elem);
			if (nodeinfo == NULL)
				break;

			GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

			Write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));

			Write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));
			if (nodeinfo->nodename)
			{
				len = strlen(nodeinfo->nodename);
				Write(ctlfd, &len, sizeof(uint32));
				Write(ctlfd, nodeinfo->nodename, len);
			}
			else
			{
				len = 0;
				Write(ctlfd, &len, sizeof(uint32));
			}

			Write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));

			if (nodeinfo->proxyname)
			{
				len = strlen(nodeinfo->proxyname);
				Write(ctlfd, &len, sizeof(uint32));
				Write(ctlfd, nodeinfo->proxyname, len);
			}
			else
			{
				len = 0;
				Write(ctlfd, &len, sizeof(uint32));
			}

			Write(ctlfd, &nodeinfo->status, sizeof (GTM_PGXCNodeStatus));

			if (nodeinfo->ipaddress)
			{
				len = strlen(nodeinfo->ipaddress);
				Write(ctlfd, &len, sizeof(uint32));
				Write(ctlfd, nodeinfo->ipaddress, len);
			}
			else
			{
				len = 0;
				Write(ctlfd, &len, sizeof(uint32));
			}

			if (nodeinfo->datafolder)
			{
				len = strlen(nodeinfo->datafolder);
				Write(ctlfd, &len, sizeof(uint32));
				Write(ctlfd, nodeinfo->datafolder, len);
			}
			else
			{
				len = 0;
				Write(ctlfd, &len, sizeof(uint32));
			}

			Write(ctlfd, &NodeEndMagic, sizeof(NodeEndMagic));

			GTM_RWLockRelease(&nodeinfo->node_lock);
		}

		GTM_RWLockRelease(&bucket->nhb_lock);
	}

	close(ctlfd);

	/* Replace former file by backup file */
	if (rename(filebkp, GTMPGXCNodeFile) < 0)
	{
		elog(LOG, "Cannot save register file");
	}

	GTM_RWLockRelease(&RegisterFileLock);
#undef Write
}

/*
 * Add a Register or Unregister record on PGXC Node file on disk.
 */
void
Recovery_RecordRegisterInfo(GTM_PGXCNodeInfo *nodeinfo, bool is_register)
{
#define Write(x, y, z) do{if (write(x, y, z) != (z)) elog(WARNING, "Write error in Recovery_RecordRegisterInfo");}while(0)
	int ctlfd;
	int len;

	GTM_RWLockAcquire(&RegisterFileLock, GTM_LOCKMODE_WRITE);

	ctlfd = open(GTMPGXCNodeFile, O_WRONLY | O_CREAT | O_APPEND,
				 S_IRUSR | S_IWUSR);

	if (ctlfd == -1 || nodeinfo == NULL)
	{
		GTM_RWLockRelease(&RegisterFileLock);
		return;
	}

	GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_READ);

	if (is_register)
		Write(ctlfd, &NodeRegisterMagic, sizeof (NodeRegisterMagic));
	else
		Write(ctlfd, &NodeUnregisterMagic, sizeof (NodeUnregisterMagic));

	Write(ctlfd, &nodeinfo->type, sizeof (GTM_PGXCNodeType));

	if (nodeinfo->nodename)
	{
		len = strlen(nodeinfo->nodename);
		Write(ctlfd, &len, sizeof(uint32));
		Write(ctlfd, nodeinfo->nodename, len);
	}
	else
	{
		len = 0;
		Write(ctlfd, &len, sizeof(uint32));
	}

	if (is_register)
	{
		int len;

		Write(ctlfd, &nodeinfo->port, sizeof (GTM_PGXCNodePort));

		if (nodeinfo->proxyname)
		{
			len = strlen(nodeinfo->proxyname);
			Write(ctlfd, &len, sizeof(uint32));
			Write(ctlfd, nodeinfo->proxyname, len);
		}
		else
		{
			len = 0;
			Write(ctlfd, &len, sizeof(uint32));
		}

		Write(ctlfd, &nodeinfo->status, sizeof (GTM_PGXCNodeStatus));

		if (nodeinfo->ipaddress)
		{
			len = strlen(nodeinfo->ipaddress);
			Write(ctlfd, &len, sizeof(uint32));
			Write(ctlfd, nodeinfo->ipaddress, len);
		}
		else
		{
			len = 0;
			Write(ctlfd, &len, sizeof(uint32));
		}

		if (nodeinfo->datafolder)
		{
			len = strlen(nodeinfo->datafolder);
			Write(ctlfd, &len, sizeof(uint32));
			Write(ctlfd, nodeinfo->datafolder, len);
		}
		else
		{
			len = 0;
			Write(ctlfd, &len, sizeof(uint32));
		}
	}

	Write(ctlfd, &NodeEndMagic, sizeof(NodeEndMagic));

	GTM_RWLockRelease(&nodeinfo->node_lock);

	close(ctlfd);
	GTM_RWLockRelease(&RegisterFileLock);
#undef Write
}

void
Recovery_RestoreRegisterInfo(void)
{
	int magic;
	int ctlfd;

#define Read(x, y, z) do{if (read(x, y, z) != (z)) elog(WARNING, "Read error in Recovery_RestoreRegisterInfo");}while(0)

	/* This is made when GTM/Proxy restarts, so it is not necessary to take a lock */
	ctlfd = open(GTMPGXCNodeFile, O_RDONLY);

	if (ctlfd == -1)
		return;

	while (read(ctlfd, &magic, sizeof (NodeRegisterMagic)) == sizeof (NodeRegisterMagic))
	{
		GTM_PGXCNodeType	type;
		GTM_PGXCNodePort	port;
		GTM_PGXCNodeStatus	status;
		char			*ipaddress, *datafolder, *nodename, *proxyname;
		int			len;

		if (magic != NodeRegisterMagic && magic != NodeUnregisterMagic)
		{
			elog(WARNING, "Start magic mismatch %x", magic);
			break;
		}

		Read(ctlfd, &type, sizeof (GTM_PGXCNodeType));
		/* Read size of nodename string */
		Read(ctlfd, &len, sizeof (uint32));
		nodename = (char *) palloc(len);
		Read(ctlfd, nodename, len);

		if (magic == NodeRegisterMagic)
		{
			Read(ctlfd, &port, sizeof (GTM_PGXCNodePort));

			/* Read size of proxyname string */
			Read(ctlfd, &len, sizeof (uint32));
			proxyname = (char *) palloc(len);
			Read(ctlfd, proxyname, len);

			Read(ctlfd, &status, sizeof (GTM_PGXCNodeStatus));

			/* Read size of ipaddress string */
			Read(ctlfd, &len, sizeof (uint32));
			ipaddress = (char *) palloc(len);
			Read(ctlfd, ipaddress, len);

			/* Read size of datafolder string */
			Read(ctlfd, &len, sizeof (uint32));
			datafolder = (char *) palloc(len);
			Read(ctlfd, datafolder, len);
		}

		/* Rebuild based on the records */
		if (magic == NodeRegisterMagic)
			Recovery_PGXCNodeRegister(type, nodename, port, proxyname, status,
									  ipaddress, datafolder, true, 0);
		else
			Recovery_PGXCNodeUnregister(type, nodename, true, 0);

		Read(ctlfd, &magic, sizeof(NodeEndMagic));

		if (magic != NodeEndMagic)
		{
			elog(WARNING, "Corrupted control file");
				return;
		}
	}

	close(ctlfd);
#undef Read
}

void
Recovery_SaveRegisterFileName(char *dir)
{
	if (!dir)
		return;

	sprintf(GTMPGXCNodeFile, "%s/%s", dir, GTM_NODE_FILE);
}

/*
 * Disconnect node whose master connection has been cut with GTM
 */
void
Recovery_PGXCNodeDisconnect(Port *myport)
{
	GTM_PGXCNodeType	type = myport->remote_type;
	char			*nodename = myport->node_name;
	GTM_PGXCNodeInfo	*nodeinfo = NULL;
	MemoryContext		oldContext;

	/* Only a master connection can disconnect a node */
	if (!myport->is_postmaster)
		return;

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	nodeinfo = pgxcnode_find_info(type, nodename);

	if (nodeinfo != NULL)
	{
		/*
		 * Disconnection cannot be made with another socket than the one used for registration.
		 * socket may have a dummy value (-1) under GTM standby node.
		 */
		if (nodeinfo->socket >= 0 && myport->sock != nodeinfo->socket)
			return;

		GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

		nodeinfo->status = NODE_DISCONNECTED;
		nodeinfo->socket = 0;

		GTM_RWLockRelease(&nodeinfo->node_lock);
	}

	MemoryContextSwitchTo(oldContext);
}

int
Recovery_PGXCNodeBackendDisconnect(GTM_PGXCNodeType type, char *nodename, int socket)
{
	GTM_PGXCNodeInfo *nodeinfo = pgxcnode_find_info(type, nodename);
	int errcode = 0;


	if (nodeinfo != NULL)
	{
		/*
		 * A node can be only disconnected by the same connection as the one used for registration
		 * or reconnection.
		 */
		if (socket != nodeinfo->socket)
			return -1;

		GTM_RWLockAcquire(&nodeinfo->node_lock, GTM_LOCKMODE_WRITE);

		nodeinfo->status = NODE_DISCONNECTED;
		nodeinfo->socket = 0;

		GTM_RWLockRelease(&nodeinfo->node_lock);
	}
	else
		errcode = -1;

	return errcode;
}

/*
 * Process MSG_BACKEND_DISCONNECT
 *
 * A Backend has disconnected on a Proxy.
 * If this backend is postmaster, mark the referenced node as disconnected.
 */
void
ProcessPGXCNodeBackendDisconnect(Port *myport, StringInfo message)
{
	MemoryContext		oldContext;
	GTM_PGXCNodeType	type;
	bool			is_postmaster;
	char			node_name[NI_MAXHOST];
	int			len;

	is_postmaster = pq_getmsgbyte(message);

	if (is_postmaster)
	{
		/* Read Node Type and name */
		memcpy(&type, pq_getmsgbytes(message, sizeof (GTM_PGXCNodeType)), sizeof (GTM_PGXCNodeType));

		/* Read Node name */
		len = pq_getmsgint(message, sizeof (int));
		if (len >= NI_MAXHOST)
		{
			elog(LOG, "Invalid node name length %d", len);
			return;
		}
		memcpy(node_name, (char *)pq_getmsgbytes(message, len), len);
		node_name[len] = '\0';
	}

	pq_getmsgend(message);

	if (!is_postmaster)
		return; /* Nothing to do */

	/*
	 * We must use the TopMostMemoryContext because the Node ID information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if (Recovery_PGXCNodeBackendDisconnect(type, node_name, myport->sock) < 0)
	{
		elog(LOG, "Cannot disconnect Unregistered node");
	}

	MemoryContextSwitchTo(oldContext);

	/*
	 * Forwarding MSG_BACKEND_DISCONNECT message to GTM standby.
	 * No need to wait any response.
	 */
	if (GetMyThreadInfo->thr_conn->standby)
	{
		int _rc;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(DEBUG1, "forwarding MSG_BACKEND_DISCONNECT to standby GTM %p.",
				  GetMyThreadInfo->thr_conn->standby);

retry:
		_rc = backend_disconnect(GetMyThreadInfo->thr_conn->standby,
					 is_postmaster,
					 type,
					 node_name);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		elog(DEBUG1, "MSG_BACKEND_DISCONNECT rc=%d done.", _rc);
	}
}
