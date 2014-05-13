/*-------------------------------------------------------------------------
 *
 * register.h
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
#ifndef GTM_REGISTER_H
#define GTM_REGISTER_H

#include "gtm/libpq-be.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_list.h"
#include "gtm/stringinfo.h"

/*
 * This structure represents the data that is saved each time a Postgres-XC node
 * registered on GTM.
 * It contains:
 *	-	Type of the Node: Proxy, Coordinator, Datanode
 *	-	Node number
 *	-	Proxy number: This ID number is set at 0 if node does not go through a Proxy
 *		or if Node Type is Proxy
 *	-	PostgreSQL port the node uses to communicate
 *	-	IP visible to GTM
 *	-	Data folder of the node
 */

typedef enum GTM_PGXCNodeStatus
{
	NODE_CONNECTED,
	NODE_DISCONNECTED
} GTM_PGXCNodeStatus;

typedef struct GTM_PGXCNodeInfo
{
	GTM_PGXCNodeType	type;		/* Type of node */
	char			*nodename;	/* Node Name */
	char			*proxyname;	/* Proxy name the node goes through */
	GTM_PGXCNodePort	port;		/* Port number of the node */
	char			*ipaddress;	/* IP address of the nodes */
	char			*datafolder;	/* Data folder of the node */
	GTM_PGXCNodeStatus	status;		/* Node status */
	GTM_RWLock		node_lock;	/* Lock on this structure */
	int			socket;		/* socket number used for registration */
} GTM_PGXCNodeInfo;

/* Maximum number of nodes that can be registered */
#define MAX_NODES 1024

size_t pgxcnode_get_all(GTM_PGXCNodeInfo **data, size_t maxlen);
size_t pgxcnode_find_by_type(GTM_PGXCNodeType type, GTM_PGXCNodeInfo **data, size_t maxlen);

int Recovery_PGXCNodeRegister(GTM_PGXCNodeType	type,
				char			*nodename,
				GTM_PGXCNodePort	port,
				char			*proxyname,
				GTM_PGXCNodeStatus	status,
				char			*ipaddress,
				char			*datafolder,
				bool			in_recovery,
				int			socket);
int Recovery_PGXCNodeUnregister(GTM_PGXCNodeType type,
								char *node_name,
								bool in_recovery,
								int socket);
int Recovery_PGXCNodeBackendDisconnect(GTM_PGXCNodeType type, char *nodename, int socket);

void Recovery_RecordRegisterInfo(GTM_PGXCNodeInfo *nodeinfo, bool is_register);
void Recovery_RestoreRegisterInfo(void);
void Recovery_SaveRegisterInfo(void);
void Recovery_PGXCNodeDisconnect(Port *myport);
void Recovery_SaveRegisterFileName(char *dir);

void ProcessPGXCNodeRegister(Port *myport, StringInfo message, bool is_backup);
void ProcessPGXCNodeUnregister(Port *myport, StringInfo message, bool is_backup);
void ProcessPGXCNodeBackendDisconnect(Port *myport, StringInfo message);
void ProcessPGXCNodeList(Port *myport, StringInfo message);

void ProcessGTMBeginBackup(Port *myport, StringInfo message);
void ProcessGTMEndBackup(Port *myport, StringInfo message);

#endif /* GTM_NODE_H */
