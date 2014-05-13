/*-------------------------------------------------------------------------
 *
 * gtm_proxy.h
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
#ifndef _GTM_PROXY_H
#define _GTM_PROXY_H

#include <setjmp.h>
#include <poll.h>

#include "gtm/gtm_c.h"
#include "gtm/palloc.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_conn.h"
#include "gtm/elog.h"
#include "gtm/gtm_list.h"
#include "gtm/gtm_msg.h"
#include "gtm/libpq-fe.h"

extern char *GTMProxyLogFile;

typedef enum GTMProxy_ThreadStatus
{
	GTM_PROXY_THREAD_STARTING,
	GTM_PROXY_THREAD_RUNNING,
	GTM_PROXY_THREAD_EXITING,
	/* Must be the last */
	GTM_PROXY_THREAD_INVALID
} GTMProxy_ThreadStatus;

typedef struct GTMProxy_ConnectionInfo
{
	/* Port contains all the vital information about this connection */
	Port				*con_port;
	struct GTMProxy_ThreadInfo	*con_thrinfo;
	bool				con_authenticated;
	bool				con_disconnected;
	GTMProxy_ConnID			con_id;

	GTM_MessageType			con_pending_msg;
	GlobalTransactionId 		con_txid;
	GTM_TransactionHandle		con_handle;
} GTMProxy_ConnectionInfo;

typedef struct GTMProxy_Connections
{
	uint32					gc_conn_count;
	uint32					gc_array_size;
	GTMProxy_ConnectionInfo	*gc_connections;
	GTM_RWLock				gc_lock;
} GTMProxy_Connections;

#define ERRORDATA_STACK_SIZE  20
#define GTM_PROXY_MAX_CONNECTIONS	1024

typedef struct GTMProxy_ThreadInfo
{
	/*
	 * Thread specific information such as connection(s) served by it
	 */
	GTM_ThreadID			thr_id;
	uint32					thr_localid;
	bool					is_main_thread;
	void * (* thr_startroutine)(void *);

	MemoryContext	thr_thread_context;
	MemoryContext	thr_message_context;
	MemoryContext	thr_current_context;
	MemoryContext	thr_error_context;
	MemoryContext	thr_parent_context;

	sigjmp_buf		*thr_sigjmp_buf;

	ErrorData		thr_error_data[ERRORDATA_STACK_SIZE];
	int				thr_error_stack_depth;
	int				thr_error_recursion_depth;
	int				thr_criticalsec_count;

	GTMProxy_ThreadStatus	thr_status;
	GTMProxy_ConnectionInfo	*thr_conn;		/* Current set of connections from clients */
	uint32					thr_conn_count;	/* number of connections served by this thread */


	/*
	 * The structure member type/sequence upto this point must match the
	 * GTM_ThreadInfo structure in gtm.h since they are shared in some common
	 * library routines such as elog.c. Keeping them in sync helps us use the
	 * same library for the proxy as well as the server.
	 */
	GTM_MutexLock			thr_lock;
	GTM_CV					thr_cv;

	/*
	 * We use a sequence number to track the state of connection/fd array.
	 * Whenever a new connection is added or an existing connection is deleted
	 * from the connection array, the sequence number is incremented. The
	 * thread main routine can then reconstruct the fd array again.
	 */
	int32					thr_seqno;

	/* connection array */
	GTMProxy_ConnectionInfo	*thr_all_conns[GTM_PROXY_MAX_CONNECTIONS];
	struct pollfd			thr_poll_fds[GTM_PROXY_MAX_CONNECTIONS];

	/* map info from ConnectionInfo->con_id to array index of thr_all_conns */
	int16					thr_conid2idx[GTM_PROXY_MAX_CONNECTIONS];

	/* Command backup */
	short					thr_any_backup[GTM_PROXY_MAX_CONNECTIONS];
	int						thr_qtype[GTM_PROXY_MAX_CONNECTIONS];
	StringInfoData			thr_inBufData[GTM_PROXY_MAX_CONNECTIONS];

	gtm_List 					*thr_processed_commands;
	gtm_List 					*thr_pending_commands[MSG_TYPE_COUNT];

	GTM_Conn				*thr_gtm_conn;		/* Connection to GTM */

	/* Reconnect Info */
	int						can_accept_SIGUSR2;
	int						reconnect_issued;
	int						can_longjmp;
	sigjmp_buf				longjmp_env;

} GTMProxy_ThreadInfo;

typedef struct GTMProxy_Threads
{
	uint32					gt_thread_count;
	uint32					gt_array_size;
	uint32					gt_next_worker;
	GTMProxy_ThreadInfo		**gt_threads;
	GTM_RWLock				gt_lock;
} GTMProxy_Threads;

extern GTMProxy_Threads *GTMProxyThreads;

int GTMProxy_ThreadAdd(GTMProxy_ThreadInfo *thrinfo);
int GTMProxy_ThreadRemove(GTMProxy_ThreadInfo *thrinfo);
int GTMProxy_ThreadJoin(GTMProxy_ThreadInfo *thrinfo);
void GTMProxy_ThreadExit(void);

extern GTMProxy_ThreadInfo *GTMProxy_ThreadCreate(void *(* startroutine)(void *), int idx);
extern GTMProxy_ThreadInfo * GTMProxy_GetThreadInfo(GTM_ThreadID thrid);
extern GTMProxy_ThreadInfo *GTMProxy_ThreadAddConnection(GTMProxy_ConnectionInfo *conninfo);
extern int GTMProxy_ThreadRemoveConnection(GTMProxy_ThreadInfo *thrinfo,
		GTMProxy_ConnectionInfo *conninfo);

/*
 * Command data - the only relevant information right now is the XID
 * and data necessary for registering (modification of Proxy number registered)
 */
typedef union GTMProxy_CommandData
{
	struct
	{
		bool			rdonly;
		GTM_IsolationLevel	iso_level;
	} cd_beg;

	struct
	{
		bool			isgxid;
		GlobalTransactionId	gxid;
		GTM_TransactionHandle	handle;
	} cd_rc;

	struct
	{
		bool			isgxid;
		GlobalTransactionId	gxid;
		GTM_TransactionHandle	handle;
	} cd_snap;

	struct
	{
		GTM_PGXCNodeType	type;
		char			*nodename;
		GTM_PGXCNodePort	port;
		char			*gtm_proxy_nodename;
		char			*datafolder;
		char			*ipaddress;
		GTM_PGXCNodeStatus	status;
	} cd_reg;
} GTMProxy_CommandData;

/*
 * Structures to be used for message proxing. There will be one such entry for
 * each pending command from a backend. To keep it simple, we have a separate
 * entry even if the commands are grouped together.
 *
 * An array of these entries is maintained which is sorted by the order in
 * which the commands are sent to the GTM server. We expect the GTM server to
 * respond back in the same order and the sorted array helps us in
 * matching/confirming the responses.
 */
typedef struct GTMProxy_CommandInfo
{
	GTM_MessageType			ci_mtype;
	int						ci_res_index;
	GTMProxy_CommandData	ci_data;
	GTMProxy_ConnectionInfo	*ci_conn;
} GTMProxy_CommandInfo;

/*
 * pthread keys to get thread specific information
 */
extern pthread_key_t					threadinfo_key;
extern MemoryContext					TopMostMemoryContext;
extern char								*GTMLogFile;
extern GTM_ThreadID						TopMostThreadID;

#define SetMyThreadInfo(thrinfo)		pthread_setspecific(threadinfo_key, (thrinfo))
#define GetMyThreadInfo					((GTMProxy_ThreadInfo *)pthread_getspecific(threadinfo_key))

#define TopMemoryContext		(GetMyThreadInfo->thr_thread_context)
#define ThreadTopContext		(GetMyThreadInfo->thr_thread_context)
#define MessageContext			(GetMyThreadInfo->thr_message_context)
#define CurrentMemoryContext	(GetMyThreadInfo->thr_current_context)
#define ErrorContext			(GetMyThreadInfo->thr_error_context)
#define errordata				(GetMyThreadInfo->thr_error_data)
#define recursion_depth			(GetMyThreadInfo->thr_error_recursion_depth)
#define errordata_stack_depth	(GetMyThreadInfo->thr_error_stack_depth)
#define CritSectionCount		(GetMyThreadInfo->thr_criticalsec_count)

#define PG_exception_stack		(GetMyThreadInfo->thr_sigjmp_buf)
#define MyConnection			(GetMyThreadInfo->thr_conn)
#define MyPort					((GetMyThreadInfo->thr_conn != NULL) ?	\
									GetMyThreadInfo->thr_conn->con_port :	\
									NULL)
#define MyThreadID				(GetMyThreadInfo->thr_id)

#define START_CRIT_SECTION()  (CritSectionCount++)

#define END_CRIT_SECTION() \
	do { \
		    Assert(CritSectionCount > 0); \
		    CritSectionCount--; \
	} while(0)

/* Signal Handler controller */
#define SIGUSR2DETECTED() (GetMyThreadInfo->reconnect_issued == TRUE)
#define RECONNECT_LONGJMP() do{longjmp(GetMyThreadInfo->longjmp_env, 1);}while(0)
#if 1
#define Disable_Longjmp() do{GetMyThreadInfo->can_longjmp = FALSE;}while(0)
#define Enable_Longjmp() \
	do{						 \
		if (SIGUSR2DETECTED()) { \
			RECONNECT_LONGJMP(); \
		} \
		else { \
			GetMyThreadInfo->can_longjmp = TRUE;	\
		} \
	} while(0)
#else
#define Disable_Longjmp()
#define Enable_Longjmp()
#endif

#endif
