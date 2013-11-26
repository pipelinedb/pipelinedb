/*-------------------------------------------------------------------------
 *
 * gtm_txn.h
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
#ifndef _GTM_TXN_H
#define _GTM_TXN_H

#include "gtm/libpq-be.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_list.h"
#include "gtm/stringinfo.h"

/* ----------------
 *		Special transaction ID values
 *
 * BootstrapGlobalTransactionId is the XID for "bootstrap" operations, and
 * FrozenGlobalTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalGlobalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define BootstrapGlobalTransactionId		((GlobalTransactionId) 1)
#define FrozenGlobalTransactionId			((GlobalTransactionId) 2)
#define FirstNormalGlobalTransactionId	((GlobalTransactionId) 3)
#define MaxGlobalTransactionId			((GlobalTransactionId) 0xFFFFFFFF)

/* ----------------
 *		transaction ID manipulation macros
 * ----------------
 */
#define GlobalTransactionIdIsNormal(xid)		((xid) >= FirstNormalGlobalTransactionId)
#define GlobalTransactionIdEquals(id1, id2)	((id1) == (id2))
#define GlobalTransactionIdStore(xid, dest)	(*(dest) = (xid))
#define StoreInvalidGlobalTransactionId(dest) (*(dest) = InvalidGlobalTransactionId)

/* advance a transaction ID variable, handling wraparound correctly */
#define GlobalTransactionIdAdvance(dest)	\
	do { \
		(dest)++; \
		if ((dest) < FirstNormalGlobalTransactionId) \
			(dest) = FirstNormalGlobalTransactionId; \
	} while(0)

/* back up a transaction ID variable, handling wraparound correctly */
#define GlobalTransactionIdRetreat(dest)	\
	do { \
		(dest)--; \
	} while ((dest) < FirstNormalGlobalTransactionId)

typedef int XidStatus;

#define TRANSACTION_STATUS_IN_PROGRESS      0x00
#define TRANSACTION_STATUS_COMMITTED        0x01
#define TRANSACTION_STATUS_ABORTED          0x02

/*
 * prototypes for functions in transam/transam.c
 */
extern bool GlobalTransactionIdDidCommit(GlobalTransactionId transactionId);
extern bool GlobalTransactionIdDidAbort(GlobalTransactionId transactionId);
extern void GlobalTransactionIdAbort(GlobalTransactionId transactionId);
extern bool GlobalTransactionIdPrecedes(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdPrecedesOrEquals(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdFollows(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdFollowsOrEquals(GlobalTransactionId id1, GlobalTransactionId id2);

/* in transam/varsup.c */
extern GlobalTransactionId GTM_GetGlobalTransactionId(GTM_TransactionHandle handle);
extern GlobalTransactionId GTM_GetGlobalTransactionIdMulti(GTM_TransactionHandle handle[], int txn_count);
extern GlobalTransactionId ReadNewGlobalTransactionId(void);
extern void SetGlobalTransactionIdLimit(GlobalTransactionId oldest_datfrozenxid);
extern void SetNextGlobalTransactionId(GlobalTransactionId gxid);
extern void GTM_SetShuttingDown(void);

/* For restoration point backup */
extern bool GTM_NeedXidRestoreUpdate(void);
extern void GTM_WriteRestorePointXid(FILE *f);

typedef enum GTM_States
{
	GTM_STARTING,
	GTM_RUNNING,
	GTM_SHUTTING_DOWN
} GTM_States;

/* Global transaction states at the GTM */
typedef enum GTM_TransactionStates
{
	GTM_TXN_STARTING,
	GTM_TXN_IN_PROGRESS,
	GTM_TXN_PREPARE_IN_PROGRESS,
	GTM_TXN_PREPARED,
	GTM_TXN_COMMIT_IN_PROGRESS,
	GTM_TXN_COMMITTED,
	GTM_TXN_ABORT_IN_PROGRESS,
	GTM_TXN_ABORTED
} GTM_TransactionStates;

typedef struct GTM_TransactionInfo
{
	GTM_TransactionHandle	gti_handle;
	GTM_ThreadID			gti_thread_id;

	bool					gti_in_use;
	GlobalTransactionId		gti_gxid;
	GTM_TransactionStates	gti_state;
	char					*gti_coordname;
	GlobalTransactionId		gti_xmin;
	GTM_IsolationLevel		gti_isolevel;
	bool					gti_readonly;
	GTMProxy_ConnID			gti_backend_id;
	char					*nodestring; /* List of nodes prepared */
	char					*gti_gid;

	GTM_SnapshotData		gti_current_snapshot;
	bool					gti_snapshot_set;

	GTM_RWLock				gti_lock;
	bool					gti_vacuum;
} GTM_TransactionInfo;

#define GTM_MAX_2PC_NODES				16
/* By default a GID length is limited to 256 bits in PostgreSQL */
#define GTM_MAX_GID_LEN					256
#define GTM_MAX_NODESTRING_LEN			1024
#define GTM_CheckTransactionHandle(x)	((x) >= 0 && (x) < GTM_MAX_GLOBAL_TRANSACTIONS)
#define GTM_IsTransSerializable(x)		((x)->gti_isolevel == GTM_ISOLATION_SERIALIZABLE)

typedef struct GTM_Transactions
{
	uint32				gt_txn_count;
	GTM_States			gt_gtm_state;

	GTM_RWLock			gt_XidGenLock;

	/*
	 * These fields are protected by XidGenLock
	 */
	GlobalTransactionId gt_nextXid;		/* next XID to assign */
	GlobalTransactionId gt_backedUpXid;	/* backed up, restoration point */

	GlobalTransactionId gt_oldestXid;	/* cluster-wide minimum datfrozenxid */
	GlobalTransactionId gt_xidVacLimit;	/* start forcing autovacuums here */
	GlobalTransactionId gt_xidWarnLimit; /* start complaining here */
	GlobalTransactionId gt_xidStopLimit; /* refuse to advance nextXid beyond here */
	GlobalTransactionId gt_xidWrapLimit; /* where the world ends */

	/*
	 * These fields are protected by TransArrayLock.
	 */
	GlobalTransactionId gt_latestCompletedXid;	/* newest XID that has committed or
										 		 * aborted */

	GlobalTransactionId	gt_recent_global_xmin;

	int32				gt_lastslot;
	GTM_TransactionInfo	gt_transactions_array[GTM_MAX_GLOBAL_TRANSACTIONS];
	gtm_List			*gt_open_transactions;

	GTM_RWLock			gt_TransArrayLock;
} GTM_Transactions;

extern GTM_Transactions	GTMTransactions;

/*
 * This macro should be used with READ lock held on gt_TransArrayLock as the
 * number of open transactions might change when counting open transactions
 * if a lock is not hold.
 */
#define GTM_CountOpenTransactions()	(gtm_list_length(GTMTransactions.gt_open_transactions))

/*
 * Two hash tables will be maintained to quickly find the
 * GTM_TransactionInfo block given either the GXID or the GTM_TransactionHandle.
 */

GTM_TransactionInfo *GTM_HandleToTransactionInfo(GTM_TransactionHandle handle);
GTM_TransactionHandle GTM_GXIDToHandle(GlobalTransactionId gxid);
GTM_TransactionHandle GTM_GIDToHandle(char *gid);

/* Transaction Control */
void GTM_InitTxnManager(void);
GTM_TransactionHandle GTM_BeginTransaction(char *coord_name,
										   GTM_IsolationLevel isolevel,
										   bool readonly);
int GTM_BeginTransactionMulti(char *coord_name,
										   GTM_IsolationLevel isolevel[],
										   bool readonly[],
										   GTMProxy_ConnID connid[],
										   int txn_count,
										   GTM_TransactionHandle txns[]);
int GTM_RollbackTransaction(GTM_TransactionHandle txn);
int GTM_RollbackTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[]);
int GTM_RollbackTransactionGXID(GlobalTransactionId gxid);
int GTM_CommitTransaction(GTM_TransactionHandle txn);
int GTM_CommitTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[]);
int GTM_CommitTransactionGXID(GlobalTransactionId gxid);
int GTM_PrepareTransaction(GTM_TransactionHandle txn);
int GTM_StartPreparedTransaction(GTM_TransactionHandle txn,
								 char *gid,
								 char *nodestring);
int GTM_StartPreparedTransactionGXID(GlobalTransactionId gxid,
									 char *gid,
									 char *nodestring);
int GTM_GetGIDData(GTM_TransactionHandle prepared_txn,
				   GlobalTransactionId *prepared_gxid,
				   char **nodestring);
uint32 GTM_GetAllPrepared(GlobalTransactionId gxids[], uint32 gxidcnt);
GTM_TransactionStates GTM_GetStatus(GTM_TransactionHandle txn);
GTM_TransactionStates GTM_GetStatusGXID(GlobalTransactionId gxid);
int GTM_GetAllTransactions(GTM_TransactionInfo txninfo[], uint32 txncnt);
void GTM_RemoveAllTransInfos(int backend_id);

GTM_Snapshot GTM_GetSnapshotData(GTM_TransactionInfo *my_txninfo,
								 GTM_Snapshot snapshot);
GTM_Snapshot GTM_GetTransactionSnapshot(GTM_TransactionHandle handle[],
		int txn_count, int *status);
void GTM_FreeCachedTransInfo(void);

void ProcessBeginTransactionCommand(Port *myport, StringInfo message);
void ProcessBkupBeginTransactionCommand(Port *myport, StringInfo message);
void GTM_BkupBeginTransactionMulti(char *coord_name,
								   GTM_TransactionHandle *txn,
								   GTM_IsolationLevel *isolevel,
								   bool *readonly,
								   GTMProxy_ConnID *connid,
								   int	txn_count);

void ProcessBeginTransactionCommandMulti(Port *myport, StringInfo message);
void ProcessBeginTransactionGetGXIDCommand(Port *myport, StringInfo message);
void ProcessCommitTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessCommitPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessRollbackTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessStartPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessPrepareTransactionCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessGetGIDDataTransactionCommand(Port *myport, StringInfo message);
void ProcessGetGXIDTransactionCommand(Port *myport, StringInfo message);
void ProcessGXIDListCommand(Port *myport, StringInfo message);
void ProcessGetNextGXIDTransactionCommand(Port *myport, StringInfo message);

void ProcessBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message);
void ProcessBkupBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message);

void ProcessBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message);
void ProcessCommitTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup);
void ProcessRollbackTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup) ;

void GTM_SaveTxnInfo(FILE *ctlf);
void GTM_RestoreTxnInfo(FILE *ctlf, GlobalTransactionId next_gxid);
void GTM_BkupBeginTransaction(char *coord_name,
							  GTM_TransactionHandle txn,
							  GTM_IsolationLevel isolevel,
							  bool readonly);
void ProcessBkupBeginTransactionGetGXIDCommand(Port *myport, StringInfo message);
void ProcessBkupBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message);


/*
 * In gtm_snap.c
 */
void ProcessGetSnapshotCommand(Port *myport, StringInfo message, bool get_gxid);
void ProcessGetSnapshotCommandMulti(Port *myport, StringInfo message);
void GTM_FreeSnapshotData(GTM_Snapshot snapshot);
#endif
