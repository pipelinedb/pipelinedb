/*-------------------------------------------------------------------------
 *
 * gtm_txn.c
 *	Transaction handling
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
#include "gtm/gtm_txn.h"

#include <unistd.h>
#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_time.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/standby_utils.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_backup.h"

extern bool Backup_synchronously;

/* Local functions */
static XidStatus GlobalTransactionIdGetStatus(GlobalTransactionId transactionId);
static bool GTM_SetDoVacuum(GTM_TransactionHandle handle);
static void init_GTM_TransactionInfo(GTM_TransactionInfo *gtm_txninfo,
									 char *coord_name,
									 GTM_TransactionHandle txn,
									 GTM_IsolationLevel isolevel,
									 GTMProxy_ConnID connid,
									 bool readonly);
static void clean_GTM_TransactionInfo(GTM_TransactionInfo *gtm_txninfo);
GTM_Transactions GTMTransactions;

void
GTM_InitTxnManager(void)
{
	int ii;

	memset(&GTMTransactions, 0, sizeof (GTM_Transactions));

	for (ii = 0; ii < GTM_MAX_GLOBAL_TRANSACTIONS; ii++)
	{
		GTM_TransactionInfo *gtm_txninfo = &GTMTransactions.gt_transactions_array[ii];
		gtm_txninfo->gti_in_use = false;
		GTM_RWLockInit(&gtm_txninfo->gti_lock);
	}

	/*
	 * XXX When GTM is stopped and restarted, it must start assinging GXIDs
	 * greater than the previously assgined values. If it was a clean shutdown,
	 * the GTM can store the last assigned value at a known location on
	 * permanent storage and read it back when it's restarted. It will get
	 * trickier for GTM failures.
	 *
	 * TODO We skip this part for the prototype.
	 */
	GTMTransactions.gt_nextXid = FirstNormalGlobalTransactionId;

	/*
	 * XXX The gt_oldestXid is the cluster level oldest Xid
	 */
	GTMTransactions.gt_oldestXid = FirstNormalGlobalTransactionId;

	/*
	 * XXX Compute various xid limits to avoid wrap-around related database
	 * corruptions. Again, this is not implemented for the prototype
	 */
	GTMTransactions.gt_xidVacLimit = InvalidGlobalTransactionId;
	GTMTransactions.gt_xidWarnLimit = InvalidGlobalTransactionId;
	GTMTransactions.gt_xidStopLimit = InvalidGlobalTransactionId;
	GTMTransactions.gt_xidWrapLimit = InvalidGlobalTransactionId;

	/*
	 * XXX Newest XID that is committed or aborted
	 */
	GTMTransactions.gt_latestCompletedXid = FirstNormalGlobalTransactionId;

	/*
	 * Initialize the locks to protect various XID fields as well as the linked
	 * list of transactions
	 */
	GTM_RWLockInit(&GTMTransactions.gt_XidGenLock);
	GTM_RWLockInit(&GTMTransactions.gt_TransArrayLock);

	/*
	 * Initialize the list
	 */
	GTMTransactions.gt_open_transactions = gtm_NIL;
	GTMTransactions.gt_lastslot = -1;

	GTMTransactions.gt_gtm_state = GTM_STARTING;

	return;
}

/*
 * Get the status of current or past transaction.
 */
static XidStatus
GlobalTransactionIdGetStatus(GlobalTransactionId transactionId)
{
	XidStatus	xidstatus = TRANSACTION_STATUS_IN_PROGRESS;

	/*
	 * Also, check to see if the transaction ID is a permanent one.
	 */
	if (!GlobalTransactionIdIsNormal(transactionId))
	{
		if (GlobalTransactionIdEquals(transactionId, BootstrapGlobalTransactionId))
			return TRANSACTION_STATUS_COMMITTED;
		if (GlobalTransactionIdEquals(transactionId, FrozenGlobalTransactionId))
			return TRANSACTION_STATUS_COMMITTED;
		return TRANSACTION_STATUS_ABORTED;
	}

	/*
	 * TODO To be implemented
	 * This code is not completed yet and the latter code must not be reached.
	 */
	Assert(0);
	return xidstatus;
}

/*
 * Given the GXID, find the corresponding transaction handle.
 */
GTM_TransactionHandle
GTM_GXIDToHandle(GlobalTransactionId gxid)
{
	gtm_ListCell *elem = NULL;
   	GTM_TransactionInfo *gtm_txninfo = NULL;

	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);

	gtm_foreach(elem, GTMTransactions.gt_open_transactions)
	{
		gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
		if (GlobalTransactionIdEquals(gtm_txninfo->gti_gxid, gxid))
			break;
		gtm_txninfo = NULL;
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

	if (gtm_txninfo != NULL)
		return gtm_txninfo->gti_handle;
	else
	{
		ereport(WARNING,
				(ERANGE, errmsg("No transaction handle for gxid: %d",
								gxid)));
		return InvalidTransactionHandle;
	}
}

/*
 * Given the GID (for a prepared transaction), find the corresponding
 * transaction handle.
 */
GTM_TransactionHandle
GTM_GIDToHandle(char *gid)
{
	gtm_ListCell *elem = NULL;
	GTM_TransactionInfo *gtm_txninfo = NULL;

	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_READ);

	gtm_foreach(elem, GTMTransactions.gt_open_transactions)
	{
		gtm_txninfo = (GTM_TransactionInfo *)gtm_lfirst(elem);
		if (gtm_txninfo->gti_gid && strcmp(gid,gtm_txninfo->gti_gid) == 0)
			break;
		gtm_txninfo = NULL;
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

	if (gtm_txninfo != NULL)
		return gtm_txninfo->gti_handle;
	else
		return InvalidTransactionHandle;
}


/*
 * Given the transaction handle, find the corresponding transaction info
 * structure
 *
 * Note: Since a transaction handle is just an index into the global array,
 * this function should be very quick. We should turn into an inline future for
 * fast path.
 */
GTM_TransactionInfo *
GTM_HandleToTransactionInfo(GTM_TransactionHandle handle)
{
	GTM_TransactionInfo *gtm_txninfo = NULL;

	if ((handle < 0) || (handle > GTM_MAX_GLOBAL_TRANSACTIONS))
	{
		ereport(WARNING,
				(ERANGE, errmsg("Invalid transaction handle: %d", handle)));
		return NULL;
	}

	gtm_txninfo = &GTMTransactions.gt_transactions_array[handle];

	if (!gtm_txninfo->gti_in_use)
	{
		ereport(WARNING,
				(ERANGE, errmsg("Invalid transaction handle, txn_info not in use")));
		return NULL;
	}

	return gtm_txninfo;
}


/*
 * Remove the given transaction info structures from the global array. If the
 * calling thread does not have enough cached structures, we in fact keep the
 * structure in the global array and also add it to the list of cached
 * structures for this thread. This ensures that the next transaction starting
 * in this thread can quickly get a free slot in the array of transactions and
 * also avoid repeated malloc/free of the structures.
 *
 * Also compute the latestCompletedXid.
 */
static void
GTM_RemoveTransInfoMulti(GTM_TransactionInfo *gtm_txninfo[], int txn_count)
{
	int ii;

	/*
	 * Remove the transaction structure from the global list of open
	 * transactions
	 */
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	for (ii = 0; ii < txn_count; ii++)
	{
		if (gtm_txninfo[ii] == NULL)
			continue;

		GTMTransactions.gt_open_transactions = gtm_list_delete(GTMTransactions.gt_open_transactions, gtm_txninfo[ii]);

		if (GlobalTransactionIdIsNormal(gtm_txninfo[ii]->gti_gxid) &&
			GlobalTransactionIdFollowsOrEquals(gtm_txninfo[ii]->gti_gxid,
											   GTMTransactions.gt_latestCompletedXid))
			GTMTransactions.gt_latestCompletedXid = gtm_txninfo[ii]->gti_gxid;

		elog(DEBUG1, "GTM_RemoveTransInfoMulti: removing transaction id %u, %lu",
				gtm_txninfo[ii]->gti_gxid, gtm_txninfo[ii]->gti_thread_id);

		/*
		 * Now mark the transaction as aborted and mark the structure as not-in-use
		 */
		clean_GTM_TransactionInfo(gtm_txninfo[ii]);
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	return;
}

/*
 * Remove all transaction infos associated with the caller thread and the given
 * backend
 *
 * Also compute the latestCompletedXid.
 */
void
GTM_RemoveAllTransInfos(int backend_id)
{
	gtm_ListCell *cell, *prev;
	GTM_ThreadID thread_id;

	thread_id = pthread_self();

	/*
	 * Scan the global list of open transactions
	 */
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);
	prev = NULL;
	cell = gtm_list_head(GTMTransactions.gt_open_transactions);
	while (cell != NULL)
	{
		GTM_TransactionInfo *gtm_txninfo = gtm_lfirst(cell);
		/*
		 * Check if current entry is associated with the thread
		 * A transaction in prepared state has to be kept alive in the structure.
		 * It will be committed by another thread than this one.
		 */
		if ((gtm_txninfo->gti_in_use) &&
			(gtm_txninfo->gti_state != GTM_TXN_PREPARED) &&
			(gtm_txninfo->gti_state != GTM_TXN_PREPARE_IN_PROGRESS) &&
			(gtm_txninfo->gti_thread_id == thread_id) &&
			((gtm_txninfo->gti_backend_id == backend_id) || (backend_id == -1)))
		{
			/* remove the entry */
			GTMTransactions.gt_open_transactions = gtm_list_delete_cell(GTMTransactions.gt_open_transactions, cell, prev);

			/* update the latestCompletedXid */
			if (GlobalTransactionIdIsNormal(gtm_txninfo->gti_gxid) &&
				GlobalTransactionIdFollowsOrEquals(gtm_txninfo->gti_gxid,
												   GTMTransactions.gt_latestCompletedXid))
				GTMTransactions.gt_latestCompletedXid = gtm_txninfo->gti_gxid;

			elog(DEBUG1, "GTM_RemoveAllTransInfos: removing transaction id %u, %lu:%lu",
					gtm_txninfo->gti_gxid, gtm_txninfo->gti_thread_id, thread_id);
			/*
			 * Now mark the transaction as aborted and mark the structure as not-in-use
			 */
			clean_GTM_TransactionInfo(gtm_txninfo);

			/* move to next cell in the list */
			if (prev)
				cell = gtm_lnext(prev);
			else
				cell = gtm_list_head(GTMTransactions.gt_open_transactions);
		}
		else
		{
			prev = cell;
			cell = gtm_lnext(cell);
		}
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	return;
}
/*
 * GlobalTransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool							/* true if given transaction committed */
GlobalTransactionIdDidCommit(GlobalTransactionId transactionId)
{
	XidStatus	xidstatus;

	xidstatus = GlobalTransactionIdGetStatus(transactionId);

	/*
	 * If it's marked committed, it's committed.
	 */
	if (xidstatus == TRANSACTION_STATUS_COMMITTED)
		return true;

	/*
	 * It's not committed.
	 */
	return false;
}

/*
 * GlobalTransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool							/* true if given transaction aborted */
GlobalTransactionIdDidAbort(GlobalTransactionId transactionId)
{
	XidStatus	xidstatus;

	xidstatus = GlobalTransactionIdGetStatus(transactionId);

	/*
	 * If it's marked aborted, it's aborted.
	 */
	if (xidstatus == TRANSACTION_STATUS_ABORTED)
		return true;

	/*
	 * It's not aborted.
	 */
	return false;
}

/*
 * GlobalTransactionIdPrecedes --- is id1 logically < id2?
 */
bool
GlobalTransactionIdPrecedes(GlobalTransactionId id1, GlobalTransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.	If both are normal, do a modulo-2^31 comparison.
	 */
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}

/*
 * GlobalTransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
bool
GlobalTransactionIdPrecedesOrEquals(GlobalTransactionId id1, GlobalTransactionId id2)
{
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 <= id2);

	diff = (int32) (id1 - id2);
	return (diff <= 0);
}

/*
 * GlobalTransactionIdFollows --- is id1 logically > id2?
 */
bool
GlobalTransactionIdFollows(GlobalTransactionId id1, GlobalTransactionId id2)
{
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 > id2);

	diff = (int32) (id1 - id2);
	return (diff > 0);
}

/*
 * GlobalTransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
bool
GlobalTransactionIdFollowsOrEquals(GlobalTransactionId id1, GlobalTransactionId id2)
{
	int32		diff;

	if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
		return (id1 >= id2);

	diff = (int32) (id1 - id2);
	return (diff >= 0);
}


/*
 * Set that the transaction is doing vacuum
 *
 */
static bool
GTM_SetDoVacuum(GTM_TransactionHandle handle)
{
	GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(handle);

	if (gtm_txninfo == NULL)
		ereport(ERROR, (EINVAL, errmsg("Invalid transaction handle")));

	gtm_txninfo->gti_vacuum = true;
	return true;
}

/*
 * Allocate the next XID for my new transaction
 *
 * The new XID is also stored into the transaction info structure of the given
 * transaction before returning.
 */
GlobalTransactionId
GTM_GetGlobalTransactionIdMulti(GTM_TransactionHandle handle[], int txn_count)
{
	GlobalTransactionId xid, start_xid = InvalidGlobalTransactionId;
	GTM_TransactionInfo *gtm_txninfo = NULL;
	int ii;

	if (Recovery_IsStandby())
	{
		ereport(ERROR, (EINVAL, errmsg("GTM is running in STANDBY mode -- can not issue new transaction ids")));
		return InvalidGlobalTransactionId;
	}

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);

	if (GTMTransactions.gt_gtm_state == GTM_SHUTTING_DOWN)
	{
		GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
		ereport(ERROR, (EINVAL, errmsg("GTM shutting down -- can not issue new transaction ids")));
		return InvalidGlobalTransactionId;
	}


	/*
	 * If we are allocating the first XID of a new page of the commit log,
	 * zero out that commit-log page before returning. We must do this while
	 * holding XidGenLock, else another xact could acquire and commit a later
	 * XID before we zero the page.  Fortunately, a page of the commit log
	 * holds 32K or more transactions, so we don't have to do this very often.
	 *
	ExtendCLOG(xid);
	 */

	/*
	 * Now advance the nextXid counter.  This must not happen until after we
	 * have successfully completed ExtendCLOG() --- if that routine fails, we
	 * want the next incoming transaction to try it again.	We cannot assign
	 * more XIDs until there is CLOG space for them.
	 */
	for (ii = 0; ii < txn_count; ii++)
	{
		xid = GTMTransactions.gt_nextXid;

		if (!GlobalTransactionIdIsValid(start_xid))
			start_xid = xid;

		/*----------
		 * Check to see if it's safe to assign another XID.  This protects against
		 * catastrophic data loss due to XID wraparound.  The basic rules are:
		 *
		 * If we're past xidVacLimit, start trying to force autovacuum cycles.
		 * If we're past xidWarnLimit, start issuing warnings.
		 * If we're past xidStopLimit, refuse to execute transactions, unless
		 * we are running in a standalone backend (which gives an escape hatch
		 * to the DBA who somehow got past the earlier defenses).
		 *
		 * Test is coded to fall out as fast as possible during normal operation,
		 * ie, when the vac limit is set and we haven't violated it.
		 *----------
		 */
		if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidVacLimit) &&
			GlobalTransactionIdIsValid(GTMTransactions.gt_xidVacLimit))
		{
			if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidStopLimit))
			{
				GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
				ereport(ERROR,
						(ERANGE,
						 errmsg("database is not accepting commands to avoid wraparound data loss in database ")));
			}
			else if (GlobalTransactionIdFollowsOrEquals(xid, GTMTransactions.gt_xidWarnLimit))
				ereport(WARNING,
				(errmsg("database must be vacuumed within %u transactions",
						GTMTransactions.gt_xidWrapLimit - xid)));
		}

		GlobalTransactionIdAdvance(GTMTransactions.gt_nextXid);
		gtm_txninfo = GTM_HandleToTransactionInfo(handle[ii]);
		Assert(gtm_txninfo);

		elog(DEBUG1, "Assigning new transaction ID = %d", xid);
		gtm_txninfo->gti_gxid = xid;
	}

	if (GTM_NeedXidRestoreUpdate())
		GTM_SetNeedBackup();
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	return start_xid;
}

/*
 * Allocate the next XID for my new transaction
 *
 * The new XID is also stored into the transaction info structure of the given
 * transaction before returning.
 */
GlobalTransactionId
GTM_GetGlobalTransactionId(GTM_TransactionHandle handle)
{
	return GTM_GetGlobalTransactionIdMulti(&handle, 1);
}

/*
 * Read nextXid but don't allocate it.
 */
GlobalTransactionId
ReadNewGlobalTransactionId(void)
{
	GlobalTransactionId xid;

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_READ);
	xid = GTMTransactions.gt_nextXid;
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	return xid;
}

/*
 * Set the nextXid.
 *
 * The GXID is usually read from a control file and set when the GTM is
 * started. When the GTM is finally shutdown, the next to-be-assigned GXID is
 * stroed in the control file.
 *
 * XXX We don't yet handle any crash recovery. So if the GTM is no shutdown normally...
 *
 * This is handled by gtm_backup.c.  Anyway, because this function is to be called by
 * GTM_RestoreTransactionId() and the backup will be performed afterwords,
 * we don't care the new value of GTMTransactions.gt_nextXid here.
 */
void
SetNextGlobalTransactionId(GlobalTransactionId gxid)
{
	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
	GTMTransactions.gt_nextXid = gxid;
	GTMTransactions.gt_gtm_state = GTM_RUNNING;
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
	return;
}


/* Transaction Control */
int
GTM_BeginTransactionMulti(char *coord_name,
					 GTM_IsolationLevel isolevel[],
					 bool readonly[],
					 GTMProxy_ConnID connid[],
					 int txn_count,
					 GTM_TransactionHandle txns[])
{
	GTM_TransactionInfo *gtm_txninfo[txn_count];
	MemoryContext oldContext;
	int kk;

	memset(gtm_txninfo, 0, sizeof (gtm_txninfo));

	/*
	 * XXX We should allocate the transaction info structure in the
	 * top-most memory context instead of a thread context. This is
	 * necessary because the transaction may outlive the thread which
	 * started the transaction. Also, since the structures are stored in
	 * the global array, it's dangerous to free the structures themselves
	 * without removing the corresponding references from the global array
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	for (kk = 0; kk < txn_count; kk++)
	{
		int ii, jj, startslot;

		/*
		 * We had no cached slots. Now find a free slot in the transation array
		 * and store the transaction info structure there
		 */
		startslot = GTMTransactions.gt_lastslot + 1;
		if (startslot >= GTM_MAX_GLOBAL_TRANSACTIONS)
			startslot = 0;

		for (ii = startslot, jj = 0;
			 jj < GTM_MAX_GLOBAL_TRANSACTIONS;
			 ii = (ii + 1) % GTM_MAX_GLOBAL_TRANSACTIONS, jj++)
		{
			if (GTMTransactions.gt_transactions_array[ii].gti_in_use == false)
			{
				gtm_txninfo[kk] = &GTMTransactions.gt_transactions_array[ii];
				break;
			}

			if (ii == GTMTransactions.gt_lastslot)
			{
				GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
				ereport(ERROR,
						(ERANGE, errmsg("Max transaction limit reached")));
			}
		}

		init_GTM_TransactionInfo(gtm_txninfo[kk], coord_name, ii, isolevel[kk], connid[kk], readonly[kk]);

		GTMTransactions.gt_lastslot = ii;

		txns[kk] = ii;

		/*
		 * Add the structure to the global list of open transactions. We should
		 * call add the element to the list in the context of TopMostMemoryContext
		 * because the list is global and any memory allocation must outlive the
		 * thread context
		 */
		GTMTransactions.gt_open_transactions = gtm_lappend(GTMTransactions.gt_open_transactions, gtm_txninfo[kk]);
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);

	MemoryContextSwitchTo(oldContext);

	return txn_count;
}

/* Transaction Control */
GTM_TransactionHandle
GTM_BeginTransaction(char *coord_name,
					 GTM_IsolationLevel isolevel,
					 bool readonly)
{
	GTM_TransactionHandle txn;
	GTMProxy_ConnID connid = -1;

	GTM_BeginTransactionMulti(coord_name, &isolevel, &readonly, &connid, 1, &txn);
	return txn;
}

static void
init_GTM_TransactionInfo(GTM_TransactionInfo *gtm_txninfo,
						 char *coord_name,
						 GTM_TransactionHandle txn,
						 GTM_IsolationLevel isolevel,
						 GTMProxy_ConnID connid,
						 bool readonly)
{
	gtm_txninfo->gti_gxid = InvalidGlobalTransactionId;
	gtm_txninfo->gti_xmin = InvalidGlobalTransactionId;
	gtm_txninfo->gti_state = GTM_TXN_STARTING;
	gtm_txninfo->gti_coordname = (coord_name ? pstrdup(coord_name) : NULL);

	gtm_txninfo->gti_isolevel = isolevel;
	gtm_txninfo->gti_readonly = readonly;
	gtm_txninfo->gti_backend_id = connid;
	gtm_txninfo->gti_in_use = true;

	gtm_txninfo->nodestring = NULL;
	gtm_txninfo->gti_gid = NULL;

	gtm_txninfo->gti_handle = txn;
	gtm_txninfo->gti_vacuum = false;
	gtm_txninfo->gti_thread_id = pthread_self();
}


/*
 * Clean up the TransactionInfo slot and pfree all the palloc'ed memory,
 * except txid array of the snapshot, which is reused.
 */
static void
clean_GTM_TransactionInfo(GTM_TransactionInfo *gtm_txninfo)
{
	gtm_txninfo->gti_state = GTM_TXN_ABORTED;
	gtm_txninfo->gti_in_use = false;
	gtm_txninfo->gti_snapshot_set = false;

	if (gtm_txninfo->gti_coordname)
	{
		pfree(gtm_txninfo->gti_coordname);
		gtm_txninfo->gti_coordname = NULL;
	}
	if (gtm_txninfo->gti_gid)
	{
		pfree(gtm_txninfo->gti_gid);
		gtm_txninfo->gti_gid = NULL;
	}
	if (gtm_txninfo->nodestring)
	{
		pfree(gtm_txninfo->nodestring);
		gtm_txninfo->nodestring = NULL;
	}
}


void
GTM_BkupBeginTransactionMulti(char *coord_name,
							  GTM_TransactionHandle *txn,
							  GTM_IsolationLevel *isolevel,
							  bool *readonly,
							  GTMProxy_ConnID *connid,
							  int	txn_count)
{
	GTM_TransactionInfo *gtm_txninfo;
	MemoryContext oldContext;
	int kk;

	gtm_txninfo = NULL;

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	for (kk = 0; kk < txn_count; kk++)
	{
		gtm_txninfo = &GTMTransactions.gt_transactions_array[txn[kk]];
		if (gtm_txninfo->gti_in_use)
		{
			GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
			elog(ERROR, "GTM_TransactionInfo already in use.  Cannot assign the transaction: handle (%d).",
				 txn[kk]);
			return;
		}
		init_GTM_TransactionInfo(gtm_txninfo, coord_name, txn[kk], isolevel[kk], connid[kk], readonly[kk]);
		GTMTransactions.gt_lastslot = txn[kk];
		GTMTransactions.gt_open_transactions = gtm_lappend(GTMTransactions.gt_open_transactions, gtm_txninfo);
	}

	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	MemoryContextSwitchTo(oldContext);
}

void
GTM_BkupBeginTransaction(char *coord_name,
						 GTM_TransactionHandle txn,
						 GTM_IsolationLevel isolevel,
						 bool readonly)
{
	GTMProxy_ConnID connid = -1;

	GTM_BkupBeginTransactionMulti(coord_name, &txn, &isolevel, &readonly, &connid, 1);
}
/*
 * Same as GTM_RollbackTransaction, but takes GXID as input
 */
int
GTM_RollbackTransactionGXID(GlobalTransactionId gxid)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_RollbackTransaction(txn);
}

/*
 * Rollback multiple transactions in one go
 */
int
GTM_RollbackTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[])
{
	GTM_TransactionInfo *gtm_txninfo[txn_count];
	int ii;

	for (ii = 0; ii < txn_count; ii++)
	{
		gtm_txninfo[ii] = GTM_HandleToTransactionInfo(txn[ii]);

		if (gtm_txninfo[ii] == NULL)
		{
			status[ii] = STATUS_ERROR;
			continue;
		}

		/*
		 * Mark the transaction as being aborted
		 */
		GTM_RWLockAcquire(&gtm_txninfo[ii]->gti_lock, GTM_LOCKMODE_WRITE);
		gtm_txninfo[ii]->gti_state = GTM_TXN_ABORT_IN_PROGRESS;
		GTM_RWLockRelease(&gtm_txninfo[ii]->gti_lock);
		status[ii] = STATUS_OK;
	}

	GTM_RemoveTransInfoMulti(gtm_txninfo, txn_count);

	return txn_count;
}

/*
 * Rollback a transaction
 */
int
GTM_RollbackTransaction(GTM_TransactionHandle txn)
{
	int status;
	GTM_RollbackTransactionMulti(&txn, 1, &status);
	return status;
}


/*
 * Same as GTM_CommitTransaction but takes GXID as input
 */
int
GTM_CommitTransactionGXID(GlobalTransactionId gxid)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_CommitTransaction(txn);
}

/*
 * Commit multiple transactions in one go
 */
int
GTM_CommitTransactionMulti(GTM_TransactionHandle txn[], int txn_count, int status[])
{
	GTM_TransactionInfo *gtm_txninfo[txn_count];
	int ii;

	for (ii = 0; ii < txn_count; ii++)
	{
		gtm_txninfo[ii] = GTM_HandleToTransactionInfo(txn[ii]);

		if (gtm_txninfo[ii] == NULL)
		{
			status[ii] = STATUS_ERROR;
			continue;
		}
		/*
		 * Mark the transaction as being aborted
		 */
		GTM_RWLockAcquire(&gtm_txninfo[ii]->gti_lock, GTM_LOCKMODE_WRITE);
		gtm_txninfo[ii]->gti_state = GTM_TXN_COMMIT_IN_PROGRESS;
		GTM_RWLockRelease(&gtm_txninfo[ii]->gti_lock);
		status[ii] = STATUS_OK;
	}

	GTM_RemoveTransInfoMulti(gtm_txninfo, txn_count);

	return txn_count;
}

/*
 * Prepare a transaction
 */
int
GTM_PrepareTransaction(GTM_TransactionHandle txn)
{
	GTM_TransactionInfo *gtm_txninfo = NULL;

	gtm_txninfo = GTM_HandleToTransactionInfo(txn);

	if (gtm_txninfo == NULL)
		return STATUS_ERROR;

	/*
	 * Mark the transaction as prepared
	 */
	GTM_RWLockAcquire(&gtm_txninfo->gti_lock, GTM_LOCKMODE_WRITE);
	gtm_txninfo->gti_state = GTM_TXN_PREPARED;
	GTM_RWLockRelease(&gtm_txninfo->gti_lock);

	return STATUS_OK;
}

/*
 * Commit a transaction
 */
int
GTM_CommitTransaction(GTM_TransactionHandle txn)
{
	int status;
	GTM_CommitTransactionMulti(&txn, 1, &status);
	return status;
}

/*
 * Prepare a transaction
 */
int
GTM_StartPreparedTransaction(GTM_TransactionHandle txn,
							 char *gid,
							 char *nodestring)
{
	GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(txn);

	if (gtm_txninfo == NULL)
		return STATUS_ERROR;

	/*
	 * Check if given GID is already in use by another transaction.
	 */
	if (GTM_GIDToHandle(gid) != InvalidTransactionHandle)
		return STATUS_ERROR;

	/*
	 * Check if given GID is already in use by another transaction.
	 */
	if (GTM_GIDToHandle(gid) != InvalidTransactionHandle)
		return STATUS_ERROR;

	/*
	 * Mark the transaction as being prepared
	 */
	GTM_RWLockAcquire(&gtm_txninfo->gti_lock, GTM_LOCKMODE_WRITE);

	gtm_txninfo->gti_state = GTM_TXN_PREPARE_IN_PROGRESS;
	if (gtm_txninfo->nodestring == NULL)
		gtm_txninfo->nodestring = (char *)MemoryContextAlloc(TopMostMemoryContext,
															 GTM_MAX_NODESTRING_LEN);
	memcpy(gtm_txninfo->nodestring, nodestring, strlen(nodestring) + 1);

	/* It is possible that no Datanode is involved in a transaction */
	if (gtm_txninfo->gti_gid == NULL)
		gtm_txninfo->gti_gid = (char *)MemoryContextAlloc(TopMostMemoryContext, GTM_MAX_GID_LEN);
	memcpy(gtm_txninfo->gti_gid, gid, strlen(gid) + 1);

	GTM_RWLockRelease(&gtm_txninfo->gti_lock);

	return STATUS_OK;
}

/*
 * Same as GTM_PrepareTransaction but takes GXID as input
 */
int
GTM_StartPreparedTransactionGXID(GlobalTransactionId gxid,
								 char *gid,
								 char *nodestring)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_StartPreparedTransaction(txn, gid, nodestring);
}

int
GTM_GetGIDData(GTM_TransactionHandle prepared_txn,
			   GlobalTransactionId *prepared_gxid,
			   char **nodestring)
{
	GTM_TransactionInfo	*gtm_txninfo = NULL;
	MemoryContext		oldContext;

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	gtm_txninfo = GTM_HandleToTransactionInfo(prepared_txn);
	if (gtm_txninfo == NULL)
		return STATUS_ERROR;

	/* then get the necessary Data */
	*prepared_gxid = gtm_txninfo->gti_gxid;
	if (gtm_txninfo->nodestring)
	{
		*nodestring = (char *) palloc(strlen(gtm_txninfo->nodestring) + 1);
		memcpy(*nodestring, gtm_txninfo->nodestring, strlen(gtm_txninfo->nodestring) + 1);
		(*nodestring)[strlen(gtm_txninfo->nodestring)] = '\0';
	}
	else
		*nodestring = NULL;

	MemoryContextSwitchTo(oldContext);

	return STATUS_OK;
}

/*
 * Get status of the given transaction
 */
GTM_TransactionStates
GTM_GetStatus(GTM_TransactionHandle txn)
{
	GTM_TransactionInfo *gtm_txninfo = GTM_HandleToTransactionInfo(txn);
	return gtm_txninfo->gti_state;
}

/*
 * Same as GTM_GetStatus but takes GXID as input
 */
GTM_TransactionStates
GTM_GetStatusGXID(GlobalTransactionId gxid)
{
	GTM_TransactionHandle txn = GTM_GXIDToHandle(gxid);
	return GTM_GetStatus(txn);
}

/*
 * Process MSG_TXN_BEGIN message
 */
void
ProcessBeginTransactionCommand(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GTM_Timestamp timestamp;
	MemoryContext oldContext;

	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator name - replace "" with that
	 */
	txn = GTM_BeginTransaction("", txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start a new transaction")));

	MemoryContextSwitchTo(oldContext);

	/* Backup first */
	if (GetMyThreadInfo->thr_conn->standby)
	{
		bkup_begin_transaction(GetMyThreadInfo->thr_conn->standby, txn, txn_isolation_level, txn_read_only, timestamp);
		/* Synch. with standby */
		if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
			gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);
	}

	/* GXID has been received, now it's time to get a GTM timestamp */
	timestamp = GTM_TimestampGetCurrent();

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn, sizeof(txn));
	pq_sendbytes(&buf, (char *)&timestamp, sizeof (GTM_Timestamp));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		/* Flush standby first */
		if (GetMyThreadInfo->thr_conn->standby)
			gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
		pq_flush(myport);
	}
	return;
}

/*
 * Process MSG_BKUP_TXN_BEGIN message
 */
void
ProcessBkupBeginTransactionCommand(Port *myport, StringInfo message)
{
	GTM_TransactionHandle txn;
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	GTM_Timestamp timestamp;
	MemoryContext oldContext;

	txn = pq_getmsgint(message, sizeof(GTM_TransactionHandle));
	txn_isolation_level = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);
	memcpy(&timestamp, pq_getmsgbytes(message, sizeof(GTM_Timestamp)), sizeof(GTM_Timestamp));
	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	GTM_BkupBeginTransaction("", txn, txn_isolation_level, txn_read_only);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Process MSG_TXN_BEGIN_GETGXID message
 */
void
ProcessBeginTransactionGetGXIDCommand(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	GTM_Timestamp timestamp;
	MemoryContext oldContext;

	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/* GXID has been received, now it's time to get a GTM timestamp */
	timestamp = GTM_TimestampGetCurrent();

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator name - replace "" with that
	 */
	txn = GTM_BeginTransaction("", txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start a new transaction")));

	gxid = GTM_GetGlobalTransactionId(txn);
	if (gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG1, "Sending transaction id %u", gxid);

	/* Backup first */
	if (GetMyThreadInfo->thr_conn->standby)
	{
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(DEBUG1, "calling begin_transaction() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

retry:
		bkup_begin_transaction_gxid(GetMyThreadInfo->thr_conn->standby,
									txn, gxid, txn_isolation_level, txn_read_only, timestamp);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		/* Sync */
		if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
			gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

	}
	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_GETGXID_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_sendbytes(&buf, (char *)&timestamp, sizeof (GTM_Timestamp));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		/* Flush standby */
		if (GetMyThreadInfo->thr_conn->standby)
			gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
		pq_flush(myport);
	}


	return;
}

static void
GTM_BkupBeginTransactionGetGXIDMulti(char *coord_name,
									 GTM_TransactionHandle *txn,
									 GlobalTransactionId *gxid,
									 GTM_IsolationLevel *isolevel,
									 bool *readonly,
									 GTMProxy_ConnID *connid,
									 int txn_count)
{
	GTM_TransactionInfo *gtm_txninfo;
	int ii;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);

	for (ii = 0; ii < txn_count; ii++)
	{
		gtm_txninfo = &GTMTransactions.gt_transactions_array[txn[ii]];
		if (gtm_txninfo->gti_in_use)
		{
			GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
			elog(ERROR, "GTM_TransactionInfo already in use.  Cannot assign the transaction: handle (%d).",
				 txn[ii]);
			MemoryContextSwitchTo(oldContext);
			return;
		}
		init_GTM_TransactionInfo(gtm_txninfo, coord_name, txn[ii], isolevel[ii], connid[ii], readonly[ii]);
		GTMTransactions.gt_lastslot = txn[ii];
		gtm_txninfo->gti_gxid = gxid[ii];
		/*
		 * Advance next gxid -- because this is called at slave only, we don't care the restoration point
		 * here.  Restoration point will be created at promotion.
		 */
		if (GlobalTransactionIdPrecedes(GTMTransactions.gt_nextXid, gxid[ii]))
			GTMTransactions.gt_nextXid = gxid[ii] + 1;
		if (!GlobalTransactionIdIsValid(GTMTransactions.gt_nextXid))	/* Handle wrap around too */
			GTMTransactions.gt_nextXid = FirstNormalGlobalTransactionId;
		GTMTransactions.gt_open_transactions = gtm_lappend(GTMTransactions.gt_open_transactions, gtm_txninfo);
	}


	GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
	MemoryContextSwitchTo(oldContext);
}

static void
GTM_BkupBeginTransactionGetGXID(char *coord_name,
								GTM_TransactionHandle txn,
								GlobalTransactionId gxid,
								GTM_IsolationLevel isolevel,
								bool readonly)
{
	GTMProxy_ConnID connid = -1;

	GTM_BkupBeginTransactionGetGXIDMulti(coord_name, &txn, &gxid, &isolevel, &readonly, &connid, 1);
}

/*
 * Process MSG_BKUP_TXN_BEGIN_GETGXID message
 */
void
ProcessBkupBeginTransactionGetGXIDCommand(Port *myport, StringInfo message)
{
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	GTM_Timestamp timestamp;

	txn = pq_getmsgint(message, sizeof(GTM_TransactionHandle));
	gxid = pq_getmsgint(message, sizeof(GlobalTransactionId));
	txn_isolation_level = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);
	memcpy(&timestamp, pq_getmsgbytes(message, sizeof(GTM_Timestamp)), sizeof(GTM_Timestamp));
	pq_getmsgend(message);

	GTM_BkupBeginTransactionGetGXID("", txn, gxid, txn_isolation_level, txn_read_only);
}

/*
 * Process MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM message
 */
void
ProcessBkupBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message)
{
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	GTM_IsolationLevel txn_isolation_level;

	txn = pq_getmsgint(message, sizeof(GTM_TransactionHandle));
	gxid = pq_getmsgint(message, sizeof(GlobalTransactionId));
	txn_isolation_level = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
	pq_getmsgend(message);

	GTM_BkupBeginTransactionGetGXID("", txn, gxid, txn_isolation_level, false);
	GTM_SetDoVacuum(txn);
}

/*
 * Process MSG_TXN_BEGIN_GETGXID_AUTOVACUUM message
 */
void
ProcessBeginTransactionGetGXIDAutovacuumCommand(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	MemoryContext oldContext;

	elog(DEBUG1, "Inside ProcessBeginTransactionGetGXIDAutovacuumCommand");

	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator name - replace "" with that
	 */
	txn = GTM_BeginTransaction("", txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start a new transaction")));

	gxid = GTM_GetGlobalTransactionId(txn);
	if (gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	/* Indicate that it is for autovacuum */
	GTM_SetDoVacuum(txn);

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG1, "Sending transaction id %d", gxid);

	/* Backup first */
	if (GetMyThreadInfo->thr_conn->standby)
	{
		GlobalTransactionId _gxid;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(DEBUG1, "calling begin_transaction_autovacuum() for standby GTM %p.",
			 GetMyThreadInfo->thr_conn->standby);

	retry:
		_gxid = bkup_begin_transaction_autovacuum(GetMyThreadInfo->thr_conn->standby,
												  txn, gxid, txn_isolation_level);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		/* Sync */
		if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
			gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

		elog(DEBUG1, "begin_transaction_autovacuum() GXID=%d done.", _gxid);
	}
	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		/* Flush standby */
		if (GetMyThreadInfo->thr_conn->standby)
			gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
		pq_flush(myport);
	}

	return;
}

/*
 * Process MSG_TXN_BEGIN_GETGXID_MULTI message
 */
void
ProcessBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message)
{
	GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
	bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
	int txn_count;
	StringInfoData buf;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId start_gxid, end_gxid;
	GTM_Timestamp timestamp;
	GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];
	MemoryContext oldContext;
	int count;
	int ii;

	txn_count = pq_getmsgint(message, sizeof (int));

	if (txn_count <= 0)
		elog(PANIC, "Zero or less transaction count");

	for (ii = 0; ii < txn_count; ii++)
	{
		txn_isolation_level[ii] = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
		txn_read_only[ii] = pq_getmsgbyte(message);
		txn_connid[ii] = pq_getmsgint(message, sizeof (GTMProxy_ConnID));
	}

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Start a new transaction
	 *
	 * XXX Port should contain Coordinator name - replace NULL with that
	 */
	count = GTM_BeginTransactionMulti(NULL, txn_isolation_level, txn_read_only, txn_connid,
									  txn_count, txn);
	if (count != txn_count)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to start %d new transactions", txn_count)));

	start_gxid = GTM_GetGlobalTransactionIdMulti(txn, txn_count);
	if (start_gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	MemoryContextSwitchTo(oldContext);

	/* GXID has been received, now it's time to get a GTM timestamp */
	timestamp = GTM_TimestampGetCurrent();

	end_gxid = start_gxid + (txn_count - 1);
	if (end_gxid < start_gxid)
		end_gxid += FirstNormalGlobalTransactionId;

	elog(DEBUG1, "Sending transaction ids from %u to %u", start_gxid, end_gxid);

	/* Backup first */
	if (GetMyThreadInfo->thr_conn->standby)
	{
		int _rc;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(DEBUG1, "calling begin_transaction_multi() for standby GTM %p.",
		     GetMyThreadInfo->thr_conn->standby);

retry:
		_rc = bkup_begin_transaction_multi(GetMyThreadInfo->thr_conn->standby,
										   txn_count,
										   txn,
										   start_gxid,
										   txn_isolation_level,
										   txn_read_only,
										   txn_connid);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		/* Sync */
		if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
			gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

		elog(DEBUG1, "begin_transaction_multi() rc=%d done.", _rc);
	}
	/* Respond to the client */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_BEGIN_GETGXID_MULTI_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
	pq_sendbytes(&buf, (char *)&start_gxid, sizeof(start_gxid));
	pq_sendbytes(&buf, (char *)&(timestamp), sizeof (GTM_Timestamp));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		/* Flush standby */
		if (GetMyThreadInfo->thr_conn->standby)
			gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
		pq_flush(myport);
	}

	return;
}

/*
 * Process MSG_BKUP_BEGIN_TXN_GETGXID_MULTI message
 */
void
ProcessBkupBeginTransactionGetGXIDCommandMulti(Port *myport, StringInfo message)
{
	int txn_count;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
	bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
	GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];
	int ii;

	txn_count = pq_getmsgint(message, sizeof(int));
	if (txn_count <= 0)
		elog(PANIC, "Zero or less transaction count.");

	for (ii = 0; ii < txn_count; ii++)
	{
		txn[ii] = pq_getmsgint(message, sizeof(GTM_TransactionHandle));
		gxid[ii] = pq_getmsgint(message, sizeof(GlobalTransactionId));
		txn_isolation_level[ii] = pq_getmsgint(message, sizeof(GTM_IsolationLevel));
		txn_read_only[ii] = pq_getmsgbyte(message);
		txn_connid[ii] = pq_getmsgint(message, sizeof(GTMProxy_ConnID));
	}

	GTM_BkupBeginTransactionGetGXIDMulti("", txn, gxid, txn_isolation_level, txn_read_only, txn_connid, txn_count);

}
/*
 * Process MSG_TXN_COMMIT/MSG_BKUP_TXN_COMMIT message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_COMMIT
 */
void
ProcessCommitTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	MemoryContext oldContext;
	int status = STATUS_OK;

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	elog(DEBUG1, "Committing transaction id %u", gxid);

	/*
	 * Commit the transaction
	 */
	status = GTM_CommitTransaction(txn);

	MemoryContextSwitchTo(oldContext);

	if(!is_backup)
	{
		if (GetMyThreadInfo->thr_conn->standby)
		{
			/*
			 * Backup first
			 */
			int _rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling commit_transaction() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = bkup_commit_transaction(GetMyThreadInfo->thr_conn->standby, gxid);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "commit_transaction() rc=%d done.", _rc);
		}

		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_COMMIT_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
		pq_sendint(&buf, status, sizeof(status));
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush standby */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	return;
}

/*
 * Process MSG_TXN_COMMIT_PREPARED/MSG_BKUP_TXN_COMMIT_PREPARED message
 * Commit a prepared transaction
 * Here the GXID used for PREPARE and COMMIT PREPARED are both committed
 *
 * is_backup indicates the message is MSG_BKUP_TXN_COMMIT_PREPARED
 */
void
ProcessCommitPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	int	txn_count = 2; /* PREPARE and COMMIT PREPARED gxid's */
	GTM_TransactionHandle txn[txn_count];
	GlobalTransactionId gxid[txn_count];
	MemoryContext oldContext;
	int status[txn_count];
	int isgxid[txn_count];
	int ii;

	for (ii = 0; ii < txn_count; ii++)
	{
		isgxid[ii] = pq_getmsgbyte(message);
		if (isgxid[ii])
		{
			const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid GXID")));
			memcpy(&gxid[ii], data, sizeof (gxid[ii]));
			txn[ii] = GTM_GXIDToHandle(gxid[ii]);
			elog(DEBUG1, "ProcessCommitTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
		}
		else
		{
			const char *data = pq_getmsgbytes(message, sizeof (txn[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid Transaction Handle")));
			memcpy(&txn[ii], data, sizeof (txn[ii]));
			elog(DEBUG1, "ProcessCommitTransactionCommandMulti: handle(%u)", txn[ii]);
		}
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	elog(DEBUG1, "Committing: prepared id %u and commit prepared id %u ", gxid[0], gxid[1]);

	/*
	 * Commit the prepared transaction.
	 */
	GTM_CommitTransactionMulti(txn, txn_count, status);

	MemoryContextSwitchTo(oldContext);

	if (!is_backup)
	{
		if (GetMyThreadInfo->thr_conn->standby)
		{
			/* Backup first */
			int _rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling commit_prepared_transaction() for standby GTM %p.",
				 GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = bkup_commit_prepared_transaction(GetMyThreadInfo->thr_conn->standby,
												   gxid[0], gxid[1] /* prepared GXID */);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "commit_prepared_transaction() rc=%d done.", _rc);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_COMMIT_PREPARED_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&gxid[0], sizeof(GlobalTransactionId));
		pq_sendint(&buf, status[0], 4);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush standby */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	return;
}


/*
 * Process MSG_TXN_GET_GID_DATA
 * This message is used after at the beginning of a COMMIT PREPARED
 * or a ROLLBACK PREPARED.
 * For a given GID the following info is returned:
 * - a fresh GXID,
 * - GXID of the transaction that made the prepare
 * - Datanode and Coordinator node list involved in the prepare
 */
void
ProcessGetGIDDataTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	char gid[1024];
	char *nodestring = NULL;
	int gidlen;
	GTM_IsolationLevel txn_isolation_level;
	bool txn_read_only;
	GTM_TransactionHandle txn, prepared_txn;
	/* Data to be sent back to client */
	GlobalTransactionId gxid, prepared_gxid;

	/* take the isolation level and read_only instructions */
	txn_isolation_level = pq_getmsgint(message, sizeof (GTM_IsolationLevel));
	txn_read_only = pq_getmsgbyte(message);

	/* receive GID */
	gidlen = pq_getmsgint(message, sizeof (GTM_StrLen));
	memcpy(gid, (char *)pq_getmsgbytes(message, gidlen), gidlen);
	gid[gidlen] = '\0';

	pq_getmsgend(message);

	/* Get the prepared Transaction for given GID */
	prepared_txn = GTM_GIDToHandle(gid);
	if (prepared_txn == InvalidTransactionHandle)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get GID Data for prepared transaction")));

	/* First get the GXID for the new transaction */
	txn = GTM_BeginTransaction("", txn_isolation_level, txn_read_only);
	if (txn == InvalidTransactionHandle)
		ereport(ERROR,
			(EINVAL,
			 errmsg("Failed to start a new transaction")));

	gxid = GTM_GetGlobalTransactionId(txn);
	if (gxid == InvalidGlobalTransactionId)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get a new transaction id")));

	/*
	 * Make the internal process, get the prepared information from GID.
	 */
	if (GTM_GetGIDData(prepared_txn, &prepared_gxid, &nodestring) != STATUS_OK)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get the information of prepared transaction")));

	/*
	 * Send a SUCCESS message back to the client
	 */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_GET_GID_DATA_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	/* Send the two GXIDs */
	pq_sendbytes(&buf, (char *)&gxid, sizeof(GlobalTransactionId));
	pq_sendbytes(&buf, (char *)&prepared_gxid, sizeof(GlobalTransactionId));

	/* Node string list */
	if (nodestring)
	{
		pq_sendint(&buf, strlen(nodestring), 4);
		pq_sendbytes(&buf, nodestring, strlen(nodestring));
	}
	else
		pq_sendint(&buf, 0, 4);

	/* End of message */
	pq_endmessage(myport, &buf);

	/* No backup to the standby because this does not change internal status */
	if (myport->remote_type != GTM_NODE_GTM_PROXY)
		pq_flush(myport);

	/* I don't think the following backup is needed. K.Suzuki, 27th, Dec., 2011 */
#if 0
	if (GetMyThreadInfo->thr_conn->standby)
	{
		int _rc;
		GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
		int count = 0;

		elog(DEBUG1, "calling get_gid_data() for standby GTM %p.",
			GetMyThreadInfo->thr_conn->standby);

retry:
		_rc = get_gid_data(GetMyThreadInfo->thr_conn->standby,
				   txn_isolation_level,
				   gid,
				   &gxid,
				   &prepared_gxid,
				   &nodestring);

		if (gtm_standby_check_communication_error(&count, oldconn))
			goto retry;

		elog(DEBUG1, "get_gid_data() rc=%d done.", _rc);
	}
#endif

	return;
}

/*
 * Process MSG_TXN_GXID_LIST
 */
void
ProcessGXIDListCommand(Port *myport, StringInfo message)
{
	MemoryContext oldContext;
	StringInfoData buf;
	char *data;
	size_t estlen, actlen; /* estimated length and actual length */

	pq_getmsgend(message);

	if (Recovery_IsStandby())
		ereport(ERROR,
			(EPERM,
			 errmsg("Operation not permitted under the standby mode.")));

	/*
	 * Do something here.
	 */
	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);

	estlen = gtm_get_transactions_size(&GTMTransactions);
	data = malloc(estlen+1);

	actlen = gtm_serialize_transactions(&GTMTransactions, data, estlen);

	elog(DEBUG1, "gtm_serialize_transactions: estlen=%ld, actlen=%ld", estlen, actlen);

	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	MemoryContextSwitchTo(oldContext);

	/*
	 * Send a SUCCESS message back to the client
	 */
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_GXID_LIST_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	pq_sendint(&buf, actlen, sizeof(int32));	/* size of serialized GTM_Transactions */
	pq_sendbytes(&buf, data, actlen);			/* serialized GTM_Transactions */
	pq_endmessage(myport, &buf);

	/* No backup to the standby because this does not change internal state */
	if (myport->remote_type != GTM_NODE_GTM_PROXY)
	{
		pq_flush(myport);
		elog(DEBUG1, "pq_flush()");
	}

	elog(DEBUG1, "ProcessGXIDListCommand() ok. %ld bytes sent. len=%d", actlen, buf.len);
	free(data);

	return;
}


/*
 * Process MSG_TXN_ROLLBACK/MSG_BKUP_TXN_ROLLBACK message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_ROLLBACK
 */
void
ProcessRollbackTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	MemoryContext oldContext;
	int status = STATUS_OK;

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	elog(DEBUG1, "Cancelling transaction id %u", gxid);

	/*
	 * Commit the transaction
	 */
	status = GTM_RollbackTransaction(txn);

	MemoryContextSwitchTo(oldContext);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling abort_transaction() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			bkup_abort_transaction(GetMyThreadInfo->thr_conn->standby, gxid);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "abort_transaction() GXID=%d done.", gxid);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_ROLLBACK_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
		pq_sendint(&buf, status, sizeof(status));
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	return;
}


/*
 * Process MSG_TXN_COMMIT_MULTI/MSG_BKUP_TXN_COMMIT_MULTI message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_COMMIT_MULTI
 */
void
ProcessCommitTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	int isgxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	MemoryContext oldContext;
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];
	int txn_count;
	int ii;

	txn_count = pq_getmsgint(message, sizeof (int));

	for (ii = 0; ii < txn_count; ii++)
	{
		isgxid[ii] = pq_getmsgbyte(message);
		if (isgxid[ii])
		{
			const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid GXID")));
			memcpy(&gxid[ii], data, sizeof (gxid[ii]));
			txn[ii] = GTM_GXIDToHandle(gxid[ii]);
			elog(DEBUG1, "ProcessCommitTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
		}
		else
		{
			const char *data = pq_getmsgbytes(message, sizeof (txn[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid Transaction Handle")));
			memcpy(&txn[ii], data, sizeof (txn[ii]));
			elog(DEBUG1, "ProcessCommitTransactionCommandMulti: handle(%u)", txn[ii]);
		}
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Commit the transaction
	 */
	GTM_CommitTransactionMulti(txn, txn_count, status);

	MemoryContextSwitchTo(oldContext);

	if (!is_backup)
	{
		if (GetMyThreadInfo->thr_conn->standby)
		{
			/* Backup first */
			int _rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling commit_transaction_multi() for standby GTM %p.",
				 GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = bkup_commit_transaction_multi(GetMyThreadInfo->thr_conn->standby, txn_count, txn);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;
			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "commit_transaction_multi() rc=%d done.", _rc);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_COMMIT_MULTI_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
		pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}
	}
	return;
}

/*
 * Process MSG_TXN_ROLLBACK_MULTI/MSG_BKUP_TXN_ROLLBACK_MULTI message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_ROLLBACK_MULTI
 */
void
ProcessRollbackTransactionCommandMulti(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	GTM_TransactionHandle txn[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	int isgxid[GTM_MAX_GLOBAL_TRANSACTIONS];
	MemoryContext oldContext;
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];
	int txn_count;
	int ii;

	txn_count = pq_getmsgint(message, sizeof (int));

	for (ii = 0; ii < txn_count; ii++)
	{
		isgxid[ii] = pq_getmsgbyte(message);
		if (isgxid[ii])
		{
			const char *data = pq_getmsgbytes(message, sizeof (gxid[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid GXID")));
			memcpy(&gxid[ii], data, sizeof (gxid[ii]));
			txn[ii] = GTM_GXIDToHandle(gxid[ii]);
			elog(DEBUG1, "ProcessRollbackTransactionCommandMulti: gxid(%u), handle(%u)", gxid[ii], txn[ii]);
		}
		else
		{
			const char *data = pq_getmsgbytes(message, sizeof (txn[ii]));
			if (data == NULL)
				ereport(ERROR,
						(EPROTO,
						 errmsg("Message does not contain valid Transaction Handle")));
			memcpy(&txn[ii], data, sizeof (txn[ii]));
			elog(DEBUG1, "ProcessRollbackTransactionCommandMulti: handle(%u)", txn[ii]);
		}
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Commit the transaction
	 */
	GTM_RollbackTransactionMulti(txn, txn_count, status);

	MemoryContextSwitchTo(oldContext);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int _rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling abort_transaction_multi() for standby GTM %p.",
				 GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = bkup_abort_transaction_multi(GetMyThreadInfo->thr_conn->standby, txn_count, gxid);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously &&(myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "abort_transaction_multi() rc=%d done.", _rc);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_ROLLBACK_MULTI_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&txn_count, sizeof(txn_count));
		pq_sendbytes(&buf, (char *)status, sizeof(int) * txn_count);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	return;
}

/*
 * Process MSG_TXN_START_PREPARED/MSG_BKUP_TXN_START_PREPARED message
 *
 * is_backup indicates if the message is MSG_BKUP_TXN_START_PREPARED.
 */
void
ProcessStartPreparedTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	GTM_StrLen gidlen, nodelen;
	char nodestring[1024];
	MemoryContext oldContext;
	char gid[1024];

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	/* get GID */
	gidlen = pq_getmsgint(message, sizeof (GTM_StrLen));
	memcpy(gid, (char *)pq_getmsgbytes(message, gidlen), gidlen);
	gid[gidlen] = '\0';

	/* get node string list */
	nodelen = pq_getmsgint(message, sizeof (GTM_StrLen));
	memcpy(nodestring, (char *)pq_getmsgbytes(message, nodelen), nodelen);
	nodestring[nodelen] = '\0';

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	/*
	 * Prepare the transaction
	 */
	if (GTM_StartPreparedTransaction(txn, gid, nodestring) != STATUS_OK)
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to prepare the transaction")));

	MemoryContextSwitchTo(oldContext);

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

			elog(DEBUG1, "calling start_prepared_transaction() for standby GTM %p.",
				 GetMyThreadInfo->thr_conn->standby);

		retry:
			_rc = backup_start_prepared_transaction(GetMyThreadInfo->thr_conn->standby,
													gxid, gid,
													nodestring);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "start_prepared_transaction() rc=%d done.", _rc);
		}
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_START_PREPARED_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&gxid, sizeof(GlobalTransactionId));
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	return;
}

/*
 * Process MSG_TXN_PREPARE/MSG_BKUP_TXN_PREPARE message
 *
 * is_backup indicates the message is MSG_BKUP_TXN_PREPARE
 */
void
ProcessPrepareTransactionCommand(Port *myport, StringInfo message, bool is_backup)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	int isgxid = 0;
	MemoryContext oldContext;

	isgxid = pq_getmsgbyte(message);

	if (isgxid)
	{
		const char *data = pq_getmsgbytes(message, sizeof (gxid));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid GXID")));
		memcpy(&gxid, data, sizeof (gxid));
		txn = GTM_GXIDToHandle(gxid);
	}
	else
	{
		const char *data = pq_getmsgbytes(message, sizeof (txn));
		if (data == NULL)
			ereport(ERROR,
					(EPROTO,
					 errmsg("Message does not contain valid Transaction Handle")));
		memcpy(&txn, data, sizeof (txn));
	}

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	/*
	 * Commit the transaction
	 */
	GTM_PrepareTransaction(txn);

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG1, "Preparing transaction id %u", gxid);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling prepare_transaction() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			bkup_prepare_transaction(GetMyThreadInfo->thr_conn->standby, gxid);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "prepare_transaction() GXID=%d done.", gxid);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, TXN_PREPARE_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}
	}
	return;

}


/*
 * Process MSG_TXN_GET_GXID message
 *
 * Notice: we don't have corresponding functions in gtm_client.c which
 * generates a command for this function.
 *
 * Because of this, GTM-standby extension is not included in this function.
 */
void
ProcessGetGXIDTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GTM_TransactionHandle txn;
	GlobalTransactionId gxid;
	const char *data;
	MemoryContext oldContext;

	elog(DEBUG3, "Inside ProcessGetGXIDTransactionCommand");

	data = pq_getmsgbytes(message, sizeof (txn));
	if (data == NULL)
		ereport(ERROR,
				(EPROTO,
				 errmsg("Message does not contain valid Transaction Handle")));
	memcpy(&txn, data, sizeof (txn));

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Get the transaction id for the given global transaction
	 */
	gxid = GTM_GetGlobalTransactionId(txn);
	if (GlobalTransactionIdIsValid(gxid))
		ereport(ERROR,
				(EINVAL,
				 errmsg("Failed to get the transaction id")));

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG3, "Sending transaction id %d", gxid);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_GET_GXID_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendbytes(&buf, (char *)&txn, sizeof(txn));
	pq_sendbytes(&buf, (char *)&gxid, sizeof(gxid));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
		pq_flush(myport);
	return;
}


/*
 * Process MSG_TXN_GET_NEXT_GXID message
 *
 * This does not need backup to the standby because no internal state changes.
 */
void
ProcessGetNextGXIDTransactionCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	GlobalTransactionId next_gxid;
	MemoryContext oldContext;

	elog(DEBUG3, "Inside ProcessGetNextGXIDTransactionCommand");

	pq_getmsgend(message);

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Get the next gxid.
	 */
	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
	next_gxid = GTMTransactions.gt_nextXid;

	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);

	MemoryContextSwitchTo(oldContext);

	elog(DEBUG3, "Sending next gxid %d", next_gxid);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, TXN_GET_NEXT_GXID_RESULT, 4);
	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}
	pq_sendint(&buf, next_gxid, sizeof(GlobalTransactionId));
	pq_endmessage(myport, &buf);

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
		pq_flush(myport);
	return;
}


/*
 * Mark GTM as shutting down. This point onwards no new GXID are issued to
 * ensure that the last GXID recorded in the control file remains sane
 */
void
GTM_SetShuttingDown(void)
{
	GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
	GTMTransactions.gt_gtm_state = GTM_SHUTTING_DOWN;
	GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
}

void
GTM_RestoreTxnInfo(FILE *ctlf, GlobalTransactionId next_gxid)
{
	GlobalTransactionId saved_gxid;

	if (ctlf)
	{
		if ((fscanf(ctlf, "%u", &saved_gxid) != 1) &&
			(!GlobalTransactionIdIsValid(next_gxid)))
			next_gxid = InitialGXIDValue_Default;
		else if (!GlobalTransactionIdIsValid(next_gxid))
			next_gxid = saved_gxid;
	}
	else if (!GlobalTransactionIdIsValid(next_gxid))
		next_gxid = InitialGXIDValue_Default;

	elog(DEBUG1, "Restoring last GXID to %u\n", next_gxid);

	if (GlobalTransactionIdIsValid(next_gxid))
		SetNextGlobalTransactionId(next_gxid);
	/* Set this otherwise a strange snapshot might be returned for the first one */
	GTMTransactions.gt_latestCompletedXid = next_gxid - 1;
	return;
}

void
GTM_SaveTxnInfo(FILE *ctlf)
{
	GlobalTransactionId next_gxid;

	next_gxid = ReadNewGlobalTransactionId();

	elog(LOG, "Saving transaction info - next_gxid: %u", next_gxid);

	fprintf(ctlf, "%u\n", next_gxid);
}

bool GTM_NeedXidRestoreUpdate(void)
{
	return(GlobalTransactionIdPrecedesOrEquals(GTMTransactions.gt_backedUpXid, GTMTransactions.gt_nextXid));
}

void GTM_WriteRestorePointXid(FILE *f)
{
	if ((MaxGlobalTransactionId - GTMTransactions.gt_nextXid) <= RestoreDuration)
		GTMTransactions.gt_backedUpXid = GTMTransactions.gt_nextXid + RestoreDuration;
	else
		GTMTransactions.gt_backedUpXid = FirstNormalGlobalTransactionId + (RestoreDuration - (MaxGlobalTransactionId - GTMTransactions.gt_nextXid));
	
	elog(LOG, "Saving transaction restoration info, backed-up gxid: %u", GTMTransactions.gt_backedUpXid);
	fprintf(f, "%u\n", GTMTransactions.gt_backedUpXid);
}

/*
 * TODO
 */
int GTM_GetAllTransactions(GTM_TransactionInfo txninfo[], uint32 txncnt);

/*
 * TODO
 */
uint32 GTM_GetAllPrepared(GlobalTransactionId gxids[], uint32 gxidcnt);

