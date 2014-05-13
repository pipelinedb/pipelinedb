/*-------------------------------------------------------------------------
 *
 * gtm_serialize_debug.c
 *  Debug functionalities of serialization management
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_serialize_debug.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/elog.h"
#include "gtm/palloc.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/assert.h"
#include "gtm/register.h"
#include "gtm/stringinfo.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"

#include "gtm/gtm_serialize.h"

void
dump_transactioninfo_elog(GTM_TransactionInfo *txn)
{
	int ii;

	elog(DEBUG1, "  ========= GTM_TransactionInfo =========");
	elog(DEBUG1, "gti_handle: %d", txn->gti_handle);
	elog(DEBUG1, "gti_thread_id: %ld", txn->gti_thread_id);
	elog(DEBUG1, "gti_in_use: %d", txn->gti_in_use);
	elog(DEBUG1, "gti_gxid: %d", txn->gti_gxid);
	elog(DEBUG1, "gti_state: %d", txn->gti_state);
	elog(DEBUG1, "gti_coordname: %s", txn->gti_coordname);
	elog(DEBUG1, "gti_xmin: %d", txn->gti_xmin);
	elog(DEBUG1, "gti_isolevel: %d", txn->gti_isolevel);
	elog(DEBUG1, "gti_readonly: %d", txn->gti_readonly);
	elog(DEBUG1, "gti_backend_id: %d", txn->gti_backend_id);
	elog(DEBUG1, "gti_nodestring: %s", txn->nodestring);
	elog(DEBUG1, "gti_gid: %s", txn->gti_gid);

	elog(DEBUG1, "  sn_xmin: %d", txn->gti_current_snapshot.sn_xmin);
	elog(DEBUG1, "  sn_xmax: %d", txn->gti_current_snapshot.sn_xmax);
	elog(DEBUG1, "  sn_recent_global_xmin: %d", txn->gti_current_snapshot.sn_recent_global_xmin);
	elog(DEBUG1, "  sn_xcnt: %d", txn->gti_current_snapshot.sn_xcnt);

	/* Print all the GXIDs in snapshot */
	for (ii = 0; ii < txn->gti_current_snapshot.sn_xcnt; ii++)
	{
		elog (DEBUG1, "  sn_xip[%d]: %d", ii, txn->gti_current_snapshot.sn_xip[ii]);
	}

	elog(DEBUG1, "gti_snapshot_set: %d", txn->gti_snapshot_set);
	elog(DEBUG1, "gti_vacuum: %d", txn->gti_vacuum);
	elog(DEBUG1, "  ========================================");
}

void
dump_transactions_elog(GTM_Transactions *txn, int num_txn)
{
	int i;

	elog(DEBUG1, "============ GTM_Transactions ============");
	elog(DEBUG1, "  gt_txn_count: %d", txn->gt_txn_count);
	elog(DEBUG1, "  gt_XidGenLock: %p", &txn->gt_XidGenLock);
	elog(DEBUG1, "  gt_nextXid: %d", txn->gt_nextXid);
	elog(DEBUG1, "  gt_oldestXid: %d", txn->gt_oldestXid);
	elog(DEBUG1, "  gt_xidVacLimit: %d", txn->gt_xidVacLimit);
	elog(DEBUG1, "  gt_xidWarnLimit: %d", txn->gt_xidWarnLimit);
	elog(DEBUG1, "  gt_xidStopLimit: %d", txn->gt_xidStopLimit);
	elog(DEBUG1, "  gt_xidWrapLimit: %d", txn->gt_xidWrapLimit);
	elog(DEBUG1, "  gt_latestCompletedXid: %d", txn->gt_latestCompletedXid);
	elog(DEBUG1, "  gt_recent_global_xmin: %d", txn->gt_recent_global_xmin);
	elog(DEBUG1, "  gt_lastslot: %d", txn->gt_lastslot);

	for (i = 0; i < num_txn; i++)
	{
		if (txn->gt_transactions_array[i].gti_gxid != InvalidGlobalTransactionId)
			dump_transactioninfo_elog(&txn->gt_transactions_array[i]);
	}

	elog(DEBUG1, "  gt_TransArrayLock: %p", &txn->gt_TransArrayLock);
	elog(DEBUG1, "==========================================");
}
