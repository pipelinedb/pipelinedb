/*-------------------------------------------------------------------------
 *
 * gtm_serialize.c
 *  Serialization management of GTM data
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_serialize.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/assert.h"
#include "gtm/register.h"
#include "gtm/stringinfo.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"

#include "gen_alloc.h"

#include "gtm/gtm_serialize.h"

/*
 * gtm_get_snapshotdata_size
 * Get a serialized size of GTM_SnapshotData structure
 * Corrected snapshort serialize data calculation.
 * May 3rd, 2011, K.Suzuki
 *
 * Serialize of snapshot_data
 *
 * sn_xmin ---> sn_xmax ---> sn_recent_global_xmin
 * ---> sn_xcnt ---> GXID * sn_xcnt
 *  |<--- sn_xip -->|
 */
size_t
gtm_get_snapshotdata_size(GTM_SnapshotData *data)
{
	size_t len = 0;
	uint32 snapshot_elements;

	snapshot_elements = data->sn_xcnt;
	len += sizeof(GlobalTransactionId);
	len += sizeof(GlobalTransactionId);
	len += sizeof(GlobalTransactionId);
	len += sizeof(uint32);
	len += sizeof(GlobalTransactionId) * snapshot_elements;

	return len;
}

/*
 * gtm_serialize_snapshotdata
 * Serialize a GTM_SnapshotData structure
 */
size_t
gtm_serialize_snapshotdata(GTM_SnapshotData *data, char *buf, size_t buflen)
{
	int len = 0;

	memset(buf, 0, buflen);

	/* size check */
	if (gtm_get_snapshotdata_size(data) > buflen)
	  return 0;

	/* GTM_SnapshotData.sn_xmin */
	memcpy(buf + len, &(data->sn_xmin), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_SnapshotData.sn_xmax */
	memcpy(buf + len, &(data->sn_xmax), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_SnapshotData.sn_recent_global_xmin */
	memcpy(buf + len, &(data->sn_recent_global_xmin), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_SnapshotData.sn_xcnt */
	memcpy(buf + len, &(data->sn_xcnt), sizeof(uint32));
	len += sizeof(uint32);

	/* GTM_SnapshotData.sn_xip */
#if 0
	/*
	 * This block of code seems to be wrong.  data->sn_xip is an array of GlobalTransacionIDs
	 * and the number of elements are indicated by sn_xcnt.
	 */
	memcpy(buf + len, &(data->sn_xip), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);
#else
	if(data->sn_xcnt > 0)
	{
		memcpy(buf + len, data->sn_xip, sizeof(GlobalTransactionId) * data->sn_xcnt);
		len += sizeof(GlobalTransactionId) * data->sn_xcnt;
	}
#endif

	return len;
}

/* -----------------------------------------------------
 * Deserialize a GTM_SnapshotData structure
 * -----------------------------------------------------
 */
size_t
gtm_deserialize_snapshotdata(GTM_SnapshotData *data, const char *buf, size_t buflen)
{
	size_t len = 0;

	/* GTM_SnapshotData.sn_xmin */
	memcpy(&(data->sn_xmin), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_SnapshotData.sn_xmax */
	memcpy(&(data->sn_xmax), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_SnapshotData.sn_recent_global_xmin */
	memcpy(&(data->sn_recent_global_xmin), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_SnapshotData.sn_xcnt */
	memcpy(&(data->sn_xcnt), buf + len, sizeof(uint32));
	len += sizeof(uint32);

	/* GTM_SnapshotData.sn_xip */
	if (data->sn_xcnt > 0)
	{
		/*
		 * Please note that this function runs with TopMemoryContext.  So we must
		 * free this area manually later.
		 */
		data->sn_xip = genAlloc(sizeof(GlobalTransactionId) * data->sn_xcnt);
		memcpy(data->sn_xip, buf + len, sizeof(GlobalTransactionId) * data->sn_xcnt);
		len += sizeof(GlobalTransactionId) * data->sn_xcnt;
	}
	else
	{
		data->sn_xip = NULL;
	}

	return len;
}


/*
 * gtm_get_transactioninfo_size
 * Get a serialized size of GTM_TransactionInfo structure
 *
 * Original gti_gid serialization was just "null-terminated string".
 * This should be prefixed with the length of the string.
 */
size_t
gtm_get_transactioninfo_size(GTM_TransactionInfo *data)
{
	size_t len = 0;

	if (data == NULL)
		return len;

	len += sizeof(GTM_TransactionHandle);		/* gti_handle */
	len += sizeof(GTM_ThreadID);			/* gti_thread_id */
	len += sizeof(bool);				/* gti_in_use */
	len += sizeof(GlobalTransactionId);		/* gti_gxid */
	len += sizeof(GTM_TransactionStates);		/* gti_state */
	len += sizeof(uint32);				/* used to store length of gti_coordname*/
	if (data->gti_coordname != NULL)
		len += strlen(data->gti_coordname);	/* gti_coordname */
	len += sizeof(GlobalTransactionId);		/* gti_xmin */
	len += sizeof(GTM_IsolationLevel);		/* gti_isolevel */
	len += sizeof(bool);				/* gti_readonly */
	len += sizeof(GTMProxy_ConnID);			/* gti_backend_id */
	len += sizeof(uint32);				/* gti_nodestring length */
	if (data->nodestring != NULL)
		len += strlen(data->nodestring);

	len += sizeof(uint32);
	if (data->gti_gid != NULL)
		len += strlen(data->gti_gid);		/* gti_gid */

	len += gtm_get_snapshotdata_size(&(data->gti_current_snapshot));
	/* gti_current_snapshot */
	len += sizeof(bool);				/* gti_snapshot_set */
	/* NOTE: nothing to be done for gti_lock */
	len += sizeof(bool);				/* gti_vacuum */

	return len;
}

/* -----------------------------------------------------
 * Serialize a GTM_TransactionInfo structure
 * -----------------------------------------------------
 */
size_t
gtm_serialize_transactioninfo(GTM_TransactionInfo *data, char *buf, size_t buflen)
{
	int len = 0;
	char *buf2;
	int i;
	int namelen;

	/* size check */
	if (gtm_get_transactioninfo_size(data) > buflen)
	  return 0;

	memset(buf, 0, buflen);

	/* GTM_TransactionInfo.gti_handle */
	memcpy(buf + len, &(data->gti_handle), sizeof(GTM_TransactionHandle));
	len += sizeof(GTM_TransactionHandle);

	/* GTM_TransactionInfo.gti_thread_id */
	memcpy(buf + len, &(data->gti_thread_id), sizeof(GTM_ThreadID));
	len += sizeof(GTM_ThreadID);

	/* GTM_TransactionInfo.gti_in_use */
	memcpy(buf + len, &(data->gti_in_use), sizeof(bool));
	len += sizeof(bool);

	/* GTM_TransactionInfo.gti_gxid */
	memcpy(buf + len, &(data->gti_gxid), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_TransactionInfo.gti_state */
	memcpy(buf + len, &(data->gti_state), sizeof(GTM_TransactionStates));
	len += sizeof(GTM_TransactionStates);

	/* GTM_TransactionInfo.gti_coordname */
	if (data->gti_coordname != NULL)
	{
		namelen = (uint32)strlen(data->gti_coordname);
		memcpy(buf + len, &namelen, sizeof(uint32));
		len += sizeof(uint32);
		memcpy(buf + len, data->gti_coordname, namelen);
		len += namelen;
	}
	else
	{
		namelen = 0;
		memcpy(buf + len, &namelen, sizeof(uint32));
		len += sizeof(uint32);
	}

	/* GTM_TransactionInfo.gti_xmin */
	memcpy(buf + len, &(data->gti_xmin), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_TransactionInfo.gti_isolevel */
	memcpy(buf + len, &(data->gti_isolevel), sizeof(GTM_IsolationLevel));
	len += sizeof(GTM_IsolationLevel);

	/* GTM_TransactionInfo.gti_readonly */
	memcpy(buf + len, &(data->gti_readonly), sizeof(bool));
	len += sizeof(bool);

	/* GTM_TransactionInfo.gti_backend_id */
	memcpy(buf + len, &(data->gti_backend_id), sizeof(GTMProxy_ConnID));
	len += sizeof(GTMProxy_ConnID);

	/* GTM_TransactionInfo.nodestring */
	if (data->nodestring != NULL)
	{
		uint32 gidlen;

		gidlen = (uint32)strlen(data->nodestring);
		memcpy(buf + len, &gidlen, sizeof(uint32));
		len += sizeof(uint32);
		memcpy(buf + len, data->nodestring, gidlen);
		len += gidlen;
	}
	else
	{
		uint32 gidlen = 0;

		memcpy(buf + len, &gidlen, sizeof(uint32));
		len += sizeof(uint32);
	}

	/* GTM_TransactionInfo.gti_gid */
	if (data->gti_gid != NULL)
	{
		uint32 gidlen;

		gidlen = (uint32)strlen(data->gti_gid);
		memcpy(buf + len, &gidlen, sizeof(uint32));
		len += sizeof(uint32);
		memcpy(buf + len, data->gti_gid, gidlen);
		len += gidlen;
	}
	else
	{
		uint32 gidlen = 0;

		memcpy(buf + len, &gidlen, sizeof(uint32));
		len += sizeof(uint32);
	}

	/* GTM_TransactionInfo.gti_current_snapshot */
	buf2 = malloc(gtm_get_snapshotdata_size(&(data->gti_current_snapshot)));
	i = gtm_serialize_snapshotdata(&(data->gti_current_snapshot),
									buf2,
									gtm_get_snapshotdata_size(&(data->gti_current_snapshot)));
	memcpy(buf + len, buf2, i);
	free(buf2);
	len += i;

	/* GTM_TransactionInfo.gti_snapshot_set */
	memcpy(buf + len, &(data->gti_snapshot_set), sizeof(bool));
	len += sizeof(bool);

	/* GTM_TransactionInfo.gti_lock would not be serialized. */

	/* GTM_TransactionInfo.gti_vacuum */
	memcpy(buf + len, &(data->gti_vacuum), sizeof(bool));
	len += sizeof(bool);

	return len;
}

/* -----------------------------------------------------
 * Deserialize a GTM_TransactionInfo structure
 * -----------------------------------------------------
 */
size_t
gtm_deserialize_transactioninfo(GTM_TransactionInfo *data, const char *buf, size_t maxlen)
{
	int len = 0;
	int i;
	uint32 string_len;

	memset(data, 0, sizeof(GTM_TransactionInfo));

	/* GTM_TransactionInfo.gti_handle */
	memcpy(&(data->gti_handle), buf + len, sizeof(GTM_TransactionHandle));
	len += sizeof(GTM_TransactionHandle);

	/* GTM_TransactionInfo.gti_thread_id */
	memcpy(&(data->gti_thread_id), buf + len, sizeof(GTM_ThreadID));
	len += sizeof(GTM_ThreadID);

	/* GTM_TransactionInfo.gti_in_use */
	memcpy(&(data->gti_in_use), buf + len, sizeof(bool));
	len += sizeof(bool);

	/* GTM_TransactionInfo.gti_gxid */
	memcpy(&(data->gti_gxid), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_TransactionInfo.gti_state */
	memcpy(&(data->gti_state), buf + len, sizeof(GTM_TransactionStates));
	len += sizeof(GTM_TransactionStates);

	/* GTM_TransactionInfo.gti_coordname */
	{
		uint32 ll;

		memcpy(&ll, buf+len, sizeof(uint32));
		len += sizeof(uint32);
		if (ll > 0)
		{
			data->gti_coordname = genAllocTop(sizeof(ll+1));	/* Should be allocated at TopMostContext */
			memcpy(data->gti_coordname, buf+len, ll);
			data->gti_coordname[ll] = 0;
		}
		else
			data->gti_coordname = NULL;
	}

	/* GTM_TransactionInfo.gti_xmin */
	memcpy(&(data->gti_xmin), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_TransactionInfo.gti_isolevel */
	memcpy(&(data->gti_isolevel), buf + len, sizeof(GTM_IsolationLevel));
	len += sizeof(GTM_IsolationLevel);

	/* GTM_TransactionInfo.gti_readonly */
	memcpy(&(data->gti_readonly), buf + len, sizeof(bool));
	len += sizeof(bool);

	/* GTM_TransactionInfo.gti_backend_id */
	memcpy(&(data->gti_backend_id), buf + len, sizeof(GTMProxy_ConnID));
	len += sizeof(GTMProxy_ConnID);

	/* GTM_TransactionInfo.gti_nodestring */
	memcpy(&string_len, buf + len, sizeof(uint32));
	len += sizeof(uint32);
	if (string_len > 0)
	{
		data->nodestring = (char *)genAllocTop(string_len + 1);	/* Should allocate at TopMostMemoryContext */
		memcpy(data->nodestring, buf + len, string_len);
		data->nodestring[string_len] = 0;		/* null-terminated */
		len += string_len;
	}
	else
		data->nodestring = NULL;

	/* GTM_TransactionInfo.gti_gid */
	memcpy(&string_len, buf + len, sizeof(uint32));
	len += sizeof(uint32);
	if (string_len > 0)
	{
		data->gti_gid = (char *)genAllocTop(string_len+1);	/* Should allocate at TopMostMemoryContext */
		memcpy(data->gti_gid, buf + len, string_len);
		data->gti_gid[string_len] = 0;				/* null-terminated */
		len += string_len;
	}
	else
		data->gti_gid = NULL;

	/* GTM_TransactionInfo.gti_current_snapshot */
	i = gtm_deserialize_snapshotdata(&(data->gti_current_snapshot),
									 buf + len,
									 sizeof(GTM_SnapshotData));
	len += i;

	/* GTM_TransactionInfo.gti_snapshot_set */
	memcpy(&(data->gti_snapshot_set), buf + len, sizeof(bool));
	len += sizeof(bool);

	/* GTM_TransactionInfo.gti_lock would not be serialized. */

	/* GTM_TransactionInfo.gti_vacuum */
	memcpy(&(data->gti_vacuum), buf + len, sizeof(bool));
	len += sizeof(bool);

	return len;
}


size_t
gtm_get_transactions_size(GTM_Transactions *data)
{
	size_t len = 0;
	int i;

	len += sizeof(uint32);/* gt_txn_count */
	len += sizeof(GTM_States);/* gt_gtm_state */

	/* NOTE: nothing to be done for gt_XidGenLock */

	len += sizeof(GlobalTransactionId);   /* gt_nextXid */
	len += sizeof(GlobalTransactionId);   /* gt_oldestXid */
	len += sizeof(GlobalTransactionId);   /* gt_xidVacLimit */
	len += sizeof(GlobalTransactionId);   /* gt_xidWarnLimit */
	len += sizeof(GlobalTransactionId);   /* gt_xidStopLimit */
	len += sizeof(GlobalTransactionId);   /* gt_xidWrapLimit */

	len += sizeof(GlobalTransactionId);   /* gt_latestCompletedXid */
	len += sizeof(GlobalTransactionId);   /* gt_recent_global_xmin */

	len += sizeof(int32); /* gt_lastslot */

	len += sizeof(int32); /* txn_count */

	for (i = 0; i < GTM_MAX_GLOBAL_TRANSACTIONS; i++)
	{
		len += sizeof(size_t); /* length */
		len += gtm_get_transactioninfo_size(&data->gt_transactions_array[i]);
	}

	/* NOTE: nothing to be done for gt_open_transactions */
	/* NOTE: nothing to be done for gt_TransArrayLock */

	return len;
}

/*
 * Return a number of serialized transactions.
 */
size_t
gtm_serialize_transactions(GTM_Transactions *data, char *buf, size_t buflen)
{
	int len = 0;
	int i;
	uint32 txn_count;

	/* size check */
	if (gtm_get_transactions_size(data) > buflen)
	  return 0;

	memset(buf, 0, buflen);

	/* GTM_Transactions.gt_txn_count */
	memcpy(buf + len, &(data->gt_txn_count), sizeof(uint32));
	len += sizeof(uint32);

	/* GTM_Transactions.gt_gtm_state */
	memcpy(buf + len, &(data->gt_gtm_state), sizeof(GTM_States));
	len += sizeof(GTM_States);

	/* NOTE: nothing to be done for gt_XidGenLock */

	/* GTM_Transactions.gt_nextXid */
	memcpy(buf + len, &(data->gt_nextXid), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_oldestXid */
	memcpy(buf + len, &(data->gt_oldestXid), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidVacLimit */
	memcpy(buf + len, &(data->gt_xidVacLimit), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidWarnLimit */
	memcpy(buf + len, &(data->gt_xidWarnLimit), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidStopLimit */
	memcpy(buf + len, &(data->gt_xidStopLimit), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidWrapLimit */
	memcpy(buf + len, &(data->gt_xidWrapLimit), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_latestCompletedXid */
	memcpy(buf + len, &(data->gt_latestCompletedXid), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_recent_global_xmin */
	memcpy(buf + len, &(data->gt_recent_global_xmin), sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_lastslot */
	memcpy(buf + len, &(data->gt_lastslot), sizeof(int32));
	len += sizeof(int32);

	/* Count up for valid transactions. */
	txn_count = 0;

	for (i = 0; i < GTM_MAX_GLOBAL_TRANSACTIONS; i++)
	{
		/* Select a used slot with the transaction array. */
		if (data->gt_transactions_array[i].gti_in_use == TRUE)
			txn_count++;
	}

	memcpy(buf + len, &txn_count, sizeof(int32));
	len += sizeof(int32);

	/*
	 * GTM_Transactions.gt_transactions_array
	 */
	for (i = 0; i < GTM_MAX_GLOBAL_TRANSACTIONS; i++)
	{
		char *buf2;
		size_t buflen2, len2;

		/*
		 * Not to include invalid global transactions.
		 */
		if (data->gt_transactions_array[i].gti_in_use != TRUE)
			continue;

		buflen2 = gtm_get_transactioninfo_size(&data->gt_transactions_array[i]);

		/* store a length of following data. */
		memcpy(buf + len, &buflen2, sizeof(size_t));
		len += sizeof(size_t);

		buf2 = (char *)malloc(buflen2);

		len2 = gtm_serialize_transactioninfo(&data->gt_transactions_array[i],
						  buf2,
						  buflen2);

		/* store a serialized GTM_TransactionInfo structure. */
		memcpy(buf + len, buf2, len2);
		len += len2;

		free(buf2);
	}

	/* NOTE: nothing to be done for gt_TransArrayLock */
	return len;
}


/*
 * Return a number of deserialized transactions.
 */
size_t
gtm_deserialize_transactions(GTM_Transactions *data, const char *buf, size_t maxlen)
{
	int len = 0;
	int i;
	uint32 txn_count;

	/* GTM_Transactions.gt_txn_count */
	memcpy(&(data->gt_txn_count), buf + len, sizeof(uint32));
	len += sizeof(uint32);

	/* GTM_Transactions.gt_gtm_state */
	memcpy(&(data->gt_gtm_state), buf + len, sizeof(GTM_States));
	len += sizeof(GTM_States);

	/* NOTE: nothing to be done for gt_XidGenLock */

	/* GTM_Transactions.gt_nextXid */
	memcpy(&(data->gt_nextXid), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_oldestXid */
	memcpy(&(data->gt_oldestXid), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidVacLimit */
	memcpy(&(data->gt_xidVacLimit), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidWarnLimit */
	memcpy(&(data->gt_xidWarnLimit), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidStopLimit */
	memcpy(&(data->gt_xidStopLimit), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_xidWrapLimit */
	memcpy(&(data->gt_xidWrapLimit), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_latestCompletedXid */
	memcpy(&(data->gt_latestCompletedXid), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_recent_global_xmin */
	memcpy(&(data->gt_recent_global_xmin), buf + len, sizeof(GlobalTransactionId));
	len += sizeof(GlobalTransactionId);

	/* GTM_Transactions.gt_lastslot */
	memcpy(&(data->gt_lastslot), buf + len, sizeof(int32));
	len += sizeof(int32);

	/* A number of valid transactions */
	memcpy(&txn_count, buf + len, sizeof(int32));
	len += sizeof(int32);

	/* GTM_Transactions.gt_transactions_array */
	for (i = 0; i < txn_count; i++)
	{
		size_t buflen2, len2;

		/* read a length of following data. */
		memcpy(&buflen2, buf + len, sizeof(size_t));
		len += sizeof(size_t);

		/* reada serialized GTM_TransactionInfo structure. */
		len2 = gtm_deserialize_transactioninfo(&(data->gt_transactions_array[i]),
						buf + len,
						buflen2);

		len += len2;
	}

	/* NOTE: nothing to be done for gt_TransArrayLock */

	return txn_count;
}


/*
 * Return size of PGXC node information
 */
size_t
gtm_get_pgxcnodeinfo_size(GTM_PGXCNodeInfo *data)
{
	size_t len = 0;

	len += sizeof(GTM_PGXCNodeType); /* type */

	len += sizeof(uint32);		/* proxy name length */
	if (data->proxyname != NULL)	/* proxy name */
		len += strlen(data->proxyname);

	len += sizeof(GTM_PGXCNodePort); /* port */

	len += sizeof(uint32);		/* node name length */
	if (data->nodename != NULL)	/* node name */
		len += strlen(data->nodename);

	len += sizeof(uint32);		/* ipaddress length */
	if (data->ipaddress != NULL)	/* ipaddress */
		len += strlen(data->ipaddress);

	len += sizeof(uint32);			/* datafolder length */
	if (data->datafolder != NULL)	/* datafolder */
		len += strlen(data->datafolder);

	len += sizeof(GTM_PGXCNodeStatus);   /* status */

	return len;
}

/*
 * Return a serialize number of PGXC node information
 */
size_t
gtm_serialize_pgxcnodeinfo(GTM_PGXCNodeInfo *data, char *buf, size_t buflen)
{
	size_t len = 0;
	uint32 len_wk;

	/* size check */
	if (gtm_get_pgxcnodeinfo_size(data) > buflen)
		return 0;

	memset(buf, 0, buflen);

	/* GTM_PGXCNodeInfo.type */
	memcpy(buf + len, &(data->type), sizeof(GTM_PGXCNodeType));
	len += sizeof(GTM_PGXCNodeType);

	/* GTM_PGXCNodeInfo.nodename */
	if (data->nodename == NULL)
		len_wk = 0;
	else
		len_wk = (uint32)strlen(data->nodename);

	memcpy(buf + len, &len_wk, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk > 0)
	{
		memcpy(buf + len, data->nodename, len_wk);
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.proxyname */
	if (data->proxyname == NULL)
		len_wk = 0;
	else
		len_wk = (uint32)strlen(data->proxyname);

	memcpy(buf + len, &len_wk, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk > 0)
	{
		memcpy(buf + len, data->proxyname, len_wk);
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.port */
	memcpy(buf + len, &(data->port), sizeof(GTM_PGXCNodePort));
	len += sizeof(GTM_PGXCNodePort);

	/* GTM_PGXCNodeInfo.ipaddress */
	if (data->ipaddress == NULL)
		len_wk = 0;
	else
		len_wk = (uint32)strlen(data->ipaddress);

	memcpy(buf + len, &len_wk, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk > 0)
	{
		memcpy(buf + len, data->ipaddress, len_wk);
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.datafolder */
	if (data->datafolder == NULL)
		len_wk = 0;
	else
		len_wk = (uint32)strlen(data->datafolder);

	memcpy(buf + len, &len_wk, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk > 0)
	{
		memcpy(buf + len, data->datafolder, len_wk);
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.status */
	memcpy(buf + len, &(data->status), sizeof(GTM_PGXCNodeStatus));
	len += sizeof(GTM_PGXCNodeStatus);

	/* NOTE: nothing to be done for node_lock */
	return len;
}


/*
 * Return a deserialize number of PGXC node information
 */
size_t
gtm_deserialize_pgxcnodeinfo(GTM_PGXCNodeInfo *data, const char *buf, size_t buflen)
{
	size_t len = 0;
	uint32 len_wk;

	/* GTM_PGXCNodeInfo.type */
	memcpy(&(data->type), buf + len, sizeof(GTM_PGXCNodeType));
	len += sizeof(GTM_PGXCNodeType);

	/* GTM_PGXCNodeInfo.nodename*/
	memcpy(&len_wk, buf + len, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk == 0)
	{
		data->nodename = NULL;
	}
	else
	{
		/* PGXCTODO: free memory */
		data->nodename = (char *)genAlloc(len_wk + 1);
		memcpy(data->nodename, buf + len, (size_t)len_wk);
		data->nodename[len_wk] = 0;	/* null_terminate */
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.proxyname*/
	memcpy(&len_wk, buf + len, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk == 0)
	{
		data->proxyname = NULL;
	}
	else
	{
		/* PGXCTODO: free memory */
		data->proxyname = (char *)genAlloc(len_wk + 1);
		memcpy(data->proxyname, buf + len, (size_t)len_wk);
		data->proxyname[len_wk] = 0;	/* null_terminate */
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.port */
	memcpy(&(data->port), buf + len, sizeof(GTM_PGXCNodePort));
	len += sizeof(GTM_PGXCNodePort);

	/* GTM_PGXCNodeInfo.ipaddress */
	memcpy(&len_wk, buf + len, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk == 0)
	{
		data->ipaddress = NULL;
	}
	else
	{
		data->ipaddress = (char *)genAlloc(len_wk + 1);
		memcpy(data->ipaddress, buf + len, (size_t)len_wk);
		data->ipaddress[len_wk] = 0;	/* null_terminate */
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.datafolder */
	memcpy(&len_wk, buf + len, sizeof(uint32));
	len += sizeof(uint32);
	if (len_wk == 0)
	{
		data->datafolder = NULL;
	}
	else
	{
		data->datafolder = (char *)genAlloc(len_wk + 1);
		memcpy(data->datafolder, buf + len, (size_t)len_wk);
		data->datafolder[len_wk] = 0;	/* null_terminate */
		len += len_wk;
	}

	/* GTM_PGXCNodeInfo.status */
	memcpy(&(data->status), buf + len, sizeof(GTM_PGXCNodeStatus));
	len += sizeof(GTM_PGXCNodeStatus);

	/* NOTE: nothing to be done for node_lock */

	return len;
}


/*
 * Return size of sequence information
 */
size_t
gtm_get_sequence_size(GTM_SeqInfo *seq)
{
	size_t len = 0;

	len += sizeof(uint32);/* gs_key.gsk_keylen */
	len += seq->gs_key->gsk_keylen;   /* gs_key.gsk_key */
	len += sizeof(GTM_SequenceKeyType);   /* gs_key.gsk_type */
	len += sizeof(GTM_Sequence);  /* gs_value */
	len += sizeof(GTM_Sequence);  /* gs_init_value */
	len += sizeof(GTM_Sequence);  /* gs_last_value */
	len += sizeof(GTM_Sequence);  /* gs_increment_by */
	len += sizeof(GTM_Sequence);  /* gs_min_value */
	len += sizeof(GTM_Sequence);  /* gs_max_value */
	len += sizeof(bool);  /* gs_cycle */
	len += sizeof(bool);  /* gs_called */
	len += sizeof(uint32);					  /* gs_ref_count */
	len += sizeof(uint32);					  /* ge_state */

	return len;
}

/*
 * Return number of serialized sequence information
 */
size_t
gtm_serialize_sequence(GTM_SeqInfo *s, char *buf, size_t buflen)
{
	size_t len = 0;

	/* size check */
	if (gtm_get_sequence_size(s) > buflen)
		return 0;

	memset(buf, 0, buflen);

	memcpy(buf + len, &s->gs_key->gsk_keylen, sizeof(uint32));
	len += sizeof(uint32);/* gs_key.gsk_keylen */

	memcpy(buf + len, s->gs_key->gsk_key, s->gs_key->gsk_keylen);
	len += s->gs_key->gsk_keylen; /* gs_key.gsk_key */

	memcpy(buf + len, &s->gs_key->gsk_type, sizeof(GTM_SequenceKeyType));
	len += sizeof(GTM_SequenceKeyType);   /* gs_key.gsk_type */

	memcpy(buf + len, &s->gs_value, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_value */

	memcpy(buf + len, &s->gs_init_value, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_init_value */

	memcpy(buf + len, &s->gs_last_value, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_last_value */

	memcpy(buf + len, &s->gs_increment_by, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_increment_by */

	memcpy(buf + len, &s->gs_min_value, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_min_value */

	memcpy(buf + len, &s->gs_max_value, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_max_value */

	memcpy(buf + len, &s->gs_cycle, sizeof(bool));
	len += sizeof(bool);  /* gs_cycle */

	memcpy(buf + len, &s->gs_called, sizeof(bool));
	len += sizeof(bool);  /* gs_called */

	memcpy(buf + len, &s->gs_ref_count, sizeof(uint32));
	len += sizeof(uint32);					  /* gs_ref_count */

	memcpy(buf + len, &s->gs_state, sizeof(uint32));
	len += sizeof(uint32);					  /* gs_state */

	return len;
}

/*
 * Return number of deserialized sequence information
 */
size_t
gtm_deserialize_sequence(GTM_SeqInfo *seq, const char *buf, size_t buflen)
{
	size_t len = 0;

	seq->gs_key = (GTM_SequenceKeyData *)genAlloc0(sizeof(GTM_SequenceKeyData));

	memcpy(&seq->gs_key->gsk_keylen, buf + len, sizeof(uint32));
	len += sizeof(uint32);/* gs_key.gsk_keylen */

	seq->gs_key->gsk_key = (char *)genAlloc0(seq->gs_key->gsk_keylen+1);
	memcpy(seq->gs_key->gsk_key, buf + len, seq->gs_key->gsk_keylen);
	len += seq->gs_key->gsk_keylen;/* gs_key.gsk_key */

	memcpy(&seq->gs_key->gsk_type, buf + len, sizeof(GTM_SequenceKeyType));
	len += sizeof(GTM_SequenceKeyType);   /* gs_key.gsk_type */

	memcpy(&seq->gs_value, buf + len, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_value */

	memcpy(&seq->gs_init_value, buf + len, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_init_value */

	memcpy(&seq->gs_last_value, buf + len, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_last_value */

	memcpy(&seq->gs_increment_by, buf + len, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_increment_by */

	memcpy(&seq->gs_min_value, buf + len, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_min_value */

	memcpy(&seq->gs_max_value, buf + len, sizeof(GTM_Sequence));
	len += sizeof(GTM_Sequence);  /* gs_max_value */

	memcpy(&seq->gs_cycle, buf + len, sizeof(bool));
	len += sizeof(bool);  /* gs_cycle */

	memcpy(&seq->gs_called, buf + len, sizeof(bool));
	len += sizeof(bool);  /* gs_called */

	memcpy(&seq->gs_ref_count, buf + len, sizeof(uint32));
	len += sizeof(uint32);

	memcpy(&seq->gs_state, buf + len, sizeof(uint32));
	len += sizeof(uint32);

	return len;
}
