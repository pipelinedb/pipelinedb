/*-------------------------------------------------------------------------
 *
 * gtm_serialize.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_serialize.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GTM_SERIALIZE_H
#define GTM_SERIALIZE_H

#include <sys/types.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm_txn.h"
#include "gtm/register.h"
#include "gtm/gtm_seq.h"

size_t gtm_get_snapshotdata_size(GTM_SnapshotData *);
size_t gtm_serialize_snapshotdata(GTM_SnapshotData *, char *, size_t);
size_t gtm_deserialize_snapshotdata(GTM_SnapshotData *, const char *, size_t);

size_t gtm_get_transactioninfo_size(GTM_TransactionInfo *);
size_t gtm_serialize_transactioninfo(GTM_TransactionInfo *, char *, size_t);
size_t gtm_deserialize_transactioninfo(GTM_TransactionInfo *, const char *, size_t);

size_t gtm_get_transactions_size(GTM_Transactions *);
size_t gtm_serialize_transactions(GTM_Transactions *, char *, size_t);
size_t gtm_deserialize_transactions(GTM_Transactions *, const char *, size_t);

size_t gtm_get_pgxcnodeinfo_size(GTM_PGXCNodeInfo *);
size_t gtm_serialize_pgxcnodeinfo(GTM_PGXCNodeInfo *, char *, size_t);
size_t gtm_deserialize_pgxcnodeinfo(GTM_PGXCNodeInfo *, const char *, size_t);

size_t gtm_get_sequence_size(GTM_SeqInfo *);
size_t gtm_serialize_sequence(GTM_SeqInfo *, char *, size_t);
size_t gtm_deserialize_sequence(GTM_SeqInfo *seq, const char *, size_t);

void dump_transactions_elog(GTM_Transactions *, int);
void dump_transactioninfo_elog(GTM_TransactionInfo *);

#endif /* GTM_SERIALIZE_H */
