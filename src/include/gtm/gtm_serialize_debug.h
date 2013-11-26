/*-------------------------------------------------------------------------
 *
 * gtm_serialize_debug.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_serialize_debug.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_SERIALIZE_DEBUG_H
#define GTM_SERIALIZE_DEBUG_H

#include <sys/types.h>

#include "gtm/gtm_txn.h"

void dump_transactions_elog(GTM_Transactions *, int);
void dump_transactioninfo_elog(GTM_TransactionInfo *);

#endif /* GTM_SERIALIZE_DEBUG_H */
