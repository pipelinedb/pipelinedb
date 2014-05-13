/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */

#include <sys/types.h>
#include <unistd.h>

#include "gtm/libpq-fe.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"

#include "test_common.h"

pthread_key_t     threadinfo_key;

void
setUp()
{
	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_GTM);
	
	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test_txn_21()
{
	/* request parameters */
	int txn_count = 2;
	GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
	bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
	GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];

	/* results */
	int txn_count2;
	GlobalTransactionId start_gxid;
	GTM_Timestamp start_ts;

	/* request parameters */
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];

	/* results */
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];

	int rc, i;

	for (i=0 ; i<txn_count ; i++)
	{
		txn_isolation_level[i] = GTM_ISOLATION_SERIALIZABLE;
		txn_read_only[i]       = false;
		//		txn_connid[i]          = InvalidGTMProxyConnID;
		txn_connid[i]          = 0;
	}

	SETUP();

	rc = begin_transaction_multi(conn,
				     txn_count, txn_isolation_level, txn_read_only, txn_connid,
				     &txn_count2, &start_gxid, &start_ts);
	_ASSERT( rc>=0 );
	_ASSERT( txn_count2==2 );

	/*
	 * commit test
	 */
	txn_count = txn_count2;

	for (i=0 ; i<txn_count ; i++)
	{
		gxid[i] = start_gxid + i;
	}

	rc = commit_transaction_multi(conn, txn_count, gxid, &txn_count2, &status);
	_ASSERT( rc>=0 );
	_ASSERT( txn_count2==2 );

	tearDown();
}

void
test_txn_22()
{
	/* request parameters */
	int txn_count = 2;
	GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
	bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
	GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];

	/* results */
	int txn_count2;
	GlobalTransactionId start_gxid;
	GTM_Timestamp start_ts;

	/* request parameters */
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];

	/* results */
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];

	int rc, i;

	for (i=0 ; i<txn_count ; i++)
	{
		txn_isolation_level[i] = GTM_ISOLATION_SERIALIZABLE;
		txn_read_only[i]       = false;
		//		txn_connid[i]          = InvalidGTMProxyConnID;
		txn_connid[i]          = 0;
	}

	SETUP();

	rc = begin_transaction_multi(conn,
				     txn_count, txn_isolation_level, txn_read_only, txn_connid,
				     &txn_count2, &start_gxid, &start_ts);
	_ASSERT( rc>=0 );
	_ASSERT( txn_count2==2 );

	/*
	 * abort test
	 */
	txn_count = txn_count2;

	for (i=0 ; i<txn_count ; i++)
	{
		gxid[i] = start_gxid + i;
	}

	rc = abort_transaction_multi(conn, txn_count, gxid, &txn_count2, &status);
	_ASSERT( rc>=0 );
	_ASSERT( txn_count2==2 );

	tearDown();
}

void
test_txn_23()
{
	/* request parameters */
	int txn_count = 2;
	GTM_IsolationLevel txn_isolation_level[GTM_MAX_GLOBAL_TRANSACTIONS];
	bool txn_read_only[GTM_MAX_GLOBAL_TRANSACTIONS];
	GTMProxy_ConnID txn_connid[GTM_MAX_GLOBAL_TRANSACTIONS];

	/* results */
	int txn_count2;
	GlobalTransactionId start_gxid;
	GTM_Timestamp start_ts;

	/* request parameters */
	GlobalTransactionId gxid[GTM_MAX_GLOBAL_TRANSACTIONS];

	/* results */
	int status[GTM_MAX_GLOBAL_TRANSACTIONS];
	GlobalTransactionId xmin_out;
	GlobalTransactionId xmax_out;
	GlobalTransactionId recent_global_xmin_out;
	int32 xcnt_out;

	int rc, i;

	for (i=0 ; i<txn_count ; i++)
	{
		txn_isolation_level[i] = GTM_ISOLATION_SERIALIZABLE;
		txn_read_only[i]       = false;
		//		txn_connid[i]          = InvalidGTMProxyConnID;
		txn_connid[i]          = 0;
	}

	SETUP();

	rc = begin_transaction_multi(conn,
				     txn_count, txn_isolation_level, txn_read_only, txn_connid,
				     &txn_count2, &start_gxid, &start_ts);
	_ASSERT( rc>=0 );
	_ASSERT( txn_count2==2 );

	/*
	 * snapshot get test
	 */
	txn_count = txn_count2;

	for (i=0 ; i<txn_count ; i++)
	{
		gxid[i] = start_gxid + i;
	}

	rc = snapshot_get_multi(conn, txn_count, gxid,
				&txn_count2, &status,
				&xmin_out, &xmax_out, &recent_global_xmin_out, &xcnt_out);
	_ASSERT( rc>=0 );
	_ASSERT( txn_count2==2 );

	tearDown();
}


void
test_txn_31()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	sleep(10); /* kill standby while sleeping. */

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	rc = abort_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	tearDown();
}


int
main(int argc, char *argv[])
{
	test_txn2_01();

	return 0;
}
