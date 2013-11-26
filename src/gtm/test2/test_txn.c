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
test_txn_01()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	rc = abort_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn_02()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	rc = commit_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn_03()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction_autovacuum(conn, GTM_ISOLATION_SERIALIZABLE);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	rc = commit_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn_11()
{
	GTM_IsolationLevel isolevel = GTM_ISOLATION_SERIALIZABLE;
	char *gid = "test";
	GlobalTransactionId gxid =InvalidGlobalTransactionId;
	GlobalTransactionId prepared_gxid =InvalidGlobalTransactionId;
	int datanodecnt = 0;
	char **datanodes = NULL;
	int coordcnt = 0;
	char **coordinators = NULL;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, isolevel, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );
	printf("test_txn_11: gxid=%d\n", gxid);

	rc = start_prepared_transaction(conn, gxid, gid,
					datanodecnt, datanodes, coordcnt, coordinators);
	_ASSERT( rc>=0 );

	rc = get_gid_data(conn, isolevel, gid,
			  &gxid, &prepared_gxid, &datanodecnt, datanodes, &coordcnt, coordinators);
	printf("test_txn_11: prepared_gxid=%d\n", prepared_gxid);
	_ASSERT( rc>=0 );

	rc = commit_prepared_transaction(conn, gxid, prepared_gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}


void
test_txn_51()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid == InvalidGlobalTransactionId );

	TEARDOWN();
}

void
test_txn_52()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	rc = prepare_transaction(conn, InvalidGlobalTransactionId);
	_ASSERT( rc==-1 );

	TEARDOWN();
}

void
test_txn_53()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	rc = commit_transaction(conn, InvalidGlobalTransactionId);
	_ASSERT( rc==-1 );

	TEARDOWN();
}

void
test_txn_54()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	rc = abort_transaction(conn, InvalidGlobalTransactionId);
	_ASSERT( rc==-1 );

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_GTM);
	
	test_txn_01();
	test_txn_02();
	test_txn_03();

	test_txn_11();

	/*
	 * connect to standby. must be prevented.
	 */
	sprintf(connect_string, "host=localhost port=6667 node_name=one remote_type=%d",
		GTM_NODE_GTM);
	
	test_txn_51();
	test_txn_52();
	test_txn_53();
	test_txn_54();

	/*
	 * promote GTM standby to become GTM active.
	 */
	system("./promote.sh");
	sleep(1);

	test_txn_01();
	test_txn_02();
	test_txn_03();

	test_txn_11();

	return 0;
}
