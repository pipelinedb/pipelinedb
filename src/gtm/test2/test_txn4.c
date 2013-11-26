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
	connect1();
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test_txn4_01()
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

	_ASSERT( grep_count(LOG_STANDBY, "Sending transaction id 3")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Preparing transaction id 3")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Committing transaction id 3")==1 );

	TEARDOWN();
}

void
test_txn4_02()
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

	_ASSERT( grep_count(LOG_STANDBY, "Sending transaction id 4")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Preparing transaction id 4")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Cancelling transaction id 4")==1 );

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	system("./stop.sh");
	system("./clean.sh");
	system("./start.sh");

	test_txn4_01();
	test_txn4_02();

	return 0;
}
