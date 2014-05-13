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
	system("./stop.sh > /dev/null");
	system("./clean.sh > /dev/null");
	system("./start.sh > /dev/null");

	connect1();
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test_txn5_01()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	rc = commit_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn5_02()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();

	rc = commit_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn5_03()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	rc = abort_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn5_04()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();

	rc = abort_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn5_05()
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

	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();

	rc = commit_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_txn5_06()
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

	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();

	rc = abort_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	TEARDOWN();
}


void
test_txn5_11()
{
	GlobalTransactionId gxid;
	int rc;
	
	SETUP();
        sleep(3);
	
	system("killall -9 gtm_standby");
	
	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
        _ASSERT( gxid != InvalidGlobalTransactionId );
        _ASSERT( grep_count(LOG_ACTIVE, "Sending transaction id 3")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "Sending transaction id 3")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=0")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=1")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=2")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "communication error with standby")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "Calling report_xcwatch_gtm_failure()...")==1 );
	
	rc = commit_transaction(conn, gxid);
        _ASSERT( rc>=0 );
        _ASSERT( grep_count(LOG_ACTIVE, "Committing transaction id 3")==1 );
        _ASSERT( grep_count(LOG_STANDBY, "Committing transaction id 3")==0 );
	
	TEARDOWN();
}

void
test_txn5_12()
{
	GlobalTransactionId gxid;
	int rc;
	
	SETUP();
        sleep(3);
	
	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
        _ASSERT( gxid != InvalidGlobalTransactionId );
	
	system("killall -9 gtm_standby");
	
	rc = commit_transaction(conn, gxid);
        _ASSERT( rc>=0 );
        _ASSERT( grep_count(LOG_ACTIVE, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=0")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=1")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "gtm_standby_reconnect_to_standby(): re-connect failed. retry=2")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "communication error with standby")==1 );
        _ASSERT( grep_count(LOG_ACTIVE, "Calling report_xcwatch_gtm_failure()...")==1 );
	
	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test_txn5_01();
	test_txn5_02();
	test_txn5_03();
	test_txn5_04();

	test_txn5_05();
	test_txn5_06();

	/* detecting communication failure, and dropping standby. */
	test_txn5_11();
	test_txn5_12();

	return 0;
}
