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
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test_standby_01()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	system("killall -9 gtm_standby");

	sprintf(connect_string, "host=localhost port=6666 node_name=one_zero_one remote_type=%d",
		GTM_NODE_GTM);
	
	conn = PQconnectGTM(connect_string);

	_ASSERT( conn!=NULL );
	_ASSERT( grep_count(LOG_ACTIVE, "Failed to establish a connection with GTM standby")==1 );
	_ASSERT( grep_count(LOG_ACTIVE, "Calling report_xcwatch_gtm_failure()...")==1 );

	TEARDOWN();
}

void
test_standby_02()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	connect1();
	_ASSERT( conn!=NULL );

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );
	_ASSERT( grep_count(LOG_ACTIVE, "Sending transaction id 3")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Sending transaction id 3")==1 );

	system("killall -9 gtm_standby");

	rc = commit_transaction(conn, gxid);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Committing transaction id 3")==1 );
	_ASSERT( grep_count(LOG_ACTIVE, "communication error with standby.")==1 );
	_ASSERT( grep_count(LOG_ACTIVE, "Calling report_xcwatch_gtm_failure()...")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Committing transaction id 3")==0 );

	TEARDOWN();
}

void
test_standby_03()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();
	connect1();
	_ASSERT( conn!=NULL );

	system("./status_a.sh > status");
	_ASSERT( grep_count("status", "active: 1")==1 );

	system("./status_s.sh > status");
	_ASSERT( grep_count("status", "active: 0")==1 );

	TEARDOWN();
}

void
test_standby_04()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();
	connect1();
	_ASSERT( conn!=NULL );

	system("./status_a.sh > status");
	_ASSERT( grep_count("status", "active: 1")==1 );
	system("./status_s.sh > status");
	_ASSERT( grep_count("status", "active: 0")==1 );

	system("killall -9 gtm");
	system("./promote.sh");

	GTMPQfinish(conn);
	connect2();
	_ASSERT( conn!=NULL );

	system("./status_s.sh > status");
	_ASSERT( grep_count("status", "active: 1")==1 );

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test_standby_01();
	test_standby_02();

	test_standby_03(); /* get status */

	test_standby_04(); /* promote */

	return 0;
}
