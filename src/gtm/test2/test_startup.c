/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */

#include <sys/types.h>
#include <unistd.h>

#include "gtm/libpq-fe.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"

#include "test_common.h"

#define client_log(x)	printf x

GlobalTransactionId gxid;

void
setUp()
{
}

void
tearDown()
{
}

void
test_startup_01()
{
	GTM_Conn *conn;
	char connect_string[100];
	int rc;

	SETUP();

	system("./start_a.sh");
	sleep(1);

	sprintf(connect_string, "host=localhost port=6666 node_name=one_zero_one remote_type=%d",
		GTM_NODE_DEFAULT);

	conn = PQconnectGTM(connect_string);
	_ASSERT(conn != NULL);

	/*
	 * create a transaction data on active GTM
	 */
	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );
	_ASSERT( grep_count(LOG_ACTIVE, "Sending transaction id 3")==1 );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Preparing transaction id 3")==1 );

	fprintf(stderr, "gxid=%d\n", gxid);

	GTMPQfinish(conn);

	/*
	 * start standby
	 */
	system("./start_s.sh");
	sleep(1);

	_ASSERT( grep_count(LOG_STANDBY, "Restoring next/last gxid from the active-GTM succeeded.")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Restoring 1 gxid(s) done.")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Restoring all of gxid(s) from the active-GTM succeeded.")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Restoring sequences done.")==1 );

	/*
	 * connecting to the standby
	 */
	sprintf(connect_string, "host=localhost port=6667 node_name=one_zero_two remote_type=%d",
		GTM_NODE_DEFAULT);

	conn = PQconnectGTM(connect_string);
	_ASSERT(conn != NULL);

	/*
	 * abort the replicated transaction
	 */
	rc = abort_transaction(conn, gxid);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Cancelling transaction id 3")==0 );
	_ASSERT( grep_count(LOG_STANDBY, "Cancelling transaction id 3")==1 );

	GTMPQfinish(conn);

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	system("./stop.sh");
	system("./clean.sh");

	test_startup_01();

	return 0;
}
