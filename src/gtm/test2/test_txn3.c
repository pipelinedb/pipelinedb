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
test_txn3_01()
{
	GlobalTransactionId gxid;
	int rc;

	SETUP();

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid != InvalidGlobalTransactionId );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc>=0 );

	sleep(2);
	system("killall -9 gtm_standby");
	sleep(1);

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
