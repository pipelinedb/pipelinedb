/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */

#include <sys/types.h>
#include <unistd.h>

#include "gtm/libpq-fe.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"
#include "gtm/register.h"

#include "test_common.h"

pthread_key_t     threadinfo_key;

void
setUp()
{
	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_GTM);
	
	conn = PQconnectGTM(connect_string);

	_ASSERT( conn!=NULL );
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test01()
{
	GlobalTransactionId gxid;
	int rc;
	char host[1024];

	printf("\n=== test01:node_register ===\n");

	setUp();

	node_get_local_addr(conn, host, sizeof(host));

	/*
	 * starting
	 */
	rc = node_register_internal(conn, GTM_NODE_GTM, host, 6667, "One zero two", "/tmp/pgxc/data/gtm_standby", NODE_DISCONNECTED);
	_ASSERT(rc == 0);
	rc = node_unregister(conn, GTM_NODE_GTM, "One zero two");
	_ASSERT(rc == 0);

	rc = node_register_internal(conn, GTM_NODE_GTM, host, 6667, "One zero two", "/tmp/pgxc/data/gtm_standby", NODE_CONNECTED);
	_ASSERT(rc == 0);

	sleep(10);

	gxid = begin_transaction(conn, GTM_ISOLATION_SERIALIZABLE, timestamp);
	_ASSERT( gxid!=InvalidGlobalTransactionId );

	rc = prepare_transaction(conn, gxid);
	_ASSERT( rc!=-1 );

	rc = abort_transaction(conn, gxid);
	_ASSERT( rc!=-1 );

	sleep(10);

	/*
	 * closing
	 */
	rc = node_unregister(conn, GTM_NODE_GTM, "One zero two");
	_ASSERT( rc==0 );

	tearDown();
}

int
main(int argc, char *argv[])
{
	test01();

	return 0;
}
