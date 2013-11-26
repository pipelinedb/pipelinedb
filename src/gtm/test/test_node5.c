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
	system("./stop.sh");
	system("./clean.sh");
	system("./start.sh");
	sleep(1);
}

void
tearDown()
{
}

void
test_node5_01()
{
	int rc;

	SETUP();

	/*
	 * active
	 */
	connect1();

	rc = node_register(conn, GTM_NODE_DATANODE,  16666, "One zero zero one", "/tmp/pgxc/data/gtm");
	_ASSERT( rc >= 0 );

	GTMPQfinish(conn);
	sleep(3);

	/*
	 * standby
	 */
	connect2();

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One zero zero one");
	_ASSERT( rc >= 0 );

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test_node5_02()
{
	int rc;

	SETUP();

	/*
	 * active
	 */
	connect1();

	rc = node_register(conn, GTM_NODE_DATANODE,  16666, "One zero zero one", "/tmp/pgxc/data/gtm");
	_ASSERT( rc >= 0 );

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One zero zero one");
	_ASSERT( rc >= 0 );

	GTMPQfinish(conn);
	sleep(3);

	/*
	 * standby
	 */
	connect2();

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One zero zero one");
	_ASSERT( rc<0 );

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test_node5_03()
{
	int rc;

	SETUP();

	/*
	 * active
	 */
	connect1();

	rc = node_register(conn, GTM_NODE_DATANODE,  16666, "One zero zero one", "/tmp/pgxc/data/gtm");
	_ASSERT( rc >= 0 );

	system("killall -9 gtm");
	system("./promote.sh");
	sleep(1);

	GTMPQfinish(conn);
	connect2();

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One zero zero one");
	_ASSERT( rc >= 0 );

	GTMPQfinish(conn);

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test_node5_01();
	test_node5_02();

	test_node5_03();

	return 0;
}
