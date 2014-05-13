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
	int rc;
	char host[1024];

	SETUP();

	node_get_local_addr(conn, host, sizeof(host));

	rc = node_register_internal(conn, GTM_NODE_GTM, host, 6667, "One zero One", "/tmp/pgxc/data/gtm_standby", NODE_DISCONNECTED);
	_ASSERT(rc == 0);

	rc = node_unregister(conn, GTM_NODE_GTM, "One zero one");
	_ASSERT(rc == 0);

	TEARDOWN();
}

void
test02()
{
	int rc;
	char host[1024];

	SETUP();

	node_get_local_addr(conn, host, sizeof(host));

	/*
	 * If a node already registered as a "DISCONNECTED" node,
	 * node_register() would not detect conflict of node information.
	 *
	 * See pgxcnode_add_info() for more details.
	 */
	rc = node_register_internal(conn, GTM_NODE_GTM, host, 6667, "One zero One", "/tmp/pgxc/data/gtm_standby", NODE_CONNECTED);
	_ASSERT(rc == 0);

	rc = node_register_internal(conn, GTM_NODE_GTM, host, 6667, "One zero One", "/tmp/pgxc/data/gtm_standby", NODE_CONNECTED);
	_ASSERT(rc != 0);

	TEARDOWN();
}

void
test03()
{
	int rc;

	SETUP();

	rc = node_unregister(conn, GTM_NODE_GTM, "One zero one");
	_ASSERT( rc==0 );

	TEARDOWN();
}

void
test04()
{
	int rc;

	SETUP();

	rc = node_unregister(conn, GTM_NODE_GTM, "One zero one");
	_ASSERT( rc!=0 );

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test01();
	test02();
	test03();
	test04();

	return 0;
}
