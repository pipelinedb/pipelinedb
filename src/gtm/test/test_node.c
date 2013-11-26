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
test_node_01()
{
	int rc;

	SETUP();

	rc = node_register(conn, GTM_NODE_DATANODE,  6666, "one", "/tmp/pgxc/data/gtm");
	_ASSERT( rc >= 0 );

	TEARDOWN();
}

void
test_node_02()
{
	int rc;

	SETUP();

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One");
	_ASSERT( rc >= 0 );

	TEARDOWN();
}

void
test_node_03()
{
	GTM_PGXCNodeInfo *data;
	int rc, i;

	SETUP();

	data = (GTM_PGXCNodeInfo *)malloc( sizeof(GTM_PGXCNodeInfo)*128 );
	memset(data, 0, sizeof(GTM_PGXCNodeInfo)*128);

	rc = get_node_list(conn, data, 128);
	_ASSERT( rc>=0 );

	for (i=0 ; i<rc ; i++)
	{
		print_nodeinfo(data[i]);
	}

	free(data);

	TEARDOWN();
}

void
test_node_04()
{
	GTM_PGXCNodeInfo *data;
	int rc, i;

	SETUP();

	data = (GTM_PGXCNodeInfo *)malloc( sizeof(GTM_PGXCNodeInfo)*128 );
	memset(data, 0, sizeof(GTM_PGXCNodeInfo)*128);

	rc = node_register(conn, GTM_NODE_DATANODE, 6666, "one", "/tmp/pgxc/data/gtm");
	_ASSERT( rc>=0 );

	rc = get_node_list(conn, data, 128);
	_ASSERT( rc>=0 );

	for (i=0 ; i<rc ; i++)
	{
		print_nodeinfo(data[i]);
	}

	free(data);

	TEARDOWN();
}


void
test_node_05()
{
	int rc, i;

	SETUP();

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One zero one");

	rc = node_register(conn, GTM_NODE_DATANODE, 6666, "One zero one", "/tmp/pgxc/data/gtm");
	_ASSERT( rc>=0 );

	sleep(5);

	rc = backend_disconnect(conn, true, GTM_NODE_DATANODE, "One Zero one");
	_ASSERT( rc>=0 );

	rc = node_unregister(conn, GTM_NODE_DATANODE, "One zero one");
	_ASSERT( rc>=0 );

	TEARDOWN();
}


int
main(int argc, char *argv[])
{
	test_node_01();
	test_node_02();

	test_node_03();
	test_node_04();

	test_node_05();

	return 0;
}
