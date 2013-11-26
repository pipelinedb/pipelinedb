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

void
setUp()
{
}

void
tearDown()
{
}

void
test01()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_GTM_PROXY);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test02()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_GTM_PROXY_POSTMASTER);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test03()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_COORDINATOR);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test04()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
	        GTM_NODE_DATANODE);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test05()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		GTM_NODE_GTM);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test06()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
	        GTM_NODE_DEFAULT);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test07()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d",
		12);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}
	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

void
test08()
{
	GTM_Conn *conn;
	char connect_string[100];

	SETUP();

	sprintf(connect_string, "host=localhost port=6668 node_name=one remote_type=%d",
		12);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}

	printf("conn = %p\n", conn);

	client_log(("PGconnectGTM() ok.\n"));

	GTMPQfinish(conn);

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test01();
	test02();
	test03();
	test04();
	test05();
	test06();
	test07(); /* must fail */
	test08(); /* must fail */

	return 0;
}
