/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */

#include <sys/types.h>
#include <unistd.h>

#include "gtm/libpq-fe.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_client.h"

#define client_log(x)	printf x

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
test11()
{
	clean_active();
	clean_standby();

	start_active();
	sleep(10);
	start_standby();
	sleep(10);
	//	stop_active();
	kill_standby();
	sleep(10);
	kill_active();
}

int
main(int argc, char *argv[])
{
	test11();

	return 0;
}

