/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */

#include <sys/types.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"

#define client_log(x)

int
main(int argc, char *argv[])
{
	int ii;
	int jj;

#define TXN_COUNT		10000
#define LOOP_COUNT		10
	
	GlobalTransactionId gxid[TXN_COUNT];
	GTM_Conn *conn;
	char connect_string[100];

	sprintf(connect_string, "host=localhost port=6666 node_name=one remote_type=%d", GTM_NODE_COORDINATOR);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection\n"));
		exit(1);
	}

	for (jj = 0; jj < LOOP_COUNT; jj++)
	{
		for (ii = 0; ii < TXN_COUNT; ii++)
		{
			int kk;
			GTM_Snapshot snapshot;

			gxid[ii] = begin_transaction(conn, GTM_ISOLATION_RC);
			if (gxid[ii] != InvalidGlobalTransactionId)
				client_log(("Started a new transaction (GXID:%u)\n", gxid[ii]));
			else
				client_log(("BEGIN transaction failed for ii=%d\n", ii));
			snapshot = get_snapshot(conn, gxid[ii], true);


			if (ii % 2 == 0)
			{
				if (!abort_transaction(conn, gxid[ii]))
					client_log(("ROLLBACK successful (GXID:%u)\n", gxid[ii]));
				else
					client_log(("ROLLBACK failed (GXID:%u)\n", gxid[ii]));
			}
			else
			{
				if (!commit_transaction(conn, gxid[ii]))
					client_log(("COMMIT successful (GXID:%u)\n", gxid[ii]));
				else
					client_log(("COMMIT failed (GXID:%u)\n", gxid[ii]));
			}
		}
	}

	GTMPQfinish(conn);
	return 0;
}
