/*
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 */
#include <sys/types.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"

#define client_log(x)	printf x

int
main(int argc, char *argv[])
{
	int ii;
	pid_t parent_pid;
	GTM_Conn *conn = NULL;
	char connect_string[100];

	//FIXME This statement is wrong
	sprintf(connect_string, "host=%s port=%d node_name=one remote_type=%d", GTM_NODE_COORDINATOR);

	conn = PQconnectGTM(connect_string);
	if (conn == NULL)
	{
		client_log(("Error in connection"));
		exit(1);
	}

	parent_pid = getpid();

	/*
	 * Create sequences
	 */
	for (ii = 0; ii < 20; ii++)
	{
		char buf[100];
		GTM_SequenceKeyData seqkey;
		sprintf(buf, "%d:%d", ii, ii);
		seqkey.gsk_keylen = strlen(buf);
		seqkey.gsk_key = buf;
		if (open_sequence(conn, &seqkey, 10, 1, 10000, 100, false))
		   client_log(("Open seq failed\n"));
		else
			client_log(("Opened Sequence %s\n", seqkey.gsk_key));
	}

	/*
	 * Close the GTM connection
	 */
	GTMPQfinish(conn);

	/*
	 * Start few process which would independently use the sequences
	 */
	for (ii = 0; ii < 3; ii++)
		fork();

	/*
	 * Each process now opens a new connection with the GTM
	 */
	conn = PQconnectGTM(connect_string);

	/*
	 * Try to read/increment the sequence
	 */
	for (ii = 0; ii < 20; ii++)
	{
		char buf[100];
		GTM_SequenceKeyData seqkey;
		GTM_Sequence seqval;
		int jj;

		sprintf(buf, "%d:%d", ii, ii);
		seqkey.gsk_keylen = strlen(buf);
		seqkey.gsk_key = buf;
		if ((seqval = get_current(conn, &seqkey)) == InvalidSequenceValue)
			client_log(("get_current seq failed for sequene %s\n", seqkey.gsk_key));
		else
			client_log(("CURRENT SEQVAL(%s): %lld\n", seqkey.gsk_key, seqval));
		
		for (jj = 0; jj < 5; jj++)
		{
			if ((seqval = get_next(conn, &seqkey)) == InvalidSequenceValue)
				client_log(("get_current seq failed for sequence %s\n", seqkey.gsk_key));
			else
				client_log(("NEXT SEQVAL(%s): %lld ", seqkey.gsk_key, seqval));
		}
		client_log(("\n"));
	}

	/*
	 * The main process now closes the sequences. We want to call close only
	 * once, hence this approach
	 */
	if (getpid() == parent_pid)
	{
		/*
		 * Wait long enough so that all other processes are done
		 */
		sleep(20);
		for (ii = 0; ii < 20; ii++)
		{
			char buf[100];
			GTM_SequenceKeyData seqkey;
			sprintf(buf, "%d:%d", ii, ii);
			seqkey.gsk_keylen = strlen(buf);
			seqkey.gsk_key = buf;
			if (close_sequence(conn, &seqkey))
				client_log(("Close seq failed for sequence %s\n", seqkey.gsk_key));
			else
				client_log(("Sequene closed %s\n", seqkey.gsk_key));
		}
	}
	GTMPQfinish(conn);
	return 0;
}
