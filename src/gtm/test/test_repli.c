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

	client_log(("PGconnectGTM() ok.\n"));
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

	SETUP();

	rc = begin_replication_initial_sync(conn);
	if ( rc>=0 )
		client_log(("begin_replication_initial_sync() ok.\n"));
	else
		client_log(("begin_replication_initial_sync() failed.\n"));

	rc = end_replication_initial_sync(conn);
	if ( rc>=0 )
		client_log(("end_replication_initial_sync() ok.\n"));
	else
		client_log(("end_replication_initial_sync() failed.\n"));

	TEARDOWN();
}

void
test04()
{
	GTM_PGXCNodeInfo *data;
	int rc, i;
	GTM_Transactions txn;
	GlobalTransactionId tmp;
	GTM_Timestamp ts;

	//	data = (GTM_PGXCNodeInfo *)malloc( sizeof(GTM_PGXCNodeInfo)*128 );
	//	memset(data, 0, sizeof(GTM_PGXCNodeInfo)*128);

	SETUP();

	tmp = begin_transaction(conn, GTM_ISOLATION_RC, &ts);

	rc = get_txn_gxid_list(conn, &txn);
	if ( rc>=0 )
		client_log(("get_txn_gxid_list() ok.\n"));
	else
		client_log(("get_txn_gxid_list() failed.\n"));

	abort_transaction(conn, tmp);

	TEARDOWN();
}

void
test05()
{
	GlobalTransactionId next_gxid;
	int rc, i;

	SETUP();

	next_gxid = get_next_gxid(conn);
	if ( next_gxid!=InvalidGlobalTransactionId )
		client_log(("get_next_gxid() ok. - %ld\n", next_gxid));
	else
		client_log(("get_txn_gxid_list() failed.\n"));

	TEARDOWN();
}

void
test08()
{
	GlobalTransactionId next_gxid;
	GTM_SequenceKeyData key1;
	GTM_SequenceKeyData key2;
	GTM_SequenceKeyData key3;
	int rc, i;
	GTM_SeqInfo *seq_list[1024];

	SETUP();

	key1.gsk_keylen = strlen("seq1");
	key1.gsk_key    = strdup("seq1");
	key2.gsk_keylen = strlen("seq2");
	key2.gsk_key    = strdup("seq2");
	key3.gsk_keylen = strlen("seq3");
	key3.gsk_key    = strdup("seq3");

	close_sequence(conn, &key1);
	close_sequence(conn, &key2);
	close_sequence(conn, &key3);

	for (i=0 ; i<1024 ; i++)
		seq_list[i] = NULL;

	rc = get_sequence_list(conn, seq_list, 1024);
	if ( rc>=0 )
	{
		client_log(("get_seq_list() ok. - %d\n", rc));

		for (i=0 ; i<rc ; i++)
		{
			client_log(("key = %s\n", seq_list[i]->gs_key->gsk_key));
		}
	}
	else
		client_log(("get_seq_list() failed.\n"));

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test01();
	test04();
	/*
	test05();
	test08();
	*/

	return 0;
}
