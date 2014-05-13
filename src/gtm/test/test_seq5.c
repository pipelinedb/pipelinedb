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
	connect1();
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test_seq5_01()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	GTM_Sequence increment;
	GTM_Sequence minval;
	GTM_Sequence maxval;
	GTM_Sequence startval;
	bool cycle;

	GTM_Sequence cur;

	SETUP();

	/*
	 * open sequence
	 */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	increment = 1;
	minval    = 0;
	maxval    = 10000;
	startval  = 0;
	cycle     = true;

	rc = open_sequence(conn,
			   &seqkey,
			   increment,
			   minval,
			   maxval,
			   startval,
			   cycle);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Opening sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Opening sequence seq1")==1 );

	/* get current */
	cur = get_current(conn, &seqkey);
	_ASSERT( cur==0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting current value 0 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting current value 0 for sequence seq1")==1 );

	/* get next */
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 1 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 1 for sequence seq1")==1 );

	system("killall -9 gtm");
	system("./promote.sh");
	sleep(1);

	GTMPQfinish(conn);
	connect2();

	/* get current */
	cur = get_current(conn, &seqkey);
	_ASSERT( cur==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting current value 1 for sequence seq1")==1 );

	/* get next */
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==2 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 2 for sequence seq1")==1 );

	/*
	 * close
	 */
	rc = close_sequence(conn, &seqkey);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_STANDBY, "Closing sequence seq1")==1 );

	TEARDOWN();
}


int
main(int argc, char *argv[])
{
	system("./stop.sh");
	system("./clean.sh");
	system("./start.sh");

	test_seq5_01();

	return 0;
}
