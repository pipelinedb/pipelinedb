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
test_seq4_01()
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

	/*
	 * get current
	 */
	cur = get_current(conn, &seqkey);
	_ASSERT( cur==0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting current value 0 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting current value 0 for sequence seq1")==1 );

	cur = get_current(conn, &seqkey);
	_ASSERT( cur==0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting current value 0 for sequence seq1")==2 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting current value 0 for sequence seq1")==2 );

	/*
	 * get next
	 */
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 1 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 1 for sequence seq1")==1 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==2 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 2 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 2 for sequence seq1")==1 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==3 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 3 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 3 for sequence seq1")==1 );

	/*
	 * set value
	 */
	rc = set_val(conn, &seqkey, 13, true);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Setting new value 13 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Setting new value 13 for sequence seq1")==1 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==14 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 14 for sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 14 for sequence seq1")==1 );

#ifdef NOT_USED /* FIXME: snaga
	/*
	 * FIXME: When `iscalled' is false, set_val() does not affect the sequence value.
	 */
	rc = set_val(conn, &seqkey, 1000, false);
#endif

	/*
	 * reset
	 */
	rc = reset_sequence(conn, &seqkey);
	_ASSERT( cur>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Resetting sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Resetting sequence seq1")==1 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 1 for sequence seq1")==2 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 1 for sequence seq1")==2 );

	/*
	 * close
	 */
	rc = close_sequence(conn, &seqkey);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Closing sequence seq1")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Closing sequence seq1")==1 );

	TEARDOWN();
}


void
test_seq4_02()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	GTM_SequenceKeyData newseqkey;
	GTM_Sequence increment;
	GTM_Sequence minval;
	GTM_Sequence maxval;
	GTM_Sequence startval;
	bool cycle;

	SETUP();

	/*
	 * create a sequence
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

	/* create a new key */
	newseqkey.gsk_key    = strdup("seqnew");
	newseqkey.gsk_keylen = strlen(newseqkey.gsk_key);
	newseqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	/*
	 * rename key
	 */
	rc = rename_sequence(conn, &seqkey, &newseqkey);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Renaming sequence seq1 to seqnew")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Renaming sequence seq1 to seqnew")==1 );
	
	rc = close_sequence(conn, &newseqkey);
	_ASSERT( rc>=0 );
	_ASSERT( grep_count(LOG_ACTIVE, "Closing sequence seqnew")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Closing sequence seqnew")==1 );

	TEARDOWN();
}


void
test_seq4_03()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	GTM_SequenceKeyData newseqkey;
	GTM_Sequence increment;
	GTM_Sequence minval;
	GTM_Sequence maxval;
	GTM_Sequence startval;
	bool cycle;

	GTM_Sequence cur;

	SETUP();

	/*
	 * create a sequence
	 */
	seqkey.gsk_key    = strdup("seq2");
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

	{
		GTM_Sequence increment;
		GTM_Sequence minval;
		GTM_Sequence maxval;
		GTM_Sequence startval;
		bool cycle;

		increment = 1;
		minval    = 100;
		maxval    = 10000;
		startval  = 100;
		cycle     = true;

		rc = alter_sequence(conn,
				    &seqkey,
				    increment,
				    minval,
				    maxval,
				    startval,
				    256, /* lastval */
				    cycle,
				    true);

		_ASSERT( rc>=0 );
		_ASSERT( grep_count(LOG_ACTIVE, "Altering sequence key seq2")==1 );
		_ASSERT( grep_count(LOG_STANDBY, "Altering sequence key seq2")==1 );
	}

	cur = get_current(conn, &seqkey);
	_ASSERT( cur==100 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting current value 100 for sequence seq2")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting current value 100 for sequence seq2")==1 );
	
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==101 );
	_ASSERT( grep_count(LOG_ACTIVE, "Getting next value 101 for sequence seq2")==1 );
	_ASSERT( grep_count(LOG_STANDBY, "Getting next value 101 for sequence seq2")==1 );

	TEARDOWN();
}


int
main(int argc, char *argv[])
{
	system("./stop.sh");
	system("./clean.sh");
	system("./start.sh");

	test_seq4_01();
	test_seq4_02(); /* rename */
	test_seq4_03(); /* alter */

	return 0;
}
