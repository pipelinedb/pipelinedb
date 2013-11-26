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
	int i;

	connect1();

	/* delete old stuffs */
	for (i=0 ; i<10 ; i++)
	{
		GTM_SequenceKeyData seqkey;
		char key[16];

		snprintf(key, sizeof(key), "seq%d", i);
		seqkey.gsk_key    = key;
		seqkey.gsk_keylen = strlen(seqkey.gsk_key);
		seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

		close_sequence(conn, &seqkey);
	}

	/* Create the first one. */
	{
		GTM_SequenceKeyData seqkey;
		GTM_Sequence increment;
		GTM_Sequence minval;
		GTM_Sequence maxval;
		GTM_Sequence startval;
		bool cycle;
		int rc;

		/* create a key */
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
	}
}

void
tearDown()
{
	GTMPQfinish(conn);
}

void
test_seq_01()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	GTM_Sequence increment;
	GTM_Sequence minval;
	GTM_Sequence maxval;
	GTM_Sequence startval;
	bool cycle;

	SETUP();

	/* create a key */
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

	TEARDOWN();
}

void
test_seq_02()
{
	int rc;
	GTM_SequenceKeyData seqkey;

	SETUP();

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	rc = close_sequence(conn, &seqkey);
	
	_ASSERT( rc>=0 );

	TEARDOWN();
}

void
test_seq_03()
{
	GTM_SequenceKeyData seqkey;
	GTM_Sequence cur;

	SETUP();

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	cur = get_current(conn, &seqkey);
	_ASSERT( cur==0 );

	TEARDOWN();
}

void
test_seq_04()
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

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==0 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==2 );

	TEARDOWN();
}

void
test_seq_05()
{
	int rc;
	GTM_SequenceKeyData seqkey;

	GTM_Sequence cur;

	SETUP();

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	rc = set_val(conn, &seqkey, 1000, true);
	_ASSERT( rc>=0 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1001 );
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1002 );

	/*
	 * FIXME: When `iscalled' is false, set_val() does not affect the sequence value.
	 */
	rc = set_val(conn, &seqkey, 1000, false);
	_ASSERT( rc>=0 );

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==0 ); /* FIXME: */
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1 ); /* FIXME: */

	TEARDOWN();
}


void
test_seq_06()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	int i;

	GTM_Sequence cur;

	SETUP();

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	for (i=0 ; i<1000 ; i++)
	{
		cur = get_next(conn, &seqkey);
		_ASSERT( cur==i );
	}
	/* get_next() x 1000 done. */

	rc = reset_sequence(conn, &seqkey);
	_ASSERT( rc>=0 );
	
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==1 );

	TEARDOWN();
}

void
test_seq_07()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	GTM_SequenceKeyData newseqkey;
	int i;

	GTM_Sequence cur;

	SETUP();

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	/* create a key */
	newseqkey.gsk_key    = strdup("seqnew");
	newseqkey.gsk_keylen = strlen(newseqkey.gsk_key);
	newseqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	rc = rename_sequence(conn, &seqkey, &newseqkey);
	_ASSERT( rc>=0 );
	
	cur = get_next(conn, &newseqkey);
	_ASSERT( cur==0 );
	
	cur = get_next(conn, &newseqkey);
	_ASSERT( cur==1 );
	
	rc = close_sequence(conn, &newseqkey);
	_ASSERT( rc>=0 );

	TEARDOWN();
}


void
test_seq_08()
{
	int rc;
	GTM_SequenceKeyData seqkey;
	GTM_SequenceKeyData newseqkey;
	int i;

	GTM_Sequence cur;

	SETUP();

	/* create a key */
	seqkey.gsk_key    = strdup("seq1");
	seqkey.gsk_keylen = strlen(seqkey.gsk_key);
	seqkey.gsk_type   = GTM_SEQ_FULL_NAME;

	cur = get_next(conn, &seqkey);
	_ASSERT( cur==0 );
	
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
	}

	cur = get_current(conn, &seqkey);
	_ASSERT( cur==100 );
	
	cur = get_next(conn, &seqkey);
	_ASSERT( cur==101 );
	
	TEARDOWN();
}


void
test_seq_09()
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
	key1.gsk_type   = GTM_SEQ_FULL_NAME;

	key2.gsk_keylen = strlen("seq2");
	key2.gsk_key    = strdup("seq2");
	key3.gsk_type   = GTM_SEQ_FULL_NAME;
	key3.gsk_keylen = strlen("seq3");
	key3.gsk_key    = strdup("seq3");
	key3.gsk_type   = GTM_SEQ_FULL_NAME;

	/* name, increment, min, max, start, cycle */
	rc = open_sequence(conn, &key2, 1, 1, 10000, 5, true);
	_ASSERT( rc==0 );
	rc = open_sequence(conn, &key3, 1, 1, 10000, 7, true);
	_ASSERT( rc==0 );

	memset(seq_list, 0, sizeof(GTM_SeqInfo *) * 1024);

	rc = get_sequence_list(conn, seq_list, 1024);
	_ASSERT( rc==3 );

	_ASSERT( strncmp(seq_list[0]->gs_key->gsk_key, key1.gsk_key, 4)==0 );
	_ASSERT( strncmp(seq_list[1]->gs_key->gsk_key, key2.gsk_key, 4)==0 );
	_ASSERT( strncmp(seq_list[2]->gs_key->gsk_key, key3.gsk_key, 4)==0 );

	{
	  GTM_Sequence cur;

	  cur = get_next(conn, seq_list[0]->gs_key);
	  fprintf(stderr, "key=%s, cur=%d\n", seq_list[0]->gs_key->gsk_key, cur);
	  _ASSERT( cur==0 );

	  cur = get_next(conn, seq_list[1]->gs_key);
	  fprintf(stderr, "key=%s, cur=%d\n", seq_list[1]->gs_key->gsk_key, cur);
	  _ASSERT( cur==5 );

	  cur = get_next(conn, seq_list[2]->gs_key);
	  fprintf(stderr, "key=%s, cur=%d\n", seq_list[2]->gs_key->gsk_key, cur);
	  _ASSERT( cur==7 );
	}

	TEARDOWN();
}

int
main(int argc, char *argv[])
{
	test_seq_01();
	test_seq_02();

	test_seq_03();
	test_seq_04();

	test_seq_05(); /* set_val */
	test_seq_06(); /* reset_sequence */

	test_seq_07(); /* rename_sequence */

	test_seq_08(); /* alter_sequence */

	test_seq_09(); /* get_sequence_list */

	return 0;
}
