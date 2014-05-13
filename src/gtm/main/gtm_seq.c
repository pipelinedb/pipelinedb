/*-------------------------------------------------------------------------
 *
 * gtm_seq.c
 *	Sequence handling on GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>

#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/standby_utils.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_backup.h"

extern bool Backup_synchronously;

typedef struct GTM_SeqInfoHashBucket
{
	gtm_List   *shb_list;
	GTM_RWLock	shb_lock;
} GTM_SeqInfoHashBucket;

#define SEQ_HASH_TABLE_SIZE		1024
static GTM_SeqInfoHashBucket GTMSequences[SEQ_HASH_TABLE_SIZE];

static uint32 seq_gethash(GTM_SequenceKey key);
static bool seq_keys_equal(GTM_SequenceKey key1, GTM_SequenceKey key2);
static bool seq_key_dbname_equal(GTM_SequenceKey nsp, GTM_SequenceKey seq);
static GTM_SeqInfo *seq_find_seqinfo(GTM_SequenceKey seqkey);
static int seq_release_seqinfo(GTM_SeqInfo *seqinfo);
static int seq_add_seqinfo(GTM_SeqInfo *seqinfo);
static int seq_remove_seqinfo(GTM_SeqInfo *seqinfo);
static GTM_SequenceKey seq_copy_key(GTM_SequenceKey key);
static int seq_drop_with_dbkey(GTM_SequenceKey nsp);
static bool GTM_NeedSeqRestoreUpdateInternal(GTM_SeqInfo *seqinfo);


/*
 * Get the hash value given the sequence key
 *
 * XXX This should probably be replaced by a better hash function.
 */
static uint32
seq_gethash(GTM_SequenceKey key)
{
	uint32 total = 0;
	int ii;

	for (ii = 0; ii < key->gsk_keylen; ii++)
		total += key->gsk_key[ii];
	return (total % SEQ_HASH_TABLE_SIZE);
}

/*
 * Return true if both keys are equal, else return false
 */
static bool
seq_keys_equal(GTM_SequenceKey key1, GTM_SequenceKey key2)
{
	Assert(key1);
	Assert(key2);

	if (key1->gsk_keylen != key2->gsk_keylen) return false;

	return (memcmp(key1->gsk_key, key2->gsk_key,
				  Min(key1->gsk_keylen, key2->gsk_keylen)) == 0);
}

/*
 * Find the seqinfo structure for the given key. The reference count is
 * incremented before structure is returned. The caller must release the
 * reference to the structure when done with it
 */
static GTM_SeqInfo *
seq_find_seqinfo(GTM_SequenceKey seqkey)
{
	uint32 hash = seq_gethash(seqkey);
	GTM_SeqInfoHashBucket *bucket;
	gtm_ListCell *elem;
	GTM_SeqInfo *curr_seqinfo = NULL;

	bucket = &GTMSequences[hash];

	GTM_RWLockAcquire(&bucket->shb_lock, GTM_LOCKMODE_READ);

	gtm_foreach(elem, bucket->shb_list)
	{
		curr_seqinfo = (GTM_SeqInfo *) gtm_lfirst(elem);
		if (seq_keys_equal(curr_seqinfo->gs_key, seqkey))
			break;
		curr_seqinfo = NULL;
	}

	if (curr_seqinfo != NULL)
	{
		GTM_RWLockAcquire(&curr_seqinfo->gs_lock, GTM_LOCKMODE_WRITE);
		if (curr_seqinfo->gs_state != SEQ_STATE_ACTIVE)
		{
			elog(LOG, "Sequence not active");
			GTM_RWLockRelease(&curr_seqinfo->gs_lock);
			return NULL;
		}
		Assert(curr_seqinfo->gs_ref_count != SEQ_MAX_REFCOUNT);
		curr_seqinfo->gs_ref_count++;
		GTM_RWLockRelease(&curr_seqinfo->gs_lock);
	}
	GTM_RWLockRelease(&bucket->shb_lock);

	return curr_seqinfo;
}

/*
 * Release previously grabbed reference to the structure. If the structure is
 * marked for deletion, it will be removed from the global array and released
 */
static int
seq_release_seqinfo(GTM_SeqInfo *seqinfo)
{
	bool remove = false;

	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);
	Assert(seqinfo->gs_ref_count > 0);
	seqinfo->gs_ref_count--;

	if ((seqinfo->gs_state == SEQ_STATE_DELETED) &&
		(seqinfo->gs_ref_count == 0))
		remove = true;

	GTM_RWLockRelease(&seqinfo->gs_lock);
	/*
	 * Remove the structure from the global hash table
	 */
	if (remove) seq_remove_seqinfo(seqinfo);
	return 0;
}

/*
 * Add a seqinfo structure to the global hash table.
 */
static int
seq_add_seqinfo(GTM_SeqInfo *seqinfo)
{
	uint32 hash = seq_gethash(seqinfo->gs_key);
	GTM_SeqInfoHashBucket	*bucket;
	gtm_ListCell *elem;

	bucket = &GTMSequences[hash];

	GTM_RWLockAcquire(&bucket->shb_lock, GTM_LOCKMODE_WRITE);

	gtm_foreach(elem, bucket->shb_list)
	{
		GTM_SeqInfo *curr_seqinfo = NULL;
		curr_seqinfo = (GTM_SeqInfo *) gtm_lfirst(elem);

		if (seq_keys_equal(curr_seqinfo->gs_key, seqinfo->gs_key))
		{
			GTM_RWLockRelease(&bucket->shb_lock);
			ereport(LOG,
					(EEXIST,
					 errmsg("Sequence with the given key already exists")));
			return EEXIST;
		}
	}

	/*
	 * Safe to add the structure to the list
	 */
	bucket->shb_list = gtm_lappend(bucket->shb_list, seqinfo);
	GTM_RWLockRelease(&bucket->shb_lock);

	return 0;
}

/*
 * Remove the seqinfo structure from the global hash table. If the structure is
 * currently referenced by some other thread, just mark the structure for
 * deletion and it will be deleted by the final reference is released.
 */
static int
seq_remove_seqinfo(GTM_SeqInfo *seqinfo)
{
	uint32 hash = seq_gethash(seqinfo->gs_key);
	GTM_SeqInfoHashBucket	*bucket;

	bucket = &GTMSequences[hash];

	GTM_RWLockAcquire(&bucket->shb_lock, GTM_LOCKMODE_WRITE);
	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);

	if (seqinfo->gs_ref_count > 1)
	{
		seqinfo->gs_state = SEQ_STATE_DELETED;
		GTM_RWLockRelease(&seqinfo->gs_lock);
		GTM_RWLockRelease(&bucket->shb_lock);
		return EBUSY;
	}

	bucket->shb_list = gtm_list_delete(bucket->shb_list, seqinfo);
	GTM_RWLockRelease(&seqinfo->gs_lock);
	GTM_RWLockRelease(&bucket->shb_lock);

	return 0;
}

static GTM_SequenceKey
seq_copy_key(GTM_SequenceKey key)
{
	GTM_SequenceKey retkey = NULL;

	/*
	 * We must use the TopMostMemoryContext because the sequence information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	retkey = (GTM_SequenceKey) MemoryContextAlloc(TopMostMemoryContext,
												  sizeof(GTM_SequenceKeyData) +
														key->gsk_keylen);

	if (retkey == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	retkey->gsk_keylen = key->gsk_keylen;
	retkey->gsk_key = (char *)((char *)retkey + sizeof (GTM_SequenceKeyData));

	memcpy(retkey->gsk_key, key->gsk_key, key->gsk_keylen);
	return retkey;
}

/*
 * Initialize a new sequence. Optionally set the initial value of the sequence.
 */
int
GTM_SeqOpen(GTM_SequenceKey seqkey,
			GTM_Sequence increment_by,
			GTM_Sequence minval,
			GTM_Sequence maxval,
			GTM_Sequence startval,
			bool cycle)
{
	GTM_SeqInfo *seqinfo = NULL;
	int errcode = 0;
	seqinfo = (GTM_SeqInfo *) palloc(sizeof (GTM_SeqInfo));

	if (seqinfo == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	GTM_RWLockInit(&seqinfo->gs_lock);

	seqinfo->gs_ref_count = 0;
	seqinfo->gs_key = seq_copy_key(seqkey);
	seqinfo->gs_state = SEQ_STATE_ACTIVE;
	seqinfo->gs_called = false;

	/*
	 * Set the increment. Default is 1
	 */
	if (SEQVAL_IS_VALID(increment_by))
		seqinfo->gs_increment_by = increment_by;
	else
		seqinfo->gs_increment_by = 1;

	/*
	 * If minval is specified, set the minvalue to the given minval,
	 * otherwise set to the defaults
	 */
	if (SEQVAL_IS_VALID(minval))
		seqinfo->gs_min_value = minval;
	else if (SEQ_IS_ASCENDING(seqinfo))
		seqinfo->gs_min_value = SEQ_DEF_MIN_SEQVAL_ASCEND;
	else
		seqinfo->gs_min_value = SEQ_DEF_MIN_SEQVAL_DESCEND;

	/*
	 * If maxval is specfied, set the maxvalue to the given maxval, otherwise
	 * set to the defaults depending on whether the seqeunce is ascending or
	 * descending. Also do some basic contraint checks
	 */
	if (SEQVAL_IS_VALID(maxval))
	{
		if (maxval < seqinfo->gs_min_value)
			ereport(ERROR,
					(ERANGE,
					 errmsg("Max value must be greater than min value")));
		seqinfo->gs_max_value = maxval;
	}
	else if (SEQ_IS_ASCENDING(seqinfo))
		seqinfo->gs_max_value = SEQ_DEF_MAX_SEQVAL_ASCEND;
	else
		seqinfo->gs_max_value = SEQ_DEF_MAX_SEQVAL_DESCEND;


	/*
	 * Set the startval if specified. Do some basic checks like startval must
	 * be in-between min and max values
	 */
	if (SEQVAL_IS_VALID(startval))
	{
		if (startval < seqinfo->gs_min_value)
			ereport(ERROR,
					(ERANGE,
					 errmsg("Start value must be greater than or equal to the min value")));

		if (startval > seqinfo->gs_max_value)
			ereport(ERROR,
					(ERANGE,
					 errmsg("Start value must be less than or equal to the max value")));

		seqinfo->gs_init_value = seqinfo->gs_value = startval;
	}
	else if (SEQ_IS_ASCENDING(seqinfo))
		seqinfo->gs_init_value = seqinfo->gs_value = SEQ_DEF_MIN_SEQVAL_ASCEND;
	else
		seqinfo->gs_init_value = seqinfo->gs_value = SEQ_DEF_MIN_SEQVAL_DESCEND;

	/*
	 * Should we wrap around ?
	 */
	seqinfo->gs_cycle = cycle;

	/* Set the last value in case of a future restart */
	seqinfo->gs_last_value = seqinfo->gs_init_value;

	seqinfo->gs_backedUpValue = seqinfo->gs_value;

	if ((errcode = seq_add_seqinfo(seqinfo)))
	{
		GTM_RWLockDestroy(&seqinfo->gs_lock);
		pfree(seqinfo->gs_key);
		pfree(seqinfo);
	}
	GTM_SetNeedBackup();

	return errcode;
}

/*
 * Alter a sequence
 */
int GTM_SeqAlter(GTM_SequenceKey seqkey,
				 GTM_Sequence increment_by,
				 GTM_Sequence minval,
				 GTM_Sequence maxval,
				 GTM_Sequence startval,
				 GTM_Sequence lastval,
				 bool cycle,
				 bool is_restart)
{
	GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);

	if (seqinfo == NULL)
	{
		ereport(LOG,
				(EINVAL,
				 errmsg("The sequence with the given key does not exist")));
		return EINVAL;
	}

	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);

	/* Modify the data if necessary */
	if (seqinfo->gs_cycle != cycle)
		seqinfo->gs_cycle = cycle;
	if (seqinfo->gs_min_value != minval)
		seqinfo->gs_min_value = minval;
	if (seqinfo->gs_max_value != maxval)
		seqinfo->gs_max_value = maxval;
	if (seqinfo->gs_increment_by != increment_by)
		seqinfo->gs_increment_by = increment_by;

	/*
	 * Check start/restart processes.
	 * Check first if restart is necessary and reset sequence in that case.
	 * If not, check if a simple start is necessary and update sequence.
	 */
	if (is_restart)
	{
		/* Restart command has been used, reset the sequence */
		seqinfo->gs_called = false;
		seqinfo->gs_init_value = seqinfo->gs_last_value = lastval;
	}
	else
	{
		/* Start has been used, reinitialize init value */
		if (seqinfo->gs_init_value != startval)
			seqinfo->gs_init_value = seqinfo->gs_last_value = startval;
	}

	/* Remove the old key with the old name */
	GTM_RWLockRelease(&seqinfo->gs_lock);
	seq_release_seqinfo(seqinfo);
	return 0;
}

/*
 * Restore a sequence.
 */
int
GTM_SeqRestore(GTM_SequenceKey seqkey,
			   GTM_Sequence increment_by,
			   GTM_Sequence minval,
			   GTM_Sequence maxval,
			   GTM_Sequence startval,
			   GTM_Sequence curval,
			   int32 state,
			   bool cycle,
			   bool called)
{
	GTM_SeqInfo *seqinfo = NULL;
	int errcode = 0;
	seqinfo = (GTM_SeqInfo *) palloc(sizeof (GTM_SeqInfo));

	if (seqinfo == NULL)
		ereport(ERROR, (ENOMEM, errmsg("Out of memory")));

	GTM_RWLockInit(&seqinfo->gs_lock);

	seqinfo->gs_ref_count = 0;
	seqinfo->gs_key = seq_copy_key(seqkey);
	seqinfo->gs_state = state;
	seqinfo->gs_called = called;

	seqinfo->gs_increment_by = increment_by;
	seqinfo->gs_min_value = minval;
	seqinfo->gs_max_value = maxval;

	seqinfo->gs_init_value = seqinfo->gs_last_value = startval;
	seqinfo->gs_value = curval;
	seqinfo->gs_backedUpValue = seqinfo->gs_value;

	/*
	 * Should we wrap around ?
	 */
	seqinfo->gs_cycle = cycle;

	if ((errcode = seq_add_seqinfo(seqinfo)))
	{
	 	GTM_RWLockDestroy(&seqinfo->gs_lock);
		pfree(seqinfo->gs_key);
		pfree(seqinfo);
	}
	return errcode;
}

/*
 * Destroy the given sequence depending on type of given key
 */
int
GTM_SeqClose(GTM_SequenceKey seqkey)
{
	int res;

	switch(seqkey->gsk_type)
	{
		case GTM_SEQ_FULL_NAME:
		{
			GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);
			if (seqinfo != NULL)
			{
				seq_remove_seqinfo(seqinfo);
				pfree(seqinfo->gs_key);
				pfree(seqinfo);
				res = 0;
			}
			else
				res = EINVAL;

			break;
		}
		case GTM_SEQ_DB_NAME:
			res = seq_drop_with_dbkey(seqkey);
			break;

		default:
			res = EINVAL;
			break;
	}

	return res;
}

/* Check if sequence key contains only Database name */
static bool
seq_key_dbname_equal(GTM_SequenceKey nsp, GTM_SequenceKey seq)
{
	Assert(nsp);
	Assert(seq);

	/*
	 * Sequence key of GTM_SEQ_DB_NAME type has to be shorter
	 * than given sequence key.
	 */
	if(nsp->gsk_keylen >= seq->gsk_keylen)
		return false;

	/*
	 * Check also if first part of sequence key name has a dot at the right place.
	 * This accelerates process instead of making numerous memcmp.
	 */
	if (seq->gsk_key[nsp->gsk_keylen - 1] != '.')
		return false;

	/* Then Check the strings */
	return (memcmp(nsp->gsk_key, seq->gsk_key, nsp->gsk_keylen - 1) == 0);
}

/*
 * Remove all sequences with given key depending on its type.
 */
static int
seq_drop_with_dbkey(GTM_SequenceKey nsp)
{
	int ii = 0;
	GTM_SeqInfoHashBucket *bucket;
	gtm_ListCell *cell, *prev;
	GTM_SeqInfo *curr_seqinfo = NULL;
	int res = 0;
	bool deleted;

	for(ii = 0; ii < SEQ_HASH_TABLE_SIZE; ii++)
	{
		bucket = &GTMSequences[ii];

		GTM_RWLockAcquire(&bucket->shb_lock, GTM_LOCKMODE_READ);

		prev = NULL;
		cell = gtm_list_head(bucket->shb_list);
		while (cell != NULL)
		{
			curr_seqinfo = (GTM_SeqInfo *) gtm_lfirst(cell);
			deleted = false;

			if (seq_key_dbname_equal(nsp, curr_seqinfo->gs_key))
			{
				GTM_RWLockAcquire(&curr_seqinfo->gs_lock, GTM_LOCKMODE_WRITE);

				if (curr_seqinfo->gs_ref_count > 1)
				{
					curr_seqinfo->gs_state = SEQ_STATE_DELETED;

					/* can not happen, be checked before called */
					elog(LOG,"Sequence %s is in use, mark for deletion only",
							 curr_seqinfo->gs_key->gsk_key);

					/*
					 * Continue to delete other sequences linked to this dbname,
					 * sequences in use are deleted later.
					 */
					res = EBUSY;
				}
				else
				{
					/* Sequence is not is busy state, it can be deleted safely */

					bucket->shb_list = gtm_list_delete_cell(bucket->shb_list, cell, prev);
					elog(LOG, "Sequence %s was deleted from GTM",
							  curr_seqinfo->gs_key->gsk_key);

					deleted = true;
				}
				GTM_RWLockRelease(&curr_seqinfo->gs_lock);
			}
			if (deleted)
			{
				if (prev)
					cell = gtm_lnext(prev);
				else
					cell = gtm_list_head(bucket->shb_list);
			}
			else
			{
				prev = cell;
				cell = gtm_lnext(cell);
			}
		}
		GTM_RWLockRelease(&bucket->shb_lock);
	}

	return res;
}
/*
 * Rename an existing sequence with a new name
 */
int
GTM_SeqRename(GTM_SequenceKey seqkey, GTM_SequenceKey newseqkey)
{
	GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);
	GTM_SeqInfo *newseqinfo = NULL;
	int errcode = 0;

	/* replace old key by new key */
	if (seqinfo == NULL)
	{
		ereport(LOG,
				(EINVAL,
				 errmsg("The sequence with the given key does not exist")));
		return EINVAL;
	}

	/* Now create the new sequence info */
	newseqinfo = (GTM_SeqInfo *) palloc(sizeof (GTM_SeqInfo));

	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);
	GTM_RWLockInit(&newseqinfo->gs_lock);

	newseqinfo->gs_ref_count = 0;
	newseqinfo->gs_key = seq_copy_key(newseqkey);
	newseqinfo->gs_state = seqinfo->gs_state;
	newseqinfo->gs_called = seqinfo->gs_called;

	newseqinfo->gs_increment_by = seqinfo->gs_increment_by;
	newseqinfo->gs_min_value = seqinfo->gs_min_value;
	newseqinfo->gs_max_value = seqinfo->gs_max_value;

	newseqinfo->gs_init_value = seqinfo->gs_init_value;
	newseqinfo->gs_value = seqinfo->gs_value;
	newseqinfo->gs_backedUpValue = seqinfo->gs_backedUpValue;
	newseqinfo->gs_cycle = seqinfo->gs_cycle;

	newseqinfo->gs_state = seqinfo->gs_state;
	newseqinfo->gs_last_value = seqinfo->gs_last_value;

	/* Add the copy to the list */
	if ((errcode = seq_add_seqinfo(newseqinfo))) /* a lock is taken here for the new sequence */
	{
		GTM_RWLockDestroy(&newseqinfo->gs_lock);
		pfree(newseqinfo->gs_key);
		pfree(newseqinfo);
		return errcode;
	}

	/* Remove the old key with the old name */
	GTM_RWLockRelease(&seqinfo->gs_lock);
	/* Release first the structure as it has been taken previously */
	seq_release_seqinfo(seqinfo);

	/* Close sequence properly, full name is here */
	seqkey->gsk_type = GTM_SEQ_FULL_NAME;
	/* Then close properly the old sequence */
	GTM_SeqClose(seqkey);
	return errcode;
}


/*
 * Set values for the sequence
 */
int
GTM_SeqSetVal(GTM_SequenceKey seqkey, GTM_Sequence nextval, bool iscalled)
{
	GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);

	if (seqinfo == NULL)
	{
		ereport(LOG,
				(EINVAL,
				 errmsg("The sequence with the given key does not exist")));

		return EINVAL;
	}

	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);

	seqinfo->gs_last_value = seqinfo->gs_value;

	if (seqinfo->gs_value != nextval)
		seqinfo->gs_value = nextval;

	seqinfo->gs_called = iscalled;

	/* If sequence is not called, reset the init value to the value set */
	if (!iscalled)
		seqinfo->gs_init_value = nextval;

	/* Remove the old key with the old name */
	GTM_RWLockRelease(&seqinfo->gs_lock);
	GTM_SetNeedBackup();
	seq_release_seqinfo(seqinfo);

	return 0;
}

/*
 * Get next value for the sequence
 */
GTM_Sequence
GTM_SeqGetNext(GTM_SequenceKey seqkey)
{
	GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);
	GTM_Sequence value;

	if (seqinfo == NULL)
	{
		ereport(LOG,
				(EINVAL,
				 errmsg("The sequence with the given key does not exist")));
		return InvalidSequenceValue;
	}

	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);

	/*
	 * If the sequence is called for the first time, initialize the value and
	 * return the start value
	 */
	if (!SEQ_IS_CALLED(seqinfo))
	{
		value = seqinfo->gs_last_value = seqinfo->gs_value = seqinfo->gs_init_value;
		seqinfo->gs_called = true;
		GTM_RWLockRelease(&seqinfo->gs_lock);
		seq_release_seqinfo(seqinfo);
		return value;
	}

	if (SEQ_IS_ASCENDING(seqinfo))
	{
		/*
		 * Check if the sequence is about to wrap-around. If the sequence does
		 * not support wrap-around, throw an error and return
		 * InvalidSequenceValue
		 */
		if (seqinfo->gs_max_value - seqinfo->gs_increment_by >= seqinfo->gs_value)
			value = seqinfo->gs_last_value = seqinfo->gs_value = seqinfo->gs_value + seqinfo->gs_increment_by;
		else if (SEQ_IS_CYCLE(seqinfo))
			value = seqinfo->gs_last_value = seqinfo->gs_value = seqinfo->gs_min_value;
		else
		{
			GTM_RWLockRelease(&seqinfo->gs_lock);
			seq_release_seqinfo(seqinfo);
			ereport(LOG,
					(ERANGE,
					 errmsg("Sequence reached maximum value")));
			return InvalidSequenceValue;
		}
	}
	else
	{
		/*
		 * Check if the sequence is about to wrap-around. If the sequence does
		 * not support wrap-around, throw an error and return
		 * InvalidSequenceValue, otherwise wrap around the sequence and reset
		 * it to the max value.
		 *
		 * Note: The gs_increment_by is a signed integer and is negative for
		 * descending sequences. So we don't need special handling below
		 */
		if (seqinfo->gs_min_value - seqinfo->gs_increment_by <= seqinfo->gs_value)
			value = seqinfo->gs_value = seqinfo->gs_last_value = seqinfo->gs_value + seqinfo->gs_increment_by;
		else if (SEQ_IS_CYCLE(seqinfo))
			value = seqinfo->gs_value = seqinfo->gs_last_value = seqinfo->gs_max_value;
		else
		{
			GTM_RWLockRelease(&seqinfo->gs_lock);
			seq_release_seqinfo(seqinfo);
			ereport(LOG,
					(ERANGE,
					 errmsg("Sequence reached minimum value")));
			return InvalidSequenceValue;
		}

	}
	GTM_RWLockRelease(&seqinfo->gs_lock);
	if (GTM_NeedSeqRestoreUpdateInternal(seqinfo))
		GTM_SetNeedBackup();
	seq_release_seqinfo(seqinfo);
	return value;
}

/*
 * Reset the sequence
 */
int
GTM_SeqReset(GTM_SequenceKey seqkey)
{
	GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);

	if (seqinfo == NULL)
	{
		ereport(LOG,
				(EINVAL,
				 errmsg("The sequence with the given key does not exist")));
		return EINVAL;
	}

	GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_WRITE);
	seqinfo->gs_value = seqinfo->gs_last_value = seqinfo->gs_backedUpValue = seqinfo->gs_init_value;
	GTM_RWLockRelease(&seqinfo->gs_lock);

	GTM_SetNeedBackup();
	seq_release_seqinfo(seqinfo);

	return 0;
}

void
GTM_InitSeqManager(void)
{
	int ii;

	for (ii = 0; ii < SEQ_HASH_TABLE_SIZE; ii++)
	{
		GTMSequences[ii].shb_list = gtm_NIL;
		GTM_RWLockInit(&GTMSequences[ii].shb_lock);
	}
}

/*
 * Process MSG_SEQUENCE_INIT/MSG_BKUP_SEQUENCE_INIT message
 *
 * is_backup indicates the message is MSG_BKUP_SEQUENCE_INIT
 */
void
ProcessSequenceInitCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey;
	GTM_Sequence increment, minval, maxval, startval;
	bool cycle;
	StringInfoData buf;
	int errcode;
	MemoryContext oldContext;

	/*
	 * Get the sequence key
	 */
	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);

	/*
	 * Read various sequence parameters
	 */
	memcpy(&increment, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		   sizeof (GTM_Sequence));
	memcpy(&minval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		   sizeof (GTM_Sequence));
	memcpy(&maxval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		   sizeof (GTM_Sequence));
	memcpy(&startval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		   sizeof (GTM_Sequence));

	cycle = pq_getmsgbyte(message);


	/*
	 * We must use the TopMostMemoryContext because the sequence information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	if ((errcode = GTM_SeqOpen(&seqkey, increment, minval, maxval, startval, cycle)))
		ereport(ERROR,
				(errcode,
				 errmsg("Failed to open a new sequence")));

	MemoryContextSwitchTo(oldContext);

	elog(LOG, "Opening sequence %s", seqkey.gsk_key);

	pq_getmsgend(message);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling open_sequence() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			rc = bkup_open_sequence(GetMyThreadInfo->thr_conn->standby,
									&seqkey,
									increment,
									minval,
									maxval,
									startval,
									cycle);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "open_sequence() returns rc %d.", rc);
		}
		/*
		 * Send a SUCCESS message back to the client
		 */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_INIT_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, seqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, seqkey.gsk_key, seqkey.gsk_keylen);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}
	}
	/* FIXME: need to check errors */
}


/*
 * Process MSG_SEQUENCE_ALTER/MSG_BKUP_SEQUENCE_ALTER message
 *
 * is_backup indicates the message is MSG_BKUP_SEQUENCE_ALTER
 */
void
ProcessSequenceAlterCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey;
	GTM_Sequence increment, minval, maxval, startval, lastval;
	bool cycle, is_restart;
	StringInfoData buf;
	int errcode;
	MemoryContext oldContext;

	/*
	 * Get the sequence key
	 */
	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);

	/*
	 * Read various sequence parameters
	 */
	memcpy(&increment, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		sizeof (GTM_Sequence));
	memcpy(&minval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		sizeof (GTM_Sequence));
	memcpy(&maxval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		sizeof (GTM_Sequence));
	memcpy(&startval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		sizeof (GTM_Sequence));
	memcpy(&lastval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		sizeof (GTM_Sequence));

	cycle = pq_getmsgbyte(message);
	is_restart = pq_getmsgbyte(message);

	/*
	 * We must use the TopMostMemoryContext because the sequence information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	elog(LOG, "Altering sequence key %s", seqkey.gsk_key);

	if ((errcode = GTM_SeqAlter(&seqkey, increment, minval, maxval, startval, lastval, cycle, is_restart)))
		ereport(ERROR,
				(errcode,
				 errmsg("Failed to open a new sequence")));

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	if (!is_backup)
	{
		/* Backup first */
		if ( GetMyThreadInfo->thr_conn->standby )
		{
			int rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling alter_sequence() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			rc = bkup_alter_sequence(GetMyThreadInfo->thr_conn->standby,
									 &seqkey,
									 increment,
									 minval,
									 maxval,
									 startval,
									 lastval,
									 cycle,
									 is_restart);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "alter_sequence() returns rc %d.", rc);
		}
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_ALTER_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, seqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, seqkey.gsk_key, seqkey.gsk_keylen);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

		/* FIXME: need to check errors */
	}
}


/*
 * Process MSG_SEQUENCE_LIST message
 */
void
ProcessSequenceListCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;
	int seq_count;
	int seq_maxcount;
	GTM_SeqInfo **seq_list;
	int i;

	if (Recovery_IsStandby())
		ereport(ERROR,
			(EPERM,
			 errmsg("Operation not permitted under the standby mode.")));

	seq_count = 0;
	seq_maxcount = 1024;
	seq_list = (GTM_SeqInfo **) palloc(seq_maxcount * sizeof(GTM_SeqInfo *));;

	/*
	 * Store pointers to all GTM_SeqInfo in the hash buckets into an array.
	 */
	for (i = 0 ; i < SEQ_HASH_TABLE_SIZE ; i++)
	{
		GTM_SeqInfoHashBucket *b;
		gtm_ListCell *elem;

		b = &GTMSequences[i];

		GTM_RWLockAcquire(&b->shb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, b->shb_list)
		{
			/* Allocate larger array if required */
			if (seq_count == seq_maxcount)
			{
				int 			newcount;
				GTM_SeqInfo   **newlist;

				newcount = 2 * seq_maxcount;
				newlist = (GTM_SeqInfo **) repalloc(seq_list, newcount * sizeof(GTM_SeqInfo *));
				/*
				 * If failed try to get less. It is unlikely to happen, but
				 * let's be safe.
				 */
				while (newlist == NULL)
				{
					newcount = seq_maxcount + (newcount - seq_maxcount) / 2 - 1;
					if (newcount <= seq_maxcount)
					{
						/* give up */
						ereport(ERROR,
								(ERANGE,
								 errmsg("Can not list all the sequences")));
					}
					newlist = (GTM_SeqInfo **) repalloc(seq_list, newcount * sizeof(GTM_SeqInfo *));
				}
				seq_maxcount = newcount;
				seq_list = newlist;
			}
			seq_list[seq_count] = (GTM_SeqInfo *) gtm_lfirst(elem);
			seq_count++;
		}

		GTM_RWLockRelease(&b->shb_lock);
	}

	pq_getmsgend(message);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, SEQUENCE_LIST_RESULT, 4);

	if (myport->remote_type == GTM_NODE_GTM_PROXY)
	{
		GTM_ProxyMsgHeader proxyhdr;
		proxyhdr.ph_conid = myport->conn_id;
		pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
	}

	/* Send a number of sequences */
	pq_sendint(&buf, seq_count, 4);

	/*
	 * Send sequences from the array
	 */
	{
		/*
		 * TODO set initial size big enough to fit any sequence, and avoid
		 * reallocations.
		 */
		size_t seq_maxlen = 256;
		char *seq_buf = (char *) palloc(seq_maxlen);

		for (i = 0 ; i < seq_count ; i++)
		{
			size_t seq_buflen = gtm_get_sequence_size(seq_list[i]);
			if (seq_buflen > seq_maxlen)
			{
				seq_maxlen = seq_buflen;
				seq_buf = (char *)repalloc(seq_buf, seq_maxlen);
			}

			gtm_serialize_sequence(seq_list[i], seq_buf, seq_buflen);

			elog(DEBUG1, "seq_buflen = %ld", seq_buflen);

			pq_sendint(&buf, seq_buflen, 4);
			pq_sendbytes(&buf, seq_buf, seq_buflen);
		}
	}

	pq_endmessage(myport, &buf);

	elog(DEBUG1, "ProcessSequenceListCommand() done.");

	if (myport->remote_type != GTM_NODE_GTM_PROXY)
		/* Don't flush to the backup because this does not change the internal status */
		pq_flush(myport);
}


/*
 * Process MSG_SEQUENCE_GET_NEXT/MSG_BKUP_SEQUENCE_GET_NEXT message
 *
 * is_backup indicates the message is MSG_BKUP_SEQUENCE_GET_NEXT
 */
void
ProcessSequenceGetNextCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey;
	StringInfoData buf;
	GTM_Sequence seqval;

	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);

	seqval = GTM_SeqGetNext(&seqkey);
	if (!SEQVAL_IS_VALID(seqval))
		ereport(ERROR,
				(ERANGE,
				 errmsg("Can not get current value of the sequence")));

	elog(DEBUG1, "Getting next value %ld for sequence %s", seqval, seqkey.gsk_key);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			GTM_Sequence loc_seq;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling get_next() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			bkup_get_next(GetMyThreadInfo->thr_conn->standby, &seqkey, &loc_seq);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously &&(myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "get_next() returns GTM_Sequence %ld.", loc_seq);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_GET_NEXT_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, seqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, seqkey.gsk_key, seqkey.gsk_keylen);
		pq_sendbytes(&buf, (char *)&seqval, sizeof (GTM_Sequence));
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush to the standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

		/* FIXME: need to check errors */
	}
}

/*
 * Process MSG_SEQUENCE_SET_VAL/MSG_BKUP_SEQUENCE_SET_VAL message
 *
 * is_backup indicates the message is MSG_BKUP_SEQUENCE_SET_VAL
 */
void
ProcessSequenceSetValCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey;
	GTM_Sequence nextval;
	MemoryContext oldContext;
	StringInfoData buf;
	bool iscalled;
	int errcode;

	/*
	 * Get the sequence key
	 */
	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);

	/* Read parameters to be set */
	memcpy(&nextval, pq_getmsgbytes(message, sizeof (GTM_Sequence)),
		   sizeof (GTM_Sequence));

	iscalled = pq_getmsgbyte(message);

	/*
	 * We must use the TopMostMemoryContext because the sequence information is
	 * not bound to a thread and can outlive any of the thread specific
	 * contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	elog(DEBUG1, "Setting new value %ld for sequence %s", nextval, seqkey.gsk_key);

	if ((errcode = GTM_SeqSetVal(&seqkey, nextval, iscalled)))
		ereport(ERROR,
				(errcode,
				 errmsg("Failed to set values of sequence")));

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling set_val() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			rc = bkup_set_val(GetMyThreadInfo->thr_conn->standby,
							  &seqkey,
							  nextval,
							  iscalled);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "set_val() returns rc %d.", rc);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_SET_VAL_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, seqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, seqkey.gsk_key, seqkey.gsk_keylen);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	/* FIXME: need to check errors */
}

/*
 * Process MSG_SEQUENCE_RESET/MSG_BKUP_SEQUENCE_RESET message
 *
 * is_backup indicates the cmessage is MSG_BKUP_SEQUENCE_RESULT
 */
void
ProcessSequenceResetCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey;
	StringInfoData buf;
	int errcode;

	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);

	elog(LOG, "Resetting sequence %s", seqkey.gsk_key);

	if ((errcode = GTM_SeqReset(&seqkey)))
		ereport(ERROR,
				(errcode,
				 errmsg("Can not reset the sequence")));

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling reset_sequence() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			rc = bkup_reset_sequence(GetMyThreadInfo->thr_conn->standby, &seqkey);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "reset_sequence() returns rc %d.", rc);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_RESET_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, seqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, seqkey.gsk_key, seqkey.gsk_keylen);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	/* FIXME: need to check errors */
}

/*
 * Process MSG_SEQUENCE_CLOSE/MSG_BKUP_SEQUENCE_CLOSE message
 *
 * is_backup indicates the message is MSG_BKUP_SEQUENCE_CLOSE
 */
void
ProcessSequenceCloseCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey;
	StringInfoData buf;
	int errcode;

	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);
	memcpy(&seqkey.gsk_type, pq_getmsgbytes(message, sizeof (GTM_SequenceKeyType)),
		   sizeof (GTM_SequenceKeyType));

	elog(DEBUG1, "Closing sequence %s", seqkey.gsk_key);

	if ((errcode = GTM_SeqClose(&seqkey)))
		ereport(ERROR,
				(errcode,
				 errmsg("Can not close the sequence")));

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling close_sequence() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			rc = bkup_close_sequence(GetMyThreadInfo->thr_conn->standby, &seqkey);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "close_sequence() returns rc %d.", rc);
		}
		/* Respond to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_CLOSE_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, seqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, seqkey.gsk_key, seqkey.gsk_keylen);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

		/* FIXME: need to check errors */
	}
}

/*
 * Process MSG_SEQUENCE_RENAME/MSG_BKUP_SEQUENCE_RENAME message
 *
 * is_backup indicates the message is MSG_BKUP_SEQUENCE_RENAME
 */
void
ProcessSequenceRenameCommand(Port *myport, StringInfo message, bool is_backup)
{
	GTM_SequenceKeyData seqkey, newseqkey;
	StringInfoData buf;
	int errcode;
	MemoryContext oldContext;

	/* get the message from backend */
	seqkey.gsk_keylen = pq_getmsgint(message, sizeof (seqkey.gsk_keylen));
	seqkey.gsk_key = (char *)pq_getmsgbytes(message, seqkey.gsk_keylen);

	/* Get the rest of the message, new name length and string with new name */
	newseqkey.gsk_keylen = pq_getmsgint(message, sizeof (newseqkey.gsk_keylen));
	newseqkey.gsk_key = (char *)pq_getmsgbytes(message, newseqkey.gsk_keylen);

	/*
	 * As when creating a sequence, we must use the TopMostMemoryContext
	 * because the sequence information is not bound to a thread and
	 * can outlive any of the thread specific contextes.
	 */
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);

	elog(LOG, "Renaming sequence %s to %s", seqkey.gsk_key, newseqkey.gsk_key);

	if ((errcode = GTM_SeqRename(&seqkey, &newseqkey)))
		ereport(ERROR,
				(errcode,
				 errmsg("Can not rename the sequence")));

	MemoryContextSwitchTo(oldContext);

	pq_getmsgend(message);

	if (!is_backup)
	{
		/* Backup first */
		if (GetMyThreadInfo->thr_conn->standby)
		{
			int rc;
			GTM_Conn *oldconn = GetMyThreadInfo->thr_conn->standby;
			int count = 0;

			elog(DEBUG1, "calling rename_sequence() for standby GTM %p.", GetMyThreadInfo->thr_conn->standby);

		retry:
			rc = bkup_rename_sequence(GetMyThreadInfo->thr_conn->standby, &seqkey, &newseqkey);

			if (gtm_standby_check_communication_error(&count, oldconn))
				goto retry;

			/* Sync */
			if (Backup_synchronously && (myport->remote_type != GTM_NODE_GTM_PROXY))
				gtm_sync_standby(GetMyThreadInfo->thr_conn->standby);

			elog(DEBUG1, "rename_sequence() returns rc %d.", rc);
		}
		/* Send a SUCCESS message back to the client */
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, SEQUENCE_RENAME_RESULT, 4);
		if (myport->remote_type == GTM_NODE_GTM_PROXY)
		{
			GTM_ProxyMsgHeader proxyhdr;
			proxyhdr.ph_conid = myport->conn_id;
			pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
		}
		pq_sendint(&buf, newseqkey.gsk_keylen, 4);
		pq_sendbytes(&buf, newseqkey.gsk_key, newseqkey.gsk_keylen);
		pq_endmessage(myport, &buf);

		if (myport->remote_type != GTM_NODE_GTM_PROXY)
		{
			/* Flush the standby first */
			if (GetMyThreadInfo->thr_conn->standby)
				gtmpqFlush(GetMyThreadInfo->thr_conn->standby);
			pq_flush(myport);
		}

	}
	/* FIXME: need to check errors */
}



/*
 * Escape whitespace and non-printable characters in the sequence name to
 * store it to the control file.
 */
static void
encode_seq_key(GTM_SequenceKey seqkey, char *buffer)
{
	int 	i;
	char	c;
	char   *out;

	out = buffer;
	for (i = 0; i < seqkey->gsk_keylen; i++)
	{
		c = seqkey->gsk_key[i];

		if (c == '\\') /* double backslach */
		{
			*out++ = '\\';
			*out++ = '\\';
		}
		else if (c > ' ') /* no need to escape */
		{
			*out++ = c;
		}
		else if (c == '\n') /* below some known non-printable chars */
		{
			*out++ = '\\';
			*out++ = 'n';
		}
		else if (c == '\r')
		{
			*out++ = '\\';
			*out++ = 'r';
		}
		else if (c == '\t')
		{
			*out++ = '\\';
			*out++ = 't';
		}
		else /* other non-printable chars */
		{
			*out++ = '\\';
			if ((int) c < 10)
			{
				*out++ = '0';
				*out++ = (char) ((int) '0' + (int) c);
			}
			else
			{
				*out++ = (char) ((int) '0' + ((int) c) / 10);
				*out++ = (char) ((int) '0' + ((int) c) % 10);
			}
		}
	}
	/* Add NULL terminator */
	*out++ = '\0';
}


/*
 * Decode the string encoded by the encode_seq_key function
 */
static void
decode_seq_key(char* value, GTM_SequenceKey seqkey)
{
	char   *in;
	char 	out[1024];
	int		len = 0;

	in = value;
	while (*in != '\0')
	{
		if (*in == '\\') /* get escaped character */
		{
			in++; /* next value */
			if (*in == '\\')
				out[len++] = *in++;
			else if (*in == 'n')
			{
				out[len++] = '\n';
				in++;
			}
			else if (*in == 'r')
			{
				out[len++] = '\r';
				in++;
			}
			else if (*in == 't')
			{
				out[len++] = '\t';
				in++;
			}
			else /* \nn format */
			{
				int val;
				val = ((int) *in++ - (int) '0');
				val *= 10;
				val += ((int) *in++ - (int) '0');
				out[len++] = (char) val;
			}
		}
		else /* get plain character */
		{
			out[len++] = *in++;
		}
	}
	/* copy result to palloc'ed memory */
	seqkey->gsk_keylen = len;
	seqkey->gsk_key = (char *) palloc(len);
	memcpy(seqkey->gsk_key, out, len);
}

static GTM_Sequence distanceToBackedUpSeqValue(GTM_SeqInfo *seqinfo)
{
	if (!SEQ_IS_CALLED(seqinfo))
		return (GTM_Sequence)0;
	if (SEQ_IS_ASCENDING(seqinfo))
	{
		if ((seqinfo->gs_backedUpValue - seqinfo->gs_value) >= 0)
			return (seqinfo->gs_backedUpValue - seqinfo->gs_value);
		if (SEQ_IS_CYCLE(seqinfo))
			return((seqinfo->gs_max_value - seqinfo->gs_value) + (seqinfo->gs_backedUpValue - seqinfo->gs_min_value));
		else
			return(seqinfo->gs_backedUpValue - seqinfo->gs_value);
	}
	else
	{
		if ((seqinfo->gs_value - seqinfo->gs_backedUpValue) >= 0)
			return(seqinfo->gs_backedUpValue - seqinfo->gs_value);
		if (SEQ_IS_CYCLE(seqinfo))
			return((seqinfo->gs_max_value - seqinfo->gs_backedUpValue) + (seqinfo->gs_value - seqinfo->gs_min_value));
		else
			return(seqinfo->gs_backedUpValue - seqinfo->gs_value);
	}
	return 0;
}

bool GTM_NeedSeqRestoreUpdate(GTM_SequenceKey seqkey)
{
	GTM_SeqInfo *seqinfo = seq_find_seqinfo(seqkey);
	if (!seqinfo)
		return FALSE;
	return GTM_NeedSeqRestoreUpdateInternal(seqinfo);
}

static bool GTM_NeedSeqRestoreUpdateInternal(GTM_SeqInfo *seqinfo)
{
	GTM_Sequence distance;

	if (!SEQ_IS_CALLED(seqinfo))
		/* The first call.  Must backup */
		return TRUE;
	distance = distanceToBackedUpSeqValue(seqinfo);
	if (SEQ_IS_ASCENDING(seqinfo))
		return(distance >= seqinfo->gs_increment_by);
	else
		return(distance <= seqinfo->gs_increment_by);
}



static void
GTM_SaveSeqInfo2(FILE *ctlf, bool isBackup)
{
	GTM_SeqInfoHashBucket *bucket;
	gtm_ListCell *elem;
	GTM_SeqInfo *seqinfo = NULL;
	int hash;
	char buffer[1024];

	for (hash = 0; hash < SEQ_HASH_TABLE_SIZE; hash++)
	{
		bucket = &GTMSequences[hash];

		GTM_RWLockAcquire(&bucket->shb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, bucket->shb_list)
		{
			seqinfo = (GTM_SeqInfo *) gtm_lfirst(elem);
			if (seqinfo == NULL)
				break;

			if (seqinfo->gs_state == SEQ_STATE_DELETED)
				continue;

			GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_READ);

			encode_seq_key(seqinfo->gs_key, buffer);
			fprintf(ctlf, "%s\t%ld\t%ld\t%ld\t%ld\t%ld\t%c\t%c\t%x\n",
					buffer, isBackup ? seqinfo->gs_backedUpValue : seqinfo->gs_value,
					seqinfo->gs_init_value, seqinfo->gs_increment_by,
					seqinfo->gs_min_value, seqinfo->gs_max_value,
					(seqinfo->gs_cycle ? 't' : 'f'),
					(seqinfo->gs_called ? 't' : 'f'),
					seqinfo->gs_state);

				GTM_RWLockRelease(&seqinfo->gs_lock);
		}
		GTM_RWLockRelease(&bucket->shb_lock);
	}

}

void GTM_SaveSeqInfo(FILE *ctlf)
{
	GTM_SaveSeqInfo2(ctlf, FALSE);
}

static void advance_gs_value(GTM_SeqInfo *seqinfo)
{
	
	GTM_Sequence distance;

	distance = seqinfo->gs_increment_by * RestoreDuration;
	if (SEQ_IS_ASCENDING(seqinfo))
	{
		if ((seqinfo->gs_max_value - seqinfo->gs_value) >= distance)
			seqinfo->gs_backedUpValue = seqinfo->gs_value + distance;
		else
		{
			if (SEQ_IS_CYCLE(seqinfo))
				seqinfo->gs_backedUpValue = seqinfo->gs_min_value + (distance - (seqinfo->gs_max_value - seqinfo->gs_value));
			else
				seqinfo->gs_backedUpValue = seqinfo->gs_max_value;
		}
	}
	else
	{
		if ((seqinfo->gs_min_value - seqinfo->gs_value) >= distance)
			seqinfo->gs_backedUpValue = seqinfo->gs_value + distance;
		else
		{
			if (SEQ_IS_CYCLE(seqinfo))
				seqinfo->gs_backedUpValue = seqinfo->gs_max_value + (distance - (seqinfo->gs_min_value - seqinfo->gs_value));
			else
				seqinfo->gs_backedUpValue = seqinfo->gs_min_value;
		}
	}
}


static void
GTM_UpdateRestorePointSeq(void)
{
	GTM_SeqInfoHashBucket *bucket;
	gtm_ListCell *elem;
	GTM_SeqInfo *seqinfo = NULL;
	int hash;

	for (hash = 0; hash < SEQ_HASH_TABLE_SIZE; hash++)
	{
		bucket = &GTMSequences[hash];

		GTM_RWLockAcquire(&bucket->shb_lock, GTM_LOCKMODE_READ);

		gtm_foreach(elem, bucket->shb_list)
		{
			seqinfo = (GTM_SeqInfo *) gtm_lfirst(elem);
			if (seqinfo == NULL)
				break;

			if (seqinfo->gs_state == SEQ_STATE_DELETED)
				continue;

			GTM_RWLockAcquire(&seqinfo->gs_lock, GTM_LOCKMODE_READ);
			advance_gs_value(seqinfo);
			GTM_RWLockRelease(&seqinfo->gs_lock);
		}

		GTM_RWLockRelease(&bucket->shb_lock);
	}

}


void GTM_WriteRestorePointSeq(FILE *ctlf)
{
	GTM_UpdateRestorePointSeq();
	GTM_SaveSeqInfo2(ctlf, TRUE);
}

void
GTM_RestoreSeqInfo(FILE *ctlf)
{
	char seqname[1024];

	if (ctlf == NULL)
		return;

	while (fscanf(ctlf, "%s", seqname) == 1)
	{
		GTM_SequenceKeyData seqkey;
		GTM_Sequence increment_by;
		GTM_Sequence minval;
		GTM_Sequence maxval;
		GTM_Sequence startval;
		GTM_Sequence curval;
		int32 state;
		bool cycle;
		bool called;
		char boolval[16];

		decode_seq_key(seqname, &seqkey);

		if (fscanf(ctlf, "%ld", &curval) != 1)
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%ld", &startval) != 1)
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%ld", &increment_by) != 1)
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%ld", &minval) != 1)
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%ld", &maxval) != 1)
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%s", boolval) == 1)
		{
			cycle = (*boolval == 't');
		}
		else
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%s", boolval) == 1)
		{
			called = (*boolval == 't');
		}
		else
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		if (fscanf(ctlf, "%x", &state) != 1)
		{
			elog(WARNING, "Corrupted control file");
			return;
		}
		GTM_SeqRestore(&seqkey, increment_by, minval, maxval, startval, curval,
					   state, cycle, called);
	}
}
