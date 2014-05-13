/*-------------------------------------------------------------------------
 *
 * gtm_seq.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_SEQ_H
#define GTM_SEQ_H

#include "gtm/stringinfo.h"
#include "gtm/gtm_lock.h"
#include "gtm/libpq-be.h"

/* Global sequence  related structures */

typedef struct GTM_SeqInfo
{
	GTM_SequenceKey	gs_key;
	GTM_Sequence	gs_value;
	GTM_Sequence	gs_backedUpValue;
	GTM_Sequence	gs_init_value;
	GTM_Sequence	gs_last_value;
	GTM_Sequence	gs_increment_by;
	GTM_Sequence	gs_min_value;
	GTM_Sequence	gs_max_value;
	bool			gs_cycle;
	bool			gs_called;

	int32			gs_ref_count;
	int32			gs_state;
	GTM_RWLock		gs_lock;
} GTM_SeqInfo;

#define SEQ_STATE_ACTIVE	1
#define SEQ_STATE_DELETED	2

#define SEQ_IS_ASCENDING(s)		((s)->gs_increment_by > 0)
#define SEQ_IS_CYCLE(s)		((s)->gs_cycle)
#define SEQ_IS_CALLED(s)	((s)->gs_called)

#define SEQ_DEF_MAX_SEQVAL_ASCEND			0x7ffffffffffffffeLL
#define SEQ_DEF_MIN_SEQVAL_ASCEND			0x1

#define SEQ_DEF_MAX_SEQVAL_DESCEND			-0x1
#define SEQ_DEF_MIN_SEQVAL_DESCEND			-0x7ffffffffffffffeLL

#define SEQ_MAX_REFCOUNT		1024

/* SEQUENCE Management */
void GTM_InitSeqManager(void);
int GTM_SeqOpen(GTM_SequenceKey seqkey,
			GTM_Sequence increment_by,
			GTM_Sequence minval,
			GTM_Sequence maxval,
			GTM_Sequence startval,
			bool cycle);
int GTM_SeqAlter(GTM_SequenceKey seqkey,
				 GTM_Sequence increment_by,
				 GTM_Sequence minval,
				 GTM_Sequence maxval,
				 GTM_Sequence startval,
				 GTM_Sequence lastval,
				 bool cycle,
				 bool is_restart);
int GTM_SeqClose(GTM_SequenceKey seqkey);
int GTM_SeqRename(GTM_SequenceKey seqkey, GTM_SequenceKey newseqkey);
GTM_Sequence GTM_SeqGetNext(GTM_SequenceKey seqkey);
int GTM_SeqSetVal(GTM_SequenceKey seqkey, GTM_Sequence nextval, bool iscalled);
int GTM_SeqReset(GTM_SequenceKey seqkey);

void ProcessSequenceInitCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceGetNextCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceSetValCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceResetCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceCloseCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceRenameCommand(Port *myport, StringInfo message, bool is_backup);
void ProcessSequenceAlterCommand(Port *myport, StringInfo message, bool is_backup);

void ProcessSequenceListCommand(Port *myport, StringInfo message);

void GTM_SaveSeqInfo(FILE *ctlf);
void GTM_RestoreSeqInfo(FILE *ctlf);
int GTM_SeqRestore(GTM_SequenceKey seqkey,
			   GTM_Sequence increment_by,
			   GTM_Sequence minval,
			   GTM_Sequence maxval,
			   GTM_Sequence startval,
			   GTM_Sequence curval,
			   int32 state,
			   bool cycle,
			   bool called);

bool GTM_NeedSeqRestoreUpdate(GTM_SequenceKey seqkey);
void GTM_WriteRestorePointSeq(FILE *f);
#endif
