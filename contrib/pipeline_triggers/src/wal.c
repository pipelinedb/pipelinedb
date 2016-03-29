/*-------------------------------------------------------------------------
 *
 * wal.c
 *	  Functionality for wal
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "wal.h"
#include "replication/slot.h"
#include "replication/logicalfuncs.h"
#include "config.h"
#include "util.h"
#include "miscadmin.h"
#include "replication/walsender_private.h"
#include "replication/walsender.h"

#define WAL_POLL_TIMEOUT 10 /* 10ms */
#define TRIGGER_REPLICATION_SLOT_NAME "pipelinedb_enterprise_trigger"

/*
 * acquire_my_replication_slot
 *
 * Acquire the replication slot that this process uses, permanently creating it
 * if it doesn't already exist
 */
static void
acquire_my_replication_slot()
{
	int i;
	bool found = false;

	Assert(!MyReplicationSlot);

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
		if (s->in_use && pg_strcasecmp(TRIGGER_REPLICATION_SLOT_NAME,
					NameStr(s->data.name)) == 0)
		{
			found = true;
			break;
		}
	}
	LWLockRelease(ReplicationSlotControlLock);

	if (!found)
	{
		LogicalDecodingContext *cxt;

		ReplicationSlotCreate(TRIGGER_REPLICATION_SLOT_NAME, true,
				RS_EPHEMERAL);

		cxt = CreateInitDecodingContext("pipeline_triggers", NIL,
				logical_read_local_xlog_page, NULL, NULL);

		/* Build initial snapshot, for the slot. Might take a while. */
		DecodingContextFindStartpoint(cxt);

		/* don't need the decoding context anymore */
		FreeDecodingContext(cxt);

		/* Ok, slot is now fully created, mark it as persistent .*/
		ReplicationSlotPersist();

		ReplicationSlotRelease();
	}

	ReplicationSlotAcquire(TRIGGER_REPLICATION_SLOT_NAME);
	Assert(MyReplicationSlot);
}

/*
 * create_wal_stream
 */
WalStream *
create_wal_stream(void *pdata)
{
	WalStream *wal_stream = palloc0(sizeof(WalStream));
	XLogRecPtr start_lsn = GetFlushRecPtr();

	CheckLogicalDecodingRequirements();
	acquire_my_replication_slot();

	wal_stream->logical_decoding_ctx =
		CreateDecodingContext(start_lsn, NIL,
				logical_read_local_xlog_page_non_block,
				trigger_prepare_write,
				trigger_write_data);

	wal_stream->logical_decoding_ctx->output_plugin_private = pdata;
	wal_stream->startptr = MyReplicationSlot->data.restart_lsn;
	wal_stream->end_of_wal = GetFlushRecPtr();

	return wal_stream;
}

/*
 * destroy_wal_stream
 */
void
destroy_wal_stream(WalStream *stream)
{
	FreeDecodingContext(stream->logical_decoding_ctx);
	ReplicationSlotRelease();
}

/*
 * wal_stream_end_rec_ptr
 */
static XLogRecPtr
wal_stream_end_rec_ptr(WalStream *stream)
{
	return stream->logical_decoding_ctx->reader->EndRecPtr;
}

/*
 * poll_wal_stream
 */
static void
poll_wal_stream(WalStream *stream)
{
	CHECK_FOR_INTERRUPTS();

	WaitLatch(MyWalSnd->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH |
			WL_TIMEOUT, WAL_POLL_TIMEOUT);
	ResetLatch(MyWalSnd->latch);

	stream->end_of_wal = GetFlushRecPtr();
}

/*
 * wal_stream_read
 */
void
wal_stream_read(WalStream *stream, bool *did_read)
{
	XLogRecord *record;
	char *errm;

	poll_wal_stream(stream);

	while (wal_stream_end_rec_ptr(stream) < stream->end_of_wal)
	{
		record = XLogReadRecord(stream->logical_decoding_ctx->reader,
								stream->startptr, &errm);

		stream->startptr = InvalidXLogRecPtr;

		if (errm)
			elog(ERROR, "%s", errm);

		/* torn page */
		if (!record)
			break;

		LogicalDecodingProcessRecord(stream->logical_decoding_ctx,
				stream->logical_decoding_ctx->reader);

		*did_read = true;
	}
}

/*
 * wal_init
 *
 * Initialize WAL global variables
 */
void
wal_init()
{
    am_walsender = true;
    InitWalSender();
    WalSndSetState(WALSNDSTATE_STREAMING);
    Assert(!am_cascading_walsender);
}