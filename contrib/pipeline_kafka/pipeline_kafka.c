/*-------------------------------------------------------------------------
 *
 * pipeline_kafka.c
 *
 *	  PipelineDB support for Kafka
 *
 * Copyright (c) 2015, PipelineDB
 *
 * contrib/pipeline_kafka.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_stream_fn.h"
#include "executor/spi.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "librdkafka/rdkafka.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pipeline_kafka.h"
#include "pipeline/stream.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

#define RETURN_SUCCESS() PG_RETURN_DATUM(CStringGetTextDatum("success"))
#define RETURN_FAILURE() PG_RETURN_DATUM(CStringGetTextDatum("failure"))

#define KAFKA_CONSUME_MAIN "kafka_consume_main"
#define PIPELINE_KAFKA_LIB "pipeline_kafka"

#define CONSUMER_RELATION "pipeline_kafka_consumers"
#define CONSUMER_RELATION_NATTS				6
#define CONSUMER_ATTR_RELATION 				1
#define CONSUMER_ATTR_TOPIC						2
#define CONSUMER_ATTR_BATCH_SIZE 			3
#define CONSUMER_ATTR_PARALLELISM 		4
#define CONSUMER_ATTR_FORMAT 					5
#define CONSUMER_ATTR_DELIMITER 			6

#define OFFSETS_RELATION "pipeline_kafka_offsets"
#define OFFSETS_RELATION_NATTS				3
#define OFFSETS_ATTR_CONSUMER 				1
#define OFFSETS_ATTR_PARTITION				2
#define OFFSETS_ATTR_OFFSET 					3

#define BROKER_RELATION "pipeline_kafka_brokers"
#define BROKER_RELATION_NATTS		1
#define BROKER_ATTR_HOST 				1

#define NUM_CONSUMERS_INIT 4
#define NUM_CONSUMERS_MAX 64

#define DEFAULT_PARALLELISM 1
#define MAX_CONSUMER_PROCS 32

#define CONSUMER_TIMEOUT 1000 /* 1s */
#define CONSUMER_BATCH_SIZE 1000

#define OPTION_DELIMITER "delimiter"
#define OPTION_FORMAT "format"
#define FORMAT_CSV "csv"

static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);

/*
 * Shared-memory state for each consumer process
 */
typedef struct KafkaConsumerProc
{
	Oid id;
	Oid consumer_id;
	int partition_group;
	NameData dbname;
	BackgroundWorkerHandle worker;
} KafkaConsumerProc;

/*
 * Shared-memory state for each consumer process group
 */
typedef struct KafkaConsumerGroup
{
	Oid consumer_id;
	int parallelism;
} KafkaConsumerGroup;

/*
 * Local-memory configuration for a consumer
 */
typedef struct KafkaConsumer
{
	Oid id;
	List *brokers;
	char *topic;
	RangeVar *rel;
	int32_t partition;
	int64_t offset;
	size_t batch_size;
	char *format;
	char *delimiter;
	int parallelism;
	int num_partitions;
	int64_t *offsets;
} KafkaConsumer;

/* Shared-memory hashtable for storing consumer process group information */
static HTAB *consumer_groups;

/* Shared-memory hashtable storing all individual consumer process information */
static HTAB *consumer_procs;

/*
 * Initialization performed at module-load time
 */
void
_PG_init(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(KafkaConsumerProc);
	ctl.hash = oid_hash;

	consumer_procs = ShmemInitHash("KafkaConsumerProcs", NUM_CONSUMERS_INIT,
			NUM_CONSUMERS_MAX, &ctl, HASH_ELEM | HASH_FUNCTION);

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(KafkaConsumerGroup);
	ctl.hash = oid_hash;

	consumer_groups = ShmemInitHash("KafkaConsumerGroups", 2 * NUM_CONSUMERS_INIT,
			2 * NUM_CONSUMERS_MAX, &ctl, HASH_ELEM | HASH_FUNCTION);
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
kafka_consume_main_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * open_pipeline_kafka_consumers
 *
 * Open and return pipeline_kafka_consumers relation
 */
static Relation
open_pipeline_kafka_consumers(void)
{
	Relation consumers = heap_openrv(makeRangeVar(NULL, CONSUMER_RELATION, -1), AccessExclusiveLock);
	return consumers;
}

/*
 * open_pipeline_kafka_brokers
 *
 * Open and return pipeline_kafka_brokers relation
 */
static Relation
open_pipeline_kafka_brokers(void)
{
	Relation brokers = heap_openrv(makeRangeVar(NULL, BROKER_RELATION, -1), AccessExclusiveLock);
	return brokers;
}

/*
 * open_pipeline_kafka_offsets
 *
 * Open and return pipeline_kafka_offsets relation
 */
static Relation
open_pipeline_kafka_offsets(void)
{
	Relation offsets = heap_openrv(makeRangeVar(NULL, OFFSETS_RELATION, -1), RowExclusiveLock);
	return offsets;
}

/*
 * librdkafka logger function
 */
static void
logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
	elog(LOG, "[kafka consumer]: %s", buf);
}

/*
 * get_all_brokers
 *
 * Return a list of all brokers in pipeline_kafka_brokers
 */
static List *
get_all_brokers(void)
{
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	Relation brokers = open_pipeline_kafka_brokers();
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(brokers));
	List *result = NIL;

	scan = heap_beginscan(brokers, GetTransactionSnapshot(), 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		char *host;
		Datum d;
		bool isnull;

		ExecStoreTuple(tup, slot, InvalidBuffer, false);
		d = slot_getattr(slot, BROKER_ATTR_HOST, &isnull);
		host = TextDatumGetCString(d);

		result = lappend(result, host);
	}

	ExecDropSingleTupleTableSlot(slot);
	heap_endscan(scan);
	heap_close(brokers, NoLock);

	return result;
}

/*
 * load_consumer_offsets
 *
 * Load all offsets for all of this consumer's partitions
 */
static void
load_consumer_offsets(KafkaConsumer *consumer, struct rd_kafka_metadata_topic *meta)
{
	MemoryContext old;
	ScanKeyData skey[1];
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	Relation offsets = open_pipeline_kafka_offsets();
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(offsets));
	int i;

	ScanKeyInit(&skey[0], OFFSETS_ATTR_CONSUMER, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(consumer->id));
	scan = heap_beginscan(offsets, GetTransactionSnapshot(), 1, skey);

	old = MemoryContextSwitchTo(CacheMemoryContext);
	consumer->offsets = palloc0(meta->partition_cnt * sizeof(int64_t));
	MemoryContextSwitchTo(old);

	/* by default, begin consuming from the end of a stream */
	for (i = 0; i < meta->partition_cnt; i++)
		consumer->offsets[i] = RD_KAFKA_OFFSET_END;

	consumer->num_partitions = meta->partition_cnt;

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum d;
		bool isnull;
		int partition;

		ExecStoreTuple(tup, slot, InvalidBuffer, false);

		d = slot_getattr(slot, OFFSETS_ATTR_PARTITION, &isnull);
		partition = DatumGetInt32(d);

		if(partition > consumer->num_partitions)
			elog(ERROR, "invalid partition id: %d", partition);

		d = slot_getattr(slot, OFFSETS_ATTR_OFFSET, &isnull);
		consumer->offsets[partition] = DatumGetInt64(d);
	}

	ExecDropSingleTupleTableSlot(slot);
	heap_endscan(scan);
	heap_close(offsets, RowExclusiveLock);
}

/*
 * load_consumer_state
 *
 * Read consumer state from pipeline_kafka_consumers into the given struct
 */
static void
load_consumer_state(Oid worker_id, KafkaConsumer *consumer)
{
	ScanKeyData skey[1];
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	Relation consumers = open_pipeline_kafka_consumers();
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(consumers));
	Datum d;
	bool isnull;
	text *qualified;
	MemoryContext old;

	MemSet(consumer, 0, sizeof(KafkaConsumer));

	ScanKeyInit(&skey[0], -2, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(worker_id));
	scan = heap_beginscan(consumers, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "kafka consumer %d not found", worker_id);

	ExecStoreTuple(tup, slot, InvalidBuffer, false);
	consumer->id = HeapTupleGetOid(tup);

	d = slot_getattr(slot, CONSUMER_ATTR_RELATION, &isnull);

	/* we don't want anything that's palloc'd to get freed when we commit */
	old = MemoryContextSwitchTo(CacheMemoryContext);

	/* target relation */
	qualified = (text *) DatumGetPointer(d);
	consumer->rel = makeRangeVarFromNameList(textToQualifiedNameList(qualified));

	/* topic */
	d = slot_getattr(slot, CONSUMER_ATTR_TOPIC, &isnull);
	consumer->topic = TextDatumGetCString(d);

	/* format */
	d = slot_getattr(slot, CONSUMER_ATTR_FORMAT, &isnull);
	consumer->format = TextDatumGetCString(d);

	/* delimiter */
	d = slot_getattr(slot, CONSUMER_ATTR_DELIMITER, &isnull);
	consumer->delimiter = TextDatumGetCString(d);

	/* now load all brokers */
	consumer->brokers = get_all_brokers();
	MemoryContextSwitchTo(old);

	d = slot_getattr(slot, CONSUMER_ATTR_PARALLELISM, &isnull);
	consumer->parallelism = DatumGetInt32(d);

	/* batch size */
	d = slot_getattr(slot, CONSUMER_ATTR_BATCH_SIZE, &isnull);
	consumer->batch_size = DatumGetInt32(d);

	ExecDropSingleTupleTableSlot(slot);
	heap_endscan(scan);
	heap_close(consumers, NoLock);
}

/*
 * copy_next
 */
static int
copy_next(void *args, void *buf, int minread, int maxread)
{
	StringInfo messages = (StringInfo) args;
	int remaining = messages->len - messages->cursor;
	int read = 0;

	if (maxread <= remaining)
		read = maxread;
	else
		read = remaining;

	if (read == 0)
		return 0;

	memcpy(buf, messages->data + messages->cursor, read);
	messages->cursor += read;

	return read;
}


/*
 * save_consumer_state
 *
 * Saves the given consumer's state to pipeline_kafka_consumers
 */
static void
save_consumer_state(KafkaConsumer *consumer, int partition_group)
{
	ScanKeyData skey[1];
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	Relation offsets = open_pipeline_kafka_offsets();
	Datum values[OFFSETS_RELATION_NATTS];
	bool nulls[OFFSETS_RELATION_NATTS];
	bool replace[OFFSETS_RELATION_NATTS];
	bool updated[consumer->num_partitions];
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(offsets));
	int partition;

	MemSet(updated, false, sizeof(updated));

	ScanKeyInit(&skey[0], OFFSETS_ATTR_CONSUMER, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(consumer->id));
	scan = heap_beginscan(offsets, GetTransactionSnapshot(), 1, skey);

	/* update any existing offset rows */
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum d;
		bool isnull;
		int partition;
		HeapTuple modified;

		ExecStoreTuple(tup, slot, InvalidBuffer, false);
		d = slot_getattr(slot, OFFSETS_ATTR_PARTITION, &isnull);
		partition = DatumGetInt32(d);

		/* we only want to update the offsets we're responsible for */
		if (partition % consumer->parallelism != partition_group)
			continue;

		MemSet(nulls, false, sizeof(nulls));
		MemSet(replace, false, sizeof(nulls));

		values[OFFSETS_ATTR_OFFSET - 1] = Int64GetDatum(consumer->offsets[partition]);
		replace[OFFSETS_ATTR_OFFSET - 1] = true;
		updated[partition] = true;

		modified = heap_modify_tuple(tup, RelationGetDescr(offsets), values, nulls, replace);
		simple_heap_update(offsets, &modified->t_self, modified);
	}

	heap_endscan(scan);

	/* now insert any offset rows that didn't already exist */
	for (partition = 0; partition < consumer->num_partitions; partition++)
	{
		if (updated[partition])
			continue;

		if (partition % consumer->parallelism != partition_group)
			continue;

		values[OFFSETS_ATTR_CONSUMER - 1] = ObjectIdGetDatum(consumer->id);
		values[OFFSETS_ATTR_PARTITION - 1] = Int32GetDatum(partition);
		values[OFFSETS_ATTR_OFFSET - 1] = Int64GetDatum(consumer->offsets[partition]);

		MemSet(nulls, false, sizeof(nulls));

		tup = heap_form_tuple(RelationGetDescr(offsets), values, nulls);
		simple_heap_insert(offsets, tup);
	}

	ExecDropSingleTupleTableSlot(slot);
	heap_close(offsets, NoLock);
}

/*
 * get_copy_statement
 *
 * Get the COPY statement that will be used to write messages to a stream
 */
static CopyStmt *
get_copy_statement(KafkaConsumer *consumer)
{
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	CopyStmt *stmt = makeNode(CopyStmt);
	Relation rel;
	TupleDesc desc;
	DefElem *delim = makeNode(DefElem);
	DefElem *format = makeNode(DefElem);
	int i;

	stmt->relation = consumer->rel;
	stmt->filename = NULL;
	stmt->options = NIL;
	stmt->is_from = true;
	stmt->query = NULL;
	stmt->attlist = NIL;

	rel = heap_openrv(consumer->rel, AccessShareLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		/*
		 * Users can't supply values for arrival_timestamp, so make
		 * sure we exclude it from the copy attr list
		 */
		char *name = NameStr(desc->attrs[i]->attname);
		if (IsStream(RelationGetRelid(rel)) && pg_strcasecmp(name, ARRIVAL_TIMESTAMP) == 0)
			continue;
		stmt->attlist = lappend(stmt->attlist, makeString(name));
	}

	if (pg_strcasecmp(consumer->format, FORMAT_CSV) == 0)
	{
		delim->defname = OPTION_DELIMITER;
		delim->arg = (Node *) makeString(consumer->delimiter);
		stmt->options = lappend(stmt->options, delim);
	}

	format->defname = OPTION_FORMAT;
	format->arg = (Node *) makeString(consumer->format);
	stmt->options = lappend(stmt->options, format);

	heap_close(rel, NoLock);

	MemoryContextSwitchTo(old);

	return stmt;
}

/*
 * execute_copy
 *
 * Write messages to stream
 */
static uint64
execute_copy(CopyStmt *stmt, StringInfo buf)
{
	uint64 processed;

	copy_iter_hook = copy_next;
	copy_iter_arg = buf;

	DoCopy(stmt, "COPY", &processed);

	return processed;
}

/*
 * kafka_consume_main
 *
 * Main function for Kafka consumers running as background workers
 */
void
kafka_consume_main(Datum arg)
{
	char err_msg[512];
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_t *kafka;
	rd_kafka_topic_t *topic;
	rd_kafka_message_t **messages;
	const struct rd_kafka_metadata *meta;
	struct rd_kafka_metadata_topic topic_meta;
	rd_kafka_resp_err_t err;
	bool found;
	Oid id = (Oid) arg;
	ListCell *lc;
	KafkaConsumerProc *proc = hash_search(consumer_procs, &id, HASH_FIND, &found);
	KafkaConsumer consumer;
	CopyStmt *copy;
	int valid_brokers = 0;
	int i;
	int my_partitions = 0;

	if (!found)
		elog(ERROR, "kafka consumer %d not found", id);

	pqsignal(SIGTERM, kafka_consume_main_sigterm);

	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* give this proc access to the database */
	BackgroundWorkerInitializeConnection(NameStr(proc->dbname), NULL);

	/* load saved consumer state */
	StartTransactionCommand();
	load_consumer_state(proc->consumer_id, &consumer);
	copy = get_copy_statement(&consumer);

	topic_conf = rd_kafka_topic_conf_new();
	kafka = rd_kafka_new(RD_KAFKA_CONSUMER, NULL, err_msg, sizeof(err_msg));
	rd_kafka_set_logger(kafka, logger);

	/*
	 * Add all brokers currently in pipeline_kafka_brokers
	 */
	if (consumer.brokers == NIL)
		elog(ERROR, "no valid brokers were found");

	foreach(lc, consumer.brokers)
		valid_brokers += rd_kafka_brokers_add(kafka, lfirst(lc));

	if (!valid_brokers)
		elog(ERROR, "no valid brokers were found");

	/*
	 * Set up our topic to read from
	 */
	topic = rd_kafka_topic_new(kafka, consumer.topic, topic_conf);
	err = rd_kafka_metadata(kafka, false, topic, &meta, CONSUMER_TIMEOUT);

  if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		elog(ERROR, "failed to acquire metadata: %s", rd_kafka_err2str(err));

  Assert(meta->topic_cnt == 1);
  topic_meta = meta->topics[0];

  load_consumer_offsets(&consumer, &topic_meta);
  CommitTransactionCommand();

  /*
   * Begin consuming all partitions that this process is responsible for
   */
  for (i = 0; i < topic_meta.partition_cnt; i++)
  {
  	int partition = topic_meta.partitions[i].id;

  	Assert(partition <= consumer.num_partitions);
  	if (partition % consumer.parallelism != proc->partition_group)
  		continue;

		elog(LOG, "[kafka consumer] %s <- %s consuming partition %d from offset %ld",
				consumer.rel->relname, consumer.topic, partition, consumer.offsets[partition]);

  	if (rd_kafka_consume_start(topic, partition, consumer.offsets[partition]) == -1)
  		elog(ERROR, "failed to start consuming: %s", rd_kafka_err2str(rd_kafka_errno2err(errno)));

  	my_partitions++;
  }

  /*
   * No point doing anything if we don't have any partitions assigned to us
   */
  if (my_partitions == 0)
  {
		elog(LOG, "[kafka consumer] %s <- %s consumer %d doesn't have any partitions to read from",
				consumer.rel->relname, consumer.topic, MyProcPid);
		goto done;
  }

	messages = palloc0(sizeof(rd_kafka_message_t) * consumer.batch_size);

	/*
	 * Consume messages until we are terminated
	 */
	while (!got_sigterm)
	{
		ssize_t num_consumed;
		int i;
		int messages_buffered = 0;
		int partition;
		StringInfoData buf;
		bool xact = false;

		for (partition = 0; partition < consumer.num_partitions; partition++)
		{
			if (partition % consumer.parallelism != proc->partition_group)
				continue;

			num_consumed = rd_kafka_consume_batch(topic, partition,
					CONSUMER_TIMEOUT, messages, consumer.batch_size);

			if (num_consumed <= 0)
				continue;

			if (!xact)
			{
				StartTransactionCommand();
				xact = true;
			}

			initStringInfo(&buf);
			for (i = 0; i < num_consumed; i++)
			{
				if (messages[i]->payload != NULL)
				{
					appendBinaryStringInfo(&buf, messages[i]->payload, messages[i]->len);
					if (buf.data[buf.len] != '\n')
						appendStringInfoChar(&buf, '\n');
					messages_buffered++;
				}
				consumer.offsets[partition] = messages[i]->offset;
				rd_kafka_message_destroy(messages[i]);
			}
		}

		if (!xact)
		{
			pg_usleep(1 * 1000);
			continue;
		}

		/* we don't want to die in the event of any errors */
		PG_TRY();
		{
			if (messages_buffered)
				execute_copy(copy, &buf);
		}
		PG_CATCH();
		{
			elog(LOG, "[kafka consumer] %s <- %s failed to process batch, dropped %d message%s:",
					consumer.rel->relname, consumer.topic, (int) num_consumed, (num_consumed == 1 ? "" : "s"));
			EmitErrorReport();
			FlushErrorState();

			AbortCurrentTransaction();
			xact = false;
		}
		PG_END_TRY();

		if (!xact)
			StartTransactionCommand();

		if (messages_buffered)
			save_consumer_state(&consumer, proc->partition_group);

		CommitTransactionCommand();
	}

done:

  hash_search(consumer_procs, &id, HASH_REMOVE, NULL);

  rd_kafka_topic_destroy(topic);
  rd_kafka_destroy(kafka);
  rd_kafka_wait_destroyed(CONSUMER_TIMEOUT);
}

/*
 * create_consumer
 *
 * Create a row in pipeline_kafka_consumers representing a topic-relation consumer
 */
static Oid
create_consumer(Relation consumers, text *relation, text *topic,
		text *format, text *delimiter, int batchsize, int parallelism)
{
	HeapTuple tup;
	Datum values[CONSUMER_RELATION_NATTS];
	bool nulls[CONSUMER_RELATION_NATTS];
	Oid oid;
	ScanKeyData skey[2];
	HeapScanDesc scan;

	MemSet(nulls, false, sizeof(nulls));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(relation));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(topic));

	scan = heap_beginscan(consumers, GetTransactionSnapshot(), 2, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
	{
		/* consumer already exists, so just update it with the given parameters */
		bool replace[CONSUMER_RELATION_NATTS];

		MemSet(replace, true, sizeof(nulls));
		replace[CONSUMER_ATTR_RELATION - 1] = false;
		replace[CONSUMER_ATTR_TOPIC - 1] = false;

		values[CONSUMER_ATTR_BATCH_SIZE - 1] = Int32GetDatum(batchsize);
		values[CONSUMER_ATTR_PARALLELISM - 1] = Int32GetDatum(parallelism);
		values[CONSUMER_ATTR_FORMAT - 1] = PointerGetDatum(format);
		values[CONSUMER_ATTR_DELIMITER - 1] = PointerGetDatum(delimiter);

		tup = heap_modify_tuple(tup, RelationGetDescr(consumers), values, nulls, replace);
		simple_heap_update(consumers, &tup->t_self, tup);

		oid = HeapTupleGetOid(tup);
	}
	else
	{
		/* consumer doesn't exist yet, create it with the given parameters */
		values[CONSUMER_ATTR_RELATION - 1] = PointerGetDatum(relation);
		values[CONSUMER_ATTR_TOPIC - 1] = PointerGetDatum(topic);
		values[CONSUMER_ATTR_BATCH_SIZE - 1] = Int32GetDatum(batchsize);
		values[CONSUMER_ATTR_PARALLELISM - 1] = Int32GetDatum(parallelism);
		values[CONSUMER_ATTR_FORMAT - 1] = PointerGetDatum(format);
		values[CONSUMER_ATTR_DELIMITER - 1] = PointerGetDatum(delimiter);

		tup = heap_form_tuple(RelationGetDescr(consumers), values, nulls);
		oid = simple_heap_insert(consumers, tup);
	}

	heap_endscan(scan);

	CommandCounterIncrement();

	return oid;
}

/*
 * get_consumer_id
 *
 * Get the pipeline_kafka_consumers oid for the given relation-topic pair
 *
 */
static Oid
get_consumer_id(Relation consumers, text *relation, text *topic)
{
	ScanKeyData skey[2];
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	Oid oid = InvalidOid;

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(relation));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(topic));

	scan = heap_beginscan(consumers, GetTransactionSnapshot(), 2, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
		oid = HeapTupleGetOid(tup);

	heap_endscan(scan);

	return oid;
}

/*
 * launch_consumer_group
 *
 * Launch a group of background worker process that will consume from the given topic
 * into the given relation
 */
static bool
launch_consumer_group(Relation consumers, Oid consumer_id)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	KafkaConsumerGroup *group;
	KafkaConsumer consumer;
	bool found;
	int i;

	group = (KafkaConsumerGroup *) hash_search(consumer_groups, &consumer_id, HASH_ENTER, &found);
	if (found)
	{
		KafkaConsumerProc *proc;
		HASH_SEQ_STATUS iter;
		bool running = false;

		hash_seq_init(&iter, consumer_procs);
		while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
		{
			if (proc->consumer_id == consumer_id)
				running = true;
		}

		/* if there are already procs running, it's a noop */
		if (running)
			return true;

		/* no procs actually running, so it's ok to launch new ones */
	}

	load_consumer_state(consumer_id, &consumer);
	group->parallelism = consumer.parallelism;

	for (i = 0; i < group->parallelism; i++)
	{
		/* we just need any unique OID here */
		Oid id = GetNewOid(consumers);
		KafkaConsumerProc *proc;

		proc = (KafkaConsumerProc *) hash_search(consumer_procs, &id, HASH_ENTER, &found);
		if (found)
			continue;

		worker.bgw_main_arg = DatumGetObjectId(id);
		worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		worker.bgw_main = NULL;
		worker.bgw_notify_pid = 0;

		/* this module is loaded dynamically, so we can't use bgw_main */
		sprintf(worker.bgw_library_name, PIPELINE_KAFKA_LIB);
		sprintf(worker.bgw_function_name, KAFKA_CONSUME_MAIN);
		snprintf(worker.bgw_name, BGW_MAXLEN, "[kafka consumer] %s <- %s", consumer.rel->relname, consumer.topic);

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
			return false;

		proc->consumer_id = consumer_id;
		proc->worker = *handle;
		proc->partition_group = i;
		namestrcpy(&proc->dbname, get_database_name(MyDatabaseId));
	}

	return true;
}

/*
 * kafka_consume_begin_tr
 *
 * Begin consuming messages from the given topic into the given relation
 */
PG_FUNCTION_INFO_V1(kafka_consume_begin_tr);
Datum
kafka_consume_begin_tr(PG_FUNCTION_ARGS)
{
	text *topic;
	text *qualified;
	RangeVar *relname;
	Relation rel;
	Relation consumers;
	Oid id;
	bool result;

	/* these all have defaults */
	text *format = PG_GETARG_TEXT_P(2);
	text *delimiter = PG_GETARG_TEXT_P(3);
	int batchsize = PG_GETARG_INT32(4);
	int parallelism = PG_GETARG_INT32(5);

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "relation cannot be null");

	topic = PG_GETARG_TEXT_P(0);
	qualified = PG_GETARG_TEXT_P(1);

	/* there's no point in progressing if there aren't any brokers */
	if (!get_all_brokers())
		elog(ERROR, "add at least one broker with kafka_add_broker");

	/* verify that the target relation actually exists */
	relname = makeRangeVarFromNameList(textToQualifiedNameList(qualified));
	rel = heap_openrv(relname, NoLock);

	if (IsStream(RelationGetRelid(rel)) && IsInferredStream(RelationGetRelid(rel)))
		ereport(ERROR,
				(errmsg("target stream must be static"),
						errhint("Use CREATE STREAM to create a stream that can consume a Kafka topic.")));

	heap_close(rel, NoLock);

	consumers = open_pipeline_kafka_consumers();
	id = create_consumer(consumers, qualified, topic, format, delimiter, batchsize, parallelism);

	result = launch_consumer_group(consumers, id);
	heap_close(consumers, NoLock);

	if (result)
		RETURN_SUCCESS();
	else
		RETURN_FAILURE();
}

/*
 * kafka_consume_end_tr
 *
 * Stop consuming messages from the given topic into the given relation
 */
PG_FUNCTION_INFO_V1(kafka_consume_end_tr);
Datum
kafka_consume_end_tr(PG_FUNCTION_ARGS)
{
	text *topic;
	text *qualified;
	Relation consumers;
	Oid id;
	bool found;
	HASH_SEQ_STATUS iter;
	KafkaConsumerProc *proc;

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "relation cannot be null");

	topic = PG_GETARG_TEXT_P(0);
	qualified = PG_GETARG_TEXT_P(1);
	consumers = open_pipeline_kafka_consumers();

	id = get_consumer_id(consumers, qualified, topic);
	if (!OidIsValid(id))
		elog(ERROR, "there are no consumers for that topic and relation");

	hash_search(consumer_groups, &id, HASH_REMOVE, &found);
	if (!found)
		elog(ERROR, "no consumer processes are running for that topic and relation");

	hash_seq_init(&iter, consumer_procs);
	while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
	{
		if (proc->consumer_id != id)
			continue;

		TerminateBackgroundWorker(&proc->worker);
		hash_search(consumer_procs, &id, HASH_REMOVE, NULL);
	}

	heap_close(consumers, NoLock);
	RETURN_SUCCESS();
}

/*
 * kafka_consume_begin_all
 *
 * Start all consumers
 */
PG_FUNCTION_INFO_V1(kafka_consume_begin_all);
Datum
kafka_consume_begin_all(PG_FUNCTION_ARGS)
{
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	Relation consumers = open_pipeline_kafka_consumers();

	scan = heap_beginscan(consumers, GetTransactionSnapshot(), 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid id = HeapTupleGetOid(tup);

		if (!launch_consumer_group(consumers, id))
			RETURN_FAILURE();
	}

	heap_endscan(scan);
	heap_close(consumers, NoLock);

	RETURN_SUCCESS();
}

/*
 * kafka_consume_end_all
 *
 * Stop all consumers
 */
PG_FUNCTION_INFO_V1(kafka_consume_end_all);
Datum
kafka_consume_end_all(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS iter;
	KafkaConsumerProc *proc;
	List *ids = NIL;
	ListCell *lc;

	hash_seq_init(&iter, consumer_procs);
	while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
	{
		TerminateBackgroundWorker(&proc->worker);
		hash_search(consumer_groups, &proc->consumer_id, HASH_REMOVE, NULL);
		ids = lappend_oid(ids, proc->id);
	}

	foreach(lc, ids)
	{
		Oid id = lfirst_oid(lc);
		hash_search(consumer_procs, &id, HASH_REMOVE, NULL);
	}

	RETURN_SUCCESS();
}

/*
 * kafka_add_broker
 *
 * Add a broker
 */
PG_FUNCTION_INFO_V1(kafka_add_broker);
Datum
kafka_add_broker(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	Datum values[1];
	bool nulls[1];
	Relation brokers;
	text *host;
	ScanKeyData skey[1];
	HeapScanDesc scan;

	if (PG_ARGISNULL(0))
		elog(ERROR, "broker cannot be null");

	host = PG_GETARG_TEXT_P(0);
	brokers = open_pipeline_kafka_brokers();

	/* don't allow duplicate brokers */
	ScanKeyInit(&skey[0], BROKER_ATTR_HOST, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(host));
	scan = heap_beginscan(brokers, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
	{
		heap_endscan(scan);
		heap_close(brokers, NoLock);
		elog(ERROR, "broker %s already exists", TextDatumGetCString(host));
	}

	/* broker host */
	values[0] = PointerGetDatum(host);

	MemSet(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(RelationGetDescr(brokers), values, nulls);
	simple_heap_insert(brokers, tup);

	heap_endscan(scan);
	heap_close(brokers, NoLock);

	RETURN_SUCCESS();
}

/*
 * kafka_remove_broker
 *
 * Remove a broker
 */
PG_FUNCTION_INFO_V1(kafka_remove_broker);
Datum
kafka_remove_broker(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	Relation brokers;
	text *host;
	ScanKeyData skey[1];
	HeapScanDesc scan;

	if (PG_ARGISNULL(0))
		elog(ERROR, "broker cannot be null");

	host = PG_GETARG_TEXT_P(0);
	brokers = open_pipeline_kafka_brokers();

	/* don't allow duplicate brokers */
	ScanKeyInit(&skey[0], BROKER_ATTR_HOST, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(host));
	scan = heap_beginscan(brokers, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "broker %s does not exist", TextDatumGetCString(host));

	simple_heap_delete(brokers, &tup->t_self);

	heap_endscan(scan);
	heap_close(brokers, NoLock);

	RETURN_SUCCESS();
}

