/*-------------------------------------------------------------------------
 *
 * triggerfuncs.c
 *	  Functions to call from continuous triggers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "postgres.h"

#include "commands/trigger.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "pipeline/trigger/trigger.h"
#include "pipeline/trigger/triggerfuncs.h"
#include "pipeline/trigger/tuple_formatter.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "storage/shmem.h"

static int fd = -1;
static const char *log_file_name = "/tmp/.pipelinedb_trigger_test.log";

static void check_file()
{
	if (fd == -1)
	{
		fd = open(log_file_name, O_RDWR | O_APPEND, 0600);
		Assert(fd != -1);
	}
}

static void
write_to_file(StringInfo info)
{
	struct iovec iov;
	check_file();

	appendStringInfoChar(info, '\n');

	iov.iov_base = info->data;
	iov.iov_len = info->len;

	/* Use writev to atomically write the tuple to the file */

	writev(fd, &iov, 1);
}

/*
 * send_alert_new_row
 *
 * Converts the 'new' tuple to copy format, and sends to the alert server
 */
Datum
pipeline_send_alert_new_row(PG_FUNCTION_ARGS)
{
	if (IsContQueryTriggerProcess())
	{
		HeapTuple new_tup = NULL;
		TriggerData *data = (TriggerData *) (fcinfo->context);

		TupleFormatter *f = get_formatter(RelationGetRelid(data->tg_relation));
		resetStringInfo(f->buf);

		new_tup = TRIGGER_FIRED_BY_INSERT(data->tg_event) ?
			data->tg_trigtuple : data->tg_newtuple;

		tf_write_tuple(f, f->slot, f->buf, new_tup);
		alert_server_push(MyAlertServer, data->tg_trigger->tgoid, f->buf);
	}
	else
		elog(ERROR, "send_alert_new_row: not called from pipelinedb trigger process");

	PG_RETURN_NULL();
}

Datum
pipeline_test_alert_new_row(PG_FUNCTION_ARGS)
{
	if (IsContQueryTriggerProcess())
	{
		HeapTuple new_tup = NULL;
		TriggerData *data = (TriggerData *) (fcinfo->context);

		TupleFormatter *f = get_formatter(RelationGetRelid(data->tg_relation));
		Assert(f);
		resetStringInfo(f->buf);

		new_tup = TRIGGER_FIRED_BY_INSERT(data->tg_event) ?
			data->tg_trigtuple : data->tg_newtuple;

		tf_write_tuple(f, f->slot, f->buf, new_tup);
		alert_server_push(MyAlertServer, data->tg_trigger->tgoid, f->buf);
		write_to_file(f->buf);
	}
	else
		elog(ERROR, "test_alert_new_row: not called from pipelinedb trigger process");

	PG_RETURN_NULL();
}

Datum
pipeline_get_alert_server_conn(PG_FUNCTION_ARGS)
{
	StringInfo info = makeStringInfo();

	ContQueryDatabaseMetadata *data =
		GetContQueryDatabaseMetadata(MyDatabaseId);

	if (data && data->alert_server_port)
		appendStringInfo(info, "tcp:%s:%d", ListenAddresses, data->alert_server_port);

	PG_RETURN_TEXT_P(CStringGetTextDatum(info->data));
}

static void
trigger_testing_setup()
{
	/* test fixtures use the same db instance */

	if (fd != -1)
	{
		close(fd);
		fd = -1;
	}

	fd = open(log_file_name, O_RDWR | O_TRUNC | O_CREAT, 0600);
}

PG_FUNCTION_INFO_V1(pipeline_trigger_debug);
Datum
pipeline_trigger_debug(PG_FUNCTION_ARGS)
{
	trigger_testing_setup();
	PG_RETURN_BOOL(true);
}
