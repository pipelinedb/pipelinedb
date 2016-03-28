/*-------------------------------------------------------------------------
 *
 * triggerfuncs.c
 *	  Functions to call from continuous triggers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "triggerfuncs.h"
#include "storage/shmem.h"
#include "lib/stringinfo.h"
#include "commands/trigger.h"
#include "utils/rel.h"
#include "nodes/print.h"
#include "trigger.h"
#include "tuple_formatter.h"
#include "config.h"

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

static int fd = -1;
static const char *log_file_name = "/tmp/.pipelinedb_cluster_test.log";

static void check_file()
{
	if (fd == -1)
	{
		fd = open(log_file_name, O_RDWR | O_APPEND | O_CREAT, 0600);
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
PG_FUNCTION_INFO_V1(send_alert_new_row);
Datum
send_alert_new_row(PG_FUNCTION_ARGS)
{
	if (is_trigger_process)
	{
		TriggerData *data = (TriggerData *) (fcinfo->context);
		TupleFormatter *f = get_formatter(RelationGetRelid(data->tg_relation));

		resetStringInfo(f->buf);
		tf_write_tuple(f, f->slot, f->buf, data->tg_newtuple);

		alert_server_push(MyAlertServer, data->tg_trigger->tgoid, f->buf);
	}
	else
		elog(ERROR, "send_alert_new_row: not called from pipelinedb trigger process");

	PG_RETURN_NULL();
}

/*
 * test_alert_new_row
 *
 * Writes a tuple in copy format to /tmp/.pipelinedb_cluster_test.log, so
 * it can be picked up by test harnesses.
 *
 * We do this to get around WAL context limitations.
 */
PG_FUNCTION_INFO_V1(test_alert_new_row);
Datum
test_alert_new_row(PG_FUNCTION_ARGS)
{
	if (is_trigger_process)
	{
		TriggerData *data = (TriggerData *) (fcinfo->context);

		TupleFormatter *f =
			get_formatter(RelationGetRelid(data->tg_relation));

		resetStringInfo(f->buf);
		tf_write_tuple(f, f->slot, f->buf, data->tg_newtuple);

		write_to_file(f->buf);
	}
	else
		elog(ERROR, "test_alert_new_row: not called from pipelinedb trigger process");

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1( trigger_testing_setup );

extern Datum
trigger_testing_setup(PG_FUNCTION_ARGS)
{
	/* test fixtures use the same db instance */

	if (fd != -1)
	{
		close(fd);
		fd = -1;
	}

	fd = open(log_file_name, O_RDWR | O_TRUNC | O_CREAT, 0600);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1( trigger_testing_sync );

static void
read_file(StringInfo out, const char* fname)
{
	FILE *f = fopen(fname, "r");
	char tmp_buf[4096];

	resetStringInfo(out);

	if (!f)
		return;

	while (true)
	{
		size_t rb = fread(tmp_buf, 1, 4096, f);

		if (rb >= 0)
			appendBinaryStringInfo(out, tmp_buf, rb);

		if (rb < 4096)
			break;
	}

	fclose(f);
	return;
}

/*
 * hup_trigs
 *
 * Find all the trigger procs on this machine and SIGHUP them
 *
 * This is used in test harnesses and testing scripts to synchronize the
 * internal state of the trigger procs.
 */
static void
hup_trigger_procs()
{
   StringInfo cmdline;
   DIR *d;
   struct dirent *e;
   char fname[256];

   e = palloc0(sizeof(struct dirent));
   d = opendir("/proc");

   cmdline = makeStringInfo();

   while ((e = readdir(d)) != NULL)
   {
	   snprintf(fname, sizeof(fname), "/proc/%s/cmdline", e->d_name);
	   read_file(cmdline, fname);

	   if (strstr(cmdline->data, "pipelinedb_enterprise trigger"))
	   {
		   int pid = atoi(e->d_name);
		   kill(pid, SIGHUP);
	   }
   }

   closedir(d);
}

/*
 * trigger_testing_sync
 */
extern Datum
trigger_testing_sync(PG_FUNCTION_ARGS)
{
	hup_trigger_procs();
	PG_RETURN_NULL();
}
