/*-------------------------------------------------------------------------
 *
 * update.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/update.c
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <sys/utsname.h>

#include "postgres.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream.h"
#include "lib/stringinfo.h"
#include "pipeline/update.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pipelinefuncs.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define ANON_ENDPOINT		"anonymous.pipelinedb.com"
#define ANON_PORT 80

#define UPDATE_AVAILABLE 201
#define NO_UPDATES 200

#define REQUEST_FMT "POST /check HTTP/1.1\r\nHost: anonymous.pipelinedb.com\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s"
#define PROC_PAYLOAD_FMT 	"{ \"t\": \"%c\", \"e\": \"%s\", \"v\": \"%s\", \"s\": \"%s\", \"sr\": \"%s\", \"sv\": \"%s\", \"ri\": %ld, \"bi\": %ld, \"ro\": %ld, \"bo\": %ld, \"er\": %ld, \"cc\": %ld, \"cd\": %ld}"
#define PAYLOAD_FMT "[%s,%s]"

#define FAIL(sock) \
	do { \
		close(sock); \
		return -1; \
	} while (0);

/* guc */
bool anonymous_update_checks;

/*
 * parse_response_code
 *
 * Parse the response code out of a raw HTTP response
 */
static int
parse_response_code(char *resp)
{
	int offset = 0;
	int max = strlen(resp);
	char *start;
	int len;
	char parsed[8];

	/* move to first SP */
	while (offset < max)
	{
		if (resp[offset] == ' ')
			break;
		if (offset++ == max)
			return -1;
	}

	offset++;
	start = resp + offset;

	/* move to second SP */
	while (offset < max)
	{
		if (resp[offset] == ' ')
			break;
		if (offset++ == max)
			return -1;
	}

	/* read everything in between */
	len = (resp + offset - start + 1);
	memcpy(parsed, start, len);
	parsed[strlen(parsed) - 1] = '\0';

	return atoi(parsed);
}

/*
 * POST the HTTP request
 */
static int
post(char *payload)
{
	struct hostent *server;
	struct sockaddr_in serv_addr;
	int sock;
	int bytes;
	int sent;
	int received;
	int total;
	char response[4096];
	StringInfo buf = makeStringInfo();

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
		return -1;

	server = gethostbyname(ANON_ENDPOINT);
	if (server == NULL)
		return -1;

	MemSet(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(ANON_PORT);
	memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);

	if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
		FAIL(sock);

	appendStringInfo(buf, REQUEST_FMT, (int) strlen(payload), payload);

	/* send request */
	sent = 0;
	do
	{
		bytes = write(sock, buf->data + sent, buf->len - sent);
		if (bytes < 0)
			FAIL(sock);
		if (bytes == 0)
			break;
		sent += bytes;
	} while (sent < buf->len);

	/* receive response */
	MemSet(response, 0, sizeof(response));
	total = sizeof(response) - 1;
	received = 0;

	do
	{
		bytes = read(sock, response + received, total - received);
		if (bytes < 0)
			FAIL(sock);
		if (bytes == 0)
			break;
		received += bytes;
	} while (received < total);

	close(sock);

	return parse_response_code(response);
}

/*
 * get_stats
 *
 * Aggregate all database-level stats
 */
static char *
get_stats(HTAB *all_dbs, bool startup, CQStatsType ptype)
{
	HASH_SEQ_STATUS db_iter;
	PgStat_StatDBEntry *db_entry;
	HTAB *stats;
	CQStatEntry *g;
	StringInfoData payload;
	struct utsname mname;
	char name[64];
	char proc = ptype == CQ_STAT_COMBINER ? 'c' : 'w';
	char *etype = startup ? "s" : "h";
	long rows_in = 0;
	long bytes_in = 0;
	long rows_out = 0;
	long bytes_out = 0;
	long cv_create = 0;
	long cv_drop = 0;
	long errors = 0;

	uname(&mname);
	strncpy(name, mname.sysname, 64);

	stats = cq_stat_fetch_all();
	if (stats == NULL)
		return NULL;

	hash_seq_init(&db_iter, all_dbs);
	while ((db_entry = (PgStat_StatDBEntry *) hash_seq_search(&db_iter)) != NULL)
	{
		g = cq_stat_get_global(db_entry->cont_queries, ptype);
		if (g == NULL || g->start_ts == 0)
			continue;

		rows_in += g->input_rows;
		bytes_in += g->input_bytes;
		rows_out += g->output_rows;
		bytes_out += g->output_bytes;
		cv_create += g->cv_create;
		cv_drop += g->cv_drop;
		errors += g->errors;
	}

	initStringInfo(&payload);
	appendStringInfo(&payload, PROC_PAYLOAD_FMT, proc, etype,
			PIPELINE_VERSION, name, mname.release, mname.version, rows_in,
			bytes_in, rows_out, bytes_out, errors, cv_create,
			cv_drop);

	return payload.data;
}

/*
 * UpdateCheck
 *
 * Check if any software updates are available and anonymously report the following information:
 *
 * 0) Operating system
 * 1) Number of continuous views
 * 2) Aggregate input/output rows/bytes
 */
void
UpdateCheck(HTAB *all_dbs, bool startup)
{
	StringInfoData payload;
	char *combiner = get_stats(all_dbs, startup, CQ_STAT_COMBINER);
	char *worker = get_stats(all_dbs, startup, CQ_STAT_WORKER);

	if (combiner == NULL)
		return;

	if (worker == NULL)
		worker = "{}";

	initStringInfo(&payload);
	appendStringInfo(&payload, PAYLOAD_FMT, combiner, worker);

	if (post(payload.data) == UPDATE_AVAILABLE)
		elog(NOTICE, "a newer version of PipelineDB is available");
}
