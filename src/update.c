/*-------------------------------------------------------------------------
 *
 * update.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
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
#include "config.h"
#include "lib/stringinfo.h"
#include "scheduler.h"
#include "stats.h"
#include "update.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define ANON_ENDPOINT		"anonymous.pipelinedb.com"
#define ANON_PORT 80

#define UPDATE_AVAILABLE 201
#define NO_UPDATES 200

#define CHECK_PATH "/check"
#define REQUEST_FMT "POST %s HTTP/1.1\r\nHost: anonymous.pipelinedb.com\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s"
#define PROC_PAYLOAD_FMT 	"{ \"e\": \"%c\", \"v\": \"%s\", \"r\": \"%s\", \"s\": \"%s\", \"sr\": \"%s\", \"sv\": \"%s\", \"ri\": %ld, \"ba\": %ld, \"bi\": %ld, \"er\": %ld, \"cq\": %d, \"id\": \"%s\", \"pg\": \"%s\", \"nw\": %d, \"nc\": %d}"

#define FAIL(sock, req) \
	do { \
		if (req) \
			pfree(req); \
		close(sock); \
		return -1; \
	} while (0);

/* guc */
bool anonymous_update_checks;

GetInstallationIdFunc GetInstallationIdHook = NULL;
VerifyConfigurationFunc VerifyConfigurationHook = NULL;

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
int
AnonymousPost(char *path, char *payload, char *resp_buf, int resp_len)
{
	struct hostent *server;
	struct sockaddr_in serv_addr;
	int sock;
	int bytes;
	int sent;
	int received;
	int total;
	StringInfoData buf;

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
		FAIL(sock, NULL);

	initStringInfo(&buf);
	appendStringInfo(&buf, REQUEST_FMT, path, (int) strlen(payload), payload);

	/* send request */
	sent = 0;
	do
	{
		bytes = write(sock, buf.data + sent, buf.len - sent);
		if (bytes < 0)
			FAIL(sock, buf.data);
		if (bytes == 0)
			break;
		sent += bytes;
	} while (sent < buf.len);

	/* receive response */
	MemSet(resp_buf, 0, resp_len);
	total = resp_len - 1;
	received = 0;

	do
	{
		bytes = read(sock, resp_buf + received, total - received);
		if (bytes < 0)
			FAIL(sock, buf.data);
		if (bytes == 0)
			break;
		received += bytes;
	} while (received < total);

	close(sock);
	pfree(buf.data);

	return parse_response_code(resp_buf);
}

/*
 * get_stats
 *
 * Aggregate all database-level stats
 */
static char *
get_stats(bool startup)
{
	StringInfoData payload;
	struct utsname mname;
	char name[64];
	char etype = startup ? 's' : 'h';
	char *id = "";
	uint64 rows_in = 0;
	uint64 batches_in = 0;
	uint64 bytes_in = 0;
	uint64 errors = 0;
	int cqcount = 0;
	int ccount = 0;
	int wcount = 0;

	GetInstallationStats(&rows_in, &batches_in, &bytes_in, &errors, &cqcount, &ccount, &wcount);

	uname(&mname);
	strncpy(name, mname.sysname, 64);

	if (GetInstallationIdHook != NULL)
		id = GetInstallationIdHook();

	initStringInfo(&payload);
	appendStringInfo(&payload, PROC_PAYLOAD_FMT, etype,
			pipeline_version_str, pipeline_revision_str, name, mname.release, mname.version, rows_in, batches_in,
			bytes_in, errors, cqcount, id, PG_MAJORVERSION, wcount, ccount);

	return payload.data;
}

/*
 * UpdateCheck
 *
 * Anonymously check if any software updates are available and report some anonymized aggregate statistics
 */
void
UpdateCheck(bool startup)
{
	char *stats = get_stats(startup);
	char buf[4096];

	if (AnonymousPost(CHECK_PATH, stats, buf, sizeof(buf)) == UPDATE_AVAILABLE)
		elog(NOTICE, "a newer version of PipelineDB is available");

	pfree(stats);
}
