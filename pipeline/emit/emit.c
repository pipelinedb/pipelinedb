/* Copyright (c) 2013-2015 PipelineDB */
#include <time.h>
#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"

#define MAX_BUF_SIZE 6 * 1024 * 1024

static void usage(void);

static void
usage(void)
{
	printf("Usage:\n\
  emit <host> <port> <database name> <user> <encoding> <stream>\n");
}


/*
 * Sends raw events to the backend
 */
int main(int argc, char* argv[])
{
	char *host;
	char *dbname;
	char *user;
	char *encoding;
	char *stream;
	char *port;
	char connectstr[64];
	char *line = NULL;
  int status = 0;
	int len = 0;
	char buf[MAX_BUF_SIZE];
	int pos;
	size_t buf_size;
	PGconn *conn;
	PGresult *res;
	size_t size;

	if (argc < 7)
	{
		usage();
		exit(1);
	}

	host = argv[1];
	port = argv[2];
	dbname = argv[3];
	user = argv[4];
	encoding = argv[5];
	stream = argv[6];
	sprintf(connectstr, "host='%s' dbname='%s' user='%s' port=%s", host, dbname, user, port);
	conn = PQconnectdb(connectstr);
	if (conn == NULL)
	{
		printf("could not connect\n");
		exit(1);
	}
	switch (PQstatus(conn))
	{
		case CONNECTION_OK:
			break;
		default:
			printf("error connecting to database\n");
			PQfinish(conn);
			exit(1);
	}

  res = PQexec(conn, "BEGIN");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
  	printf("error beginning transaction\n");
  	PQfinish(conn);
  	PQclear(res);
  	exit(1);
  }

  pos = 0;
  buf_size = 0;

	/* Read in event data from stdin. Each line is a separate event */
	while (getline(&line, &size, stdin) > 0)
	{
		int nlen;
		len = strlen(line);
		line[len - 1] = '\0';
		len--;
		if (buf_size + len > MAX_BUF_SIZE)
		{
			/* flush buffer to server */
			if (PQsendEvents(encoding, stream, buf, buf_size, conn) != 0)
				printf("error sending %s\n", line);

			/* reset buffer */
			memset(buf, 0, MAX_BUF_SIZE);
			pos = 0;
			buf_size = 0;
		}

		nlen = htonl(len);
		memcpy(buf + pos, &nlen, 4);
		pos += 4;
		memcpy(buf + pos, line, len);
		pos += len;
		buf_size += 4 + len;
	}

	if (PQsendEvents(encoding, stream, buf, buf_size, conn) != 0)
		printf("error sending %s\n", line);

	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		printf("error committing transaction\n");
		status = 1;
	}

	PQfinish(conn);
	free(line);

	return status;
}
