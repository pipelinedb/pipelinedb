/*-------------------------------------------------------------------------
 *
 * extended_inserts.c
 *
 *  Writes to streams using the extended protocol for testing

 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int
main(int argc, char **argv)
{
	PGconn *conn;
	PGresult *res;
	int i;
	char connstr[256];
	char *stream;
	int nrows;
	char *vals[3];
	char buf[1024];

	srand(time(NULL));

	if (argc != 5)
	{
		fprintf(stderr, "Usage: extended <database> <port> <stream> <# rows>\n");
		exit(1);
	}

	sprintf(connstr, "dbname=%s port=%s host=localhost", argv[1], argv[2]);
	stream = argv[3];
	nrows = atoi(argv[4]);

	conn = PQconnectdb(connstr);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "connection to database failed: %s", PQerrorMessage(conn));
		exit(1);
	}

	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
		exit(1);
	}

	PQclear(res);

	sprintf(buf, "INSERT INTO %s (x, y, z) VALUES ($1, $2, $3)", stream);
	if ((res = PQprepare(conn, "params", buf, 3, NULL)) == NULL)
		fprintf(stderr, "PREPARE command failed: %s", PQerrorMessage(conn));

	for (i = 0; i < nrows; i++)
	{
		char x[16];
		char y[16];
		char z[16];

		sprintf(x, "%d", rand() % 100);
		sprintf(y, "%d", rand() % 100);
		sprintf(z, "%d", rand() % 100);

		vals[0] = x;
		vals[1] = y;
		vals[2] = z;

		res =  PQexecPrepared(conn, "params", 3, (const char * const *) vals, NULL, NULL, 0);
	}

	/* now perform unparamaterized inserts */
	sprintf(buf, "INSERT INTO %s (x, y, z) VALUES (0, 0, 0)", stream);
	if ((res = PQprepare(conn, "no params", buf, 0, NULL)) == NULL)
		fprintf(stderr, "PREPARE command failed: %s", PQerrorMessage(conn));

	for (i = 0; i < nrows; i++)
		res =  PQexecPrepared(conn, "no params", 0, NULL, NULL, NULL, 0);

	res = PQexec(conn, "END");
	PQclear(res);
	PQfinish(conn);

	return 0;
}
