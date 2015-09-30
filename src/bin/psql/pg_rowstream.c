#include "pg_rowstream.h"
#include "adhoc_util.h"

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

/*
 * Create a default initialized PGRowStream and returns a pointer to it.
 *
 * cb and ctx refer to the callback that will be fired when a row
 * has been parsed.
 *
 * self->fd will be set to stdin, and set to non blocking
 */

static void
handle_data(PGRowStream *stream, const char *buf);

PGRowStream *PGRowStreamInit(const char* sql, RowFunc cb, void *ctx)
{
	PGRowStream *self = pg_malloc0(sizeof(PGRowStream));
	PGresult *res = 0;

	const char *conninfo = "dbname=pipeline host=/tmp";

    self->conn = PQconnectdb(conninfo);

    if (PQstatus(self->conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(self->conn));
		exit(1);
    }

	self->callback = cb;
	self->cb_ctx = ctx;

	PQsetnonblocking(self->conn, true);
	res = PQexec(self->conn, sql);

	(void) (res);

	return self;
}

/*
 * Clean up the rowstream resources and pg_free s
 */
void
PGRowStreamDestroy(PGRowStream *s)
{
	PQrequestCancel(s->conn);
	usleep(100000);
	PQfinish(s->conn);

	memset(s, 0, sizeof(PGRowStream));
	pg_free(s);
}

int
PGRowStreamFd(PGRowStream *s)
{
	return PQsocket(s->conn);
}

/*
 * valid row types are h,k,i,u,d
 *
 * (mnemonics for header, key, insert, update, delete)
*/

typedef struct RowMessage
{
	int type;
	Row row;
} RowMessage;

static void chomp(char* line)
{
	size_t n = strlen(line);

	if (n == 0)
	{
		return;
	}

	if (line[n-1] == '\n')
	{
		line[n-1] = '\0';
	}
}

/*
 * Parse the simple text format into row data.
 *
 * Space delimited text, with row type in the first column, e.g.
 *
 * h a b c  		<- header row, column names are a,b,c
 * k a 				<- key row, primary key is a
 * i foo bar stuff 	<- insert row, a=foo, b=bar, c=stuff
 * u foo bar wef 	<- update row with key=a, a=foo, b=bar, c=lame
 * d foo 			<- delete row with key a
 *
 * Returns allocated data inside RowMessage.row (fields, ptr)
 */

static RowMessage
parse_text_row(const char *line)
{
	RowMessage msg;
	int i = 0;
	char *sptr = 0;
	char *tok = 0;

	memset(&msg, 0, sizeof(RowMessage));

	msg.row.ptr = pg_strdup(line);
	chomp(msg.row.ptr);

	msg.row.n = tabs(msg.row.ptr);
	msg.row.fields = pg_malloc(sizeof(Field) * msg.row.n);

	sptr = (char *) msg.row.ptr;
	tok = strtok(sptr, "\t");
	sptr = 0;

	msg.type = tok[0];

	while (true)
	{
		tok = strtok(sptr, "\t");

		if (!tok)
			break;

		msg.row.fields[i].n = strlen(tok);
		msg.row.fields[i].data = tok;
		i++;

		sptr = 0;
	}

	return msg;
}

bool PGRowStreamPostInit(PGRowStream *s)
{
	char *buffer = 0;

	while (true)
	{
		int rc = PQgetCopyData(s->conn, &buffer, 1);

		if (rc == 0)
		{
			break;
		}

		if (rc < 0)
		{
			return false;
		}

		chomp(buffer);
		handle_data(s, buffer);
		free(buffer);
	}

	return true;
}

/*
 * Complete lines are handed to parse_text_row, and then stream->callback is
 * fired with the result.
 */
static void
handle_data(PGRowStream *stream, const char *buf)
{
	RowMessage msg = parse_text_row(buf);
	stream->callback(stream->cb_ctx, msg.type, &msg.row);
}

/*
 * Loop over the input stream appending data until we hit EOF, or would block.
 * Returns true if stream is finished.
 */
bool
PGRowStreamHandleInput(PGRowStream *s)
{
	char *buffer = 0;
	PQconsumeInput(s->conn);

	while (true)
	{
		int rc = PQgetCopyData(s->conn, &buffer, 1);

		if (rc == 0)
		{
			break;
		}

		if (rc < 0)
		{
			return true;
		}

		chomp(buffer);

		handle_data(s, buffer);
		free(buffer);
	}

	return false;
}
