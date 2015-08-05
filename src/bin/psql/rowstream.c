#include "rowstream.h"
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

RowStream* RowStreamInit(RowFunc cb, void *ctx)
{
	RowStream *self = pg_malloc(sizeof(RowStream));
	memset(self, 0, sizeof(RowStream));

	self->fd = STDIN_FILENO;
	fcntl(self->fd, F_SETFL, O_NONBLOCK);

	memset(self->buf, 0, sizeof(self->buf));

	initPQExpBuffer(&self->flex);

	self->callback = cb;
	self->cb_ctx = ctx;

	return self;
}

void RowStreamDestroy(RowStream *s)
{
	termPQExpBuffer(&s->flex);
	memset(s, 0, sizeof(RowStream));

	pg_free(s);
}

int RowStreamFd(RowStream *s)
{
	return s->fd;
}

static inline size_t spaces(const char* s)
{
	size_t cnt = 0;

	while (*s)
	{
		if (*s == ' ')
		{
			cnt++;
		}

		s++;
	}

	return cnt;
}

RowMessage parse_text_row(const char* line);

RowMessage parse_text_row(const char* line)
{
	RowMessage msg;
	int i = 0;
	char* sptr = 0;
	char* tok = 0;

	memset(&msg, 0, sizeof(RowMessage));

	msg.row.ptr = strdup(line);
	msg.row.n = spaces(line);
	msg.row.fields = pg_malloc(sizeof(Field) * msg.row.n);

	sptr = (char*) msg.row.ptr;
	tok = strtok(sptr, " ");
	sptr = 0;

	msg.type = tok[0];

	while (true)
	{
		tok = strtok(sptr, " ");

		if (!tok) 
			break;

		msg.row.fields[i].n = strlen(tok);
		msg.row.fields[i].data = tok;
		i++;

		sptr = 0;
	}

	return msg;
}

static void inline append_data(RowStream *stream, const char* buf, size_t nr)
{
	size_t i = 0;
	RowMessage msg;

	for (i = 0; i < nr; ++i)
	{
		appendBinaryPQExpBuffer(&stream->flex, buf + i, 1);

		if (buf[i] == '\n')
		{
			stream->flex.data[stream->flex.len-1] = '\0';
			msg = parse_text_row(stream->flex.data);

			stream->callback(stream->cb_ctx, msg.type, &msg.row);
			resetPQExpBuffer(&stream->flex);
		}
	}
}

bool RowStreamHandleInput(RowStream *s)
{
	while (true)
	{
		ssize_t nr = read(s->fd, s->buf, 4096);

		if (nr == 0) {
			return true;
		}

		if (nr == -1) {
			return false;
		}

		append_data(s, s->buf, nr);
	}

	return false;
}
