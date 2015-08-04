#ifndef ROWSTREAM_H_E3E391A9
#define ROWSTREAM_H_E3E391A9

#include "oldrow.h"

typedef void (*row_processor) (void* ctx, OldRow row);

typedef struct RowStream
{
	int fd;
	char buf[4096];

	FlexString flex;

	row_processor process_row;
	void* pr_ctx;

} RowStream;

void append_data(RowStream *stream, const char* buf, size_t n);

bool handle_row_stream(RowStream *stream);
void destroy_row_stream(RowStream *stream);

bool handle_row_stream(RowStream *stream)
{
	while (true)
	{
		ssize_t nr = read(stream->fd, stream->buf, 4096);

		if (nr == 0) {
			return true;
		}

		if (nr == -1) {
			return false;
		}

		append_data(stream, stream->buf, nr);
	}

	return false;
}

RowStream* init_row_stream(row_processor proc, void *pctx);

RowStream* init_row_stream(row_processor proc, void *pctx)
{
	RowStream *stream = malloc(sizeof(RowStream));
	memset(stream, 0, sizeof(RowStream));

	stream->fd = STDIN_FILENO;
	fcntl(stream->fd, F_SETFL, O_NONBLOCK);

	stream->process_row = proc;
	stream->pr_ctx = pctx;

	init_flex(&stream->flex);

	return stream;
}

int num_fields(const char* line);

int num_fields(const char* line)
{
	const char* s = line;
	int cnt = 0;

	while (*s)
	{
		if (*s == ' ') cnt++;
		s++;
	}

	return cnt + 1;
}



void destroy_row_stream(RowStream *stream)
{
}

OldRow parse_text_row(const char* line);

void append_data(RowStream *stream, const char* buf, size_t nr)
{
	size_t i = 0;

	for (i = 0; i < nr; ++i) {

		append_flex(&(stream->flex), buf + i, 1);

		if (buf[i] == '\n') {

			OldRow row = {0};

			stream->flex.buf[stream->flex.n-1] = '\0';
			row = parse_text_row(stream->flex.buf);

			stream->process_row(stream->pr_ctx, row);
			reset_flex(&stream->flex);
		}
	}
}


OldRow parse_text_row(const char* line)
{
	OldRow row = {0,0,0};
	int i = 0;
	char* sptr = 0;

	row.ptr = strdup(line);
	row.num_fields = num_fields(line);
	row.fields = malloc(sizeof(Field) * row.num_fields);

	sptr = (char*) row.ptr;

	while (true)
	{
		char* tok = strtok(sptr, " ");

		if (!tok) 
			break;

		row.fields[i].len = strlen(tok);
		row.fields[i].data = tok;
		i++;

		sptr = 0;
	}

	return row;
}



#endif
