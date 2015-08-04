#ifndef ROWSTREAM_H_E3E391A9
#define ROWSTREAM_H_E3E391A9

#include "rowmap.h"
#include "adhoc_compat.h"
#include <stdbool.h>

typedef struct RowMessage
{
	int type; // i, u, d
	Row row;
} RowMessage;

typedef void (*RowFunc) (void* ctx, int type, Row *row);

typedef struct RowStream
{
	int fd;
	char buf[4096];

	FlexString flex;

	RowFunc callback;
	void* cb_ctx;

} RowStream;

RowStream* RowStreamInit(RowFunc fn, void *ctx);

int RowStreamFd(RowStream *s);
bool RowStreamHandleInput(RowStream *s);

#endif
