#include "adhoc_compat.h"
#include "postgres_fe.h"

#include <assert.h>
#include <string.h>

#include <stdlib.h>
	
int assert_enabled = 0;

void die(const char* s) {

	fprintf(stderr, "%s\n", s);
	exit(1);
}

void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	assert(0);
	abort();
}

void elog_start(const char *filename, int lineno, const char *funcname)
{
}

void elog_finish(int elevel, const char *fmt,...)
{
	assert(0);
	abort();
}

void init_flex(FlexString* f)
{
	memset(f, 0, sizeof(FlexString));

	f->buf = malloc(1025);
	f->cap = 1024;
	f->n = 0;
	f->buf[0] = '\0';
}

void cleanup_flex(FlexString* f)
{
	free(f->buf);
	memset(f, 0, sizeof(FlexString));
}

void reset_flex(FlexString* f)
{
	f->buf[0] = '\0';
	f->n = 0;
}

void append_flex(FlexString* f, const char* s, size_t n)
{
	size_t ns = f->n + n;

	if (f->cap < ns)
	{
		f->buf = realloc(f->buf, ns);
		f->cap = ns;
	}

	memcpy(f->buf + f->n, s, n);

	f->n = ns;
	f->buf[f->n] = '\0';
}

size_t length_flex(FlexString* f)
{
	return f->n;
}
