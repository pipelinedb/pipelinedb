#include "adhoc_compat.h"
#include "postgres_fe.h"

#include <assert.h>
#include <string.h>

#include <stdlib.h>
	
int assert_enabled = 0;

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
