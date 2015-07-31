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

// initStringInfo

void initPAString(PAString str)
{
	int	size = 1024;

	str->data = (char *) palloc(size);
	str->maxlen = size;

	resetPAString(str);
}

void
resetPAString(PAString str)
{
	str->data[0] = '\0';
	str->len = 0;
}

void appendPAStringBinary(PAString str, const char *data, int datalen);

void
appendPAStringString(PAString str, const char *s)
{
	appendPAStringBinary(str, s, strlen(s));
}

void enlargePAString(PAString str, int needed);

#define MaxAllocSize	((Size) 0x3fffffff)		/* 1 gigabyte - 1 */

void
enlargePAString(PAString str, int needed)
{
	int			newlen;

	/*
	 * Guard against out-of-range "needed" values.  Without this, we can get
	 * an overflow or infinite loop in the following.
	 */
	if (needed < 0)
	{
		assert(0);
		abort();
	}

	if (((Size) needed) >= (MaxAllocSize - (Size) str->len))
	{
		assert(0);
		abort();
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= MaxAllocSize */

	if (needed <= str->maxlen)
		return;					/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * str->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	str->data = (char *) repalloc(str->data, newlen);
	str->maxlen = newlen;
}

void
appendPAStringBinary(PAString str, const char *data, int datalen)
{
	assert(str != NULL);

	/* Make more room if needed */
	enlargePAString(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data.  (Some callers are dealing with text but call this because
	 * their input isn't null-terminated.)
	 */
	str->data[str->len] = '\0';
}

