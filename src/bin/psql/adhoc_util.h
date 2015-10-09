/*-------------------------------------------------------------------------
 *
 * adhoc_util.h
 *    Misc utils for the adhoc client
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/adhoc_util.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADHOC_UTIL_H
#define ADHOC_UTIL_H

#include <unistd.h> 

void fatal_error(const char *file, unsigned line, const char *fmt, ...)
	__attribute__ ((format (printf, 3, 4)));

#define FATAL_ERROR(...) { fatal_error(__FILE__, __LINE__, __VA_ARGS__); }

/*
 * The adhoc client requires its own assert because it needs to clean up
 * the ncurses window before printing out any errors.
 */

#ifndef USE_ASSERT_CHECKING
#define TermAssert(x)
#else
#define TermAssert(x) { if (!(x)) { do_endwin(); Assert((x)); } }
#endif

extern void do_endwin(void);

/*
 * Count the number of spaces in a nul terminated string.
 * Utilised by row parsers.
 */
static inline size_t
spaces(const char *s)
{
	size_t n = 0;

	while (*s != '\0')
	{
		if (*s == ' ')
			n++;

		s++;
	}

	return n;
}

static inline size_t
tabs(const char *s)
{
	size_t n = 0;

	while (*s != '\0')
	{
		if (*s == '\t')
			n++;

		s++;
	}

	return n;
}

#endif
