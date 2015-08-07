#include "adhoc_util.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <ncurses.h>

void
fatal_error(const char *file, unsigned line, const char *fmt, ...)
{
	va_list ap;

	/* clean up after ncurses to make error readable */
	endwin();

	fprintf(stderr, "padhoc: %s:%d: error: ", file, line);

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);

	fprintf(stderr, "\n");

	exit(1);
}
