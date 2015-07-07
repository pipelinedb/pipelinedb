/* Like vsprintf but provides a pointer to malloc'd storage, which must
   be freed by the caller.
   Copyright (C) 1994, 1998, 1999, 2000, 2001 Free Software Foundation, Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2, or (at your option)
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.  */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#if __STDC__
# include <stdarg.h>
#else
# include <varargs.h>
#endif

#ifdef TEST
int global_total_width;
#endif

/* Make sure we have a va_copy that will work on all platforms */
#ifndef va_copy
# ifdef __va_copy
#  define va_copy(d, s)         __va_copy((d), (s))
# else
#  define va_copy(d, s)         memcpy(&(d), &(s), sizeof(va_list))
# endif
#endif

int lw_vasprintf (char **result, const char *format, va_list args);
int lw_asprintf
#if __STDC__
(char **result, const char *format, ...);
#else
(result, va_alist);
char **result;
va_dcl
#endif


static int
int_vasprintf (result, format, args)
char **result;
const char *format;
va_list *args;
{
	const char *p = format;
	/* Add one to make sure that it is never zero, which might cause malloc
	   to return NULL.  */
	int total_width = strlen (format) + 1;
	va_list ap;

	memcpy (&ap, args, sizeof (va_list));

	while (*p != '\0')
	{
		if (*p++ == '%')
		{
			while (strchr ("-+ #0", *p))
				++p;
			if (*p == '*')
			{
				++p;
				total_width += abs (va_arg (ap, int));
			}
			else
				total_width += strtoul (p, (char **) &p, 10);
			if (*p == '.')
			{
				++p;
				if (*p == '*')
				{
					++p;
					total_width += abs (va_arg (ap, int));
				}
				else
					total_width += strtoul (p, (char **) &p, 10);
			}
			while (strchr ("hlLjtz", *p))
				++p;
			/* Should be big enough for any format specifier except %s
			   and floats.  */
			total_width += 30;
			switch (*p)
			{
			case 'd':
			case 'i':
			case 'o':
			case 'u':
			case 'x':
			case 'X':
			case 'c':
				(void) va_arg (ap, int);
				break;
			case 'f':
			{
				double arg = va_arg (ap, double);
				if (arg >= 1.0 || arg <= -1.0)
					/* Since an ieee double can have an exponent of 307, we'll
					   make the buffer wide enough to cover the gross case. */
					total_width += 307;
			}
			break;
			case 'e':
			case 'E':
			case 'g':
			case 'G':
				(void) va_arg (ap, double);
				break;
			case 's':
				total_width += strlen (va_arg (ap, char *));
				break;
			case 'p':
			case 'n':
				(void) va_arg (ap, char *);
				break;
			}
			p++;
		}
	}
#ifdef TEST
	global_total_width = total_width;
#endif
	*result = malloc (total_width);
	if (*result != NULL)
		return vsprintf (*result, format, *args);
	else
		return 0;
}

int
lw_vasprintf (result, format, args)
char **result;
const char *format;
va_list args;
{
	va_list temp;

	va_copy(temp, args);

	return int_vasprintf (result, format, &temp);
}

int
lw_asprintf
#if __STDC__
(char **result, const char *format, ...)
#else
(result, va_alist)
char **result;
va_dcl
#endif
{
	va_list args;
	int done;

#if __STDC__
	va_start (args, format);
#else
	char *format;
	va_start (args);
	format = va_arg (args, char *);
#endif
	done = lw_vasprintf (result, format, args);
	va_end (args);

	return done;
}
