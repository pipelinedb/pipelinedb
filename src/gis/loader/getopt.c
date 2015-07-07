/*
**      @(#)getopt.c    2.5 (smail) 9/15/87
*/

#include "../postgis_config.h"
#include <stdio.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include "getopt.h"

/*
 * Here's something you've all been waiting for:  the AT&T public domain
 * source for getopt(3).  It is the code which was given out at the 1985
 * UNIFORUM conference in Dallas.  I obtained it by electronic mail
 * directly from AT&T.  The people there assure me that it is indeed
 * in the public domain.
 *
 * There is no manual page.  That is because the one they gave out at
 * UNIFORUM was slightly different from the current System V Release 2
 * manual page.  The difference apparently involved a note about the
 * famous rules 5 and 6, recommending using white space between an option
 * and its first argument, and not grouping options that have arguments.
 * Getopt itself is currently lenient about both of these things.  White
 * space is allowed, but not mandatory, and the last option in a group can
 * have an argument.  That particular version of the man page evidently
 * has no official existence, and my source at AT&T did not send a copy.
 * The current SVR2 man page reflects the actual behavor of this getopt.
 * However, I am not about to post a copy of anything licensed by AT&T.
 */

#define ERR(s, c)\
  if(pgis_opterr){\
    fprintf(stderr, "%s%s%c\n", argv[0], s, c);\
  }

int     pgis_opterr = 1;
int     pgis_optind = 1;
int     pgis_optopt;
char    *pgis_optarg;

int
pgis_getopt(int argc, char **argv, char *opts)
{
	static int sp = 1;
	register int c;
	register char *cp;

	if (sp == 1)
	{
		if (pgis_optind >= argc ||
		        argv[pgis_optind][0] != '-' /* && argv[pgis_optind][0] != '/' */ ||
		        argv[pgis_optind][1] == '\0')
		{
			return(EOF);
		}
		else if (strcmp(argv[pgis_optind], "--") == 0)
		{
			pgis_optind++;
			return(EOF);
		}
	}
	pgis_optopt = c = argv[pgis_optind][sp];
	if (c == ':' || (cp=strchr(opts, c)) == 0)
	{
		ERR(": illegal option -- ", c);
		if (argv[pgis_optind][++sp] == '\0')
		{
			pgis_optind++;
			sp = 1;
		}
		return('?');
	}
	if (*++cp == ':')
	{
		if (argv[pgis_optind][sp+1] != '\0')
			pgis_optarg = &argv[pgis_optind++][sp+1];
		else if (++pgis_optind >= argc)
		{
			ERR(": option requires an argument -- ", c);
			sp = 1;
			return('?');
		}
		else
			pgis_optarg = argv[pgis_optind++];
		sp = 1;
	}
	else
	{
		if (argv[pgis_optind][++sp] == '\0')
		{
			sp = 1;
			pgis_optind++;
		}
		pgis_optarg = NULL;
	}
	return(c);
}

