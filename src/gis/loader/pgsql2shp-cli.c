/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2003 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 *
 * PostGIS to Shapefile converter
 *
 * Original Author: Jeff Lounsbury <jeffloun@refractions.net>
 * Maintainer: Sandro Santilli <strk@keybit.bet>
 *
 **********************************************************************/

#include "pgsql2shp-core.h"
#include "../postgis_config.h"


static void
usage(int status)
{
  /* TODO: if status != 0 print all to stderr */

	printf(_( "RELEASE: %s (r%d)\n" ), POSTGIS_LIB_VERSION, POSTGIS_SVN_REVISION);
	printf(_("USAGE: pgsql2shp [<options>] <database> [<schema>.]<table>\n"
	         "       pgsql2shp [<options>] <database> <query>\n"
	         "\n"
	         "OPTIONS:\n" ));
	printf(_("  -f <filename>  Use this option to specify the name of the file to create.\n" ));
	printf(_("  -h <host>  Allows you to specify connection to a database on a\n"
	         "     machine other than the default.\n" ));
	printf(_("  -p <port>  Allows you to specify a database port other than the default.\n" ));
	printf(_("  -P <password>  Connect to the database with the specified password.\n" ));
	printf(_("  -u <user>  Connect to the database as the specified user.\n" ));
	printf(_("  -g <geometry_column> Specify the geometry column to be exported.\n" ));
	printf(_("  -b Use a binary cursor.\n" ));
	printf(_("  -r Raw mode. Do not assume table has been created by the loader. This would\n"
	         "     not unescape attribute names and will not skip the 'gid' attribute.\n" ));
	printf(_("  -k Keep PostgreSQL identifiers case.\n" ));
	printf(_("  -m <filename>  Specify a file containing a set of mappings of (long) column\n"
	         "     names to 10 character DBF column names. The content of the file is one or\n"
	         "     more lines of two names separated by white space and no trailing or\n"
	         "     leading space. For example:\n"
	         "         COLUMNNAME DBFFIELD1\n"
	         "         AVERYLONGCOLUMNNAME DBFFIELD2\n" ));
	printf(_("  -? Display this help screen.\n\n" ));
	exit(status);
}

int
main(int argc, char **argv)
{
	SHPDUMPERCONFIG *config;
	SHPDUMPERSTATE *state;

	int ret, c, i;

	/* If no options are specified, display usage */
	if (argc == 1)
	{
		usage(0); /* TODO: should this exit with error ? */
	}

	/* Parse command line options and set configuration */
	config = malloc(sizeof(SHPDUMPERCONFIG));
	set_dumper_config_defaults(config);

	while ((c = pgis_getopt(argc, argv, "bf:h:du:p:P:g:rkm:")) != EOF)
	{
		switch (c)
		{
		case 'b':
			config->binary = 1;
			break;
		case 'f':
			config->shp_file = pgis_optarg;
			break;
		case 'h':
			config->conn->host = pgis_optarg;
			break;
		case 'd':
			config->dswitchprovided = 1;
			break;
		case 'r':
			config->includegid = 1;
			config->unescapedattrs = 1;
			break;
		case 'u':
			config->conn->username = pgis_optarg;
			break;
		case 'p':
			config->conn->port = pgis_optarg;
			break;
		case 'P':
			config->conn->password = pgis_optarg;
			break;
		case 'g':
			config->geo_col_name = pgis_optarg;
			break;
		case 'm':
			config->column_map_filename = pgis_optarg;
			break;
		case 'k':
			config->keep_fieldname_case = 1;
			break;
		default:
			usage(pgis_optopt == '?' ? 0 : 1);
		}
	}


	/* Determine the database name from the next argument, if no database, exit. */
	if (pgis_optind < argc)
	{
		config->conn->database = argv[pgis_optind];
		pgis_optind++;
	}
	else
	{
		usage(1);
	}


	/* Determine the table and schema names from the next argument if supplied, otherwise if
	   it's a user-defined query then set that instead */
	if (pgis_optind < argc)
	{
		/* User-defined queries begin with SELECT */
		if (!strncmp(argv[pgis_optind], "SELECT ", 7) ||
			!strncmp(argv[pgis_optind], "select ", 7))
		{
			config->usrquery = argv[pgis_optind];
		}
		else
		{
			/* Schema qualified table name */
			char *strptr = argv[pgis_optind];
			char *chrptr = strchr(strptr, '.');
			
				/* OK, this is a schema-qualified table name... */
      if (chrptr)
      {
        if ( chrptr == strptr ) 
        {
          /* table is ".something" display help  */
          usage(0);
          exit(0);
        }
        /* Null terminate at the '.' */
        *chrptr = '\0';
        /* Copy in the parts */
        config->schema = strdup(strptr);
        config->table = strdup(chrptr+1);
      }
      else
      {
        config->table = strdup(strptr);
      }
		}
	}
	else
	{
		usage(1);
	}

	state = ShpDumperCreate(config);

	ret = ShpDumperConnectDatabase(state);
	if (ret != SHPDUMPEROK)
	{
		fprintf(stderr, "%s\n", state->message);
		fflush(stderr);
		exit(1);
	}

	/* Display a warning if the -d switch is used with PostGIS >= 1.0 */
	if (state->pgis_major_version > 0 && state->config->dswitchprovided)
	{
		fprintf(stderr, _("WARNING: -d switch is useless when dumping from postgis-1.0.0+\n"));
		fflush(stderr);
	}

	/* Open the table ready to return rows */
	fprintf(stdout, _("Initializing... \n"));
	fflush(stdout);

	ret = ShpDumperOpenTable(state);
	if (ret != SHPDUMPEROK)
	{
		fprintf(stderr, "%s\n", state->message);
		fflush(stderr);

		if (ret == SHPDUMPERERR)
			exit(1);
	}

	fprintf(stdout, _("Done (postgis major version: %d).\n"), state->pgis_major_version);
	fprintf(stdout, _("Output shape: %s\n"), shapetypename(state->outshptype));
	fprintf(stdout, _("Dumping: "));
	fflush(stdout);

	for (i = 0; i < ShpDumperGetRecordCount(state); i++)
	{
		/* Mimic existing behaviour */
		if (!(state->currow % state->config->fetchsize))
		{
			fprintf(stdout, "X");
			fflush(stdout);
		}

		ret = ShpLoaderGenerateShapeRow(state);
		if (ret != SHPDUMPEROK)
		{
			fprintf(stderr, "%s\n", state->message);
			fflush(stderr);
	
			if (ret == SHPDUMPERERR)
				exit(1);
		}
	}

	fprintf(stdout, _(" [%d rows].\n"), ShpDumperGetRecordCount(state));
	fflush(stdout);

	ret = ShpDumperCloseTable(state);
	if (ret != SHPDUMPEROK)
	{
		fprintf(stderr, "%s\n", state->message);
		fflush(stderr);

		if (ret == SHPDUMPERERR)
			exit(1);
	}

	ShpDumperDestroy(state);

	return 0;
}
