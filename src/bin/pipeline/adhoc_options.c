/*-------------------------------------------------------------------------
 *
 * adhoc_options.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "adhoc_options.h"
#include "postgres_fe.h"
#include "common/username.h"
#include "getopt_long.h"

/*
 * Most of this code has been ripped out of psql.
 *
 * It has been cutdown to support the following flags:
 *
 * General options:
 *   -c, --command=COMMAND    run adhoc sql command
 *   -d, --dbname=DBNAME      database name to connect to (default: "pipeline")
 * Connection options:
 *   -h, --host=HOSTNAME      database server host or socket directory (default: "local socket")
 *   -p, --port=PORT          database server port (default: "5432")
 *   -U, --username=USERNAME  database user name (default: "jason")
 *   -w, --no-password        never prompt for password
 *   -W, --password           force password prompt (should happen automatically)
 */
enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES
};

typedef struct _psqlSettings
{
	PGconn	   *db;				/* connection to backend */
	bool		notty;			/* stdin or stdout is not a tty (as determined*/
	const char *progname;		/* in case you renamed psql */
	enum trivalue getPassword;	/* prompt the user for a username and password */
} PsqlSettings;

PsqlSettings pset;

static void
usage()
{
	const char* env = 0;
	const char *user;
	char	   *errstr;

	/* Find default user, in case we need it. */
	user = getenv("PGUSER");

	if (!user)
	{
		user = get_user_name(&errstr);

		if (!user)
		{
			fprintf(stderr, "%s\n", errstr);
			exit(EXIT_FAILURE);
		}
	}

	printf("General options:\n");
	printf("  -c, --command=COMMAND    run adhoc sql command\n");

	/* Display default database */
	env = getenv("PGDATABASE");
	if (!env)
		env = user;

	printf("  -d, --dbname=DBNAME      database name to connect to (default: \"%s\")\n", env);

	printf("Connection options:\n");

	/* Display default host */
	env = getenv("PGHOST");
	printf("  -h, --host=HOSTNAME      database server host or socket directory (default: \"%s\")\n",
		   env ? env : "local socket");

	env = getenv("PGPORT");
	printf("  -p, --port=PORT          database server port (default: \"%s\")\n",
		   env ? env : DEF_PGPORT_STR);

	env = getenv("PGUSER");

	if (!env)
		env = user;

	printf("  -U, --username=USERNAME  database user name (default: \"%s\")\n", env);
	printf("  -w, --no-password        never prompt for password\n");
	printf("  -W, --password           force password prompt (should happen automatically)\n");
}

volatile bool cancel_pressed = false;

void
psql_error(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

void
psql_error(const char *fmt,...)
{
	va_list		ap;

	fflush(stdout);

	va_start(ap, fmt);
	vfprintf(stderr, _(fmt), ap);
	va_end(ap);
}

static void
parse_psql_options(int argc, char *argv[], struct adhoc_opts * options)
{
	static struct option long_options[] =
	{
		{"command", required_argument, NULL, 'c'},
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;

	memset(options, 0, sizeof *options);

	while ((c = getopt_long(argc, argv, "d:c:h:p:U:wW?01",
							long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'd':
				options->dbname = pg_strdup(optarg);
				break;
			case 'c':
				options->action_string = pg_strdup(optarg);
				break;
			case 'h':
				options->host = pg_strdup(optarg);
				break;
			case 'p':
				options->port = pg_strdup(optarg);
				break;
			case 'U':
				options->username = pg_strdup(optarg);
				break;
			case 'w':
				pset.getPassword = TRI_NO;
				break;
			case 'W':
				pset.getPassword = TRI_YES;
				break;
			case '?':
				/* Actual help option given */
				if (strcmp(argv[optind - 1], "--help") == 0 || strcmp(argv[optind - 1], "-?") == 0)
				{
					usage();
					exit(EXIT_SUCCESS);
				}
				/* unknown option reported by getopt */
				else
				{
					fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
							pset.progname);
					exit(EXIT_FAILURE);
				}
				break;
			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						pset.progname);
				exit(EXIT_FAILURE);
				break;
		}
	}

	/*
	 * if we still have arguments, use it as the database name and username
	 */
	while (argc - optind >= 1)
	{
		if (!options->dbname)
			options->dbname = argv[optind];
		else if (!options->username)
			options->username = argv[optind];
		else 
			fprintf(stderr, _("%s: warning: extra command-line argument \"%s\" ignored\n"),
					pset.progname, argv[optind]);

		optind++;
	}
}

/* 
 * parse the given command line options and try to connect to the db.
 *
 * On success, the db conn is returned inside options->db
 */
int handle_options(int argc, char** argv, struct adhoc_opts * options)
{
	char	   *password = NULL;
	char	   *password_prompt = NULL;
	bool new_pass = true;

	parse_psql_options(argc, argv, options);

	if (!options->action_string)
	{
		fprintf(stderr, "Error: Must specify an sql command\n\n");
		usage();
		exit(1);
	}

	if (options->username == NULL)
		password_prompt = pg_strdup(_("Password: "));
	else
		password_prompt = psprintf(_("Password for user %s: "),
								   options->username);

	if (pset.getPassword == TRI_YES)
		password = simple_prompt(password_prompt, 100, false);

	do
	{
#define PARAMS_ARRAY_SIZE	8
		const char **keywords = 
			pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
		const char **values = pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

		keywords[0] = "host";
		values[0] = options->host;
		keywords[1] = "port";
		values[1] = options->port;
		keywords[2] = "user";
		values[2] = options->username;
		keywords[3] = "password";
		values[3] = password;
		keywords[4] = "dbname";
		values[4] = options->dbname;
		keywords[5] = "fallback_application_name";
		values[5] = pset.progname;
		keywords[6] = "client_encoding";
		values[6] = (pset.notty || getenv("PGCLIENTENCODING")) ? NULL : "auto";
		keywords[7] = NULL;
		values[7] = NULL;

		new_pass = false;
		pset.db = PQconnectdbParams(keywords, values, true);

		free(keywords);
		free(values);

		if (PQstatus(pset.db) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(pset.db) &&
			password == NULL &&
			pset.getPassword != TRI_NO)
		{
			PQfinish(pset.db);
			password = simple_prompt(password_prompt, 100, false);
			new_pass = true;
		}
	} while (new_pass);

	options->db = pset.db;

	return 0;
}
