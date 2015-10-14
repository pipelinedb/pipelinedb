/*
 * padhoc - PipelineDB ncurses app for adhoc continuous queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/adhoc.c
 */

#include "postgres_fe.h"
#include "rowmap.h"
#include "rowstream.h"
#include "pg_rowstream.h"
#include "screen.h"
#include "model.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include "getopt_long.h"

#include "variables.h"
#include "common/username.h"

typedef struct App
{
	Model   *model;
	Screen  *screen;
} App;

volatile bool keep_running = true;

static void
sighandle(int x)
{
	keep_running = false;
}

static void row_event_dispatcher(void *ctx, int type, Row *row);

enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES
};

struct adhoc_opts
{
	char	   *dbname;
	char	   *host;
	char	   *port;
	char	   *username;
	char	   *action_string;
};

typedef struct _psqlSettings
{
	PGconn	   *db;				/* connection to backend */
	bool		notty;			/* stdin or stdout is not a tty (as determined*/
	const char *progname;		/* in case you renamed psql */
	VariableSpace vars;			/* "shell variable" repository */
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
			printf(stderr, "%s\n", errstr);
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
psql_error(const char *fmt,...);

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

static int handle_options(int argc, char** argv, struct adhoc_opts * options)
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

	return 0;
}

/*
 * Initializes the padhoc app, and runs the main event loop.
 *
 * The event loop is written in non blocking style, and deals with two fds
 *
 * pfd[0] - row input stream (currently stdin)
 * pfd[1] - screen tty fd (setup with ncurses)
 *
 * To ease debugging, the screen can be disabled (by leaving as NULL).
 */
int
main(int argc, char *argv[])
{
	struct adhoc_opts options;
	handle_options(argc, argv, &options);

	

	App app = {0,0};
	Model *model = ModelInit();

	Screen *screen = 0;
	PGRowStream *stream = 0;

	/*
	 * Setup a signal handler to break out of the loop upon ctrl-c. We must
	 * do this so the app will clean up the terminal properly when it is
	 * shutdown.
	 */
	signal(SIGINT, sighandle);
	stream = PGRowStreamInit(pset.db, options.action_string, row_event_dispatcher, &app);

	/* Comment the next line out to ease debugging */
	screen = ScreenInit(model);

	app.model = model;
	app.screen = screen;

	bool ok = PGRowStreamPostInit(stream);

	if (!ok)
	{
		if (screen)
		{
			ScreenDestroy(screen);
		}

		fprintf(stderr, "could not execute query %s\n", argv[1]);
		exit(1);
	}

	while (keep_running)
	{
		struct pollfd pfd[2];
		int rc = 0;

		memset(&pfd, 0, sizeof(pfd));

		pfd[0].fd = PGRowStreamFd(stream);
		pfd[0].events = POLLIN;

		if (screen)
		{
			pfd[1].fd = ScreenFd(screen);
			pfd[1].events = POLLIN;
		}

		rc = poll(pfd, screen ? 2 : 1, -1);

		if (rc < 0)
		{
			keep_running = false;
			break;
		}

		/* Handle the row stream fd */
		if (pfd[0].revents & POLLIN)
		{
			if (screen && ScreenIsPaused(screen))
			{
				/*
				 * If we are paused, don't read any new rows. This will block
				 * the upstream writer.
				 */
				usleep(1000);
			}
			else
			{
				bool fin = PGRowStreamHandleInput(stream);

				if (fin)
					break;
			}
		}

		/* Handle the screen fd if we have one */
		if (screen)
		{
			if (pfd[1].revents & POLLIN)
				ScreenHandleInput(screen);
		}
	}

	if (screen)
		ScreenDestroy(screen);

	PGRowStreamDestroy(stream);
	ModelDestroy(model);

	return 0;
}

/*
 * Callback fired from RowStream
 */
static void
row_event_dispatcher(void *ctx, int type, Row *row)
{
	App *app = (App *)(ctx);
	bool dirty = false;

	switch (type)
	{
		case 'h':
			ModelHeaderRow(app->model, row);
			dirty = true;
			break;
		case 'k':
			ModelKeyRow(app->model, row);
			break;
		case 'i':
			ModelInsertRow(app->model, row);
			dirty = true;
			break;
		case 'u':
			ModelUpdateRow(app->model, row);
			dirty = true;
			break;
		case 'd':
			ModelDeleteRow(app->model, row);
			dirty = true;
			break;
	}

	if (app->screen && dirty)
		ScreenUpdate(app->screen);
}
