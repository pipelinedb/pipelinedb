/*-------------------------------------------------------------------------
 *
 * pg_dumpall.c
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * src/bin/pg_dump/pg_dumpall.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <time.h>
#include <unistd.h>

#ifdef ENABLE_NLS
#include <locale.h>
#endif

#include "getopt_long.h"

#include "dumputils.h"
#include "dumpmem.h"
#include "pg_backup.h"

/* version string we expect back from pg_dump */
#define PGDUMP_VERSIONSTR "pg_dump (PostgreSQL) " PG_VERSION "\n"


static void help(void);

static void dropRoles(PGconn *conn);
static void dumpRoles(PGconn *conn);
static void dumpRoleMembership(PGconn *conn);
static void dumpGroups(PGconn *conn);
static void dropTablespaces(PGconn *conn);
static void dumpTablespaces(PGconn *conn);
static void dropDBs(PGconn *conn);
static void dumpCreateDB(PGconn *conn);
static void dumpDatabaseConfig(PGconn *conn, const char *dbname);
static void dumpUserConfig(PGconn *conn, const char *username);
static void dumpDbRoleConfig(PGconn *conn);
static void makeAlterConfigCommand(PGconn *conn, const char *arrayitem,
					   const char *type, const char *name, const char *type2,
					   const char *name2);
static void dumpDatabases(PGconn *conn);
static void dumpTimestamp(char *msg);
static void doShellQuoting(PQExpBuffer buf, const char *str);

static int	runPgDump(const char *dbname);
static void buildShSecLabels(PGconn *conn, const char *catalog_name,
				 uint32 objectId, PQExpBuffer buffer,
				 const char *target, const char *objname);
static PGconn *connectDatabase(const char *dbname, const char *pghost, const char *pgport,
	  const char *pguser, enum trivalue prompt_password, bool fail_on_error);
static PGresult *executeQuery(PGconn *conn, const char *query);
static void executeCommand(PGconn *conn, const char *query);

#ifdef PGXC
static void dumpNodes(PGconn *conn);
static void dumpNodeGroups(PGconn *conn);
#endif /* PGXC */

static char pg_dump_bin[MAXPGPATH];
static PQExpBuffer pgdumpopts;
static bool skip_acls = false;
static bool verbose = false;

static int	binary_upgrade = 0;
static int	column_inserts = 0;
static int	disable_dollar_quoting = 0;
static int	disable_triggers = 0;
static int	inserts = 0;
static int	no_tablespaces = 0;
static int	use_setsessauth = 0;
static int	no_security_labels = 0;
static int	no_unlogged_table_data = 0;
static int	server_version;

static FILE *OPF;
static char *filename = NULL;

#ifdef PGXC
static int	dump_nodes = 0;
static int include_nodes = 0;
#endif /* PGXC */

int
main(int argc, char *argv[])
{
	char	   *pghost = NULL;
	char	   *pgport = NULL;
	char	   *pguser = NULL;
	char	   *pgdb = NULL;
	char	   *use_role = NULL;
	enum trivalue prompt_password = TRI_DEFAULT;
	bool		data_only = false;
	bool		globals_only = false;
	bool		output_clean = false;
	bool		roles_only = false;
	bool		tablespaces_only = false;
	PGconn	   *conn;
	int			encoding;
	const char *std_strings;
	int			c,
				ret;
	int			optindex;

	static struct option long_options[] = {
		{"data-only", no_argument, NULL, 'a'},
		{"clean", no_argument, NULL, 'c'},
		{"file", required_argument, NULL, 'f'},
		{"globals-only", no_argument, NULL, 'g'},
		{"host", required_argument, NULL, 'h'},
		{"ignore-version", no_argument, NULL, 'i'},
		{"database", required_argument, NULL, 'l'},
		{"oids", no_argument, NULL, 'o'},
		{"no-owner", no_argument, NULL, 'O'},
		{"port", required_argument, NULL, 'p'},
		{"roles-only", no_argument, NULL, 'r'},
		{"schema-only", no_argument, NULL, 's'},
		{"superuser", required_argument, NULL, 'S'},
		{"tablespaces-only", no_argument, NULL, 't'},
		{"username", required_argument, NULL, 'U'},
		{"verbose", no_argument, NULL, 'v'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"no-privileges", no_argument, NULL, 'x'},
		{"no-acl", no_argument, NULL, 'x'},

		/*
		 * the following options don't have an equivalent short option letter
		 */
		{"attribute-inserts", no_argument, &column_inserts, 1},
		{"binary-upgrade", no_argument, &binary_upgrade, 1},
		{"column-inserts", no_argument, &column_inserts, 1},
		{"disable-dollar-quoting", no_argument, &disable_dollar_quoting, 1},
		{"disable-triggers", no_argument, &disable_triggers, 1},
		{"inserts", no_argument, &inserts, 1},
		{"lock-wait-timeout", required_argument, NULL, 2},
		{"no-tablespaces", no_argument, &no_tablespaces, 1},
		{"quote-all-identifiers", no_argument, &quote_all_identifiers, 1},
		{"role", required_argument, NULL, 3},
		{"use-set-session-authorization", no_argument, &use_setsessauth, 1},
		{"no-security-labels", no_argument, &no_security_labels, 1},
		{"no-unlogged-table-data", no_argument, &no_unlogged_table_data, 1},
#ifdef PGXC
		{"dump-nodes", no_argument, &dump_nodes, 1},
		{"include-nodes", no_argument, &include_nodes, 1},
#endif
		{NULL, 0, NULL, 0}
	};

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_dump"));

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help();
			exit_nicely(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_dumpall (PostgreSQL) " PG_VERSION);
			exit_nicely(0);
		}
	}

	if ((ret = find_other_exec(argv[0], "pg_dump", PGDUMP_VERSIONSTR,
							   pg_dump_bin)) < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv[0], full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			fprintf(stderr,
					_("The program \"pg_dump\" is needed by %s "
					  "but was not found in the\n"
					  "same directory as \"%s\".\n"
					  "Check your installation.\n"),
					progname, full_path);
		else
			fprintf(stderr,
					_("The program \"pg_dump\" was found by \"%s\"\n"
					  "but was not the same version as %s.\n"
					  "Check your installation.\n"),
					full_path, progname);
		exit_nicely(1);
	}

	pgdumpopts = createPQExpBuffer();

	while ((c = getopt_long(argc, argv, "acf:gh:il:oOp:rsS:tU:vwWx", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'a':
				data_only = true;
				appendPQExpBuffer(pgdumpopts, " -a");
				break;

			case 'c':
				output_clean = true;
				break;

			case 'f':
				filename = optarg;
				appendPQExpBuffer(pgdumpopts, " -f ");
				doShellQuoting(pgdumpopts, filename);
				break;

			case 'g':
				globals_only = true;
				break;

			case 'h':
				pghost = optarg;
				appendPQExpBuffer(pgdumpopts, " -h ");
				doShellQuoting(pgdumpopts, pghost);
				break;

			case 'i':
				/* ignored, deprecated option */
				break;

			case 'l':
				pgdb = optarg;
				break;

			case 'o':
				appendPQExpBuffer(pgdumpopts, " -o");
				break;

			case 'O':
				appendPQExpBuffer(pgdumpopts, " -O");
				break;

			case 'p':
				pgport = optarg;
				appendPQExpBuffer(pgdumpopts, " -p ");
				doShellQuoting(pgdumpopts, pgport);
				break;

			case 'r':
				roles_only = true;
				break;

			case 's':
				appendPQExpBuffer(pgdumpopts, " -s");
				break;

			case 'S':
				appendPQExpBuffer(pgdumpopts, " -S ");
				doShellQuoting(pgdumpopts, optarg);
				break;

			case 't':
				tablespaces_only = true;
				break;

			case 'U':
				pguser = optarg;
				appendPQExpBuffer(pgdumpopts, " -U ");
				doShellQuoting(pgdumpopts, pguser);
				break;

			case 'v':
				verbose = true;
				appendPQExpBuffer(pgdumpopts, " -v");
				break;

			case 'w':
				prompt_password = TRI_NO;
				appendPQExpBuffer(pgdumpopts, " -w");
				break;

			case 'W':
				prompt_password = TRI_YES;
				appendPQExpBuffer(pgdumpopts, " -W");
				break;

			case 'x':
				skip_acls = true;
				appendPQExpBuffer(pgdumpopts, " -x");
				break;

			case 0:
				break;

			case 2:
				appendPQExpBuffer(pgdumpopts, " --lock-wait-timeout ");
				doShellQuoting(pgdumpopts, optarg);
				break;

			case 3:
				use_role = optarg;
				appendPQExpBuffer(pgdumpopts, " --role ");
				doShellQuoting(pgdumpopts, use_role);
				break;

			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit_nicely(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit_nicely(1);
	}

	/* Make sure the user hasn't specified a mix of globals-only options */
	if (globals_only && roles_only)
	{
		fprintf(stderr, _("%s: options -g/--globals-only and -r/--roles-only cannot be used together\n"),
				progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit_nicely(1);
	}

	if (globals_only && tablespaces_only)
	{
		fprintf(stderr, _("%s: options -g/--globals-only and -t/--tablespaces-only cannot be used together\n"),
				progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit_nicely(1);
	}

	if (roles_only && tablespaces_only)
	{
		fprintf(stderr, _("%s: options -r/--roles-only and -t/--tablespaces-only cannot be used together\n"),
				progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit_nicely(1);
	}

	/* Add long options to the pg_dump argument list */
	if (binary_upgrade)
		appendPQExpBuffer(pgdumpopts, " --binary-upgrade");
	if (column_inserts)
		appendPQExpBuffer(pgdumpopts, " --column-inserts");
	if (disable_dollar_quoting)
		appendPQExpBuffer(pgdumpopts, " --disable-dollar-quoting");
	if (disable_triggers)
		appendPQExpBuffer(pgdumpopts, " --disable-triggers");
	if (inserts)
		appendPQExpBuffer(pgdumpopts, " --inserts");
	if (no_tablespaces)
		appendPQExpBuffer(pgdumpopts, " --no-tablespaces");
	if (quote_all_identifiers)
		appendPQExpBuffer(pgdumpopts, " --quote-all-identifiers");
	if (use_setsessauth)
		appendPQExpBuffer(pgdumpopts, " --use-set-session-authorization");
	if (no_security_labels)
		appendPQExpBuffer(pgdumpopts, " --no-security-labels");
	if (no_unlogged_table_data)
		appendPQExpBuffer(pgdumpopts, " --no-unlogged-table-data");

#ifdef PGXC
	if (include_nodes)
		appendPQExpBuffer(pgdumpopts, " --include-nodes");
#endif

	/*
	 * If there was a database specified on the command line, use that,
	 * otherwise try to connect to database "postgres", and failing that
	 * "template1".  "postgres" is the preferred choice for 8.1 and later
	 * servers, but it usually will not exist on older ones.
	 */
	if (pgdb)
	{
		conn = connectDatabase(pgdb, pghost, pgport, pguser,
							   prompt_password, false);

		if (!conn)
		{
			fprintf(stderr, _("%s: could not connect to database \"%s\"\n"),
					progname, pgdb);
			exit_nicely(1);
		}
	}
	else
	{
		conn = connectDatabase("postgres", pghost, pgport, pguser,
							   prompt_password, false);
		if (!conn)
			conn = connectDatabase("template1", pghost, pgport, pguser,
								   prompt_password, true);

		if (!conn)
		{
			fprintf(stderr, _("%s: could not connect to databases \"postgres\" or \"template1\"\n"
							  "Please specify an alternative database.\n"),
					progname);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit_nicely(1);
		}
	}

	/*
	 * Open the output file if required, otherwise use stdout
	 */
	if (filename)
	{
		OPF = fopen(filename, PG_BINARY_W);
		if (!OPF)
		{
			fprintf(stderr, _("%s: could not open the output file \"%s\": %s\n"),
					progname, filename, strerror(errno));
			exit_nicely(1);
		}
	}
	else
		OPF = stdout;

	/*
	 * Get the active encoding and the standard_conforming_strings setting, so
	 * we know how to escape strings.
	 */
	encoding = PQclientEncoding(conn);
	std_strings = PQparameterStatus(conn, "standard_conforming_strings");
	if (!std_strings)
		std_strings = "off";

	/* Set the role if requested */
	if (use_role && server_version >= 80100)
	{
		PQExpBuffer query = createPQExpBuffer();

		appendPQExpBuffer(query, "SET ROLE %s", fmtId(use_role));
		executeCommand(conn, query->data);
		destroyPQExpBuffer(query);
	}

	/* Force quoting of all identifiers if requested. */
	if (quote_all_identifiers && server_version >= 90100)
		executeCommand(conn, "SET quote_all_identifiers = true");

	fprintf(OPF, "--\n-- PostgreSQL database cluster dump\n--\n\n");
	if (verbose)
		dumpTimestamp("Started on");

	/*
	 * We used to emit \connect postgres here, but that served no purpose
	 * other than to break things for installations without a postgres
	 * database.  Everything we're restoring here is a global, so whichever
	 * database we're connected to at the moment is fine.
	 */

	/* Replicate encoding and std_strings in output */
	fprintf(OPF, "SET client_encoding = '%s';\n",
			pg_encoding_to_char(encoding));
	fprintf(OPF, "SET standard_conforming_strings = %s;\n", std_strings);
	if (strcmp(std_strings, "off") == 0)
		fprintf(OPF, "SET escape_string_warning = off;\n");
	fprintf(OPF, "\n");

	if (!data_only)
	{
		/*
		 * If asked to --clean, do that first.	We can avoid detailed
		 * dependency analysis because databases never depend on each other,
		 * and tablespaces never depend on each other.	Roles could have
		 * grants to each other, but DROP ROLE will clean those up silently.
		 */
		if (output_clean)
		{
			if (!globals_only && !roles_only && !tablespaces_only)
				dropDBs(conn);

			if (!roles_only && !no_tablespaces)
			{
				if (server_version >= 80000)
					dropTablespaces(conn);
			}

			if (!tablespaces_only)
				dropRoles(conn);
		}

		/*
		 * Now create objects as requested.  Be careful that option logic here
		 * is the same as for drops above.
		 */
		if (!tablespaces_only)
		{
			/* Dump roles (users) */
			dumpRoles(conn);

			/* Dump role memberships --- need different method for pre-8.1 */
			if (server_version >= 80100)
				dumpRoleMembership(conn);
			else
				dumpGroups(conn);
		}

		if (!roles_only && !no_tablespaces)
		{
			/* Dump tablespaces */
			if (server_version >= 80000)
				dumpTablespaces(conn);
		}

		/* Dump CREATE DATABASE commands */
		if (!globals_only && !roles_only && !tablespaces_only)
			dumpCreateDB(conn);

		/* Dump role/database settings */
		if (!tablespaces_only && !roles_only)
		{
			if (server_version >= 90000)
				dumpDbRoleConfig(conn);
		}

#ifdef PGXC
		/* Dump nodes and node groups */
		if (dump_nodes)
		{
			dumpNodes(conn);
			dumpNodeGroups(conn);
		}
#endif
	}

	if (!globals_only && !roles_only && !tablespaces_only)
		dumpDatabases(conn);

	PQfinish(conn);

	if (verbose)
		dumpTimestamp("Completed on");
	fprintf(OPF, "--\n-- PostgreSQL database cluster dump complete\n--\n\n");

	if (filename)
		fclose(OPF);

	exit_nicely(0);
}


static void
help(void)
{
	printf(_("%s extracts a PostgreSQL database cluster into an SQL script file.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);

	printf(_("\nGeneral options:\n"));
	printf(_("  -f, --file=FILENAME          output file name\n"));
	printf(_("  --lock-wait-timeout=TIMEOUT  fail after waiting TIMEOUT for a table lock\n"));
	printf(_("  --help                       show this help, then exit\n"));
	printf(_("  --version                    output version information, then exit\n"));
	printf(_("\nOptions controlling the output content:\n"));
	printf(_("  -a, --data-only              dump only the data, not the schema\n"));
	printf(_("  -c, --clean                  clean (drop) databases before recreating\n"));
	printf(_("  -g, --globals-only           dump only global objects, no databases\n"));
	printf(_("  -o, --oids                   include OIDs in dump\n"));
	printf(_("  -O, --no-owner               skip restoration of object ownership\n"));
	printf(_("  -r, --roles-only             dump only roles, no databases or tablespaces\n"));
	printf(_("  -s, --schema-only            dump only the schema, no data\n"));
	printf(_("  -S, --superuser=NAME         superuser user name to use in the dump\n"));
	printf(_("  -t, --tablespaces-only       dump only tablespaces, no databases or roles\n"));
	printf(_("  -x, --no-privileges          do not dump privileges (grant/revoke)\n"));
	printf(_("  --binary-upgrade             for use by upgrade utilities only\n"));
	printf(_("  --column-inserts             dump data as INSERT commands with column names\n"));
	printf(_("  --disable-dollar-quoting     disable dollar quoting, use SQL standard quoting\n"));
	printf(_("  --disable-triggers           disable triggers during data-only restore\n"));
	printf(_("  --inserts                    dump data as INSERT commands, rather than COPY\n"));
	printf(_("  --no-security-labels         do not dump security label assignments\n"));
	printf(_("  --no-tablespaces             do not dump tablespace assignments\n"));
	printf(_("  --no-unlogged-table-data     do not dump unlogged table data\n"));
	printf(_("  --quote-all-identifiers      quote all identifiers, even if not key words\n"));
	printf(_("  --use-set-session-authorization\n"
			 "                               use SET SESSION AUTHORIZATION commands instead of\n"
			 "                               ALTER OWNER commands to set ownership\n"));
#ifdef PGXC
	printf(_("  --dump-nodes                 include nodes and node groups in the dump\n"));
	printf(_("  --include-nodes              include TO NODE clause in the dumped CREATE TABLE commands\n"));
#endif

	printf(_("\nConnection options:\n"));
	printf(_("  -h, --host=HOSTNAME      database server host or socket directory\n"));
	printf(_("  -l, --database=DBNAME    alternative default database\n"));
	printf(_("  -p, --port=PORT          database server port number\n"));
	printf(_("  -U, --username=NAME      connect as specified database user\n"));
	printf(_("  -w, --no-password        never prompt for password\n"));
	printf(_("  -W, --password           force password prompt (should happen automatically)\n"));
	printf(_("  --role=ROLENAME          do SET ROLE before dump\n"));

	printf(_("\nIf -f/--file is not used, then the SQL script will be written to the standard\n"
			 "output.\n\n"));
	printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}


/*
 * Drop roles
 */
static void
dropRoles(PGconn *conn)
{
	PGresult   *res;
	int			i_rolname;
	int			i;

	if (server_version >= 80100)
		res = executeQuery(conn,
						   "SELECT rolname "
						   "FROM pg_authid "
						   "ORDER BY 1");
	else
		res = executeQuery(conn,
						   "SELECT usename as rolname "
						   "FROM pg_shadow "
						   "UNION "
						   "SELECT groname as rolname "
						   "FROM pg_group "
						   "ORDER BY 1");

	i_rolname = PQfnumber(res, "rolname");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Drop roles\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *rolename;

		rolename = PQgetvalue(res, i, i_rolname);

		fprintf(OPF, "DROP ROLE %s;\n", fmtId(rolename));
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}

/*
 * Dump roles
 */
static void
dumpRoles(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	PGresult   *res;
	int			i_oid,
				i_rolname,
				i_rolsuper,
				i_rolinherit,
				i_rolcreaterole,
				i_rolcreatedb,
				i_rolcanlogin,
				i_rolconnlimit,
				i_rolpassword,
				i_rolvaliduntil,
				i_rolreplication,
				i_rolcomment;
	int			i;

	/* note: rolconfig is dumped later */
	if (server_version >= 90100)
		printfPQExpBuffer(buf,
						  "SELECT oid, rolname, rolsuper, rolinherit, "
						  "rolcreaterole, rolcreatedb, "
						  "rolcanlogin, rolconnlimit, rolpassword, "
						  "rolvaliduntil, rolreplication, "
			  "pg_catalog.shobj_description(oid, 'pg_authid') as rolcomment "
						  "FROM pg_authid "
						  "ORDER BY 2");
	else if (server_version >= 80200)
		printfPQExpBuffer(buf,
						  "SELECT oid, rolname, rolsuper, rolinherit, "
						  "rolcreaterole, rolcreatedb, "
						  "rolcanlogin, rolconnlimit, rolpassword, "
						  "rolvaliduntil, false as rolreplication, "
			  "pg_catalog.shobj_description(oid, 'pg_authid') as rolcomment "
						  "FROM pg_authid "
						  "ORDER BY 2");
	else if (server_version >= 80100)
		printfPQExpBuffer(buf,
						  "SELECT oid, rolname, rolsuper, rolinherit, "
						  "rolcreaterole, rolcreatedb, "
						  "rolcanlogin, rolconnlimit, rolpassword, "
						  "rolvaliduntil, false as rolreplication, "
						  "null as rolcomment "
						  "FROM pg_authid "
						  "ORDER BY 2");
	else
		printfPQExpBuffer(buf,
						  "SELECT 0, usename as rolname, "
						  "usesuper as rolsuper, "
						  "true as rolinherit, "
						  "usesuper as rolcreaterole, "
						  "usecreatedb as rolcreatedb, "
						  "true as rolcanlogin, "
						  "-1 as rolconnlimit, "
						  "passwd as rolpassword, "
						  "valuntil as rolvaliduntil, "
						  "false as rolreplication, "
						  "null as rolcomment "
						  "FROM pg_shadow "
						  "UNION ALL "
						  "SELECT 0, groname as rolname, "
						  "false as rolsuper, "
						  "true as rolinherit, "
						  "false as rolcreaterole, "
						  "false as rolcreatedb, "
						  "false as rolcanlogin, "
						  "-1 as rolconnlimit, "
						  "null::text as rolpassword, "
						  "null::abstime as rolvaliduntil, "
						  "false as rolreplication, "
						  "null as rolcomment "
						  "FROM pg_group "
						  "WHERE NOT EXISTS (SELECT 1 FROM pg_shadow "
						  " WHERE usename = groname) "
						  "ORDER BY 2");

	res = executeQuery(conn, buf->data);

	i_oid = PQfnumber(res, "oid");
	i_rolname = PQfnumber(res, "rolname");
	i_rolsuper = PQfnumber(res, "rolsuper");
	i_rolinherit = PQfnumber(res, "rolinherit");
	i_rolcreaterole = PQfnumber(res, "rolcreaterole");
	i_rolcreatedb = PQfnumber(res, "rolcreatedb");
	i_rolcanlogin = PQfnumber(res, "rolcanlogin");
	i_rolconnlimit = PQfnumber(res, "rolconnlimit");
	i_rolpassword = PQfnumber(res, "rolpassword");
	i_rolvaliduntil = PQfnumber(res, "rolvaliduntil");
	i_rolreplication = PQfnumber(res, "rolreplication");
	i_rolcomment = PQfnumber(res, "rolcomment");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Roles\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		const char *rolename;
		Oid			auth_oid;

		auth_oid = atooid(PQgetvalue(res, i, i_oid));
		rolename = PQgetvalue(res, i, i_rolname);

		resetPQExpBuffer(buf);

		if (binary_upgrade)
		{
			appendPQExpBuffer(buf, "\n-- For binary upgrade, must preserve pg_authid.oid\n");
			appendPQExpBuffer(buf,
							  "SELECT binary_upgrade.set_next_pg_authid_oid('%u'::pg_catalog.oid);\n\n",
							  auth_oid);
		}

		/*
		 * We dump CREATE ROLE followed by ALTER ROLE to ensure that the role
		 * will acquire the right properties even if it already exists (ie, it
		 * won't hurt for the CREATE to fail).  This is particularly important
		 * for the role we are connected as, since even with --clean we will
		 * have failed to drop it.
		 */
		appendPQExpBuffer(buf, "CREATE ROLE %s;\n", fmtId(rolename));
		appendPQExpBuffer(buf, "ALTER ROLE %s WITH", fmtId(rolename));

		if (strcmp(PQgetvalue(res, i, i_rolsuper), "t") == 0)
			appendPQExpBuffer(buf, " SUPERUSER");
		else
			appendPQExpBuffer(buf, " NOSUPERUSER");

		if (strcmp(PQgetvalue(res, i, i_rolinherit), "t") == 0)
			appendPQExpBuffer(buf, " INHERIT");
		else
			appendPQExpBuffer(buf, " NOINHERIT");

		if (strcmp(PQgetvalue(res, i, i_rolcreaterole), "t") == 0)
			appendPQExpBuffer(buf, " CREATEROLE");
		else
			appendPQExpBuffer(buf, " NOCREATEROLE");

		if (strcmp(PQgetvalue(res, i, i_rolcreatedb), "t") == 0)
			appendPQExpBuffer(buf, " CREATEDB");
		else
			appendPQExpBuffer(buf, " NOCREATEDB");

		if (strcmp(PQgetvalue(res, i, i_rolcanlogin), "t") == 0)
			appendPQExpBuffer(buf, " LOGIN");
		else
			appendPQExpBuffer(buf, " NOLOGIN");

		if (strcmp(PQgetvalue(res, i, i_rolreplication), "t") == 0)
			appendPQExpBuffer(buf, " REPLICATION");
		else
			appendPQExpBuffer(buf, " NOREPLICATION");

		if (strcmp(PQgetvalue(res, i, i_rolconnlimit), "-1") != 0)
			appendPQExpBuffer(buf, " CONNECTION LIMIT %s",
							  PQgetvalue(res, i, i_rolconnlimit));

		if (!PQgetisnull(res, i, i_rolpassword))
		{
			appendPQExpBuffer(buf, " PASSWORD ");
			appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolpassword), conn);
		}

		if (!PQgetisnull(res, i, i_rolvaliduntil))
			appendPQExpBuffer(buf, " VALID UNTIL '%s'",
							  PQgetvalue(res, i, i_rolvaliduntil));

		appendPQExpBuffer(buf, ";\n");

		if (!PQgetisnull(res, i, i_rolcomment))
		{
			appendPQExpBuffer(buf, "COMMENT ON ROLE %s IS ", fmtId(rolename));
			appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolcomment), conn);
			appendPQExpBuffer(buf, ";\n");
		}

		if (!no_security_labels && server_version >= 90200)
			buildShSecLabels(conn, "pg_authid", auth_oid,
							 buf, "ROLE", rolename);

		fprintf(OPF, "%s", buf->data);
	}

	/*
	 * Dump configuration settings for roles after all roles have been dumped.
	 * We do it this way because config settings for roles could mention the
	 * names of other roles.
	 */
	if (server_version >= 70300)
		for (i = 0; i < PQntuples(res); i++)
			dumpUserConfig(conn, PQgetvalue(res, i, i_rolname));

	PQclear(res);

	fprintf(OPF, "\n\n");

	destroyPQExpBuffer(buf);
}


/*
 * Dump role memberships.  This code is used for 8.1 and later servers.
 *
 * Note: we expect dumpRoles already created all the roles, but there is
 * no membership yet.
 */
static void
dumpRoleMembership(PGconn *conn)
{
	PGresult   *res;
	int			i;

	res = executeQuery(conn, "SELECT ur.rolname AS roleid, "
					   "um.rolname AS member, "
					   "a.admin_option, "
					   "ug.rolname AS grantor "
					   "FROM pg_auth_members a "
					   "LEFT JOIN pg_authid ur on ur.oid = a.roleid "
					   "LEFT JOIN pg_authid um on um.oid = a.member "
					   "LEFT JOIN pg_authid ug on ug.oid = a.grantor "
					   "ORDER BY 1,2,3");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Role memberships\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *roleid = PQgetvalue(res, i, 0);
		char	   *member = PQgetvalue(res, i, 1);
		char	   *option = PQgetvalue(res, i, 2);

		fprintf(OPF, "GRANT %s", fmtId(roleid));
		fprintf(OPF, " TO %s", fmtId(member));
		if (*option == 't')
			fprintf(OPF, " WITH ADMIN OPTION");

		/*
		 * We don't track the grantor very carefully in the backend, so cope
		 * with the possibility that it has been dropped.
		 */
		if (!PQgetisnull(res, i, 3))
		{
			char	   *grantor = PQgetvalue(res, i, 3);

			fprintf(OPF, " GRANTED BY %s", fmtId(grantor));
		}
		fprintf(OPF, ";\n");
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}

/*
 * Dump group memberships from a pre-8.1 server.  It's annoying that we
 * can't share any useful amount of code with the post-8.1 case, but
 * the catalog representations are too different.
 *
 * Note: we expect dumpRoles already created all the roles, but there is
 * no membership yet.
 */
static void
dumpGroups(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	PGresult   *res;
	int			i;

	res = executeQuery(conn,
					   "SELECT groname, grolist FROM pg_group ORDER BY 1");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Role memberships\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *groname = PQgetvalue(res, i, 0);
		char	   *grolist = PQgetvalue(res, i, 1);
		PGresult   *res2;
		int			j;

		/*
		 * Array representation is {1,2,3} ... convert to (1,2,3)
		 */
		if (strlen(grolist) < 3)
			continue;

		grolist = pg_strdup(grolist);
		grolist[0] = '(';
		grolist[strlen(grolist) - 1] = ')';
		printfPQExpBuffer(buf,
						  "SELECT usename FROM pg_shadow "
						  "WHERE usesysid IN %s ORDER BY 1",
						  grolist);
		free(grolist);

		res2 = executeQuery(conn, buf->data);

		for (j = 0; j < PQntuples(res2); j++)
		{
			char	   *usename = PQgetvalue(res2, j, 0);

			/*
			 * Don't try to grant a role to itself; can happen if old
			 * installation has identically named user and group.
			 */
			if (strcmp(groname, usename) == 0)
				continue;

			fprintf(OPF, "GRANT %s", fmtId(groname));
			fprintf(OPF, " TO %s;\n", fmtId(usename));
		}

		PQclear(res2);
	}

	PQclear(res);
	destroyPQExpBuffer(buf);

	fprintf(OPF, "\n\n");
}


/*
 * Drop tablespaces.
 */
static void
dropTablespaces(PGconn *conn)
{
	PGresult   *res;
	int			i;

	/*
	 * Get all tablespaces except built-in ones (which we assume are named
	 * pg_xxx)
	 */
	res = executeQuery(conn, "SELECT spcname "
					   "FROM pg_catalog.pg_tablespace "
					   "WHERE spcname !~ '^pg_' "
					   "ORDER BY 1");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Drop tablespaces\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *spcname = PQgetvalue(res, i, 0);

		fprintf(OPF, "DROP TABLESPACE %s;\n", fmtId(spcname));
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}

/*
 * Dump tablespaces.
 */
static void
dumpTablespaces(PGconn *conn)
{
	PGresult   *res;
	int			i;

	/*
	 * Get all tablespaces except built-in ones (which we assume are named
	 * pg_xxx)
	 */
	if (server_version >= 90200)
		res = executeQuery(conn, "SELECT oid, spcname, "
						 "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
						   "pg_catalog.pg_tablespace_location(oid), spcacl, "
						   "array_to_string(spcoptions, ', '),"
						"pg_catalog.shobj_description(oid, 'pg_tablespace') "
						   "FROM pg_catalog.pg_tablespace "
						   "WHERE spcname !~ '^pg_' "
						   "ORDER BY 1");
	else if (server_version >= 90000)
		res = executeQuery(conn, "SELECT oid, spcname, "
						 "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
						   "spclocation, spcacl, "
						   "array_to_string(spcoptions, ', '),"
						"pg_catalog.shobj_description(oid, 'pg_tablespace') "
						   "FROM pg_catalog.pg_tablespace "
						   "WHERE spcname !~ '^pg_' "
						   "ORDER BY 1");
	else if (server_version >= 80200)
		res = executeQuery(conn, "SELECT oid, spcname, "
						 "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
						   "spclocation, spcacl, null, "
						"pg_catalog.shobj_description(oid, 'pg_tablespace') "
						   "FROM pg_catalog.pg_tablespace "
						   "WHERE spcname !~ '^pg_' "
						   "ORDER BY 1");
	else
		res = executeQuery(conn, "SELECT oid, spcname, "
						 "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
						   "spclocation, spcacl, "
						   "null, null "
						   "FROM pg_catalog.pg_tablespace "
						   "WHERE spcname !~ '^pg_' "
						   "ORDER BY 1");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Tablespaces\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		PQExpBuffer buf = createPQExpBuffer();
		uint32		spcoid = atooid(PQgetvalue(res, i, 0));
		char	   *spcname = PQgetvalue(res, i, 1);
		char	   *spcowner = PQgetvalue(res, i, 2);
		char	   *spclocation = PQgetvalue(res, i, 3);
		char	   *spcacl = PQgetvalue(res, i, 4);
		char	   *spcoptions = PQgetvalue(res, i, 5);
		char	   *spccomment = PQgetvalue(res, i, 6);
		char	   *fspcname;

		/* needed for buildACLCommands() */
		fspcname = pg_strdup(fmtId(spcname));

		appendPQExpBuffer(buf, "CREATE TABLESPACE %s", fspcname);
		appendPQExpBuffer(buf, " OWNER %s", fmtId(spcowner));

		appendPQExpBuffer(buf, " LOCATION ");
		appendStringLiteralConn(buf, spclocation, conn);
		appendPQExpBuffer(buf, ";\n");

		if (spcoptions && spcoptions[0] != '\0')
			appendPQExpBuffer(buf, "ALTER TABLESPACE %s SET (%s);\n",
							  fspcname, spcoptions);

		if (!skip_acls &&
			!buildACLCommands(fspcname, NULL, "TABLESPACE", spcacl, spcowner,
							  "", server_version, buf))
		{
			fprintf(stderr, _("%s: could not parse ACL list (%s) for tablespace \"%s\"\n"),
					progname, spcacl, fspcname);
			PQfinish(conn);
			exit_nicely(1);
		}

		if (spccomment && strlen(spccomment))
		{
			appendPQExpBuffer(buf, "COMMENT ON TABLESPACE %s IS ", fspcname);
			appendStringLiteralConn(buf, spccomment, conn);
			appendPQExpBuffer(buf, ";\n");
		}

		if (!no_security_labels && server_version >= 90200)
			buildShSecLabels(conn, "pg_tablespace", spcoid,
							 buf, "TABLESPACE", fspcname);

		fprintf(OPF, "%s", buf->data);

		free(fspcname);
		destroyPQExpBuffer(buf);
	}

	PQclear(res);
	fprintf(OPF, "\n\n");
}


/*
 * Dump commands to drop each database.
 *
 * This should match the set of databases targeted by dumpCreateDB().
 */
static void
dropDBs(PGconn *conn)
{
	PGresult   *res;
	int			i;

	if (server_version >= 70100)
		res = executeQuery(conn,
						   "SELECT datname "
						   "FROM pg_database d "
						   "WHERE datallowconn ORDER BY 1");
	else
		res = executeQuery(conn,
						   "SELECT datname "
						   "FROM pg_database d "
						   "ORDER BY 1");

	if (PQntuples(res) > 0)
		fprintf(OPF, "--\n-- Drop databases\n--\n\n");

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *dbname = PQgetvalue(res, i, 0);

		/*
		 * Skip "template1" and "postgres"; the restore script is almost
		 * certainly going to be run in one or the other, and we don't know
		 * which.  This must agree with dumpCreateDB's choices!
		 */
		if (strcmp(dbname, "template1") != 0 &&
			strcmp(dbname, "postgres") != 0)
		{
			fprintf(OPF, "DROP DATABASE %s;\n", fmtId(dbname));
		}
	}

	PQclear(res);

	fprintf(OPF, "\n\n");
}

/*
 * Dump commands to create each database.
 *
 * To minimize the number of reconnections (and possibly ensuing
 * password prompts) required by the output script, we emit all CREATE
 * DATABASE commands during the initial phase of the script, and then
 * run pg_dump for each database to dump the contents of that
 * database.  We skip databases marked not datallowconn, since we'd be
 * unable to connect to them anyway (and besides, we don't want to
 * dump template0).
 */
static void
dumpCreateDB(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	char	   *default_encoding = NULL;
	char	   *default_collate = NULL;
	char	   *default_ctype = NULL;
	PGresult   *res;
	int			i;

	fprintf(OPF, "--\n-- Database creation\n--\n\n");

	/*
	 * First, get the installation's default encoding and locale information.
	 * We will dump encoding and locale specifications in the CREATE DATABASE
	 * commands for just those databases with values different from defaults.
	 *
	 * We consider template0's encoding and locale (or, pre-7.1, template1's)
	 * to define the installation default.	Pre-8.4 installations do not have
	 * per-database locale settings; for them, every database must necessarily
	 * be using the installation default, so there's no need to do anything
	 * (which is good, since in very old versions there is no good way to find
	 * out what the installation locale is anyway...)
	 */
	if (server_version >= 80400)
		res = executeQuery(conn,
						   "SELECT pg_encoding_to_char(encoding), "
						   "datcollate, datctype "
						   "FROM pg_database "
						   "WHERE datname = 'template0'");
	else if (server_version >= 70100)
		res = executeQuery(conn,
						   "SELECT pg_encoding_to_char(encoding), "
						   "null::text AS datcollate, null::text AS datctype "
						   "FROM pg_database "
						   "WHERE datname = 'template0'");
	else
		res = executeQuery(conn,
						   "SELECT pg_encoding_to_char(encoding), "
						   "null::text AS datcollate, null::text AS datctype "
						   "FROM pg_database "
						   "WHERE datname = 'template1'");

	/* If for some reason the template DB isn't there, treat as unknown */
	if (PQntuples(res) > 0)
	{
		if (!PQgetisnull(res, 0, 0))
			default_encoding = pg_strdup(PQgetvalue(res, 0, 0));
		if (!PQgetisnull(res, 0, 1))
			default_collate = pg_strdup(PQgetvalue(res, 0, 1));
		if (!PQgetisnull(res, 0, 2))
			default_ctype = pg_strdup(PQgetvalue(res, 0, 2));
	}

	PQclear(res);

	/* Now collect all the information about databases to dump */
	if (server_version >= 80400)
		res = executeQuery(conn,
						   "SELECT datname, "
						   "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where datname='template0'))), "
						   "pg_encoding_to_char(d.encoding), "
						   "datcollate, datctype, datfrozenxid, "
						   "datistemplate, datacl, datconnlimit, "
						   "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
			  "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
						   "WHERE datallowconn ORDER BY 1");
	else if (server_version >= 80100)
		res = executeQuery(conn,
						   "SELECT datname, "
						   "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where datname='template0'))), "
						   "pg_encoding_to_char(d.encoding), "
		   "null::text AS datcollate, null::text AS datctype, datfrozenxid, "
						   "datistemplate, datacl, datconnlimit, "
						   "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
			  "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
						   "WHERE datallowconn ORDER BY 1");
	else if (server_version >= 80000)
		res = executeQuery(conn,
						   "SELECT datname, "
						   "coalesce(usename, (select usename from pg_shadow where usesysid=(select datdba from pg_database where datname='template0'))), "
						   "pg_encoding_to_char(d.encoding), "
		   "null::text AS datcollate, null::text AS datctype, datfrozenxid, "
						   "datistemplate, datacl, -1 as datconnlimit, "
						   "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
		   "FROM pg_database d LEFT JOIN pg_shadow u ON (datdba = usesysid) "
						   "WHERE datallowconn ORDER BY 1");
	else if (server_version >= 70300)
		res = executeQuery(conn,
						   "SELECT datname, "
						   "coalesce(usename, (select usename from pg_shadow where usesysid=(select datdba from pg_database where datname='template0'))), "
						   "pg_encoding_to_char(d.encoding), "
		   "null::text AS datcollate, null::text AS datctype, datfrozenxid, "
						   "datistemplate, datacl, -1 as datconnlimit, "
						   "'pg_default' AS dattablespace "
		   "FROM pg_database d LEFT JOIN pg_shadow u ON (datdba = usesysid) "
						   "WHERE datallowconn ORDER BY 1");
	else if (server_version >= 70100)
		res = executeQuery(conn,
						   "SELECT datname, "
						   "coalesce("
					"(select usename from pg_shadow where usesysid=datdba), "
						   "(select usename from pg_shadow where usesysid=(select datdba from pg_database where datname='template0'))), "
						   "pg_encoding_to_char(d.encoding), "
						   "null::text AS datcollate, null::text AS datctype, 0 AS datfrozenxid, "
						   "datistemplate, '' as datacl, -1 as datconnlimit, "
						   "'pg_default' AS dattablespace "
						   "FROM pg_database d "
						   "WHERE datallowconn ORDER BY 1");
	else
	{
		/*
		 * Note: 7.0 fails to cope with sub-select in COALESCE, so just deal
		 * with getting a NULL by not printing any OWNER clause.
		 */
		res = executeQuery(conn,
						   "SELECT datname, "
					"(select usename from pg_shadow where usesysid=datdba), "
						   "pg_encoding_to_char(d.encoding), "
						   "null::text AS datcollate, null::text AS datctype, 0 AS datfrozenxid, "
						   "'f' as datistemplate, "
						   "'' as datacl, -1 as datconnlimit, "
						   "'pg_default' AS dattablespace "
						   "FROM pg_database d "
						   "ORDER BY 1");
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *dbname = PQgetvalue(res, i, 0);
		char	   *dbowner = PQgetvalue(res, i, 1);
		char	   *dbencoding = PQgetvalue(res, i, 2);
		char	   *dbcollate = PQgetvalue(res, i, 3);
		char	   *dbctype = PQgetvalue(res, i, 4);
		uint32		dbfrozenxid = atooid(PQgetvalue(res, i, 5));
		char	   *dbistemplate = PQgetvalue(res, i, 6);
		char	   *dbacl = PQgetvalue(res, i, 7);
		char	   *dbconnlimit = PQgetvalue(res, i, 8);
		char	   *dbtablespace = PQgetvalue(res, i, 9);
		char	   *fdbname;

		fdbname = pg_strdup(fmtId(dbname));

		resetPQExpBuffer(buf);

		/*
		 * Skip the CREATE DATABASE commands for "template1" and "postgres",
		 * since they are presumably already there in the destination cluster.
		 * We do want to emit their ACLs and config options if any, however.
		 */
		if (strcmp(dbname, "template1") != 0 &&
			strcmp(dbname, "postgres") != 0)
		{
			appendPQExpBuffer(buf, "CREATE DATABASE %s", fdbname);

			appendPQExpBuffer(buf, " WITH TEMPLATE = template0");

			if (strlen(dbowner) != 0)
				appendPQExpBuffer(buf, " OWNER = %s", fmtId(dbowner));

			if (default_encoding && strcmp(dbencoding, default_encoding) != 0)
			{
				appendPQExpBuffer(buf, " ENCODING = ");
				appendStringLiteralConn(buf, dbencoding, conn);
			}

			if (default_collate && strcmp(dbcollate, default_collate) != 0)
			{
				appendPQExpBuffer(buf, " LC_COLLATE = ");
				appendStringLiteralConn(buf, dbcollate, conn);
			}

			if (default_ctype && strcmp(dbctype, default_ctype) != 0)
			{
				appendPQExpBuffer(buf, " LC_CTYPE = ");
				appendStringLiteralConn(buf, dbctype, conn);
			}

			/*
			 * Output tablespace if it isn't the default.  For default, it
			 * uses the default from the template database.  If tablespace is
			 * specified and tablespace creation failed earlier, (e.g. no such
			 * directory), the database creation will fail too.  One solution
			 * would be to use 'SET default_tablespace' like we do in pg_dump
			 * for setting non-default database locations.
			 */
			if (strcmp(dbtablespace, "pg_default") != 0 && !no_tablespaces)
				appendPQExpBuffer(buf, " TABLESPACE = %s",
								  fmtId(dbtablespace));

			if (strcmp(dbconnlimit, "-1") != 0)
				appendPQExpBuffer(buf, " CONNECTION LIMIT = %s",
								  dbconnlimit);

			appendPQExpBuffer(buf, ";\n");

			if (strcmp(dbistemplate, "t") == 0)
			{
				appendPQExpBuffer(buf, "UPDATE pg_catalog.pg_database SET datistemplate = 't' WHERE datname = ");
				appendStringLiteralConn(buf, dbname, conn);
				appendPQExpBuffer(buf, ";\n");
			}

			if (binary_upgrade)
			{
				appendPQExpBuffer(buf, "-- For binary upgrade, set datfrozenxid.\n");
				appendPQExpBuffer(buf, "UPDATE pg_catalog.pg_database "
								  "SET datfrozenxid = '%u' "
								  "WHERE datname = ",
								  dbfrozenxid);
				appendStringLiteralConn(buf, dbname, conn);
				appendPQExpBuffer(buf, ";\n");
			}
		}

		if (!skip_acls &&
			!buildACLCommands(fdbname, NULL, "DATABASE", dbacl, dbowner,
							  "", server_version, buf))
		{
			fprintf(stderr, _("%s: could not parse ACL list (%s) for database \"%s\"\n"),
					progname, dbacl, fdbname);
			PQfinish(conn);
			exit_nicely(1);
		}

		fprintf(OPF, "%s", buf->data);

		if (server_version >= 70300)
			dumpDatabaseConfig(conn, dbname);

		free(fdbname);
	}

	PQclear(res);
	destroyPQExpBuffer(buf);

	fprintf(OPF, "\n\n");
}


/*
 * Dump database-specific configuration
 */
static void
dumpDatabaseConfig(PGconn *conn, const char *dbname)
{
	PQExpBuffer buf = createPQExpBuffer();
	int			count = 1;

	for (;;)
	{
		PGresult   *res;

		if (server_version >= 90000)
			printfPQExpBuffer(buf, "SELECT setconfig[%d] FROM pg_db_role_setting WHERE "
							  "setrole = 0 AND setdatabase = (SELECT oid FROM pg_database WHERE datname = ", count);
		else
			printfPQExpBuffer(buf, "SELECT datconfig[%d] FROM pg_database WHERE datname = ", count);
		appendStringLiteralConn(buf, dbname, conn);

		if (server_version >= 90000)
			appendPQExpBuffer(buf, ")");

		appendPQExpBuffer(buf, ";");

		res = executeQuery(conn, buf->data);
		if (PQntuples(res) == 1 &&
			!PQgetisnull(res, 0, 0))
		{
			makeAlterConfigCommand(conn, PQgetvalue(res, 0, 0),
								   "DATABASE", dbname, NULL, NULL);
			PQclear(res);
			count++;
		}
		else
		{
			PQclear(res);
			break;
		}
	}

	destroyPQExpBuffer(buf);
}



/*
 * Dump user-specific configuration
 */
static void
dumpUserConfig(PGconn *conn, const char *username)
{
	PQExpBuffer buf = createPQExpBuffer();
	int			count = 1;

	for (;;)
	{
		PGresult   *res;

		if (server_version >= 90000)
			printfPQExpBuffer(buf, "SELECT setconfig[%d] FROM pg_db_role_setting WHERE "
							  "setdatabase = 0 AND setrole = "
					   "(SELECT oid FROM pg_authid WHERE rolname = ", count);
		else if (server_version >= 80100)
			printfPQExpBuffer(buf, "SELECT rolconfig[%d] FROM pg_authid WHERE rolname = ", count);
		else
			printfPQExpBuffer(buf, "SELECT useconfig[%d] FROM pg_shadow WHERE usename = ", count);
		appendStringLiteralConn(buf, username, conn);
		if (server_version >= 90000)
			appendPQExpBuffer(buf, ")");

		res = executeQuery(conn, buf->data);
		if (PQntuples(res) == 1 &&
			!PQgetisnull(res, 0, 0))
		{
			makeAlterConfigCommand(conn, PQgetvalue(res, 0, 0),
								   "ROLE", username, NULL, NULL);
			PQclear(res);
			count++;
		}
		else
		{
			PQclear(res);
			break;
		}
	}

	destroyPQExpBuffer(buf);
}


/*
 * Dump user-and-database-specific configuration
 */
static void
dumpDbRoleConfig(PGconn *conn)
{
	PQExpBuffer buf = createPQExpBuffer();
	PGresult   *res;
	int			i;

	printfPQExpBuffer(buf, "SELECT rolname, datname, unnest(setconfig) "
					  "FROM pg_db_role_setting, pg_authid, pg_database "
		  "WHERE setrole = pg_authid.oid AND setdatabase = pg_database.oid");
	res = executeQuery(conn, buf->data);

	if (PQntuples(res) > 0)
	{
		fprintf(OPF, "--\n-- Per-Database Role Settings \n--\n\n");

		for (i = 0; i < PQntuples(res); i++)
		{
			makeAlterConfigCommand(conn, PQgetvalue(res, i, 2),
								   "ROLE", PQgetvalue(res, i, 0),
								   "DATABASE", PQgetvalue(res, i, 1));
		}

		fprintf(OPF, "\n\n");
	}

	PQclear(res);
	destroyPQExpBuffer(buf);
}


/*
 * Helper function for dumpXXXConfig().
 */
static void
makeAlterConfigCommand(PGconn *conn, const char *arrayitem,
					   const char *type, const char *name,
					   const char *type2, const char *name2)
{
	char	   *pos;
	char	   *mine;
	PQExpBuffer buf;

	mine = pg_strdup(arrayitem);
	pos = strchr(mine, '=');
	if (pos == NULL)
	{
		free(mine);
		return;
	}

	buf = createPQExpBuffer();

	*pos = 0;
	appendPQExpBuffer(buf, "ALTER %s %s ", type, fmtId(name));
	if (type2 != NULL && name2 != NULL)
		appendPQExpBuffer(buf, "IN %s %s ", type2, fmtId(name2));
	appendPQExpBuffer(buf, "SET %s TO ", fmtId(mine));

	/*
	 * Some GUC variable names are 'LIST' type and hence must not be quoted.
	 */
	if (pg_strcasecmp(mine, "DateStyle") == 0
		|| pg_strcasecmp(mine, "search_path") == 0)
		appendPQExpBuffer(buf, "%s", pos + 1);
	else
		appendStringLiteralConn(buf, pos + 1, conn);
	appendPQExpBuffer(buf, ";\n");

	fprintf(OPF, "%s", buf->data);
	destroyPQExpBuffer(buf);
	free(mine);
}



/*
 * Dump contents of databases.
 */
static void
dumpDatabases(PGconn *conn)
{
	PGresult   *res;
	int			i;

	if (server_version >= 70100)
		res = executeQuery(conn, "SELECT datname FROM pg_database WHERE datallowconn ORDER BY 1");
	else
		res = executeQuery(conn, "SELECT datname FROM pg_database ORDER BY 1");

	for (i = 0; i < PQntuples(res); i++)
	{
		int			ret;

		char	   *dbname = PQgetvalue(res, i, 0);

		if (verbose)
			fprintf(stderr, _("%s: dumping database \"%s\"...\n"), progname, dbname);

		fprintf(OPF, "\\connect %s\n\n", fmtId(dbname));

		if (filename)
			fclose(OPF);

		ret = runPgDump(dbname);
		if (ret != 0)
		{
			fprintf(stderr, _("%s: pg_dump failed on database \"%s\", exiting\n"), progname, dbname);
			exit_nicely(1);
		}

		if (filename)
		{
			OPF = fopen(filename, PG_BINARY_A);
			if (!OPF)
			{
				fprintf(stderr, _("%s: could not re-open the output file \"%s\": %s\n"),
						progname, filename, strerror(errno));
				exit_nicely(1);
			}
		}

	}

	PQclear(res);
}



/*
 * Run pg_dump on dbname.
 */
static int
runPgDump(const char *dbname)
{
	PQExpBuffer cmd = createPQExpBuffer();
	int			ret;

	appendPQExpBuffer(cmd, SYSTEMQUOTE "\"%s\" %s", pg_dump_bin,
					  pgdumpopts->data);

	/*
	 * If we have a filename, use the undocumented plain-append pg_dump
	 * format.
	 */
	if (filename)
		appendPQExpBuffer(cmd, " -Fa ");
	else
		appendPQExpBuffer(cmd, " -Fp ");

	doShellQuoting(cmd, dbname);

	appendPQExpBuffer(cmd, "%s", SYSTEMQUOTE);

	if (verbose)
		fprintf(stderr, _("%s: running \"%s\"\n"), progname, cmd->data);

	fflush(stdout);
	fflush(stderr);

	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	return ret;
}

/*
 * buildShSecLabels
 *
 * Build SECURITY LABEL command(s) for an shared object
 *
 * The caller has to provide object type and identifier to select security
 * labels from pg_seclabels system view.
 */
static void
buildShSecLabels(PGconn *conn, const char *catalog_name, uint32 objectId,
				 PQExpBuffer buffer, const char *target, const char *objname)
{
	PQExpBuffer sql = createPQExpBuffer();
	PGresult   *res;

	buildShSecLabelQuery(conn, catalog_name, objectId, sql);
	res = executeQuery(conn, sql->data);
	emitShSecLabels(conn, res, buffer, target, objname);

	PQclear(res);
	destroyPQExpBuffer(sql);
}

/*
 * Make a database connection with the given parameters.  An
 * interactive password prompt is automatically issued if required.
 *
 * If fail_on_error is false, we return NULL without printing any message
 * on failure, but preserve any prompted password for the next try.
 */
static PGconn *
connectDatabase(const char *dbname, const char *pghost, const char *pgport,
	   const char *pguser, enum trivalue prompt_password, bool fail_on_error)
{
	PGconn	   *conn;
	bool		new_pass;
	const char *remoteversion_str;
	int			my_version;
	static char *password = NULL;

	if (prompt_password == TRI_YES && !password)
		password = simple_prompt("Password: ", 100, false);

	/*
	 * Start the connection.  Loop until we have a password if requested by
	 * backend.
	 */
	do
	{
#define PARAMS_ARRAY_SIZE	7
		const char **keywords = pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
		const char **values = pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

		keywords[0] = "host";
		values[0] = pghost;
		keywords[1] = "port";
		values[1] = pgport;
		keywords[2] = "user";
		values[2] = pguser;
		keywords[3] = "password";
		values[3] = password;
		keywords[4] = "dbname";
		values[4] = dbname;
		keywords[5] = "fallback_application_name";
		values[5] = progname;
		keywords[6] = NULL;
		values[6] = NULL;

		new_pass = false;
		conn = PQconnectdbParams(keywords, values, true);

		free(keywords);
		free(values);

		if (!conn)
		{
			fprintf(stderr, _("%s: could not connect to database \"%s\"\n"),
					progname, dbname);
			exit_nicely(1);
		}

		if (PQstatus(conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(conn) &&
			password == NULL &&
			prompt_password != TRI_NO)
		{
			PQfinish(conn);
			password = simple_prompt("Password: ", 100, false);
			new_pass = true;
		}
	} while (new_pass);

	/* check to see that the backend connection was successfully made */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		if (fail_on_error)
		{
			fprintf(stderr,
					_("%s: could not connect to database \"%s\": %s\n"),
					progname, dbname, PQerrorMessage(conn));
			exit_nicely(1);
		}
		else
		{
			PQfinish(conn);
			return NULL;
		}
	}

	remoteversion_str = PQparameterStatus(conn, "server_version");
	if (!remoteversion_str)
	{
		fprintf(stderr, _("%s: could not get server version\n"), progname);
		exit_nicely(1);
	}
	server_version = parse_version(remoteversion_str);
	if (server_version < 0)
	{
		fprintf(stderr, _("%s: could not parse server version \"%s\"\n"),
				progname, remoteversion_str);
		exit_nicely(1);
	}

	my_version = parse_version(PG_VERSION);
	if (my_version < 0)
	{
		fprintf(stderr, _("%s: could not parse version \"%s\"\n"),
				progname, PG_VERSION);
		exit_nicely(1);
	}

	/*
	 * We allow the server to be back to 7.0, and up to any minor release of
	 * our own major version.  (See also version check in pg_dump.c.)
	 */
	if (my_version != server_version
		&& (server_version < 70000 ||
			(server_version / 100) > (my_version / 100)))
	{
		fprintf(stderr, _("server version: %s; %s version: %s\n"),
				remoteversion_str, progname, PG_VERSION);
		fprintf(stderr, _("aborting because of server version mismatch\n"));
		exit_nicely(1);
	}

	/*
	 * On 7.3 and later, make sure we are not fooled by non-system schemas in
	 * the search path.
	 */
	if (server_version >= 70300)
		executeCommand(conn, "SET search_path = pg_catalog");

	return conn;
}


/*
 * Run a query, return the results, exit program on failure.
 */
static PGresult *
executeQuery(PGconn *conn, const char *query)
{
	PGresult   *res;

	if (verbose)
		fprintf(stderr, _("%s: executing %s\n"), progname, query);

	res = PQexec(conn, query);
	if (!res ||
		PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: query failed: %s"),
				progname, PQerrorMessage(conn));
		fprintf(stderr, _("%s: query was: %s\n"),
				progname, query);
		PQfinish(conn);
		exit_nicely(1);
	}

	return res;
}

/*
 * As above for a SQL command (which returns nothing).
 */
static void
executeCommand(PGconn *conn, const char *query)
{
	PGresult   *res;

	if (verbose)
		fprintf(stderr, _("%s: executing %s\n"), progname, query);

	res = PQexec(conn, query);
	if (!res ||
		PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: query failed: %s"),
				progname, PQerrorMessage(conn));
		fprintf(stderr, _("%s: query was: %s\n"),
				progname, query);
		PQfinish(conn);
		exit_nicely(1);
	}

	PQclear(res);
}


/*
 * dumpTimestamp
 */
static void
dumpTimestamp(char *msg)
{
	char		buf[256];
	time_t		now = time(NULL);

	/*
	 * We don't print the timezone on Win32, because the names are long and
	 * localized, which means they may contain characters in various random
	 * encodings; this has been seen to cause encoding errors when reading the
	 * dump script.
	 */
	if (strftime(buf, sizeof(buf),
#ifndef WIN32
				 "%Y-%m-%d %H:%M:%S %Z",
#else
				 "%Y-%m-%d %H:%M:%S",
#endif
				 localtime(&now)) != 0)
		fprintf(OPF, "-- %s %s\n\n", msg, buf);
}


/*
 * Append the given string to the shell command being built in the buffer,
 * with suitable shell-style quoting.
 */
static void
doShellQuoting(PQExpBuffer buf, const char *str)
{
	const char *p;

#ifndef WIN32
	appendPQExpBufferChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\'')
			appendPQExpBuffer(buf, "'\"'\"'");
		else
			appendPQExpBufferChar(buf, *p);
	}
	appendPQExpBufferChar(buf, '\'');
#else							/* WIN32 */

	appendPQExpBufferChar(buf, '"');
	for (p = str; *p; p++)
	{
		if (*p == '"')
			appendPQExpBuffer(buf, "\\\"");
		else
			appendPQExpBufferChar(buf, *p);
	}
	appendPQExpBufferChar(buf, '"');
#endif   /* WIN32 */
}

#ifdef PGXC
static void
dumpNodes(PGconn *conn)
{
	PQExpBuffer query;
	PGresult   *res;
	int			num;
	int			i;

	query = createPQExpBuffer();

	appendPQExpBuffer(query, "select 'CREATE NODE ' || node_name || '"
					" WITH (TYPE = ' || chr(39) || (case when node_type='C'"
					" then 'coordinator' else 'datanode' end) || chr(39)"
					" || ' , HOST = ' || chr(39) || node_host || chr(39)"
					" || ', PORT = ' || node_port || (case when nodeis_primary='t'"
					" then ', PRIMARY' else ' ' end) || (case when nodeis_preferred"
					" then ', PREFERRED' else ' ' end) || ');' "
					" as node_query from pg_catalog.pgxc_node order by oid");

	res = executeQuery(conn, query->data);

	num = PQntuples(res);

	if (num > 0)
		fprintf(OPF, "--\n-- Nodes\n--\n\n");

	for (i = 0; i < num; i++)
	{
		fprintf(OPF, "%s\n", PQgetvalue(res, i, PQfnumber(res, "node_query")));
	}
	fprintf(OPF, "\n");

	PQclear(res);
	destroyPQExpBuffer(query);
}

static void
dumpNodeGroups(PGconn *conn)
{
	PQExpBuffer query;
	PGresult   *res;
	int			num;
	int			i;

	query = createPQExpBuffer();

	appendPQExpBuffer(query,
						"select 'CREATE NODE GROUP ' || pgxc_group.group_name"
						" || ' WITH(' || string_agg(node_name,',') || ');'"
						" as group_query from pg_catalog.pgxc_node, pg_catalog.pgxc_group"
						" where pgxc_node.oid = any (pgxc_group.group_members)"
						" group by pgxc_group.group_name"
						" order by pgxc_group.group_name");

	res = executeQuery(conn, query->data);

	num = PQntuples(res);

	if (num > 0)
		fprintf(OPF, "--\n-- Node groups\n--\n\n");

	for (i = 0; i < num; i++)
	{
		fprintf(OPF, "%s\n", PQgetvalue(res, i, PQfnumber(res, "group_query")));
	}
	fprintf(OPF, "\n");

	PQclear(res);
	destroyPQExpBuffer(query);
}
#endif
