/*
 * ------------------------------------------------------------------------
 *
 * pgxc_clean utility
 *
 *	Recovers outstanding 2PC when after crashed nodes or entire cluster
 *  is recovered.
 *
 *  Depending upon how nodes/XC cluster fail, there could be outstanding
 *  2PC transactions which are partly prepared and partly commited/borted.
 *  Such transactions must be commited/aborted to remove them from the
 *  snapshot.
 *
 *  This utility checks if there's such outstanding transactions and
 *  cleans them up.
 *
 * Command syntax
 *
 * pgxc_clean [option ... ] [database] [user]
 *
 * Options are:
 *
 *  -a, --all				cleanup all the database avilable
 *  -d, --dbname=DBNAME		database name to clean up.   Multiple -d option
 *                          can be specified.
 *  -h, --host=HOSTNAME		Coordinator hostname to connect to.
 *  -N, --no-clean			only test.  no cleanup actually.
 *  -o, --output=FILENAME	output file name.
 *  -p, --port=PORT			Coordinator port number.
 *  -q, --quiet				do not print messages except for error, default.
 *  -s, --status			prints out 2PC status.
 *  -U, --username=USERNAME	database user name
 *  -v, --verbose			same as -s, plus prints result of each cleanup.
 *  -V, --version			prints out the version,
 *  -w, --no-password		never prompt for the password.
 *  -W, --password			prompt for the password,
 *  -?, --help				prints help message
 *
 * ------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <pwd.h>
#include <errno.h>
#include <string.h>
#include "libpq-fe.h"
#include "pg_config.h"
#include "getopt_long.h"
#include "pgxc_clean.h"
#include "txninfo.h"
#include "port.h"

/* Who I am */
const char *progname;
char *my_nodename;
int   my_nodeidx = -1;		/* Index in pgxc_clean_node_info */

/* Databases to clean */
bool clean_all_databases = false;		/* "--all" overrides specific database specification */

database_names *head_database_names = NULL;
database_names *last_database_names = NULL;

/* Coordinator to connect to */
char *coordinator_host = NULL;
int coordinator_port = -1;

typedef enum passwd_opt
{
	TRI_DEFAULT,
	TRI_YES,
	TRI_NO
} passwd_opt;

/* Miscellaneous */
char *output_filename = NULL;
char *username = NULL;
bool version_opt = false;
passwd_opt try_password_opt = TRI_DEFAULT;
bool status_opt = false;
bool no_clean_opt = false;
bool verbose_opt = false;
FILE *outf;
FILE *errf;

/* Global variables */
node_info	*pgxc_clean_node_info;
int			pgxc_clean_node_count;

database_info *head_database_info;
database_info *last_database_info;

static char *password = NULL;
static char password_prompt[256];

/* Funcs */
static void add_to_database_list(char *dbname);
static void parse_pgxc_clean_options(int argc, char *argv[]);
static void usage(void);
static char *GetUserName(void);
static void showVersion(void);
static PGconn *loginDatabase(char *host, int port, char *user, char *password,
							char *dbname, const char *progname, char *encoding, char *password_prompt);
static void getMyNodename(PGconn *conn);
static void recover2PCForDatabase(database_info *db_info);
static void recover2PC(PGconn *conn, txn_info *txn);
static void getDatabaseList(PGconn *conn);
static void getNodeList(PGconn *conn);
static void showVersion(void);
static void add_to_database_list(char *dbname);
static void parse_pgxc_clean_options(int argc, char *argv[]);
static void usage(void);
static void getPreparedTxnList(PGconn *conn);
static void getTxnInfoOnOtherNodesAll(PGconn *conn);
static void do_commit(PGconn *conn, txn_info *txn);
static void do_abort(PGconn *conn, txn_info *txn);
static void do_commit_abort(PGconn *conn, txn_info *txn, bool is_commit);
static bool setMaintenanceMode(PGconn *conn);

/*
 * Connection to the Coordinator
 */
PGconn *coord_conn;

/*
 *
 * Main
 *
 */
int main(int argc, char *argv[])
{

	/* Should setup pglocale when it is supported by XC core */

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			showVersion();
			exit(0);
		}
	}
	parse_pgxc_clean_options(argc, argv);

	/*
	 * Check missing arguments
	 */
	if (clean_all_databases == false && head_database_names == NULL)
	{
		fprintf(stderr, "%s: you must specify -a or -d option.\n", progname);
		exit(1);
	}

	/*
	 * Arrange my environment
	 */
	if (output_filename)
	{
		/* Prepare output filename */
		outf = fopen(output_filename, "w");
		if (outf == NULL)
		{
			fprintf(stderr, "%s: Cannot ope output file %s (%s)\n", progname, output_filename, strerror(errno));
			exit(1);
		}
		errf = outf;
	}
	else
	{
		outf = stdout;
		errf = stderr;
	}
	if (coordinator_host == NULL)
	{
		/* Default Coordinator host */
		if ((coordinator_host = getenv("PGHOST")) == NULL)
			coordinator_host = "localhost";
	}
	if (coordinator_port == -1)
	{
		/* Default Coordinator port */
		char *pgport;

		if ((pgport = getenv("PGPORT")) == NULL)
			coordinator_port = DEF_PGPORT;		/* pg_config.h */
		else
			coordinator_port = atoi(pgport);
	}
	if (username == NULL)
		strcpy(password_prompt, "Password: ");
	else
		sprintf(password_prompt, "Password for user %s: ", username);
	if (try_password_opt == TRI_YES)
		password = simple_prompt(password_prompt, 100, false);

	if (verbose_opt)
	{
		/* Print environments */
		fprintf(outf, "%s (%s): Cleanup outstanding 2PCs.\n", progname, PG_VERSION);
		/* Target databaess */
		fprintf(outf, "Target databases:");
		if (clean_all_databases)
			fprintf(outf, "(ALL)\n");
		else
		{
			database_names *cur_name;

			for(cur_name = head_database_names; cur_name; cur_name = cur_name->next)
				fprintf(outf, " %s", cur_name->database_name);
			fprintf(outf, "\n");
		}
		/* Username to use */
		fprintf(outf, "Username: %s\n", username ? username : "default");
		/* Status opt */
		fprintf(outf, "Status opt: %s\n", status_opt ? "on" : "off");
		/* No-dlean opt */
		fprintf(outf, "no-clean: %s\n", no_clean_opt ? "on" : "off");
	}

	/* Tweak options --> should be improved in the next releases */
	if (status_opt)
		verbose_opt = true;
	/* Connect to XC server */
	if (verbose_opt)
	{
		fprintf(outf, "%s: connecting to database \"%s\", host: \"%s\", port: %d\n",
				progname,
				clean_all_databases ? "postgres" : head_database_names->database_name,
				coordinator_host, coordinator_port);
	}
	coord_conn = loginDatabase(coordinator_host, coordinator_port, username, password,
							   clean_all_databases ? "postgres" : head_database_names->database_name,
							   progname, "auto", password_prompt);
	if (verbose_opt)
	{
		fprintf(outf, "%s: connected successfully\n", progname);
	}

	/*
	 * Get my nodename (connected Coordinator)
	 */
	getMyNodename(coord_conn);
	if (verbose_opt)
	{
		fprintf(outf, "%s: Connected to the node \"%s\"\n", progname, my_nodename);
	}

	/*
	 * Get available databases
	 *
	 * pgxc_clean assumes that all the database are available from the connecting Coordinator.
	 * Some (expert) DBA can create a database local to subset of the node by EXECUTE DIRECT.
	 * In this case, DBA may have to clean outstanding 2PC transactions manually or clean
	 * 2PC transactions by connecting pgxc_clean to different Coordinators.
	 *
	 * If such node-subset database is found to be used widely, pgxc_clean may need
	 * an extension to deal with this case.
	 */
	if (clean_all_databases)
		getDatabaseList(coord_conn);
	if (verbose_opt)
	{
		database_info *cur_database;

		fprintf(outf, "%s: Databases visible from the node \"%s\": ", progname, my_nodename);

		if (head_database_info)
		{
			for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
			{
				fprintf(outf, " \"%s\"", cur_database->database_name);
			}
			fputc('\n', outf);
		}
	}

	/*
	 * Get list of Coordinators
	 *
	 * As in the case of database, we clean transactions in visible nodes from the
	 * connecting Coordinator. DBA can also setup different node configuration
	 * at different Coordinators. In this case, DBA should be careful to choose
	 * appropriate Coordinator to clean up transactions.
	 */
	getNodeList(coord_conn);
	if (verbose_opt)
	{
		int ii;

		fprintf(outf, "%s: Node list visible from the node \"%s\"\n", progname, my_nodename);

		for (ii = 0; ii < pgxc_clean_node_count; ii++)
		{
			fprintf(outf, "Name: %s, host: %s, port: %d, type: %s\n",
					pgxc_clean_node_info[ii].node_name,
					pgxc_clean_node_info[ii].host,
					pgxc_clean_node_info[ii].port,
					pgxc_clean_node_info[ii].type == NODE_TYPE_COORD ? "coordinator" : "datanode");
		}
	}

	/*
	 * Get list of prepared statement
	 */
	getPreparedTxnList(coord_conn);

	/*
	 * Check if there're any 2PC candidate to recover
	 */
	if (!check2PCExists())
	{
		fprintf(errf, "%s: There's no prepared 2PC in this cluster.  Exiting.\n", progname);
		exit(0);
	}


	/*
	 * Check status of each prepared transaction.  To do this, look into
	 * nodes where the transaction is not recorded as "prepared".
	 * Possible status are unknown (prepare has not been issued), committed or
	 * aborted.
	 */
	getTxnInfoOnOtherNodesAll(coord_conn);
	if (verbose_opt)
	{
		/* Print all the prepared transaction list */
		database_info *cur_db;

		fprintf(outf, "%s: 2PC transaction list.\n", progname);
		for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
		{
			txn_info *txn;

			fprintf(outf, "Database: \"%s\":\n", cur_db->database_name);

			for (txn = cur_db->head_txn_info; txn; txn = txn->next)
			{
				int ii;

				fprintf(outf, "    gxid: %d, xid: \"%s\", owner: %s\n", txn->gxid, txn->xid, txn->owner);
				for (ii = 0; ii < pgxc_clean_node_count; ii++)
				{
					fprintf(outf, "        node: %s, status: %s\n",
							pgxc_clean_node_info[ii].node_name,
							str_txn_stat(txn->txn_stat[ii]));
				}
			}
		}
	}

	/*
	 * Then disconnect from the database.
	 * I need to login to specified databases which 2PC is issued for.  Again, we assume
	 * that all the prepare is issued against the same database in each node, which
	 * current Coordinator does and there seems to be no way to violate this assumption.
	 */
	if (verbose_opt)
	{
		fprintf(outf, "%s: disconnecting\n", progname);
	}
	PQfinish(coord_conn);

	/*
	 * If --no-clean option is specified, we exit here.
	 */
	if (no_clean_opt)
	{
		fprintf(outf, "--no-clean opt is specified. Exiting.\n");
		exit(0);
	}

	/*
	 * Recover 2PC for specified databases
	 */
	if (clean_all_databases)
	{
		database_info *cur_database_info;

		for(cur_database_info = head_database_info; cur_database_info; cur_database_info = cur_database_info->next)
		{
			recover2PCForDatabase(cur_database_info);
		}
	}
	else
	{
		database_info *cur_database_info;
		database_names *cur_database_name;

		for(cur_database_name = head_database_names; cur_database_name; cur_database_name = cur_database_name->next)
		{
			cur_database_info = find_database_info(cur_database_name->database_name);
			if (cur_database_info)
			{
				recover2PCForDatabase(cur_database_info);
			}
		}
	}
	exit(0);
}

static void
getMyNodename(PGconn *conn)
{
	static const char *stmt = "SELECT pgxc_node_str()";
	PGresult *res;

	res = PQexec(conn, stmt);

	/* Error handling here */
	if (res)
		my_nodename = strdup(PQgetvalue(res, 0, 0));
	else
		my_nodename = strdup("unknown");

	PQclear(res);
}

static void
recover2PCForDatabase(database_info *db_info)
{
	PGconn 		*coord_conn;
	txn_info   	*cur_txn;

	if (verbose_opt)
		fprintf(outf, "%s: recovering 2PC for database \"%s\"\n", progname, db_info->database_name);
	coord_conn = loginDatabase(coordinator_host, coordinator_port, username, password, db_info->database_name,
							   progname, "auto", password_prompt);
	if (coord_conn == NULL)
	{
		fprintf(errf, "Could not connect to the database %s.\n", db_info->database_name);
		return;
	}
	if (!setMaintenanceMode(coord_conn))
	{
		/* Cannot recover */
		fprintf(errf, "Skipping database %s.\n", db_info->database_name);
		PQfinish(coord_conn);
		return;
	}
	if (verbose_opt)
		fprintf(outf, "%s: connected to the database \"%s\"\n", progname, db_info->database_name);
	for(cur_txn = db_info->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	{
		recover2PC(coord_conn, cur_txn);
	}
	PQfinish(coord_conn);
}

static void
recover2PC(PGconn *conn, txn_info *txn)
{
	TXN_STATUS txn_stat;

	txn_stat = check_txn_global_status(txn);
	if (verbose_opt)
	{
		fprintf(outf, "    Recovering TXN: gxid: %d, xid: \"%s\", owner: \"%s\", global status: %s\n",
				txn->gxid, txn->xid, txn->owner, str_txn_stat(txn_stat));
	}
	switch (txn_stat)
	{
		case TXN_STATUS_FAILED:
			if (verbose_opt)
				fprintf(outf, "        Recovery not needed.\n");
			return;
		case TXN_STATUS_PREPARED:
			if (verbose_opt)
				fprintf(outf, "        Recovery not needed.\n");
			return;
		case TXN_STATUS_COMMITTED:
			do_commit(conn, txn);
			return;
		case TXN_STATUS_ABORTED:
			do_abort(conn, txn);
			return;
		default:
			fprintf(stderr, "        Unknown TXN status, pgxc_clean error.\n");
			exit(1);
	}
	return;
}

static void
do_commit(PGconn *conn, txn_info *txn)
{
	do_commit_abort(conn, txn, true);
}

static void
do_abort(PGconn *conn, txn_info *txn)
{
	do_commit_abort(conn, txn, false);
}

static void
do_commit_abort(PGconn *conn, txn_info *txn, bool is_commit)
{
	int ii;
	static const char *EXEC_DIRECT_STMT_FMT = "EXECUTE DIRECT ON (%s) '%s PREPARED ''%s'';';";
	static const char *GLOBAL_STMT_FMT = "%s PREPARED '%s';";
	char stmt[1024];
	PGresult *res;
	ExecStatusType res_status;

	if (verbose_opt)
		fprintf(outf, "    %s... ", is_commit ? "committing" : "aborting");
	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		if (txn->txn_stat[ii] == TXN_STATUS_PREPARED && ii != my_nodeidx)
		{

			sprintf(stmt, EXEC_DIRECT_STMT_FMT,
					pgxc_clean_node_info[ii].node_name,
					is_commit ? "COMMIT" : "ROLLBACK",
					txn->xid);
			res = PQexec(conn, stmt);
			res_status = PQresultStatus(res);
			if (verbose_opt)
			{
				if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
					fprintf(outf, "succeeded (%s), ", pgxc_clean_node_info[ii].node_name);
				else
					fprintf(outf, "failed (%s: %s), ",
							pgxc_clean_node_info[ii].node_name,
							PQresultErrorMessage(res));
			}
			else
			{
				if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
				{
					fprintf(errf, "Failed to recover TXN, gxid: %d, xid: \"%s\", owner: \"%s\", node: \"%s\" (%s)\n",
							txn->gxid, txn->xid, txn->owner, pgxc_clean_node_info[ii].node_name,
							PQresultErrorMessage(res));
				}
			}
			PQclear(res);
		}
	}
	/* Issue global statment */
	sprintf(stmt, GLOBAL_STMT_FMT,
			is_commit ? "COMMIT" : "ROLLBACK",
			txn->xid);
	res = PQexec(conn, stmt);
	res_status = PQresultStatus(res);
	if (verbose_opt)
	{
		if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
			fprintf(outf, "succeeded (%s)\n", my_nodename);
		else
			fprintf(outf, "failed (%s: %s)\n",
					my_nodename,
					PQresultErrorMessage(res));
	}
	else if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
	{
		fprintf(errf, "Failed to recover TXN, gxid: %d, xid: \"%s\", owner: \"%s\", node: \"%s\" (%s)\n",
				txn->gxid, txn->xid, txn->owner, my_nodename, PQresultErrorMessage(res));
	}
	PQclear(res);
}

#if 0
static database_info *
find_database_info(char *dbname)
{
	database_info *cur_database_info;

	for(cur_database_info = head_database_info; cur_database_info; cur_database_info = cur_database_info->next)
	{
		if (strcmp(cur_database_info->database_name, dbname) == 0)
			return(cur_database_info);
	}
	return(NULL);
}
#endif


static PGconn *
loginDatabase(char *host, int port, char *user, char *password, char *dbname, const char *progname, char *encoding, char *password_prompt)
{
	bool new_pass = false;
	PGconn *coord_conn;
	char port_s[32];
#define PARAMS_ARRAY_SIZE 8
	const char *keywords[PARAMS_ARRAY_SIZE];
	const char *values[PARAMS_ARRAY_SIZE];

	sprintf(port_s, "%d", port);

	keywords[0] = "host";
	values[0] = host;
	keywords[1] = "port";
	values[1] = port_s;
	keywords[2] = "user";
	values[2] = user;
	keywords[3] = "password";
	keywords[4] = "dbname";
	values[4] = dbname;
	keywords[5] = "fallback_application_name";
	values[5] = progname;
	keywords[6] = "client_encoding";
	values[6] = encoding;
	keywords[7] = NULL;
	values[7] = NULL;

	/* Loop until we have a password if requested by backend */
	do
	{
		values[3] = password;

		new_pass = false;
		coord_conn = PQconnectdbParams(keywords, values, true);

		if (PQstatus(coord_conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(coord_conn) &&
			password == NULL &&
			try_password_opt != TRI_NO)
		{
			PQfinish(coord_conn);
			password = simple_prompt(password_prompt, 100, false);
			new_pass = true;
		}
	} while (new_pass);

	return(coord_conn);
}


static TXN_STATUS
getTxnStatus(PGconn *conn, GlobalTransactionId gxid, int node_idx)
{
	char *node_name;
	char stmt[1024];
	PGresult *res;
	char *res_s;

	static const char *STMT_FORM = "EXECUTE DIRECT ON (%s) 'SELECT pgxc_is_committed(''%d''::xid);'";

	node_name = pgxc_clean_node_info[node_idx].node_name;
	sprintf(stmt, STMT_FORM, node_name, gxid);

	res = PQexec(conn, stmt);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain transaction status for node %s, gxid %d\n", node_name, gxid);
		exit(1);
	}
	if (PQgetisnull(res, 0, 0))
		return TXN_STATUS_UNKNOWN;
	res_s = PQgetvalue(res, 0, 0);
	if (strcmp(res_s, "t") == 0)
		return TXN_STATUS_COMMITTED;
	else
		return TXN_STATUS_ABORTED;
}

static void
getTxnInfoOnOtherNodes(PGconn *conn, txn_info *txn)
{
	int ii;

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		if (txn->txn_stat[ii] == TXN_STATUS_INITIAL)
			txn->txn_stat[ii] = getTxnStatus(conn, txn->gxid, ii);
	}
}


static void
getTxnInfoOnOtherNodesForDatabase(PGconn *conn, database_info *database)
{
	txn_info *cur_txn;

	for (cur_txn = database->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	{
		getTxnInfoOnOtherNodes(conn, cur_txn);
	}
}


static void
getTxnInfoOnOtherNodesAll(PGconn *conn)
{
	database_info *cur_database;

	for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
	{
		getTxnInfoOnOtherNodesForDatabase(conn, cur_database);
	}
}



static void
getPreparedTxnListOfNode(PGconn *conn, int idx)
{
	int prep_txn_count;
	int ii;
	PGresult *res;
	ExecStatusType pq_status;

#define MAX_STMT_LEN 1024

	/* SQL Statement */
	static const char *STMT_GET_PREP_TXN_ON_NODE
		= "EXECUTE DIRECT ON (%s) 'SELECT TRANSACTION, GID, OWNER, DATABASE FROM PG_PREPARED_XACTS;'";
	char stmt[MAX_STMT_LEN];

	sprintf(stmt, STMT_GET_PREP_TXN_ON_NODE,
			pgxc_clean_node_info[idx].node_name);

	res = PQexec(conn, stmt);
	if (res == NULL || (pq_status = PQresultStatus(res)) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain prepared transaction list for node %s.(%s)\n",
				pgxc_clean_node_info[idx].node_name, res ? PQresultErrorMessage(res) : "");
		PQclear(res);
		exit (1);
	}
	prep_txn_count = PQntuples(res);
	for (ii = 0; ii < prep_txn_count; ii++)
	{
		GlobalTransactionId gxid;
		char *xid;
		char *owner;
		char *database_name;

		gxid = atoi(PQgetvalue(res, ii, 0));
		xid = strdup(PQgetvalue(res, ii, 1));
		owner = strdup(PQgetvalue(res, ii, 2));
		database_name = strdup(PQgetvalue(res, ii, 3));

		add_txn_info(database_name, pgxc_clean_node_info[idx].node_name, gxid, xid, owner,
					 TXN_STATUS_PREPARED);
		if(xid)
			 free(xid);
		if (owner)
			free(owner);
		if (database_name)
			free(database_name);
	}
	PQclear(res);
}

static void
getPreparedTxnList(PGconn *conn)
{
	int ii;

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		getPreparedTxnListOfNode(conn, ii);
	}
}

static void
getDatabaseList(PGconn *conn)
{
	int database_count;
	int ii;
	PGresult *res;
	char *dbname;

	/* SQL Statement */
	static const char *STMT_GET_DATABASE_LIST = "SELECT DATNAME FROM PG_DATABASE;";

	/*
	 * Get database list
	 */
	res = PQexec(conn, STMT_GET_DATABASE_LIST);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain database list.\n");
		PQclear(res);
		exit (1);
	}
	database_count = PQntuples(res);
	for(ii = 0; ii < database_count; ii++)
	{
		dbname = PQgetvalue(res, ii, 0);
		if (strcmp(dbname, "template0") == 0)
			/* Skip template0 database */
			continue;
		add_database_info(dbname);
	}
	PQclear(res);
}

static void
getNodeList(PGconn *conn)
{
	int ii;
	PGresult *res;

	/* SQL Statement */
	static const char *STMT_GET_NODE_INFO = "SELECT NODE_NAME, NODE_TYPE, NODE_PORT, NODE_HOST FROM PGXC_NODE;";

	res = PQexec(conn, STMT_GET_NODE_INFO);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain node list.\n");
		PQclear(res);
		exit (1);
	}
	pgxc_clean_node_count = PQntuples(res);
	pgxc_clean_node_info = (node_info *)calloc(pgxc_clean_node_count, sizeof(node_info));
	if (pgxc_clean_node_info == NULL)
	{
		fprintf(stderr, "No more memory.\n");
		exit(1);
	}

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		char *node_name;
		char *node_type_c;
		NODE_TYPE node_type;
		int  port;
		char *host;

		node_name = strdup(PQgetvalue(res, ii, 0));
		node_type_c = strdup(PQgetvalue(res, ii, 1));
		switch (node_type_c[0])
		{
			case 'C':
				/* pgxc_clean has to connect to the Coordinator */
				node_type = NODE_TYPE_COORD;
				if (strcmp(node_name, my_nodename) == 0)
					my_nodeidx = ii;
				break;
			case 'D':
				node_type = NODE_TYPE_DATANODE;
				break;
			default:
				fprintf(stderr, "Invalid catalog data (node_type), node_name: %s, node_type: %s\n", node_name, node_type_c);
				exit(1);
		}
		port = atoi(PQgetvalue(res, ii, 2));
		host = strdup(PQgetvalue(res, ii, 3));
		set_node_info(node_name, port, host, node_type, ii);

		if (node_name)
			free(node_name);
		if (node_type_c)
			free(node_type_c);
		if (host)
			free(host);
	}
	/* Check if local Coordinator has been found */
	if (my_nodeidx == -1)
	{
		fprintf(stderr, "Failed to identify the coordinator which %s is connecting to.  ", progname);
		fprintf(stderr, "Connecting to a wrong node.\n");
		exit(1);
	}
}



static void
showVersion(void)
{
	puts("pgxc_clean (Postgres-XC) " PGXC_VERSION);
}

static void
add_to_database_list(char *dbname)
{
	if (head_database_names == NULL)
	{
		head_database_names = last_database_names = (database_names *)malloc(sizeof(database_names));
		if (head_database_names == NULL)
		{
			fprintf(stderr, "No more memory, FILE:%s, LINE:%d.\n", __FILE__, __LINE__);
			exit(1);
		}
	}
	else
	{
		last_database_names->next = (database_names *)malloc(sizeof(database_names));
		if (last_database_names->next == NULL)
		{
			fprintf(stderr, "No more memory, FILE:%s, LINE:%d.\n", __FILE__, __LINE__);
			exit(1);
		}
		last_database_names = last_database_names->next;
	}
	last_database_names->next = NULL;
	last_database_names->database_name = dbname;
}

static void
parse_pgxc_clean_options(int argc, char *argv[])
{
	static struct option long_options[] =
	{
		{"all", no_argument, NULL, 'a'},
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"no-clean", no_argument, NULL, 'N'},
		{"output", required_argument, NULL, 'o'},
		{"port", required_argument, NULL, 'p'},
		{"quiet", no_argument, NULL, 'q'},
		{"username", required_argument, NULL, 'U'},
		{"verbose", no_argument, NULL, 'v'},
		{"version", no_argument, NULL, 'V'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"help", no_argument, NULL, '?'},
		{"status", no_argument, NULL, 's'},
		{NULL, 0, NULL, 0}
	};

	int optindex;
	extern char *optarg;
	extern int optind;
	int c;

	progname = get_progname(argv[0]);		/* Should be more fancy */

	while ((c = getopt_long(argc, argv, "ad:h:No:p:qU:vVwWs?", long_options, &optindex)) != -1)
	{
		switch(c)
		{
			case 'a':
				clean_all_databases = true;
				break;
			case 'd':
				add_to_database_list(optarg);
				break;
			case 'h':
				coordinator_host = optarg;
				break;
			case 'N':
				no_clean_opt = true;
				break;
			case 'o':
				output_filename = optarg;
				break;
			case 'p':
				coordinator_port = atoi(optarg);
				break;
			case 'q':
				verbose_opt = false;
				break;
			case 'U':
				username = optarg;
				break;
			case 'V':
				version_opt = 0;
				break;
			case 'v':
				verbose_opt = true;
				break;
			case 'w':
				try_password_opt = TRI_NO;
				break;
			case 'W':
				try_password_opt = TRI_YES;
				break;
			case 's':
				status_opt = true;
				break;
			case '?':
				if (strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0)
				{
					usage();
					exit(0);
				}
				else
				{
					fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
					exit(1);
				}
				break;
			default:
				fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
				exit(1);
				break;
		}
	}

	while (argc - optind >= 1)
	{
		if (head_database_names == NULL)
		{
			if (strcmp(argv[optind], "template0") == 0)
			{
				fprintf(stderr, "%s: You should not clean template0 database.\n", progname);
				exit(1);
			}
			add_to_database_list(argv[optind]);
		}
		if (username == NULL)
			username = argv[optind];
		else
			fprintf(stderr, "%s: warning: extra command-line argument \"%s\" ignored\n",
					progname, argv[optind]);
		optind++;
	}

	if (!clean_all_databases && head_database_names == NULL)
	{
		fprintf(stderr, "Please specify at least one database or -a for all\n");
		exit(1);
	}
}

static bool setMaintenanceMode(PGconn *conn)
{
	static const char *SetMaintenanceModeCommand = "SET xc_maintenance_mode = on;";
	PGresult *res;
	ExecStatusType res_status;

	res = PQexec(conn, SetMaintenanceModeCommand);
	res_status = PQresultStatus(res);
	if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
		return true;
	else
		fprintf(errf, "Failed to set xc_maintenance_mode. (%s)\n",
				PQresultErrorMessage(res));
	return false;
}

static char *GetUserName(void)
{
	struct passwd *passwd;

	passwd = getpwuid(getuid());
	if (passwd)
		return(strdup(passwd->pw_name));
	else
	{
		fprintf(stderr, "%s: could not get current user name: %s\n", progname, strerror(errno));
		exit(1);
	}
	return NULL;
}

static void usage(void)
{
	char *env;
	char *user;

	user = getenv("PGUSER");
	if (!user)
		user = GetUserName();

	printf("pgxc_clean cleans up outstanding 2PCs after failed node is recovered.\n"
		   "Usage:\n"
		   "pgxc_clean [OPTION ...] [DBNAME [USERNAME]]\n\n"
		   "Options:\n");

	env = getenv("PGDATABASE");
	if (!env)
		env = user;
	printf("  -a, --all                cleanup all the databases available.\n");
	printf("  -d, --dbname=DBNAME      database name to clean up (default: \"%s\")\n", env);
	env = getenv("PGHOST");
	printf("  -h, --host=HOSTNAME      target coordinator host address, (default: \"%s\")\n", env ? env : "local socket");
	printf("  -N, no-clean             only collect 2PC information.  Do not recover them\n");
	printf("  -o, --output=FILENAME    output file name.\n");
	env = getenv("PGPORT");
	printf("  -p, --port=PORT          port number of the coordinator (default: \"%s\")\n", env ? env : DEF_PGPORT_STR);
	printf("  -q, --quiet              quiet mode.  do not print anything but error information.\n");
	printf("  -s, --status             prints out 2PC status\n");
	env = getenv("PGUSER");
	if (!env)
		env = user;
	printf("  -U, --username=USERNAME  database user name (default: \"%s\")\n", env);
	printf("  -v, --verbose            print recovery information.\n");
	printf("  -V, --version            prints out the version.\n");
	printf("  -w, --no-password        never prompt for the password.\n");
	printf("  -W, --password           prompt for the password.\n");
	printf("  -?, --help               print this message.\n");
}
