/*-------------------------------------------------------------------------
 *
 * options.h
 *	  recv_alerts options parsing
 *
 *	  This header is private to recv_alerts.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef ALERT_OPTIONS_H
#define ALERT_OPTIONS_H

#define MAX_NODES 64
#define QUERY_STR "SELECT CONCAT('tcp:',HOST,':',port+2000) " \
				  "FROM pipelinedb_enterprise.shards a " \
				  "INNER JOIN pipelinedb_enterprise.nodes b ON " \
				  "(a.owner=b.name AND a.continuous_view = '%s')"

#define die(format, ...) \
{ \
	fprintf(stderr, "pipeline-recv-alerts: ERROR: " format "\n", \
		##__VA_ARGS__); \
	exit(1); \
}

#define die_with_help(format, ...) \
{ \
	fprintf(stderr, "pipeline-recv-alerts: ERROR: " format "\n", \
		##__VA_ARGS__); \
	fprintf(stderr, "Try \"pipeline-recv_alerts --help\" for " \
			"more information.\n"); \
	exit(1); \
}

#define PUSH_DEBUG 0

#define print_to_err(format, ...) { fprintf (stderr, format "\n", ##__VA_ARGS__); }

#if PUSH_DEBUG
	#define debugmsg(format, ...) { fprintf (stdout, "%.6f " format "\n", get_time(), ##__VA_ARGS__); fflush(stdout); }
#else
	#define debugmsg(format, ...)
#endif

static const char *get_host_name(void);
static const char *get_dbname(void);
static const char *get_port(void);
static const char *get_user_name(void);

typedef struct IPCDesc
{
	char *str;
	char *host;
	uint16_t port;
} IPCDesc;

typedef struct Args
{
	struct
	{
		char *user;
		char *host;
		char *port;
		char *dbname;

		bool no_password;
		bool force_password;
	} conn;

	char *subs;
	int num_nodes;
	IPCDesc nodes[MAX_NODES];
} Args;

typedef struct CommandArgs
{
	char **data;
	int n;

} CommandArgs;

/*
 * print_usage
 */
static void
print_usage()
{
	printf("pipeline-recv-alerts is the PipelineDB tool for receiving alerts.\n\n");
	printf("Usage:\n");
	printf("  pipeline-recv-alerts -a alert_name [OPTION]...\n\n");

	printf("General options:\n");
	printf("  -a, --alert=ALERTNAME   alert name to subscribe to (view_name.trigger_name)\n");
	printf("\n");
	printf("Connection options:\n");
	printf("  -d, --dbname=DBNAME     database name to connect to (default: \"%s\")\n",
			get_dbname());
	printf("  -h, --host=HOSTNAME     database server host (default: \"%s\")\n",
			get_host_name());
	printf("  -p, --port=PORT         database server port (default: \"%s\")\n",
			get_port());
	printf("  -U, --username=USERNAME database user name (default: \"%s\")\n",
			get_user_name());
	printf("  -w, --no-password       never prompt for password\n");
	printf("  -W, --password          force password prompt\n");
}

/*
 * get_time
 */
static double
get_time()
{
	struct timeval tv;
	gettimeofday(&tv, 0);

	return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

/*
 * get_user_name
 *
 * get default db host name to display in usage
 */
static const char *
get_host_name()
{
	const char *host = getenv("PGHOST");

	if (host)
		return host;

	return "local socket";
}

/*
 * get_dbname
 *
 * get default db name to display in usage
 */
static const char *
get_dbname()
{
	const char *dbname = getenv("PGDATABASE");

	if (dbname)
		return dbname;

	return get_user_name();
}

/*
 * get_port
 *
 * get default db port to display in usage
 */
static const char *
get_port()
{
	const char *port = getenv("PGPORT");

	if (port)
		return port;

	return "5432";
}

/*
 * get_user_name
 *
 * get default db user name to display in usage
 */
static const char *
get_user_name()
{
	struct passwd *pw;
	uid_t user_id = geteuid();

	const char *user = getenv("PGUSER");

	if (user)
		return user;

	user_id = geteuid();
	pw = getpwuid(user_id);

	if (!pw)
		return NULL;

	return pw->pw_name;
}

/*
 * read_first_page
 *
 * Read first 4096 bytes of fd and append into string info
 */
static void
read_first_page(StringInfo info, int fd)
{
	char buf[4096];
	int nbytes = read(fd, buf, sizeof(buf));

	if (nbytes > 0)
		appendStringInfo(info, buf, nbytes);
}

/*
 * append_arg_val
 *
 * Append an argument and a value to supplied command args
 */
static void
append_arg_val(CommandArgs *args, const char *flag, const char *val)
{
	int index = args->n;

	args->n += 2;
	args->data = realloc(args->data, (args->n+1) * sizeof(char *));

	Assert(args->data);

	args->data[index] = strdup(flag);
	args->data[index+1] = strdup(val);
	args->data[args->n] = NULL;
}

/*
 * append_arg
 *
 * Append an argument to supplied command args
 */
static void
append_arg(CommandArgs *args, const char *flag)
{
	int index = args->n;

	args->n += 1;
	args->data = realloc(args->data, (args->n+1) * sizeof(char *));

	args->data[index] = strdup(flag);
	args->data[args->n] = NULL;
}

/*
 * make_pipeline_args
 *
 * Builds the command line args to execve the pipeline binary with.
 */
static CommandArgs *
make_pipeline_args(Args *args)
{
	char *copy = 0;
	const char *view_name = 0;
	char *saveptr = 0;
	StringInfo qcmd_str;

	CommandArgs *cmd_args = palloc0(sizeof(CommandArgs));

	append_arg(cmd_args, "pipeline");

	if (args->conn.user)
		append_arg_val(cmd_args, "-U", args->conn.user);

	if (args->conn.host)
		append_arg_val(cmd_args, "-h", args->conn.host);

	if (args->conn.port)
		append_arg_val(cmd_args, "-p", args->conn.port);

	if (args->conn.dbname)
		append_arg_val(cmd_args, "-d", args->conn.dbname);

	if (args->conn.no_password)
		append_arg(cmd_args, "-w");

	if (args->conn.force_password)
		append_arg(cmd_args, "-W");

	qcmd_str = makeStringInfo();

	copy = strdup(args->subs);
	view_name = strtok_r(copy, ".", &saveptr);

	if (!view_name)
		view_name = "null";

	appendStringInfo(qcmd_str, "copy (" QUERY_STR ") to stdout;", view_name);
	append_arg_val(cmd_args, "-c", qcmd_str->data);

	return cmd_args;
}

/*
 * parse_ipc_desc
 *
 * Parse strings like tcp:host:port into an IPCDesc structure
 */
static void
parse_ipc_desc(IPCDesc *desc, const char* s)
{
	int p = 0;
	char *copy = strdup(s);
	char *saveptr = 0;

	char *schema = strtok_r(copy, "/:", &saveptr);
	char *host = strtok_r(0, "/:", &saveptr);
	char *port = strtok_r(0, "/:", &saveptr);

	if (!schema || !host || !port)
		die_with_help("could not parse protocol %s", s);

	if (strcmp(schema, "tcp") != 0)
		die_with_help("invalid schema in %s", s);

	p = atoi(port);

	if (!(p >= 0 && p <= 65535))
		die_with_help("invalid port in %s", s);

	desc->str = copy;
	desc->host = host;
	desc->port = p;
}

/*
 * auto_configure
 *
 * Connect to an instance using the pipeline client and run a
 * query to determine which nodes to connect to.
 */
static void
auto_configure(Args *args)
{
	CommandArgs *cmd_args = make_pipeline_args(args);

	int stdout_pipe_fd[2];
	int stderr_pipe_fd[2];

	pid_t pid;

	if (pipe(stdout_pipe_fd) == -1)
		die("pipe error: %m");

	if (pipe(stderr_pipe_fd) == -1)
		die("pipe error: %m");

	if ((pid = fork()) == -1)
		die("fork error: %m");

	if (pid == 0)
	{
		dup2(stdout_pipe_fd[1], STDOUT_FILENO);
		dup2(stderr_pipe_fd[1], STDERR_FILENO);

		close(stdout_pipe_fd[0]);
		close(stdout_pipe_fd[1]);

		close(stderr_pipe_fd[0]);
		close(stderr_pipe_fd[1]);

		execvp("pipeline", cmd_args->data);

		fprintf(stderr, "could not execute pipeline binary: %m\n");
		exit(1);
	}
	else
	{
		int status = 0;

		StringInfo out_lines = makeStringInfo();
		StringInfo err_lines = makeStringInfo();

		close(stdout_pipe_fd[1]);
		close(stderr_pipe_fd[1]);

		read_first_page(out_lines, stdout_pipe_fd[0]);
		read_first_page(err_lines, stderr_pipe_fd[0]);

		wait(&status);

		if (status != 0 || err_lines->len != 0)
			die_with_help("could not connect to database %s :\n %s",
						  args->conn.dbname ? args->conn.dbname : "",
						  err_lines->len ? err_lines->data : "");

		{
			char *line = 0;
			char *line_ptr = out_lines->data;
			char *saveptr = 0;

			while ((args->num_nodes < MAX_NODES) && (line = strtok_r(line_ptr, "\n", &saveptr)))
			{
				parse_ipc_desc(&args->nodes[args->num_nodes], line);
				args->num_nodes++;
				line_ptr = NULL;
			}
		}
	}
}

/*
 * is_either
 *
 * returns true if key is either m1 or m2
 */
static bool
is_either(const char *key, const char *m1, const char *m2)
{
	return (strcmp(key, m1) == 0) || (strcmp(key, m2) == 0);
}

/*
 * parse_args
 *
 * parse the command line args and fill out the Args struct
 */
static void
parse_args(Args *out, int argc, char **argv)
{
	char **arg = argv + 1;
	memset(out, 0, sizeof(Args));

	while (*arg)
	{
		if (is_either(*arg, "-a", "--alert"))
		{
			arg++;

			if (*arg)
			{
				out->subs = *arg;
				arg++;
			}
		}
		else if (is_either(*arg, "-h", "--host"))
		{
			arg++;

			if (*arg)
			{
				out->conn.host = *arg;
				arg++;
			}
		}
		else if (is_either(*arg, "-p", "--port"))
		{
			arg++;

			if (*arg)
			{
				out->conn.port = *arg;
				arg++;
			}
		}
		else if (is_either(*arg, "-U", "--username"))
		{
			arg++;

			if (*arg)
			{
				out->conn.user = *arg;
				arg++;
			}
		}
		else if (is_either(*arg, "-d", "--dbname"))
		{
			arg++;

			if (*arg)
			{
				out->conn.dbname = *arg;
				arg++;
			}
		}
		else if (is_either(*arg, "-w", "--no-password"))
		{
			arg++;
			out->conn.no_password = true;
		}
		else if (is_either(*arg, "-W", "--password"))
		{
			arg++;
			out->conn.force_password = true;
		}
		else if (strcmp(*arg, "--help") == 0)
		{
			arg++;

			print_usage();
			exit(0);
		}
		else
		{
			die_with_help("unrecognized option '%s'", *arg);
		}
	}
}

/*
 * cleanup_args
 *
 * free the internal data in args
 */
static void
cleanup_args(Args *args)
{
	int i = 0;

	for (i = 0; i < args->num_nodes; ++i)
		free(args->nodes[i].str);
}

#endif
