/*-------------------------------------------------------------------------
 *
 * gtm_ctl --- start/stops/restarts the GTM server/proxy
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"

#include <locale.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "libpq/pqsignal.h"

#define GTM_CONTROL_FILE		"gtm.control"

/* PID can be negative for standalone backend */
typedef long pgpid_t;

typedef enum
{
	SMART_MODE,
	FAST_MODE,
	IMMEDIATE_MODE
} ShutdownMode;


typedef enum
{
	NO_COMMAND = 0,
	START_COMMAND,
	STOP_COMMAND,
	PROMOTE_COMMAND,
	RESTART_COMMAND,
	STATUS_COMMAND,
	RECONNECT_COMMAND
} CtlCommand;

#define DEFAULT_WAIT	60

static bool do_wait = false;
static bool wait_set = false;
static int	wait_seconds = DEFAULT_WAIT;
static bool silent_mode = false;
static ShutdownMode shutdown_mode = SMART_MODE;
static int	sig = SIGTERM;		/* default */
static CtlCommand ctl_command = NO_COMMAND;
static char *gtm_data = NULL;
static char *gtmdata_opt = NULL;
static char *gtm_opts = NULL;
static const char *progname;
static char *log_file = NULL;
static char *control_file = NULL;
static char *gtmdata_D = NULL;
static char *gtm_path = NULL;
static char *gtm_app = NULL;
static char *argv0 = NULL;

static void
write_stderr(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));
static void *pg_malloc(size_t size);
static char *xstrdup(const char *s);
static void do_advice(void);
static void do_help(void);
static void set_mode(char *modeopt);
static void do_start(void);
static void do_stop(void);
static void do_restart(void);
static void do_reconnect(void);
static void print_msg(const char *msg);

static pgpid_t get_pgpid(void);
static char **readfile(const char *path);
static int	start_gtm(void);
static void read_gtm_opts(void);

static bool test_gtm_connection();
static bool gtm_is_alive(pid_t pid);

static void *pg_realloc(void *ptr, size_t size);

static char gtmopts_file[MAXPGPATH];
static char pid_file[MAXPGPATH];
static char conf_file[MAXPGPATH];
static int RunAsDaemon(char *cmd);

/*
 * Write errors to stderr (or by gtm_equal means when stderr is
 * not available).
 */
static void
write_stderr(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);

	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, fmt, ap);
	va_end(ap);
}

/*
 * routines to check memory allocations and fail noisily.
 */

static void *
pg_malloc(size_t size)
{
	void	   *result;

	result = malloc(size);
	if (!result)
	{
		write_stderr(_("%s: out of memory\n"), progname);
		exit(1);
	}
	return result;
}


static char *
xstrdup(const char *s)
{
	char	   *result;

	result = strdup(s);
	if (!result)
	{
		write_stderr(_("%s: out of memory\n"), progname);
		exit(1);
	}
	return result;
}

/*
 * Given an already-localized string, print it to stdout unless the
 * user has specified that no messages should be printed.
 */
static void
print_msg(const char *msg)
{
	if (!silent_mode)
	{
		fputs(msg, stdout);
		fflush(stdout);
	}
}

static pgpid_t
get_pgpid(void)
{
	FILE	   *pidf;
	long		pid;

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		/* No pid file, not an error on startup */
		if (errno == ENOENT)
			return 0;
		else
		{
			write_stderr(_("%s: could not open PID file \"%s\": %s\n"),
						 progname, pid_file, strerror(errno));
			exit(1);
		}
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		write_stderr(_("%s: invalid data in PID file \"%s\"\n"),
					 progname, pid_file);
		exit(1);
	}
	fclose(pidf);
	return (pgpid_t) pid;
}


/*
 * get the lines from a text file - return NULL if file can't be opened
 */
static char **
readfile(const char *path)
{
	FILE	   *infile;
	int			maxlength = 0,
				linelen = 0;
	int			nlines = 0;
	char	  **result;
	char	   *buffer;
	int			c;

	if ((infile = fopen(path, "r")) == NULL)
		return NULL;

	/* pass over the file twice - the first time to size the result */

	while ((c = fgetc(infile)) != EOF)
	{
		linelen++;
		if (c == '\n')
		{
			nlines++;
			if (linelen > maxlength)
				maxlength = linelen;
			linelen = 0;
		}
	}

	/* handle last line without a terminating newline (yuck) */
	if (linelen)
		nlines++;
	if (linelen > maxlength)
		maxlength = linelen;

	/* set up the result and the line buffer */
	result = (char **) pg_malloc((nlines + 1) * sizeof(char *));
	buffer = (char *) pg_malloc(maxlength + 1);

	/* now reprocess the file and store the lines */
	rewind(infile);
	nlines = 0;
	while (fgets(buffer, maxlength + 1, infile) != NULL)
		result[nlines++] = xstrdup(buffer);

	fclose(infile);
	free(buffer);
	result[nlines] = NULL;

	return result;
}



/*
 * start/test/stop routines
 */

static int
start_gtm(void)
{
	char		cmd[MAXPGPATH];
	char		gtm_app_path[MAXPGPATH];
	int			len;

	/*
	 * Since there might be quotes to handle here, it is easier simply to pass
	 * everything to a shell to process them.
	 */

	memset(gtm_app_path, 0, MAXPGPATH);
	memset(cmd, 0, MAXPGPATH);

	/*
	 * Build gtm binary path. We should leave one byte at the end for '\0'
	 */
	len = 0;
	if (gtm_path != NULL)
	{
		strncpy(gtm_app_path, gtm_path, MAXPGPATH - len - 1);

		len = strlen(gtm_app_path);
		strncat(gtm_app_path, "/", MAXPGPATH - len - 1);

		len = strlen(gtm_app_path);
	}

	if (strlen(gtm_app) >= (MAXPGPATH - len - 1))
	{
		write_stderr("gtm command exceeds max size");
		exit(1);
	}

	strncat(gtm_app_path, gtm_app, MAXPGPATH - len - 1);

	if (log_file != NULL)
		len = snprintf(cmd, MAXPGPATH - 1, SYSTEMQUOTE "\"%s\" %s%s -l %s &" SYSTEMQUOTE,
				 gtm_app_path, gtmdata_opt, gtm_opts, log_file);
	else
		len = snprintf(cmd, MAXPGPATH - 1, SYSTEMQUOTE "\"%s\" %s%s < \"%s\" 2>&1 &" SYSTEMQUOTE,
				 gtm_app_path, gtmdata_opt, gtm_opts, DEVNULL);

	if (len >= MAXPGPATH - 1)
	{
		write_stderr("gtm command exceeds max size");
		exit(1);
	}

	if (log_file)
		return (RunAsDaemon(cmd));
	else
		return system(cmd);
}

/*
 * Run specified command as a daemon.
 * Assume that *cmd includes '&' to run
 * the command at background so that we need fork()
 * only once.
 */
static int RunAsDaemon(char *cmd)
{
	switch (fork())
	{
		int status;

		case 0:
			/*
			 * Using fileno(xxx) may encounter trivial error because xxx may
			 * have been closed at somewhere else and fileno() may fail.  
			 * Its safer to use literal file descriptor here.
			 */
			close(0);
			close(1);
			close(2);
			if ((status = system(cmd)) == -1)
				/* 
				 * Same behavior as /bin/sh could not be
				 * executed.
				 */
				exit(127);
			else
				exit(WEXITSTATUS(status));
			break;
		case -1:
			return -1;
		default:
			return 0;
			break;
	}
}


/*
 * Find the gtm port and try a connection
 */
static bool
test_gtm_connection()
{
	GTM_Conn	   *conn;
	bool		success = false;
	int			i;
	char		portstr[32];
	char	   *p;
	char	   *q;
	char		connstr[128];	/* Should be way more than enough! */

	*portstr = '\0';

	/*
	 * Look in gtm_opts for a -p switch.
	 *
	 * This parsing code is not amazingly bright; it could for instance
	 * get fooled if ' -p' occurs within a quoted argument value.  Given
	 * that few people pass complicated settings in gtm_opts, it's
	 * probably good enough.
	 */
	for (p = gtm_opts; *p;)
	{
		/* advance past whitespace */
		while (isspace((unsigned char) *p))
			p++;

		if (strncmp(p, "-p", 2) == 0)
		{
			p += 2;
			/* advance past any whitespace/quoting */
			while (isspace((unsigned char) *p) || *p == '\'' || *p == '"')
				p++;
			/* find end of value (not including any ending quote!) */
			q = p;
			while (*q &&
				   !(isspace((unsigned char) *q) || *q == '\'' || *q == '"'))
				q++;
			/* and save the argument value */
			strlcpy(portstr, p, Min((q - p) + 1, sizeof(portstr)));
			/* keep looking, maybe there is another -p */
			p = q;
		}
		/* Advance to next whitespace */
		while (*p && !isspace((unsigned char) *p))
			p++;
	}

	/*
	 * Search config file for a 'port' option.
	 *
	 * This parsing code isn't amazingly bright either, but it should be okay
	 * for valid port settings.
	 */
	if (!*portstr)
	{
		char      **optlines;

		optlines = readfile(conf_file);
		if (optlines != NULL)
		{
			for (; *optlines != NULL; optlines++)
			{
				p = *optlines;

				while (isspace((unsigned char) *p))
					p++;
				if (strncmp(p, "port", 4) != 0)
					continue;
				p += 4;
				while (isspace((unsigned char) *p))
					p++;
				if (*p != '=')
					continue;
				p++;
				/* advance past any whitespace/quoting */
				while (isspace((unsigned char) *p) || *p == '\'' || *p == '"')
					p++;
				/* find end of value (not including any ending quote/comment!) */
				q = p;
				while (*q &&
					   !(isspace((unsigned char) *q) ||
						 *q == '\'' || *q == '"' || *q == '#'))
					q++;
				/* and save the argument value */
				strlcpy(portstr, p, Min((q - p) + 1, sizeof(portstr)));
				/* keep looking, maybe there is another */
			}
		}
	}

	/* Still not found? Use compiled-in default */
#define GTM_DEFAULT_PORT               6666
	if (!*portstr)
		snprintf(portstr, sizeof(portstr), "%d", GTM_DEFAULT_PORT);

	/*
	 * We need to set a connect timeout otherwise on Windows the SCM will
	 * probably timeout first
	 * a PGXC node ID has to be set for GTM connection protocol,
	 * so its value doesn't really matter here.
	 */
	snprintf(connstr, sizeof(connstr),
			 "host=localhost port=%s connect_timeout=5 node_name=one", portstr);

	for (i = 0; i < wait_seconds; i++)
	{
		if ((conn = PQconnectGTM(connstr)) != NULL &&
			(GTMPQstatus(conn) == CONNECTION_OK))
		{
			GTMPQfinish(conn);
			success = true;
			break;
		}
		else
		{
			GTMPQfinish(conn);
			print_msg(".");
			sleep(1); /* 1 sec */
		}
	}

	return success;
}

static void
read_gtm_opts(void)
{
	if (gtm_opts == NULL)
	{
		gtm_opts = "";		/* default */
		if (ctl_command == RESTART_COMMAND)
		{
			char	  **optlines;

			optlines = readfile(gtmopts_file);
			if (optlines == NULL)
			{
				write_stderr(_("%s: could not read file \"%s\"\n"), progname, gtmopts_file);
				exit(1);
			}
			else if (optlines[0] == NULL || optlines[1] != NULL)
			{
				write_stderr(_("%s: option file \"%s\" must have exactly one line\n"),
							 progname, gtmopts_file);
				exit(1);
			}
			else
			{
				int			len;
				char	   *optline;

				optline = optlines[0];
				/* trim off line endings */
				len = strcspn(optline, "\r\n");
				optline[len] = '\0';

				gtm_opts = optline;
			}
		}
	}
}

static void
do_start(void)
{
	pgpid_t		pid;
	pgpid_t		old_pid = 0;
	int			exitcode;

	if (ctl_command != RESTART_COMMAND)
	{
		old_pid = get_pgpid();
		if (old_pid != 0)
			write_stderr(_("%s: another server might be running; "
						   "trying to start server anyway\n"),
						 progname);
	}

	read_gtm_opts();

	/* The binary for both gtm and gtm_standby is the same */
	if (strcmp(gtm_app, "gtm_standby") == 0)
		gtm_app = "gtm";

	if (gtm_path == NULL)
	{
		int		 ret;
		char	*found_path;
		char	 version_str[MAXPGPATH];

		found_path = pg_malloc(MAXPGPATH);
		sprintf(version_str, "%s (Postgres-XC) %s\n", gtm_app, PGXC_VERSION);

		if ((ret = find_other_exec(argv0, gtm_app, version_str, found_path)) < 0)
		{
			char		full_path[MAXPGPATH];

			if (find_my_exec(argv0, full_path) < 0)
				strlcpy(full_path, progname, sizeof(full_path));

			if (ret == -1)
				write_stderr(_("The program \"%s\" is needed by gtm_ctl "
							   "but was not found in the\n"
							   "same directory as \"%s\".\n"
							   "Check your installation.\n"),
							 gtm_app, full_path);
			else
				write_stderr(_("The program \"%s\" was found by \"%s\"\n"
							   "but was not the same version as gtm_ctl.\n"
							   "Check your installation.\n"),
							 gtm_app, full_path);
			exit(1);
		}

		*last_dir_separator(found_path) = '\0';

		gtm_path = found_path;
	}

	exitcode = start_gtm();
	if (exitcode != 0)
	{
		write_stderr(_("%s: could not start server: exit code was %d\n"),
					 progname, exitcode);
		exit(1);
	}

	if (old_pid != 0)
	{
		sleep(1);
		pid = get_pgpid();
		if (pid == old_pid)
		{
			write_stderr(_("%s: could not start server\n"
						   "Examine the log output.\n"),
						 progname);
			exit(1);
		}
	}

	if (do_wait)
	{
		print_msg(_("waiting for server to start..."));

		if (test_gtm_connection() == false)
		{
			printf(_("could not start server\n"));
			exit(1);
		}
		else
		{
			print_msg(_(" done\n"));
			print_msg(_("server started\n"));
		}
	}
	else
		print_msg(_("server starting\n"));
}


static void
do_stop(void)
{
	int			cnt;
	pgpid_t		pid;

	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"), progname, pid_file);
		write_stderr(_("Is server running?\n"));
		exit(1);
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		write_stderr(_("%s: cannot stop server; "
					   "single-user server is running (PID: %ld)\n"),
					 progname, pid);
		exit(1);
	}

	if (kill((pid_t) pid, sig) != 0)
	{
		write_stderr(_("%s: could not send stop signal (PID: %ld): %s\n"), progname, pid,
					 strerror(errno));
		exit(1);
	}

	if (!do_wait)
	{
		print_msg(_("server shutting down\n"));
		return;
	}
	else
	{
		print_msg(_("waiting for server to shut down..."));

		for (cnt = 0; cnt < wait_seconds; cnt++)
		{
			if ((pid = get_pgpid()) != 0)
			{
				print_msg(".");
				sleep(1);		/* 1 sec */
			}
			else
				break;
		}

		if (pid != 0)			/* pid file still exists */
		{
			print_msg(_(" failed\n"));

			write_stderr(_("%s: server does not shut down\n"), progname);
			exit(1);
		}
		print_msg(_(" done\n"));

		printf(_("server stopped\n"));
	}
}

static void
do_promote(void)
{
	pgpid_t		pid;

	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"), progname, pid_file);
		write_stderr(_("Is server running?\n"));
		exit(1);
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		write_stderr(_("%s: cannot promote server; "
					   "single-user server is running (PID: %ld)\n"),
					 progname, pid);
		exit(1);
	}

	if (kill((pid_t) pid, SIGUSR1) != 0)
	{
		write_stderr(_("%s: could not send promote signal (PID: %ld): %s\n"), progname, pid,
					 strerror(errno));
		exit(1);
	}
}

/*
 * At least we expect the following argument
 *
 * 1) -D datadir
 * 2) -o options: we expect that -t and -s options are specified here.
 *		Check will be done in GTM-Proxy. If there's an error, it will be
 *		logged. In this case, GTM-Proxy won't terminate. It will continue
 *		to read/write with old GTM.
 *
 * Because they are not passed to gtm directly, they should appear in
 * gtm_ctl argument, not in -o options.  They're specific to gtm_ctl
 * reconnect.
 */
static void
do_reconnect(void)
{
	pgpid_t	pid;
	char *reconnect_point_file_nam;
	FILE *reconnect_point_file;

#ifdef GTM_SBY_DEBUG
	write_stderr("Reconnecting to new GTM ... DEBUG MODE.");
#endif

	/*
	 * Target must be "gtm_proxy"
	 */
	if (strcmp(gtm_app, "gtm_proxy") != 0)
	{
		write_stderr(_("%s: only gtm_proxy can accept reconnect command\n"), progname);
		exit(1);
	}
	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"), progname, pid_file);
		write_stderr(_("Is server running?\n"));
		exit(1);
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		write_stderr(_("%s: cannot promote server; "
					   "single-user server is running (PID: %ld)\n"),
					 progname, pid);
		exit(1);
	}
	read_gtm_opts();
	/*
	 * Pass reconnect info to GTM-Proxy.
	 *
	 * Option arguments are written to new gtm file under -D directory.
	 */
	reconnect_point_file_nam = malloc(strlen(gtm_data) + 9);
	if (reconnect_point_file_nam == NULL)
	{
		write_stderr(_("%s: No memory available.\n"), progname);
		exit(1);
	}

	snprintf(reconnect_point_file_nam, strlen(gtm_data) + 8, "%s/newgtm", gtm_data);
	reconnect_point_file = fopen(reconnect_point_file_nam, "w");

	if (reconnect_point_file == NULL)
	{
		write_stderr(_("%s: Cannot open reconnect point file %s\n"), progname, reconnect_point_file_nam);
		exit(1);
	}

	fprintf(reconnect_point_file, "%s\n", gtm_opts);
	fclose(reconnect_point_file);
	free(reconnect_point_file_nam);

	if (kill((pid_t) pid, SIGUSR1) != 0)
	{
		write_stderr(_("%s: could not send promote signal (PID: %ld): %s\n"), progname, pid,
					 strerror(errno));
		exit(1);
	}
}


/*
 *	restart/reload routines
 */

static void
do_restart(void)
{
	int			cnt;
	pgpid_t		pid;

	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"),
					 progname, pid_file);
		write_stderr(_("Is server running?\n"));
		write_stderr(_("starting server anyway\n"));
		do_start();
		return;
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		if (gtm_is_alive((pid_t) pid))
		{
			write_stderr(_("%s: cannot restart server; "
						   "single-user server is running (PID: %ld)\n"),
						 progname, pid);
			write_stderr(_("Please terminate the single-user server and try again.\n"));
			exit(1);
		}
	}

	if (gtm_is_alive((pid_t) pid))
	{
		if (kill((pid_t) pid, sig) != 0)
		{
			write_stderr(_("%s: could not send stop signal (PID: %ld): %s\n"), progname, pid,
						 strerror(errno));
			exit(1);
		}

		print_msg(_("waiting for server to shut down..."));

		/* always wait for restart */

		for (cnt = 0; cnt < wait_seconds; cnt++)
		{
			if ((pid = get_pgpid()) != 0)
			{
				print_msg(".");
				sleep(1);		/* 1 sec */
			}
			else
				break;
		}

		if (pid != 0)			/* pid file still exists */
		{
			print_msg(_(" failed\n"));

			write_stderr(_("%s: server does not shut down\n"), progname);
			exit(1);
		}

		print_msg(_(" done\n"));
		printf(_("server stopped\n"));
	}
	else
	{
		write_stderr(_("%s: old server process (PID: %ld) seems to be gone\n"),
					 progname, pid);
		write_stderr(_("starting server anyway\n"));
	}

	do_start();
}


static void
do_status(void)
{
	pgpid_t	pid;
	char datpath[MAXPGPATH];
	int mode;
	FILE *pidf;

	/*
	 * Read a PID file to get GTM server status instead of attaching shared memory.
	 */
	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		write_stderr(_("%s: could not open PID file \"%s\": %s\n"),
					 progname, pid_file, strerror(errno));
		exit(1);
	}

	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		write_stderr(_("%s: invalid data in PID file \"%s\"\n"),
					 progname, pid_file);
		exit(1);
	}

	if (fscanf(pidf, "%s", datpath) != 1)
	{
		write_stderr(_("%s: invalid data in PID file \"%s\"\n"),
					 progname, pid_file);
		exit(1);
	}

	if (strcmp(gtm_app, "gtm_proxy") != 0)
	{
		if (fscanf(pidf, "%d", &mode) != 1)
		{
			write_stderr(_("%s: invalid data in PID file \"%s\"\n"),
						 progname, pid_file);
			exit(1);
		}
	}

	fclose(pidf);

	pid = get_pgpid();

	if (pid == 0)				/* no pid file */
	{
		write_stderr(_("%s: PID file \"%s\" does not exist\n"),
					 progname, pid_file);
		write_stderr(_("Is server running?\n"));
		exit(1);
	}
	else if (pid < 0)			/* standalone backend, not gtm */
	{
		pid = -pid;
		if (gtm_is_alive((pid_t) pid))
		{
			write_stderr(_("%s: cannot get server status; "
						   "single-user server is running (PID: %ld)\n"),
						 progname, pid);
			write_stderr(_("Please terminate the single-user server and try again.\n"));
			exit(1);
		}
	}
	else
	{
		if (gtm_is_alive((pid_t) pid))
		{
			char      **optlines;

			printf(_("%s: server is running (PID: %ld)\n"),
				   progname, pid);

			optlines = readfile(gtmopts_file);
			if (optlines != NULL)
				for (; *optlines != NULL; optlines++)
					fputs(*optlines, stdout);
			if (strcmp(gtm_app, "gtm_proxy") != 0)
				printf("%d %s\n", mode, mode == 1 ? "master" : "slave");
			return;
		}
	}

	write_stderr(_("%s: no server running\n"), progname);
	exit(1);
}


/*
 *	utility routines
 */

static bool
gtm_is_alive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $GTMDATA, that means it's not the
	 * gtm we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the gtm,
	 * either.	(Windows hasn't got getppid(), though.)
	 */
	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

static void
do_advice(void)
{
	write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
}


static void
do_help(void)
{
	printf(_("%s is a utility to start, stop or restart,\n"
			 "a GTM server, a GTM standby or GTM proxy.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s start   -Z STARTUP_MODE [-w] [-t SECS] [-D DATADIR] [-l FILENAME] [-o \"OPTIONS\"]\n"), progname);
	printf(_("  %s stop    -Z STARTUP_MODE [-W] [-t SECS] [-D DATADIR] [-m SHUTDOWN-MODE]\n"), progname);
	printf(_("  %s promote -Z STARTUP_MODE [-w] [-t SECS] [-D DATADIR]\n"), progname);
	printf(_("  %s restart -Z STARTUP_MODE [-w] [-t SECS] [-D DATADIR] [-m SHUTDOWN-MODE]\n"
		 "                 [-o \"OPTIONS\"]\n"), progname);
	printf(_("  %s status  -Z STARTUP_MODE [-w] [-t SECS] [-D DATADIR]\n"), progname);
	printf(_("  %s reconnect -Z STARTUP_MODE [-D DATADIR] -o \"OPTIONS\"]\n"), progname);

	printf(_("\nCommon options:\n"));
	printf(_("  -D DATADIR             location of the database storage area\n"));
	printf(_("  -i nodename            set gtm_proxy nodename registered on GTM\n"));
	printf(_("                         (option ignored if used with GTM)\n"));
	printf(_("  -t SECS                seconds to wait when using -w option\n"));
	printf(_("  -w                     wait until operation completes\n"));
	printf(_("  -W                     do not wait until operation completes\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  --help                 show this help, then exit\n"));
	printf(_("(The default is to wait for shutdown, but not for start or restart.)\n\n"));

	printf(_("\nOptions for start or restart:\n"));
	printf(_("  -l FILENAME            write (or append) server log to FILENAME\n"));
	printf(_("  -o OPTIONS             command line options to pass to gtm\n"
			 "                         (GTM server executable)\n"));
	printf(_("  -p PATH-TO-GTM/PROXY   path to gtm/gtm_proxy executables\n"));
	printf(_("  -Z STARTUP-MODE        can be \"gtm\", \"gtm_standby\" or \"gtm_proxy\"\n"));
	printf(_("\nOptions for stop or restart:\n"));
	printf(_("  -m SHUTDOWN-MODE   can be \"smart\", \"fast\", or \"immediate\"\n"));

	printf(_("\nOptions for reconnect:\n"));
	printf(_("  -t NewGTMPORT          Port number of new GTM.\n"));
	printf(_("  -s NewGTMHost          Host Name of new GTM.\n"));

	printf(_("\nShutdown modes are:\n"));
	printf(_("  smart       quit after all clients have disconnected\n"));
	printf(_("  fast        quit directly, with proper shutdown\n"));
	printf(_("  immediate   quit without complete shutdown; will lead to recovery on restart\n"));
}


static void
set_mode(char *modeopt)
{
	if (strcmp(modeopt, "s") == 0 || strcmp(modeopt, "smart") == 0)
	{
		shutdown_mode = SMART_MODE;
		sig = SIGTERM;
	}
	else if (strcmp(modeopt, "f") == 0 || strcmp(modeopt, "fast") == 0)
	{
		shutdown_mode = FAST_MODE;
		sig = SIGINT;
	}
	else if (strcmp(modeopt, "i") == 0 || strcmp(modeopt, "immediate") == 0)
	{
		shutdown_mode = IMMEDIATE_MODE;
		sig = SIGQUIT;
	}
	else
	{
		write_stderr(_("%s: unrecognized shutdown mode \"%s\"\n"), progname, modeopt);
		do_advice();
		exit(1);
	}
}

static void
trim_last_slash(char *buf)
{
	char *lastpos = NULL;
	for (;*buf ; buf++)
	{
		if (*buf == '/')
		{
			lastpos = buf;
			continue;
		}
		if (lastpos && (*buf != ' ' && *buf != '\t'))
		{
			lastpos = NULL;
			continue;
		}
	}
	if (lastpos)
		*lastpos = 0;
}

int
main(int argc, char **argv)
{
	int			c;
	char		*nodename = NULL; /* GTM Proxy nodename */

	progname = "gtm_ctl";

	/*
	 * save argv[0] so do_start() can look for the gtm if necessary. we
	 * don't look for gtm here because in many cases we won't need it.
	 */
	argv0 = argv[0];

	umask(077);

	/* support --help and --version even if invoked as root */
	if (argc > 1)
	{
		if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0 ||
			strcmp(argv[1], "-?") == 0)
		{
			do_help();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("gtm_ctl (Postgres-XC) " PGXC_VERSION);
			exit(0);
		}
	}

	/*
	 * Disallow running as root, to forestall any possible security holes.
	 */
	if (geteuid() == 0)
	{
		write_stderr(_("%s: cannot be run as root\n"
					   "Please log in (using, e.g., \"su\") as the "
					   "(unprivileged) user that will\n"
					   "own the server process.\n"),
					 progname);
		exit(1);
	}

	/*
	 * 'Action' can be before or after args so loop over both. Some
	 * getopt_long() implementations will reorder argv[] to place all flags
	 * first (GNU?), but we don't rely on it. Our /port version doesn't do
	 * that.
	 */
	optind = 1;

	/* process command-line options */
	while (optind < argc)
	{
		while ((c = getopt(argc, argv, "D:i:l:m:o:p:t:wWZ:C:")) != -1)
		{
			switch (c)
			{
				case 'D':
					{
						char	   *env_var = pg_malloc(strlen(optarg) + 9);

						gtmdata_D = xstrdup(optarg);
						canonicalize_path(gtmdata_D);
						snprintf(env_var, strlen(optarg) + 9, "GTMDATA=%s",
								 gtmdata_D);
						putenv(env_var);

						/*
						 * We could pass GTMDATA just in an environment
						 * variable but we do -D too for clearer gtm
						 * 'ps' display
						 */
						gtmdata_opt = (char *) pg_malloc(strlen(gtmdata_D) + 8);
						snprintf(gtmdata_opt, strlen(gtmdata_D) + 8,
								 "-D \"%s\" ",
								 gtmdata_D);
						break;
					}
				case 'i':
					nodename = strdup(optarg);
					break;
				case 'l':
					log_file = xstrdup(optarg);
					break;
				case 'm':
					set_mode(optarg);
					break;
				case 'o':
					gtm_opts = xstrdup(optarg);
					break;
				case 'p':
					gtm_path = xstrdup(optarg);
					canonicalize_path(gtm_path);
					break;
				case 't':
					wait_seconds = atoi(optarg);
					break;
				case 'C':
					control_file = xstrdup(optarg);
					break;
				case 'w':
					do_wait = true;
					wait_set = true;
					break;
				case 'W':
					do_wait = false;
					wait_set = true;
					break;
				case 'Z':
					gtm_app = xstrdup(optarg);
					if (strcmp(gtm_app,"gtm_proxy") != 0
						&& strcmp(gtm_app,"gtm_standby") != 0
						&& strcmp(gtm_app,"gtm") != 0)
					{
						write_stderr(_("%s: %s launch name set not correct\n"), progname, gtm_app);
						do_advice();
						exit(1);
					}
					break;
				default:
					/* getopt_long already issued a suitable error message */
					do_advice();
					exit(1);
			}
		}

		/* Process an action */
		if (optind < argc)
		{
			if (ctl_command != NO_COMMAND)
			{
				write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
				do_advice();
				exit(1);
			}

			if (strcmp(argv[optind], "start") == 0)
				ctl_command = START_COMMAND;
			else if (strcmp(argv[optind], "stop") == 0)
				ctl_command = STOP_COMMAND;
			else if (strcmp(argv[optind], "promote") == 0)
				ctl_command = PROMOTE_COMMAND;
			else if (strcmp(argv[optind], "restart") == 0)
				ctl_command = RESTART_COMMAND;
			else if (strcmp(argv[optind], "status") == 0)
				ctl_command = STATUS_COMMAND;
			else if (strcmp(argv[optind], "reconnect") == 0)
				ctl_command = RECONNECT_COMMAND;
			else
			{
				write_stderr(_("%s: unrecognized operation mode \"%s\"\n"),
							 progname, argv[optind]);
				do_advice();
				exit(1);
			}
			optind++;
		}
	}

	/*
	 * Take care of the control file (-C Option)
	 */
	if (control_file)
	{
		char ctrl_path[MAXPGPATH+1];
		char C_opt_path[MAXPGPATH+1];
		char bkup_path[MAXPGPATH+1];
		FILE *f1, *f2;
		int c;

		if (!gtmdata_D)
		{
			write_stderr(_("No -D option specified.\n"));
			exit(1);
		}
		if ((strcmp(gtm_app, "gtm") != 0) && (strcmp(gtm_app, "gtm_master") != 0))
		{
			write_stderr(_("-C option is valid only for gtm.\n"));
			exit(1);
		}
		/* If there's already a control file, backup it to *.bak */
		trim_last_slash(gtmdata_D);
		snprintf(ctrl_path, MAXPGPATH, "%s/%s", gtmdata_D, GTM_CONTROL_FILE);
		if ((f1 = fopen(ctrl_path, "r")))
		{

			snprintf(bkup_path, MAXPGPATH, "%s/%s.bak", gtmdata_D, GTM_CONTROL_FILE);
			if (!(f2 = fopen(bkup_path, "w")))
			{
				fclose(f1);
				write_stderr(_("Cannot open backup file, %s/%s.bak, %s\n"),
							 gtmdata_D, GTM_CONTROL_FILE, strerror(errno));
				exit(1);
			}
			while ((c = getc(f1)) != EOF)
				putc(c, f2);
			fclose(f1);
			fclose(f2);
		}
		/* Copy specified control file. */
		snprintf(C_opt_path, MAXPGPATH, "%s/%s", gtmdata_D, control_file);
		if (!(f1 = fopen(ctrl_path, "w")))
		{
			write_stderr(_("Cannot oopen control file, %s, %s\n"), ctrl_path, strerror(errno));
			exit(1);
		}
		if (!(f2 = fopen(C_opt_path, "r")))
		{
			fclose(f1);
			write_stderr(_("Cannot open -C option file, %s, %s\n"), C_opt_path, strerror(errno));
			exit(1);
		}
		while ((c = getc(f2)) != EOF)
			putc(c, f1);
		fclose(f1);
		fclose(f2);
	}

	if (ctl_command == NO_COMMAND)
	{
		write_stderr(_("%s: no operation specified\n"), progname);
		do_advice();
		exit(1);
	}

	gtm_data = getenv("GTMDATA");

	if (gtm_data)
	{
		gtm_data = xstrdup(gtm_data);
		canonicalize_path(gtm_data);
	}

	if (!gtm_data)
	{
		write_stderr("%s: no GTM/GTM Proxy directory specified \n",
					 progname);
		do_advice();
		exit(1);
	}

	/*
	 * pid files of gtm and gtm proxy are named differently
	 * -Z option has also to be set for STOP_COMMAND
	 * or gtm_ctl will not be able to find the correct pid_file
	 */
	if (!gtm_app)
	{
		write_stderr("%s: no launch option not specified\n",
					 progname);
		do_advice();
		exit(1);
	}

	if (strcmp(gtm_app,"gtm_proxy") != 0 &&
		strcmp(gtm_app, "gtm_standby") != 0 &&
		strcmp(gtm_app,"gtm") != 0)
	{
		write_stderr(_("%s: launch option incorrect\n"),
						progname);
		do_advice();
		exit(1);
	}

	/* Check if GTM Proxy ID is set, this is not necessary when stopping */
	if (ctl_command == START_COMMAND ||
		ctl_command == RESTART_COMMAND)
	{
		/* Rebuild option string to include Proxy ID */
		if (strcmp(gtm_app, "gtm_proxy") == 0)
		{
			gtmdata_opt = (char *) pg_realloc(gtmdata_opt, strlen(gtmdata_opt) + 9);
			if (nodename)
				sprintf(gtmdata_opt, "%s -i %s ", gtmdata_opt, nodename);
			else
				sprintf(gtmdata_opt, "%s ", gtmdata_opt);
		}
	}

	if (!wait_set)
	{
		switch (ctl_command)
		{
			case RESTART_COMMAND:
			case START_COMMAND:
			case PROMOTE_COMMAND:
			case STATUS_COMMAND:
				do_wait = false;
				break;
			case STOP_COMMAND:
				do_wait = true;
				break;
			default:
				break;
		}
	}

	/* Build strings for pid file and option file */
	if (strcmp(gtm_app,"gtm_proxy") == 0)
	{
		snprintf(pid_file, MAXPGPATH, "%s/gtm_proxy.pid", gtm_data);
		snprintf(gtmopts_file, MAXPGPATH, "%s/gtm_proxy.opts", gtm_data);
		snprintf(conf_file, MAXPGPATH, "%s/gtm_proxy.conf", gtm_data);
	}
	else if (strcmp(gtm_app,"gtm") == 0)
	{
		snprintf(pid_file, MAXPGPATH, "%s/gtm.pid", gtm_data);
		snprintf(gtmopts_file, MAXPGPATH, "%s/gtm.opts", gtm_data);
		snprintf(conf_file, MAXPGPATH, "%s/gtm.conf", gtm_data);
	}
	else if (strcmp(gtm_app,"gtm_standby") == 0)
	{
		snprintf(pid_file, MAXPGPATH, "%s/gtm.pid", gtm_data);
		snprintf(gtmopts_file, MAXPGPATH, "%s/gtm.opts", gtm_data);
		snprintf(conf_file, MAXPGPATH, "%s/gtm.conf", gtm_data);
	}

	if (ctl_command==STATUS_COMMAND)
		gtm_opts = xstrdup("-c");

	switch (ctl_command)
	{
		case START_COMMAND:
			do_start();
			break;
		case STOP_COMMAND:
			do_stop();
			break;
		case PROMOTE_COMMAND:
			do_promote();
			break;
		case RESTART_COMMAND:
			do_restart();
			break;
		case STATUS_COMMAND:
			do_status();
			break;
		case RECONNECT_COMMAND:
			do_reconnect();
			break;
		default:
			break;
	}

	exit(0);
}

/*
 * Safer versions of standard realloc C library function. If an
 * out-of-memory condition occurs, these functions will bail out
 * safely; therefore, its return value is guaranteed to be non-NULL.
 */
static void *
pg_realloc(void *ptr, size_t size)
{
	void       *tmp;

	tmp = realloc(ptr, size);
	if (!tmp)
		write_stderr("out of memory\n");
	return tmp;
}
