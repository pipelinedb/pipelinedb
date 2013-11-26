/*-------------------------------------------------------------------------
 *
 * initgtm --- initialize a GTM (Global transaction manager) installation
 *
 * initgtm creates (initializes) a GTM/GTM-Proxy for Postgres-XC database
 * cluster (site, instance, installation, whatever).
 *
 * Note:
 *	 The program has some memory leakage - it isn't worth cleaning it up.
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/initgtm/initgtm.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <locale.h>
#include <signal.h>
#include <time.h>

#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "getaddrinfo.h"
#include "getopt_long.h"
#include "miscadmin.h"

#include "postgres.h"

/*
 * these values are passed in by makefile defines
 */
static char *share_path = NULL;

/* values to be obtained from arguments */
static char *pg_data = "";
static bool debug = false;
static bool noclean = false;
static bool show_setting = false;

/* internal vars */
static const char *progname;
static char *conf_file; /* Used by GTM */
static bool made_new_pgdata = false;
static bool found_existing_pgdata = false;
static bool caught_signal = false;
static bool output_failed = false;
static int	output_errno = 0;

/* about instance initialized */
static bool is_gtm = true; /* GTM or proxy */

/* defaults for all nodes */
static int	n_port = 6666;
static char	*n_name = "one";

/* defaults for proxies - connection parameters to GTM */
static int	gtm_port = 6668;
static char *gtm_host = "localhost";

/* path to 'initgtm' binary directory */
static char bin_path[MAXPGPATH];
static char backend_exec[MAXPGPATH];

static void *pg_malloc(size_t size);
static char *xstrdup(const char *s);
static char **replace_token(char **lines,
			  const char *token, const char *replacement);

#ifndef HAVE_UNIX_SOCKETS
static char **filter_lines_with_token(char **lines, const char *token);
#endif
static char **readfile(const char *path);
static void writefile(char *path, char **lines);
static void exit_nicely(void);
static char *get_id(void);
static bool mkdatadir(const char *subdir);
static void set_input(char **dest, char *filename);
static void check_input(char *path);
static void set_null_conf(void);
static void setup_config(void);
static void trapsig(int signum);
static void check_ok(void);
static void usage(const char *progname);

#ifdef WIN32
static int	CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo);
#endif

#ifndef WIN32
#define QUOTE_PATH	""
#define DIR_SEP "/"
#else
#define QUOTE_PATH	"\""
#define DIR_SEP "\\"
#endif

/*
 * routines to check mem allocations and fail noisily.
 *
 * Note that we can't call exit_nicely() on a memory failure, as it calls
 * rmtree() which needs memory allocation. So we just exit with a bang.
 */
static void *
pg_malloc(size_t size)
{
	void	   *result;

	result = malloc(size);
	if (!result)
	{
		fprintf(stderr, _("%s: out of memory\n"), progname);
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
		fprintf(stderr, _("%s: out of memory\n"), progname);
		exit(1);
	}
	return result;
}

/*
 * make a copy of the array of lines, with token replaced by replacement
 * the first time it occurs on each line.
 *
 * This does most of what sed was used for in the shell script, but
 * doesn't need any regexp stuff.
 */
static char **
replace_token(char **lines, const char *token, const char *replacement)
{
	int			numlines = 1;
	int			i;
	char	  **result;
	int			toklen,
				replen,
				diff;

	for (i = 0; lines[i]; i++)
		numlines++;

	result = (char **) pg_malloc(numlines * sizeof(char *));

	toklen = strlen(token);
	replen = strlen(replacement);
	diff = replen - toklen;

	for (i = 0; i < numlines; i++)
	{
		char	   *where;
		char	   *newline;
		int			pre;

		/* just copy pointer if NULL or no change needed */
		if (lines[i] == NULL || (where = strstr(lines[i], token)) == NULL)
		{
			result[i] = lines[i];
			continue;
		}

		/* if we get here a change is needed - set up new line */

		newline = (char *) pg_malloc(strlen(lines[i]) + diff + 1);

		pre = where - lines[i];

		strncpy(newline, lines[i], pre);

		strcpy(newline + pre, replacement);

		strcpy(newline + pre + replen, lines[i] + pre + toklen);

		result[i] = newline;
	}

	return result;
}

/*
 * make a copy of lines without any that contain the token
 *
 * a sort of poor man's grep -v
 */
#ifndef HAVE_UNIX_SOCKETS
static char **
filter_lines_with_token(char **lines, const char *token)
{
	int			numlines = 1;
	int			i,
				src,
				dst;
	char	  **result;

	for (i = 0; lines[i]; i++)
		numlines++;

	result = (char **) pg_malloc(numlines * sizeof(char *));

	for (src = 0, dst = 0; src < numlines; src++)
	{
		if (lines[src] == NULL || strstr(lines[src], token) == NULL)
			result[dst++] = lines[src];
	}

	return result;
}
#endif

/*
 * get the lines from a text file
 */
static char **
readfile(const char *path)
{
	FILE	   *infile;
	int			maxlength = 1,
				linelen = 0;
	int			nlines = 0;
	char	  **result;
	char	   *buffer;
	int			c;

	if ((infile = fopen(path, "r")) == NULL)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, path, strerror(errno));
		exit_nicely();
	}

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
 * write an array of lines to a file
 *
 * This is only used to write text files.  Use fopen "w" not PG_BINARY_W
 * so that the resulting configuration files are nicely editable on Windows.
 */
static void
writefile(char *path, char **lines)
{
	FILE	   *out_file;
	char	  **line;

	if ((out_file = fopen(path, "w")) == NULL)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"),
				progname, path, strerror(errno));
		exit_nicely();
	}
	for (line = lines; *line != NULL; line++)
	{
		if (fputs(*line, out_file) < 0)
		{
			fprintf(stderr, _("%s: could not write file \"%s\": %s\n"),
					progname, path, strerror(errno));
			exit_nicely();
		}
		free(*line);
	}
	if (fclose(out_file))
	{
		fprintf(stderr, _("%s: could not write file \"%s\": %s\n"),
				progname, path, strerror(errno));
		exit_nicely();
	}
}


/*
 * clean up any files we created on failure
 * if we created the data directory remove it too
 */
static void
exit_nicely(void)
{
	if (!noclean)
	{
		if (made_new_pgdata)
		{
			fprintf(stderr, _("%s: removing data directory \"%s\"\n"),
					progname, pg_data);
			if (!rmtree(pg_data, true))
				fprintf(stderr, _("%s: failed to remove data directory\n"),
						progname);
		}
		else if (found_existing_pgdata)
		{
			fprintf(stderr,
					_("%s: removing contents of data directory \"%s\"\n"),
					progname, pg_data);
			if (!rmtree(pg_data, false))
				fprintf(stderr, _("%s: failed to remove contents of data directory\n"),
						progname);
		}
		/* otherwise died during startup, do nothing! */
	}
	else
	{
		if (made_new_pgdata || found_existing_pgdata)
			fprintf(stderr,
			  _("%s: data directory \"%s\" not removed at user's request\n"),
					progname, pg_data);
	}

	exit(1);
}

/*
 * find the current user
 *
 * on unix make sure it isn't really root
 */
static char *
get_id(void)
{
#ifndef WIN32

	struct passwd *pw;

	if (geteuid() == 0)			/* 0 is root's uid */
	{
		fprintf(stderr,
				_("%s: cannot be run as root\n"
				  "Please log in (using, e.g., \"su\") as the "
				  "(unprivileged) user that will\n"
				  "own the server process.\n"),
				progname);
		exit(1);
	}

	pw = getpwuid(geteuid());
	if (!pw)
	{
		fprintf(stderr,
			  _("%s: could not obtain information about current user: %s\n"),
				progname, strerror(errno));
		exit(1);
	}
#else							/* the windows code */

	struct passwd_win32
	{
		int			pw_uid;
		char		pw_name[128];
	}			pass_win32;
	struct passwd_win32 *pw = &pass_win32;
	DWORD		pwname_size = sizeof(pass_win32.pw_name) - 1;

	pw->pw_uid = 1;
	if (!GetUserName(pw->pw_name, &pwname_size))
	{
		fprintf(stderr, _("%s: could not get current user name: %s\n"),
				progname, strerror(errno));
		exit(1);
	}
#endif

	return xstrdup(pw->pw_name);
}


/*
 * make the data directory (or one of its subdirectories if subdir is not NULL)
 */
static bool
mkdatadir(const char *subdir)
{
	char	   *path;

	path = pg_malloc(strlen(pg_data) + 2 +
					 (subdir == NULL ? 0 : strlen(subdir)));

	if (subdir != NULL)
		sprintf(path, "%s/%s", pg_data, subdir);
	else
		strcpy(path, pg_data);

	if (pg_mkdir_p(path, S_IRWXU) == 0)
		return true;

	fprintf(stderr, _("%s: could not create directory \"%s\": %s\n"),
			progname, path, strerror(errno));

	return false;
}


/*
 * set name of given input file variable under data directory
 */
static void
set_input(char **dest, char *filename)
{
	*dest = pg_malloc(strlen(share_path) + strlen(filename) + 2);
	sprintf(*dest, "%s/%s", share_path, filename);
}

/*
 * check that given input file exists
 */
static void
check_input(char *path)
{
	struct stat statbuf;

	if (stat(path, &statbuf) != 0)
	{
		if (errno == ENOENT)
		{
			fprintf(stderr,
					_("%s: file \"%s\" does not exist\n"), progname, path);
			fprintf(stderr,
					_("This might mean you have a corrupted installation or identified\n"
					"the wrong directory with the invocation option -L.\n"));
		}
		else
		{
			fprintf(stderr,
				 _("%s: could not access file \"%s\": %s\n"), progname, path,
					strerror(errno));
			fprintf(stderr,
					_("This might mean you have a corrupted installation or identified\n"
					"the wrong directory with the invocation option -L.\n"));
		}
		exit(1);
	}
	if (!S_ISREG(statbuf.st_mode))
	{
		fprintf(stderr,
				_("%s: file \"%s\" is not a regular file\n"), progname, path);
		fprintf(stderr,
		_("This might mean you have a corrupted installation or identified\n"
		  "the wrong directory with the invocation option -L.\n"));
		exit(1);
	}
}


/*
 * set up an empty config file so we can check config settings by launching
 * a test backend
 */
static void
set_null_conf(void)
{
	FILE	   *conf_file;
	char	   *path;

	path = pg_malloc(strlen(pg_data) + 17);
	if (is_gtm)
		sprintf(path, "%s/gtm.conf", pg_data);
	else
		sprintf(path, "%s/gtm_proxy.conf", pg_data);
	conf_file = fopen(path, PG_BINARY_W);
	if (conf_file == NULL)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"),
				progname, path, strerror(errno));
		exit_nicely();
	}
	if (fclose(conf_file))
	{
		fprintf(stderr, _("%s: could not write file \"%s\": %s\n"),
				progname, path, strerror(errno));
		exit_nicely();
	}
	free(path);
}


/*
 * set up all the config files
 */
static void
setup_config(void)
{
	char	  **conflines;
	char		repltok[100];
	char		path[MAXPGPATH];

	fputs(_("creating configuration files ... "), stdout);
	fflush(stdout);

	/* gtm.conf/gtm_proxy.conf */

	conflines = readfile(conf_file);

	/* Set options dedicated to both nodes */
	snprintf(repltok, sizeof(repltok), "nodename = '%s'", n_name);
	conflines = replace_token(conflines, "#nodename = ''", repltok);

	snprintf(repltok, sizeof(repltok), "port = %d", n_port);
	conflines = replace_token(conflines, "#port = 6666", repltok);

	if (is_gtm)
		snprintf(path, sizeof(path), "%s/gtm.conf", pg_data);
	else
	{
		/* Set options dedicated to Proxy */
		snprintf(repltok, sizeof(repltok), "gtm_host = '%s'", gtm_host);
		conflines = replace_token(conflines, "#gtm_host = ''", repltok);

		snprintf(repltok, sizeof(repltok), "gtm_port = %d", gtm_port);
		conflines = replace_token(conflines, "#gtm_port =", repltok);

		snprintf(path, sizeof(path), "%s/gtm_proxy.conf", pg_data);
	}

	writefile(path, conflines);
	chmod(path, S_IRUSR | S_IWUSR);

	free(conflines);

	check_ok();
}


/*
 * signal handler in case we are interrupted.
 *
 * The Windows runtime docs at
 * http://msdn.microsoft.com/library/en-us/vclib/html/_crt_signal.asp
 * specifically forbid a number of things being done from a signal handler,
 * including IO, memory allocation and system calls, and only allow jmpbuf
 * if you are handling SIGFPE.
 *
 * I avoided doing the forbidden things by setting a flag instead of calling
 * exit_nicely() directly.
 *
 * Also note the behaviour of Windows with SIGINT, which says this:
 *	 Note	SIGINT is not supported for any Win32 application, including
 *	 Windows 98/Me and Windows NT/2000/XP. When a CTRL+C interrupt occurs,
 *	 Win32 operating systems generate a new thread to specifically handle
 *	 that interrupt. This can cause a single-thread application such as UNIX,
 *	 to become multithreaded, resulting in unexpected behavior.
 *
 * I have no idea how to handle this. (Strange they call UNIX an application!)
 * So this will need some testing on Windows.
 */
static void
trapsig(int signum)
{
	/* handle systems that reset the handler, like Windows (grr) */
	pqsignal(signum, trapsig);
	caught_signal = true;
}

/*
 * call exit_nicely() if we got a signal, or else output "ok".
 */
static void
check_ok(void)
{
	if (caught_signal)
	{
		printf(_("caught signal\n"));
		fflush(stdout);
		exit_nicely();
	}
	else if (output_failed)
	{
		printf(_("could not write to child process: %s\n"),
			   strerror(output_errno));
		fflush(stdout);
		exit_nicely();
	}
	else
	{
		/* all seems well */
		printf(_("ok\n"));
		fflush(stdout);
	}
}

/* Hack to suppress a warning about %x from some versions of gcc */
static inline size_t
my_strftime(char *s, size_t max, const char *fmt, const struct tm * tm)
{
	return strftime(s, max, fmt, tm);
}

#ifdef WIN32

/*
 * Replace 'needle' with 'replacement' in 'str' . Note that the replacement
 * is done in-place, so 'replacement' must be shorter than 'needle'.
 */
static void
strreplace(char *str, char *needle, char *replacement)
{
	char	   *s;

	s = strstr(str, needle);
	if (s != NULL)
	{
		int			replacementlen = strlen(replacement);
		char	   *rest = s + strlen(needle);

		memcpy(s, replacement, replacementlen);
		memmove(s + replacementlen, rest, strlen(rest) + 1);
	}
}
#endif   /* WIN32 */


#ifdef WIN32
typedef BOOL (WINAPI * __CreateRestrictedToken) (HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);

/* Windows API define missing from some versions of MingW headers */
#ifndef  DISABLE_MAX_PRIVILEGE
#define DISABLE_MAX_PRIVILEGE	0x1
#endif

/*
 * Create a restricted token and execute the specified process with it.
 *
 * Returns 0 on failure, non-zero on success, same as CreateProcess().
 *
 * On NT4, or any other system not containing the required functions, will
 * NOT execute anything.
 */
static int
CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo)
{
	BOOL		b;
	STARTUPINFO si;
	HANDLE		origToken;
	HANDLE		restrictedToken;
	SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
	SID_AND_ATTRIBUTES dropSids[2];
	__CreateRestrictedToken _CreateRestrictedToken = NULL;
	HANDLE		Advapi32Handle;

	ZeroMemory(&si, sizeof(si));
	si.cb = sizeof(si);

	Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
	if (Advapi32Handle != NULL)
	{
		_CreateRestrictedToken = (__CreateRestrictedToken) GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
	}

	if (_CreateRestrictedToken == NULL)
	{
		fprintf(stderr, "WARNING: cannot create restricted tokens on this platform\n");
		if (Advapi32Handle != NULL)
			FreeLibrary(Advapi32Handle);
		return 0;
	}

	/* Open the current token to use as a base for the restricted one */
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken))
	{
		fprintf(stderr, "Failed to open process token: %lu\n", GetLastError());
		return 0;
	}

	/* Allocate list of SIDs to remove */
	ZeroMemory(&dropSids, sizeof(dropSids));
	if (!AllocateAndInitializeSid(&NtAuthority, 2,
		 SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0,
								  0, &dropSids[0].Sid) ||
		!AllocateAndInitializeSid(&NtAuthority, 2,
	SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS, 0, 0, 0, 0, 0,
								  0, &dropSids[1].Sid))
	{
		fprintf(stderr, "Failed to allocate SIDs: %lu\n", GetLastError());
		return 0;
	}

	b = _CreateRestrictedToken(origToken,
							   DISABLE_MAX_PRIVILEGE,
							   sizeof(dropSids) / sizeof(dropSids[0]),
							   dropSids,
							   0, NULL,
							   0, NULL,
							   &restrictedToken);

	FreeSid(dropSids[1].Sid);
	FreeSid(dropSids[0].Sid);
	CloseHandle(origToken);
	FreeLibrary(Advapi32Handle);

	if (!b)
	{
		fprintf(stderr, "Failed to create restricted token: %lu\n", GetLastError());
		return 0;
	}

#ifndef __CYGWIN__
	AddUserToTokenDacl(restrictedToken);
#endif

	if (!CreateProcessAsUser(restrictedToken,
							 NULL,
							 cmd,
							 NULL,
							 NULL,
							 TRUE,
							 CREATE_SUSPENDED,
							 NULL,
							 NULL,
							 &si,
							 processInfo))

	{
		fprintf(stderr, "CreateProcessAsUser failed: %lu\n", GetLastError());
		return 0;
	}

	return ResumeThread(processInfo->hThread);
}
#endif

/*
 * print help text
 */
static void
usage(const char *progname)
{
	printf(_("%s initializes a GTM for Postgres-XC database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [NODE-TYPE] [OPTION]... [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR     location for this GTM node\n"));
	printf(_(" [-Z]NODE-TYPE              can be \"gtm\" or \"gtm_proxy\""));
	printf(_("\nLess commonly used options:\n"));
	printf(_("  -d, --debug               generate lots of debugging output\n"));
	printf(_("  -n, --noclean             do not clean up after errors\n"));
	printf(_("  -s, --show                show internal settings\n"));
	printf(_("\nOther options:\n"));
	printf(_("  -?, --help                show this help, then exit\n"));
	printf(_("  -V, --version             output version information, then exit\n"));
}

int
main(int argc, char *argv[])
{
	/*
	 * options with no short version return a low integer, the rest return
	 * their short version value
	 */
	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"debug", no_argument, NULL, 'd'},
		{"show", no_argument, NULL, 's'},
		{"noclean", no_argument, NULL, 'n'},
		{NULL, 0, NULL, 0}
	};

	int			c, ret;
	int			option_index;
	char	   *effective_user;
	char		bin_dir[MAXPGPATH];
	char	   *pg_data_native;
	bool		node_type_specified = false;

	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("initgtm"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
		usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("initgtm (Postgres-XC) " PGXC_VERSION);
			exit(0);
		}
	}

	/* process command-line options */

	while ((c = getopt_long(argc, argv, "dD:nsZ:", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
				pg_data = xstrdup(optarg);
				break;
			case 'd':
				debug = true;
				printf(_("Running in debug mode.\n"));
				break;
			case 'n':
				noclean = true;
				printf(_("Running in noclean mode.  Mistakes will not be cleaned up.\n"));
				break;
			case 's':
				show_setting = true;
				break;
			case 'Z':
				if (strcmp(xstrdup(optarg), "gtm") == 0)
					is_gtm = true;
				else if (strcmp(xstrdup(optarg), "gtm_proxy") == 0)
					is_gtm = false;
				else
				{
					fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
							progname);
					exit(1);
				}
				node_type_specified = true;
				break;
			default:
				/* getopt_long already emitted a complaint */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	/* Non-option argument specifies data directory */
	if (optind < argc)
	{
		pg_data = xstrdup(argv[optind]);
		optind++;
	}

	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind + 1]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Check on definition of GTM data folder */
	if (strlen(pg_data) == 0)
	{
		fprintf(stderr,
				_("%s: no data directory specified\n"
				  "You must identify the directory where the data for this GTM system\n"
				  "will reside.  Do this with either the invocation option -D or the\n"
				  "environment variable PGDATA.\n"),
				progname);
		exit(1);
	}

	if (!node_type_specified)
	{
		fprintf(stderr,
				_("%s: no node type specified\n"
				  "You must identify the node type chosen for initialization.\n"
				  "Do this with the invocation option -Z by choosing \"gtm\" or"
				  "\"gtm_proxy\"\n"),
				progname);
		exit(1);
	}

	pg_data_native = pg_data;
	canonicalize_path(pg_data);

#ifdef WIN32

	/*
	 * Before we execute another program, make sure that we are running with a
	 * restricted token. If not, re-execute ourselves with one.
	 */

	if ((restrict_env = getenv("PG_RESTRICT_EXEC")) == NULL
		|| strcmp(restrict_env, "1") != 0)
	{
		PROCESS_INFORMATION pi;
		char	   *cmdline;

		ZeroMemory(&pi, sizeof(pi));

		cmdline = xstrdup(GetCommandLine());

		putenv("PG_RESTRICT_EXEC=1");

		if (!CreateRestrictedProcess(cmdline, &pi))
		{
			fprintf(stderr, "Failed to re-exec with restricted token: %lu.\n", GetLastError());
		}
		else
		{
			/*
			 * Successfully re-execed. Now wait for child process to capture
			 * exitcode.
			 */
			DWORD		x;

			CloseHandle(pi.hThread);
			WaitForSingleObject(pi.hProcess, INFINITE);

			if (!GetExitCodeProcess(pi.hProcess, &x))
			{
				fprintf(stderr, "Failed to get exit code from subprocess: %lu\n", GetLastError());
				exit(1);
			}
			exit(x);
		}
	}
#endif

	/* Like for initdb, check if a valid version of Postgres is running */
	if ((ret = find_other_exec(argv[0], "postgres", PG_BACKEND_VERSIONSTR,
							   backend_exec)) < 0)
	{
		char        full_path[MAXPGPATH];

		if (find_my_exec(argv[0], full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			fprintf(stderr,
					_("The program \"postgres\" is needed by %s "
					  "but was not found in the\n"
					  "same directory as \"%s\".\n"
					  "Check your installation.\n"),
					progname, full_path);
		else
			fprintf(stderr,
					_("The program \"postgres\" was found by \"%s\"\n"
					  "but was not the same version as %s.\n"
					  "Check your installation.\n"),
					full_path, progname);
		exit(1);
	}

	/* store binary directory */
	strcpy(bin_path, backend_exec);
	*last_dir_separator(bin_path) = '\0';
	canonicalize_path(bin_path);

	if (!share_path)
	{
		share_path = pg_malloc(MAXPGPATH);
		get_share_path(backend_exec, share_path);
	}
	else if (!is_absolute_path(share_path))
	{
		fprintf(stderr, _("%s: input file location must be an absolute path\n"), progname);
		exit(1);
	}

	canonicalize_path(share_path);

	effective_user = get_id();

	/* Take into account GTM and GTM-proxy cases */
	if (is_gtm)
		set_input(&conf_file, "gtm.conf.sample");
	else
		set_input(&conf_file, "gtm_proxy.conf.sample");

	if (show_setting || debug)
	{
		fprintf(stderr,
				"VERSION=%s\n"
				"GTMDATA=%s\nshare_path=%s\nGTMPATH=%s\n"
				"GTM_CONF_SAMPLE=%s\n",
				PGXC_VERSION,
				pg_data, share_path, bin_path,
				conf_file);
		if (show_setting)
			exit(0);
	}

	check_input(conf_file);

	printf(_("The files belonging to this GTM system will be owned "
			 "by user \"%s\".\n"
			 "This user must also own the server process.\n\n"),
		   effective_user);

	printf("\n");

	umask(S_IRWXG | S_IRWXO);

	/*
	 * now we are starting to do real work, trap signals so we can clean up
	 */

	/* some of these are not valid on Windows */
#ifdef SIGHUP
	pqsignal(SIGHUP, trapsig);
#endif
#ifdef SIGINT
	pqsignal(SIGINT, trapsig);
#endif
#ifdef SIGQUIT
	pqsignal(SIGQUIT, trapsig);
#endif
#ifdef SIGTERM
	pqsignal(SIGTERM, trapsig);
#endif

	/* Ignore SIGPIPE when writing to backend, so we can clean up */
#ifdef SIGPIPE
	pqsignal(SIGPIPE, SIG_IGN);
#endif

	switch (pg_check_dir(pg_data))
	{
		case 0:
			/* PGDATA not there, must create it */
			printf(_("creating directory %s ... "),
				   pg_data);
			fflush(stdout);

			if (!mkdatadir(NULL))
				exit_nicely();
			else
				check_ok();

			made_new_pgdata = true;
			break;

		case 1:
			/* Present but empty, fix permissions and use it */
			printf(_("fixing permissions on existing directory %s ... "),
				   pg_data);
			fflush(stdout);

			if (chmod(pg_data, S_IRWXU) != 0)
			{
				fprintf(stderr, _("%s: could not change permissions of directory \"%s\": %s\n"),
						progname, pg_data, strerror(errno));
				exit_nicely();
			}
			else
				check_ok();

			found_existing_pgdata = true;
			break;

		case 2:
			/* Present and not empty */
			fprintf(stderr,
					_("%s: directory \"%s\" exists but is not empty\n"),
					progname, pg_data);
			fprintf(stderr,
					_("If you want to create a new GTM system, either remove or empty\n"
					  "the directory \"%s\" or run %s\n"
					  "with an argument other than \"%s\".\n"),
					pg_data, progname, pg_data);
			exit(1);			/* no further message needed */

		default:
			/* Trouble accessing directory */
			fprintf(stderr, _("%s: could not access directory \"%s\": %s\n"),
					progname, pg_data, strerror(errno));
			exit_nicely();
	}

	/* Select suitable configuration settings */
	set_null_conf();

	/* Now create all the text config files */
	setup_config();

	/* Get directory specification used to start this executable */
	strcpy(bin_dir, argv[0]);
	get_parent_directory(bin_dir);

	if (is_gtm)
		printf(_("\nSuccess. You can now start the GTM server using:\n\n"
				 "    %s%s%sgtm%s -D %s%s%s\n"
				 "or\n"
				 "    %s%s%sgtm_ctl%s -Z gtm -D %s%s%s -l logfile start\n\n"),
		   QUOTE_PATH, bin_dir, (strlen(bin_dir) > 0) ? DIR_SEP : "", QUOTE_PATH,
			   QUOTE_PATH, pg_data_native, QUOTE_PATH,
		   QUOTE_PATH, bin_dir, (strlen(bin_dir) > 0) ? DIR_SEP : "", QUOTE_PATH,
			   QUOTE_PATH, pg_data_native, QUOTE_PATH);
	else
		printf(_("\nSuccess. You can now start the GTM proxy server using:\n\n"
				 "    %s%s%sgtm_proxy%s -D %s%s%s\n"
				 "or\n"
				 "    %s%s%sgtm_ctl%s -Z gtm_proxy -D %s%s%s -l logfile start\n\n"),
		   QUOTE_PATH, bin_dir, (strlen(bin_dir) > 0) ? DIR_SEP : "", QUOTE_PATH,
			   QUOTE_PATH, pg_data_native, QUOTE_PATH,
		   QUOTE_PATH, bin_dir, (strlen(bin_dir) > 0) ? DIR_SEP : "", QUOTE_PATH,
			   QUOTE_PATH, pg_data_native, QUOTE_PATH);

	return 0;
}
