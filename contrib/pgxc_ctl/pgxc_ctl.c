/*-------------------------------------------------------------------------
 *
 * pgxc_ctl.c
 *
 *    Main module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
/*
 *  PXC_CTL  Postgres-XC configurator and operation tool
 *
 *
 * Command line options
 *
 * -c --configuration file : configuration file.  Rerative path
 *							start at $HOME/.pgxc_ctl or homedir if
 *							specified by --home option
 * --home homedir : home directory of pgxc_ctl.  Default is
 *					$HOME/.pgxc_ctl.  You can override this
 *					with PGXC_CTL_HOME environment or option.
 *					Command argument has the highest priority.
 *
 * -v | --verbose: verbose mode.  You can set your default in
 *					pgxc_ctl_rc file at home.
 *
 * --silent: Opposite to --verbose.
 *
 * -V | --version: prints out the version
 *
 * -l | --logdir dir: Log directory.   Default is $home/pgxc_log
 *
 * -L | --logfile file: log file.  Default is the timestamp.
 *					Relative path starts with --logdir.
 *
 */


#include <stdlib.h>
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <getopt.h>

#include "config.h"
#include "variables.h"
#include "pgxc_ctl.h"
#include "bash_handler.h"
#include "signature.h"
#include "pgxc_ctl_log.h"
#include "varnames.h"
#include "do_command.h"
#include "utils.h"

/*
 * Common global variable
 */
char pgxc_ctl_home[MAXPATH+1];
char pgxc_ctl_bash_path[MAXPATH+1];
char pgxc_ctl_config_path[MAXPATH+1];
char progname[MAXPATH+1];
char *myName;
char *defaultDatabase;
#define versionString "V1.0 for Postgres-XC 1.1"

FILE *inF;
FILE *outF;

static void build_pgxc_ctl_home(char *home);
static void trim_trailing_slash(char *path);
static void startLog(char *path, char *logFileNam);
static void print_version(void);
static void print_help(void);

static void
trim_trailing_slash(char *path)
{
	char *curr = path;
	char *last = path;
	
	while (*curr)
	{
		last = curr;
		curr++;
	}
	while (last != path)
	{
		if (*last == '/')
		{
			last = 0;
			last--;
			continue;
		}
		else
			return;
	}
}
	

static void
build_pgxc_ctl_home(char *home)
{
	char *env_pgxc_ctl_home = getenv(PGXC_CTL_HOME);
	char *env_home = getenv(HOME);		/* We assume this is always available */

	if (home)
	{
		if (home[0] == '/')
		{
			/* Absolute path */
			strncpy(pgxc_ctl_home, home, MAXPATH);
			goto set_bash;
		}
		else
		{
			/* Relative path */
			trim_trailing_slash(home);
			snprintf(pgxc_ctl_home, MAXPATH, "%s/%s", env_home, home);
			goto set_bash;
		}
	}
	if ((env_pgxc_ctl_home = getenv(PGXC_CTL_HOME)) == NULL)
	{
		snprintf(pgxc_ctl_home, MAXPATH, "%s/%s", env_home, pgxc_ctl_home_def);
		goto set_bash;
	}
	if (env_pgxc_ctl_home[0] == '/') /* Absoute path */
	{
		strncpy(pgxc_ctl_home, env_pgxc_ctl_home, MAXPATH);
		goto set_bash;
	}
	trim_trailing_slash(env_pgxc_ctl_home);
	if (env_pgxc_ctl_home[0] == '\0' || env_pgxc_ctl_home[0] == ' ' || env_pgxc_ctl_home[0] == '\t')
	{
		/* Null environment */
		snprintf(pgxc_ctl_home, MAXPATH, "%s/%s", env_home, pgxc_ctl_home_def);
		goto set_bash;
	}
	snprintf(pgxc_ctl_home, MAXPATH, "%s/%s", env_home, home);
	goto set_bash;

set_bash:
	snprintf(pgxc_ctl_bash_path, MAXPATH, "%s/%s", pgxc_ctl_home, PGXC_CTL_BASH);
	/*
	 * Create home dir if necessary and change current directory to it.
	 */
	{
		struct stat buf;
		char	cmd[MAXLINE+1];
		
		if (stat(pgxc_ctl_home, &buf) ==0)
		{
			if (S_ISDIR(buf.st_mode))
			{
				Chdir(pgxc_ctl_home, TRUE);
				return;
			}
			else
			{
				fprintf(stderr, "%s is not directory.  Check your configurfation\n", pgxc_ctl_home);
				exit(1);
			}
		}
		snprintf(cmd, MAXLINE, "mkdir -p %s", pgxc_ctl_home);
		system(cmd);
		if (stat(pgxc_ctl_home, &buf) ==0)
		{
			if (S_ISDIR(buf.st_mode))
			{
				Chdir(pgxc_ctl_home, TRUE);
				return;
			}
			else
			{
				fprintf(stderr, "Creating %s directory failed. Check your configuration\n", pgxc_ctl_home);
				exit(1);
			}
		}
		fprintf(stderr, "Creating directory %s failed. %s\n", pgxc_ctl_home, strerror(errno));
		exit(1);
	}
	return;
}


static void
build_configuration_path(char *path)
{
	struct stat statbuf;
	int rr;

	if (path)
		reset_var_val(VAR_configFile, path);
	if (!find_var(VAR_configFile) || !sval(VAR_configFile) || (sval(VAR_configFile)[0] == 0))
	{
		/* Default */
		snprintf(pgxc_ctl_config_path, MAXPATH, "%s/%s", pgxc_ctl_home, DEFAULT_CONF_FILE_NAME);
		rr = stat(pgxc_ctl_config_path, &statbuf);
		if (rr || !S_ISREG(statbuf.st_mode))
		{
			/* No configuration specified and the default does not apply --> simply ignore */
			elog(ERROR, "ERROR: Default configuration file \"%s\" was not found while no configuration file was specified\n", 
				pgxc_ctl_config_path);
			pgxc_ctl_config_path[0] = 0;
			return;
		}
	}
		else if (sval(VAR_configFile)[0]  == '/')
	{
		/* Absolute path */
		strncpy(pgxc_ctl_config_path, sval(VAR_configFile), MAXPATH);
	}
	else
	{
		/* Relative path from $pgxc_ctl_home */
		snprintf(pgxc_ctl_config_path, MAXPATH, "%s/%s", pgxc_ctl_home, sval(VAR_configFile));
	}
	rr = stat(pgxc_ctl_config_path, &statbuf);
	if (rr || !S_ISREG(statbuf.st_mode))
	{
		if (rr)
			elog(ERROR, "ERROR: File \"%s\" not found or not a regular file. %s\n", 
					 pgxc_ctl_config_path, strerror(errno));
		else
			elog(ERROR, "ERROR: File \"%s\" not found or not a regular file", 
					 pgxc_ctl_config_path);
	}
	return;
}


static void
read_configuration(void)
{
	FILE *conf;
	char cmd[MAXPATH+1];

	install_pgxc_ctl_bash(pgxc_ctl_bash_path);
	if (pgxc_ctl_config_path[0])
		snprintf(cmd, MAXPATH, "%s --home %s --configuration %s", 
				 pgxc_ctl_bash_path, pgxc_ctl_home, pgxc_ctl_config_path);
	else
		snprintf(cmd, MAXPATH, "%s --home %s", pgxc_ctl_bash_path, pgxc_ctl_home);
	elog(NOTICE, "Reading configuration using %s\n", cmd);
	conf = popen(cmd, "r");
	if (conf == NULL)
	{
		elog(ERROR, "ERROR: Cannot execute %s, %s", cmd, strerror(errno));
		return;
	}
	read_vars(conf);
	fclose(conf);
	uninstall_pgxc_ctl_bash(pgxc_ctl_bash_path);
	elog(INFO, "Finished to read configuration.\n");
}

static void
prepare_pgxc_ctl_bash(char *path)
{
	struct stat buf;
	int rc;

	rc = stat(path, &buf);
	if (rc)
		install_pgxc_ctl_bash(path);
	else
		if (S_ISREG(buf.st_mode))
			return;
	rc = stat(path, &buf);
	if (S_ISREG(buf.st_mode))
		return;
	fprintf(stderr, "Error: caould not install bash script %s\n", path);
	exit(1);
}

static void
pgxcCtlMkdir(char *path)
{
	char cmd[MAXPATH+1];

	snprintf(cmd, MAXPATH, "mkdir -p %s", path);
	system(cmd);
}

static void
startLog(char *path, char *logFileNam)
{
	char logFilePath[MAXPATH+1];

	if(path)
	{
		trim_trailing_slash(path);
		pgxcCtlMkdir(path);
		if(logFileNam)
		{
			if (logFileNam[0] == '/')
			{
				fprintf(stderr, "ERROR: both --logdir and --logfile are specified and logfile was abosolute path.\n");
				exit(1);
			}
			if (path[0] == '/')
				snprintf(logFilePath, MAXPATH, "%s/%s", path, logFileNam);
			else
				snprintf(logFilePath, MAXPATH, "%s/%s/%s", pgxc_ctl_home, path, logFileNam);
			initLog(NULL, logFilePath);
		}
		else
		{
			if (path[0] == '/')
				initLog(path, NULL);
			else
			{
				snprintf(logFilePath, MAXPATH, "%s/%s", pgxc_ctl_home, path);
				initLog(logFilePath, NULL);
			}
		}
	}
	else
	{
		if (logFileNam && logFileNam[0] == '/')
		{
			/* This is used as log file path */
			initLog(NULL, logFileNam);
			return;
		}
		else
		{
			snprintf(logFilePath, MAXPATH, "%s/pgxc_log", pgxc_ctl_home);
			pgxcCtlMkdir(logFilePath);
			initLog(logFilePath, NULL);
		}
	}
	return;
}

static void
setDefaultIfNeeded(char *name, char *val)
{
	if (!find_var(name) || !sval(name))
	{
		if (val)
			reset_var_val(name, val);
		else
			reset_var(name);
	}
}

static void
setup_my_env(void)
{
	char path[MAXPATH+1];
	char *home;
	FILE *ini_env;

	char *selectVarList[] = {
		VAR_pgxc_ctl_home,
		VAR_xc_prompt,
		VAR_verbose,
		VAR_logDir,
		VAR_logFile,
		VAR_tmpDir,
		VAR_localTmpDir,
		VAR_configFile,
		VAR_echoAll,
		VAR_debug,
		VAR_printMessage,
		VAR_logMessage,
		VAR_defaultDatabase,
		VAR_pgxcCtlName,
		VAR_printLocation,
		VAR_logLocation,
		NULL
	};

	ini_env = fopen("/etc/pgxc_ctl", "r");
	if (ini_env)
	{
		read_selected_vars(ini_env, selectVarList);
		fclose(ini_env);
	}
	if ((home = getenv("HOME")))
	{
		snprintf(path, MAXPATH, "%s/.pgxc_ctl", getenv("HOME"));
		if ((ini_env = fopen(path, "r")))
		{
			read_selected_vars(ini_env, selectVarList);
			fclose(ini_env);
		}
	}
	/*
	 * Setup defaults
	 */
	snprintf(path, MAXPATH, "%s/pgxc_ctl", getenv("HOME"));
	setDefaultIfNeeded(VAR_pgxc_ctl_home, path);
	setDefaultIfNeeded(VAR_xc_prompt, "PGXC ");
	snprintf(path, MAXPATH, "%s/pgxc_ctl/pgxc_log", getenv("HOME"));
	setDefaultIfNeeded(VAR_logDir, path);
	setDefaultIfNeeded(VAR_logFile, NULL);
	setDefaultIfNeeded(VAR_tmpDir, "/tmp");
	setDefaultIfNeeded(VAR_localTmpDir, "/tmp");
	setDefaultIfNeeded(VAR_configFile, "pgxc_ctl.conf");
	setDefaultIfNeeded(VAR_echoAll, "n");
	setDefaultIfNeeded(VAR_debug, "n");
	setDefaultIfNeeded(VAR_printMessage, "info");
	setDefaultIfNeeded(VAR_logMessage, "info");
	setDefaultIfNeeded(VAR_pgxcCtlName, DefaultName);
	myName = Strdup(sval(VAR_pgxcCtlName));
	setDefaultIfNeeded(VAR_defaultDatabase, DefaultDatabase);
	defaultDatabase = Strdup(sval(VAR_defaultDatabase));
	setDefaultIfNeeded(VAR_printLocation, "n");
	setDefaultIfNeeded(VAR_logLocation, "n");
}

int
main(int argc, char *argv[])
{
	char *configuration = NULL;
	char *infile = NULL;
	char *outfile = NULL;
	char *verbose = NULL;
	int version_opt = 0;
	char *logdir = NULL;
	char *logfile = NULL;
	char *home = NULL;
	int help_opt = 0;

	int c;
	
	static struct option long_options[] = {
		{"configuration",	required_argument, 0, 'c'},
		{"silent", no_argument, 0, 1},
		{"verbose", no_argument, 0, 'v'},
		{"version", no_argument, 0, 'V'},
		{"logdir", required_argument, 0, 'l'},
		{"logfile", required_argument, 0, 'L'},
		{"home", required_argument, 0, 2},
		{"infile", required_argument, 0, 'i'},
		{"outfile", required_argument, 0, 'o'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	strcpy(progname, argv[0]);
	init_var_hash();

	while(1) {
		int option_index = 0;

		c = getopt_long(argc, argv, "i:o:c:vVl:L:h", long_options, &option_index);

		if (c == -1)
			break;
		switch(c)
		{
			case 1:
				verbose = "n";
				break;
			case 2:
				if (home)
					free (home);
				home = strdup(optarg);
				break;
			case 'i':
				if (infile)
					free(infile);
				infile = strdup(optarg);
				break;
			case 'o':
				if (outfile)
					free(outfile);
				outfile = strdup(optarg);
				break;
			case 'v':
				verbose = "y";
				break;
			case 'V':
				version_opt = 1;
				break;
			case 'l':
				if (logdir)
					free(logdir);
				logdir = strdup(optarg);
				break;
			case 'L':
				if (logfile)
					free(logfile);
				logfile = strdup(optarg);
				break;
			case 'c':
				if (configuration)
					free(configuration);
				configuration = strdup(optarg);
				break;
			case 'h':
				help_opt = 1;
				break;
			default:
				fprintf(stderr, "Invalid optin value, received code 0%o\n", c);
				exit(1);
		}
	}
	if (version_opt || help_opt)
	{
		if (version_opt)
			print_version();
		if (help_opt)
			print_help();
		exit(0);
	}
	setup_my_env();		/* Read $HOME/.pgxc_ctl */
	build_pgxc_ctl_home(home);
	if (infile)
		reset_var_val(VAR_configFile, infile);
	if (logdir)
		reset_var_val(VAR_logDir, logdir);
	if (logfile)
		reset_var_val(VAR_logFile, logfile);
	startLog(sval(VAR_logDir), sval(VAR_logFile));
	prepare_pgxc_ctl_bash(pgxc_ctl_bash_path);
	build_configuration_path(configuration);
	read_configuration();
	check_configuration();
	/*
	 * Setop output
	 */
	if (outfile)
	{
		elog(INFO, "Output file: %s\n", outfile);
		if ((outF = fopen(outfile, "w")))
			dup2(fileno(outF),2);
		else
			elog(ERROR, "ERROR: Cannot open output file %s, %s\n", outfile, strerror(errno));
	}
	else
		outF = stdout;
	/* 
	 * Startup Message
	 */
	elog(NOTICE, "   ******** PGXC_CTL START ***************\n\n");
	elog(NOTICE, "Current directory: %s\n", pgxc_ctl_home);
	/*
	 * Setup input
	 */
	if (infile)
	{
		elog(INFO, "Input file: %s\n", infile);
		inF =  fopen(infile, "r");
		if(inF == NULL)
		{
			elog(ERROR, "ERROR: Cannot open input file %s, %s\n", infile, strerror(errno));
			exit(1);
		}
	}
	else
		inF = stdin;
	/*
	 * If we have remaing arguments, they will be treated as a command to do.  Do this
	 * first, then handle the input from input file specified by -i option.
	 * If it is not found, then exit.
	 */
#if 0
	print_vars();
#endif
	if (optind < argc)
	{
		char orgBuf[MAXLINE + 1];
		char wkBuf[MAXLINE + 1];
		orgBuf[0] = 0;
		while (optind < argc)
		{
			strncat(orgBuf, argv[optind++], MAXLINE);
			strncat(orgBuf, " ", MAXLINE);
		}
		strncpy(wkBuf, orgBuf, MAXLINE);
		do_singleLine(orgBuf, wkBuf);
		if (infile)
			do_command(inF, outF);
	}
	else
		do_command(inF, outF);
	exit(0);
}

static void
print_version(void)
{
	printf("Pgxc_ctl %s\n", versionString);
}

static void
print_help(void)
{
	printf(
	"pgxc_ctl [option ...] [command]\n"
	"option:\n"
	"   -c or --configuration conf_file: Specify configruration file.\n"
	"   -v or --verbose: Specify verbose output.\n"
	"   -V or --version: Print version and exit.\n"
	"   -l or --logdir log_directory: specifies what directory to write logs.\n"
	"   -L or --logfile log_file: Specifies log file.\n"
	"   --home home_direcotry: Specifies pgxc_ctl work director.\n"
	"   -i or --infile input_file: Specifies inptut file.\n"
	"   -o or --outfile output_file: Specifies output file.\n"
	"   -h or --help: Prints this message and exits.\n"
	"For more deatils, refer to pgxc_ctl reference manual included in\n"
	"postgres-xc reference manual.\n");
}
