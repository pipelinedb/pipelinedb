/*--------------------------------------------------------------------
 * guc.c
 *
 * Support for grand unified configuration scheme, including SET
 * command, configuration file, and
 command line options.
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc.c
 *
 *--------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

#include "gtm/gtm.h"
#include "gtm/path.h"
#include "gtm/gtm_opt_tables.h"
#include "gtm/gtm_opt.h"
#include "gtm/gtm_standby.h"

#define CONFIG_FILENAME "gtm_proxy.conf"
const char *config_filename = CONFIG_FILENAME;

/*
 * Variables declared elsewhere for gtm, mainly option variables.
 */

extern char *GTMProxyNodeName;
extern char *ListenAddresses;
extern int GTMPortNumber;
extern char *error_reporter;
extern char *status_reader;
extern int log_min_messages;
extern int tcp_keepalives_idle;
extern int tcp_keepalives_count;
extern int tcp_keepalives_interval;
extern char *GTMServerHost;
extern int GTMProxyPortNumber;
extern int GTMConnectRetryInterval;
extern int GTMServerPortNumber;
extern int GTMProxyWorkerThreads;
extern char *GTMProxyDataDir;
extern char *GTMProxyConfigFileName;
extern char *GTMConfigFileName;


/*
 * Macros for values
 *
 * Some of them are declared also in proxy_main.c.
 */
#define GTM_PROXY_DEFAULT_WORKERS 2

/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level)
 */

Server_Message_Level_Options();

static const struct config_enum_entry gtm_startup_mode_options[] = {
	{"act", GTM_ACT_MODE, false},
	{"standby", GTM_STANDBY_MODE, false},
	{NULL, 0, false}
};


/*
 * GTM option variables that are exported from this module
 */
char	   *data_directory;
char	   *GTMConfigFileName;


/*
 * Displayable names for context types (enum GtmContext)
 */
gtmOptContext_Names();

/*
 * Displayable names for source types (enum GtmSource)
 *
 */
gtmOptSource_Names();

/*
 * Displayable names for GTM variable types (enum config_type)
 *
 * Note: these strings are deliberately not localized.
 */
Config_Type_Names();


/*
 * Contents of GTM tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION:
 *
 * 1. Declare a global variable of type bool, int, double, or char*
 *	  and make use of it.
 *
 * 2. Decide at what times it's safe to set the option. See guc.h for
 *	  details.
 *
 * 3. Decide on a name, a default value, upper and lower bounds (if
 *	  applicable), etc.
 *
 * 4. Add a record below.
 *
 * 5. Add it to src/backend/utils/misc/postgresql.conf.sample, if
 *	  appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GTMOPT_LIST option you must edit pg_dumpall.c to ensure
 *	  it is not single quoted at dump time.
 */

/*
 * Definition of option name strings are given in gtm_opt.h, both for gtm.conf
 * and gtm_proxy.conf.   They will be used in command line option handling too.
 */


/******** option records follow ********/

struct config_bool ConfigureNamesBool[] =
{
	/* End-of-list marker */
	{
		{NULL, 0, NULL, NULL, 0}, NULL, false, false, NULL
	}
};


struct config_int ConfigureNamesInt[] =
{
	{
		{
			GTM_OPTNAME_PORT, GTMC_STARTUP,
			gettext_noop("Listen Port of GTM_Proxy server."),
			NULL,
			0
		},
		&GTMProxyPortNumber,
		6666, 0, INT_MAX,
		0, NULL
	},
	{
		{
			GTM_OPTNAME_GTM_PORT, GTMC_SIGHUP,
			gettext_noop("GTM server port number."),
			NULL,
			0
		},
		&GTMServerPortNumber,
		6666, 0, INT_MAX,
	    0, NULL
	},
	{
		{
			GTM_OPTNAME_CONNECT_RETRY_INTERVAL, GTMC_SIGHUP,
			gettext_noop("Interval in second to detect reconnect command."),
			NULL,
			GTMOPT_UNIT_TIME
		},
		&GTMConnectRetryInterval,
		60, 0, INT_MAX,
		0, NULL
	},
	{
		{
			GTM_OPTNAME_KEEPALIVES_IDLE, GTMC_STARTUP,
			gettext_noop("Sets \"keepalives_idle\" option for the connection to GTM."),
		    NULL,
			GTMOPT_UNIT_TIME
		},
		&tcp_keepalives_idle,
		0, 0, INT_MAX,
		0, NULL
	},
	{
		{
			GTM_OPTNAME_KEEPALIVES_INTERVAL, GTMC_STARTUP,
			gettext_noop("Sets \"keepalives_interval\" option fo the connetion to GTM."),
		 	NULL,
			GTMOPT_UNIT_TIME
		},
		&tcp_keepalives_interval,
		0, 0, INT_MAX,
		0, NULL
	},
	{
		{
			GTM_OPTNAME_KEEPALIVES_COUNT, GTMC_STARTUP,
			gettext_noop("Sets \"keepalives_count\" option to the connection to GTM."),
			NULL,
			0
		},
		&tcp_keepalives_count,
		0, 0, INT_MAX,
		0, NULL
	},
	{
		{
			GTM_OPTNAME_WORKER_THREADS, GTMC_STARTUP,
			gettext_noop("Number of worker thread."),
			NULL,
			0
		},
		&GTMProxyWorkerThreads,
		GTM_PROXY_DEFAULT_WORKERS, 1, INT_MAX,
		0, NULL
	},
	/* End-of-list marker */
	{
		{NULL, 0, NULL, NULL, 0}, NULL, 0, 0, 0, 0, NULL
	}
};


struct config_real ConfigureNamesReal[] =
{
	/* End-of-list marker */
	{
		{NULL, 0, NULL, NULL, 0}, NULL, 0.0, 0.0, 0.0, 0.0, NULL
	}
};

struct config_string ConfigureNamesString[] =
{
	{
		{
			GTM_OPTNAME_DATA_DIR, GTMC_STARTUP,
			gettext_noop("Work directory."),
			NULL,
			0
		},
		&GTMProxyDataDir,
		NULL,
		NULL,
		NULL
	},

	{
		{
			GTM_OPTNAME_CONFIG_FILE, GTMC_SIGHUP,
		 	gettext_noop("Configuration file name."),
		 	NULL,
		 	0
		},
		&GTMConfigFileName,
		CONFIG_FILENAME,
		NULL,
		NULL
	},

	{
		{
			GTM_OPTNAME_LISTEN_ADDRESSES, GTMC_STARTUP,
			gettext_noop("Listen address."),
			NULL,
			0
		},
		&ListenAddresses,
		"*",
		NULL, NULL
	},

	{
		{
			GTM_OPTNAME_NODENAME, GTMC_STARTUP,
		 	gettext_noop("My node name."),
			NULL,
		 	0,
		},
		&GTMProxyNodeName,
		NULL,
		NULL, NULL
	},

	{
		{
			GTM_OPTNAME_GTM_HOST, GTMC_SIGHUP,
			gettext_noop("Address of target GTM ACT."),
			NULL,
			0
		},
		&GTMServerHost,
		NULL,
		NULL, NULL
	},

	{
		{
			GTM_OPTNAME_LOG_FILE, GTMC_SIGHUP,
			gettext_noop("Log file name."),
			NULL,
			0
		},
		&GTMLogFile,
		"gtm_proxy.log",
		NULL, NULL
	},

	{
		{
			GTM_OPTNAME_ERROR_REPORTER, GTMC_SIGHUP,
			gettext_noop("Command to report various errors."),
			NULL,
			0
		},
		&error_reporter,
		NULL,
		NULL, NULL
	},

	{
		{
			GTM_OPTNAME_STATUS_READER, GTMC_SIGHUP,
			gettext_noop("Command to get status of global XC node status."),
			gettext_noop("Runs when configuration file is read by SIGHUP"),
			0
		},
		&status_reader,
		NULL,
		NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, NULL, NULL}, NULL, NULL, NULL, NULL
	}
};


struct config_enum ConfigureNamesEnum[] =
{
	{
		{
			GTM_OPTNAME_LOG_MIN_MESSAGES, GTMC_SIGHUP,
			gettext_noop("Minimum message level to write to the log file."),
			NULL,
		 	0
		},
		&log_min_messages,
		WARNING,
		server_message_level_options,
		WARNING, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, NULL, NULL, 0}, NULL, 0, NULL, 0, NULL
	}
};

/******** end of options list ********/

/*
 * Actual lookup of variables is done through this single, sorted array.
 */
struct config_generic **gtm_opt_variables;

/* Current number of variables contained in the vector */
int	num_gtm_opt_variables;

/* Vector capacity */
int	size_gtm_opt_variables;


bool reporting_enabled;	/* TRUE to enable GTMOPT_REPORT */

int	GTMOptUpdateCount = 0; /* Indicates when specific option is updated */
