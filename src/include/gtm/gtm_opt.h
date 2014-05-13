/*--------------------------------------------------------------------
 * gtm_opt.h
 *
 * External declarations pertaining to gtm/main/gtm_opt.c, gtm/proxy/gtm_proxy_opt.c and
 * gtm/common/gtm_opt_file.l
 *
 * Portions Copyright (c) 2011, Postgres-XC Development Group
 * Portions Copyright (c) 2000-2011, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 * Modified by Koichi Suzuki <koichi.szk@gmail.com>
 *
 * src/include/gtm/gtm_opt.h
 *--------------------------------------------------------------------
 */
#ifndef GTM_OPT_H
#define GTM_OPT_H

/*
 * Certain options can only be set at certain times. The rules are
 * like this:
 *
 * INTERNAL options cannot be set by the user at all, but only through
 * internal processes ("server_version" is an example).  These are GUC
 * variables only so they can be shown by SHOW, etc.
 *
 * POSTMASTER options can only be set when the postmaster starts,
 * either from the configuration file or the command line.
 *
 * SIGHUP options can only be set at postmaster startup or by changing
 * the configuration file and sending the HUP signal to the postmaster
 * or a backend process. (Notice that the signal receipt will not be
 * evaluated immediately. The postmaster and the backend check it at a
 * certain point in their main loop. It's safer to wait than to read a
 * file asynchronously.)
 *
 * BACKEND options can only be set at postmaster startup, from the
 * configuration file, or by client request in the connection startup
 * packet (e.g., from libpq's PGOPTIONS variable).  Furthermore, an
 * already-started backend will ignore changes to such an option in the
 * configuration file.	The idea is that these options are fixed for a
 * given backend once it's started, but they can vary across backends.
 *
 * SUSET options can be set at postmaster startup, with the SIGHUP
 * mechanism, or from SQL if you're a superuser.
 *
 * USERSET options can be set by anyone any time.
 */
typedef enum
{
	GTMC_DEFAULT,
	GTMC_STARTUP,
	GTMC_SIGHUP,
	GTMC_USERSET
} GtmOptContext;

/*
 * The following type records the source of the current setting.  A
 * new setting can only take effect if the previous setting had the
 * same or lower level.  (E.g, changing the config file doesn't
 * override the postmaster command line.)  Tracking the source allows us
 * to process sources in any convenient order without affecting results.
 * Sources <= GTMC_S_OVERRIDE will set the default used by RESET, as well
 * as the current value.  Note that source == GTMC_S_OVERRIDE should be
 * used when setting a GTMC_INTERNAL option.
 *
 * GTMC_S_INTERACTIVE isn't actually a source value, but is the
 * dividing line between "interactive" and "non-interactive" sources for
 * error reporting purposes.
 *
 * GTMC_S_TEST is used when testing values to be stored as per-database or
 * per-user defaults ("doit" will always be false, so this never gets stored
 * as the actual source of any value).	This is an interactive case, but
 * it needs its own source value because some assign hooks need to make
 * different validity checks in this case.
 *
 * NB: see GtmOptSource_Names in gtm_opt.c and gtm_proxy_opt.c if you change this.
 */
typedef enum
{
	GTMC_S_DEFAULT,				/* hard-wired default ("boot_val") */
	GTMC_S_DYNAMIC_DEFAULT,		/* default computed during initialization */
	GTMC_S_ENV_VAR,				/* postmaster environment variable *//* Not used in GTM */
	GTMC_S_FILE,				/* gtm.conf or gtm_proxy.conf */
	GTMC_S_ARGV,				/* postmaster command line */
	GTMC_S_DATABASE,			/* per-database setting *//* Not used in GTM */
	GTMC_S_USER,				/* per-user setting *//* Not used in GTM */
	GTMC_S_DATABASE_USER,		/* per-user-and-database setting *//* Not used in GTM */
	GTMC_S_CLIENT,				/* from client connection request *//* Not used in GTM */
	GTMC_S_OVERRIDE,			/* special case to forcibly set default *//* Not used in GTM */
	GTMC_S_INTERACTIVE,			/* dividing line for error reporting *//* Not used in GTM */
	GTMC_S_TEST,				/* test per-database or per-user setting *//* Not used in GTM */
	GTMC_S_SESSION				/* SET command *//* Not used in GTM */
} GtmOptSource;

/*
 * Parsing the configuration file will return a list of name-value pairs
 * with source location info.
 */
typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *filename;
	int			sourceline;
	struct ConfigVariable *next;
} ConfigVariable;

extern bool ParseConfigFile(const char *config_file, const char *calling_file,
				int depth, int elevel,
				ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigFp(FILE *fp, const char *config_file,
			  int depth, int elevel,
			  ConfigVariable **head_p, ConfigVariable **tail_p);
extern void FreeConfigVariables(ConfigVariable *list);

/*
 * The possible values of an enum variable are specified by an array of
 * name-value pairs.  The "hidden" flag means the value is accepted but
 * won't be displayed when guc.c is asked for a list of acceptable values.
 */
struct config_enum_entry
{
	const char *name;
	int			val;
	bool		hidden;
};

/*
 * Signatures for per-variable check/assign/show hook functions
 */
/* No hook in GTM */
#if 0
typedef bool (*GtmOptBoolCheckHook) (bool *newval, void **extra, GtmOptSource source);
typedef bool (*GtmOptIntCheckHook) (int *newval, void **extra, GtmOptSource source);
typedef bool (*GtmOptRealCheckHook) (double *newval, void **extra, GtmOptSource source);
typedef bool (*GtmOptStringCheckHook) (char **newval, void **extra, GtmOptSource source);
typedef bool (*GtmOptEnumCheckHook) (int *newval, void **extra, GtmOptSource source);

typedef void (*GtmOptBoolAssignHook) (bool newval, void *extra);
typedef void (*GtmOptIntAssignHook) (int newval, void *extra);
typedef void (*GtmOptRealAssignHook) (double newval, void *extra);
typedef void (*GtmOptStringAssignHook) (const char *newval, void *extra);
typedef void (*GtmOptEnumAssignHook) (int newval, void *extra);

typedef const char *(*GtmOptShowHook) (void);
#endif

/*
 * Miscellaneous
 */
/*
 * GTM does not have SET command so it is not used in GTM.  It's a dummy.
 */
typedef enum
{
	/* Types of set_config_option actions */
	GTMOPT_ACTION_SET,				/* regular SET command */
	GTMOPT_ACTION_LOCAL,			/* SET LOCAL command */
	GTMOPT_ACTION_SAVE				/* function SET option */
} GtmOptAction;

#define GTMOPT_QUALIFIER_SEPARATOR '.'

/*
 * bit values in "flags" of a GUC variable
 */
#define GTMOPT_LIST_INPUT			0x0001	/* input can be list format */
#define GTMOPT_LIST_QUOTE			0x0002	/* double-quote list elements */
#define GTMOPT_NO_SHOW_ALL			0x0004	/* exclude from SHOW ALL */
#define GTMOPT_NO_RESET_ALL			0x0008	/* exclude from RESET ALL */
#define GTMOPT_REPORT				0x0010	/* auto-report changes to client */
#define GTMOPT_NOT_IN_SAMPLE		0x0020	/* not in postgresql.conf.sample */
#define GTMOPT_DISALLOW_IN_FILE		0x0040	/* can't set in postgresql.conf */
#define GTMOPT_CUSTOM_PLACEHOLDER	0x0080	/* placeholder for custom variable */
#define GTMOPT_SUPERUSER_ONLY		0x0100	/* show only to superusers */
#define GTMOPT_IS_NAME				0x0200	/* limit string to NAMEDATALEN-1 */

#define GTMOPT_UNIT_KB				0x0400	/* value is in kilobytes */
#define GTMOPT_UNIT_BLOCKS			0x0800	/* value is in blocks */
#define GTMOPT_UNIT_XBLOCKS			0x0C00	/* value is in xlog blocks */
#define GTMOPT_UNIT_MEMORY			0x0C00	/* mask for KB, BLOCKS, XBLOCKS */

#define GTMOPT_UNIT_MS				0x1000	/* value is in milliseconds */
#define GTMOPT_UNIT_S				0x2000	/* value is in seconds */
#define GTMOPT_UNIT_MIN				0x4000	/* value is in minutes */
#define GTMOPT_UNIT_TIME			0x7000	/* mask for MS, S, MIN */

#define GTMOPT_NOT_WHILE_SEC_REST	0x8000	/* can't set if security restricted */

/*
 * Functions exported by gtm_opt.c
 */
extern void SetConfigOption(const char *name, const char *value,
				GtmOptContext context, GtmOptSource source);

extern void EmitWarningsOnPlaceholders(const char *className);

extern const char *GetConfigOption(const char *name, bool restrict_superuser);
extern const char *GetConfigOptionResetString(const char *name);
extern void ProcessConfigFile(GtmOptContext context);
extern void InitializeGTMOptions(void);
extern bool SelectConfigFiles(const char *userDoption, const char *progname);
extern void ResetAllOptions(void);
extern int	NewGTMNestLevel(void);
extern bool parse_int(const char *value, int *result, int flags,
					  const char **hintmsg);
extern bool parse_real(const char *value, double *result);
extern bool set_config_option(const char *name, const char *value,
							  GtmOptContext context, GtmOptSource source,
							  bool changeVal);

extern char *GetConfigOptionByName(const char *name, const char **varname);
extern void GetConfigOptionByNum(int varnum, const char **values, bool *noshow);
extern int	GetNumConfigOptions(void);
extern void ParseLongOption(const char *string, char **name, char **value);

#ifndef PG_KRB_SRVTAB
#define PG_KRB_SRVTAB ""
#endif
#ifndef PG_KRB_SRVNAM
#define PG_KRB_SRVNAM ""
#endif

/* upper limit for GUC variables measured in kilobytes of memory */
/* note that various places assume the byte size fits in a "long" variable */
#if SIZEOF_SIZE_T > 4 && SIZEOF_LONG > 4
#define MAX_KILOBYTES	INT_MAX
#else
#define MAX_KILOBYTES	(INT_MAX / 1024)
#endif

#ifdef TRACE_SORT
extern bool trace_sort;
#endif
#ifdef TRACE_SYNCSCAN
extern bool trace_syncscan;
#endif
#ifdef DEBUG_BOUNDED_SORT
extern bool optimize_bounded_sort;
#endif


/*
 * Log_min_messages ENUM strings
 */
#define Server_Message_Level_Options()\
static const struct config_enum_entry server_message_level_options[] = {\
	{"debug", DEBUG2, true},\
	{"debug5", DEBUG5, false},\
	{"debug4", DEBUG4, false},\
	{"debug3", DEBUG3, false},\
	{"debug2", DEBUG2, false},\
	{"debug1", DEBUG1, false},\
	{"info", INFO, false},\
	{"notice", NOTICE, false},\
	{"warning", WARNING, false},\
	{"error", ERROR, false},\
	{"log", LOG, false},\
	{"fatal", FATAL, false},\
	{"panic", PANIC, false},\
	{NULL, 0, false}\
}

/*
 * Server Startup Option ENUM strings
 */
#define Gtm_Startup_Mode_Options()\
static const struct config_enum_entry gtm_startup_mode_options[] = {\
	{"act", GTM_ACT_MODE, false},\
	{"standby", GTM_STANDBY_MODE, false},\
	{NULL, 0, false}\
}

/*
 * Displayable names for context types (enum GtmContext)
 *
 * Note: these strings are deliberately not localized.
 */
#define gtmOptContext_Names()\
const char *const GtmOptContext_Names[] =\
{\
	 /* GTMC_STGARTUP */	"startup",\
	 /* GTMC_SIGHUP */		"sighup"\
}

/*
 * Displayable names for source types (enum GtmSource)
 *
 * Note: these strings are deliberately not localized.
 */
#define gtmOptSource_Names()\
const char *const GtmOptSource_Names[] =\
{\
	 /* GTMC_S_DEFAULT */ 			"default",\
	 /* GTMC_S_DYNAMIC_DEFAULT */ 	"default",\
	 /* GTMC_S_ENV_VAR */ 			"environment variable",\
	 /* GTMC_S_FILE */ 				"configuration file",\
	 /* GTMC_S_ARGV */ 				"command line",\
	 /* GTMC_S_DATABASE */ 			"database",\
	 /* GTMC_S_USER */ 				"user",\
	 /* GTMC_S_DATABASE_USER */ 	"database user",\
	 /* GTMC_S_CLIENT */ 			"client",\
	 /* GTMC_S_OVERRIDE */ 			"override",\
	 /* GTMC_S_INTERACTIVE */ 		"interactive",\
	 /* GTMC_S_TEST */ 				"test",\
	 /* GTMC_S_SESSION */ 			"session"\
}

/*
 * Displayable names for GTM variable types (enum config_type)
 *
 * Note: these strings are deliberately not localized.
 */
#define Config_Type_Names()\
const char *const config_type_names[] =\
{\
	 /* GTMC_BOOL */	"bool",\
	 /* GTMC_INT */		"integer",\
	 /* GTMC_REAL */	"real",\
	 /* GTMC_STRING */	"string",\
	 /* GTMC_ENUM */	"enum"\
}


/*
 * Option name defintion --- common to gtm.conf and gtm_proxy.conf
 *
 * This will be used both in *.conf and command line option override.
 */

#define GTM_OPTNAME_ACTIVE_HOST			"active_host"
#define GTM_OPTNAME_ACTIVE_PORT 		"active_port"
#define GTM_OPTNAME_CONFIG_FILE			"config_file"
#define GTM_OPTNAME_DATA_DIR			"data_dir"
#define GTM_OPTNAME_ERROR_REPORTER		"error_reporter"
#define GTM_OPTNAME_CONNECT_RETRY_INTERVAL "gtm_connect_retry_interval"
#define GTM_OPTNAME_GTM_HOST			"gtm_host"
#define GTM_OPTNAME_GTM_PORT			"gtm_port"
#define GTM_OPTNAME_KEEPALIVES_IDLE		"keepalives_idle"
#define GTM_OPTNAME_KEEPALIVES_INTERVAL	"keepalives_interval"
#define GTM_OPTNAME_KEEPALIVES_COUNT	"keepalives_count"
#define GTM_OPTNAME_LISTEN_ADDRESSES	"listen_addresses"
#define GTM_OPTNAME_LOG_FILE			"log_file"
#define GTM_OPTNAME_LOG_MIN_MESSAGES	"log_min_messages"
#define GTM_OPTNAME_NODENAME			"nodename"
#define GTM_OPTNAME_PORT				"port"
#define GTM_OPTNAME_STARTUP				"startup"
#define GTM_OPTNAME_STATUS_READER		"status_reader"
#define GTM_OPTNAME_SYNCHRONOUS_BACKUP	"synchronous_backup"
#define GTM_OPTNAME_WORKER_THREADS		"worker_threads"


#endif   /* GTM_OPT_H */
