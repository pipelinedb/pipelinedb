/* -*-pgsql-c-*- */
/*
 * Scanner for the configuration file
 *
 * Copyright (c) 2000-2011, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/guc-file.l
 */

#include "gtm/gtm.h"

#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

#include "mb/pg_wchar.h"
#include "gtm/path.h"
#include "gtm/assert.h"
#include "gtm/gtm_opt.h"
#include "gtm/gtm_opt_tables.h"
#include "gtm/elog.h"
#include "gtm_opt_scanner.c"

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  ereport(ERROR, (errmsg_internal("%s", msg)))

static unsigned int ConfigFileLineno;

/* flex fails to supply a prototype for GTMOPT_yylex, so provide one */
int GTMOPT_GTMOPT_yylex(void);

/* Functions defined in this file */
static char *GTMOPT_scanstr(const char *s);
static struct config_generic *find_option(const char *name, bool create_placeholders, int elevel);
static char *gtm_opt_strdup(int elevel, const char *src);
static int gtm_opt_name_compare(const char *namea, const char *nameb);
struct config_generic **get_gtm_opt_variables(void);
void build_gtm_opt_variables(void);
static bool gtm_opt_parse_bool(const char *value, bool *result);
static bool gtm_opt_parse_bool_with_len(const char *value, size_t len, bool *result);
static void set_config_sourcefile(const char *name, char *sourcefile, int sourceline);
static int gtm_opt_var_compare(const void *a, const void *b);
static void InitializeOneGTMOption(struct config_generic * gconf);
static void ReportGTMOption(struct config_generic * record);
static char *_ShowOption(struct config_generic * record, bool use_units);

/*
 * Variables to bel fed by specific option definition: gtm_opt.c and gtm_proxy_opt.c
 */
extern char *GTMConfigFileName;
extern char	   *data_directory;
extern struct config_generic **gtm_opt_variables;
extern int num_gtm_opt_variables;
extern int	size_gtm_opt_variables;
extern bool reporting_enabled;	/* TRUE to enable GTMOPT_REPORT */
extern char *config_filename;   /* Default configuration file name */
extern int	GTMOptUpdateCount; /* Indicates when specific option is updated */
extern bool isStartUp;

/*
 * Tables of options: to be defined in gtm_opt.c and gtm_proxy_opt.c
 */
extern struct config_bool ConfigureNamesBool[];
extern struct config_int ConfigureNamesInt[];
extern struct config_real ConfigureNamesReal[];
extern struct config_string ConfigureNamesString[];
extern struct config_enum ConfigureNamesEnum[];

/*
 * Note: MAX_BACKENDS is limited to 2^23-1 because inval.c stores the
 * backend ID as a 3-byte signed integer.  Even if that limitation were
 * removed, we still could not exceed INT_MAX/4 because some places compute
 * 4*MaxBackends without any overflow check.  This is rechecked in
 * check_maxconnections, since MaxBackends is computed as MaxConnections
 * plus autovacuum_max_workers plus one (for the autovacuum launcher).
 */
#define MAX_BACKENDS	0x7fffff

#define KB_PER_MB (1024)
#define KB_PER_GB (1024*1024)

#define MS_PER_S 1000
#define S_PER_MIN 60
#define MS_PER_MIN (1000 * 60)
#define MIN_PER_H 60
#define S_PER_H (60 * 60)
#define MS_PER_H (1000 * 60 * 60)
#define MIN_PER_D (60 * 24)
#define S_PER_D (60 * 60 * 24)
#define MS_PER_D (1000 * 60 * 60 * 24)

/*
 * Exported function to read and process the configuration file. The
 * parameter indicates in what context the file is being read --- either
 * postmaster startup (including standalone-backend startup) or SIGHUP.
 * All options mentioned in the configuration file are set to new values.
 * If an error occurs, no values will be changed.
 */
void
ProcessConfigFile(GtmOptContext context)
{
	int			elevel;
	ConfigVariable *item,
				   *head,
				   *tail;
	char	   *cvc = NULL;
	int			i;

	Assert((context == GTMC_STARTUP || context == GTMC_SIGHUP));

	if (context == GTMC_SIGHUP)
		elevel = DEBUG2;
	else
		elevel = ERROR;

	/* Parse the file into a list of option names and values */
	head = tail = NULL;

	if (!ParseConfigFile(GTMConfigFileName, NULL, 0, elevel, &head, &tail))
		goto cleanup_list;

#if 0
	/* No custom_variable_classes now */
	/*
	 * This part of the code remained the same as original guc.c because
	 * we might want to have custom variable class for gtm.conf.
	 */
	/*
	 * We need the proposed new value of custom_variable_classes to check
	 * custom variables with.  ParseConfigFile ensured that if it's in
	 * the file, it's first in the list.  But first check to see if we
	 * have an active value from the command line, which should override
	 * the file in any case.  (Since there's no relevant env var, the
	 * only possible nondefault sources are the file and ARGV.)
	 */
	cvc_struct = (struct config_string *)
		find_option("custom_variable_classes", false, elevel);
	Assert(cvc_struct);
	if (cvc_struct->gen.reset_source > GTMC_S_FILE)
	{
		cvc = gtm_opt_strdup(elevel, cvc_struct->reset_val);
		if (cvc == NULL)
			goto cleanup_list;
	}
	else if (head != NULL &&
			 gtm_opt_name_compare(head->name, "custom_variable_classes") == 0)
	{
		/*
		 * Need to canonicalize the value by calling the check hook.
		 */
		void   *extra = NULL;

		cvc = gtm_opt_strdup(elevel, head->value);
		if (cvc == NULL)
			goto cleanup_list;
		if (extra)
			free(extra);
	}
#endif

	/*
	 * Mark all extant GUC variables as not present in the config file.
	 * We need this so that we can tell below which ones have been removed
	 * from the file since we last processed it.
	 */
	for (i = 0; i < num_gtm_opt_variables; i++)
	{
		struct config_generic *gconf = gtm_opt_variables[i];

		gconf->status &= ~GTMOPT_IS_IN_FILE;
	}

	/*
	 * Check if all options are valid.  As a side-effect, the GTMOPT_IS_IN_FILE
	 * flag is set on each GUC variable mentioned in the list.
	 */
	for (item = head; item; item = item->next)
	{
		char *sep = strchr(item->name, GTMOPT_QUALIFIER_SEPARATOR);

		if (sep)
		{
			/*
			 * There is no GUC entry.  If we called set_config_option then
			 * it would make a placeholder, which we don't want to do yet,
			 * since we could still fail further down the list.  Do nothing
			 * (assuming that making the placeholder will succeed later).
			 */
			if (find_option(item->name, false, elevel) == NULL)
				continue;
			/*
			 * 3. There is already a GUC entry (either real or placeholder) for
			 * the variable.  In this case we should let set_config_option
			 * check it, since the assignment could well fail if it's a real
			 * entry.
			 */
		}

		if (!set_config_option(item->name, item->value, context,
							   GTMC_S_FILE, false))
			goto cleanup_list;
	}

	/*
	 * Check for variables having been removed from the config file, and
	 * revert their reset values (and perhaps also effective values) to the
	 * boot-time defaults.  If such a variable can't be changed after startup,
	 * just throw a warning and continue.  (This is analogous to the fact that
	 * set_config_option only throws a warning for a new but different value.
	 * If we wanted to make it a hard error, we'd need an extra pass over the
	 * list so that we could throw the error before starting to apply
	 * changes.)
	 */
	for (i = 0; i < num_gtm_opt_variables; i++)
	{
		struct config_generic *gconf = gtm_opt_variables[i];
		GtmOptStack   *stack;

		if (gconf->reset_source != GTMC_S_FILE ||
			(gconf->status & GTMOPT_IS_IN_FILE))
			continue;
		if (gconf->context < GTMC_SIGHUP)
		{
			/*
			 * In the original code, errcode() stores specified error code to sqlerrcode, which does not
			 * exist in GTM.
			 */
			if (isStartUp)
			{
				write_stderr("parameter \"%s\" cannot be changed without restarting the server",
							 gconf->name);
			}
			else
			{
				ereport(elevel,
						(0,
						 errmsg("parameter \"%s\" cannot be changed without restarting the server",
								gconf->name)));
			}
			continue;
		}

		/*
		 * Reset any "file" sources to "default", else set_config_option
		 * will not override those settings.
		 */
		if (gconf->reset_source == GTMC_S_FILE)
			gconf->reset_source = GTMC_S_DEFAULT;
		if (gconf->source == GTMC_S_FILE)
			gconf->source = GTMC_S_DEFAULT;
		for (stack = gconf->stack; stack; stack = stack->prev)
		{
			if (stack->source == GTMC_S_FILE)
				stack->source = GTMC_S_DEFAULT;
		}

		/* Now we can re-apply the wired-in default (i.e., the boot_val) */
		set_config_option(gconf->name, NULL, context, GTMC_S_DEFAULT,
						  true);
		if (context == GTMC_SIGHUP)
		{
			if (isStartUp)
			{
				write_stderr("parameter \"%s\" removed from configuration file, reset to default\n",
							 gconf->name);
			}
			else
			{
				ereport(elevel,
						(errmsg("parameter \"%s\" removed from configuration file, reset to default",
								gconf->name)));
			}
		}
	}

	/*
	 * Restore any variables determined by environment variables or
	 * dynamically-computed defaults.  This is a no-op except in the case
	 * where one of these had been in the config file and is now removed.
	 *
	 * In particular, we *must not* do this during the postmaster's
	 * initial loading of the file, since the timezone functions in
	 * particular should be run only after initialization is complete.
	 *
	 * XXX this is an unmaintainable crock, because we have to know how
	 * to set (or at least what to call to set) every variable that could
	 * potentially have GTMC_S_DYNAMIC_DEFAULT or GTMC_S_ENV_VAR source.
	 * However, there's no time to redesign it for 9.1.
	 */

	/* If we got here all the options checked out okay, so apply them. */
	for (item = head; item; item = item->next)
	{
		char   *pre_value = NULL;

		if (set_config_option(item->name, item->value, context,
								 GTMC_S_FILE, true))
		{
			set_config_sourcefile(item->name, item->filename,
								  item->sourceline);

			if (pre_value)
			{
				const char *post_value = GetConfigOption(item->name, false);

				if (!post_value)
					post_value = "";
				if (strcmp(pre_value, post_value) != 0)
				{
					if (isStartUp)
					{
						write_stderr("parameter \"%s\" changed to \"%s\"\n",
									 item->name, item->value);
					}
					else
					{
						ereport(elevel,
								(errmsg("parameter \"%s\" changed to \"%s\"",
										item->name, item->value)));
					}
				}
			}
		}

		if (pre_value)
			free(pre_value);
	}

	/* PGXCTODO: configuration file reload time update */

cleanup_list:
	FreeConfigVariables(head);
	if (cvc)
		free(cvc);
}

/*
 * See next function for details. This one will just work with a config_file
 * name rather than an already opened File Descriptor
 */
bool
ParseConfigFile(const char *config_file, const char *calling_file,
				int depth, int elevel,
				ConfigVariable **head_p,
				ConfigVariable **tail_p)
{
	bool		OK = true;
	FILE	   *fp;
	char		abs_path[MAXPGPATH];

	/*
	 * Reject too-deep include nesting depth.  This is just a safety check
	 * to avoid dumping core due to stack overflow if an include file loops
	 * back to itself.  The maximum nesting depth is pretty arbitrary.
	 */
	if (depth > 10)
	{
		if (isStartUp)
		{
			write_stderr("could not open configuration file \"%s\": maximum nesting depth exceeded\n",
						 config_file);
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("could not open configuration file \"%s\": maximum nesting depth exceeded",
							config_file)));
		}
		return false;
	}

	/*
	 * If config_file is a relative path, convert to absolute.  We consider
	 * it to be relative to the directory holding the calling file.
	 */
	if (!is_absolute_path(config_file))
	{
		if (calling_file != NULL)
		{
			strlcpy(abs_path, calling_file, sizeof(abs_path));
			get_parent_directory(abs_path);
			join_path_components(abs_path, abs_path, config_file);
			canonicalize_path(abs_path);
			config_file = abs_path;
		}
		else
		{
			/*
			 * calling_file is NULL, we make an absolute path from $PGDATA
			 */
			join_path_components(abs_path, data_directory, config_file);
			canonicalize_path(abs_path);
			config_file = abs_path;
		}
	}

	fp = fopen(config_file, "r");
	if (!fp)
	{
		if (isStartUp)
		{
			write_stderr("could not open configuration file \"%s\": %m\n",
						 config_file);
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("could not open configuration file \"%s\": %m",
							config_file)));
		}
		return false;
	}

	OK = ParseConfigFp(fp, config_file, depth, elevel, head_p, tail_p);

	fclose(fp);

	return OK;
}

/*
 * Read and parse a single configuration file.  This function recurses
 * to handle "include" directives.
 *
 * Input parameters:
 *	fp: file pointer from AllocateFile for the configuration file to parse
 *	config_file: absolute or relative path of file to read
 *	depth: recursion depth (used only to prevent infinite recursion)
 *	elevel: error logging level determined by ProcessConfigFile()
 * Output parameters:
 *	head_p, tail_p: head and tail of linked list of name/value pairs
 *
 * *head_p and *tail_p must be initialized to NULL before calling the outer
 * recursion level.  On exit, they contain a list of name-value pairs read
 * from the input file(s).
 *
 * Returns TRUE if successful, FALSE if an error occurred.  The error has
 * already been ereport'd, it is only necessary for the caller to clean up
 * its own state and release the name/value pairs list.
 *
 * Note: if elevel >= ERROR then an error will not return control to the
 * caller, and internal state such as open files will not be cleaned up.
 * This case occurs only during postmaster or standalone-backend startup,
 * where an error will lead to immediate process exit anyway; so there is
 * no point in contorting the code so it can clean up nicely.
 */
bool
ParseConfigFp(FILE *fp, const char *config_file, int depth, int elevel,
			  ConfigVariable **head_p, ConfigVariable **tail_p)
{
	bool		OK = true;
	YY_BUFFER_STATE lex_buffer;
	int			token;

	/*
	 * Parse
	 */
	lex_buffer = GTMOPT_yy_create_buffer(fp, YY_BUF_SIZE);
	GTMOPT_yy_switch_to_buffer(lex_buffer);

	ConfigFileLineno = 1;

	/* This loop iterates once per logical line */
	while ((token = GTMOPT_yylex()))
	{
		char	   *opt_name, *opt_value;
		ConfigVariable *item;

		if (token == GTMOPT_EOL)	/* empty or comment line */
			continue;

		/* first token on line is option name */
		if (token != GTMOPT_ID && token != GTMOPT_QUALIFIED_ID)
			goto parse_error;
		opt_name = strdup(GTMOPT_yytext);

		/* next we have an optional equal sign; discard if present */
		token = GTMOPT_yylex();
		if (token == GTMOPT_EQUALS)
			token = GTMOPT_yylex();

		/* now we must have the option value */
		if (token != GTMOPT_ID &&
			token != GTMOPT_STRING &&
			token != GTMOPT_INTEGER &&
			token != GTMOPT_REAL &&
			token != GTMOPT_UNQUOTED_STRING)
			goto parse_error;
		if (token == GTMOPT_STRING)	/* strip quotes and escapes */
			opt_value = GTMOPT_scanstr(GTMOPT_yytext);
		else
			opt_value = strdup(GTMOPT_yytext);

		/* now we'd like an end of line, or possibly EOF */
		token = GTMOPT_yylex();
		if (token != GTMOPT_EOL)
		{
			if (token != 0)
				goto parse_error;
			/* treat EOF like \n for line numbering purposes, cf bug 4752 */
			ConfigFileLineno++;
		}

		/* OK, process the option name and value */
		if (gtm_opt_name_compare(opt_name, "include") == 0)
		{
			/*
			 * An include directive isn't a variable and should be processed
			 * immediately.
			 */
			unsigned int save_ConfigFileLineno = ConfigFileLineno;

			if (!ParseConfigFile(opt_value, config_file,
								 depth + 1, elevel,
								 head_p, tail_p))
			{
				free(opt_name);
				free(opt_value);
				OK = false;
				goto cleanup_exit;
			}
			GTMOPT_yy_switch_to_buffer(lex_buffer);
			ConfigFileLineno = save_ConfigFileLineno;
			free(opt_name);
			free(opt_value);
		}
		else if (gtm_opt_name_compare(opt_name, "custom_variable_classes") == 0)
		{
			/*
			 * This variable must be processed first as it controls
			 * the validity of other variables; so it goes at the head
			 * of the result list.  If we already found a value for it,
			 * replace with this one.
			 */
			item = *head_p;
			if (item != NULL &&
				gtm_opt_name_compare(item->name, "custom_variable_classes") == 0)
			{
				/* replace existing head item */
				free(item->name);
				free(item->value);
				item->name = opt_name;
				item->value = opt_value;
				item->filename = strdup(config_file);
				item->sourceline = ConfigFileLineno-1;
			}
			else
			{
				/* prepend to list */
				item = malloc(sizeof *item);
				item->name = opt_name;
				item->value = opt_value;
				item->filename = strdup(config_file);
				item->sourceline = ConfigFileLineno-1;
				item->next = *head_p;
				*head_p = item;
				if (*tail_p == NULL)
					*tail_p = item;
			}
		}
		else
		{
			/* ordinary variable, append to list */
			item = malloc(sizeof *item);
			item->name = opt_name;
			item->value = opt_value;
			item->filename = strdup(config_file);
			item->sourceline = ConfigFileLineno-1;
			item->next = NULL;
			if (*head_p == NULL)
				*head_p = item;
			else
				(*tail_p)->next = item;
			*tail_p = item;
		}

		/* break out of loop if read EOF, else loop for next line */
		if (token == 0)
			break;
	}

	/* successful completion of parsing */
	goto cleanup_exit;

 parse_error:
	if (token == GTMOPT_EOL || token == 0)
	{
		if (isStartUp)
		{
			write_stderr("syntax error in file \"%s\" line %u, near end of line\n",
						 config_file, ConfigFileLineno - 1);
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("syntax error in file \"%s\" line %u, near end of line",
							config_file, ConfigFileLineno - 1)));
		}
	}
	else
	{
		if (isStartUp)
		{
			write_stderr("syntax error in file \"%s\" line %u, near token \"%s\"\n",
						 config_file, ConfigFileLineno, GTMOPT_yytext);
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("syntax error in file \"%s\" line %u, near token \"%s\"",
							config_file, ConfigFileLineno, GTMOPT_yytext)));
		}
	}
	OK = false;

cleanup_exit:
	GTMOPT_yy_delete_buffer(lex_buffer);
	return OK;
}


/*
 * Free a list of ConfigVariables, including the names and the values
 */
void
FreeConfigVariables(ConfigVariable *list)
{
	ConfigVariable *item;

	item = list;
	while (item)
	{
		ConfigVariable *next = item->next;

		free(item->name);
		free(item->value);
		free(item->filename);
		free(item);
		item = next;
	}
}


/*
 *		scanstr
 *
 * Strip the quotes surrounding the given string, and collapse any embedded
 * '' sequences and backslash escapes.
 *
 * the string returned is malloc'd and should eventually be free'd by the
 * caller.
 */
static char *
GTMOPT_scanstr(const char *s)
{
	char	   *newStr;
	int			len,
				i,
				j;

	Assert(s != NULL && s[0] == '\'');
	len = strlen(s);
	Assert(len >= 2);
	Assert(s[len-1] == '\'');

	/* Skip the leading quote; we'll handle the trailing quote below */
	s++, len--;

	/* Since len still includes trailing quote, this is enough space */
	newStr = malloc(len);

	for (i = 0, j = 0; i < len; i++)
	{
		if (s[i] == '\\')
		{
			i++;
			switch (s[i])
			{
				case 'b':
					newStr[j] = '\b';
					break;
				case 'f':
					newStr[j] = '\f';
					break;
				case 'n':
					newStr[j] = '\n';
					break;
				case 'r':
					newStr[j] = '\r';
					break;
				case 't':
					newStr[j] = '\t';
					break;
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
					{
						int			k;
						long		octVal = 0;

						for (k = 0;
							 s[i + k] >= '0' && s[i + k] <= '7' && k < 3;
							 k++)
							octVal = (octVal << 3) + (s[i + k] - '0');
						i += k - 1;
						newStr[j] = ((char) octVal);
					}
					break;
				default:
					newStr[j] = s[i];
					break;
			}					/* switch */
		}
		else if (s[i] == '\'' && s[i+1] == '\'')
		{
			/* doubled quote becomes just one quote */
			newStr[j] = s[++i];
		}
		else
			newStr[j] = s[i];
		j++;
	}

	/* We copied the ending quote to newStr, so replace with \0 */
	Assert(j > 0 && j <= len);
	newStr[--j] = '\0';

	return newStr;
}

/*
 * The following code includes most of the code ported from guc.c.
 * Because they should be shared by gtm_opt.c and gtm_proxy_opt.c, they are placed here.
 */

/*
 * Some infrastructure for checking malloc/strdup/realloc calls
 */
static void *
gtm_opt_malloc(int elevel, size_t size)
{
	void	   *data;

	data = malloc(size);
	if (data == NULL)
	{
		if (isStartUp)
		{
			write_stderr("out of memory\n");
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("out of memory")));
		}
	}
	return data;
}

#if 0
/* PGXCTODO: this will be used for future extensions */
static void *
gtm_opt_realloc(int elevel, void *old, size_t size)
{
	void	   *data;

	data = realloc(old, size);
	if (data == NULL)
	{
		if (isStartUp)
		{
			write_stderr("out of memory\n");
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("out of memory")));
		}
	}
	return data;
}
#endif

static char *
gtm_opt_strdup(int elevel, const char *src)
{
	char	   *data;

	data = strdup(src);
	if (data == NULL)
	{
		if (isStartUp)
		{
			write_stderr("out of memory\n");
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("out of memory")));
		}
	}
	return data;
}

/*
 * Detect whether strval is referenced anywhere in a GTM string item
 */
static bool
string_field_used(struct config_string * conf, char *strval)
{
	GtmOptStack   *stack;

	if (strval == *(conf->variable) ||
		strval == conf->reset_val ||
		strval == conf->boot_val)
		return true;
	for (stack = conf->gen.stack; stack; stack = stack->prev)
	{
		if (strval == stack->prior.val.stringval ||
			strval == stack->masked.val.stringval)
			return true;
	}
	return false;
}


/*
 * Support for assigning to a field of a string GTM item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void
set_string_field(struct config_string * conf, char **field, char *newval)
{
	char	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/* Free old value if it's not NULL and isn't referenced anymore */
	if (oldval && !string_field_used(conf, oldval))
		free(oldval);
}


/*
 * Detect whether an "extra" struct is referenced anywhere in a GTM item
 */
static bool
extra_field_used(struct config_generic * gconf, void *extra)
{
	GtmOptStack   *stack;

	if (extra == gconf->extra)
		return true;
	switch (gconf->vartype)
	{
		case GTMC_BOOL:
			if (extra == ((struct config_bool *) gconf)->reset_extra)
				return true;
			break;
		case GTMC_INT:
			if (extra == ((struct config_int *) gconf)->reset_extra)
				return true;
			break;
		case GTMC_REAL:
			if (extra == ((struct config_real *) gconf)->reset_extra)
				return true;
			break;
		case GTMC_STRING:
			if (extra == ((struct config_string *) gconf)->reset_extra)
				return true;
			break;
		case GTMC_ENUM:
			if (extra == ((struct config_enum *) gconf)->reset_extra)
				return true;
			break;
	}
	for (stack = gconf->stack; stack; stack = stack->prev)
	{
		if (extra == stack->prior.extra ||
			extra == stack->masked.extra)
			return true;
	}

	return false;
}


/*
 * Support for assigning to an "extra" field of a GTM item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void
set_extra_field(struct config_generic * gconf, void **field, void *newval)
{
	void	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/* Free old value if it's not NULL and isn't referenced anymore */
	if (oldval && !extra_field_used(gconf, oldval))
		free(oldval);
}


/*
 * Support for copying a variable's active value into a stack entry.
 * The "extra" field associated with the active value is copied, too.
 *
 * NB: be sure stringval and extra fields of a new stack entry are
 * initialized to NULL before this is used, else we'll try to free() them.
 */
static void
set_stack_value(struct config_generic * gconf, config_var_value *val)
{
	switch (gconf->vartype)
	{
		case GTMC_BOOL:
			val->val.boolval =
				*((struct config_bool *) gconf)->variable;
			break;
		case GTMC_INT:
			val->val.intval =
				*((struct config_int *) gconf)->variable;
			break;
		case GTMC_REAL:
			val->val.realval =
				*((struct config_real *) gconf)->variable;
			break;
		case GTMC_STRING:
			set_string_field((struct config_string *) gconf,
							 &(val->val.stringval),
							 *((struct config_string *) gconf)->variable);
			break;
		case GTMC_ENUM:
			val->val.enumval =
				*((struct config_enum *) gconf)->variable;
			break;
	}
	set_extra_field(gconf, &(val->extra), gconf->extra);
}

#if 0
/* PGXCTODO: This is let for future extension support */
/*
 * Support for discarding a no-longer-needed value in a stack entry.
 * The "extra" field associated with the stack entry is cleared, too.
 */
static void
discard_stack_value(struct config_generic * gconf, config_var_value *val)
{
	switch (gconf->vartype)
	{
		case GTMC_BOOL:
		case GTMC_INT:
		case GTMC_REAL:
		case GTMC_ENUM:
			/* no need to do anything */
			break;
		case GTMC_STRING:
			set_string_field((struct config_string *) gconf,
							 &(val->val.stringval),
							 NULL);
			break;
	}
	set_extra_field(gconf, &(val->extra), NULL);
}
#endif

/*
 * Fetch the sorted array pointer (exported for help_config.c's use ONLY)
 */
struct config_generic **
get_gtm_opt_variables(void)
{
	return gtm_opt_variables;
}

/*
 * Build the sorted array.	This is split out so that it could be
 * re-executed after startup (eg, we could allow loadable modules to
 * add vars, and then we'd need to re-sort).
 */
void
build_gtm_opt_variables(void)
{
	int			size_vars;
	int			num_vars = 0;
	struct config_generic **gtm_opt_vars;
	int			i;

	for (i = 0; ConfigureNamesBool[i].gen.name; i++)
	{
		struct config_bool *conf = &ConfigureNamesBool[i];

		/* Rather than requiring vartype to be filled in by hand, do this: */
		conf->gen.vartype = GTMC_BOOL;
		num_vars++;
	}

	for (i = 0; ConfigureNamesInt[i].gen.name; i++)
	{
		struct config_int *conf = &ConfigureNamesInt[i];

		conf->gen.vartype = GTMC_INT;
		num_vars++;
	}

	for (i = 0; ConfigureNamesReal[i].gen.name; i++)
	{
		struct config_real *conf = &ConfigureNamesReal[i];

		conf->gen.vartype = GTMC_REAL;
		num_vars++;
	}

	for (i = 0; ConfigureNamesString[i].gen.name; i++)
	{
		struct config_string *conf = &ConfigureNamesString[i];

		conf->gen.vartype = GTMC_STRING;
		num_vars++;
	}

	for (i = 0; ConfigureNamesEnum[i].gen.name; i++)
	{
		struct config_enum *conf = &ConfigureNamesEnum[i];

		conf->gen.vartype = GTMC_ENUM;
		num_vars++;
	}

	/*
	 * Create table with 20% slack
	 */
	size_vars = num_vars + num_vars / 4;

	gtm_opt_vars = (struct config_generic **)
		gtm_opt_malloc(FATAL, size_vars * sizeof(struct config_generic *));

	num_vars = 0;

	for (i = 0; ConfigureNamesBool[i].gen.name; i++)
		gtm_opt_vars[num_vars++] = &ConfigureNamesBool[i].gen;

	for (i = 0; ConfigureNamesInt[i].gen.name; i++)
		gtm_opt_vars[num_vars++] = &ConfigureNamesInt[i].gen;

	for (i = 0; ConfigureNamesReal[i].gen.name; i++)
		gtm_opt_vars[num_vars++] = &ConfigureNamesReal[i].gen;

	for (i = 0; ConfigureNamesString[i].gen.name; i++)
		gtm_opt_vars[num_vars++] = &ConfigureNamesString[i].gen;

	for (i = 0; ConfigureNamesEnum[i].gen.name; i++)
		gtm_opt_vars[num_vars++] = &ConfigureNamesEnum[i].gen;

	if (gtm_opt_variables)
		free(gtm_opt_variables);
	gtm_opt_variables = gtm_opt_vars;
	num_gtm_opt_variables = num_vars;
	size_gtm_opt_variables = size_vars;
	qsort((void *) gtm_opt_variables, num_gtm_opt_variables,
		  sizeof(struct config_generic *), gtm_opt_var_compare);
}


#if 0
/* PGXCTODO: This is let for future extension support */
/*
 * Add a new GTM variable to the list of known variables. The
 * list is expanded if needed.
 */
static bool
add_gtm_opt_variable(struct config_generic * var, int elevel)
{
	if (num_gtm_opt_variables + 1 >= size_gtm_opt_variables)
	{
		/*
		 * Increase the vector by 25%
		 */
		int			size_vars = size_gtm_opt_variables + size_gtm_opt_variables / 4;
		struct config_generic **gtm_opt_vars;

		if (size_vars == 0)
		{
			size_vars = 100;
			gtm_opt_vars = (struct config_generic **)
				gtm_opt_malloc(elevel, size_vars * sizeof(struct config_generic *));
		}
		else
		{
			gtm_opt_vars = (struct config_generic **)
				gtm_opt_realloc(elevel, gtm_opt_variables, size_vars * sizeof(struct config_generic *));
		}

		if (gtm_opt_vars == NULL)
			return false;		/* out of memory */

		gtm_opt_variables = gtm_opt_vars;
		size_gtm_opt_variables = size_vars;
	}
	gtm_opt_variables[num_gtm_opt_variables++] = var;
	qsort((void *) gtm_opt_variables, num_gtm_opt_variables,
		  sizeof(struct config_generic *), gtm_opt_var_compare);
	return true;
}


/*
 * Create and add a placeholder variable. It's presumed to belong
 * to a valid custom variable class at this point.
 */
static struct config_generic *
add_placeholder_variable(const char *name, int elevel)
{
	size_t		sz = sizeof(struct config_string) + sizeof(char *);
	struct config_string *var;
	struct config_generic *gen;

	var = (struct config_string *) gtm_opt_malloc(elevel, sz);
	if (var == NULL)
		return NULL;
	memset(var, 0, sz);
	gen = &var->gen;

	gen->name = gtm_opt_strdup(elevel, name);
	if (gen->name == NULL)
	{
		free(var);
		return NULL;
	}

	gen->context = GTMC_USERSET;
	gen->short_desc = "GTM placeholder variable";
	gen->flags = GTMOPT_NO_SHOW_ALL | GTMOPT_NOT_IN_SAMPLE | GTMOPT_CUSTOM_PLACEHOLDER;
	gen->vartype = GTMC_STRING;

	/*
	 * The char* is allocated at the end of the struct since we have no
	 * 'static' place to point to.	Note that the current value, as well as
	 * the boot and reset values, start out NULL.
	 */
	var->variable = (char **) (var + 1);

	if (!add_gtm_opt_variable((struct config_generic *) var, elevel))
	{
		free((void *) gen->name);
		free(var);
		return NULL;
	}

	return gen;
}
#endif

/*
 * Look up option NAME.  If it exists, return a pointer to its record,
 * else return NULL.  If create_placeholders is TRUE, we'll create a
 * placeholder record for a valid-looking custom variable name.
 */
static struct config_generic *
find_option(const char *name, bool create_placeholders, int elevel)
{
	const char **key = &name;
	struct config_generic **res;

	Assert(name);

	/*
	 * By equating const char ** with struct config_generic *, we are assuming
	 * the name field is first in config_generic.
	 */
	res = (struct config_generic **) bsearch((void *) &key,
											 (void *) gtm_opt_variables,
											 num_gtm_opt_variables,
											 sizeof(struct config_generic *),
											 gtm_opt_var_compare);
	if (res)
		return *res;

	/* Unknown name */
	return NULL;
}


/*
 * comparator for qsorting and bsearching gtm_opt_variables array
 */
static int
gtm_opt_var_compare(const void *a, const void *b)
{
	struct config_generic *confa = *(struct config_generic **) a;
	struct config_generic *confb = *(struct config_generic **) b;

	return gtm_opt_name_compare(confa->name, confb->name);
}


/*
 * the bare comparison function for GTM names
 */
static int
gtm_opt_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * array ordering has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}


/*
 * Initialize GTM options during program startup.
 *
 * Note that we cannot read the config file yet, since we have not yet
 * processed command-line switches.
 */
void
InitializeGTMOptions(void)
{
	int			i;

	/*
	 * Build sorted array of all GTM variables.
	 */
	build_gtm_opt_variables();

	/*
	 * Load all variables with their compiled-in defaults, and initialize
	 * status fields as needed.
	 */
	for (i = 0; i < num_gtm_opt_variables; i++)
	{
		InitializeOneGTMOption(gtm_opt_variables[i]);
	}

	reporting_enabled = false;

}


/*
 * Initialize one GTM option variable to its compiled-in default.
 *
 * Note: the reason for calling check_hooks is not that we think the boot_val
 * might fail, but that the hooks might wish to compute an "extra" struct.
 */
static void
InitializeOneGTMOption(struct config_generic * gconf)
{
	gconf->status = 0;
	gconf->reset_source = GTMC_S_DEFAULT;
	gconf->source = GTMC_S_DEFAULT;
	gconf->stack = NULL;
	gconf->extra = NULL;
	gconf->sourcefile = NULL;
	gconf->sourceline = 0;
	gconf->context = GTMC_DEFAULT;

	switch (gconf->vartype)
	{
		case GTMC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) gconf;
				bool		newval = conf->boot_val;
				void	   *extra = NULL;

				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case GTMC_INT:
			{
				struct config_int *conf = (struct config_int *) gconf;
				int			newval = conf->boot_val;
				void	   *extra = NULL;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case GTMC_REAL:
			{
				struct config_real *conf = (struct config_real *) gconf;
				double		newval = conf->boot_val;
				void	   *extra = NULL;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case GTMC_STRING:
			{
				struct config_string *conf = (struct config_string *) gconf;
				char	   *newval;
				void	   *extra = NULL;

				/* non-NULL boot_val must always get strdup'd */
				if (conf->boot_val != NULL)
					newval = gtm_opt_strdup(FATAL, conf->boot_val);
				else
					newval = NULL;

				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
		case GTMC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) gconf;
				int			newval = conf->boot_val;
				void	   *extra = NULL;

				*conf->variable = conf->reset_val = newval;
				conf->gen.extra = conf->reset_extra = extra;
				break;
			}
	}
}


/*
 * Select the configuration files and data directory to be used, and
 * do the initial read of postgresql.conf.
 *
 * This is called after processing command-line switches.
 *		userDoption is the -D switch value if any (NULL if unspecified).
 *		progname is just for use in error messages.
 *
 * Returns true on success; on failure, prints a suitable error message
 * to stderr and returns false.
 */
bool
SelectConfigFiles(const char *userDoption, const char *progname)
{
	char	   *configdir;
	char	   *fname;
	struct stat stat_buf;

	/* configdir is -D option, or $PGDATA if no -D */
	if (userDoption)
		configdir = make_absolute_path(userDoption);
	else
		configdir = NULL;

	/*
	 * Find the configuration file: if config_file was specified on the
	 * command line, use it, else use configdir/postgresql.conf.  In any case
	 * ensure the result is an absolute path, so that it will be interpreted
	 * the same way by future backends.
	 */
	if (GTMConfigFileName)
	{
		if (GTMConfigFileName[0] == '/')
			fname = make_absolute_path(GTMConfigFileName);
		else
		{
			if (configdir)
			{
				fname = gtm_opt_malloc(FATAL,
									   strlen(configdir) + strlen(GTMConfigFileName) + 2);
				sprintf(fname, "%s/%s", configdir, GTMConfigFileName);
			}
			else
				fname = make_absolute_path(GTMConfigFileName);
		}
	}
	else if (configdir)
	{
		fname = gtm_opt_malloc(FATAL,
						   strlen(configdir) + strlen(config_filename) + 2);
		sprintf(fname, "%s/%s", configdir, config_filename);
	}
	else
	{
		write_stderr("%s does not know where to find the server configuration file.\n"
					 "You must specify the --config-file or -D invocation "
					 "option or set the PGDATA environment variable.\n",
					 progname);
		return false;
	}

	/*
	 * Set the GTMConfigFileName GTM variable to its final value, ensuring that
	 * it can't be overridden later.
	 */
	SetConfigOption("config_file", fname, GTMC_STARTUP, GTMC_S_OVERRIDE);
	free(fname);

	/*
	 * Now read the config file for the first time.
	 */
	if (stat(GTMConfigFileName, &stat_buf) != 0)
	{
		write_stderr("%s cannot access the server configuration file \"%s\": %s\n",
					 progname, GTMConfigFileName, strerror(errno));
		return false;
	}

	ProcessConfigFile(GTMC_STARTUP);

	free(configdir);

	return true;
}

/*
 * Reset all options to their saved default values (implements RESET ALL)
 */
void
ResetAllOptions(void)
{
	int			i;

	for (i = 0; i < num_gtm_opt_variables; i++)
	{
		struct config_generic *gconf = gtm_opt_variables[i];

		/* Don't reset if special exclusion from RESET ALL */
		if (gconf->flags & GTMOPT_NO_RESET_ALL)
			continue;
		/* No need to reset if wasn't SET */
		if (gconf->source <= GTMC_S_OVERRIDE)
			continue;

		switch (gconf->vartype)
		{
			case GTMC_BOOL:
				{
					struct config_bool *conf = (struct config_bool *) gconf;

					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case GTMC_INT:
				{
					struct config_int *conf = (struct config_int *) gconf;

					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case GTMC_REAL:
				{
					struct config_real *conf = (struct config_real *) gconf;

					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case GTMC_STRING:
				{
					struct config_string *conf = (struct config_string *) gconf;

					set_string_field(conf, conf->variable, conf->reset_val);
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
			case GTMC_ENUM:
				{
					struct config_enum *conf = (struct config_enum *) gconf;

					*conf->variable = conf->reset_val;
					set_extra_field(&conf->gen, &conf->gen.extra,
									conf->reset_extra);
					break;
				}
		}

		gconf->source = gconf->reset_source;

		if (gconf->flags & GTMOPT_REPORT)
			ReportGTMOption(gconf);
	}
}



/*
 * push_old_value
 *		Push previous state during transactional assignment to a GTM variable.
 */
static void
push_old_value(struct config_generic * gconf)
{
	GtmOptStack   *stack;

	/* If we're not inside a nest level, do nothing */
	if (GTMOptUpdateCount == 0)
		return;

	/* Do we already have a stack entry of the current nest level? */
	stack = gconf->stack;
	if (stack && stack->nest_level >= GTMOptUpdateCount)
		return;

	/*
	 * Push a new stack entry
	 *
	 * We keep all the stack entries in TopTransactionContext for simplicity.
	 */
	stack = (GtmOptStack *) MemoryContextAllocZero(TopMemoryContext,
												sizeof(GtmOptStack));

	stack->prev = gconf->stack;
	stack->nest_level = GTMOptUpdateCount;
	stack->source = gconf->source;
	set_stack_value(gconf, &stack->prior);

	gconf->stack = stack;
}



/*
 * Enter a new nesting level for GTM values.  This is called at subtransaction
 * start and when entering a function that has proconfig settings.	NOTE that
 * we must not risk error here, else subtransaction start will be unhappy.
 */
int
NewGTMNestLevel(void)
{
	return ++GTMOptUpdateCount;
}

/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, optionally followed by
 * a unit name if "flags" indicates a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 *	HINT message, or NULL if no hint provided.
 */
bool
parse_int(const char *value, int *result, int flags, const char **hintmsg)
{
	int64		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	/* We assume here that int64 is at least as wide as long */
	errno = 0;
	val = strtol(value, &endptr, 0);

	if (endptr == value)
		return false;			/* no HINT for integer syntax error */

	if (errno == ERANGE || val != (int64) ((int32) val))
	{
		if (hintmsg)
			*hintmsg = gettext_noop("Value exceeds integer range.");
		return false;
	}

	/* allow whitespace between integer and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		/*
		 * Note: the multiple-switch coding technique here is a bit tedious,
		 * but seems necessary to avoid intermediate-value overflows.
		 */
		if (flags & GTMOPT_UNIT_MEMORY)
		{
			/* Set hint for use if no match or trailing garbage */
			if (hintmsg)
				*hintmsg = gettext_noop("Valid units for this parameter are \"kB\", \"MB\", and \"GB\".");

#if BLCKSZ < 1024 || BLCKSZ > (1024*1024)
#error BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_BLCKSZ < 1024 || XLOG_BLCKSZ > (1024*1024)
#error XLOG_BLCKSZ must be between 1KB and 1MB
#endif

			if (strncmp(endptr, "kB", 2) == 0)
			{
				endptr += 2;
				switch (flags & GTMOPT_UNIT_MEMORY)
				{
					case GTMOPT_UNIT_BLOCKS:
						val /= (BLCKSZ / 1024);
						break;
					case GTMOPT_UNIT_XBLOCKS:
						val /= (XLOG_BLCKSZ / 1024);
						break;
				}
			}
			else if (strncmp(endptr, "MB", 2) == 0)
			{
				endptr += 2;
				switch (flags & GTMOPT_UNIT_MEMORY)
				{
					case GTMOPT_UNIT_KB:
						val *= KB_PER_MB;
						break;
					case GTMOPT_UNIT_BLOCKS:
						val *= KB_PER_MB / (BLCKSZ / 1024);
						break;
					case GTMOPT_UNIT_XBLOCKS:
						val *= KB_PER_MB / (XLOG_BLCKSZ / 1024);
						break;
				}
			}
			else if (strncmp(endptr, "GB", 2) == 0)
			{
				endptr += 2;
				switch (flags & GTMOPT_UNIT_MEMORY)
				{
					case GTMOPT_UNIT_KB:
						val *= KB_PER_GB;
						break;
					case GTMOPT_UNIT_BLOCKS:
						val *= KB_PER_GB / (BLCKSZ / 1024);
						break;
					case GTMOPT_UNIT_XBLOCKS:
						val *= KB_PER_GB / (XLOG_BLCKSZ / 1024);
						break;
				}
			}
		}
		else if (flags & GTMOPT_UNIT_TIME)
		{
			/* Set hint for use if no match or trailing garbage */
			if (hintmsg)
				*hintmsg = gettext_noop("Valid units for this parameter are \"ms\", \"s\", \"min\", \"h\", and \"d\".");

			if (strncmp(endptr, "ms", 2) == 0)
			{
				endptr += 2;
				switch (flags & GTMOPT_UNIT_TIME)
				{
					case GTMOPT_UNIT_S:
						val /= MS_PER_S;
						break;
					case GTMOPT_UNIT_MIN:
						val /= MS_PER_MIN;
						break;
				}
			}
			else if (strncmp(endptr, "s", 1) == 0)
			{
				endptr += 1;
				switch (flags & GTMOPT_UNIT_TIME)
				{
					case GTMOPT_UNIT_MS:
						val *= MS_PER_S;
						break;
					case GTMOPT_UNIT_MIN:
						val /= S_PER_MIN;
						break;
				}
			}
			else if (strncmp(endptr, "min", 3) == 0)
			{
				endptr += 3;
				switch (flags & GTMOPT_UNIT_TIME)
				{
					case GTMOPT_UNIT_MS:
						val *= MS_PER_MIN;
						break;
					case GTMOPT_UNIT_S:
						val *= S_PER_MIN;
						break;
				}
			}
			else if (strncmp(endptr, "h", 1) == 0)
			{
				endptr += 1;
				switch (flags & GTMOPT_UNIT_TIME)
				{
					case GTMOPT_UNIT_MS:
						val *= MS_PER_H;
						break;
					case GTMOPT_UNIT_S:
						val *= S_PER_H;
						break;
					case GTMOPT_UNIT_MIN:
						val *= MIN_PER_H;
						break;
				}
			}
			else if (strncmp(endptr, "d", 1) == 0)
			{
				endptr += 1;
				switch (flags & GTMOPT_UNIT_TIME)
				{
					case GTMOPT_UNIT_MS:
						val *= MS_PER_D;
						break;
					case GTMOPT_UNIT_S:
						val *= S_PER_D;
						break;
					case GTMOPT_UNIT_MIN:
						val *= MIN_PER_D;
						break;
				}
			}
		}

		/* allow whitespace after unit */
		while (isspace((unsigned char) *endptr))
			endptr++;

		if (*endptr != '\0')
			return false;		/* appropriate hint, if any, already set */

		/* Check for overflow due to units conversion */
		if (val != (int64) ((int32) val))
		{
			if (hintmsg)
				*hintmsg = gettext_noop("Value exceeds integer range.");
			return false;
		}
	}

	if (result)
		*result = (int) val;
	return true;
}



/*
 * Try to parse value as a floating point number in the usual format.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
bool
parse_real(const char *value, double *result)
{
	double		val;
	char	   *endptr;

	if (result)
		*result = 0;			/* suppress compiler warning */

	errno = 0;
	val = strtod(value, &endptr);
	if (endptr == value || errno == ERANGE)
		return false;

	/* allow whitespace after number */
	while (isspace((unsigned char) *endptr))
		endptr++;
	if (*endptr != '\0')
		return false;

	if (result)
		*result = val;
	return true;
}



/*
 * Lookup the value for an enum option with the selected name
 * (case-insensitive).
 * If the enum option is found, sets the retval value and returns
 * true. If it's not found, return FALSE and retval is set to 0.
 */
bool
config_enum_lookup_by_name(struct config_enum * record, const char *value,
						   int *retval)
{
	const struct config_enum_entry *entry;

	for (entry = record->options; entry && entry->name; entry++)
	{
		if (pg_strcasecmp(value, entry->name) == 0)
		{
			*retval = entry->val;
			return TRUE;
		}
	}

	*retval = 0;
	return FALSE;
}



/*
 * Return a list of all available options for an enum, excluding
 * hidden ones, separated by the given separator.
 * If prefix is non-NULL, it is added before the first enum value.
 * If suffix is non-NULL, it is added to the end of the string.
 */
static char *
config_enum_get_options(struct config_enum * record, const char *prefix,
						const char *suffix, const char *separator)
{
	const struct config_enum_entry *entry;
	StringInfoData retstr;
	int			seplen;

	initStringInfo(&retstr);
	appendStringInfoString(&retstr, prefix);

	seplen = strlen(separator);
	for (entry = record->options; entry && entry->name; entry++)
	{
		if (!entry->hidden)
		{
			appendStringInfoString(&retstr, entry->name);
			appendBinaryStringInfo(&retstr, separator, seplen);
		}
	}

	/*
	 * All the entries may have been hidden, leaving the string empty if no
	 * prefix was given. This indicates a broken GTM setup, since there is no
	 * use for an enum without any values, so we just check to make sure we
	 * don't write to invalid memory instead of actually trying to do
	 * something smart with it.
	 */
	if (retstr.len >= seplen)
	{
		/* Replace final separator */
		retstr.data[retstr.len - seplen] = '\0';
		retstr.len -= seplen;
	}

	appendStringInfoString(&retstr, suffix);

	return retstr.data;
}


/*
 * Sets option `name' to given value. The value should be a string
 * which is going to be parsed and converted to the appropriate data
 * type.  The context and source parameters indicate in which context this
 * function is being called so it can apply the access restrictions
 * properly.
 *
 * If value is NULL, set the option to its default value (normally the
 * reset_val, but if source == GTMC_S_DEFAULT we instead use the boot_val).
 *
 * action indicates whether to set the value globally in the session, locally
 * to the current top transaction, or just for the duration of a function call.
 *
 * If changeVal is false then don't really set the option but do all
 * the checks to see if it would work.
 *
 * If there is an error (non-existing option, invalid value) then an
 * ereport(ERROR) is thrown *unless* this is called in a context where we
 * don't want to ereport (currently, startup or SIGHUP config file reread).
 * In that case we write a suitable error message via ereport(LOG) and
 * return false. This is working around the deficiencies in the ereport
 * mechanism, so don't blame me.  In all other cases, the function
 * returns true, including cases where the input is valid but we chose
 * not to apply it because of context or source-priority considerations.
 *
 * See also SetConfigOption for an external interface.
 */
bool
set_config_option(const char *name, const char *value,
				  GtmOptContext context, GtmOptSource source,
				  bool changeVal)
{
	struct config_generic *record;
	int			elevel;
	bool		prohibitValueChange = false;
	bool		makeDefault;

	if (context == GTMC_SIGHUP || source == GTMC_S_DEFAULT)
	{
		/*
		 * To avoid cluttering the log, only the postmaster bleats loudly
		 * about problems with the config file.
		 */
		elevel = DEBUG3;
	}
	else if (source == GTMC_S_DATABASE || source == GTMC_S_USER ||
			 source == GTMC_S_DATABASE_USER)
		elevel = WARNING;
	else
		elevel = ERROR;

	record = find_option(name, true, elevel);
	if (record == NULL)
	{
		if (isStartUp)
		{
			write_stderr("unrecognized configuration parameter \"%s\"\n", name);
		}
		else
		{
			ereport(elevel,
					(0,
					 errmsg("unrecognized configuration parameter \"%s\"", name)));
		}
		return false;
	}

	/*
	 * If source is postgresql.conf, mark the found record with
	 * GTMOPT_IS_IN_FILE. This is for the convenience of ProcessConfigFile.  Note
	 * that we do it even if changeVal is false, since ProcessConfigFile wants
	 * the marking to occur during its testing pass.
	 */
	if (source == GTMC_S_FILE)
		record->status |= GTMOPT_IS_IN_FILE;

	/*
	 * Check if the option can be set at this time. See guc.h for the precise
	 * rules.
	 */
	switch (record->context)
	{
		case GTMC_DEFAULT:
		case GTMC_STARTUP:
			if (context == GTMC_SIGHUP)
			{
				/*
				 * We are re-reading a GTMC_POSTMASTER variable from
				 * postgresql.conf.  We can't change the setting, so we should
				 * give a warning if the DBA tries to change it.  However,
				 * because of variant formats, canonicalization by check
				 * hooks, etc, we can't just compare the given string directly
				 * to what's stored.  Set a flag to check below after we have
				 * the final storable value.
				 *
				 * During the "checking" pass we just do nothing, to avoid
				 * printing the warning twice.
				 */
				if (!changeVal)
					return true;

				prohibitValueChange = true;
			}
			else if (context != GTMC_STARTUP)
			{
				if (isStartUp)
				{
					write_stderr("parameter \"%s\" cannot be changed without restarting the server\n",
								 name);
				}
				else
				{
					ereport(elevel,
							(0,
							 errmsg("parameter \"%s\" cannot be changed without restarting the server",
									name)));
				}
				return false;
			}
			break;
		case GTMC_SIGHUP:
			if (context != GTMC_SIGHUP && context != GTMC_STARTUP)
			{
				if (isStartUp)
				{
					write_stderr("parameter \"%s\" cannot be changed now\n",
								 name);
				}
				else
				{
					ereport(elevel,
							(0,
							 errmsg("parameter \"%s\" cannot be changed now",
									name)));
				}
				return false;
			}

			/*
			 * Hmm, the idea of the SIGHUP context is "ought to be global, but
			 * can be changed after postmaster start". But there's nothing
			 * that prevents a crafty administrator from sending SIGHUP
			 * signals to individual backends only.
			 */
			break;
		default:
			if (isStartUp)
			{
				write_stderr("GtmOptContext invalid (%d)\n",
							 context);
			}
			else
			{
				ereport(elevel,
						(0,
						 errmsg("GtmOptContext invalid (%d)",
								context)));
			}
			return false;
	}

	/*
	 * Should we set reset/stacked values?	(If so, the behavior is not
	 * transactional.)	This is done either when we get a default value from
	 * the database's/user's/client's default settings or when we reset a
	 * value to its default.
	 */
	makeDefault = changeVal && (source <= GTMC_S_OVERRIDE) &&
		((value != NULL) || source == GTMC_S_DEFAULT);

	/*
	 * Ignore attempted set if overridden by previously processed setting.
	 * However, if changeVal is false then plow ahead anyway since we are
	 * trying to find out if the value is potentially good, not actually use
	 * it. Also keep going if makeDefault is true, since we may want to set
	 * the reset/stacked values even if we can't set the variable itself.
	 */
	if (record->source > source)
	{
		if (changeVal && !makeDefault)
		{
			if (isStartUp)
			{
				write_stderr("\"%s\": setting ignored because previous source is higher priority\n",
							 name);
			}
			else
			{
				elog(DEBUG3, "\"%s\": setting ignored because previous source is higher priority",
					 name);
			}
			return true;
		}
		changeVal = false;
	}

	/*
	 * Evaluate value and set variable.
	 */
	switch (record->vartype)
	{
		case GTMC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) record;
				bool		newval;
				void	   *newextra = NULL;

				if (value)
				{
					if (!gtm_opt_parse_bool(value, &newval))
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" requires a Boolean value\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" requires a Boolean value",
											name)));
						}
						return false;
					}
				}
				else if (source == GTMC_S_DEFAULT)
				{
					newval = conf->boot_val;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
				}

				if (prohibitValueChange)
				{
					if (*conf->variable != newval)
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" cannot be changed without restarting the server\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" cannot be changed without restarting the server",
											name)));
						}
					}
					return false;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen);

					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
				}
				if (makeDefault)
				{
					GtmOptStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.boolval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;
			}

		case GTMC_INT:
			{
				struct config_int *conf = (struct config_int *) record;
				int			newval;
				void	   *newextra = NULL;

				if (value)
				{
					const char *hintmsg;

					if (!parse_int(value, &newval, conf->gen.flags, &hintmsg))
					{
						if (isStartUp)
						{
							write_stderr("invalid value for parameter \"%s\": \"%s\"\n",
										 name, value);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("invalid value for parameter \"%s\": \"%s\"",
											name, value),
									 hintmsg ? errhint("%s", _(hintmsg)) : 0));
						}
						return false;
					}
					if (newval < conf->min || newval > conf->max)
					{
						if (isStartUp)
						{
							write_stderr("%d is outside the valid range for parameter \"%s\" (%d .. %d)\n",
										 newval, name, conf->min, conf->max);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)",
											newval, name, conf->min, conf->max)));
						}
						return false;
					}
				}
				else if (source == GTMC_S_DEFAULT)
				{
					newval = conf->boot_val;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
				}

				if (prohibitValueChange)
				{
					if (*conf->variable != newval)
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" cannot be changed without restarting the server\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" cannot be changed without restarting the server",
											name)));
						}
					}
					return false;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen);

					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
				}
				if (makeDefault)
				{
					GtmOptStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.intval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;
			}

		case GTMC_REAL:
			{
				struct config_real *conf = (struct config_real *) record;
				double		newval;
				void	   *newextra = NULL;

				if (value)
				{
					if (!parse_real(value, &newval))
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" requires a numeric value\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" requires a numeric value",
											name)));
						}
						return false;
					}
					if (newval < conf->min || newval > conf->max)
					{
						if (isStartUp)
						{
							write_stderr("%g is outside the valid range for parameter \"%s\" (%g .. %g)\n",
										 newval, name, conf->min, conf->max);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("%g is outside the valid range for parameter \"%s\" (%g .. %g)",
											newval, name, conf->min, conf->max)));
						}
						return false;
					}
				}
				else if (source == GTMC_S_DEFAULT)
				{
					newval = conf->boot_val;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
				}

				if (prohibitValueChange)
				{
					if (*conf->variable != newval)
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" cannot be changed without restarting the server\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" cannot be changed without restarting the server",
											name)));
						}
					}
					return false;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen);

					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
				}
				if (makeDefault)
				{
					GtmOptStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.realval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;
			}

		case GTMC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;
				char	   *newval;
				void	   *newextra = NULL;

				if (value)
				{
					/*
					 * The value passed by the caller could be transient, so
					 * we always strdup it.
					 */
					newval = gtm_opt_strdup(elevel, value);
					if (newval == NULL)
						return false;
				}
				else if (source == GTMC_S_DEFAULT)
				{
					/* non-NULL boot_val must always get strdup'd */
					if (conf->boot_val != NULL)
					{
						newval = gtm_opt_strdup(elevel, conf->boot_val);
						if (newval == NULL)
							return false;
					}
					else
						newval = NULL;

				}
				else
				{
					/*
					 * strdup not needed, since reset_val is already under
					 * guc.c's control
					 */
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
				}

				if (prohibitValueChange)
				{
					/* newval shouldn't be NULL, so we're a bit sloppy here */
					if (*conf->variable == NULL || newval == NULL ||
						strcmp(*conf->variable, newval) != 0)
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" cannot be changed without restarting the server\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" cannot be changed without restarting the server",
											name)));
						}
					}
					return false;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen);

					set_string_field(conf, conf->variable, newval);
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
				}

				if (makeDefault)
				{
					GtmOptStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						set_string_field(conf, &conf->reset_val, newval);
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							set_string_field(conf, &stack->prior.val.stringval,
											 newval);
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
						}
					}
				}

				/* Perhaps we didn't install newval anywhere */
				if (newval && !string_field_used(conf, newval))
					free(newval);
				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;
			}

		case GTMC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) record;
				int			newval;
				void	   *newextra = NULL;

				if (value)
				{
					if (!config_enum_lookup_by_name(conf, value, &newval))
					{
						char	   *hintmsg;

						hintmsg = config_enum_get_options(conf,
														"Available values: ",
														  ".", ", ");

						if (isStartUp)
						{
							write_stderr("invalid value for parameter \"%s\": \"%s\". %s\n",
										 name, value, hintmsg);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("invalid value for parameter \"%s\": \"%s\"",
											name, value),
									 hintmsg ? errhint("%s", _(hintmsg)) : 0));
						}

						if (hintmsg)
							free(hintmsg);
						return false;
					}
				}
				else if (source == GTMC_S_DEFAULT)
				{
					newval = conf->boot_val;
				}
				else
				{
					newval = conf->reset_val;
					newextra = conf->reset_extra;
					source = conf->gen.reset_source;
				}

				if (prohibitValueChange)
				{
					if (*conf->variable != newval)
					{
						if (isStartUp)
						{
							write_stderr("parameter \"%s\" cannot be changed without restarting the server\n",
										 name);
						}
						else
						{
							ereport(elevel,
									(0,
									 errmsg("parameter \"%s\" cannot be changed without restarting the server",
											name)));
						}
					}
					return false;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(&conf->gen);

					*conf->variable = newval;
					set_extra_field(&conf->gen, &conf->gen.extra,
									newextra);
					conf->gen.source = source;
				}
				if (makeDefault)
				{
					GtmOptStack   *stack;

					if (conf->gen.reset_source <= source)
					{
						conf->reset_val = newval;
						set_extra_field(&conf->gen, &conf->reset_extra,
										newextra);
						conf->gen.reset_source = source;
					}
					for (stack = conf->gen.stack; stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.enumval = newval;
							set_extra_field(&conf->gen, &stack->prior.extra,
											newextra);
							stack->source = source;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(&conf->gen, newextra))
					free(newextra);
				break;
			}
	}

	if (changeVal && (record->flags & GTMOPT_REPORT))
		ReportGTMOption(record);

	return true;
}




/*
 * Set the fields for source file and line number the setting came from.
 */
static void
set_config_sourcefile(const char *name, char *sourcefile, int sourceline)
{
	struct config_generic *record;
	int			elevel;

	/*
	 * To avoid cluttering the log, only the postmaster bleats loudly about
	 * problems with the config file.
	 */
	elevel = DEBUG3;

	record = find_option(name, true, elevel);
	/* should not happen */
	if (record == NULL)
	{
		if (isStartUp)
			write_stderr("unrecognized configuration parameter \"%s\"\n", name);
		else
			elog(ERROR, "unrecognized configuration parameter \"%s\"", name);
	}

	sourcefile = gtm_opt_strdup(elevel, sourcefile);
	if (record->sourcefile)
		free(record->sourcefile);
	record->sourcefile = sourcefile;
	record->sourceline = sourceline;
}


/*
 * Set a config option to the given value. See also set_config_option,
 * this is just the wrapper to be called from outside GTM.	NB: this
 * is used only for non-transactional operations.
 *
 * Note: there is no support here for setting source file/line, as it
 * is currently not needed.
 */
void
SetConfigOption(const char *name, const char *value,
				GtmOptContext context, GtmOptSource source)
{
	(void) set_config_option(name, value, context, source,
							 true);
}




/*
 * Fetch the current value of the option `name'. If the option doesn't exist,
 * throw an ereport and don't return.
 *
 * If restrict_superuser is true, we also enforce that only superusers can
 * see GTMOPT_SUPERUSER_ONLY variables.  This should only be passed as true
 * in user-driven calls.
 *
 * The string is *not* allocated for modification and is really only
 * valid until the next call to configuration related functions.
 */
const char *
GetConfigOption(const char *name, bool restrict_superuser)
{
	struct config_generic *record;
	static char buffer[256];

	record = find_option(name, false, ERROR);
	if (record == NULL)
	{
		if (isStartUp)
			write_stderr("unrecognized configuration parameter \"%s\"\n", name);
		else
			ereport(ERROR,
					(0,
					 errmsg("unrecognized configuration parameter \"%s\"", name)));
	}
	switch (record->vartype)
	{
		case GTMC_BOOL:
			return *((struct config_bool *) record)->variable ? "on" : "off";

		case GTMC_INT:
			snprintf(buffer, sizeof(buffer), "%d",
					 *((struct config_int *) record)->variable);
			return buffer;

		case GTMC_REAL:
			snprintf(buffer, sizeof(buffer), "%g",
					 *((struct config_real *) record)->variable);
			return buffer;

		case GTMC_STRING:
			return *((struct config_string *) record)->variable;

		case GTMC_ENUM:
			return config_enum_lookup_by_value((struct config_enum *) record,
								 *((struct config_enum *) record)->variable);
	}
	return NULL;
}


/*
 * Get the RESET value associated with the given option.
 *
 * Note: this is not re-entrant, due to use of static result buffer;
 * not to mention that a string variable could have its reset_val changed.
 * Beware of assuming the result value is good for very long.
 */
const char *
GetConfigOptionResetString(const char *name)
{
	struct config_generic *record;
	static char buffer[256];

	record = find_option(name, false, ERROR);
	if (record == NULL)
	{
		if (isStartUp)
			write_stderr("unrecognized configuration parameter \"%s\"\n", name);
		else
			ereport(ERROR,
					(0,
					 errmsg("unrecognized configuration parameter \"%s\"", name)));
	}

	switch (record->vartype)
	{
		case GTMC_BOOL:
			return ((struct config_bool *) record)->reset_val ? "on" : "off";

		case GTMC_INT:
			snprintf(buffer, sizeof(buffer), "%d",
					 ((struct config_int *) record)->reset_val);
			return buffer;

		case GTMC_REAL:
			snprintf(buffer, sizeof(buffer), "%g",
					 ((struct config_real *) record)->reset_val);
			return buffer;

		case GTMC_STRING:
			return ((struct config_string *) record)->reset_val;

		case GTMC_ENUM:
			return config_enum_lookup_by_value((struct config_enum *) record,
								 ((struct config_enum *) record)->reset_val);
	}
	return NULL;
}


void
EmitWarningsOnPlaceholders(const char *className)
{
	int			classLen = strlen(className);
	int			i;

	for (i = 0; i < num_gtm_opt_variables; i++)
	{
		struct config_generic *var = gtm_opt_variables[i];

		if ((var->flags & GTMOPT_CUSTOM_PLACEHOLDER) != 0 &&
			strncmp(className, var->name, classLen) == 0 &&
			var->name[classLen] == GTMOPT_QUALIFIER_SEPARATOR)
		{
			if (isStartUp)
				write_stderr("unrecognized configuration parameter \"%s\"\n",
							 var->name);
			else
				ereport(WARNING,
						(0,
						 errmsg("unrecognized configuration parameter \"%s\"",
								var->name)));
		}
	}
}


/*
 * Return GTM variable value by name; optionally return canonical
 * form of name.  Return value is malloc'd.
 */
char *
GetConfigOptionByName(const char *name, const char **varname)
{
	struct config_generic *record;

	record = find_option(name, false, ERROR);
	if (record == NULL)
	{
		if (isStartUp)
			write_stderr("unrecognized configuration parameter \"%s\"\n", name);
		else
			ereport(ERROR,
					(0,
					 errmsg("unrecognized configuration parameter \"%s\"", name)));
	}
	if (varname)
		*varname = record->name;

	return _ShowOption(record, true);
}

/*
 * Return GTM variable value by variable number; optionally return canonical
 * form of name.  Return value is malloc'd.
 */
void
GetConfigOptionByNum(int varnum, const char **values, bool *noshow)
{
	char		buffer[256];
	struct config_generic *conf;

	/* check requested variable number valid */
	Assert((varnum >= 0) && (varnum < num_gtm_opt_variables));

	conf = gtm_opt_variables[varnum];

	if (noshow)
	{
		if (conf->flags & GTMOPT_NO_SHOW_ALL)
			*noshow = true;
		else
			*noshow = false;
	}

	/* first get the generic attributes */

	/* name */
	values[0] = conf->name;

	/* setting : use _ShowOption in order to avoid duplicating the logic */
	values[1] = _ShowOption(conf, false);

	/* unit */
	if (conf->vartype == GTMC_INT)
	{
		static char buf[8];

		switch (conf->flags & (GTMOPT_UNIT_MEMORY | GTMOPT_UNIT_TIME))
		{
			case GTMOPT_UNIT_KB:
				values[2] = "kB";
				break;
			case GTMOPT_UNIT_BLOCKS:
				snprintf(buf, sizeof(buf), "%dkB", BLCKSZ / 1024);
				values[2] = buf;
				break;
			case GTMOPT_UNIT_XBLOCKS:
				snprintf(buf, sizeof(buf), "%dkB", XLOG_BLCKSZ / 1024);
				values[2] = buf;
				break;
			case GTMOPT_UNIT_MS:
				values[2] = "ms";
				break;
			case GTMOPT_UNIT_S:
				values[2] = "s";
				break;
			case GTMOPT_UNIT_MIN:
				values[2] = "min";
				break;
			default:
				values[2] = "";
				break;
		}
	}
	else
		values[2] = NULL;

#if 0
	/* PGXCTODO: Group parameters are not used yet */
	/* group */
	values[3] = config_group_names[conf->group];
#endif

	/* short_desc */
	values[4] = conf->short_desc;

	/* extra_desc */
	values[5] = conf->long_desc;

	/* context */
	values[6] = GtmOptContext_Names[conf->context];

	/* vartype */
	values[7] = config_type_names[conf->vartype];

	/* source */
	values[8] = GtmOptSource_Names[conf->source];

	/* now get the type specifc attributes */
	switch (conf->vartype)
	{
		case GTMC_BOOL:
			{
				struct config_bool *lconf = (struct config_bool *) conf;

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				values[12] = strdup(lconf->boot_val ? "on" : "off");

				/* reset_val */
				values[13] = strdup(lconf->reset_val ? "on" : "off");
			}
			break;

		case GTMC_INT:
			{
				struct config_int *lconf = (struct config_int *) conf;

				/* min_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->min);
				values[9] = strdup(buffer);

				/* max_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->max);
				values[10] = strdup(buffer);

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->boot_val);
				values[12] = strdup(buffer);

				/* reset_val */
				snprintf(buffer, sizeof(buffer), "%d", lconf->reset_val);
				values[13] = strdup(buffer);
			}
			break;

		case GTMC_REAL:
			{
				struct config_real *lconf = (struct config_real *) conf;

				/* min_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->min);
				values[9] = strdup(buffer);

				/* max_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->max);
				values[10] = strdup(buffer);

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->boot_val);
				values[12] = strdup(buffer);

				/* reset_val */
				snprintf(buffer, sizeof(buffer), "%g", lconf->reset_val);
				values[13] = strdup(buffer);
			}
			break;

		case GTMC_STRING:
			{
				struct config_string *lconf = (struct config_string *) conf;

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				if (lconf->boot_val == NULL)
					values[12] = NULL;
				else
					values[12] = strdup(lconf->boot_val);

				/* reset_val */
				if (lconf->reset_val == NULL)
					values[13] = NULL;
				else
					values[13] = strdup(lconf->reset_val);
			}
			break;

		case GTMC_ENUM:
			{
				struct config_enum *lconf = (struct config_enum *) conf;

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */

				/*
				 * NOTE! enumvals with double quotes in them are not
				 * supported!
				 */
				values[11] = config_enum_get_options((struct config_enum *) conf,
													 "{\"", "\"}", "\",\"");

				/* boot_val */
				values[12] = strdup(config_enum_lookup_by_value(lconf,
														   lconf->boot_val));

				/* reset_val */
				values[13] = strdup(config_enum_lookup_by_value(lconf,
														  lconf->reset_val));
			}
			break;

		default:
			{
				/*
				 * should never get here, but in case we do, set 'em to NULL
				 */

				/* min_val */
				values[9] = NULL;

				/* max_val */
				values[10] = NULL;

				/* enumvals */
				values[11] = NULL;

				/* boot_val */
				values[12] = NULL;

				/* reset_val */
				values[13] = NULL;
			}
			break;
	}

	/*
	 * If the setting came from a config file, set the source location. For
	 * security reasons, we don't show source file/line number for
	 * non-superusers.
	 */
	if (conf->source == GTMC_S_FILE)
	{
		values[14] = conf->sourcefile;
		snprintf(buffer, sizeof(buffer), "%d", conf->sourceline);
		values[15] = strdup(buffer);
	}
	else
	{
		values[14] = NULL;
		values[15] = NULL;
	}
}

/*
 * Return the total number of GTM variables
 */
int
GetNumConfigOptions(void)
{
	return num_gtm_opt_variables;
}


static char *
_ShowOption(struct config_generic * record, bool use_units)
{
	char		buffer[256];
	const char *val;

	switch (record->vartype)
	{
		case GTMC_BOOL:
			{
				struct config_bool *conf = (struct config_bool *) record;

				val = *conf->variable ? "on" : "off";
			}
			break;

		case GTMC_INT:
			{
				struct config_int *conf = (struct config_int *) record;

				/*
				 * Use int64 arithmetic to avoid overflows in units
				 * conversion.
				 */
				int64		result = *conf->variable;
				const char *unit;

				if (use_units && result > 0 &&
					(record->flags & GTMOPT_UNIT_MEMORY))
				{
					switch (record->flags & GTMOPT_UNIT_MEMORY)
					{
						case GTMOPT_UNIT_BLOCKS:
							result *= BLCKSZ / 1024;
							break;
						case GTMOPT_UNIT_XBLOCKS:
							result *= XLOG_BLCKSZ / 1024;
							break;
					}

					if (result % KB_PER_GB == 0)
					{
						result /= KB_PER_GB;
						unit = "GB";
					}
					else if (result % KB_PER_MB == 0)
					{
						result /= KB_PER_MB;
						unit = "MB";
					}
					else
					{
						unit = "kB";
					}
				}
				else if (use_units && result > 0 &&
						 (record->flags & GTMOPT_UNIT_TIME))
				{
					switch (record->flags & GTMOPT_UNIT_TIME)
					{
						case GTMOPT_UNIT_S:
							result *= MS_PER_S;
							break;
						case GTMOPT_UNIT_MIN:
							result *= MS_PER_MIN;
							break;
					}

					if (result % MS_PER_D == 0)
					{
						result /= MS_PER_D;
						unit = "d";
					}
					else if (result % MS_PER_H == 0)
					{
						result /= MS_PER_H;
						unit = "h";
					}
					else if (result % MS_PER_MIN == 0)
					{
						result /= MS_PER_MIN;
						unit = "min";
					}
					else if (result % MS_PER_S == 0)
					{
						result /= MS_PER_S;
						unit = "s";
					}
					else
					{
						unit = "ms";
					}
				}
				else
					unit = "";

				snprintf(buffer, sizeof(buffer), INT64_FORMAT "%s",
						 result, unit);
				val = buffer;

			}
			break;

		case GTMC_REAL:
			{
				struct config_real *conf = (struct config_real *) record;

				snprintf(buffer, sizeof(buffer), "%g",
						 *conf->variable);
				val = buffer;
			}
			break;

		case GTMC_STRING:
			{
				struct config_string *conf = (struct config_string *) record;

				 if (*conf->variable && **conf->variable)
					val = *conf->variable;
				else
					val = "";
			}
			break;

		case GTMC_ENUM:
			{
				struct config_enum *conf = (struct config_enum *) record;

				val = config_enum_lookup_by_value(conf, *conf->variable);
			}
			break;

		default:
			/* just to keep compiler quiet */
			val = "???";
			break;
	}

	return strdup(val);
}



/*
 * A little "long argument" simulation, although not quite GNU
 * compliant. Takes a string of the form "some-option=some value" and
 * returns name = "some_option" and value = "some value" in malloc'ed
 * storage. Note that '-' is converted to '_' in the option name. If
 * there is no '=' in the input string then value will be NULL.
 */
void
ParseLongOption(const char *string, char **name, char **value)
{
	size_t		equal_pos;
	char	   *cp;

	AssertArg(string);
	AssertArg(name);
	AssertArg(value);

	equal_pos = strcspn(string, "=");

	if (string[equal_pos] == '=')
	{
		*name = gtm_opt_malloc(FATAL, equal_pos + 1);
		strlcpy(*name, string, equal_pos + 1);

		*value = gtm_opt_strdup(FATAL, &string[equal_pos + 1]);
	}
	else
	{
		/* no equal sign in string */
		*name = gtm_opt_strdup(FATAL, string);
		*value = NULL;
	}

	for (cp = *name; *cp; cp++)
		if (*cp == '-')
			*cp = '_';
}

#if 0
/*
 * keep-alive related APIs will be used in future extensions
 */
void
gtm_assign_tcp_keepalives_idle(int newval, void *extra)
{
	/*
	 * The kernel API provides no way to test a value without setting it; and
	 * once we set it we might fail to unset it.  So there seems little point
	 * in fully implementing the check-then-assign GTM API for these
	 * variables.  Instead we just do the assignment on demand.  pqcomm.c
	 * reports any problems via elog(LOG).
	 *
	 * This approach means that the GTM value might have little to do with the
	 * actual kernel value, so we use a show_hook that retrieves the kernel
	 * value rather than trusting GTM's copy.
	 */
#if 0
	(void) pq_setkeepalivesidle(newval, MyProcPort);
#else
	(void) pq_setkeepalivesidle_all(newval);
#endif
}

const char *
gtm_show_tcp_keepalives_idle(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

#if 0
	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesidle(MyProcPort));
#else
	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesidle_all());
#endif
	return nbuf;
}

void
gtm_assign_tcp_keepalives_interval(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
#if 0
	(void) pq_setkeepalivesinterval(newval, MyProcPort);
#else
	(void) pq_setkeepalivesinterval_all(newval);
#endif
}

const char *
gtm_show_tcp_keepalives_interval(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

#if 0
	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesinterval(MyProcPort));
#else
	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesinterval_all());
#endif
	return nbuf;
}

void
gtm_assign_tcp_keepalives_count(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
#if 0
	(void) pq_setkeepalivescount(newval, MyProcPort);
#else
	(void) pq_setkeepalivescount_all(newval);
#endif
}

const char *
gtm_show_tcp_keepalives_count(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

#if 0
	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivescount(MyProcPort));
#else
	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivescount_all());
#endif
	return nbuf;
}
#endif

/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
static bool
gtm_opt_parse_bool(const char *value, bool *result)
{
	return gtm_opt_parse_bool_with_len(value, strlen(value), result);
}

static bool
gtm_opt_parse_bool_with_len(const char *value, size_t len, bool *result)
{
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = false;		/* suppress compiler warning */
	return false;
}

/*
 * ReportGUCOption: if appropriate, transmit option value to frontend
 */
static void
ReportGTMOption(struct config_generic * record)
{
	/* So far, it is empty. */
}

/*
 * Lookup the name for an enum option with the selected value.
 * Should only ever be called with known-valid values, so throws
 * an elog(ERROR) if the enum option is not found.
 *
 * The returned string is a pointer to static data and not
 * allocated for modification.
 */
const char *
config_enum_lookup_by_value(struct config_enum * record, int val)
{
	const struct config_enum_entry *entry;

	for (entry = record->options; entry && entry->name; entry++)
	{
		if (entry->val == val)
			return entry->name;
	}

	if (isStartUp)
		write_stderr("could not find enum option %d for %s\n",
					 val, record->gen.name);
	else
		elog(ERROR, "could not find enum option %d for %s",
			 val, record->gen.name);
	return NULL;				/* silence compiler */
}
