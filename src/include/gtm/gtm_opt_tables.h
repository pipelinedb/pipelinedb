/*-------------------------------------------------------------------------
 *
 * gtm_opt_tables.h
 *		Declarations of tables used by GTM configuration file.
 *
 * Portions Copyright (c) 2011, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 *
 *	  src/include/gtm/gtm_opt_tables.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_OPT_TABLES_H
#define GTM_OPT_TABLES_H

#include "gtm/gtm_opt.h"

/*
 * GUC supports these types of variables:
 */
enum config_type
{
	GTMC_BOOL,
	GTMC_INT,
	GTMC_REAL,
	GTMC_STRING,
	GTMC_ENUM
};

union config_var_val
{
	bool		boolval;
	int			intval;
	double		realval;
	char	   *stringval;
	int			enumval;
};

/*
 * The actual value of a GUC variable can include a malloc'd opaque struct
 * "extra", which is created by its check_hook and used by its assign_hook.
 */
typedef struct config_var_value
{
	union config_var_val val;
	void	   *extra;
} config_var_value;

/*
 * Stack entry for saving the state a variable had prior to an uncommitted
 * transactional change.  In GTM, only chance to update options is SIGINT.
 */
typedef enum
{
	/* This is almost GtmOptAction, but we need a fourth state for SET+LOCAL */
	GTMOPT_SAVE,				/* entry caused by function SET option */
	GTMOPT_SET,					/* entry caused by plain SET command */
	GTMOPT_LOCAL,				/* entry caused by SET LOCAL command */
	GTMOPT_SET_LOCAL			/* entry caused by SET then SET LOCAL */
} GtmOptStackState;

typedef struct guc_stack
{
	struct guc_stack *prev;		/* previous stack item, if any */
	int			nest_level;		/* nesting depth at which we made entry */
	GtmOptStackState state;		/* see enum above */
	GtmOptSource	source;			/* source of the prior value */
	config_var_value prior;		/* previous value of variable */
	config_var_value masked;	/* SET value in a GTMOPT_SET_LOCAL entry */
	/* masked value's source must be GTMC_S_SESSION, so no need to store it */
} GtmOptStack;

/*
 * Generic fields applicable to all types of variables
 *
 * The short description should be less than 80 chars in length. Some
 * applications may use the long description as well, and will append
 * it to the short description. (separated by a newline or '. ')
 *
 * Note that sourcefile/sourceline are kept here, and not pushed into stacked
 * values, although in principle they belong with some stacked value if the
 * active value is session- or transaction-local.  This is to avoid bloating
 * stack entries.  We know they are only relevant when source == GTMC_S_FILE.
 */
struct config_generic
{
	/* constant fields, must be set correctly in initial value: */
	const char 		*name;			/* name of variable - MUST BE FIRST */
	GtmOptContext	context;		/* context required to set the variable */
	const char 		*short_desc;	/* short desc. of this variable's purpose */
	const char		*long_desc;		/* long desc. of this variable's purpose */
	int				flags;			/* flag bits, see below */
	/* variable fields, initialized at runtime: */
	enum config_type vartype;		/* type of variable (set only at startup) */
	int				status;			/* status bits, see below */
	GtmOptSource	reset_source;	/* source of the reset_value */
	GtmOptSource	source;			/* source of the current actual value */
	GtmOptStack   	*stack;			/* stacked prior values */
	void	   		*extra;			/* "extra" pointer for current actual value */
	char	   		*sourcefile;	/* file current setting is from (NULL if not
									 * file) */
	int				sourceline;		/* line in source file */
};

/* bit values in flags field are defined in guc.h */

/* bit values in status field */
#define GTMOPT_IS_IN_FILE		0x0001		/* found it in config file */
/*
 * Caution: the GTMOPT_IS_IN_FILE bit is transient state for ProcessConfigFile.
 * Do not assume that its value represents useful information elsewhere.
 */


/* GUC records for specific variable types */

struct config_bool
{
	struct config_generic gen;
	/* constant fields, must be set correctly in initial value: */
	bool	   *variable;
	bool		boot_val;
	/* variable fields, initialized at runtime: */
	bool		reset_val;
	void	   *reset_extra;
};

struct config_int
{
	struct config_generic gen;
	/* constant fields, must be set correctly in initial value: */
	int		   *variable;
	int			boot_val;
	int			min;
	int			max;
	/* variable fields, initialized at runtime: */
	int			reset_val;
	void	   *reset_extra;
};

struct config_real
{
	struct config_generic gen;
	/* constant fields, must be set correctly in initial value: */
	double	   *variable;
	double		boot_val;
	double		min;
	double		max;
	/* variable fields, initialized at runtime: */
	double		reset_val;
	void	   *reset_extra;
};

struct config_string
{
	struct config_generic gen;
	/* constant fields, must be set correctly in initial value: */
	char	  **variable;
	const char *boot_val;
	/* variable fields, initialized at runtime: */
	char	   *reset_val;
	void	   *reset_extra;
};

struct config_enum
{
	struct config_generic gen;
	/* constant fields, must be set correctly in initial value: */
	int		   *variable;
	int			boot_val;
	const struct config_enum_entry *options;
	/* variable fields, initialized at runtime: */
	int			reset_val;
	void	   *reset_extra;
};

/* constant tables corresponding to enums above and in guc.h */
extern const char *const config_group_names[];
extern const char *const config_type_names[];
extern const char *const GtmOptContext_Names[];
extern const char *const GtmOptSource_Names[];

/* get the current set of variables */
extern struct config_generic **get_guc_variables(void);

extern void build_guc_variables(void);

/* search in enum options */
extern const char *config_enum_lookup_by_value(struct config_enum * record, int val);
extern bool config_enum_lookup_by_name(struct config_enum * record,
						   const char *value, int *retval);


#endif   /* GTM_OPT_TABLES_H */
