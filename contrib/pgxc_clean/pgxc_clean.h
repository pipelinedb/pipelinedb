#ifndef PGXC_CLEAN
#define PGXC_CLEAN

typedef struct database_names
{
	struct database_names *next;
	char *database_name;
} database_names;

extern FILE *outf;
extern FILE *errf;

#endif /* PGXC_CLEAN */
