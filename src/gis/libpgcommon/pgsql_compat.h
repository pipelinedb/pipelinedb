#ifndef _PGSQL_COMPAT_H
#define _PGSQL_COMPAT_H 1

/* Make sure PG_NARGS is defined for older PostgreSQL versions */
#ifndef PG_NARGS
#define PG_NARGS() (fcinfo->nargs)
#endif

#endif /* _PGSQL_COMPAT_H */
