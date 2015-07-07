/* declarations for getopt and envargs */

extern int pgis_opterr;
extern int pgis_optind;
extern int pgis_optopt;
extern char *pgis_optarg;

extern int pgis_getopt(int argc, char **argv, char *opts);
extern void envargs(int *pargc, char ***pargv, char *envstr);
