/*
 * src/pl/plpython/plpy_procedure.h
 */

#ifndef PLPY_PROCEDURE_H
#define PLPY_PROCEDURE_H

#include "plpy_typeio.h"


extern void init_procedure_caches(void);


/* cached procedure data */
typedef struct PLyProcedure
{
	char	   *proname;		/* SQL name of procedure */
	char	   *pyname;			/* Python name of procedure */
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
	bool		fn_readonly;
	PLyTypeInfo result;			/* also used to store info for trigger tuple
								 * type */
	bool		is_setof;		/* true, if procedure returns result set */
	PyObject   *setof;			/* contents of result set. */
	char	   *src;			/* textual procedure code, after mangling */
	char	  **argnames;		/* Argument names */
	PLyTypeInfo args[FUNC_MAX_ARGS];
	int			nargs;
	PyObject   *code;			/* compiled procedure code */
	PyObject   *statics;		/* data saved across calls, local scope */
	PyObject   *globals;		/* data saved across calls, global scope */
} PLyProcedure;

/* the procedure cache entry */
typedef struct PLyProcedureEntry
{
	Oid			fn_oid;			/* hash key */
	PLyProcedure *proc;
} PLyProcedureEntry;

/* PLyProcedure manipulation */
extern char *PLy_procedure_name(PLyProcedure *proc);
extern PLyProcedure *PLy_procedure_get(Oid fn_oid, bool is_trigger);
extern void PLy_procedure_compile(PLyProcedure *proc, const char *src);
extern void PLy_procedure_delete(PLyProcedure *proc);

#endif   /* PLPY_PROCEDURE_H */
