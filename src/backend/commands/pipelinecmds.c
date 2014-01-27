/*-------------------------------------------------------------------------
 *
 * pipelinecmds.c
 *	  commands specific to PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/commands/pipelinecmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "commands/pipelinecmds.h"
#include "utils/builtins.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "parser/analyze.h"
#include "regex/regex.h"

#define QUERY_BEGINS_HERE " AS "

/*
 * RegisterQuery
 *
 * Registers a continuous query. This is essentially a matter of putting
 * it in the pipeline_queries catalog table and marking it as inactive.
 */
extern void
RegisterQuery(RangeVar *name, const char *rawquery)
{
	/*
	 * This is a hacky way to extract the original registered query,
	 * but it should always work since the grammar is of the form:
	 *
	 * REGISTER <name> AS ( ... )
	 *
	 * We don't just blindly include the ( ... ) component as part of the
	 * RegisterStmt, because we want it to be parsed for correctness.
	 * However, we still want to store the original ( ... ) in the catalog,
	 * assuming it's correct.
	 *
	 * Deparsing isn't sufficient for recovering the original query, because
	 * wildcards will have already been expanded, and thus the original
	 * query may not work as intended over time.
	 */
	char *upperquery = pstrdup(rawquery);
	char *uppername = pstrdup(name->relname);
	int offset = 0;
	int i;
	char *query_to_register;
	for (i=0; upperquery[i] != '\0'; i++) upperquery[i] = toupper(upperquery[i]);
	for (i=0; uppername[i] != '\0'; i++) uppername[i] = toupper(uppername[i]);

	/* Move past the query name part of the query before we look for AS */
	query_to_register = strstr(upperquery, uppername) + strlen(uppername);
	query_to_register = strstr(query_to_register, QUERY_BEGINS_HERE) + strlen(QUERY_BEGINS_HERE);

	/* We want to take the substring from the original string */
	offset = query_to_register - upperquery;

	AddQuery(name->relname, rawquery + offset, PIPELINE_QUERY_STATE_INACTIVE);
}


/*
 * ActivateQuery
 *
 * Activates a REGISTERed continuous query. If the query with the given name
 * is already active, it will be re-activated with whatever the current
 * query payload is.
 */
extern void
ActivateQuery(RangeVar *name)
{
//	SetQueryState(name, PIPELINE_QUERY_STATE_ACTIVE);
}


/*
 * DeactivateQuery
 *
 * Deactivates a REGISTERed and ACTIVE continuous query. If the query isn't
 * ACTIVE, this is a noop.
>>>>>>> master
 */
extern void
DeactivateQuery(RangeVar *name)
{
//	SetQueryState(name, PIPELINE_QUERY_STATE_INACTIVE);
}
