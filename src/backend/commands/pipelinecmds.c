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
RegisterQuery(const char *name, const char *source)
{
	/*
	 * This is a hacky way to extract the original registered query,
	 * but it should always work since the grammar is of the form:
	 *
	 * REGISTER <name> AS ( ... )
	 *
	 * We don't just blindly include the ( ... ) because we want that part
	 * to be parsed for correctness as early as possible. The alternative
	 * would be to parse the ( ... ) on the backend, and send an error
	 * to the client if anything is wrong, but that seems overly complex
	 * considering this approach will always work.
	 *
	 * Also, deparsing is tricky because wildcards will have already been expanded,
	 * and thus the original query may not work as intended over time.
	 */

	char *query_to_register = strstr(source, QUERY_BEGINS_HERE);
	query_to_register += strlen(QUERY_BEGINS_HERE); /* Exclude this */

	AddQuery(name, query_to_register, PIPELINE_QUERY_STATE_INACTIVE);
}
