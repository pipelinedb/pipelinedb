/*-------------------------------------------------------------------------
 *
 * cont_analyze.h
 *	  Interface for for parsing and analyzing continuous queries
 *
 * src/include/catalog/pipeline/cont_analyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQPARSE_H
#define CQPARSE_H

typedef struct ContAnalyzeContext
{
	ParseState *pstate;
	int colno;
	List *colnames;
	List *types;
	List *cols;
	List *targets;
	List *rels;
	List *streams;
	List *funcs;
	List *windows;
} ContAnalyzeContext;


extern ContAnalyzeContext *MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select);
extern void ValidateContinuousQuery(CreateContinuousViewStmt *stmt, const char *sql);

#endif
