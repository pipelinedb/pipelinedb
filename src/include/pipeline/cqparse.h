/*-------------------------------------------------------------------------
 *
 * cqparse.h
 *	  Interface for for parsing and analyzing continuous queries
 *
 * src/include/catalog/pipeline/cqparser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQPARSE_H
#define CQPARSE_H

typedef struct CQParseState
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
} CQParseContext;


extern CQParseContext *MakeCQParseContext(ParseState *pstate, SelectStmt *select);
extern void ValidateContinuousQuery(SelectStmt *select, const char *sql);

#endif
