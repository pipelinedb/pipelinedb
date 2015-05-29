/*-------------------------------------------------------------------------
 *
 * cont_analyze.h
 *	  Interface for for parsing and analyzing continuous queries
 *
 * src/include/catalog/pipeline/cont_analyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_ANALYZE_H
#define CONT_ANALYZE_H

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
	Node *expr;
	int location;
} ContAnalyzeContext;

extern bool collect_rels_and_streams(Node *node, ContAnalyzeContext *context);
extern bool collect_types_and_cols(Node *node, ContAnalyzeContext *context);

extern ContAnalyzeContext *MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select);
extern void ValidateContQuery(CreateContViewStmt *stmt, const char *sql);

extern void transformContSelectStmt(ParseState *pstate, SelectStmt *select);
extern RangeTblEntry *transformStreamDesc(ParseState *pstate, StreamDesc *stream);

#endif
