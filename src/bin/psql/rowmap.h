/*-------------------------------------------------------------------------
 *
 * rowmap.h
 *    Interface for the adhoc client row map
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/rowmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ROWMAP_H_46D31946
#define ROWMAP_H_46D31946

#include "postgres_fe.h"
#include "pqexpbuffer.h"

/*
 * Basic data reprentation for the adhoc client.
 *
 * These structs act as simple containers for the row data.
 *
 * Note - a row can be either a complete row or just key fields
 */

typedef struct Field
{
	char    *data;
	size_t  n;
} Field;

typedef struct Row
{
	char    *ptr;
	Field   *fields;
	size_t  n;
} Row;

typedef void (*RowFunc) (void *ctx, int type, Row *row);

extern void RowCleanup(Row *r);
extern size_t RowSize(Row *r);
extern void RowKeyReset(void);
extern void RowKeyAdd(size_t i);
extern size_t RowFieldLength(Row *r, size_t i);
extern char *RowFieldValue(Row *r, size_t i);
extern Field *RowGetField(Row *r, size_t i);
extern Row RowGetKey(Row *r);

/*
 * Interface modelled after an rbtree. Internally it is using bsd_rbtree
 *
 * Row data is allocated outside here, but it is cleaned up here on
 * update/delete.
 */

typedef struct RowMap RowMap;

typedef void *RowIterator;
extern RowMap *RowMapInit(void);

/* RowMapDestroy will perform RowCleanup on all rows */
extern void RowMapDestroy(RowMap *m);

extern void RowMapErase(RowMap *m, Row *key);
extern void RowMapUpdate(RowMap *m, Row *row);
extern size_t RowMapSize(RowMap *m);

/* iteration funcs */
extern RowIterator RowMapBegin(RowMap *m);
extern RowIterator RowMapEnd(RowMap *m);
extern RowIterator RowMapFindWithRow(RowMap *m, Row *row);
extern RowIterator RowMapFindWithKey(RowMap *m, Row *key);
extern RowIterator RowMapLowerBound(RowMap *m, Row *key);

extern Row* GetRow(RowIterator iter);

extern bool RowIteratorEqual(RowIterator a, RowIterator b);
extern RowIterator RowIteratorNext(RowMap *m, RowIterator a);
extern RowIterator RowIteratorPrev(RowMap *m, RowIterator a);

/* debugging funcs */
extern void RowDump(Row *row);
extern void RowDumpToString(Row *row, PQExpBuffer buf);
extern void RowMapDump(RowMap *m);

#endif
