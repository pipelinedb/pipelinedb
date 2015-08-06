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

#include <unistd.h>
#include <stdint.h>

#include "postgres_fe.h"
#include "pqexpbuffer.h"

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

extern void RowCleanup(Row *r);
extern size_t RowSize(Row *r);
extern void RowKeyReset(void);
extern void RowKeyAdd(size_t i);
extern size_t RowFieldLength(Row *r, size_t i);
extern char *RowFieldValue(Row *r, size_t i);
extern Field *RowGetField(Row *r, size_t i);
extern Row RowGetKey(Row *r);

typedef struct RowMap
{
	Row     *rows;
	size_t  n;
	size_t  cap;
} RowMap;

typedef Row *RowIterator;

extern RowMap *RowMapInit(void);
extern void RowMapDestroy(RowMap *m);
extern void RowMapErase(RowMap *m, Row *key);
extern void RowMapUpdate(RowMap *m, Row *row);
extern size_t RowMapSize(RowMap *m);

extern void RowDump(Row *row);
extern void RowMapDump(RowMap *m);
extern void RowDumpToString(Row *row, PQExpBuffer buffer);

extern RowIterator RowMapBegin(RowMap *m);
extern RowIterator RowMapEnd(RowMap *m);
extern RowIterator RowMapFindWithRow(RowMap *m, Row *row);
extern RowIterator RowMapFindWithKey(RowMap *m, Row *key);
extern RowIterator RowMapLowerBound(RowMap *m, Row *key);

#endif
