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

/*
 * Basic data reprentation for the adhoc client.
 *
 * These structs act as simple containers for the row data.
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

extern void RowCleanup(Row *r);
extern size_t RowSize(Row *r);
extern void RowKeyReset(void);
extern void RowKeyAdd(size_t i);
extern size_t RowFieldLength(Row *r, size_t i);
extern char *RowFieldValue(Row *r, size_t i);
extern Field *RowGetField(Row *r, size_t i);
extern Row RowGetKey(Row *r);

/*
 * Interface modelled after an rbtree. Internally it is currently using
 * a sorted array which resorts after each update/delete.
 *
 * Row data is allocated outside here, but it is cleaned up here on
 * update/delete.
 *
 * TODO - replace with rbtree.
 */

typedef struct RowMap
{
	Row     *rows;
	size_t  n;
	size_t  cap;
} RowMap;

typedef Row *RowIterator;

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

/* debug funcs */
extern void RowDump(Row *row);
extern void RowMapDump(RowMap *m);
extern void RowDumpToString(Row *row, PQExpBuffer buffer);

#endif
