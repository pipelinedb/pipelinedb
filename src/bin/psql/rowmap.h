#ifndef MODEL_H_EE43F6F4
#define MODEL_H_EE43F6F4

#include <stdint.h>
#include <stdlib.h>

typedef struct Field {

	char* data;
	size_t n;
} Field;

typedef struct Row {

	char* ptr;
	Field* fields;
	size_t n;

} Row;

void RowCleanup(Row *row);
size_t RowSize(Row* r);

void RowKeyReset(void);
void RowKeyAdd(size_t i);

size_t RowFieldLength(Row* r, size_t i);
char* RowFieldValue(Row* r, size_t i);
Field* RowGetField(Row* r, size_t i);

Row RowGetKey(Row* r);

typedef struct RowMap {

	Row* rows;

	size_t n;
	size_t cap;

} RowMap;

typedef Row* RowIterator;

RowMap* RowMapInit(void);
void RowMapDestroy(RowMap* m);

void RowMapErase(RowMap *m, Row* key);
void RowMapUpdate(RowMap *m, Row* row);
size_t RowMapSize(RowMap *m);
void RowMapDump(RowMap *m);

void RowDump(Row* row);

RowIterator RowMapBegin(RowMap *m);
RowIterator RowMapEnd(RowMap *m);

RowIterator RowMapFindWithRow(RowMap *m, Row* row);
RowIterator RowMapFindWithKey(RowMap *m, Row* key);

RowIterator RowMapLowerBound(RowMap *m, Row* key);

#endif
