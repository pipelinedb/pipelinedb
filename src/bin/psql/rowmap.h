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

size_t RowFieldLength(Row* r, size_t i);
char* RowFieldValue(Row* r, size_t i);
char* RowGetKey(Row* r);

typedef struct RowMap {

	Row* rows;

	size_t n;
	size_t cap;

} RowMap;

typedef Row* RowIterator;

RowMap* RowMapInit(void);
void RowMapDestroy(RowMap* m);

void RowMapErase(RowMap *m, const char* key);
void RowMapUpdate(RowMap *m, Row* row);
size_t RowMapSize(RowMap *m);
void RowMapDump(RowMap *m);

RowIterator RowMapBegin(RowMap *m);
RowIterator RowMapEnd(RowMap *m);
RowIterator RowMapFind(RowMap *m, const char* key);

RowIterator RowMapLowerBound(RowMap *m, const char* key);

#endif
