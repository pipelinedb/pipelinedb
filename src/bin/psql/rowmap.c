#include "rowmap.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

void RowCleanup(Row *row)
{
	free(row->fields);
	free(row->ptr);
	memset(row, 0, sizeof(Row));
}

size_t RowSize(Row* r)
{
	return r->n;
}

size_t RowFieldLength(Row* r, size_t i)
{
	return r->fields[i].n;
}

char* RowFieldValue(Row* r, size_t i)
{
	return r->fields[i].data;
}

char* RowGetKey(Row* r)
{
	return RowFieldValue(r, 0);
}

void RowMapAppend(RowMap *, Row* row);

void RowMapAppend(RowMap *m, Row* row)
{
	size_t ns = m->n + 1;

	if (ns > m->cap)
	{
		size_t nc = m->cap ? m->cap : 1;

		while (nc < ns)
		{
			nc *= 2;
		}

		m->rows = realloc(m->rows, nc * sizeof(Row));
		m->cap = nc;
	}

	m->rows[m->n++] = *row;
}

void RowMapSort(RowMap *m);

int row_cmp(const void* p1, const void* p2);
int row_cmp(const void* p1, const void* p2)
{
	Row *r1 = (Row*)(p1);
	Row *r2 = (Row*)(p2);

	return strcmp(RowGetKey(r1), RowGetKey(r2));
}

void RowMapSort(RowMap *m)
{
	qsort(m->rows, m->n, sizeof(Row), row_cmp);
}

RowMap* RowMapInit()
{
	RowMap* m = malloc(sizeof(RowMap));
	memset(m, 0, sizeof(RowMap));

	return m;
}

void RowMapDestroy(RowMap *m)
{
	size_t i = 0;

	for (i = 0; i < m->n; ++i)
	{
		RowCleanup(&m->rows[i]);
	}

	free(m->rows);

	memset(m, 0, sizeof(RowMap));
	free(m);
}

void RowMapDump(RowMap* m)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		printf("%s\n", RowGetKey(&m->rows[i]));
	}
}


void RowMapErase(RowMap *m, const char* key)
{
	RowIterator iter = RowMapFind(m, key);
	size_t rem = 0;

	if (iter == RowMapEnd(m))
		return;

	rem = RowMapEnd(m) - iter;
	memmove(iter, iter + 1, rem * sizeof(Row));
	m->n--;
}

void RowMapUpdate(RowMap *m, Row* row)
{
	RowIterator iter = RowMapFind(m, RowGetKey(row));

	if (iter == RowMapEnd(m))
	{
		RowMapAppend(m, row);
		RowMapSort(m);
	}
	else
	{
		RowCleanup(iter);
		*iter = *row;
	}
}

size_t RowMapSize(RowMap* m)
{
	return m->n;
}

RowIterator RowMapBegin(RowMap *m)
{
	return m->rows;
}

RowIterator RowMapEnd(RowMap *m)
{
	return m->rows + m->n;
}

RowIterator RowMapFind(RowMap *m, const char* key)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		if (strcmp(RowGetKey(&m->rows[i]), key) == 0)
		{
			break;
		}
	}

	return m->rows + i;
}

RowIterator RowMapLowerBound(RowMap *m, const char* key)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		if (strcmp(RowGetKey(&m->rows[i]), key) >= 0)
		{
			break;
		}
	}

	return m->rows + i;
}
