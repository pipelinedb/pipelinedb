#include "postgres_fe.h"
#include "rowmap.h"

void RowCleanup(Row *row)
{
	pg_free(row->fields);
	pg_free(row->ptr);
	memset(row, 0, sizeof(Row));
}

size_t RowSize(Row *r)
{
	return r->n;
}

static size_t *g_row_key = 0;
static size_t g_row_key_n = 0;

void RowKeyReset()
{
	pg_free(g_row_key);
	g_row_key_n = 0;
	g_row_key = 0;
}

void RowKeyAdd(size_t i)
{
	g_row_key = pg_realloc(g_row_key, (g_row_key_n + 1) * sizeof(size_t));
	g_row_key[g_row_key_n] = i;
	g_row_key_n++;
}

size_t RowFieldLength(Row *r, size_t i)
{
	return r->fields[i].n;
}

Field *RowGetField(Row *r, size_t i)
{
	return r->fields + i;
}

static inline int ncmp(size_t a, size_t b)
{
	if (a < b)
	{
		return -1;
	}

	return (a == b) ? 0 : 1;
}

int FieldCmp(Field *f1, Field *f2);

int FieldCmp(Field *f1, Field *f2)
{
	size_t n = Min(f1->n, f2->n);
	int c1 = memcmp(f1->data, f2->data, n);

	if (c1 == 0)
	{
		return ncmp(f1->n, f2->n);
	}

	return c1;
}

char *RowFieldValue(Row *r, size_t i)
{
	return r->fields[i].data;
}

static inline size_t key_length(Row *r)
{
	size_t n = 0;
	size_t i = 0;

	for (i = 0; i < g_row_key_n; ++i)
	{
		n += r->fields[i].n;
		n += 1;
	}

	return n;
}

Row RowGetKey(Row *r)
{
	Row row = {0,0,0};
	size_t nb = key_length(r);
	size_t n = g_row_key_n;

	char *d = pg_malloc(nb);

	size_t i = 0;
	char *out_iter = d;

	row.ptr = d;
	row.fields = pg_malloc(n * sizeof(Field));
	row.n = n;

	assert(RowSize(r) > g_row_key_n);

	for (i = 0; i < n; ++i)
	{
		size_t col = g_row_key[i];
		char *tok = out_iter;

		memcpy(out_iter, r->fields[col].data, r->fields[col].n);
		out_iter += r->fields[col].n;
		*out_iter++ = '\0';

		row.fields[i].data = tok; 
		row.fields[i].n = r->fields[col].n; 
	}

	return row;
}

void RowMapAppend(RowMap *, Row *row);

void RowMapAppend(RowMap *m, Row *row)
{
	size_t ns = m->n + 1;

	if (ns > m->cap)
	{
		size_t nc = m->cap ? m->cap : 1;

		while (nc < ns)
		{
			nc *= 2;
		}

		m->rows = pg_realloc(m->rows, nc * sizeof(Row));
		m->cap = nc;
	}

	m->rows[m->n++] = *row;
}

void RowMapSort(RowMap *m);

int row_cmp_row_row(const void *p1, const void *p2);
int row_cmp_row_key(const void *p1, const void *p2);

int row_cmp_row_row(const void *p1, const void *p2)
{
	Row *r1 = (Row *) p1;
	Row *r2 = (Row *) p2;

	// composite key support

	size_t i = 0;

	for (i = 0; i < g_row_key_n; ++i)
	{
		size_t col = g_row_key[i];

		int c = FieldCmp(RowGetField(r1, col),
						 RowGetField(r2, col));

		if (c != 0)
		{
			return c;
		}
	}

	return 0;
}

int row_cmp_row_key(const void *p1, const void *p2)
{
	Row *r1 = (Row *) p1;
	Row *r2 = (Row *) p2;

	size_t i = 0;

	if (RowSize(r2) == 0)
	{
		return 1;
	}

	if (RowSize(r2) != g_row_key_n)
	{
		RowDump(r1);
		RowDump(r2);
	}

	assert(RowSize(r2) == g_row_key_n);

	// composite key support

	for (i = 0; i < g_row_key_n; ++i)
	{
		size_t col = g_row_key[i];

		int c = FieldCmp(RowGetField(r1, col),
						 RowGetField(r2, i));

		if (c != 0)
		{
			return c;
		}
	}

	return 0;
}

void RowMapSort(RowMap *m)
{
	qsort(m->rows, m->n, sizeof(Row), row_cmp_row_row);
}

RowMap *RowMapInit()
{
	RowMap *m = pg_malloc(sizeof(RowMap));
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

	pg_free(m->rows);

	memset(m, 0, sizeof(RowMap));
	pg_free(m);
}

void RowDump(Row *row)
{
	size_t i = 0;

	for (i = 0; i < RowSize(row); ++i)
	{
		printf("%.*s ", (int) RowFieldLength(row, i), RowFieldValue(row, i));
	}

	printf("\n");
}

void RowMapDump(RowMap *m)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		RowDump(m->rows + i);
	}
}

void RowMapErase(RowMap *m, Row *key)
{
	RowIterator iter = RowMapFindWithKey(m, key);
	size_t rem = 0;

	if (iter == RowMapEnd(m))
		return;

	RowCleanup(iter);

	rem = RowMapEnd(m) - (iter + 1);

	memmove(iter, iter + 1, rem * sizeof(Row));
	m->n--;
}

void RowMapUpdate(RowMap *m, Row *row)
{
	RowIterator iter = RowMapFindWithRow(m, row);

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

size_t RowMapSize(RowMap *m)
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

RowIterator RowMapFindWithRow(RowMap *m, Row *row)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		if (row_cmp_row_row(&m->rows[i], row) == 0)
		{
			break;
		}
	}

	return m->rows + i;
}

RowIterator RowMapFindWithKey(RowMap *m, Row *key)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		if (row_cmp_row_key(&m->rows[i], key) == 0)
		{
			break;
		}
	}

	return m->rows + i;
}

RowIterator RowMapLowerBound(RowMap *m, Row *key)
{
	size_t i = 0;

	for (; i < m->n; ++i)
	{
		if (row_cmp_row_key(&m->rows[i], key) >= 0)
		{
			break;
		}
	}

	return m->rows + i;
}
