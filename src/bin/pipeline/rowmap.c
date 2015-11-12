#include "postgres_fe.h"
#include "rowmap.h"
#include "adhoc_util.h"
#include "bsd_rbtree.h"

static size_t *g_row_key = NULL;
static size_t g_row_key_n = 0;

void
RowCleanup(Row *row)
{
	pg_free(row->fields);
	pg_free(row->ptr);
	memset(row, 0, sizeof(Row));
}

size_t
RowSize(Row *r)
{
	return r->n;
}

typedef struct TreeNode
{
	bsd_rb_node_t node;
	Row row;
} TreeNode;

typedef struct Key
{
	Row *row;
	int is_key;
} Key;

struct RowMap
{
	bsd_rb_tree_t tree;
	bsd_rb_tree_ops_t ops;
};

void
RowKeyReset()
{
	pg_free(g_row_key);
	g_row_key_n = 0;
	g_row_key = 0;
}

void
RowKeyAdd(size_t i)
{
	g_row_key = pg_realloc(g_row_key, (g_row_key_n + 1) * sizeof(size_t));
	g_row_key[g_row_key_n] = i;
	g_row_key_n++;
}

size_t
RowFieldLength(Row *r, size_t i)
{
	return r->fields[i].n;
}

Field *
RowGetField(Row *r, size_t i)
{
	return r->fields + i;
}

static inline int
ncmp(size_t a, size_t b)
{
	if (a < b)
		return -1;

	return (a == b) ? 0 : 1;
}

static int
field_cmp(Field *f1, Field *f2)
{
	size_t n = Min(f1->n, f2->n);
	int c1 = memcmp(f1->data, f2->data, n);

	if (c1 == 0)
		return ncmp(f1->n, f2->n);

	return c1;
}

char *
RowFieldValue(Row *r, size_t i)
{
	return r->fields[i].data;
}

static inline size_t
key_length(Row *r)
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

/* make a new row by copying the key fields from r */
Row
RowGetKey(Row *r)
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

	TermAssert(RowSize(r) > g_row_key_n);

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

static int
row_cmp_row_row(Row *r1, Row *r2)
{
	size_t i = 0;

	for (i = 0; i < g_row_key_n; ++i)
	{
		size_t col = g_row_key[i];

		int c = field_cmp(RowGetField(r1, col),
						  RowGetField(r2, col));

		if (c != 0)
			return c;
	}

	return 0;
}

static int
row_cmp_row_key(Row *r1, Row *r2)
{
	size_t i = 0;

	if (RowSize(r2) == 0)
		return 1;

	TermAssert(RowSize(r2) == g_row_key_n);

	for (i = 0; i < g_row_key_n; ++i)
	{
		size_t col = g_row_key[i];

		int c = field_cmp(RowGetField(r1, col),
						 RowGetField(r2, i));

		if (c != 0)
			return c;
	}

	return 0;
}

static int
compare_key(void *ctx, const void *a, const void *b)
{
	const TreeNode *na = (const TreeNode *)(a);
	const Key *nb = (const Key *)(b);

	if (nb->is_key)
		return row_cmp_row_key((Row *) &na->row, (Row *) nb->row);
	else
		return row_cmp_row_row((Row *) &na->row, (Row *) nb->row);
}

static int
compare_nodes(void *ctx, const void *a, const void *b)
{
	const TreeNode *na = (const TreeNode *)(a);
	const TreeNode *nb = (const TreeNode *)(b);

	return row_cmp_row_row((Row *) &na->row, (Row *) &nb->row);
}

RowMap *
RowMapInit()
{
	RowMap *m = pg_malloc0(sizeof(RowMap));

	m->ops.bsd_rbto_compare_nodes = compare_nodes;
	m->ops.bsd_rbto_compare_key = compare_key;
	m->ops.bsd_rbto_node_offset = 0;
	m->ops.bsd_rbto_context = 0;

	bsd_rb_tree_init(&m->tree, &m->ops);

	return m;
}

/* clean up all the rows, and row storage and call pg_free on m */
void
RowMapDestroy(RowMap *m)
{
	void *node = bsd_rb_tree_iterate(&m->tree, 0, 0);

	while(node != 0)
	{
		Row *row;
		void *delnode;

		row = GetRow(node);
		delnode = node;

		node = bsd_rb_tree_iterate(&m->tree, node, 1);
		bsd_rb_tree_remove_node(&m->tree, delnode);

		RowCleanup(row);
		pg_free(delnode);
	}

	memset(m, 0, sizeof(RowMap));
	pg_free(m);
}

Row *
GetRow(RowIterator iter)
{
	return &((TreeNode *)(iter))->row;
}

void
RowMapErase(RowMap *m, Row *key)
{
	RowIterator iter = RowMapFindWithKey(m, key);

	if (iter == RowMapEnd(m))
		return;

	RowCleanup(GetRow(iter));
	bsd_rb_tree_remove_node(&m->tree, iter);
}

void
RowMapUpdate(RowMap *m, Row *row)
{
	TreeNode *res = 0;

	TreeNode *new_node = pg_malloc0(sizeof(TreeNode));
	new_node->row = *row;

	res = bsd_rb_tree_insert_node(&m->tree, new_node);

	if (res != new_node)
	{
		/* replace old contents, free unneeded new node */
		
		RowCleanup(&res->row);

		res->row = *row;
		pg_free(new_node);
	}
}

size_t
RowMapSize(RowMap *m)
{
	return bsd_rb_tree_count(&m->tree);
}

RowIterator
RowMapBegin(RowMap *m)
{
	return bsd_rb_tree_iterate(&m->tree, 0, 0);
}

RowIterator
RowMapEnd(RowMap *m)
{
	return 0;
}

RowIterator
RowMapFindWithRow(RowMap *m, Row *row)
{
	Key k = { row, false };
	void *node = bsd_rb_tree_find_node(&m->tree, &k);

	return node;
}

RowIterator
RowMapFindWithKey(RowMap *m, Row *key)
{
	Key k = { key, true};
	void *node = bsd_rb_tree_find_node(&m->tree, &k);
	return node;
}

RowIterator
RowMapLowerBound(RowMap *m, Row *key)
{
	Key k = {key, true};
	return bsd_rb_tree_find_node_geq(&m->tree, &k);
}

void
RowMapDump(RowMap *m)
{
	void *node = bsd_rb_tree_iterate(&m->tree, 0, 0);

	for(; node != 0; node = bsd_rb_tree_iterate(&m->tree, node, 1))
	{
		RowDump(GetRow(node));
	}
}

bool RowIteratorEqual(RowIterator a, RowIterator b)
{
	return a == b;
}

RowIterator RowIteratorNext(RowMap *m, RowIterator a)
{
	return bsd_rb_tree_iterate(&m->tree, a, 1);
}

RowIterator RowIteratorPrev(RowMap *m, RowIterator a)
{
	return bsd_rb_tree_iterate(&m->tree, a, 0);
}

/* debugging funcs */
void
RowDump(Row *row)
{
	size_t i = 0;

	for (i = 0; i < RowSize(row); ++i)
		printf("%.*s ", (int) RowFieldLength(row, i), RowFieldValue(row, i));

	printf("\n");
}

void
RowDumpToString(Row *row, PQExpBuffer buf)
{
	size_t i = 0;

	for (i = 0; i < RowSize(row); ++i)
	{
		Field *f = RowGetField(row, i);
		appendBinaryPQExpBuffer(buf, f->data, f->n);
		appendPQExpBuffer(buf, " ");
	}
}
