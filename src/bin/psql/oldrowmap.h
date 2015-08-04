#ifndef OLDROWMAP_H_18A90DF6
#define OLDROWMAP_H_18A90DF6

#include "oldrow.h"

typedef struct Node {

	RBNode node;
	OldRow row;

} Node;

int nodeCompare(const RBNode* ra, const RBNode *rb, void *arg);
void nodeCombine(RBNode* ra, const RBNode *rb, void *arg);
RBNode* nodeAlloc(void* arg);
void nodeFree(RBNode *x, void* arg);
void cleanup_row(OldRow *row);

//void process_row(const char* row_buf);

int nodeCompare(const RBNode* ra, const RBNode *rb, void *arg)
{
	Node* a = (Node*) (ra);
	Node* b = (Node*) (rb);

	return strcmp(a->row.fields[1].data, 
				  b->row.fields[1].data);
}

RBNode* nodeAlloc(void *arg)
{
	return malloc(sizeof(Node));
}

void nodeFree(RBNode *x, void* arg) 
{
	free(x);
}

void cleanup_row(OldRow* row) {

	free(row->fields);
	free((void*) row->ptr);

	memset(row, 0, sizeof(OldRow));
}

void nodeCombine(RBNode* ra, const RBNode *rb, void *arg) 
{
	Node* a = (Node*) (ra);
	Node* b = (Node*) (rb);

	cleanup_row(&(a->row));
	a->row = b->row;
}

#endif
