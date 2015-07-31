#include "postgres_fe.h"
#include "adhoc_compat.h"

#include <utils/rbtree.h>


/*
 * pipeline adhoc query client
 */

typedef struct Field {

	int len;
	char* data;
} Field;

struct Node {

	RBNode node;
	Field** rows;
};

int
main(int argc, char *argv[])
{
	PAStringData str;

	initPAString(&str);
	appendPAStringString(&str, "foo bar slart");

	printf("%s\n", str.data);
	free(str.data);

	// initStringInfo
	// initStringInfo
	//	appendStringInfo(&sd, "what");
	//	RBTree* tree = rb_create(4, 0, 0, 0, 0, 0);

	//	extern RBTree *rb_create(Size node_size,
	//			  rb_comparator comparator,
	//			  rb_combiner combiner,
	//			  rb_allocfunc allocfunc,
	//			  rb_freefunc freefunc,
	//			  void *arg);

	return 0;
}
