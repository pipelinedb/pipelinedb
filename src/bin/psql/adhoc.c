#include "postgres_fe.h"
#include "adhoc_compat.h"

#include <curses.h>
#include <utils/rbtree.h>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

/*
 * pipeline adhoc query client
 */

void die(const char* s) {

	fprintf(stderr, s);
	exit(1);
}

RBTree* tree = 0;

typedef struct Field {

	int len;
	const char* data;
} Field;

typedef struct Row {

	const char* ptr; // points to original malloced chunk.
	int num_fields;
	Field* fields; // array of fields
} Row;

typedef struct Node {

	RBNode node;
	Row row;

} Node;

void test_read(void);
int nodeCompare(const RBNode* ra, const RBNode *rb, void *arg);
void nodeCombine(RBNode* ra, const RBNode *rb, void *arg);
RBNode* nodeAlloc(void* arg);
void nodeFree(RBNode *x, void* arg);

void cleanup_row(Row* row);
void process_row(const char* row_buf);

int nodeCompare(const RBNode* ra, const RBNode *rb, void *arg)
{
	Node* a = (Node*) (ra);
	Node* b = (Node*) (rb);

	return strcmp(a->row.fields[0].data, 
				  b->row.fields[0].data);

}

RBNode* nodeAlloc(void *arg)
{
	return malloc(sizeof(Node));
}

void nodeFree(RBNode *x, void* arg) 
{
	free(x);
}

void cleanup_row(Row* row) {

	free(row->fields);
	free((void*) row->ptr);

	memset(row, 0, sizeof(Row));
}

void nodeCombine(RBNode* ra, const RBNode *rb, void *arg) 
{
	Node* a = (Node*) (ra);
	Node* b = (Node*) (rb);

	cleanup_row(&(a->row));
	a->row = b->row;
}

char row_hdr[4];
int row_n = 0;
int seen = 0;
char *row_buf = 0;
int row_state = 0;

void process_row(const char* row_buf)
{
	Row row = {0,0,0};
	int nf = 0;
	int i = 0;

	const char* iter = row_buf;
	memcpy(&nf, iter, 4); iter += 4;


	row.ptr = row_buf;
	row.num_fields = nf-1;
	row.fields = malloc(sizeof(Field) * row.num_fields);

	for (i = 0; i < nf; ++i) {

		int fn = 0;
		memcpy(&fn, iter, 4); iter += 4;

		if (i > 0) {
			row.fields[i-1].len = fn;
			row.fields[i-1].data = iter;
		}

		iter += fn;
		iter += 1; // nul term
	}

	{
		Node data;
		bool is_new;

		data.row = row;
		rb_insert(tree, (const RBNode*) &data, &is_new);
	}
}

void append_data(const char* buf, size_t n);

void append_data(const char* buf, size_t n) {

	size_t i = 0;

	for (i = 0; i < n; ++i) {

		switch (row_state) 
		{
			case 0:
				row_hdr[seen++] = buf[i];

				if (seen == 4) {

					memcpy(&row_n, row_hdr, 4);

					assert(row_n != 0);
					row_buf = malloc(row_n);
					row_state = 1;
					seen = 0;
				}

				break;

			case 1:
				row_buf[seen++] = buf[i];
				
				if (seen == row_n)
				{
					process_row(row_buf);

					memset(row_hdr, 0, 4);
					row_n = 0;
					seen = 0;
					row_buf = 0;
					row_state = 0;
				}

				break;
		}
	}
}

typedef struct Screen
{
	FILE* term_in;
	SCREEN* main_screen;
	int fd;
} Screen;

Screen* init_screen() 
{
	Screen* screen = malloc(sizeof(Screen));
	const char* term_type = 0;

	memset(screen, 0, sizeof(screen));

	term_type = getenv("TERM");

	if (term_type == NULL || *term_type == '\0') {
		term_type = "unknown";
	}

	screen->term_in = 0;
	screen->term_in = fopen("/dev/tty", "r");

	if (screen->term_in == NULL) {
		die("could not open /dev/tty");
	}

	screen->main_screen = newterm(term_type, stdout, screen->term_in);
	set_term(screen->main_screen);
	screen->fd = fileno(screen->term_in);

	timeout(0);
	clear();
	noecho();
	cbreak();	/* Line buffering disabled. pass on everything */
	curs_set(0);

	keypad(stdscr, TRUE);
	refresh();

	return screen;
}

void destroy_screen(Screen* s)
{
	endwin();
	memset(s, 0, sizeof(s));
	free(s);
}

typedef struct ScreenView 
{
	// essentially our position in the data stream.
	// key etc goes here

} ScreenView;

void test_screen()
{
	Screen* screen = init_screen();
	FILE* debug = fopen("/tmp/debug.txt", "w");

	// main event loop, handles screen and row updates.

	while (true)
	{
		struct pollfd pfd;

		pfd.fd = screen->fd;
		pfd.events = POLLIN;
		pfd.revents = 0;

		int rc = poll(&pfd, 1, -1);
		int c = getch();

		mvprintw(0,0,"got %d\n", c);
		refresh();

//		fprintf(debug, "got %d\n");
//		fflush(debug);

//		int fd = screen->fd;
//		int c = getch();
	}

	destroy_screen(screen);
}

void test_read()
{
	char buf[4096];
	int fd = 0;
	bool finished = false;

	fd = STDIN_FILENO;
	fcntl(fd, F_SETFL, O_NONBLOCK);

	while (!finished)
	{
		int rc = 0;
		struct pollfd pfd;

		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;

		rc = poll(&pfd, 1, -1);

		if (rc < 0) {
			break;
		}

		if (rc == 0) {
			continue;
		}

		if (pfd.revents | POLLIN) {

			while (true) {

				ssize_t nr = read(fd, buf, 4096);

				if (nr == 0) {
					finished = true;
					break;
				}

				if (nr == -1) {
					break;
				}

				append_data(buf, nr);
			}
		}
	}
}

int
main(int argc, char *argv[])
{
	test_screen();

//	const char* term_type = 0;
//	FILE* term_in = 0;
//	SCREEN* main_screen = 0;
//
//	term_type = getenv("TERM");
//
//	if (term_type == NULL || *term_type == '\0') {
//		term_type = "unknown";
//	}
//
//	term_in = fopen("/dev/tty", "r");
//
//	if (term_in == NULL) {
//
//		perror("fopen");
//		return 1;
//	}
//
//	main_screen = newterm(term_type, stdout, term_in);
//	set_term(main_screen);

//	SCREEN* main_screen = newterm(term_type, stdout, term_in);
//	set_term(main_screen);
//
//	void* aux = 0;
//	int foo = 0;
//	tree = rb_create(sizeof(Node), nodeCompare, nodeCombine, nodeAlloc, nodeFree, aux);
//
//	test_read();
//
//	rb_begin_iterate(tree, LeftRightWalk);
//
//	{
//		Node* node;
//
//		while ((node = (Node*) rb_iterate(tree))) {
//
//			printf("got node %s\n", node->row.fields[0].data);
//		}
//	}
//
//	return 0;
}
