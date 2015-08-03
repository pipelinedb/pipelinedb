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

typedef struct RowStream
{
	int fd;
	char buf[4096];

	char* flex_buf;
	size_t flex_cap;
	size_t flex_n;

} RowStream;

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

void append_data(RowStream *stream, const char* buf, size_t n);

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


RowStream* init_row_stream()
{
	RowStream *stream = malloc(sizeof(RowStream));
	memset(stream, 0, sizeof(RowStream));

	stream->fd = STDIN_FILENO;
	fcntl(stream->fd, F_SETFL, O_NONBLOCK);

	return stream;
}

int num_fields(const char* line)
{
	const char* s = line;
	int cnt = 0;

	while (*s)
	{
		if (*s == ' ') cnt++;
		s++;
	}

	return cnt + 1;
}

Row parse_text_row(const char* line)
{
	Row row = {0,0,0};
	int nf = 0;
	int i = 0;
	const char* sptr = 0;

	row.ptr = strdup(line);
	row.num_fields = num_fields(line);
	row.fields = malloc(sizeof(Field) * row.num_fields);

	sptr = row.ptr;

	while (true) {

		const char* tok = strtok(sptr, " ");

		if (!tok) 
			break;

		row.fields[i].len = strlen(tok);
		row.fields[i].data = tok;
		i++;

		sptr = 0;
	}

	return row;
}

void print_row(Row row)
{
	size_t i = 0;

	for (i = 0; i < row.num_fields; ++i) {

		printf("%d:%s ", i, row.fields[i].data);
	}

	printf("\n");
}

void append_data(RowStream *stream, const char* buf, size_t nr)
{
	size_t ns = stream->flex_n + nr;
	size_t i = 0;

	if (ns > stream->flex_cap) {

		stream->flex_buf = realloc(stream->flex_buf, ns + 1);
		stream->flex_cap = ns;
	}

	for (i = 0; i < nr; ++i) {

		stream->flex_buf[stream->flex_n++] = buf[i];

		if (buf[i] == '\n') {

			stream->flex_buf[stream->flex_n-1] = '\0';

			Row row = parse_text_row(stream->flex_buf);
			print_row(row);

			cleanup_row(&row);

			// reset
			stream->flex_n = 0;
			stream->flex_buf[stream->flex_n] = '\0';
		}
	}
}

// returns true if finished

bool handle_row_stream(RowStream *stream)
{
	while (true)
	{
		ssize_t nr = read(stream->fd, stream->buf, 4096);

		if (nr == 0) {
			return true;
		}

		if (nr == -1) {
			return false;
		}

		append_data(stream, stream->buf, nr);
	}

	return false;
}

void destroy_row_stream(RowStream *stream);
void destroy_row_stream(RowStream *stream)
{

}

void test_screen()
{
//	Screen* screen = init_screen();
	RowStream* stream = init_row_stream();
//	FILE* debug = fopen("/tmp/debug.txt", "w");

	// main event loop, handles screen and row updates.

	while (true)
	{
		struct pollfd pfd[2];
		memset(pfd, 0, sizeof(pfd));

		pfd[0].fd = stream->fd;
		pfd[0].events = POLLIN;
		pfd[0].revents = 0;

		int rc = poll(&pfd, 1, -1);

		if (rc < 0) {
			die("poll error");
		}

		printf("rc %d\n", pfd[0].revents);

		if (pfd[0].revents & POLLIN)
		{
			bool fin = handle_row_stream(stream);

			if (fin) {

				break;
			}
		}
		
//		if (pfd[1].revents & POLLIN) {
//			handleP
//		}

//		int rc = poll(&pfd, 1, -1);
//		int c = getch();
//
//		mvprintw(0,0,"got %d\n", c);
//		refresh();

//		fprintf(debug, "got %d\n");
//		fflush(debug);

//		int fd = screen->fd;
//		int c = getch();
	}

//	destroy_screen(screen);
	destroy_row_stream(stream);
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

				append_data(0, buf, nr);
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
