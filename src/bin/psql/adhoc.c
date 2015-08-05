#include "postgres_fe.h"
#include "adhoc_compat.h"
#include "rowmap.h"
#include "rowstream.h"
#include "screen.h"
#include "model.h"

#include <curses.h>
#include <utils/rbtree.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>

/*
 * pipeline adhoc query client
 */

typedef struct App
{
	Model* model;
	Screen* screen;
} App;

volatile int keep_running = 1;
void sighandle(int);

void sighandle(int x)
{
	keep_running = 0;
}

void row_callback(void* ctx, int type, Row* row);

int
main(int argc, char *argv[])
{
	App app = {0,0};
	Model *model = ModelInit();

	Screen *screen = 0;
	RowStream *stream = 0;

	signal(SIGINT, sighandle);
	screen = ScreenInit(model);

	app.model = model;
	app.screen = screen;

	stream = RowStreamInit(row_callback, &app);

	while (keep_running)
	{
		struct pollfd pfd[2];
		int rc = 0;

		memset(&pfd, 0, sizeof(pfd));

		pfd[0].fd = RowStreamFd(stream);
		pfd[0].events = POLLIN;
		pfd[0].revents = 0;

		if (screen)
		{
			pfd[1].fd = ScreenFd(screen);
			pfd[1].events = POLLIN;
			pfd[1].revents = 0;
		}

		rc = poll(pfd, screen ? 2 : 1, -1);

		if (rc < 0)
		{
			break;
		}

		if (pfd[0].revents & POLLIN)
		{
			// TODO - take a snapshot instead
			if (screen && ScreenIsPaused(screen))
			{
				usleep(1000);
			}
			else
			{
				bool fin = RowStreamHandleInput(stream);

				if (fin)
					break;
			}
		}

		if (screen)
		{
			if (pfd[1].revents & POLLIN)
			{
				ScreenHandleInput(screen);
			}
		}
	}

	if (screen)
	{
		ScreenDestroy(screen);
	}

	RowStreamDestroy(stream);
	ModelDestroy(model);

	return 0;
}

void row_callback(void* ctx, int type, Row* row)
{
	App *app = (App*)(ctx);

	switch (type) {

		case 'k':
			ModelSetKey(app->model, row);
			break;
		case 'h':
			ModelSetHeader(app->model, row);
			if (app->screen) ScreenUpdate(app->screen);
			break;
		case 'i': case 'u':
			ModelAddRow(app->model, row);
			if (app->screen) ScreenUpdate(app->screen);
			break;
		case 'd':
			ModelDeleteRow(app->model, row);
			if (app->screen) ScreenUpdate(app->screen);
			break;
	}
}
