/*
 * padhoc - PipelineDB ncurses app for adhoc continuous queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/adhoc.c
 */

#include "postgres_fe.h"
#include "rowmap.h"
#include "rowstream.h"
#include "pg_rowstream.h"
#include "screen.h"
#include "model.h"
#include "adhoc_options.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>

typedef struct App
{
	Model   *model;
	Screen  *screen;
} App;

volatile bool keep_running = true;

static void
sighandle(int x)
{
	keep_running = false;
}

static void row_event_dispatcher(void *ctx, int type, Row *row);

/*
 * Initializes the padhoc app, and runs the main event loop.
 *
 * The event loop is written in non blocking style, and deals with two fds
 *
 * pfd[0] - row input stream (currently stdin)
 * pfd[1] - screen tty fd (setup with ncurses)
 *
 * To ease debugging, the screen can be disabled (by leaving as NULL).
 */
int
main(int argc, char *argv[])
{
	bool ok;
	adhoc_opts options;

	App app = {0,0};
	Model *model = ModelInit();

	Screen *screen = 0;
	PGRowStream *stream = 0;

	memset(&options, 0, sizeof(adhoc_opts));

	handle_options(argc, argv, &options);

	/*
	 * Setup a signal handler to break out of the loop upon ctrl-c. We must
	 * do this so the app will clean up the terminal properly when it is
	 * shutdown.
	 */
	signal(SIGINT, sighandle);
	stream = PGRowStreamInit(options.db, options.action_string, row_event_dispatcher, &app);

	/* Comment the next line out to ease debugging */
	screen = ScreenInit(model);

	app.model = model;
	app.screen = screen;

	ok = PGRowStreamPostInit(stream);

	if (!ok)
	{
		if (screen)
		{
			ScreenDestroy(screen);
		}

		fprintf(stderr, "could not execute query %s\n", argv[1]);
		exit(1);
	}

	while (keep_running)
	{
		struct pollfd pfd[2];
		int rc = 0;

		memset(&pfd, 0, sizeof(pfd));

		pfd[0].fd = PGRowStreamFd(stream);
		pfd[0].events = POLLIN;

		if (screen)
		{
			pfd[1].fd = ScreenFd(screen);
			pfd[1].events = POLLIN;
		}

		rc = poll(pfd, screen ? 2 : 1, -1);

		if (rc < 0)
		{
			keep_running = false;
			break;
		}

		/* Handle the row stream fd */
		if (pfd[0].revents & POLLIN)
		{
			if (screen && ScreenIsPaused(screen))
			{
				/*
				 * If we are paused, don't read any new rows. This will block
				 * the upstream writer.
				 */
				usleep(1000);
			}
			else
			{
				bool fin = PGRowStreamHandleInput(stream);

				if (fin)
					break;
			}
		}

		/* Handle the screen fd if we have one */
		if (screen)
		{
			if (pfd[1].revents & POLLIN)
				ScreenHandleInput(screen);
		}
	}

	if (screen)
		ScreenDestroy(screen);

	PGRowStreamDestroy(stream);
	ModelDestroy(model);

	return 0;
}

/*
 * Callback fired from RowStream
 */
static void
row_event_dispatcher(void *ctx, int type, Row *row)
{
	App *app = (App *)(ctx);
	bool dirty = false;

	switch (type)
	{
		case 'h':
			ModelHeaderRow(app->model, row);
			dirty = true;
			break;
		case 'k':
			ModelKeyRow(app->model, row);
			break;
		case 'i':
			ModelInsertRow(app->model, row);
			dirty = true;
			break;
		case 'u':
			ModelUpdateRow(app->model, row);
			dirty = true;
			break;
		case 'd':
			ModelDeleteRow(app->model, row);
			dirty = true;
			break;
	}

	if (app->screen && dirty)
		ScreenUpdate(app->screen);
}
