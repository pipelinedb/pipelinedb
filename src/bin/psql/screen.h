/*-------------------------------------------------------------------------
 *
 * screen.h
 *    Interface for the adhoc client ncurses ui
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/screen.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCREEN_H
#define SCREEN_H

#include "model.h"

#include <stdio.h>
#include <ncurses.h>

/*
 * Stores the view related information, and ncurses state.
 */

typedef struct Screen
{
	Model   *model;
	Row     key;
	FILE    *term_in;
	int     fd;
	SCREEN  *nterm;
	int     x_pos;
	int     x_col;
	int     pause;
} Screen;

extern Screen *ScreenInit(Model *m);
extern void ScreenDestroy(Screen *s);

/* required for poll/select */
extern int ScreenFd(Screen *s);

/* to be called when the fd is ready */
extern void ScreenHandleInput(Screen *s);

/* used for model update notifications */
extern void ScreenUpdate(Screen *s);

extern bool ScreenIsPaused(Screen *s);

#endif
