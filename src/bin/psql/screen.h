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

extern int ScreenFd(Screen *s);
extern void ScreenHandleInput(Screen *s);

extern void ScreenUpdate(Screen *s);
extern bool ScreenIsPaused(Screen *s);

#endif
