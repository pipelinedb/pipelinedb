/*-------------------------------------------------------------------------
 *
 * mirror_ringbuf.h
 *	  Interface for mirror ringbuf
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#ifndef MIRROR_RINGBUF_H
#define MIRROR_RINGBUF_H

/*
 * The purpose of the mirror_ringbuf is to make rebuffering operations easier.
 *
 * Any writes to the circular buffer are duplicated to the right half of the
 * memory region.
 *
 * For example - an 8 element circular buffer that has wrapped would
 * look like this in memory. This allows readers to grab pointers to
 * contiguous data.
 *
 *  <-------------- bytes[] ------------>
 *
 *                     |
 * [ 3 4 5 6 7 0 1 2 ] | [ 3 4 5 6 7 0 1 2 ]
 *                     |
 *
 */
typedef struct mirror_ringbuf
{
	int size;
	int writen;
	int readn;
	int started;

	char bytes[];
} mirror_ringbuf;

mirror_ringbuf *
mirror_ringbuf_init(void *ptr, int size);

extern int mirror_ringbuf_calc_mem(int size);

extern int mirror_ringbuf_avail_write(mirror_ringbuf *buf);
extern int mirror_ringbuf_avail_read(mirror_ringbuf *buf);

extern void mirror_ringbuf_write(mirror_ringbuf *buf, const void *ptr, int n);
extern void mirror_ringbuf_read(mirror_ringbuf *buf, void *ptr, int n);

extern void *mirror_ringbuf_peek(mirror_ringbuf *buf);
extern void mirror_ringbuf_consume(mirror_ringbuf *buf, int n);

#endif
