/*-------------------------------------------------------------------------
 *
 * mirror_ringbuf.c
 *	  Functionality for mirror ringbuf
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pipeline/trigger/mirror_ringbuf.h"

static unsigned
floor_pow_two(unsigned x)
{
    x = x | (x >> 1);
    x = x | (x >> 2);
    x = x | (x >> 4);
    x = x | (x >> 8);
    x = x | (x >> 16);
    return x - (x >> 1);
}

static unsigned
ceil_pow_two(unsigned v)
{
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v++;

	return v;
}

/*
 * mirror_ringbuf_init
 *
 * Given a block of memory, and its size in bytes, initialize a mirrored
 * ring buffer within it.
 *
 * Note: This will make the buffer size a power of 2.
 * To calculate the optimum value to pass in, use mirror_ringbuf_calc_mem
 */
mirror_ringbuf *
mirror_ringbuf_init(void *ptr, int size)
{
	int avail = (size - (sizeof(mirror_ringbuf))) / 2;
	mirror_ringbuf *ringbuf = (mirror_ringbuf *) (ptr);

	Assert(avail > 0);

	ringbuf->size = floor_pow_two(avail);
	ringbuf->writen = 0;
	ringbuf->readn = 0;
	ringbuf->started = 0;

	return ringbuf;
}

/*
 * mirror_ringbuf_calc_mem
 *
 * Given a lower bound memory size in bytes, calculate the optimum size
 */
int
mirror_ringbuf_calc_mem(int lower_bound)
{
	return ceil_pow_two(lower_bound) * 2 + sizeof(mirror_ringbuf);
}

/*
 * mirror_ringbuf_avail_write
 *
 * Returns how many bytes are left for writing
 */
int
mirror_ringbuf_avail_write(mirror_ringbuf *buf)
{
	return buf->size - mirror_ringbuf_avail_read(buf);
}

/*
 * mirror_ringbuf_avail_read
 *
 * Returns how many bytes are available for reading
 */
int
mirror_ringbuf_avail_read(mirror_ringbuf *buf)
{
	if (!buf->started)
		return 0;

	return ((buf->writen - buf->readn) + buf->size) % buf->size;
}

/*
 * mirror_ringbuf_write
 *
 * append bytes to the buffer.
 * It is an error to write into a full buffer
 */
void
mirror_ringbuf_write(mirror_ringbuf *buf, const void *msg, int n)
{
	int l = 0;
	int r = 0;

	Assert(n <= mirror_ringbuf_avail_write(buf));

	l = Min(buf->size - buf->writen, n);
	r = n - l;

	/* write message handling any neccessary wrapping */
	memcpy(buf->bytes + buf->writen, msg, l);
	memcpy(buf->bytes, (char *) msg + l, r);

	/* write duplicate parts in the mirror */
	memcpy(buf->bytes + buf->size + buf->writen, msg, l);
	memcpy(buf->bytes + buf->size, (char *) msg + l, r);

	buf->writen = (buf->writen + n) % buf->size;

	if (!buf->started)
		buf->started = 1;
}

/*
 * mirror_ringbuf_read
 *
 * Consume bytes from the buffer by copying
 * It is an error to try to read more bytes than available
 */
void
mirror_ringbuf_read(mirror_ringbuf *buf, void *out, int n)
{
	Assert(n <= mirror_ringbuf_avail_read(buf));

	memcpy(out, buf->bytes + buf->readn, n);
	buf->readn = (buf->readn + n) % buf->size;
}

/*
 * mirror_ringbuf_consume
 *
 * Advance the read ptr by n
 * It is an error to try to read more bytes than available
 */
void
mirror_ringbuf_consume(mirror_ringbuf *buf, int n)
{
	Assert(n <= mirror_ringbuf_avail_read(buf));
	buf->readn = (buf->readn + n) % buf->size;
}

/*
 * mirror_ringbuf_peek
 *
 * Get the read ptr
 */
void *
mirror_ringbuf_peek(mirror_ringbuf *buf)
{
	return buf->bytes + buf->readn;
}
