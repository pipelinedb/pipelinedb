/*-------------------------------------------------------------------------
 *
 * cmsketch.c
 *	  Count-Min Sketch implementation.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/cmsketch.c
 *
 *-------------------------------------------------------------------------
 */
#include <limits.h>
#include <math.h>
#include "pipeline/cmsketch.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

/*
 * These values give us an error bound of 0.2% with a confidence of 99.5% and the size
 * of the resulting Count-Min Sketch structure is ~31k
 */
#define DEFAULT_P 0.995
#define DEFAULT_EPS 0.002
#define MURMUR_SEED 0x99496f1ddc863e6fL

CountMinSketch *
CountMinSketchCreateWithDAndW(uint32_t d, uint32_t w)
{
	CountMinSketch *cms = palloc0(sizeof(CountMinSketch) + (sizeof(uint32_t) * d * w));
	cms->d = d;
	cms->w = w;

	SET_VARSIZE(cms, CountMinSketchSize(cms));

	return cms;
}

CountMinSketch *
CountMinSketchCreateWithEpsAndP(float8 epsilon, float8 p)
{
	uint32_t w = (uint32_t) ceil(exp(1) / epsilon);
	uint32_t d = (uint32_t) ceil(log(1 / (1 - p)));
	return CountMinSketchCreateWithDAndW(d, w);
}

CountMinSketch *
CountMinSketchCreate(void)
{
	return CountMinSketchCreateWithEpsAndP(DEFAULT_EPS, DEFAULT_P);
}

void
CountMinSketchDestroy(CountMinSketch *cms)
{
	pfree(cms);
}

CountMinSketch *
CountMinSketchCopy(CountMinSketch *cms)
{
	Size size = CountMinSketchSize(cms);
	char *new = palloc(size);
	memcpy(new, (char *) cms, size);
	return (CountMinSketch *) new;
}

void
CountMinSketchAdd(CountMinSketch *cms, void *key, Size size, uint32_t count)
{
	/*
	 * Since we only have positive increments, we're using the conservative update
	 * variant which apparently has better accuracy--albeit moderately slower.
	 *
	 * http://dimacs.rutgers.edu/~graham/pubs/papers/cmencyc.pdf
	 */
	uint32_t min = UINT_MAX;
	uint32_t i;
	uint64_t hash[2];

	MurmurHash3_128(key, size, MURMUR_SEED, &hash);

	for (i = 0; i < cms->d; i++)
	{
		uint32_t start = i * cms->w;
		uint32_t j = (hash[0] + (i * hash[1])) % cms->w;
		min = Min(min, cms->table[start + j]);
	}

	for (i = 0; i < cms->d; i++)
	{
		uint32_t start = i * cms->w;
		uint32_t j = (hash[0] + (i * hash[1])) % cms->w;
		cms->table[start + j] = Max(cms->table[start + j], min + count);
	}

	cms->count += count;
}

uint32_t
CountMinSketchEstimateFrequency(CountMinSketch *cms, void *key, Size size)
{
	uint32_t count = UINT_MAX;
	uint32_t i;
	uint64_t hash[2];

	MurmurHash3_128(key, size, MURMUR_SEED, &hash);

	for (i = 0; i < cms->d; i++)
	{
		uint32_t start = i * cms->w;
		uint32_t j = (hash[0] + (i * hash[1])) % cms->w;
		count = Min(count, cms->table[start + j]);
	}

	return count;
}

uint64_t
CountMinSketchTotal(CountMinSketch *cms)
{
	return cms->count;
}

float8
CountMinSketchEstimateNormFrequency(CountMinSketch *cms, void *key, Size size)
{
	return 1.0 * CountMinSketchEstimateFrequency(cms, key, size) / cms->count;
}

CountMinSketch *
CountMinSketchMerge(CountMinSketch *result, CountMinSketch* incoming)
{
	uint32_t i;

	if (result->d != incoming->d || result->w != incoming->w)
		elog(ERROR, "cannot merge count-min sketches of different sizes");

	for (i = 0; i < result->d * result->w; i++)
		result->table[i] += incoming->table[i];

	result->count += incoming->count;

	return result;
}

Size
CountMinSketchSize(CountMinSketch *cms)
{
	return sizeof(CountMinSketch) + (sizeof(uint32_t) * cms->d * cms->w);
}
