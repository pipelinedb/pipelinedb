/*-------------------------------------------------------------------------
 *
 * cmsketch.h
 *	  Interface for Count-Min Sketch support
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_CMSKETCH_H
#define PIPELINE_CMSKETCH_H

#include "c.h"

/*
 * Unfortunately we can't do top-K with Count-Min Sketch for continuous
 * queries because it will require us to store O(n) values. This isn't a
 * problem in the case when Count-Min Sketch structures are not being
 * merged in which case only O(1) values need to be stored.
 */
typedef struct CountMinSketch
{
	uint32	vl_len_;
	uint64_t count;
	uint32_t d;
	uint32_t w;
	uint32_t table[1];
} CountMinSketch;

extern CountMinSketch *CountMinSketchCreateWithDAndW(uint32_t d, uint32_t w);
extern CountMinSketch *CountMinSketchCreateWithEpsAndP(float8 epsilon, float8 p);
extern CountMinSketch *CountMinSketchCreate(void);
extern void CountMinSketchDestroy(CountMinSketch *cms);

extern CountMinSketch *CountMinSketchCopy(CountMinSketch *cms);
extern void CountMinSketchAdd(CountMinSketch *cms, void *key, Size size, uint32_t count);
extern uint32_t CountMinSketchEstimateFrequency(CountMinSketch *cms, void *key, Size size);
extern float8 CountMinSketchEstimateNormFrequency(CountMinSketch *cms, void *key, Size size);
extern uint64_t CountMinSketchTotal(CountMinSketch *cms);
extern CountMinSketch *CountMinSketchMerge(CountMinSketch *result, CountMinSketch* incoming);
extern Size CountMinSketchSize(CountMinSketch *cms);

#endif
