/*-------------------------------------------------------------------------
 *
 * gcs.h
 *	  Interface for Golomb-coded Set support
 *
 * src/include/pipeline/gcs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_GCS_H
#define PIPELINE_GCS_H

#include "c.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

typedef struct BitReader
{
	uint8_t *bytes;
	uint32_t len;
	uint32_t accum;
	uint8_t naccum;
} BitReader;

extern BitReader *BitReaderCreate(uint8_t *bytes, uint32_t len);
extern uint32_t BitReaderRead(BitReader *r, uint8_t nbits);
extern void BitReaderDestroy(BitReader *r);

typedef struct BitWriter
{
	StringInfoData buf;
	uint64_t accum;
	uint8_t naccum;
} BitWriter;

extern BitWriter *BitWriterCreate(void);
extern void BitWriterWrite(BitWriter *w, uint8_t nbits, uint64_t val);
extern void BitWriterFlush(BitWriter *w);
extern void BitWriterDestroy(BitWriter *w);

/*
 * All GolombCodedSets MUST be compressed before writing them out to disk
 * as bytea structures.
 */
typedef struct GolombCodedSet
{
	uint32	vl_len_;
	uint32_t p;
	uint32_t n;
	List *vals; /* for dirty storage */
	uint32_t nvals;
	uint32_t idx[64][2]; /* TODO(usmanm): Index the compressed array for fast containment checks */
	uint32_t blen;
	uint8_t b[1];
} GolombCodedSet;

extern GolombCodedSet *GolombCodedSetCreateWithPAndN(float8 p, uint32_t n);
extern GolombCodedSet *GolombCodedSetCreate(void);
extern void GolombCodedSetDestroy(GolombCodedSet *gcs);

extern GolombCodedSet *GolombCodedSetCopy(GolombCodedSet *gcs);
extern void GolombCodedSetAdd(GolombCodedSet *gcs, void *key, Size size);
extern bool GolombCodedSetContains(GolombCodedSet *gcs, void *key, Size size);
extern GolombCodedSet *GolombCodedSetUnion(GolombCodedSet *result, GolombCodedSet *incoming);
extern GolombCodedSet *GolombCodedSetIntersection(GolombCodedSet *result, GolombCodedSet *incoming);
extern float8 GolombCodedSetFillRatio(GolombCodedSet *gcs);
extern Size GolombCodedSetSize(GolombCodedSet *gcs);
extern GolombCodedSet *GolombCodedSetCompress(GolombCodedSet *gcs);

typedef struct GCSReader
{
	BitReader *bit_reader;
	GolombCodedSet *gcs;
	uint32_t logp;
	int32_t prev;
} GCSReader;

extern GCSReader *GCSReaderCreate(GolombCodedSet *gcs);
extern int32_t GCSReaderNext(GCSReader *r);
extern void GCSReaderDestroy(GCSReader *r);

typedef struct GCSWriter
{
	BitWriter *bit_writer;
	GolombCodedSet *gcs;
	uint32_t logp;
	uint32_t nvals;
} GCSWriter;

extern GCSWriter *GCSWriterCreate(GolombCodedSet *gcs);
extern void GCSWriterWrite(GCSWriter *w, int32_t val);
extern void GCSWriterFlush(GCSWriter *w);
extern GolombCodedSet *GCSWriterGenerateGCS(GCSWriter *w);
extern void GCSWriterDestroy(GCSWriter *w);

#endif
