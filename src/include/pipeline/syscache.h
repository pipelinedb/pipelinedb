#ifndef PIPELINE_SYSCACHE_H
#define PIPELINE_SYSCACHE_H

#include "access/attnum.h"
#include "access/htup.h"

enum PipelineSysCacheIdentifier
{
	PIPELINECOMBINEOID = 0,
	PIPELINECOMBINETRANSFNOID,
	PIPELINEQUERYID,
	PIPELINEQUERYRELID,
	PIPELINEQUERYMATRELID,
	PIPELINEQUERYOID,
	PIPELINESTREAMRELID,
	PIPELINESTREAMOID,
};

extern void InitPipelineSysCache(void);

extern HeapTuple SearchPipelineSysCache(int cacheId,
			   Datum key1, Datum key2, Datum key3, Datum key4);
extern Datum PipelineSysCacheGetAttr(int cacheId, HeapTuple tup,
				AttrNumber attributeNumber,
				bool *isNull);
#define SearchPipelineSysCache1(cacheId, key1) \
	SearchPipelineSysCache(cacheId, key1, 0, 0, 0)
#define SearchPipelineSysCache2(cacheId, key1, key2) \
	SearchPipelineSysCache(cacheId, key1, key2, 0, 0)

#endif
