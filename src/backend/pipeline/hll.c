/*-------------------------------------------------------------------------
 *
 * hll.c
 *	  HyperLogLog implementation
 *
 *
 * src/backend/pipeline/hll.h
 *
 *-------------------------------------------------------------------------
 */
#include "pipeline/hll.h"
#include "utils/palloc.h"


/*
 * MurmurHash - 64-bit version
 */
uint64
MurmurHash64A(const void *key, Size keysize, uint32 seed)
{
  const uint64 m = 0xc6a4a7935bd1e995;
  const int r = 47;
  const uint64 *data = (const uint64 *) key;
  const uint64 *end = data + (keysize / 8);
  const uint8 *data2;
  uint64 h = seed ^ keysize;

  while(data != end)
  {
		uint64 k = *data++;

		k *= m;
		k ^= k >> r;
		k *= m;

		h ^= k;
		h *= m;
  }

  data2 = (const uint8*) data;

  switch(keysize & 7)
  {
		case 7: h ^= (uint64) data2[6] << 48;
		case 6: h ^= (uint64) data2[5] << 40;
		case 5: h ^= (uint64) data2[4] << 32;
		case 4: h ^= (uint64) data2[3] << 24;
		case 3: h ^= (uint64) data2[2] << 16;
		case 2: h ^= (uint64) data2[1] << 8;
		case 1: h ^= (uint64) data2[0];
						h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;

}

/*
 * HLLCreate
 *
 * Create an empty HyperLogLog structure with the given parameters
 */
HyperLogLog *
HLLCreate(int p, int k)
{

	return NULL;
}
