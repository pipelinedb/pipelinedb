/*
 * Miscellaneous utilities
 *
 * src/backend/pipeline/miscutils.c
 */
#include "postgres.h"
#include "pipeline/miscutils.h"
#include "port.h"

#define MURMUR_SEED 0xadc83b19ULL

void
append_suffix(char *str, char *suffix, int max_len)
{
	strcpy(&str[Min(strlen(str), max_len - strlen(suffix))], suffix);
}

int
skip_substring(char *str, char* substr, int start)
{
	while(pg_strncasecmp(substr, &str[start++], strlen(substr)) != 0 &&
			start < strlen(str) - strlen(substr));

	if (start == strlen(str) - strlen(substr))
		return -1;

	return start + strlen(substr);
}

char *
random_hex(int len)
{
	int i = 0;
	char *buf = palloc(len + 1);
	while (i < len) {
		sprintf(&buf[i++], "%x", rand() % 16);
	}
	return buf;
}

/*
 * MurmurHash - 64-bit version
 */
uint64_t
MurmurHash64AWithSeed(const void *key, Size keysize, uint64_t seed)
{
  static const uint64_t m = 0xc6a4a7935bd1e995;
  static const int r = 47;

  const uint8_t *data = (const uint8_t *) key;
  const uint8_t *end = data + (keysize - (keysize & 7));
  uint64_t h = seed ^ (keysize * m);

  while (data != end)
  {
		uint64 k = *((uint64 *) data);

		k *= m;
		k ^= k >> r;
		k *= m;

		h ^= k;
		h *= m;
		data += 8;
  }

  switch(keysize & 7)
  {
		case 7: h ^= (uint64) data[6] << 48;
		case 6: h ^= (uint64) data[5] << 40;
		case 5: h ^= (uint64) data[4] << 32;
		case 4: h ^= (uint64) data[3] << 24;
		case 3: h ^= (uint64) data[2] << 16;
		case 2: h ^= (uint64) data[1] << 8;
		case 1: h ^= (uint64) data[0];
						h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

uint64_t
MurmurHash64A(const void *key, Size keysize)
{
	return MurmurHash64AWithSeed(key, keysize, MURMUR_SEED);
}
