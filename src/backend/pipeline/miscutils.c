/*
 * Miscellaneous utilities
 *
 * src/backend/pipeline/miscutils.c
 */
#include "postgres.h"
#include "pipeline/miscutils.h"
#include "port.h"

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
	char *buf = palloc(len);
	while (len)
		buf[--len] = rand() % 256;
	return buf;
}
