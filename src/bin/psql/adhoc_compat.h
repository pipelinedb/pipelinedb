#ifndef ADHOC_COMPAT_H_028E0843
#define ADHOC_COMPAT_H_028E0843

#include <unistd.h>

void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber);

void elog_start(const char *filename, int lineno, const char *funcname);
void elog_finish(int elevel, const char *fmt,...);

typedef struct FlexString {
	char* buf;
	size_t n;
	size_t cap;
} FlexString;

void init_flex(FlexString* f);
void destroy_flex(FlexString* f);
void reset_flex(FlexString* f);
void append_flex(FlexString* f, const char* s, size_t n);
void cleanup_flex(FlexString* f);

size_t length_flex(FlexString* f);

#endif
