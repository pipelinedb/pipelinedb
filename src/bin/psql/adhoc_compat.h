#ifndef ADHOC_COMPAT_H_028E0843
#define ADHOC_COMPAT_H_028E0843

typedef struct PAStringData
{
	char	   *data;
	int			len; // size 
	int			maxlen; // cap
} PAStringData;

extern int assert_enabled;

typedef PAStringData* PAString; 

void initPAString(PAString str);
void resetPAString(PAString str);
void appendPAStringString(PAString str, const char *s);

void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber);

void elog_start(const char *filename, int lineno, const char *funcname);
void elog_finish(int elevel, const char *fmt,...);

#endif
