
/* Opaque type to use in standardizer cache API */
typedef void *StdCache;

StdCache GetStdCache(FunctionCallInfo fcinfo);
bool IsInStdCache(StdCache STDCache, char *lextab, char *gaztab, char *rultab);
void AddToStdCache(StdCache cache, char *lextab, char *gaztab, char *rultab);
STANDARDIZER *GetStdFromStdCache(StdCache STDCache,  char *lextab, char *gaztab, char *rultab);

/*
 * This is the only interface external code should be calling
 * it will get the standardizer out of the cache, or
 * it will create a new one and save it in the cache
*/
STANDARDIZER *GetStdUsingFCInfo(FunctionCallInfo fcinfo, char *lextab, char *gaztab, char *rultab);

