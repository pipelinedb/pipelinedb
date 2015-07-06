

/* PostgreSQL headers */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "access/hash.h"
#include "utils/hsearch.h"
#include "funcapi.h"
#include "catalog/pg_type.h"

/* standardizer headers */
#undef DEBUG
//#define DEBUG 1

#include "pagc_api.h"
#include "pagc_std_api.h"
#include "std_pg_hash.h"

/* C headers */
#include <sys/time.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#ifdef DEBUG
#define SET_TIME(a) gettimeofday(&(a), NULL)
#define ELAPSED_T(a,b) \
    elapsed = (b.tv_sec - a.tv_sec)*1000.0; \
    elapsed += (b.tv_usec - a.tv_usec)/1000.0;
#else
#define SET_TIME(a) do { ; } while (0)
#define ELAPSED_T(a,b) do { ; } while (0)
#endif

#define MAX_RULE_LENGTH 128
#define TUPLIMIT 1000

#define STD_CACHE_ITEMS 4
#define STD_BACKEND_HASH_SIZE 16

static HTAB* StdHash = NULL;


typedef struct
{
    char *lextab;
    char *gaztab;
    char *rultab;
    STANDARDIZER *std;
    MemoryContext std_mcxt;
}
StdCacheItem;

typedef struct
{
    StdCacheItem StdCache[STD_CACHE_ITEMS];
    int NextSlot;
    MemoryContext StdCacheContext;
}
StdPortalCache;

typedef struct
{
    MemoryContext context;
    STANDARDIZER *std;
}
StdHashEntry;

typedef struct lex_columns
{
    int seq;
    int word;
    int stdword;
    int token;
} lex_columns_t;

typedef struct rules_columns
{
    int rule;
} rules_columns_t;



/* Memory context hash table function prototypes */
uint32 mcxt_ptr_hash_std(const void *key, Size keysize);
static void CreateStdHash(void);
static void AddStdHashEntry(MemoryContext mcxt, STANDARDIZER *std);
static StdHashEntry *GetStdHashEntry(MemoryContext mcxt);
static void DeleteStdHashEntry(MemoryContext mcxt);

/* Memory context cache function prototypes */
static void StdCacheInit(MemoryContext context);
static void StdCacheReset(MemoryContext context);
static void StdCacheDelete(MemoryContext context);
static bool StdCacheIsEmpty(MemoryContext context);
static void StdCacheStats(MemoryContext context, int level);
#ifdef MEMORY_CONTEXT_CHECKING
static void StdCacheCheck(MemoryContext context);
#endif

static bool IsInStdPortalCache(StdPortalCache *STDCache,  char *lextab, char *gaztab, char *rultab);
static STANDARDIZER *GetStdFromPortalCache(StdPortalCache *STDCache,  char *lextab, char *gaztab, char *rultab);
static void AddToStdPortalCache(StdPortalCache *STDCache, char *lextab, char *gaztab, char *rultab);
static StdPortalCache *GetStdPortalCache(FunctionCallInfo fcinfo);


/* standardizer api functions */

static STANDARDIZER *CreateStd(char *lextab, char *gaztab, char *rultab);
static int parse_rule(char *buf, int *rule);
static int fetch_lex_columns(SPITupleTable *tuptable, lex_columns_t *lex_cols);
static int tableNameOk(char *t);
static int load_lex(LEXICON *lex, char *tabname);
static int fetch_rules_columns(SPITupleTable *tuptable, rules_columns_t *rules_cols);
static int load_rules(RULES *rules, char *tabname);


/* Memory context definition must match the current version of PostgreSQL */
static MemoryContextMethods StdCacheContextMethods =
{
    NULL,
    NULL,
    NULL,
    StdCacheInit,
    StdCacheReset,
    StdCacheDelete,
    NULL,
    StdCacheIsEmpty,
    StdCacheStats
#ifdef MEMORY_CONTEXT_CHECKING
    , StdCacheCheck
#endif
};


static void
StdCacheInit(MemoryContext context)
{
    /* NOP - initialized when first used. */
}


static void
StdCacheReset(MemoryContext context)
{
    // NOP - Seems to be a required function
}


static void
StdCacheDelete(MemoryContext context)
{
    StdHashEntry *she;

    DBG("Enter: StdCacheDelete");
    /* lookup the hash entry in the global hash table
       so we can free it */
    she = GetStdHashEntry(context);

    if (!she)
        elog(ERROR, "StdCacheDelete: Trying to delete non-existant hash entry object with MemoryContext key (%p)", (void *)context);

    DBG("deleting std object (%p) with MemoryContext key (%p)", she->std, context);

    if (she->std)
        std_free(she->std);

    DeleteStdHashEntry(context);
}


static bool
StdCacheIsEmpty(MemoryContext context)
{
    // always return false - another required function
    return FALSE;
}


static void
StdCacheStats(MemoryContext context, int level)
{
    // another required function
    fprintf(stderr, "%s: STANDARDIZER context\n", context->name);
}


#ifdef MEMORY_CONTEXT_CHECKING
static void
StdCacheCheck(MemoryContext context)
{
    // NOP - another reuired function
}
#endif


uint32
mcxt_ptr_hash_std(const void *key, Size keysize)
{
    uint32 hashval;
    hashval = DatumGetUInt32(hash_any(key, keysize));
    return hashval;
}


static void
CreateStdHash(void)
{
    HASHCTL ctl;
    
    ctl.keysize = sizeof(MemoryContext);
    ctl.entrysize = sizeof(StdHashEntry);
    ctl.hash = mcxt_ptr_hash_std;

    StdHash = hash_create("PAGC Address Standardizer Backend MemoryContext Hash", STD_BACKEND_HASH_SIZE, &ctl, (HASH_ELEM | HASH_FUNCTION));
    DBG("CreateStdHash: created StdHash (%p)", StdHash);
}


static void
AddStdHashEntry(MemoryContext mcxt, STANDARDIZER *std)
{
    bool found;
    void **key;
    StdHashEntry *he;

    DBG("Enter: AddStdHashEntry(mcxt=%p, std=%p)", mcxt, std);
    /* The hash key is the MemoryContext pointer */
    key = (void *)&mcxt;

    he = (StdHashEntry *) hash_search(StdHash, key, HASH_ENTER, &found);
    DBG("AddStdHashEntry: he=%p, found=%d", he, found);
    if (!found) {
        DBG("&he->context=%p", &he->context);
        he->context = mcxt;
        DBG("&he->std=%p", &he->std);
        he->std = std;
        DBG("Leaving AddStdHashEntry");
    }
    else {
        elog(ERROR, "AddStdHashEntry: This memory context is already in use! (%p)", (void *)mcxt);
    }
}

static StdHashEntry *
GetStdHashEntry(MemoryContext mcxt)
{
    void **key;
    StdHashEntry *he;

    DBG("Enter: GetStdHashEntry");
    key = (void *)&mcxt;
    he = (StdHashEntry *) hash_search(StdHash, key, HASH_FIND, NULL);
    return he;
}


static void
DeleteStdHashEntry(MemoryContext mcxt)
{
    void **key;
    StdHashEntry *he;

    DBG("Enter: DeleteStdHashEntry");
    key = (void *)&mcxt;
    he = (StdHashEntry *) hash_search(StdHash, key, HASH_REMOVE, NULL);
    if (!he)
        elog(ERROR, "DeleteStdHashEntry: There was an error removing the STD object from this MemoryContext (%p)", (void *)mcxt);

    he->std = NULL;
}


/* public api */
bool
IsInStdCache(StdCache STDCache, char *lextab, char *gaztab, char *rultab) {
    return IsInStdPortalCache((StdPortalCache *) STDCache, lextab, gaztab, rultab);
}


static bool
IsInStdPortalCache(StdPortalCache *STDCache,  char *lextab, char *gaztab, char *rultab)
{
    int i;

    DBG("Enter: IsInStdPortalCache");
    for (i=0; i<STD_CACHE_ITEMS; i++) {
        StdCacheItem *ci = &STDCache->StdCache[i];
        if (ci->lextab && !strcmp(ci->lextab, lextab) &&
            ci->lextab && !strcmp(ci->gaztab, gaztab) &&
            ci->lextab && !strcmp(ci->rultab, rultab))
                return TRUE;
    }

    return FALSE;
}


/* public api */
STANDARDIZER *
GetStdFromStdCache(StdCache STDCache,  char *lextab, char *gaztab, char *rultab) {
    return GetStdFromPortalCache((StdPortalCache *) STDCache, lextab, gaztab, rultab);
}


static STANDARDIZER *
GetStdFromPortalCache(StdPortalCache *STDCache,  char *lextab, char *gaztab, char *rultab)
{
    int i;

    DBG("Enter: GetStdFromPortalCache");
    for (i=0; i<STD_CACHE_ITEMS; i++) {
        StdCacheItem *ci = &STDCache->StdCache[i];
        if (ci->lextab && !strcmp(ci->lextab, lextab) &&
            ci->lextab && !strcmp(ci->gaztab, gaztab) &&
            ci->lextab && !strcmp(ci->rultab, rultab))
                return STDCache->StdCache[i].std;
    }

    return NULL;
}


static void
DeleteNextSlotFromStdCache(StdPortalCache *STDCache)
{
    MemoryContext old_context;

    DBG("Enter: DeleteNextSlotFromStdCache");
    if (STDCache->StdCache[STDCache->NextSlot].std != NULL) {
        StdCacheItem *ce = &STDCache->StdCache[STDCache->NextSlot];
        DBG("Removing STD cache entry ('%s', '%s', '%s') index %d", ce->lextab, ce->gaztab, ce->rultab, STDCache->NextSlot);

        /* zero out the entries and free the memory context
           We will get a callback to free the std object.
        */
        old_context = MemoryContextSwitchTo(STDCache->StdCacheContext);
        MemoryContextDelete(ce->std_mcxt);
        pfree(ce->lextab);
        ce->lextab = NULL;
        pfree(ce->gaztab);
        ce->gaztab = NULL;
        pfree(ce->rultab);
        ce->rultab = NULL;
        ce->std    = NULL;
        MemoryContextSwitchTo(old_context);
    }
}


/* public api */
void
AddToStdCache(StdCache cache, char *lextab, char *gaztab, char *rultab) {
    AddToStdPortalCache((StdPortalCache *) cache, lextab, gaztab, rultab);
}


static void
AddToStdPortalCache(StdPortalCache *STDCache, char *lextab, char *gaztab, char *rultab)
{
    MemoryContext STDMemoryContext;
    MemoryContext old_context;
    STANDARDIZER *std = NULL;

    DBG("Enter: AddToStdPortalCache");
    std = CreateStd(lextab, gaztab, rultab);
    if (!std)
        elog(ERROR,
            "AddToStdPortalCache: could not create address standardizer for '%s', '%s', '%s'", lextab, gaztab, rultab);

    /* if the NextSlot in the cache is used, then delete it */
    if (STDCache->StdCache[STDCache->NextSlot].std != NULL) {
#ifdef DEBUG
        StdCacheItem *ce = &STDCache->StdCache[STDCache->NextSlot];
        DBG("Removing item from STD cache ('%s', '%s', '%s') index %d", ce->lextab, ce->gaztab, ce->rultab, STDCache->NextSlot);
#endif
        DeleteNextSlotFromStdCache(STDCache);
    }

    DBG("Adding item to STD cache ('%s', '%s', '%s') index %d", lextab, gaztab, rultab, STDCache->NextSlot);

    STDMemoryContext = MemoryContextCreate(T_AllocSetContext, 8192,
                                           &StdCacheContextMethods,
                                           STDCache->StdCacheContext,
                                           "PAGC STD Memory Context");

    /* Create the backend hash if it doesn't already exist */
    DBG("Check if StdHash exists (%p)", StdHash);
    if (!StdHash)
        CreateStdHash();

    /*
     * Add the MemoryContext to the backend hash so we can
     * clean up upon portal shutdown
     */
    DBG("Adding standardizer obj (%p) to hash table with MemoryContext key (%p)", std, STDMemoryContext);

    AddStdHashEntry(STDMemoryContext, std);

    /* change memory contexts so the pstrdup are allocated in the
     * context of this cache item. They will be freed when the
     * cache item is deleted.
    */
    DBG("AddToStdPortalCache: changing memory context to %p", STDCache->StdCacheContext);
    old_context = MemoryContextSwitchTo(STDCache->StdCacheContext);
    DBG("  old_context= %p", old_context);
    STDCache->StdCache[STDCache->NextSlot].lextab = pstrdup(lextab);
    DBG("  pstrdup(lextab) completed");
    STDCache->StdCache[STDCache->NextSlot].gaztab = pstrdup(gaztab);
    DBG("  pstrdup(gaztab) completed");
    STDCache->StdCache[STDCache->NextSlot].rultab = pstrdup(rultab);
    DBG("  pstrdup(rultab) completed");
    MemoryContextSwitchTo(old_context);
    DBG(" changed memory context to %p", old_context);

    STDCache->StdCache[STDCache->NextSlot].std = std;
    STDCache->StdCache[STDCache->NextSlot].std_mcxt = STDMemoryContext;
    STDCache->NextSlot = (STDCache->NextSlot + 1) % STD_CACHE_ITEMS;
    DBG("STDCache->NextSlot=%d", STDCache->NextSlot);
}


/* pubilc api */
StdCache
GetStdCache(FunctionCallInfo fcinfo) {
    return (StdCache) GetStdPortalCache(fcinfo);
}


static StdPortalCache *
GetStdPortalCache(FunctionCallInfo fcinfo)
{
    StdPortalCache *STDCache;

    DBG("Enter: GetStdPortalCache");
    /* create it if we don't already have one for this portal */
    if (fcinfo->flinfo->fn_extra == NULL) {
        MemoryContext old_context;

        old_context = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        STDCache = palloc(sizeof(StdPortalCache));
        MemoryContextSwitchTo(old_context);

        if (STDCache) {
            int i;

            DBG("Allocating STDCache for portal with STD MemoryContext (%p)", fcinfo->flinfo->fn_mcxt);
            /* initial the cache items */
            for (i=0; i<STD_CACHE_ITEMS; i++) {
                STDCache->StdCache[i].lextab   = NULL;
                STDCache->StdCache[i].gaztab   = NULL;
                STDCache->StdCache[i].rultab   = NULL;
                STDCache->StdCache[i].std      = NULL;
                STDCache->StdCache[i].std_mcxt = NULL;
            }
            STDCache->NextSlot = 0;
            STDCache->StdCacheContext = fcinfo->flinfo->fn_mcxt;

            /* Store the pointer in fcinfo->flinfo->fn_extra */
            fcinfo->flinfo->fn_extra = STDCache;
        }
    }
    else {
        /* Use the existing cache */
        STDCache = fcinfo->flinfo->fn_extra;
    }

    return STDCache;
}

/* public api */
STANDARDIZER *
GetStdUsingFCInfo(FunctionCallInfo fcinfo, char *lextab, char *gaztab, char *rultab)
{
    STANDARDIZER *std;
    StdCache *std_cache = NULL;

    DBG("GetStdUsingFCInfo: calling GetStdCache(fcinfo)");
    std_cache = GetStdCache(fcinfo);
    if (!std_cache)
        return NULL;

    DBG("GetStdUsingFCInfo: calling IsInStdCache(std_cache, lextab, gaztab, rultab)");
    if (!IsInStdCache(std_cache, lextab, gaztab, rultab)) {
        DBG("GetStdUsingFCInfo: calling AddToStdCache(std_cache, lextab, gaztab, rultab)");
        AddToStdCache(std_cache, lextab, gaztab, rultab);
    }

    DBG("GetStdUsingFCInfo: GetStdFromStdCache(std_cache, lextab, gaztab, rultab)");
    std = GetStdFromStdCache(std_cache, lextab, gaztab, rultab);

    return std;
}


static STANDARDIZER *
CreateStd(char *lextab, char *gaztab, char *rultab)
{
    STANDARDIZER        *std;
    LEXICON             *lex;
    LEXICON             *gaz;
    RULES               *rules;
    int                  err;
    int                  SPIcode;

    DBG("Enter: CreateStd");
    SPIcode = SPI_connect();
    if (SPIcode != SPI_OK_CONNECT) {
        elog(ERROR, "CreateStd: couldn't open a connection to SPI");
    }

    std = std_init();
    if (!std)
        elog(ERROR, "CreateStd: could not allocate memory (std)");

    lex = lex_init(std->err_p);
    if (!lex) {
        std_free(std);
        SPI_finish();
        elog(ERROR, "CreateStd: could not allocate memory (lex)");
    }

    err = load_lex(lex, lextab);
    if (err == -1) {
        lex_free(lex);
        std_free(std);
        SPI_finish();
        elog(ERROR, "CreateStd: failed to load '%s' for lexicon", lextab);
    }

    gaz = lex_init(std->err_p);
    if (!gaz) {
        lex_free(lex);
        std_free(std);
        SPI_finish();
        elog(ERROR, "CreateStd: could not allocate memory (gaz)");
    }

    err = load_lex(gaz, gaztab);
    if (err == -1) {
        lex_free(gaz);
        lex_free(lex);
        std_free(std);
        SPI_finish();
        elog(ERROR, "CreateStd: failed to load '%s' for gazeteer", gaztab);
    }

    rules = rules_init(std->err_p);
    if (!rules) {
        lex_free(gaz);
        lex_free(lex);
        std_free(std);
        SPI_finish();
        elog(ERROR, "CreateStd: could not allocate memory (rules)");
    }

    err = load_rules(rules, rultab);
    if (err == -1) {
        rules_free(rules);
        lex_free(gaz);
        lex_free(lex);
        std_free(std);
        SPI_finish();
        elog(ERROR, "CreateStd: failed to load '%s' for rules", rultab);
    }

    std_use_lex(std, lex);
    std_use_gaz(std, gaz);
    std_use_rules(std, rules);
    std_ready_standardizer(std);

    SPI_finish();

    return std;
}


static int parse_rule(char *buf, int *rule)
{
    int nr = 0;
    int *r = rule;
    char *p = buf;
    char *q;


    while (1) {
        *r = strtol( p, &q, 10 );
        if (p == q) break;
        p = q;
        nr++;
        r++;
        if (nr > MAX_RULE_LENGTH) return -1;
    }

    return nr;
}


#define FETCH_COL(TRGT,NAME,NAME2) \
    TRGT->NAME = SPI_fnumber(SPI_tuptable->tupdesc,NAME2);\
    if (TRGT->NAME == SPI_ERROR_NOATTRIBUTE) err++;

#define CHECK_TYP(TRGT,NAME,TYPE) \
    if (SPI_gettypeid(SPI_tuptable->tupdesc, TRGT->NAME) != TYPE) {\
        DBG("CHECK_TYP: expecting %d, got: %d", TYPE, SPI_gettypeid(SPI_tuptable->tupdesc, TRGT->NAME));\
        err++;\
    }

#define GET_INT_FROM_TUPLE(TRGT,WHICH,NULLMSG) \
    binval = SPI_getbinval(tuple, tupdesc, WHICH, &isnull);\
    if (isnull) { \
        elog(NOTICE, NULLMSG); \
        return -1; \
    } \
    TRGT = DatumGetInt32(binval);

#define GET_TEXT_FROM_TUPLE(TRGT,WHICH) \
    TRGT = DatumGetCString(SPI_getvalue(tuple, tupdesc, WHICH));


static int fetch_lex_columns(SPITupleTable *tuptable, lex_columns_t *lex_cols)
{
    int err = 0;
    FETCH_COL(lex_cols,seq,"seq");
    FETCH_COL(lex_cols,word,"word");
    FETCH_COL(lex_cols,stdword,"stdword");
    FETCH_COL(lex_cols,token,"token");
    if (err) {
        elog(NOTICE, "lexicon queries must return columns 'seq', 'word', 'stdword' and 'token'");
        return -1;
    }
    CHECK_TYP(lex_cols,seq,INT4OID);
    CHECK_TYP(lex_cols,word,TEXTOID);
    CHECK_TYP(lex_cols,stdword,TEXTOID);
    CHECK_TYP(lex_cols,token,INT4OID);
    if (err) {
        elog(NOTICE, "lexicon column types must be: 'seq' int4, 'word' text, 'stdword' text, and 'token' int4");
        return -1;
    }
    return 0;
}

/* snitize table names, leave '.' for schema */

static int tableNameOk(char *t)
{
    while (*t != '\0') {
        if (!(isalnum(*t) || *t == '_' || *t == '.' || *t == '"'))
            return 0;
        t++;
    }
    return 1;
}

static int load_lex(LEXICON *lex, char *tab)
{
    int ret;
    SPIPlanPtr SPIplan;
    Portal SPIportal;
    bool moredata = TRUE;
#ifdef DEBUG
    struct timeval t1, t2;
    double elapsed;
#endif
    char *sql;

    int ntuples;
    int total_tuples = 0;

    lex_columns_t lex_columns = {seq: -1, word: -1, stdword: -1, token: -1};

    int seq;
    char *word;
    char *stdword;
    int token;

    DBG("start load_lex\n");
    SET_TIME(t1);

    if (!tab || !strlen(tab)) {
        elog(NOTICE, "load_lex: rules table is not usable");
        return -1;
    }
    if (!tableNameOk(tab)) {
        elog(NOTICE, "load_lex: lex and gaz table names may only be alphanum and '.\"_' characters (%s)", tab);
        return -1;
    }
    sql = SPI_palloc(strlen(tab)+65);
    strcpy(sql, "select seq, word, stdword, token from ");
    strcat(sql, tab);
    strcat(sql, " order by id ");

    /* get the sql for the lexicon records and prepare the query */
    SPIplan = SPI_prepare(sql, 0, NULL);
    if (SPIplan == NULL) {
        elog(NOTICE, "load_lex: couldn't create query plan for the lex data via SPI (%s)", sql);
        return -1;
    }

    /* get the sql for the lexicon records and prepare the query */
    SPIplan = SPI_prepare(sql, 0, NULL);
    if (SPIplan == NULL) {
        elog(NOTICE, "load_lex: couldn't create query plan for the lexicon data via SPI");
        return -1;
    }

    if ((SPIportal = SPI_cursor_open(NULL, SPIplan, NULL, NULL, true)) == NULL) {
        elog(NOTICE, "load_lex: SPI_cursor_open('%s') returns NULL", sql);
        return -1;
    }

    while (moredata == TRUE) {
        //DBG("calling SPI_cursor_fetch");
        SPI_cursor_fetch(SPIportal, TRUE, TUPLIMIT);

        if (SPI_tuptable == NULL) {
            elog(NOTICE, "load_lex: SPI_tuptable is NULL");
            return -1;
        }

        if (lex_columns.seq == -1) {
            ret = fetch_lex_columns(SPI_tuptable, &lex_columns);
            if (ret)
                return ret;
        }

        ntuples = SPI_processed;
        //DBG("Reading edges: %i - %i", total_tuples, total_tuples+ntuples);
        total_tuples += ntuples;

        if (ntuples > 0) {
            int t;
            Datum binval;
            bool isnull;
            SPITupleTable *tuptable = SPI_tuptable;
            TupleDesc tupdesc = SPI_tuptable->tupdesc;

            for (t = 0; t < ntuples; t++) {
                //if (t%100 == 0) { DBG("    t: %i", t); }
                HeapTuple tuple = tuptable->vals[t];
                GET_INT_FROM_TUPLE(seq,lex_columns.seq,"load_lex: seq contains a null value");
                GET_TEXT_FROM_TUPLE(word,lex_columns.word);
                GET_TEXT_FROM_TUPLE(stdword,lex_columns.stdword);
                GET_INT_FROM_TUPLE(token,lex_columns.token,"load_lex: token contains a null value");
                lex_add_entry(lex, seq, word, stdword, token);
            }
            //DBG("calling SPI_freetuptable");
            SPI_freetuptable(tuptable);
            //DBG("back from SPI_freetuptable");
        }
        else
            moredata = FALSE;

    }

    SET_TIME(t2);
    ELAPSED_T(t1, t2);
    DBG("Time to read %i lexicon records: %.1f ms.", total_tuples, elapsed);

    return 0;
}

static int fetch_rules_columns(SPITupleTable *tuptable, rules_columns_t *rules_cols)
{
    int err = 0;
    FETCH_COL(rules_cols,rule,"rule");
    if (err) {
        elog(NOTICE, "rules queries must return column 'rule'");
        return -1;
    }
    CHECK_TYP(rules_cols,rule,TEXTOID);
    if (err) {
        elog(NOTICE, "rules column type must be: 'rule' text");
        return -1;
    }
    return 0;
}

static int load_rules(RULES *rules, char *tab)
{
    int ret;
    SPIPlanPtr SPIplan;
    Portal SPIportal;
    bool moredata = TRUE;
#ifdef DEBUG
    struct timeval t1, t2;
    double elapsed;
#endif
    char *sql;

    int rule_arr[MAX_RULE_LENGTH];

    int ntuples;
    int total_tuples = 0;

    rules_columns_t rules_columns = {rule: -1};

    char *rule;

    DBG("start load_rules\n");
    SET_TIME(t1);

    if (!tab || !strlen(tab)) {
        elog(NOTICE, "load_rules: rules table is not usable");
        return -1;
    }
    if (!tableNameOk(tab)) {
        elog(NOTICE, "load_rules: rules table name may only be alphanum and '.\"_' characters (%s)", tab);
        return -1;
    }
    sql = SPI_palloc(strlen(tab)+35);
    strcpy(sql, "select rule from ");
    strcat(sql, tab);
    strcat(sql, " order by id ");

    /* get the sql for the lexicon records and prepare the query */
    SPIplan = SPI_prepare(sql, 0, NULL);
    if (SPIplan == NULL) {
        elog(NOTICE, "load_rules: couldn't create query plan for the rule data via SPI (%s)", sql);
        return -1;
    }

    if ((SPIportal = SPI_cursor_open(NULL, SPIplan, NULL, NULL, true)) == NULL) {
        elog(NOTICE, "load_rules: SPI_cursor_open('%s') returns NULL", sql);
        return -1;
    }

    while (moredata == TRUE) {
        //DBG("calling SPI_cursor_fetch");
        SPI_cursor_fetch(SPIportal, TRUE, TUPLIMIT);

        if (SPI_tuptable == NULL) {
            elog(NOTICE, "load_rules: SPI_tuptable is NULL");
            return -1;
        }

        if (rules_columns.rule == -1) {
            ret = fetch_rules_columns(SPI_tuptable, &rules_columns);
            if (ret)
                return ret;
        }

        ntuples = SPI_processed;
        //DBG("Reading edges: %i - %i", total_tuples, total_tuples+ntuples);

        if (ntuples > 0) {
            int t;
            SPITupleTable *tuptable = SPI_tuptable;
            TupleDesc tupdesc = SPI_tuptable->tupdesc;

            for (t = 0; t < ntuples; t++) {
                int nr;
                //if (t%100 == 0) { DBG("    t: %i", t); }
                HeapTuple tuple = tuptable->vals[t];
                GET_TEXT_FROM_TUPLE(rule,rules_columns.rule);
                nr = parse_rule(rule, rule_arr);
                if (nr == -1) {
                    elog(NOTICE, "load_roles: rule exceeds 128 terms");
                    return -1;
                }
                ret = rules_add_rule(rules, nr, rule_arr);
                if (ret != 0) {
                    elog(NOTICE,"load_roles: failed to add rule %d (%d): %s",
                         total_tuples+t+1, ret, rule);
                    return -1;
                }
            }
            //DBG("calling SPI_freetuptable");
            SPI_freetuptable(tuptable);
            //DBG("back from SPI_freetuptable");
        }
        else
            moredata = FALSE;

        total_tuples += ntuples;
    }

    ret = rules_ready(rules);
    if (ret != 0) {
        elog(NOTICE, "load_roles: failed to ready the rules: err: %d", ret);
        return -1;
    }


    SET_TIME(t2);
    ELAPSED_T(t1, t2);
    DBG("Time to read %i rule records: %.1f ms.", total_tuples, elapsed);

    return 0;
}


