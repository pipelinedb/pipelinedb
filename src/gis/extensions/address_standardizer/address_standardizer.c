#include "postgres.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "fmgr.h"

#undef DEBUG
//#define DEBUG 1

#include "pagc_api.h"
#include "pagc_std_api.h"
#include "std_pg_hash.h"
#include "parseaddress-api.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum standardize_address(PG_FUNCTION_ARGS);
Datum standardize_address1(PG_FUNCTION_ARGS);


static char *text2char(text *in)
{
    char *out = palloc(VARSIZE(in));
    memcpy(out, VARDATA(in), VARSIZE(in) - VARHDRSZ);
    out[VARSIZE(in) - VARHDRSZ] = '\0';
    return out;
}

/*
 * The signature for standardize_address follows. The lextab, gaztab and
 * rultab should not change once the reference has been standardized and
 * the same tables must be used for a geocode request as were used on the
 * reference set or the matching will get degregated.
 *
 *   select * from standardize_address(
 *       lextab text,  -- name of table of view
 *       gaztab text,  -- name of table or view
 *       rultab text,  -- name of table of view
 *       micro text,   -- '123 main st'
 *       macro text);  -- 'boston ma 01002'
 *
 * If you want to standardize a whole table then call it like:
 *
 *   insert into stdaddr (...)
 *       select (std).* from (
 *           select standardize_address(
 *               'lextab', 'gaztab', 'rultab', micro, marco) as std
 *             from table_to_standardize) as foo;
 *
 * The structure of the lextab and gaztab tables of views must be:
 *
 *    seq int4
 *    word text
 *    stdword text
 *    token int4
 *
 * the rultab table or view must have columns:
 *
 *    rule text
*/

PG_FUNCTION_INFO_V1(standardize_address);

Datum standardize_address(PG_FUNCTION_ARGS)
{
    TupleDesc            tuple_desc;
    AttInMetadata       *attinmeta;
    STANDARDIZER        *std;
    char                *lextab;
    char                *gaztab;
    char                *rultab;
    char                *micro;
    char                *macro;
    Datum                result;
    STDADDR             *stdaddr;
    char               **values;
    int                  k;
    HeapTuple            tuple;

    DBG("Start standardize_address");

    lextab = text2char(PG_GETARG_TEXT_P(0));
    gaztab = text2char(PG_GETARG_TEXT_P(1));
    rultab = text2char(PG_GETARG_TEXT_P(2));
    micro  = text2char(PG_GETARG_TEXT_P(3));
    macro  = text2char(PG_GETARG_TEXT_P(4));

    DBG("calling RelationNameGetTupleDesc");
    if (get_call_result_type( fcinfo, NULL, &tuple_desc ) != TYPEFUNC_COMPOSITE ) {
        elog(ERROR, "standardize_address() was called in a way that cannot accept record as a result");
    }
    BlessTupleDesc(tuple_desc);
    attinmeta = TupleDescGetAttInMetadata(tuple_desc);

    DBG("calling GetStdUsingFCInfo(fcinfo, '%s', '%s', '%s')", lextab, gaztab, rultab);
    std = GetStdUsingFCInfo(fcinfo, lextab, gaztab, rultab);
    if (!std)
        elog(ERROR, "standardize_address() failed to create the address standardizer object!");

    DBG("calling std_standardize_mm('%s', '%s')", micro, macro);
    stdaddr = std_standardize_mm( std, micro, macro, 0 );

    DBG("back from fetch_stdaddr");

    values = (char **) palloc(16 * sizeof(char *));
    for (k=0; k<16; k++) {
        values[k] = NULL;
    }
    DBG("setup values array for natts=%d", tuple_desc->natts);
    if (stdaddr) {
        values[0] = stdaddr->building   ? pstrdup(stdaddr->building) : NULL;
        values[1] = stdaddr->house_num  ? pstrdup(stdaddr->house_num) : NULL;
        values[2] = stdaddr->predir     ? pstrdup(stdaddr->predir) : NULL;
        values[3] = stdaddr->qual       ? pstrdup(stdaddr->qual) : NULL;
        values[4] = stdaddr->pretype    ? pstrdup(stdaddr->pretype) : NULL;
        values[5] = stdaddr->name       ? pstrdup(stdaddr->name) : NULL;
        values[6] = stdaddr->suftype    ? pstrdup(stdaddr->suftype) : NULL;
        values[7] = stdaddr->sufdir     ? pstrdup(stdaddr->sufdir) : NULL;
        values[8] = stdaddr->ruralroute ? pstrdup(stdaddr->ruralroute) : NULL;
        values[9] = stdaddr->extra      ? pstrdup(stdaddr->extra) : NULL;
        values[10] = stdaddr->city      ? pstrdup(stdaddr->city) : NULL;
        values[11] = stdaddr->state     ? pstrdup(stdaddr->state) : NULL;
        values[12] = stdaddr->country   ? pstrdup(stdaddr->country) : NULL;
        values[13] = stdaddr->postcode  ? pstrdup(stdaddr->postcode) : NULL;
        values[14] = stdaddr->box       ? pstrdup(stdaddr->box) : NULL;
        values[15] = stdaddr->unit      ? pstrdup(stdaddr->unit) : NULL;
    }

    DBG("calling heap_form_tuple");
    tuple = BuildTupleFromCStrings(attinmeta, values);

    /* make the tuple into a datum */
    DBG("calling HeapTupleGetDatum");
    result = HeapTupleGetDatum(tuple);

    /* clean up (this is not really necessary */
    DBG("freeing values, nulls, and stdaddr");
    stdaddr_free(stdaddr);

    DBG("returning standardized result");
    PG_RETURN_DATUM(result);
}


PG_FUNCTION_INFO_V1(standardize_address1);

Datum standardize_address1(PG_FUNCTION_ARGS)
{
    TupleDesc            tuple_desc;
    AttInMetadata       *attinmeta;
    STANDARDIZER        *std;
    char                *lextab;
    char                *gaztab;
    char                *rultab;
    char                *addr;
    char                *micro;
    char                *macro;
    Datum                result;
    STDADDR             *stdaddr;
    char               **values;
    int                  k;
    HeapTuple            tuple;
    ADDRESS             *paddr;
    HHash               *stH;
    int                  err;

    DBG("Start standardize_address");

    lextab = text2char(PG_GETARG_TEXT_P(0));
    gaztab = text2char(PG_GETARG_TEXT_P(1));
    rultab = text2char(PG_GETARG_TEXT_P(2));
    addr   = text2char(PG_GETARG_TEXT_P(3));

    DBG("calling RelationNameGetTupleDesc");
    if (get_call_result_type( fcinfo, NULL, &tuple_desc ) != TYPEFUNC_COMPOSITE ) {
        elog(ERROR, "standardize_address() was called in a way that cannot accept record as a result");
    }
    BlessTupleDesc(tuple_desc);
    attinmeta = TupleDescGetAttInMetadata(tuple_desc);

    DBG("Got tupdesc, allocating HHash");

    stH = (HHash *) palloc0(sizeof(HHash));
    if (!stH) {
         elog(ERROR, "standardize_address: Failed to allocate memory for hash!");
         return -1;
    }

    DBG("going to load_state_hash");

    err = load_state_hash(stH);
    if (err) {
        DBG("got err=%d from load_state_hash().", err);
#ifdef USE_HSEARCH
        DBG("calling hdestroy_r(stH).");
        hdestroy_r(stH);
#endif
        elog(ERROR, "standardize_address: load_state_hash() failed(%d)!", err);
        return -1;
    }

    DBG("calling parseaddress()");
    paddr = parseaddress(stH, addr, &err);
    if (!paddr) {
        elog(ERROR, "parse_address: parseaddress() failed!");
        return -1;
    }

    /* check for errors and comput length of macro string */
    if (paddr->street2)
        elog(ERROR, "standardize_address() can not be passed an intersection.");
    if (! paddr-> address1)
        elog(ERROR, "standardize_address() could not parse the address into components.");

    k = 1;
    if (paddr->city) k += strlen(paddr->city) + 1;
    if (paddr->st)   k += strlen(paddr->st)   + 1;
    if (paddr->zip)  k += strlen(paddr->zip)  + 1;
    if (paddr->cc)   k += strlen(paddr->cc)   + 1;

    /* create micro and macro from paddr */
    micro = pstrdup(paddr->address1);
    macro = (char *) palloc(k * sizeof(char));

    *macro = '\0';
    if (paddr->city) { strcat(macro, paddr->city); strcat(macro, ","); }
    if (paddr->st  ) { strcat(macro, paddr->st  ); strcat(macro, ","); }
    if (paddr->zip ) { strcat(macro, paddr->zip ); strcat(macro, ","); }
    if (paddr->cc  ) { strcat(macro, paddr->cc  ); strcat(macro, ","); }

    DBG("calling GetStdUsingFCInfo(fcinfo, '%s', '%s', '%s')", lextab, gaztab, rultab);
    std = GetStdUsingFCInfo(fcinfo, lextab, gaztab, rultab);
    if (!std)
        elog(ERROR, "standardize_address() failed to create the address standardizer object!");

    DBG("calling std_standardize_mm('%s', '%s')", micro, macro);
    stdaddr = std_standardize_mm( std, micro, macro, 0 );

    DBG("back from fetch_stdaddr");

    values = (char **) palloc(16 * sizeof(char *));
    for (k=0; k<16; k++) {
        values[k] = NULL;
    }
    DBG("setup values array for natts=%d", tuple_desc->natts);
    if (stdaddr) {
        values[0] = stdaddr->building   ? pstrdup(stdaddr->building) : NULL;
        values[1] = stdaddr->house_num  ? pstrdup(stdaddr->house_num) : NULL;
        values[2] = stdaddr->predir     ? pstrdup(stdaddr->predir) : NULL;
        values[3] = stdaddr->qual       ? pstrdup(stdaddr->qual) : NULL;
        values[4] = stdaddr->pretype    ? pstrdup(stdaddr->pretype) : NULL;
        values[5] = stdaddr->name       ? pstrdup(stdaddr->name) : NULL;
        values[6] = stdaddr->suftype    ? pstrdup(stdaddr->suftype) : NULL;
        values[7] = stdaddr->sufdir     ? pstrdup(stdaddr->sufdir) : NULL;
        values[8] = stdaddr->ruralroute ? pstrdup(stdaddr->ruralroute) : NULL;
        values[9] = stdaddr->extra      ? pstrdup(stdaddr->extra) : NULL;
        values[10] = stdaddr->city      ? pstrdup(stdaddr->city) : NULL;
        values[11] = stdaddr->state     ? pstrdup(stdaddr->state) : NULL;
        values[12] = stdaddr->country   ? pstrdup(stdaddr->country) : NULL;
        values[13] = stdaddr->postcode  ? pstrdup(stdaddr->postcode) : NULL;
        values[14] = stdaddr->box       ? pstrdup(stdaddr->box) : NULL;
        values[15] = stdaddr->unit      ? pstrdup(stdaddr->unit) : NULL;
    }

    DBG("calling heap_form_tuple");
    tuple = BuildTupleFromCStrings(attinmeta, values);

    /* make the tuple into a datum */
    DBG("calling HeapTupleGetDatum");
    result = HeapTupleGetDatum(tuple);

    /* clean up (this is not really necessary */
    DBG("freeing values, nulls, and stdaddr");
    stdaddr_free(stdaddr);

    DBG("freeing values, hash, and paddr");
    free_state_hash(stH);

    DBG("returning standardized result");
    PG_RETURN_DATUM(result);
}


