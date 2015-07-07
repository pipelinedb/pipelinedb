#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "postgres.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "fmgr.h"

#include "parseaddress-api.h"
#include <pcre.h>
#include <string.h>

#undef DEBUG
//#define DEBUG 1

#ifdef DEBUG
#define DBG(format, arg...)                     \
    elog(NOTICE, format , ## arg)
#else
#define DBG(format, arg...) do { ; } while (0)
#endif

Datum parse_address(PG_FUNCTION_ARGS);

static char *text2char(text *in)
{
    char *out = palloc(VARSIZE(in));
    memcpy(out, VARDATA(in), VARSIZE(in) - VARHDRSZ);
    out[VARSIZE(in) - VARHDRSZ] = '\0';
    return out;
}

PG_FUNCTION_INFO_V1(parse_address);

Datum parse_address(PG_FUNCTION_ARGS)
{
    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;
    Datum                result;
    ADDRESS             *paddr;
    HHash               *stH;
    char                *str;
    char               **values;
    int                  err;
    HeapTuple            tuple;


    DBG("Start standardize_address");

    str = text2char(PG_GETARG_TEXT_P(0));

    DBG("str='%s'", str);

    if (get_call_result_type( fcinfo, NULL, &tupdesc ) != TYPEFUNC_COMPOSITE ) {
        elog(ERROR, "function returning record called in context"
            " that cannot accept type record");
        return -1;
    }
    BlessTupleDesc(tupdesc);
    attinmeta = TupleDescGetAttInMetadata(tupdesc);

    DBG("Got tupdesc, allocating HHash");

    stH = (HHash *) palloc0(sizeof(HHash));
    if (!stH) {
         elog(ERROR, "parse_address: Failed to allocate memory for hash!");
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
        elog(ERROR, "parse_address: load_state_hash() failed(%d)!", err);
        return -1;
    }

    DBG("calling parseaddress()");
    paddr = parseaddress(stH, str, &err);
    if (!paddr) {
        elog(ERROR, "parse_address: parseaddress() failed!");
        return -1;
    }

    DBG("setup values array for natts=%d", tupdesc->natts);
    values = (char **) palloc(9 * sizeof(char *));
    if (!values) {
        elog(ERROR, "parse_address: out of memory!");
        return -1;
    }
    values[0] = paddr->num;
    values[1] = paddr->street;
    values[2] = paddr->street2;
    values[3] = paddr->address1;
    values[4] = paddr->city;
    values[5] = paddr->st;
    values[6] = paddr->zip;
    values[7] = paddr->zipplus;
    values[8] = paddr->cc;

    DBG("calling heap_form_tuple");
    tuple = BuildTupleFromCStrings(attinmeta, values);

    /* make the tuple into a datum */
    DBG("calling HeapTupleGetDatum");
    result = HeapTupleGetDatum(tuple);

    /* clean up (this is not really necessary */
    DBG("freeing values, hash, and paddr");
    free_state_hash(stH);

    DBG("returning parsed address result");
    return result;
}

