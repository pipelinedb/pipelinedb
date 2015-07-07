/*
 * parseaddres.c - utility to crack a string into address, city st zip
 *
 * Copyright 2006 Stephen Woodbridge
 *
 * This code is released under and MIT-X style license,
 *
 * Stphen Woodbridge
 * woodbri@swoodbridge.com
 * woodbr@imaptools.com
 *
 *
 * TODO:
 *   * add recognition of country before or after postalcode
 *   * have clean trailing punctionation return a code if a comma was removed
 *     if comma and no state then there is probably no city
 *
 */

#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <pcre.h>
#include "parseaddress-api.h"

#undef DEBUG
//#define DEBUG 1

#ifdef DEBUG
#define DBG(format, arg...)                     \
    elog(NOTICE, format , ## arg)
#else
#define DBG(format, arg...) do { ; } while (0)
#endif

const char *get_state_regex(char *st);
const char *parseaddress_cvsid();
char *clean_leading_punct(char *s);

const char *get_state_regex(char *st)
{
    int i;
    int cmp;
#include "parseaddress-stcities.h"

    if (!st || strlen(st) != 2) return NULL;

    for (i=0; i<NUM_STATES; i++) {
        cmp = strcmp(states[i], st);
        if (cmp == 0)
            return stcities[i];
        else if (cmp > 0)
            return NULL;
    }
    return NULL;
}

int clean_trailing_punct(char *s)
{
    int i;
    int ret = 0;

    i=strlen(s)-1;
    while (ispunct(s[i]) || isspace(s[i])) {
        if (s[i] == ',') ret = 1;
        s[i--] = '\0';
    }
    return ret;
}

char *clean_leading_punct(char *s)
{
    int i;

    for (i=0; i<strlen(s); i++)
        if (!(ispunct(s[i]) || isspace(s[i])))
            break;

    return s + i;
}

void strtoupper(char *s)
{
    int i;

    for (i=0; i<strlen(s); i++)
        s[i] = toupper(s[i]);
}

int match(char *pattern, char *s, int *ovect, int options)
{
    const char *error;
    int erroffset;
    pcre *re;
    int rc;
    
    re = pcre_compile(pattern, options, &error, &erroffset, NULL);
    if (!re) return -99;
    
    rc = pcre_exec(re, NULL, s, strlen(s), 0, 0, ovect, OVECCOUNT);
    free(re);
    
    if (rc < 0) return rc;
    else if (rc == 0) rc = OVECCOUNT/3;

    return rc;
}

#define RET_ERROR(a,e) if (!a) {*reterr = e; return NULL;}

ADDRESS *parseaddress(HHash *stH, char *s, int *reterr)
{

#include "parseaddress-regex.h"

    int ovect[OVECCOUNT];
    char c;
    char *stregx;
    char *caregx;
    char *state = NULL;
    char *regx;
    int mi;
    int i, j;
    int rc;
    int comma = 0;
    ADDRESS *ret;
#ifdef USE_HSEARCH
    ENTRY e, *ep;
    int err;
#else
    char *key;
    char *val;
#endif

    ret = (ADDRESS *) palloc0(sizeof(ADDRESS));

    /* check if we were passed a lat lon */
    rc = match("^\\s*([-+]?\\d+(\\.\\d*)?)[\\,\\s]+([-+]?\\d+(\\.\\d*)?)\\s*$", s, ovect, 0);
    if (rc >= 3) {
        *(s+ovect[3]) = '\0';
        ret->lat = strtod(s+ovect[2], NULL);
        ret->lon = strtod(s+ovect[6], NULL);
        return ret;
    }

    /* clean the string of multiple white spaces and . */
    
    for (i=0, j=0; i<strlen(s); i++) {
        c = s[i];
        if (c == '.') c = s[i] = ' ';
        if (j == 0 && isspace(c)) continue;
        if (i && isspace(c) && isspace(s[i-1])) continue;
        s[j] = s[i];
        j++;
    }
    if (isspace(s[j-1])) j--;
    s[j] = '\0';

    /* clean trailing punctuation */
    comma |= clean_trailing_punct(s);

    /* assume country code is US */

    ret->cc  = (char *) palloc0(3 * sizeof(char));
    strcpy(ret->cc, "US");

    /* get US zipcode components */

    rc = match("\\b(\\d{5})[-\\s]?(\\d{4})?$", s, ovect, 0);
    if (rc >= 2) {
        ret->zip = (char *) palloc0((ovect[3]-ovect[2]+1) * sizeof(char));
        strncpy(ret->zip, s+ovect[2], ovect[3]-ovect[2]);
        if (rc >= 3) {
            ret->zipplus = (char *) palloc0((ovect[5]-ovect[4]+1) * sizeof(char));
            strncpy(ret->zipplus, s+ovect[4], ovect[5]-ovect[4]);
        }
        /* truncate the postalcode off the string */
        *(s+ovect[0]) = '\0';
        comma = 0;
    }
    /* get canada zipcode components */
    else {
        rc = match("\\b([a-z]\\d[a-z]\\s?\\d[a-z]\\d)$", s, ovect, PCRE_CASELESS);
        if (rc >= 1) {
            ret->zip = (char *) palloc0((ovect[1]-ovect[0]+1) * sizeof(char));
            strncpy(ret->zip, s+ovect[0], ovect[1]-ovect[0]);
            strcpy(ret->cc, "CA");
            /* truncate the postalcode off the string */
            *(s+ovect[0]) = '\0';
            comma = 0;
        }
    }

    /* clean trailing punctuation */
    comma |= clean_trailing_punct(s);

    /* get state components */

    caregx = "^(?-xism:(?i:(?=[abmnopqsy])(?:n[ltsu]|[am]b|[bq]c|on|pe|sk|yt)))$";
    stregx = "\\b(?-xism:(?i:(?=[abcdfghiklmnopqrstuvwy])(?:a(?:l(?:a(?:bam|sk)a|berta)?|mer(?:ican)?\\ samoa|r(?:k(?:ansas)?|izona)?|[kszb])|s(?:a(?:moa|skatchewan)|outh\\ (?:carolin|dakot)a|\\ (?:carolin|dakot)a|[cdk])|c(?:a(?:lif(?:ornia)?)?|o(?:nn(?:ecticut)?|lorado)?|t)|d(?:e(?:la(?:ware)?)?|istrict\\ of\\ columbia|c)|f(?:l(?:(?:orid)?a)?|ederal\\ states\\ of\\ micronesia|m)|m(?:i(?:c(?:h(?:igan)?|ronesia)|nn(?:esota)?|ss(?:(?:issipp|our)i)?)?|a(?:r(?:shall(?:\\ is(?:l(?:and)?)?)?|yland)|ss(?:achusetts)?|ine|nitoba)?|o(?:nt(?:ana)?)?|[ehdnstpb])|g(?:u(?:am)?|(?:eorgi)?a)|h(?:awai)?i|i(?:d(?:aho)?|l(?:l(?:inois)?)?|n(?:d(?:iana)?)?|(?:ow)?a)|k(?:(?:ansa)?s|(?:entuck)?y)|l(?:a(?:bordor)?|ouisiana)|n(?:e(?:w(?:\\ (?:foundland(?:\\ and\\ labordor)?|hampshire|jersey|mexico|(?:yor|brunswic)k)|foundland)|(?:brask|vad)a)?|o(?:rth(?:\\ (?:mariana(?:\\ is(?:l(?:and)?)?)?|(?:carolin|dakot)a)|west\\ territor(?:ies|y))|va\\ scotia)|\\ (?:carolin|dakot)a|u(?:navut)?|[vhjmycdblsf]|w?t)|o(?:h(?:io)?|k(?:lahoma)?|r(?:egon)?|n(?:t(?:ario)?)?)|p(?:a(?:lau)?|e(?:nn(?:sylvania)?|i)?|r(?:ince\\ edward\\ island)?|w|uerto\\ rico)|r(?:hode\\ island|i)|t(?:e(?:nn(?:essee)?|xas)|[nx])|ut(?:ah)?|v(?:i(?:rgin(?:\\ islands|ia))?|(?:ermon)?t|a)|w(?:a(?:sh(?:ington)?)?|i(?:sc(?:onsin)?)?|y(?:oming)?|(?:est)?\\ virginia|v)|b(?:ritish\\ columbia|c)|q(?:uebe)?c|y(?:ukon|t))))$";
    
    rc = match(stregx, s, ovect, PCRE_CASELESS);
    if (rc > 0) {
        state = (char *) palloc0((ovect[1]-ovect[0]+1) * sizeof(char));
        strncpy(state, s+ovect[0], ovect[1]-ovect[0]);

        /* truncate the state/province off the string */
        *(s+ovect[0]) = '\0';
        
        /* lookup state in hash and get abbreviation */
        strtoupper(state);
#ifdef USE_HSEARCH
        e.key = state;
        err = hsearch_r(e, FIND, &ep, stH);
        if (err) {
            ret->st = (char *) palloc0(3 * sizeof(char));
            strcpy(ret->st, ep->data);
        }
#else
        key = state;
        val = (char *)hash_get(stH, key);
        if (val) {
            ret->st = pstrdup(val);
        }
#endif
        else {
            *reterr = 1002;
            return NULL;
        }

        /* check if it a Canadian Province */
        rc = match(caregx, ret->st, ovect, PCRE_CASELESS);
        if (rc > 0) {
            strcpy(ret->cc, "CA");
            // if (ret->cc) printf("  CC: %s\n", ret->cc);
        }
        comma = 0;
    }

    /* clean trailing punctuation */
    comma |= clean_trailing_punct(s);

    /* get city components */

    /*
     * This part is ambiguous without punctuation after the street
     * because we can have any of the following forms:
     *
     * num predir? prefixtype? street+ suffixtype? suffdir?,
     *     ((north|south|east|west)? city)? state? zip?
     *
     * and technically num can be of the form:
     *
     *   pn1? n1 pn2? n2? sn2?
     * where
     * pn1 is a prefix character
     * n1  is a number
     * pn2 is a prefix character
     * n2  is a number
     * sn2 is a suffix character
     *
     * and a trailing letter might be [NSEW] which predir can also be
     *
     * So it is ambigious whether a directional between street and city
     * belongs to which component. Futher since the the street and the city
     * are both just a string of arbitrary words, it is difficult if not
     * impossible to determine if an give word belongs to sone side or the
     * other.
     *
     * So for the best results users should include a comma after the street.
     *
     * The approach will be as follows:
     *   1. look for a comma and assume this is the separator
     *   2. if we can find a state specific regex try that
     *   3. else loop through an array of possible regex patterns
     *   4. fail and assume there is not city
    */

    /* look for a comma */
    DBG("parse_address: s=%s", s);
    mi = 0;

    regx = "(?:,\\s*)([^,]+)$";
    rc = match((char *)regx, s, ovect, 0);
    if (rc <= 0) {
        /* look for state specific regex */
        mi++;
        regx = (char *) get_state_regex(ret->st);
        if (regx)
            rc = match((char *)regx, s, ovect, 0);
    }
    DBG("Checked for comma: %d", rc);
    if (rc <= 0 && ret->st && strlen(ret->st)) {
        /* look for state specific regex */
        mi++;
        regx = (char *) get_state_regex(ret->st);
        if (regx)
            rc = match((char *)regx, s, ovect, 0);
    }
    DBG("Checked for state-city: %d", rc);
    if (rc <= 0) {
        /* run through the regx's and see if we get a match */
        for (i=0; i<nreg; i++) {
            mi++;
            rc = match((char *)t_regx[i], s, ovect, 0);
            DBG("    rc=%d, i=%d", rc, i);
            if (rc > 0) break;
        }
        DBG("rc=%d, i=%d", rc, i);
    }
    DBG("Checked regexs: %d, %d, %d", rc, ovect[2], ovect[3]);
    if (rc > 0 && ovect[3]>ovect[2]) {
        /* we have a match so process it */
        ret->city = (char *) palloc0((ovect[3]-ovect[2]+1) * sizeof(char));
        strncpy(ret->city, s+ovect[2], ovect[3]-ovect[2]);
        /* truncate the state/province off the string */
        *(s+ovect[2]) = '\0';
    }
        
    /* clean trailing punctuation */
    clean_trailing_punct(s);

    /* check for [@] that would indicate a intersection */
    /* -- 2010-12-11 : per Nancy R. we are using @ to indicate an intersection
       ampersand is used in both street names and landmarks so it is highly
       ambiguous -- */
    rc = match("^([^@]+)\\s*[@]\\s*([^@]+)$", s, ovect, 0);
    if (rc > 0) {
        s[ovect[3]] = '\0';
        clean_trailing_punct(s+ovect[2]);
        ret->street = pstrdup(s+ovect[2]);

        s[ovect[5]] = '\0';
        clean_leading_punct(s+ovect[4]);
        ret->street2 = pstrdup(s+ovect[4]);
    }
    else {

        /* and the remainder must be the address components */
        ret->address1 = pstrdup(clean_leading_punct(s));

        /* split the number off the street if it exists */
        rc = match("^((?i)[nsew]?\\d+[-nsew]*\\d*[nsew]?\\b)", s, ovect, 0);
        if (rc > 0) {
            ret->num = (char *) palloc0((ovect[1]-ovect[0]+1) * sizeof(char));
            strncpy(ret->num, s, ovect[1]-ovect[0]);
            ret->street = pstrdup(clean_leading_punct(s+ovect[1]));
        }
    }

    return ret;
}

int load_state_hash(HHash *stH)
{
    char * words[][2] = {
        {"ALABAMA"                      , "AL"},
        {"ALASKA"                       , "AK"},
        {"AMERICAN SAMOA"               , "AS"},
        {"AMER SAMOA"                   , "AS"},
        {"SAMOA"                        , "AS"},
        {"ARIZONA"                      , "AZ"},
        {"ARKANSAS"                     , "AR"},
        {"ARK"                          , "AR"},
        {"CALIFORNIA"                   , "CA"},
        {"CALIF"                        , "CA"},
        {"COLORADO"                     , "CO"},
        {"CONNECTICUT"                  , "CT"},
        {"CONN"                         , "CT"},
        {"DELAWARE"                     , "DE"},
        {"DELA"                         , "DE"},
        {"DISTRICT OF COLUMBIA"         , "DC"},
        {"FEDERAL STATES OF MICRONESIA" , "FM"},
        {"MICRONESIA"                   , "FM"},
        {"FLORIDA"                      , "FL"},
        {"FLA"                          , "FL"},
        {"GEORGIA"                      , "GA"},
        {"GUAM"                         , "GU"},
        {"HAWAII"                       , "HI"},
        {"IDAHO"                        , "ID"},
        {"ILLINOIS"                     , "IL"},
        {"ILL"                          , "IL"},
        {"INDIANA"                      , "IN"},
        {"IND"                          , "IN"},
        {"IOWA"                         , "IA"},
        {"KANSAS"                       , "KS"},
        {"KENTUCKY"                     , "KY"},
        {"LOUISIANA"                    , "LA"},
        {"MAINE"                        , "ME"},
        {"MARSHALL ISLAND"              , "MH"},
        {"MARSHALL ISL"                 , "MH"},
        {"MARSHALL IS"                  , "MH"},
        {"MARSHALL"                     , "MH"},
        {"MARYLAND"                     , "MD"},
        {"MASSACHUSETTS"                , "MA"},
        {"MASS"                         , "MA"},
        {"MICHIGAN"                     , "MI"},
        {"MICH"                         , "MI"},
        {"MINNESOTA"                    , "MN"},
        {"MINN"                         , "MN"},
        {"MISSISSIPPI"                  , "MS"},
        {"MISS"                         , "MS"},
        {"MISSOURI"                     , "MO"},
        {"MONTANA"                      , "MT"},
        {"MONT"                         , "MT"},
        {"NEBRASKA"                     , "NE"},
        {"NEVADA"                       , "NV"},
        {"NEW HAMPSHIRE"                , "NH"},
        {"NEW JERSEY"                   , "NJ"},
        {"NEW MEXICO"                   , "NM"},
        {"NEW YORK"                     , "NY"},
        {"NORTH CAROLINA"               , "NC"},
        {"N CAROLINA"                   , "NC"},
        {"NORTH DAKOTA"                 , "ND"},
        {"N DAKOTA"                     , "ND"},
        {"NORTH MARIANA ISL"            , "MP"},
        {"NORTH MARIANA IS"             , "MP"},
        {"NORTH MARIANA"                , "MP"},
        {"NORTH MARIANA ISLAND"         , "MP"},
        {"OHIO"                         , "OH"},
        {"OKLAHOMA"                     , "OK"},
        {"OREGON"                       , "OR"},
        {"PALAU"                        , "PW"},
        {"PENNSYLVANIA"                 , "PA"},
        {"PENN"                         , "PA"},
        {"PUERTO RICO"                  , "PR"},
        {"RHODE ISLAND"                 , "RI"},
        {"SOUTH CAROLINA"               , "SC"},
        {"S CAROLINA"                   , "SC"},
        {"SOUTH DAKOTA"                 , "SD"},
        {"S DAKOTA"                     , "SD"},
        {"TENNESSEE"                    , "TN"},
        {"TENN"                         , "TN"},
        {"TEXAS"                        , "TX"},
        {"UTAH"                         , "UT"},
        {"VERMONT"                      , "VT"},
        {"VIRGIN ISLANDS"               , "VI"},
        {"VIRGINIA"                     , "VA"},
        {"WASHINGTON"                   , "WA"},
        {"WASH"                         , "WA"},
        {"WEST VIRGINIA"                , "WV"},
        {"W VIRGINIA"                   , "WV"},
        {"WISCONSIN"                    , "WI"},
        {"WISC"                         , "WI"},
        {"WYOMING"                      , "WY"},
        {"ALBERTA"                      , "AB"},
        {"BRITISH COLUMBIA"             , "BC"},
        {"MANITOBA"                     , "MB"},
        {"NEW BRUNSWICK"                , "NB"},
        {"NEW FOUNDLAND AND LABORDOR"   , "NL"},
        {"NEW FOUNDLAND"                , "NL"},
        {"NEWFOUNDLAND"                 , "NL"},
        {"LABORDOR"                     , "NL"},
        {"NORTHWEST TERRITORIES"        , "NT"},
        {"NORTHWEST TERRITORY"          , "NT"},
        {"NWT"                          , "NT"},
        {"NOVA SCOTIA"                  , "NS"},
        {"NUNAVUT"                      , "NU"},
        {"ONTARIO"                      , "ON"},
        {"ONT"                          , "ON"},
        {"PRINCE EDWARD ISLAND"         , "PE"},
        {"PEI"                          , "PE"},
        {"QUEBEC"                       , "QC"},
        {"SASKATCHEWAN"                 , "SK"},
        {"YUKON"                        , "YT"},
        {"NF"                           , "NL"},
        {NULL, NULL}
    };

#ifdef USE_HSEARCH
    ENTRY e, *ep;
    int err;
#else
    char *key;
    char *val;
#endif
    int i, cnt;

    /* count the entries above */
    cnt = 0;
    while (words[cnt][0]) cnt++;

    DBG("Words cnt=%d", cnt);

#ifdef USE_HSEARCH
    if (! hcreate_r(cnt*2, stH)) return 1001;
    for (i=0; i<cnt; i++) {
        e.key  = words[i][0];
        e.data = words[i][1];
        err = hsearch_r(e, ENTER, &ep, stH);
        /* there should be no failures */
        if (!err) return 1003;
        e.key  = words[i][1];
        e.data = words[i][1];
        err = hsearch_r(e, ENTER, &ep, stH);
        /* there should be no failures */
        if (!err) return 1003;
    }
#else
    if (! stH ) return 1001;
    for (i=0; i<cnt; i++) {
        //DBG("load_hash i=%d", i);
        key = words[i][0];
        val = words[i][1];
        hash_set(stH, key, (void *)val);
        key = words[i][1];
        val = words[i][1];
        hash_set(stH, key, (void *)val);
    }
#endif
    return 0;
}

void free_state_hash(HHash *stH)
{
//#if 0
#ifdef USE_HSEARCH
    if (stH) hdestroy_r(stH);
#else
    if (stH) hash_free(stH);
#endif
//#endif
}
