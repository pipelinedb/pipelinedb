/* -- pagc_api.h 

This file is the common header file for PAGC internal routines

Prototype 20H10 (This file was written by Walter Sinclair).

Copyright (c) 2001-2012 Walter Bruce Sinclair

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/* For pagc-0.4.2 : last revised 2012-08-31 */

#ifndef PGC_H
#define PGC_H

#define BUILD_API

#include <postgres.h>

#ifdef DEBUG
#define DBG(format, arg...)                     \
    elog(NOTICE, format , ## arg)
#else
#define DBG(format, arg...) do { ; } while (0)
#endif

#include "pagc_tools.h"

#ifndef BUILD_API
#include "pagc_common.h"

#ifdef MINGW32
#include <windows.h>
#endif

#ifdef ENABLE_THREADED
#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif
#endif

#else

#define SENTINEL '\0'
#define BLANK_STRING(STR) *STR = SENTINEL
#define MAXSTRLEN 256

/* -- boolean -- */
#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

#define ERR_FAIL -2
#define FAIL -1
#define BOTH 2

typedef int SYMB ;
typedef double DS_Score_t ;
typedef void * DS_Handle ;

#endif

#include <math.h>
#ifndef BUILD_API
#include "ds.h"

#define THREE_SOURCE_IDS

//#define WITH_ALT_LEFT_RIGHT

/* 2009-07-21 : keep track of version 
   2010-02-01 : increase to 0.3.0
   2010-08-20 : increase to 0.4.0 
   2011-08-14 : increase to 0.4.1 */

#ifdef WITH_ALT_LEFT_RIGHT
#define CUR_PAGC_VER 41
#else
#define CUR_PAGC_VER 40
#endif
#endif
/* -- uncomment to index soundex-zip combo keys
#define COMBINE_POSTAL_WITH_SOUNDEX
-- */


#define USE_METERS
#define MAX_ATT_FLDS 4
#define MAX_REDIRECTS 6
#define COMMA ','
#define NEITHER 0
#define LEFT -1
#define RIGHT 1
#define OCCUPANCY_MULTIPLE 16
#define EXACT_MICRO_MATCH 32
#define EXACT_MACRO_MATCH 64
#define MACRO_THRESHOLD_MATCH 128
#define EPSILON 0
/* 2009-10-16 : increase size of MAXPHRASE and LANDMARK_ARRAY_WIDTH */
#define MAXPHRASE 10
#define LANDMARK_ARRAY_WIDTH 10 + MAXPHRASE
#define MAXFLDLEN MAXSTRLEN
#define MAX_REC_SPACE 262144
#define MAX_SCHEMAS 6 /* 2008-07-28 increased from 3 */
#define MAX_ERRORS 512

/* -- input symbols -- */
#define NUMBER 0
#define WORD 1
#define TYPE 2

#define ROAD 6
#define STOPWORD 7

#define DASH 9

#define AMPERS 13

#define ORD 15

#define SINGLE 18
#define BUILDH 19
#define MILE 20
#define DOUBLE 21
#define DIRECT 22
#define MIXED 23
#define BUILDT 24
#define FRACT 25
#define PCT 26
#define PCH 27
#define QUINT 28
#define QUAD 29
#define MAXINSYM QUAD + 1

/* -- output symbols -- */
#define BLDNG 0
#define HOUSE 1
#define PREDIR 2
#define QUALIF 3
#define PRETYP 4
#define STREET 5
#define SUFTYP 6
#define SUFDIR 7
#define RR 8
#define UNKNWN 9
#define CITY 10
#define PROV 11
#define NATION 12
#define POSTAL 13
#define BOXH 14
#define BOXT 15
#define UNITH 16
#define UNITT 17
#define MAXOUTSYM UNITT + 1
#define NEEDHEAD BOXH

/* 2009-07-19 : redefinitions in a landmarks context :
needed to collect from standardization output. */
#define FEATNAME 0
#define FEATTYPE 8
#define FEATAREA 9
#define FEAT_L 7
#define FEAT_T 8
#define FEAT_A 9

/* -- comparison types -- */
#define NO_COMPARISON 0
#define CHAR_SINGLE 1
#define POSTAL_SINGLE 2
#define NUMBER_SINGLE 3
#define CHAR_LEFT_RIGHT 4
#define POSTAL_LEFT_RIGHT 5
#define POSTAL_SPLIT 6
#define NUMBER_INTERVAL 7
#define NUMBER_INTERVAL_LEFT_RIGHT 8
#define POSTAL_LEFT_RIGHT_SPLIT 9
#define CHAR_ALT 10 /* 2008-07-30 : for alternate names */
#ifdef WITH_ALT_LEFT_RIGHT
#define CHAR_ALT_LEFT_RIGHT 11 /* 2008-07-30 : for alternate names */
#endif

#define ERR_READ -1
#define OK_READ 0
#define GOOD_READ 1
#define MAX_EDIT_DIST 2
#define US_FULL_CODE_LEN 10
#define US_PART_CODE_LEN 5
#define SXCODELEN 4
#define DEGREES_TO_RADIANS .0174532925199432958
#define DOUBLE_PI 6.2831853071795864769
#define PI 3.14159265358979323846
#define HALF_PI 1.57079632679489661923

#ifdef USE_METERS
#define EARTH_RADIUS 6378000.
#endif

#ifndef BUILD_API
/* FLAGS : 2009-07-19 : express as hexadecimal */

typedef unsigned int PAGC_FLAG_T ;

#define REVERSE_GEO				0x00000001u /* reserved for future use*/
/* pagc_common.h : #define STATISTICS 2 */
/* ds.h : #define HAVE_DBF_POSITION	0x00000004u */
#define ENABLE_PSEUDO			0x00000008u /* -- allows pseudo edit of reference -- FLPSEUD in schema -- */
#define ENABLE_CROSS			0x00000010u /* schema : XSTREET with fields */
#define ENABLE_ALT				0x00000020u /* reserved for future use*/
#define ENABLE_LAND				0x00000040u /* 2009-07-19 : denote presence of Feature Name field */
/* pagc_common.h : #define PRINT_PROGRESS 128 */
#define APPEND					0x00000100u /* -- internal flag for APPEND builds -- */
/* pagc_common.h :#define ZERO_IS_BLANK 512 */
#define HAVE_OCCUPANCY			0x00000400u /* schema : OCCUP1 or OCCUP2 with fields */
/* pagc_common.h :#define LOG_COMPLETE 2048 */
/* pagc_common.h :#define RNF_PRETYPE_REDIRECT 4096 */
#define CONCURRENT_PRIVATE		0x00002000u /* schema: FLCONPR -- deprecating */
#define NO_STOP_ON_EXACT		0x00004000u /* schema: FLNOSEX */
#define CAN_THREAD				0x00008000u
/* ds.h #define READ_POINTS_SEQUENTIAL	0x00010000u */
#define HAVE_OFFICIAL_STREET	0x00020000u /* schema : FLOFFST -- 2008-07-28 : new */
#define HAVE_SOURCE_ID			0x00040000u /* schema : SOURCEID with field */
#define HAVE_FEAT_TYPE			0x00080000u /* 2009-07-19 : denote presence of Feature Type field */
#define HAVE_SUBDISTRICT		0x00100000u /* 2009-07-19 : denote presence of Feature Area field */

#ifdef THREE_SOURCE_IDS
#define HAVE_SOURCE_ID_B			0x00200000u /* schema : SOURCEID_B with field */
#define HAVE_SOURCE_ID_C			0x00400000u /* schema : SOURCEID_C with field */
#endif
#endif
/* -- error records -- */

typedef struct err_rec 
{
	int is_fatal ; /* -- is this a fatal error ? -- */
	char content_buf[MAXSTRLEN] ; /* -- storage for message -- */
} ERR_REC ;

typedef struct err_param 
{
	int last_err ;
	int first_err ;
	int next_fatal ;
	ERR_REC err_array[MAX_ERRORS] ;
	char *error_buf ;
	FILE *stream ; /* -- stream for log file -- */
} ERR_PARAM ;

/*===================================================================
                          STANDARDIZATION
===================================================================*/
typedef int NODE ;
typedef int DEFDEF ;

#define MAXTEXT 31

#define FIRST_LEX_POS 0
#define RIGHT_COMPRESS STOPWORD
#define LEFT_COMPRESS WORD

/* -- weight names -- */
#define LOW 3
#define LOW_MEDIUM 6
#define MEDIUM 9
#define HIGH_MEDIUM  12
#define HIGH 15
#define EXCELLENT 16
#define PERFECT 17
#define NUMBER_OF_WEIGHTS PERFECT + 1

#define MAXDEF 8
#define MAX_STZ 6 /* <revision date='2012-06-03'>return to 6</revision> */
#define MAXMORPHS 64
#define MAXLEX 64

/* -- options for SendFields -- */
#define PSEUDO_XML 0
#define PSEUDO_CSV 1
#define SCREEN 2
#define NO_FORMAT 3

/* -- clause/class numbers -- */
#define MACRO_C 0
#define MICRO_C 1
#define ARC_C 2
#define CIVIC_C 3
#define EXTRA_C 4
#define MAX_CL 5

#define EXTRA_STATE 6

/* -- start_states -- */
#define MICRO_B 0
#define MICRO_M 1
#define MACRO   2
#define PREFIX  3
#define EXIT  4

/* -- tokens -- 
   used in tokenize.c -- */
#define DFRACT 0
#define DSINGLE 1
#define DDOUBLE 2
/* -- changed so not to conflict with Windows def --*/
#define DWORDT 3
#define DNUMBER 4
#define DMIXED 5
#define DPOSTH 6
#define DPOSTT 7
#define DZIPH 8
#define DZIPT 9
#define DDIRLET 10
#define DORD 11
#define DUNIT 12

/* ================= standardization records ===================*/

/* -- This structure stores a definition for a lexical entry -- */
typedef struct def  
{
	int Order ; /* -- the order in the list -- */
	SYMB Type ; /* -- the Input symbol -- */
	int Protect ; 
	char *Standard ; /* -- The standardization -- */
	struct def *Next ;
} DEF ;

/* -- This stores the data for a lexical entry -- */
typedef struct entry 
{
	char *Lookup ; /* -- To match against the input word -- */
	DEF *DefList ; /* -- list of definitions and outputs for this word -- */
	struct entry *Next ;
} ENTRY ;


/* -- storage for standardization rules -- */
typedef struct keyword 
{
	SYMB *Input ; /* -- List of input symbols -- */
	SYMB *Output ; /* -- List of output symbols, 1-1 with input -- */
	SYMB Type ; /* -- The classification of the rule -- */
	SYMB Weight ;
	int Length ; /* -- The number of symbols -- */
	int hits ; /* -- if collecting statistics -- */
	int best ; /* -- if collecting statistics -- */
	struct keyword *OutputNext ;
} KW ;


typedef struct lexeme 
{
	int StartMorph ;
	int EndMorph ;
	DEF *DefList ;
	char Text[MAXTEXT] ;
} LEXEME ;

/* 2006-11-02 */
typedef struct rule_param 
{
	int num_nodes ;
	int rules_read ;
	int collect_statistics ;
	int total_key_hits ;
	int total_best_keys ;
	NODE **gamma_matrix;
	SYMB *rule_space ;
	KW ***output_link ;
	KW *key_space ;
} RULE_PARAM ;

/* -- structure used to assemble composite output -- */
typedef struct seg 
{
	SYMB sub_sym ; /* -- Used in forced standardization -- */
	int Start ; /* -- the start target position -- */
	int End ; /* -- the end position -- */
	int State ; /* -- row number of the tran table, used in clausetree -- */
	DS_Score_t Value ; /* -- the calculated value of the target segment -- */
	SYMB *Output ; /* -- the output copied from the rule -- */
	KW *Key ; /* -- the rule itself, used in clausetree construction -- */
} SEG ;

/* -- storage structure for standardization candidates -- */
typedef struct stz 
{
    DS_Score_t score ; /* -- standardization score -- */
    DS_Score_t raw_score ;
    KW *build_key ; /* -- use to collect statistics -- */
    DEF *definitions[MAXLEX] ; /* -- lexical or input definitions -- */ 
    SYMB output[MAXLEX] ; /* -- output tokens -- */
} STZ ;

/* 2006-11-02 */
typedef struct stz_param 
{
	int stz_list_size ;
	int last_stz_output ;
	double stz_list_cutoff ;
	SEG *segs ;
	STZ **stz_array ;
} STZ_PARAM ;

/* 2006-11-14 */
struct morph 
{
	int Term ;
	int TextLen ;
	char Text[MAXTEXT] ;
	DEFDEF Sym ;
} ;

typedef struct stand_param 
{
	int cur_morph ;
	int base_morph ;
	int LexNum ;
	int analyze_complete ;
	int *have_ref_att ; /* build.c (transform_rows) */
	RULE_PARAM *rules ;
	/*-- <remarks> 2009-08-13 : support multiple lexicons </remarks> --*/
	ENTRY **lexicon ;
	ENTRY **address_lexicon ;
	ENTRY **poi_lexicon ;
	/*-- <revision date='2012-06-01'> Add gaz_lexicon to be triggered on __start_state__ = MACRO </revision> --*/
	ENTRY **gaz_lexicon ;
	/*-- <revision date='2012-07-22'> Keep track of start_state </revision> --*/
	int start_state ; 
	ERR_PARAM *errors ;
	STZ_PARAM *stz_info ; /* structure created by analyze.c (create_segments) */
	DEF **default_def ;
	char **standard_fields ;
	struct morph morph_array[MAXMORPHS] ;
	SYMB best_output[MAXLEX] ;
	SYMB target[MAXLEX] ; /* -- target for Aho-Corasick -- */
	LEXEME lex_vector[MAXLEX] ;
	int cur_sym_sel[MAXLEX] ; /* -- currently selected symbol 
                                  for each lexeme -- */
	int orig_str_pos[MAXLEX] ; /* -- compression buffer -- */
	int def_cnt[MAXLEX] ; /* -- number of symbols for each lexeme -- */
	NODE registry[MAXLEX + 1] ; /* -- Aho-Corasick : offsets to output 
                                         links -- */
	DEF *best_defs[MAXLEX] ;
	DEF *def_array[MAXLEX][MAXDEF] ; /* -- the definitions for each 
                                               lexeme -- */
	SYMB comp_lex_sym[MAXLEX][MAXDEF] ; /* -- symbols for each lexeme -- */
} STAND_PARAM ;


/* ================ NON-STANDARDIZATION RECORDS ================== */

#ifndef BUILD_API

/* -- The attribute structure is used to coordinate the schema in the
   reference data with the postal attributes identified by the standardizer
   and which may be used in the user's addresses. -- */
typedef struct attribute 
{
	SYMB symbol ; 
	int comp_type ; /* -- The comparision type used in matching -- */
	int check_dir ; /* -- TRUE if a non-blank reference field is read -- */
	int num_redirects ;
	DS_Score_t m_weight ; /* -- matching weights -- */
	DS_Score_t u_weight ;
	DS_Field_t ru_fld_idx[MAX_ATT_FLDS] ; /* -- field index for unstandardized
                                     ref table -- */
	DS_Field_t rs_fld_idx[MAX_ATT_FLDS] ; /* -- field index for standardized
                                     ref table -- */
	DS_Field_t rs_off_fld_idx[MAX_ATT_FLDS] ; /* field index for official names in
									standardized ref table -- 2009-11-21 */
	SYMB redirects[MAX_REDIRECTS] ; /* -- reference fields to which this
                                        attribute should be redirected -- */
} ATTRIBUTE ;

/* -------------------------------------------------------------- 
This structure is used to store the information on each reference
record with which we attempt to match the user record 
---------------------------------------------------------------- */
typedef struct candidate 
{
	int score_card ; /*2008-12-15*/
	DS_Entity_t record ; /* -- reference database record number -- */
	int stz ; /* -- The standardization being used -- */
	int edit_distance ; /* -- for use in matching -- */
	DS_Score_t score ; /* -- score calculated in ReadScoreStandardized -- */
	PAGC_POINT position ;
	int block_face ;
	char data[BETA_BUF_SIZE] ; /* -- data for display to user - added in
                   betaref.c (ReadScoreStandardized) -- */
} CANDIDATE ; /* -- structure initialized in Index/CreateCandidate --*/

typedef struct int_candidate 
{
	DS_Entity_t record_A ;
	DS_Entity_t record_B ;
	int edit_distance_A ;
	int edit_distance_B ;
	DS_Score_t score ;
	PAGC_POINT position ;
	int stz_A ;
	int stz_B ;
	char cur_cand_data_A[BETA_BUF_SIZE] ;
	char cur_cand_data_B[BETA_BUF_SIZE] ;
} INT_CANDIDATE ;
#endif

/* ================== global record =================== */

typedef struct pagc_global 
{
	int log_init ;
	RULE_PARAM *rules ;
	DEF **default_def ;
	/*-- <revision date='2009-08-13'> Support multiple lexicons </revision> --*/
	ENTRY **addr_lexicon ; /*-- 2006-11-20 --*/
	ENTRY **poi_lexicon ;
	/*-- <revision date='2012-07-16'> gaz_lexicon </revision> --*/
	ENTRY **gaz_lexicon ; 
	DS_Handle _file_sys ;
	ERR_PARAM *process_errors ;
} PAGC_GLOBAL ;

#ifndef BUILD_API
/* <revision date='2012-04-26'>Divert approx functions to TRIE_ARRAY -- moved approx definitions to approx.c </revision>*/
typedef void * RECOGNIZER_HANDLE ;

/* ============================ main schema record ========================== */

typedef struct schema 
{
	DS_Score_t match_weight[MAXOUTSYM] ; /* match weight for each token employed in the schema */
	DS_Score_t unmatch_weight[MAXOUTSYM] ; /* the non-match weight for each */
	DS_Score_t max_score ; /* the maximum score for address matching */
	DS_Score_t score_range ; /* used in normalizing scores for addresses */
	DS_Score_t starting_cutoff ; /* initial cutoff for candidate elimination */
	DS_Score_t user_cut ; /* client-supplied cutoff */
	/*-- intersection scoring --*/
	DS_Score_t max_x_score ; /* the maximum for intersection matching */
	DS_Score_t max_m_weight ; /* used in intersection scoring for the cross street */
	DS_Score_t max_u_weight ; /* used in intersection scoring for the cross street */
	DS_Score_t x_m_weight ; /* used in intersection scoring for the cross street */
	DS_Score_t x_u_weight ; /* used in intersection scoring for the cross street */
	DS_Score_t x_range ; /* used for normalizing intersection scores */
	/*-- <revision date='2009-08-20'> Landmark scoring. </revision> --*/
	DS_Score_t max_p_score ; /* the maximum for landmark matching */
	DS_Score_t land_words_m_weight ; /* for landmark fields */
	DS_Score_t land_words_u_weight ; /* for landmark fields */
	DS_Score_t land_type_m_weight ; /* for landmark fields */
	DS_Score_t land_type_u_weight ; /* for landmark fields */
	DS_Score_t land_area_m_weight ; /* for landmark fields */
	DS_Score_t land_area_u_weight ; /* for landmark fields */
	DS_Score_t score_p_range ; /* landmark score normalization */
	DS_Metric_t lat_units ; /* size in meters of a degree of latitude */
	DS_Metric_t lon_units ; /* size in meters of a degree of longitude */
	DS_Coord_t MBR_max_X ; /* Minimal Bounding Rectangle maximum point X coordinate */
	DS_Coord_t MBR_max_Y ; /* MBR maximum point Y coordinate */
	DS_Coord_t MBR_min_X ; /* Minimum point X */
	DS_Coord_t MBR_min_Y ; /* Minimum point Y */
	int pagc_ver ; /*-- <revision date='2009-07-21'> Tracking version. </revision> --*/
	DS_Dim_t shp_typ ; /* Shape type in shapefile - point, arc */ 
	int both_sides ; /* do arcs have both sides, ie both left and right? -- used for SITE_INTERPOLATE */
	DS_Field_t from_cross_col ; /* the from cross street in the reference shapeset (for intersections) */
	DS_Field_t to_cross_col ; /* the to cross street for intersections */
	DS_Field_t alt_street_col ;  /* alternate name column in reference shapeset - for future use */
	/*-- landmark columns --*/
	int total_landmark_words ; /*-- <revision date='2009-07-26'/> --*/
	DS_Field_t landmark_alpha ;    /*-- <revision date='2009-07-19-21'> landmark columns </revision> --*/
	DS_Field_t landmark_beta ;
	DS_Field_t landmark_beta_official ;
	DS_Field_t landmark_type_alpha ; /*-- <revision date='2009-07-19'> new </revision> --*/
	DS_Field_t landmark_type_beta ;
	DS_Field_t subdistrict_alpha ; /*-- <revision date='2009-07-19'> new </revision> --*/
	DS_Field_t subdistrict_beta ;
	DS_Field_t occ_field1 ; /* -- if HAVE_OCCUPANCY -- */
	DS_Field_t occ_field2 ; /* -- if HAVE_OCCUPANCY -- */
	/*-- <revision date='2010-09-27'> Eliminate X_field and Y_field for HAVE_DBF_POSITION 
		and use following: </revision> --*/
	DS_Geo_t typ_geo_cols ;
	DS_Field_t num_geo_cols ;
	DS_Field_t geo_cols[3] ;
	/*-- DS_Field_t X_field ; DS_Field_t Y_field ; --*/
	DS_Field_t num_official_fields ; /*  if HAVE_OFFICIAL_STREET, state the number of fields */
	DS_Field_t source_id_alpha ; /* if HAVE_SOURCE_ID, dbf field 2008-17-17 */
	DS_Field_t source_id_beta ; /* if HAVE_SOURCE_ID, standard field 2008-17-17 */
#ifdef THREE_SOURCE_IDS
	DS_Field_t source_id_alpha_b ; /* if HAVE_SOURCE_ID_B, dbf field 2008-17-17 */
	DS_Field_t source_id_beta_b ; /* if HAVE_SOURCE_ID_B, standard field 2008-17-17 */
	DS_Field_t source_id_alpha_c ; /* if HAVE_SOURCE_ID_C, dbf field 2008-17-17 */
	DS_Field_t source_id_beta_c ; /* if HAVE_SOURCE_ID_C, standard field 2008-17-17 */
#endif
	int num_atts ;  /* how many address attributes */
	int have_micros ;
	int have_macros ; /* does schema have place state zip attributes? */
	int have_postal ; /* does it have, specifically, a postal attribute ? */
	DS_Entity_t last_number ;  /* last record number for beta reference */
	int RedirectDir ;  /* determine if a redirect of directional attribute is required */
	int RedirectTyp ;  /* determine if a redirect of a street type attribute is required */
	int RedirectQual ; /* determine if a redirect of a qualifier attribute is required */
	DS_Field_t number_of_beta_fields ;
	int variants ; /* how many appends have been made */
	int beta_insert ; /* are we in read or write mode */
	int little_endian ; /* is it little or big? */
	int q_depth ; /* granularity for reverse geocoding */
	PAGC_FLAG_T flags ; /* bit flags indicating what features are enabled for this schema */
	ATTRIBUTE attributes[MAXOUTSYM] ; /* array for address attributes */
	ATTRIBUTE *last_att ;  /* address of the last attribute */
	ATTRIBUTE *attribute_index[MAXOUTSYM] ; /* an index into the attributes by token */
#ifdef USE_DITTO_FIELD
	/*-- <revision date='2008-05-08'> Allocated field to store last postal code read. </revision> --*/
	char *ditto_field ;
#endif
	char *idx_nam[ MAX_INDICES ] ; /* pointers to allocated index names */
	char *beta_table_name ;
	DS_Handle _beta_factory ;
	/*-- <revision date='2012-03-27'> Remove pool handles and replace with RECOGNIZER_HANDLES </revision> --*/
	RECOGNIZER_HANDLE street_postal_trie ; /*-- <revision date='2012-03-27'>change type</revision> --*/
	RECOGNIZER_HANDLE concat_trie ;
	RECOGNIZER_HANDLE landmark_name_trie ;
} SCHEMA ;

/* ===================== build database control record ============= */

typedef struct bdb_build_param 
{
	ERR_PARAM *errors ;
	SCHEMA *schema ;
	DS_Handle _beta_attribute_interface ;
	DS_Handle _idx_db[MAX_INDICES] ;
	DS_Byte_t *shape_buf ;
} BDB_BUILD_PARAM ;


/* ============ structures for intersection search =========== */ 

typedef struct x_cand 
{
	DS_Entity_t beta_rec ;
	int reversed ;
	int strategy ;
	int stz_num ;
	int distance ;
	DS_Coord_t X ;
	DS_Coord_t Y ;
	struct x_cand *cluster ;
} X_CAND ;

typedef struct pair_read 
{
	DS_Entity_t record_1 ;
	DS_Entity_t record_2 ;
	struct pair_read *next ;
} PAIR_READ ;

typedef struct box_cell 
{
	X_CAND *splitter ;
	PAIR_READ *pair_list ;
	struct box_cell *cell_list[4] ;
} BOX_CELL ;


/* ============== structure for recording beta records read ======== */

typedef struct record_read 
{
	DS_Entity_t row_no ;
	int stz ;
	struct record_read * left ;
	struct record_read * right ;
} RECORD_READ ;

typedef struct r_r_mgr 
{
	int current_offset ;
	void *base ; /* 2010-06-24 : change to void * for flexibility */
	struct r_r_mgr *prev_block ;
	struct r_r_mgr *next_block ;
} R_R_MGR ; 

/* 2011-01-24 : keep schema linkages on hand until closure */
typedef struct schema_db_linkage 
{
	SCHEMA * linked_schema ;
	DS_Handle _linked_beta ;
	DS_Handle _linked_idx[MAX_INDICES] ;
} SCHEMA_DB_LINKAGE ;

/* ============ main matching context record ========== */

typedef struct pagc_context 
{
	SCHEMA *schema ;
	ERR_PARAM *errors ;
	int num_backlinks ; /* 2011-01-24 : keep schema linkages on hand until closure */
	SCHEMA_DB_LINKAGE * schema_backlinks[MAX_SCHEMAS] ; /* 2011-01-24 */
	DS_Handle _beta_attribute_interface ;
	DS_Handle _idx_db[MAX_INDICES] ; /* reader handles */
	int private_errs ;
	int numb_cands ;
	DS_Entity_t matched_ref_row ;
	DS_Entity_t matched_ref_row_B ;
	int strategy ;
	int next_free ;
	int query_begin ;
	int concat_reverse ; /* use for intersection concat keys */
	int collect_all ;
	INT_CANDIDATE ** int_cand_list ;
	char **street_words ;
	char **landmark_words ; /* 2009-09-17 */
	CANDIDATE **cand_list ;
/*   int numb_x_cands ;  2010-06-26, no longer needed */
/*   X_CAND *x_cand_list ;  2010-06-26, no longer needed */
	STAND_PARAM *standard_p ;
	STAND_PARAM *standard_p_B ;
	double seg_length[DS_MAX_VERTICES] ;
	DS_Score_t cand_list_cutoff ;
	DS_Score_t intersection_cutoff ;
	R_R_MGR *r_r ; /* -- allocated memory -- */
	R_R_MGR *pair_r_r ; /* 2010-06-26 new structure */
	R_R_MGR *box_r_r ; /* 2010-06-26 new structure */
	R_R_MGR *x_cand_r_r ; /* 2010-06-26 new structure */
	RECORD_READ **rec_hash_tab ; /* -- allocated memory -- */ 
	DS_Byte_t *shape_buf ;
	char transfer_buf[MAX_TRANSFER_BUF_SIZE] ;
/*   PAIR_READ *pair_buf ;  2010-06-26, no longer needed */
	BOX_CELL **overlap_buf ; /* -- allocated memory -- */
	BOX_CELL *box_root ; 
/*   BOX_CELL *box_cell_array ;   2010-06-26, no longer needed */
/*   int num_box_cells ;  2010-06-26, no longer needed */
} PAGC_CONTEXT ;


/* =============== client interface record (not exposed) =========== */

typedef struct client_handle 
{
	int handle_check ;
	int num_contexts ;
	int num_schemas ;
	PAGC_GLOBAL *global_record ;
	FILE *misc_aux_stream ;
	STAND_PARAM *misc_stand ;
	PAGC_CONTEXT *context_records[MAX_CONTEXTS] ;
	SCHEMA *schema_records[MAX_SCHEMAS] ;
} CLIENT_HANDLE ;

/* 2009-07-27 : structure for landmark scoring */
typedef struct cand_score_params 
{
	int target_word_cnt ;
	int postal_idx ; /* which requester field has the postal */
	int city_idx ;
	int prov_idx ;
	int type_idx ;
	int subdistrict_idx ;
	int soundex_approx ;
	char **words_buf ;
	DS_Score_t *word_weights ; /* 2009-11-20 */
} CAND_SCORE_PARAMS ;

/*-- <revision date='2012-08-20'> New definition </revision> --*/
typedef struct check_macro
{
	int in_line_string ;
	SYMB last_checked ;
	int __primary_in_reference__[4] ; // Each reference present marks this
	int __secondary_in_reference__[4] ;
	int action_alt_left ;
#ifdef WITH_ALT_LEFT_RIGHT
	int action_alt_right ;
#endif
	char __field_primary__[4][MAXSTRLEN] ;
	char __field_secondary__[4][MAXSTRLEN];
	char __field_alternate_primary__[4][MAXSTRLEN];
	char __field_alternate_secondary__[4][MAXSTRLEN];
	char __unstandard_left__[MAXSTRLEN] ;
	char __unstandard_right__[MAXSTRLEN] ;
	char __unstandard_alt_left__[MAXSTRLEN] ;
	char __unstandard_alt_right__[MAXSTRLEN] ;
	/*-- <remarks> These should be in the lexicon consulted for MACRO </remarks> --*/
	const char __dummies__[4][6] ;
} CHECK_MACRO ;

/* ===================== prototypes for functions ================ */

/* -- approx.c -- */
/*-- <revision date='2012-04-26'>Divert approx functions to TRIE_ARRAY</revision> --*/
RECOGNIZER_HANDLE _new_recognizer_(DS_Handle, DS_Index_Link , int, int, const char *, int, ERR_PARAM *);
void _free_recognizer_(RECOGNIZER_HANDLE);
int _insert_recognizer_key_(RECOGNIZER_HANDLE, char *, char *) ;
int _recognize_approx_(RECOGNIZER_HANDLE, PAGC_CONTEXT *, char *, char *, int, char **, int , int , CAND_SCORE_PARAMS *) ;

#endif

/* -- standard.c -- */
int standardize_field(STAND_PARAM *, char *, int) ;
void close_stand_context(STAND_PARAM *) ;
STAND_PARAM *init_stand_context(PAGC_GLOBAL *, ERR_PARAM *, int) ;
void close_stand_process(PAGC_GLOBAL *) ;
/* 2009-08-13 : support multiple lexicons */
int init_stand_process(PAGC_GLOBAL *, const char *, const char *, const char *, const char *) ;

/* -- tokenize.c -- */
void initialize_morphs(STAND_PARAM *) ;
int setup_default_defs(PAGC_GLOBAL *) ;
void remove_default_defs(PAGC_GLOBAL *) ;
int process_input(STAND_PARAM *) ;
int new_morph(STAND_PARAM *, DEFDEF, const char *, int) ;
void set_term(STAND_PARAM *, int, const char *);

int is_symb_on_list(SYMB, SYMB *) ;
int find_def_type(DEF *, SYMB *) ;

/* -- export.c -- */
void stuff_fields(STAND_PARAM *) ;
void init_output_fields(STAND_PARAM *, int) ;
int sym_to_field(SYMB) ;
void send_fields_to_stream(char **, FILE *, int, int) ;

/* -- analyze.c -- */
int install_def_block_table(ENTRY **, ERR_PARAM *) ;
STZ_PARAM *create_segments(ERR_PARAM *) ;
void destroy_segments(STZ_PARAM *) ; 
int get_next_stz(STAND_PARAM *, int) ;
double get_stz_downgrade(STAND_PARAM *, int) ;
/*-- <revision date='2012-07-22'> Keep track of start_state </revision> --*/
int evaluator(STAND_PARAM *) ;
void output_raw_elements(STAND_PARAM *,ERR_PARAM *) ;

/* -- gamma.c -- */
void refresh_transducer(NODE *, SYMB *, NODE **) ;
int is_input_symbol(SYMB) ;
int is_output_symbol(SYMB) ;
RULE_PARAM *create_rules(const char *, PAGC_GLOBAL *) ;
void destroy_rules(RULE_PARAM *) ;
#ifdef BUILD_API
int output_rule_statistics(RULE_PARAM *, ERR_PARAM *) ;
#else
int output_rule_statistics(RULE_PARAM *, ERR_PARAM *, char *, DS_Handle) ;
#endif

/* -- lexicon.c -- */
ENTRY **create_lexicon(PAGC_GLOBAL *, const char *, const char *) ;
void destroy_lexicon(ENTRY **) ;
void destroy_def_list(DEF *) ;
ENTRY *find_entry(ENTRY **, char *) ;
DEF *create_def (SYMB, char *, int, int, ERR_PARAM *) ;

/* -- err_param.c -- */
ERR_PARAM *init_errors(PAGC_GLOBAL *, const char *) ;
void close_errors(ERR_PARAM *) ;
int empty_errors(ERR_PARAM *, int *, char *) ;
void register_error(ERR_PARAM *) ;
void send_fields_to_error(ERR_PARAM *, char **) ;

/* -- util.c -- */

FILE *open_aux_file(PAGC_GLOBAL *, const char *) ;

#ifndef BUILD_API

/* -- candform.c -- */
int sads_format_standard_fields(STAND_PARAM *, int, char *) ;
void fetch_standard_headers(char *) ;
/* 2008-07-21 sads_format_candidate : add is_parity_mismatch argument, 
   add source_identifier argument */
#ifdef THREE_SOURCE_IDS
int sads_format_candidate(PAGC_CONTEXT *, DS_Entity_t, int, char *, int, int, int, int *, char *, char *, char *) ;
#else
int sads_format_candidate(PAGC_CONTEXT *, DS_Entity_t, int, char *, int, int, int, int *, char *) ;
#endif
void cand_header_list(PAGC_CONTEXT *, int, char * ) ;
/* 2008-07-28 ols_format_candidate : new routine */
int ols_format_candidate(PAGC_CONTEXT *, DS_Entity_t, int, char *, char *, int, int, int *) ;


/* -- init.c -- */
/* 2009-08-13 : support multiple lexicons */
PAGC_GLOBAL *init_global(int, const char *, const char *, const char *, const char *, const char *, const char *) ;
void close_global(PAGC_GLOBAL *) ;
SCHEMA *init_schema(ERR_PARAM *) ;
int close_schema(SCHEMA *, ERR_PARAM *) ; /* 2011-01-22 : return error code */
PAGC_CONTEXT *init_context(PAGC_GLOBAL *, SCHEMA *, ERR_PARAM *, int, const char *) ;
void close_context(PAGC_CONTEXT *) ;


/* -- build.c -- */
int build_beta(PAGC_GLOBAL *, SCHEMA *, ERR_PARAM *, DS_Handle, DS_Handle, BDB_BUILD_PARAM *, char *, char *, DS_Entity_t, DS_Entity_t) ;

/* -- collect.c -- */
int match_address(PAGC_CONTEXT *, char *, char *, int, int) ;
int match_landmark(PAGC_CONTEXT *, char *, char *, char *, char *, int) ;
/* 2008-12-15 : add int arg to save_candidate */
int save_candidate(PAGC_CONTEXT *, DS_Entity_t, int, DS_Score_t, int, int,  char *) ;
int match_intersection( PAGC_CONTEXT *, char *, char *, char *,  char *, int) ;
int save_intersection_candidate(PAGC_CONTEXT *, DS_Entity_t, DS_Entity_t, int, int, int, int, DS_Score_t, DS_Coord_t, DS_Coord_t) ;


/* -- geocode.c -- */
DS_Entity_t locate_incident_arcs(PAGC_CONTEXT *, DS_Entity_t, int, int) ;
DS_Score_t score_arc_direction(PAGC_CONTEXT *, DS_Entity_t, PAGC_POINT *, DS_Angular_t) ;
int geocode_address_candidate(PAGC_CONTEXT *, PAGC_POINT *, int, int *, DS_Metric_t) ;
int geocode_intersection_candidate(PAGC_CONTEXT *, PAGC_POINT *, int) ;
int geocode_landmark_candidate(PAGC_CONTEXT *, PAGC_POINT *, int) ;
DS_Metric_t pyth_dist2(SCHEMA *, DS_Coord_t *, DS_Coord_t *, DS_Coord_t *, DS_Coord_t *) ;
DS_Metric_t degree_dist(DS_Metric_t, DS_Coord_t,  DS_Coord_t) ;
int collect_incident_arcs(PAGC_CONTEXT *, int, int, int) ;

/* -- score.c -- */
int read_score_stand(PAGC_CONTEXT *, DS_Score_t *, char **, DS_Entity_t, int, int *) ; 
int read_score_stand_land( PAGC_CONTEXT *, DS_Score_t *, char **, DS_Entity_t, int *, CAND_SCORE_PARAMS *) ; 
int resolve_range_direction(int *, int *, int, int, int, int) ;
DS_Score_t interpolate_weight(DS_Score_t, DS_Score_t, DS_Score_t) ;
int match_number_interval_left_right(int, int, int, int, int, int) ;
DS_Score_t normalize_score(SCHEMA *, DS_Score_t) ;
DS_Score_t normalize_landmark_score(SCHEMA *, DS_Score_t) ;
DS_Score_t max_context_score(PAGC_CONTEXT *) ;

/* -- make_sch.c -- */
void get_weight_pair(SCHEMA *, ATTRIBUTE *) ;
int build_ref_schema(SCHEMA * , DS_Handle, ERR_PARAM *, DS_Handle, const char *,  PAGC_FLAG_T) ;
ATTRIBUTE *get_att_by_symbol(SCHEMA *, SYMB) ;
int is_official(SCHEMA *, SYMB) ; /* 2009-11-23 : new function */

/* -- restore.c -- */
int restore_build_state(PAGC_GLOBAL *, SCHEMA *, const char *, int) ;
int save_build_state(SCHEMA * , const char *, ERR_PARAM *, DS_Handle) ;

/* -- shapeset.c -- */
int open_alpha_for_build(DS_Handle *, DS_Handle *, DS_Handle *,  DS_Handle , const char *, const char *, char **, ERR_PARAM *) ;
void close_alpha(DS_Handle *, DS_Handle *, DS_Handle *, ERR_PARAM *) ;
int open_positioning(SCHEMA *, DS_Handle *, char *, DS_Handle, ERR_PARAM *) ;
void set_feature_shape_type(SCHEMA *) ;
int set_matching_units(SCHEMA *) ;
void update_mbr(SCHEMA *, DS_Handle) ;

/* -- index.c -- */
BDB_BUILD_PARAM * open_build_db(SCHEMA *, ERR_PARAM *) ;
void close_build_db(BDB_BUILD_PARAM *) ;
int open_context_db(PAGC_CONTEXT *, SCHEMA *, ERR_PARAM *) ;
void close_context_db(PAGC_CONTEXT *) ;
int open_schema_db(SCHEMA *, ERR_PARAM *, DS_Handle, const char *, int) ;
int close_schema_db(SCHEMA *, ERR_PARAM *) ;
int create_schema_indices(SCHEMA *, ERR_PARAM *) ;
int open_index(SCHEMA *, ERR_PARAM *, DS_Index_Link, int) ;

/* -- indexput.c -- */
/*	<revision date='2012-03-27'>new args for insert_key and insert_concat_key */
int insert_key(BDB_BUILD_PARAM *, DS_Index_Link, char *, DS_Entity_t, char *) ;
int insert_concat_key(BDB_BUILD_PARAM *, DS_Index_Link,char *, DS_Entity_t, int, PAGC_POINT *, char *) ;
int insert_attribute_point(SCHEMA *, BDB_BUILD_PARAM *, DS_Handle, DS_Entity_t, DS_Entity_t, ERR_PARAM *) ;
int insert_shape(SCHEMA *, BDB_BUILD_PARAM *, DS_Handle, DS_Entity_t, DS_Entity_t, ERR_PARAM *err_p, PAGC_POINT *, PAGC_POINT * ) ;


/* -- indexget.c -- */
int fetch_shape(PAGC_CONTEXT *, DS_Entity_t, int *, DS_Coord_t **, DS_Coord_t **) ;
int register_candidate(PAGC_CONTEXT *, char **, DS_Index_Link, int, char *, int, CAND_SCORE_PARAMS *) ;
int read_arc_endpoints(PAGC_CONTEXT *, DS_Entity_t, PAGC_POINT *, PAGC_POINT *) ;
DS_Entity_t find_arcs_by_point(PAGC_CONTEXT *, DS_Entity_t, PAGC_POINT *, DS_Angular_t) ;
int print_beta_text(PAGC_CONTEXT *) ;
int print_index_text(PAGC_CONTEXT *, DS_Index_Link) ;
int print_shape_index(PAGC_CONTEXT *, DS_Index_Link) ;
int calc_landmark_word_weights(PAGC_CONTEXT *, int, int *, DS_Score_t *) ; 

/* -- alpharef.c -- */
int read_alpha_house(DS_Handle, DS_Entity_t, DS_Field_t, int) ;
int extract_house(const char *, int) ;
/* 2008-07-30 : add unstandard_mac_alternate arg and flag for alternate city names 
   2009-11-23 : add arrays for official name fields */
/*-- <revision date='2012-08-30'> Use check_macro </revision> --*/
int read_unstandardized(SCHEMA *, DS_Handle, DS_Entity_t, int *, int *, int *, char *, CHECK_MACRO* , char *, char **, DS_Field_t *, int *, ERR_PARAM *) ;

/* -- makebeta.c -- */	
int init_standardized_table(SCHEMA *, ERR_PARAM *) ;
int soundex_street_words(char *, char **) ; 
/* 2008-08-01 : add stand_alt_macro flag for alternate city names */
/*-- <revision date='2012-08-30'> Use check_macro </revision> --*/
int write_standardized(SCHEMA *, BDB_BUILD_PARAM *, char **, char **, CHECK_MACRO *, int, int *, char *, char *, char *, DS_Entity_t, int) ;
void do_left_saves(char **, char *, char *, char *, int) ;
int index_cross_streets(SCHEMA *, BDB_BUILD_PARAM *, char **, char **, DS_Handle, DS_Entity_t, DS_Entity_t, STAND_PARAM *, PAGC_POINT *, PAGC_POINT *, ERR_PARAM *) ;
int write_occupancy_only(SCHEMA *, DS_Handle, char **, DS_Entity_t) ;
/* 2008-08-01 : new routine to standardize alternate city names */
#ifdef WITH_ALT_LEFT_RIGHT
int write_alt_macro_only(SCHEMA *, DS_Handle, char **, int, DS_Entity_t) ;
#else
int write_alt_macro_only(SCHEMA *, DS_Handle , char **, DS_Entity_t) ;
#endif
/* 2009-07-22 : new routines to standardize and write landmark names */
int write_landmark_name_only(SCHEMA *, BDB_BUILD_PARAM *, char **, char **, char **,  DS_Entity_t, ERR_PARAM *) ;
int tokenize_landmark_words(char *, char **) ;

#endif

/* ============================ MACROS ========================== */

#define IS_BLANK( STR ) *STR == SENTINEL
#define SPACE 0x20

/* ================ ERROR MACROS ==================== */

#define LOG_DS_ERR( INTF, WHERE ) \
	ds_copy_error( INTF , WHERE -> error_buf ) ; \
	register_error( WHERE )

#define TERMINATE_INTERFACE( STATUS_REG_V , INTF , WHERE ) \
	STATUS_REG_V = ds_terminate( INTF ) ; \
	if ( STATUS_REG_V != DS_OK ) { \
		LOG_DS_ERR( INTF, WHERE ) ; \
	} \
	ds_dispose_interface( INTF )

#define LOG_MESS(STR,WHERE) \
   sprintf( WHERE -> error_buf , \
            STR ) ; \
   register_error( WHERE ) 

#define LOG_MESS1( TEMP,INSERT,WHERE) \
   sprintf( WHERE -> error_buf , \
            TEMP, \
            INSERT ) ; \
   register_error( WHERE ) 

#define LOG_MESS2( TEMP,INSERT1,INSERT2,WHERE ) \
   sprintf( WHERE -> error_buf , \
            TEMP, \
            INSERT1, \
            INSERT2 ) ; \
   register_error( WHERE ) 

#define LOG_MESS3( TEMP,INSERT1,INSERT2,INSERT3,WHERE ) \
   sprintf( WHERE -> error_buf , \
            TEMP, \
            INSERT1, \
            INSERT2 , \
            INSERT3 ) ; \
   register_error( WHERE  ) 

   
#define RET_ERR(STR,WHERE,RET) \
   LOG_MESS(STR,WHERE) ; \
   return RET

#define RET_ERR1(TEMP,INSERT,WHERE,RET) \
   LOG_MESS1(TEMP,INSERT,WHERE) ; \
   return RET

#define RET_ERR2(TEMP,INSERT1,INSERT2,WHERE,RET) \
   LOG_MESS2(TEMP,INSERT1,INSERT2,WHERE) ; \
   return RET

#define RET_ERR3(TEMP,INSERT1,INSERT2,INSERT3,WHERE,RET) \
   LOG_MESS3(TEMP,INSERT1,INSERT2,INSERT3,WHERE) ; \
   return RET

#define FATAL_EXIT exit(1)

#define FATAL_ERR( MSG ) \
   fprintf( stderr , MSG ) ; \
   FATAL_EXIT

#define CLIENT_ERR( PTR ) PTR -> next_fatal = FALSE

#define MEM_ERR(PTR,WHERE,RET) \
   if ( PTR == NULL ) {\
      RET_ERR("Insufficient Memory",WHERE,RET) ; \
   }

/* ----------- ALLOCATION MACROS ----------- */

#define PAGC_STORE_STR(DEST,SRC,WHERE,RET_VAL) \
   DEST = (char * ) malloc( sizeof( char ) * ( strlen( SRC ) + 1 ) ) ; \
   MEM_ERR(DEST,WHERE,RET_VAL) ; \
   BLANK_STRING(DEST) ; \
   strcpy(DEST,SRC) 


#define PAGC_ALLOC_STRUC(LOC,TYP,WHERE,EXIT_TYPE) \
   LOC = ( TYP * ) malloc( sizeof( TYP ) ) ; \
   MEM_ERR(LOC,WHERE,EXIT_TYPE)

#define PAGC_CALLOC_STRUC(LOC,TYP,NUM,WHERE,EXIT_TYPE) \
   LOC = ( TYP* ) calloc( (NUM) , sizeof( TYP ) ) ; \
   MEM_ERR(LOC,WHERE,EXIT_TYPE)

#define PAGC_CALLOC_2D_ARRAY(PTR,TYP,ROWS,COLS,WHERE,EXIT_TYPE) \
   { \
      TYP **temp_ptr ; \
      int row_num ; \
      PAGC_CALLOC_STRUC(temp_ptr,TYP*,ROWS,WHERE,EXIT_TYPE) ; \
      for ( row_num = 0 ; row_num < ROWS ; row_num++ ) { \
        PAGC_CALLOC_STRUC(temp_ptr[row_num],TYP,COLS,WHERE,EXIT_TYPE) ; \
      } \
      PTR = temp_ptr ; \
   }

#define FREE_AND_NULL(PTR) \
	if (PTR !=NULL)\
	{\
		free (PTR) ; \
		PTR = NULL ; \
	}

#define PAGC_DESTROY_2D_ARRAY(PTR,TYP,ROWS) \
	{ \
		int row_num ; \
		TYP *row_val ; \
		for (row_num = 0;row_num < ROWS;row_num++)\
		{\
			if ((row_val = PTR[row_num]) != NULL) \
			{\
				FREE_AND_NULL(row_val);\
			}\
		}\
		FREE_AND_NULL(PTR) ; \
	}




/* ================ FILE OPEN MACROS ==================== */

/* -- changed so not to conflict with Windows def --*/
#define PAGC_FILE_OPEN(HANDLE,FNAME,MODE,WHERE,RET) \
   if ( ( HANDLE = fopen( FNAME , \
                          MODE ) ) == NULL ) { \
      RET_ERR1( "\nCan't open: %s\n" ,FNAME,WHERE,RET) ; \
   }

#define OPEN_ALLOCATED_NAME(ALLOC_NAME,EXT,HANDLE,NAME,MODE,DS_SYS,WHERE,RET) \
   if ( ( ALLOC_NAME = ds_alloc_file_name(DS_SYS,NAME,EXT) ) == NULL ) { \
      return RET ; \
   } \
   PAGC_FILE_OPEN(HANDLE,ALLOC_NAME,MODE,WHERE,RET)



#define SPACE_APPEND_WITH_LEN( D, S , L ) \
   char_append( " " , D , S , L )

/* ================ SOUNDEX MACROS ==================== */

#define MAKE_SOUNDEX_KEY(DEST,CNT,SW) \
  BLANK_STRING(DEST); \
  for ( CNT = 0 ; CNT < MAXPHRASE ; CNT++ ) { \
     if ( SW[ CNT ][ 0 ] == SENTINEL ) break ; \
     COMMA_APPEND_WITH_LEN( DEST , SW[ CNT ] , MAXSTRLEN ) ; \
  }

/* construct concatenated keys for the concat index */
#define MAKE_CONCAT_KEY(TARGET,SOURCE_A,SOURCE_B) \
   BLANK_STRING(TARGET) ; \
   strcpy( TARGET , SOURCE_A ) ; \
   COMMA_APPEND_WITH_LEN( TARGET , SOURCE_B , MAXSTRLEN ) 

#define MAKE_CONCAT_SOUNDEX_KEY(SOURCE_A,SOURCE_B,DEST,HOLD,CNT,SW) \
   soundex_street_words( SOURCE_A, SW) ; \
   MAKE_SOUNDEX_KEY(DEST,CNT,SW) ; \
   soundex_street_words( SOURCE_B, SW) ; \
   MAKE_SOUNDEX_KEY(HOLD,CNT,SW) ; \
   COMMA_APPEND_WITH_LEN(DEST,HOLD,MAXSTRLEN)


#define RNF_SENTINEL '_'
/* 2011-01-14 : interpret initial space in a field to indicate a blank field */
#define IS_ALPHA_STR_SENTINEL(V) ( ( *V == SENTINEL ) || ( *V == RNF_SENTINEL ) || ( *V == SPACE ) )

/* --------------------------------------------------
macros for converting and verifying pagc_client args
-----------------------------------------------------*/

#define HANDLE_CHECK 1014

#define CONVERT_HANDLE( NATIVE_PTR , CLIENT_PTR ) \
   if ( CLIENT_PTR == NULL ) return FALSE ; \
   NATIVE_PTR = ( CLIENT_HANDLE * ) CLIENT_PTR ; \
   if ( NATIVE_PTR -> handle_check != HANDLE_CHECK ) return 0
   
#define CHECK_BOUNDS( ARRAY_SIZE , ARRAY_IDX ) \
   if ( ( ARRAY_IDX > ARRAY_SIZE ) || (ARRAY_IDX < 1 ) ) { \
      CLIENT_ERR( pagc_p -> global_record -> process_errors ) ; \
      RET_ERR1( "No such entity such as %d" , \
                ARRAY_IDX , \
                pagc_p -> global_record -> process_errors , \
                0 ) ; \
   }

#define CHECK_BOUNDS_ABSOLUTE( ARRAY_SIZE , ARRAY_IDX ) \
   if ( ( ARRAY_IDX >= ARRAY_SIZE ) || (ARRAY_IDX < 0 ) ) { \
      CLIENT_ERR( pagc_p -> global_record -> process_errors ) ; \
      RET_ERR1( "No such entity such as %d" , \
                ARRAY_IDX , \
                pagc_p -> global_record -> process_errors , \
                0 ) ; \
   }


#define UPDATE_SCHEMA_BOUNDS( PTR ) \
   pagc_p -> schema_records[ pagc_p -> num_schemas ] = PTR ; \
   pagc_p -> num_schemas++ ; \
   return( pagc_p -> num_schemas )

#define UPDATE_CONTEXT_BOUNDS( PTR ) \
   pagc_p -> context_records[ pagc_p -> num_contexts ] = PTR ; \
   pagc_p -> num_contexts++ ; \
   return( pagc_p -> num_contexts )

#define SCHEMA_INDEX_TO_POINTER( IDX , PTR ) \
   PTR = pagc_p -> schema_records[ IDX - 1 ]

#define CONTEXT_INDEX_TO_POINTER( IDX , PTR ) \
   PTR = pagc_p -> context_records[ IDX - 1 ]

#define LIMIT_BOUNDS( CNT, MAX ) \
   if ( CNT == MAX ) { \
      RET_ERR1( "%d exceeds maximum allowed" , \
                CNT , \
                pagc_p -> global_record -> process_errors , \
                0 ) ; \
   }




/* ================== BETA READ MACROS ================ */

#define READ_BETA_STRING(DEST,NUM) \
   DEST = ds_attribute_read_string_field( ctx_p -> _beta_attribute_interface , row_num , att -> rs_fld_idx[ NUM ] ) ; \
   if ( DEST == NULL ) return FALSE

#define READ_BETA_INT(DEST,NUM) \
   DEST = ds_attribute_read_integer_field( ctx_p -> _beta_attribute_interface , row_num , att -> rs_fld_idx[ NUM ] ) ; \
   if ( DEST == ERR_FAIL ) return FALSE

#define INT32_AS_BYTES( PTR_VAL ) \
	* ( ( int32_t * ) ( PTR_VAL ) )

#define INTEGER_AS_BYTES( PTR_VAL ) \
	* ( ( int * ) ( PTR_VAL ) )
	
#define DOUBLE_AS_BYTES( PTR_VAL ) \
	*( ( double * ) ( PTR_VAL ) )


/* ================= floating point comparison macros ======== */
#define R_ERR .00001
#define IS_DOUBLE_EQUAL( FX, FY ) ( ( fabs( FX - FY ) <= R_ERR )? TRUE : FALSE )
#define IS_DOUBLE_NOT_EQUAL(FX,FY) ( ( fabs( FX - FY ) > R_ERR )? TRUE : FALSE )
#define IS_DOUBLE_LESS(FX,FY) ( ( ( FX - FY ) < R_ERR )? TRUE : FALSE )
#define IS_DOUBLE_GREATER(FX,FY) ( ( ( FX - FY ) > R_ERR )? TRUE : FALSE )
#define IS_DOUBLE_LESS_OR_EQUAL(FX,FY) ( ( ( FX - FY ) <= R_ERR )? TRUE : FALSE )
#define IS_DOUBLE_GREATER_OR_EQUAL(FX,FY) ( ( ( FX - FY ) >= R_ERR )? TRUE : FALSE )

#endif
