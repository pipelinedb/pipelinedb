
#ifndef PAGC_STD_API_H
#define PAGC_STD_API_H

#define BUILD_API

typedef struct LEXICON_s {
    ENTRY **hash_table;
    ERR_PARAM *err_p;
} LEXICON;

typedef struct RULES_s {
    int ready;
    int rule_number;
    int last_node;
    RULE_PARAM *r_p;
    ERR_PARAM *err_p;
    NODE **Trie;
    SYMB *rule_end ;
    SYMB *r ;
} RULES;

typedef struct STANDARDIZER_s {
    PAGC_GLOBAL *pagc_p;
    STAND_PARAM *misc_stand;
    ERR_PARAM *err_p;
} STANDARDIZER;

typedef struct STDADDR_s {  // define as required
   char *building;
   char *house_num;
   char *predir;
   char *qual;
   char *pretype;
   char *name;
   char *suftype;
   char *sufdir;
   char *ruralroute;
   char *extra;
   char *city;
   char *state;
   char *country;
   char *postcode;
   char *box;
   char *unit;
} STDADDR;

LEXICON * lex_init();
int lex_add_entry(LEXICON *lex, int seq, char *word, char
*stdword, SYMB token);
void lex_free(LEXICON *lex);

RULES *rules_init();
int rules_add_rule(RULES *rules, int num, int *rule);
int rules_add_rule_from_str(RULES *rules, char *rule);
int rules_ready(RULES *rules);
void rules_free(RULES *rules);

STANDARDIZER *std_init();
int std_use_lex(STANDARDIZER *std, LEXICON *lex);
int std_use_gaz(STANDARDIZER *std, LEXICON *gaz);
int std_use_rules(STANDARDIZER *std, RULES *rules);
int std_ready_standardizer(STANDARDIZER *std);
void std_free(STANDARDIZER *std);

STDADDR *std_standardize_one(STANDARDIZER *std, char *address_one_line, int options);

STDADDR *std_standardize_mm(STANDARDIZER *std, char *micro, char *macro, int options);

STDADDR *std_standardize(STANDARDIZER *std, char *address, char *city, char *state, char *postcode, char *country, int options);

void stdaddr_free(STDADDR *stdaddr);
void print_stdaddr(STDADDR *stdaddr);

#endif
