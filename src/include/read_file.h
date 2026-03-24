#pragma once

#ifndef DUCKFIXGENERALH
#if __has_include("include/df_general.h")
#include "include/df_general.h"
#define DUCKFIXGENERALH 0
#elif __has_include("df_general.h")
#include "df_general.h"
#define DUCKFIXGENERALH 1
#elif __has_include("src/include/df_general.h")
#include "src/include/df_general.h"
#define DUCKFIXGENERALH 2
#endif
#endif

#ifndef DUCKFIXLOADH
#if __has_include("include/df_load.h")
#include "include/df_load.h"
#define DUCKFIXLOADH 0
#elif __has_include("df_load.h")
#include "df_load.h"
#define DUCKFIXLOADH 1
#elif __has_include("src/include/df_load.h")
#include "src/include/df_load.h"
#define DUCKFIXLOADH 2 
#endif
#endif

#ifndef STDIOH
#include <stdio.h>
#define STDIOH 0
#endif

#ifndef STDLIBH
#include <stdlib.h>
#define STDLIB 0
#endif


#ifndef NEXTCOMMA
#define NEXTCOMMA() \
for(;ii < end_ln; ii++) { \
  if (sf[ii] == ',') { break;  \
  } else if ((sf[ii] == ' ') || (sf[ii] == '\t')) { \
  } else if (sf[ii]=='\"') { \
    ii = get_end_quote("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='[') { \
    ii =  get_end_bracket("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='{') { \
    ii =  get_end_brace("nextcomma", sf, ii, end_ln); \
  } \
} \
ii++ 
#endif

#ifndef NEXTCHARSEP
#define NEXTCHARSEP() \
for(;ii < end_ln; ii++) { \
  if (sf[ii] == dfl->char_sep) { break;  \
  } else if ((sf[ii] == ' ') || (sf[ii] == '\t')) { \
  } else if (sf[ii]=='\"') { \
    ii = get_end_quote("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='[') { \
    ii =  get_end_bracket("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='{') { \
    ii =  get_end_brace("nextcomma", sf, ii, end_ln); \
  } \
} \
ii++ 
#endif

DF_field_list *create_blank_field_list(DF_config_file *dfc, int verbose);
int delete_field_list(DF_field_list **p_dfl, int verbose); 
DF_field_list *generate_field_list(char *tgt_filename, DF_config_file *dfc, char char_sep, int verbose, int standard_vector_size, 
  long long int start_byte, long long int end_byte, char*ignore_line_text, char*keep_line_text);
int PRINT_dfl(DF_field_list *dfl);
int update_field_list_on_field(long int iline, int onfield, char*sf, iStr st_fld, iStr end_fld, DF_config_file *dfc, DF_field_list *dfl, int verbose);
iStr get_next_newln(char *sf, iStr st, iStr nmax, int verbose);
int get_multi_equals_bounds(char *sf, iStr st_eq, iStr nlen, iStr*p_st_v, iStr*p_end_v, char on_eq, char on_sp);
int clear_m_visited(DF_config_file *dfc);
int confirm_txt_exists(int lntxt, const char*seektxt, const char*sf, iStr st, iStr end);
int test_replace_field_list(DF_config_file *dfc, DF_field_list **p_dfl, int verbose);
