#pragma once

#ifndef DUCKFIXGENERALH
#include "include/df_general.h"
#define DUCKFIXGENERALH 0
#endif

#ifndef DUCKFIXLOADH
#include "include/df_load.h"
#define DUCKFIXLOADH 0
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
DF_field_list *generate_field_list(char *tgt_filename, DF_config_file *dfc, char char_sep, int verbose, int standard_vector_size);
int PRINT_dfl(DF_field_list *dfl);
int update_field_list_on_field(long int iline, int onfield, char*sf, iStr st_fld, iStr end_fld, DF_config_file *dfc, DF_field_list *dfl, int verbose);
iStr get_next_newln(char *sf, iStr st, iStr nmax, int verbose);
