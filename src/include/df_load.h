#pragma once


#ifndef STDIOH
#include <stdio.h>
#define STDIOH 0
#endif

#ifndef DUCKFIXGENERALH
#include "include/df_general.h"
#define DUCKFIXGENERALH 0
#endif


//      printf("ERROR(%s--%s) ", (char*) sx, (char*) stt);               \
//      printf("sf[ii=%ld]=\'%c\'," (long int) ii, (char) sf[ii]);       \
//      printf(" invalid here.   ");                                     \
//      printf("sf[%ld:%ld]=", (long int) st, (long int) ii+1);          \
//      printf("\"%*.s\".", (long int) ii+1-st, sf + st);                \
//

#ifndef PUSH_TO_CHAR_WO
#define PUSH_TO_CHAR_WO(sx,cx) \
  for(;ii < nmax;ii++) {                                               \
    if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {     \
    } else if (sf[ii] == (cx)) {                                       \
      break;                                                                 \
    } else {                                                           \
      printf("ERROR(%s--%s) ", (char*) sx, (char*) stt);               \
      printf("sf[ii=%ld]=", (long int) ii);                            \
      printf("\'%c\'", (char) (sf[ii]));                               \
      printf(" invalid here.   ");                                     \
      printf("sf[%ld:%ld]=", (long int) st, (long int) ii+1);          \
      printf("\"%*.s\".", (long int) ii+1-st, sf + st);                \
      return(-1);                                                      \
    }                                                                  \
  }                                                                    \
  ii += 0 
#endif

#ifndef PUSH_TO_CHAR 
#define PUSH_TO_CHAR(cx) \
  for(;ii < nmax;ii++) {                                               \
    if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {     \
    } else if (sf[ii] == (cx)) {                                       \
      break;                                                           \
    } else {                                                           \
    }                                                                  \
  }                                                                    \
  ii += 0 
#endif


int delete_DF_Schema(DF_Schema *dfs, int verbose);
iStr get_end_brace(char *name_str, char *ast, iStr on_i, iStr nmax);
iStr get_end_bracket(char *name_str, char *ast, iStr on_i, iStr nmax);
iStr get_end_quote(char *name_str, char *ast, iStr on_i, iStr nmax);
iStr get_end_number(char *prstr, char* sf, iStr ii, iStr nmax);
//iStr get_end_quote(char* vstr, char *in_str, iStr on_i, iStr nmax);
int get_next_comma(char* assignment, char *sf, iStr on_i, iStr nmax);
int old_str_eq(const char *mst, const char *challengestr, iStr lencstr);
iStr load_file_to_str(char **out_p_sf, char *json_filename, int verbose);
//int get_next_key(char* assignment, char *sf, iStr nmax, iStr st, iStr end, iStr *p_st, iStr *p_end);
iStr get_next_key(char* assignment, char *sf, iStr st, iStr nmax, int verbose);
iStr get_value_bounds(char* assignment, char*sf, iStr key_loc, iStr nmax, int verbose, iStr *p_vst, iStr *p_vend);
iStr find_key(const char *seek_key, iStr len_key, char*sf, iStr st, iStr len_sf, int verbose);
int get_n_schema(char *assignment, char *sf, iStr on_i, iStr nmax, int verbose);
int delete_schemas(int n_schemas, DF_Schema **p_schemas, int verbose);
int delete_config_file(DF_config_file **pdfc, int verbose);
DF_Schema *create_blank_schemas(int n_schmeas);
DF_config_file *new_config_file();
DF_config_file *get_config_file(char *sf, iStr on_i, iStr nmax, int verbose);
void PRINT_dfc(DF_config_file *dfc);
int delete_fix_fields(int n_fix_fields, DF_Fix_Field **p_fxs, int verbose);
int populate_fixfield(iStr i_fvals_start, iStr nmax, char* sf, int verbose,
  int i_onfxs, DF_Fix_Field **dffs);
int populate_fix_fields(DF_config_file **p_dfc, char*sf, iStr on_i, iStr nmax, int verbose);
int correct_brace(char *sf, iStr *p_st, iStr *p_end, iStr nmax);
int populate_encode(char *assignment, DF_Fix_Field*dff, int i_onfxs, char*sf, iStr i_fvals_start, iStr nmax, int verbose);
