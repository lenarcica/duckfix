////////////////////////////////////////////////////////////////////////////
/// Duckdb Table Bind functions
///
///  -- Alan Lenarcic 2026-02-04
///  lenarcic@post.harvard.edu
///  GPLv2 Licensed.  (consider rolling your own code)
///
///  Note that DuckDB Bind functions are global and "first" thing called by
///   a table function.
///
///  Here that means in this case the bind function needs to
///    a. Read the JSON configuration script for data on all FIX fields and other schemas. (DF_config_file *dfc)
///    b. Read the entire sample CSV through once to measure how often fields actually occur (DF_field_list *dfl)
///    c. Create a schema for the table including all of the columns in order that will make it up.
///
///  The BIND then calls INIT (or a number of Threads of LOCAL_INIT), which then will call a "main function" multiple
///   times until a "chunk is complete"
///
#ifndef DUCKDBEXTENSIONH
#include "duckdb_extension.h"
#define DUCKDBEXTENSIONH 0
//#ifndef duckdb_ext_api
//#define DUCKDB_EXTENSION_EXTERN extern duckdb_ext_api_v1 duckdb_ext_api;
//#endif
#endif

#ifndef DUCKFIXLOADH
#define DUCKDFIXLOADH 0
#include "include/df_load.h"
#endif

#ifndef DUCKFIX_READ_FILEH
#include "include/read_file.h"
#define DUCKFIX_READ_FILEH
#endif
#ifndef DUCKFIXTABLETYPESH
#include "include/df_table_types.h"
#define DUCKFIXTABLETYPESH 0
#endif

DUCKDB_EXTENSION_EXTERN

df_bind_data* create_null_bind_data() {
  df_bind_data *df_bd = (df_bind_data*) duckdb_malloc(sizeof(df_bind_data)+1);
  df_bd->verbose = 0;  df_bd->dfl = NULL; df_bd->dfc = NULL;
  df_bd->ignore_line_text = NULL; df_bd->keep_line_text = NULL; df_bd->file_name = NULL;
  df_bd->start_byte = 0;  df_bd->end_byte = -1;
  df_bd->cardinality = 0;  df_bd->DONE = 0;  df_bd->report_bust = 0; df_bd->report_line = 0;
  return(df_bd);
}

void destroy_df_extra_info(void *v_df_xi) {
  #ifdef DEBUG_MODE
  printf("destroy_df_extra_info() we have now called. \n");
  #endif
  df_extra_info *df_xi = (df_extra_info*) v_df_xi;
  //printf("df_bind.c->destroy_df_extra_info() start. \n");
  if (df_xi == NULL) { return; }
  if (df_xi->verbose > 0) {
    printf("destroy_df_xtra_info -- starting. \n");
  }
  int verbose = df_xi->verbose;
  //if (df_xi->file_name != NULL) { free(df_xi->file_name); df_xi->file_name = NULL; }
  if (verbose >= 1) {
    printf("destroy_df_extra_info() deleting df_xi now. \n");
  }
  duckdb_free(df_xi);  df_xi = NULL;
  if (verbose >= 1) {
    printf("destroy_df_extra_info() all concluded df_xi was non null. \n");
  }
}

int copy_destroy_bind(df_bind_data **p_df_bd, const char *ntype) {
  df_bind_data *df_bd = (df_bind_data*) p_df_bd[0];
  printf("copy_destroy_bind(%s) initiated with  df_bd->verbose=%d. \n", ntype, (int) df_bd->verbose);
  
  //df_bind_data *ndf_bd = (df_bind_data*) duckdb_malloc(sizeof(df_bind_data));
  df_bind_data *ndf_bd = create_null_bind_data();
  ndf_bd->verbose = df_bd->verbose;
  ndf_bd->cardinality = df_bd->cardinality;  ndf_bd->DONE = df_bd->DONE;
  ndf_bd->dfc=NULL; ndf_bd->file_name=NULL; ndf_bd->dfl = NULL; ndf_bd->ignore_line_text=NULL;
  ndf_bd->keep_line_text = NULL;
  int ttc = 0;
  if (df_bd->dfc != NULL) {
    ttc = test_replace_config_file(&df_bd->dfc, 2); ndf_bd->dfc=df_bd->dfc;
  } 
  if (df_bd->dfl != NULL) {
    ttc = test_replace_field_list(&df_bd->dfl, 2); ndf_bd->dfl=df_bd->dfl;
  } 
  df_bd->dfc=NULL; df_bd->dfl = NULL;


  if (df_bd->file_name != NULL) {
    printf("copy_destroy_bind(%s): clear file_name; \n", ntype);
    test_replace_string(&df_bd->file_name, strlen(df_bd->file_name), "copy_destroy_bind(df_bd->file_name)", 1);
  }
  ndf_bd->file_name = df_bd->file_name; df_bd->file_name = NULL;

  if (df_bd->ignore_line_text != NULL) {
    printf("clearing ignore_line_text(%s); \n", ntype);
    test_replace_string(&df_bd->ignore_line_text, strlen(df_bd->ignore_line_text), "copy_destroy_bind(df_bd->ignore_line_text)", 1);
  }
  ndf_bd->ignore_line_text = df_bd->ignore_line_text; df_bd->ignore_line_text = NULL;
  ndf_bd->report_bust = df_bd->report_bust; ndf_bd->report_line=df_bd->report_line;

  if (df_bd->fix35array != NULL) {
    printf("clearing fix35array(%d); \n", df_bd->len_fix35array);
    test_replace_string(&df_bd->fix35array, df_bd->len_fix35array, "copy_destroy_bind(df_bd->ignore_line_text)", 1);
  }
  ndf_bd->fix35array = df_bd->fix35array;  ndf_bd->len_fix35array = df_bd->len_fix35array;  df_bd->len_fix35array = 0;
  df_bd->fix35array = NULL;

  if (df_bd->keep_line_text != NULL) {
    printf("clearing keep_line_text(%s); \n", ntype);
    test_replace_string(&df_bd->keep_line_text, strlen(df_bd->keep_line_text), "copy_destroy_bind(df_bd->keep_line_text)", 1);
  }
  ndf_bd->keep_line_text = df_bd->keep_line_text; df_bd->keep_line_text = NULL;
  printf("copy_destroy_bind(%s) finished. Now free df_bd\n", ntype);
  destroy_df_bind_data( (void*) df_bd); df_bd = NULL;
  printf("copy_destroy_bind(%s) we freed df_bd, re-attaching ndf_bd. \n", ntype);
  p_df_bd[0] = ndf_bd;
  printf("copy_destroy_bind(%s) returning from destroy. file_name = %s. \n", 
     ntype, p_df_bd[0]->file_name==NULL ? "NULL FILENAME" : p_df_bd[0]->file_name);
  return(1);

}
void destroy_df_bind_data(void *v_df_bd) {
  if (v_df_bd == NULL) { return; }
  df_bind_data *df_bd = (df_bind_data*) v_df_bd;
  //duckdb_destroy_result(ta_bd->p_result);
  //duckdb_free(ta_bd->p_result);  ta_bd->p_result = NULL;
  int verbose = df_bd->verbose;
  int DONE = df_bd->DONE;
  //verbose = 4;
  if (verbose >= 1) {
    printf("destroy_df_bind_data(DONE=%d) we have been called. \n", DONE);
  }
  if (verbose > 0) {
    printf("destroy_df_bind_data INTIALIZING(%s,%s)\n",
      df_bd->dfc != NULL ? "dfc is not null" : "dfc is null",
      df_bd->dfl != NULL ? "dfl is not null" : "dfl is null");
  }
  if (df_bd->file_name != NULL) { if (verbose >=1 ) { printf("free df_bd->file_name\n"); } free(df_bd->file_name); df_bd->file_name=NULL; }
  if (df_bd->dfc != NULL) { delete_config_file(&df_bd->dfc, verbose-1); df_bd->dfc = NULL; }
  if (df_bd->dfl != NULL) { delete_field_list(&df_bd->dfl, verbose-1); df_bd->dfl = NULL; }
  if (df_bd->ignore_line_text != NULL) { if (verbose >= 1) { printf("free_df_bd->ignore_line_text\n"); } free(df_bd->ignore_line_text); df_bd->ignore_line_text = NULL; }
  if (df_bd->keep_line_text != NULL) { if (verbose >= 1) { printf("free df_bd->keep_line_text\n"); } free(df_bd->keep_line_text); df_bd->keep_line_text = NULL; }
  if (df_bd->fix35array != NULL) { if (verbose >= 1) { printf("free df_bd->fix35array\n"); } free(df_bd->fix35array); df_bd->fix35array = NULL; }
  if (df_bd->verbose > 0) {
    printf("df_bind.c->destroy_df_bind_data(DONE=%d) has been called I hope you didn't need that data. \n", DONE);
  }
  if (verbose > 3) {
    printf("df_bind.c->destroy_df_bind_data(DONE=%d) are we all done?\n", DONE);
  } 
  duckdb_free(df_bd); df_bd = NULL;
  if (verbose >= 1) {
    printf("df_bind.c->destory_df_bind_data(DONE=%d): df_bd has been freed. \n", DONE);
  }
  return; 
}

#define my_dbv_clear() \
  if (verbose >= 1) { printf("my_dbv_clear(%s,v=%d) starting. \n", stt, verbose);  }                         \
  if (df_file_name != NULL) { duckdb_destroy_value(&df_file_name); df_file_name=NULL; }                      \
  if (df_json_file_name != NULL) { duckdb_destroy_value(&df_json_file_name); df_json_file_name=NULL; }       \
  if (df_verbose != NULL) { duckdb_destroy_value(&df_verbose); df_verbose=NULL; }                            \
  if (df_ignore_line_text != NULL) { duckdb_destroy_value(&df_ignore_line_text); df_ignore_line_text=NULL; } \
  if (df_keep_line_text != NULL) { duckdb_destroy_value(&df_keep_line_text); df_keep_line_text=NULL; }       \
  if (df_start_byte != NULL) { duckdb_destroy_value(&df_start_byte); df_start_byte=NULL; }                   \
  if (df_end_byte != NULL) { duckdb_destroy_value(&df_end_byte); df_end_byte=NULL; }                         \
  if (verbose >= 1) { printf("my_dbv_clear ---- about to destroy df_fix_sep,df_char_sep.\n"); }              \
  if (df_fix_sep != NULL) { duckdb_destroy_value(&df_fix_sep); df_fix_sep=NULL; }                            \
  if (df_char_sep != NULL) { duckdb_destroy_value(&df_char_sep); df_char_sep=NULL; }                         \
  if (df_report_bust != NULL) { duckdb_destroy_value(&df_report_bust); df_report_bust=NULL;}                 \
  if (df_report_line != NULL) { duckdb_destroy_value(&df_report_line); df_report_line=NULL;}                 \
  if (df_fix35array != NULL) { duckdb_destroy_value(&df_fix35array); df_fix35array=NULL; }                   \
  if (df_fix35keep != NULL) { duckdb_destroy_value(&df_fix35keep); df_fix35keep = NULL; }                    \
  if (df_default_date != NULL) { duckdb_destroy_value(&df_default_date); df_default_date = NULL; }           \
  df_file_name = NULL

#define my_dbp_clear() \
  if (file_name != NULL) { duckdb_free(file_name); file_name = NULL;                  }                      \
  if (json_file_name != NULL) { duckdb_free(json_file_name); json_file_name = NULL; }                        \
  if (keep_line_text != NULL) {   duckdb_free(keep_line_text); keep_line_text = NULL; }                      \
  if (ignore_line_text != NULL) { duckdb_free(ignore_line_text); ignore_line_text = NULL; }                  \
  if (v_char_sep != NULL) { duckdb_free(v_char_sep);  v_char_sep = NULL; }                                   \
  if (v_fix_sep != NULL) { duckdb_free(v_fix_sep);  v_fix_sep = NULL; }                                      \
  if (ddbv_fix35array != NULL) { duckdb_free(ddbv_fix35array); ddbv_fix35array = NULL; }                     \
  if (v_fix35keep != NULL) { duckdb_free(v_fix35keep); v_fix35keep = NULL; }                                 \
  if (v_default_date != NULL) {free(v_default_date); v_default_date=NULL; }                                  \
  keep_line_text=NULL


//my_dbp_clear();                                                                                            
#define my_clear() \
  if (verbose >= 1) { printf("my_clear(%s,v=%d) starting. \n", stt, verbose); }                              \
  my_dbv_clear();                                                                                            \
  if (dfc != NULL) {  delete_config_file(&dfc,verbose); }                                                    \
  if (dfl != NULL) {  delete_field_list(&dfl,verbose); }                                                     \
  if (json_sf != NULL) { if(verbose >= 1) { printf("duckfix_bind->freeing json_sf\n");   }                   \
     free(json_sf); json_sf = NULL; }                                                                        \
  if (verbose >= 1) { printf("my_clear(%s,v=%d) finished. \n", stt, verbose); }                              \
  if (fix35array != NULL)  { free(fix35array); fix35array = NULL; }                                          \
  if (v_default_date != NULL) {free(v_default_date); v_default_date=NULL; }                                  \
  dfc=NULL; dfl=NULL

void duckfix_bind(duckdb_bind_info b_info) {
  #ifdef DEBUG_MODE 
    printf("duckfix_bind:: Called, we have DEBUG_MODE is defined.\n");
  #endif
  #ifndef DEBUG_MODE
    printf("duckfix_bind: Note DEBUG_MODE was not defined. \n");
  #endif
  char stt[500]; 
  sprintf(stt, "df_bind.c->duckfix_bind(): ");
  int32_t verbose = 0;
  df_extra_info *x_info = duckdb_bind_get_extra_info((duckdb_bind_info) b_info);
  #ifdef DEBUG_MODE
  verbose = x_info->verbose;
  if (x_info != NULL) {
    vpt(1, "df_bind.c->duckfix_bind() we got x_info is not null. \n");
  } else {
    vpt(1, "df_bind.c->duckfix_bind() we got x_info is null. \n");
  }
  #endif
  //duckdb_connection ddb_con = x_info->ddb_con;
  duckdb_value df_verbose = duckdb_bind_get_named_parameter(b_info, "verbose");
  verbose = 0;
  if (df_verbose == NULL) {
  } else if (!duckdb_is_null_value(df_verbose)) {
    verbose = duckdb_get_int32(df_verbose);
  } else {
    #ifdef DEBUG_MODE
      printf("df_bind.c->duckfix_bind(): note that verbose is not supplied. \n");
    #endif
  }
  vpt(1, "--df_bind.c -- Note we received verbose = %d. \n", verbose); 
  if (df_verbose != NULL) { duckdb_destroy_value(&df_verbose); df_verbose=NULL; }
  vpt(1, "--df_bind.c -- result of destroy df_verbose is done. \n");
  DF_config_file *dfc = NULL;  DF_field_list *dfl = NULL;

  duckdb_value df_char_sep = NULL;  
  duckdb_value df_file_name = NULL; duckdb_value df_json_file_name=NULL;
  duckdb_value df_start_byte=NULL; duckdb_value df_end_byte = NULL;
  duckdb_value df_keep_line_text=NULL; duckdb_value df_ignore_line_text=NULL;
  duckdb_value df_fix_sep=NULL;  duckdb_value df_report_bust = NULL;  int report_bust = 0;
  duckdb_value df_report_line = NULL; int report_line = 0;
  duckdb_value df_fix35array = NULL;  duckdb_value df_fix35keep = NULL;  
  duckdb_value df_default_date = NULL; 
  char *v_char_sep = NULL; char *v_fix_sep = NULL; 
  char *file_name=NULL; char * json_file_name=NULL; char *v_fix35keep = NULL; char fix35keep = '\0';
  char *ignore_line_text=NULL; char*keep_line_text=NULL; char * ddbv_fix35array = NULL;
  char *fix35array = NULL; int len_fix35array = 0;
  char *v_default_date = NULL; int len_default_date = 0;  short default_date[3];

  char fix_sep = '\0'; char char_sep='\0';
  // ll, long-lived copies that will live on stack here in the function;
  char ll_file_name[600]; char ll_json_file_name[600];  ll_file_name[0] = '\0'; ll_json_file_name[0] = '\0';
  int len_file_name = 0; int len_json_file_name = 0;
  char ll_ignore_line_text[105]; char ll_keep_line_text[105];
  ll_ignore_line_text[0] = '\0'; ll_keep_line_text[0] = '\0';
  int len_ignore_line_text = 0; int len_keep_line_text = 0;

  // Extra reporting options
  df_report_bust = duckdb_bind_get_named_parameter(b_info, "report_bust");
  report_bust = 0;
  if (df_report_bust == NULL) {
  } else if (!duckdb_is_null_value(df_report_bust)) {
    report_bust = duckdb_get_int32(df_report_bust);
  } 
  if (df_report_bust != NULL) { duckdb_destroy_value(&df_report_bust); df_report_bust=NULL; }
  df_report_line = duckdb_bind_get_named_parameter(b_info, "report_line");
  report_line = 0;
  if (df_report_line == NULL) {
  } else if (!duckdb_is_null_value(df_report_line)) {
    report_line = duckdb_get_int32(df_report_line);
  } 
  if (df_report_line != NULL) { duckdb_destroy_value(&df_report_line); df_report_line=NULL; }

  char *json_sf = NULL;
  df_start_byte = duckdb_bind_get_named_parameter(b_info, "start_byte");
  int64_t start_byte = 0;
  if (verbose >= 1) {
    printf("df_bind.c->duckfix_bind(): looking for start_byte input. \n");
    if (df_start_byte == NULL) {
      printf("df_bind.c->duckfix_bind() -- db_start_byte is null. \n");
    } else if (duckdb_is_null_value(df_start_byte)) {
      printf("df_bind.c->duckfix_bind() -- start_byte is null. \n");
    } else {
      printf("df_bind.c->duckfix_bind() -- start_byte is non null. \n");
    }
  }
  if ((df_start_byte != NULL) && (!duckdb_is_null_value(df_start_byte))) {
    start_byte = duckdb_get_int64(df_start_byte);
    if (verbose >= 1) {
      printf("df_bind.c->duckfix_bind(): we read that start_byte =%lld. \n", (long long int) start_byte);
    }
  }
  
  df_end_byte = duckdb_bind_get_named_parameter(b_info, "end_byte");
  int64_t end_byte = -1;
  if ((df_end_byte != NULL) && (!duckdb_is_null_value(df_end_byte))) {
    end_byte = duckdb_get_int64(df_end_byte);
  }

  idx_t standard_vector_size = duckdb_vector_size();
  #ifdef DEBUG_MODE
  if (verbose >= 1) {
    sprintf(stt, "df_bind.c->duckfix_bind(v=%ld,sb=%lld,eb=%lld): ", (long int) verbose, (long long int) start_byte, (long long int) end_byte);
  }
  if (verbose >= 2) {
    printf("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\n");
    sprintf(stt, "BBB duckfix_bind(v=%ld): ", (long int) verbose);
    vpt(2, " -- Init. \n");
  }
  vpt(2, " We have received standard_vector_size=%ld. looking for char_sep.\n", (long int) standard_vector_size);
  #endif

  df_char_sep = duckdb_bind_get_named_parameter(b_info, "char_sep");
  vpt(2, " We are looking to charge char_sep. \n");
  if (df_char_sep == NULL) {
    vpt(2, "-- we had df_char_sep was complete NULL.\n");
  } else if (duckdb_is_null_value(df_char_sep)) {
    vpt(2, "-- we have a null value for df_char_sep.\n");
  } else {
    vpt(2, "-- we have non null value df_char_sep, trying to get varchar from it. \n");
    v_char_sep = duckdb_get_varchar( (duckdb_value) df_char_sep);
    vpt(2, "-- we got a pointer and strlen(v_char_sep) = %ld. \n", (int) strlen(v_char_sep));
    if (strlen(v_char_sep) >= 1) { 
      char_sep = v_char_sep[0]; 
    } 
    if ((char_sep >= '1') && (char_sep <= '9')) {
      printf("duckdb you supplied char_sep=\'%c\' but we believe this is a hexadecimal number. \n", char_sep);
      char_sep = (char) ((int) char_sep - ((int)'0'));
    }
    vpt(3, "duckfix_bind: Clearing v_char_sep because we think we can. \n");
  }
  //if (df_char_sep != NULL) { duckdb_destroy_value(&df_char_sep); df_char_sep=NULL; }


  df_fix_sep = duckdb_bind_get_named_parameter(b_info, "fix_sep");
  if (df_fix_sep == NULL) {
  } else if (duckdb_is_null_value(df_fix_sep)) {
  } else {
    v_fix_sep = duckdb_get_varchar( (duckdb_value) df_fix_sep);
    vpt(2, "-- we got a pointer and strlen(v_fix_sep) = %ld. \n", (int) strlen(v_fix_sep));
    if (strlen(v_fix_sep) >= 1) { 
      fix_sep = v_fix_sep[0]; 
    } 
    if ((fix_sep >= '1') && (fix_sep <= '9')) {
      vpt(1,"duckdb you supplied fix_sep=\'%c\' but we believe this is a hexadecimal number. \n", fix_sep);
      fix_sep = (char) ((int) fix_sep - ((int)'0'));
    }
    vpt(3, "duckfix_bind: Clearing v_fix_sep because we think we can. \n");
  }
  //if (df_fix_sep != NULL) { duckdb_destroy_value(&df_fix_sep); df_fix_sep=NULL; }
  if (fix_sep <= '\0') { fix_sep='\0'; }
  if (char_sep <= '\0') { char_sep='\0'; }
  vpt(1, " After load, fix_sep=\'%c\',char_sep=\'%c\'\n",
     fix_sep == '\0' ? '0' : (char_sep >= 1 && char_sep <= 9) ? (char) (fix_sep + '0') : fix_sep,
     char_sep == '\0' ? '0' : (char_sep >= 1 && char_sep <= 9) ? (char) (char_sep + '0') : char_sep);

  vpt(2, " We are looking to try and extract default_date. \n");
  df_default_date= duckdb_bind_get_named_parameter(b_info, "default_date");
  if ((df_default_date != NULL) && (!duckdb_is_null_value(df_default_date))) { 
    v_default_date = duckdb_get_varchar((duckdb_value) df_default_date); 
    len_default_date = v_default_date != NULL ? strlen(v_default_date) : 0;
  }
  if (len_default_date == 10) {
    char bld[6];
    bld[0] = v_default_date[0]; bld[1] = v_default_date[1]; bld[2] = v_default_date[2]; bld[3] = v_default_date[3];
    bld[4] = '\0';  default_date[0] = atoi(bld);
    bld[0] = v_default_date[5]; bld[1] = v_default_date[6];
    default_date[1] = bld[0] == '0' ? ((short) bld[1] - '0') : (short) atoi(bld);
    bld[0] = v_default_date[8]; bld[1] = v_default_date[9];
    default_date[2] = bld[0] == '0' ? ((short) bld[1] - '0') : (short) atoi(bld);
  } else if (v_default_date != NULL) {
    vpt(-1, "ERROR issue, default_date was supplied but its length is %ld. \n", (long int) len_default_date);
  }

  vpt(2, " We are looking to try and extract json_file_name. \n");
  df_json_file_name = duckdb_bind_get_named_parameter(b_info, "json_file_name");
  json_file_name = NULL;
  if (!duckdb_is_null_value(df_json_file_name)) { 
    json_file_name = duckdb_get_varchar((duckdb_value) df_json_file_name); 
  }
  if ((json_file_name == NULL) || (strlen(json_file_name) <= 0)) {
    printf("%s -- ERROR  json_filename given is blank.\n", stt);
    duckdb_bind_set_error(b_info, "ERROR in BIND: json_file_name is NULL\n");
    my_clear();
    return;
  }
  vpt(2, "  -- After retrieving, we have json_file_name = %s. \n", json_file_name);
  sprintf(ll_json_file_name, "%.*s\0", (int) (strlen(json_file_name) < 595 ? strlen(json_file_name) : 595), json_file_name);
  len_json_file_name = strlen(ll_json_file_name);

  df_file_name = duckdb_bind_get_named_parameter(b_info, "file_name");
  file_name = NULL;
  if (!duckdb_is_null_value(df_file_name)) { file_name = duckdb_get_varchar(df_file_name); }
  if ((file_name == NULL) || (strlen(file_name) <= 0)) {
    printf("duckfix_bind, error, file_name given is blank. json_file_name was %s\n",
     json_file_name); 
    duckdb_bind_set_error(b_info, "ERROR in BIND: file_name is NULL\n");
    my_clear();
    return;
  }
  sprintf(ll_file_name, "%.*s\0", (int) (strlen(file_name) < 595 ? strlen(file_name) : 595), file_name);
  len_file_name = strlen(ll_file_name);

  if (verbose >= 1) {
    printf("df_bind.c->duckfix_bind() -- We are looking for ignore_line_text. \n");
  }
  df_ignore_line_text = duckdb_bind_get_named_parameter(b_info, "ignore_line_text");
  ignore_line_text = NULL;
  if (verbose >= 1) {
    if (df_ignore_line_text == NULL) {
      printf(" ---   We have df_ignore_line_text appears to be null parameter. \n");
    } else if (duckdb_is_null_value(df_ignore_line_text)) {
      printf(" ---   We have ignore_line_text appears to be null parameter. \n");
    } else {
      printf(" ---   We have ignore_line_text appears to be non null. \n");
    }
  }
  if ((df_ignore_line_text != NULL) && (!duckdb_is_null_value(df_ignore_line_text))) { ignore_line_text = duckdb_get_varchar(df_ignore_line_text); }
  ll_ignore_line_text[0] = '\0';
  if (ignore_line_text != NULL) {
    sprintf(ll_ignore_line_text, "%.*s\0", (int) (strlen(ignore_line_text) < 99 ? strlen(ignore_line_text) : 99), ignore_line_text);
    len_ignore_line_text = (strlen(ignore_line_text) < 99 ? strlen(ignore_line_text) : 99);
  }

  df_keep_line_text = duckdb_bind_get_named_parameter(b_info, "keep_line_text");
  if (verbose >= 1) {
    if (df_keep_line_text == NULL) {
      printf(" ---   We have df_keep_line_text appears to be null parameter. \n");
    } else if (duckdb_is_null_value(df_keep_line_text)) {
      printf(" ---   We have keep_line_text appears to be null parameter. \n");
    } else {
      printf(" ---   We have keep_line_text appears to be non null. \n");
    }
  }
  if ((df_keep_line_text != NULL) && (!duckdb_is_null_value(df_keep_line_text))) { keep_line_text = duckdb_get_varchar(df_keep_line_text); }
  ll_keep_line_text[0] = '\0';
  if (keep_line_text != NULL) {
    sprintf(ll_keep_line_text, "%.*s\0", (int) (strlen(keep_line_text) < 99 ? strlen(keep_line_text) : 99), keep_line_text);
    len_keep_line_text = (strlen(keep_line_text) < 99 ? strlen(keep_line_text) : 99);
  }

  //df_fix35array = duckdb_bind_get_named_parameter(b_info, "fix35array");
  df_fix35array = NULL;
  if (verbose >= 1) {
    if (df_fix35array == NULL) { printf("%s: df_fix35array is null. \n", stt);
     } else if (duckdb_is_null_value(df_fix35array)) { printf("%s: df_fix35array is a null parameter. \n", stt);
     }
  }
  vpt(1, "Trying to investigate df_fix35array\n");
  if (df_fix35array == NULL) {
    vpt(1, "df_fix35array is NULL. \n");
  } else if ((df_fix35array != NULL)  && (!duckdb_is_null_value(df_fix35array))) {
    duckdb_logical_type lt_fix35array = duckdb_get_value_type(df_fix35array);
    //duckdb_array_type_array_size(duckdb_logical_type type)
    if (lt_fix35array == NULL) {
      printf("%s: Error, lt_fix35 logical type of df_fix35 array is NULL! \n", stt); 
    } else if (duckdb_get_type_id(lt_fix35array) != DUCKDB_TYPE_ARRAY) {
      printf("%s: Issue, lt_fix35array is not type array it is type %d. \n", stt, (int) duckdb_get_type_id(lt_fix35array));
    } else {
      int len35array = duckdb_array_type_array_size(lt_fix35array);
      duckdb_logical_type ltc_fix35array = duckdb_array_type_child_type(lt_fix35array);
      if (len35array <= 0) {  vpt(1, ": df_fix35array is length 0. \n");
      } else if (duckdb_get_type_id(ltc_fix35array) != DUCKDB_TYPE_VARCHAR) {
        vpt(1,": Note type of id of ltc_fix35array is %d, not compatible for fix35 limits. We need strings \n", (int) duckdb_get_type_id(ltc_fix35array));
      } else {
        vpt(1, " Attempting to get vector out of df_fix35array!\n");

      }
      duckdb_destroy_logical_type(&ltc_fix35array); ltc_fix35array = NULL;
    }
    duckdb_destroy_logical_type(&lt_fix35array);  lt_fix35array = NULL;
  }
  if (fix35array == NULL) {
    vpt(1, "Looking for fix35keep. \n");
    df_fix35keep = duckdb_bind_get_named_parameter(b_info, "fix35keep");
    v_fix35keep = NULL; 
    if (df_fix35keep == NULL) { vpt(1, " note df_fix35keep is NULL. \n");
    } else if ((df_fix35keep != NULL) && (!duckdb_is_null_value(df_fix35keep))) { 
      v_fix35keep = duckdb_get_varchar(df_fix35keep); 
      if ((v_fix35keep == NULL) || (strlen(v_fix35keep) <= 0)) {
        printf("duckfix_bind, error, v_fix35keep given is blank.\n");
      } else { 
        vpt(1, " Note we have length of v_fix35keep = %d. \n", (int) strlen(v_fix35keep));
        fix35keep = v_fix35keep[0];  fix35array = malloc(sizeof(char) * 4);  len_fix35array = 1;
        fix35array[0] = fix35keep; fix35array[1] = '\0';  fix35array[2] = '\0';
      }
    }
    if (df_fix35keep != NULL) { duckdb_destroy_value(&df_fix35keep); df_fix35keep = NULL; }
    
  }

  vpt(2, "--- Note we have start_byte=%lld, end_byte=%lld, ignore_line_text=%s, keep_line_text=%s. \n", 
     (long long int) start_byte, (long long int) end_byte,
     (len_ignore_line_text > 0) ? ll_ignore_line_text : "NULL",
     (len_keep_line_text > 0) ? ll_keep_line_text : "NULL");

  vpt(2, "-----------------------------------------------------------------------------------\n");
  vpt(2, " -- Step 2: Reading json_file_name=\"%s\" and generating configuration.\n", json_file_name);
  vpt(2, " -- We are ready to load file to string with json_file_name=\"%s\", file_name=\"%s\". \n",
     ll_json_file_name, ll_file_name);

  vpt(2, " -- Execute load_file_to_str(&json_sf, json_file_name, verbose). \n");
  //json_sf = NULL; // Text of complete JSON instructions. \n");
  iStr nmax =  load_file_to_str(&json_sf, ll_json_file_name, verbose-2);
  if ((nmax <= 0) || (json_sf == NULL)) {
    vpt(-100, "ERROR, tried to load json file \"%s\", but received only %ld as a result. \n",
      json_file_name, (long int) nmax);  
    duckdb_bind_set_error(b_info, "ERROR in LOAD JSON File run of load_file_to_str(...)\n");
    my_clear();
    return;
  }
  vpt(2, " -- We loaded json_sf, length=%ld -- Begining get_config_file. \n", (int) strlen(json_sf));
 
  dfc=NULL;
  dfc = get_config_file(json_sf, 0, nmax, verbose-3);
  if (json_sf != NULL) { free(json_sf); json_sf=NULL; }
  if (dfc == NULL) {
    vpt(0, "ERROR, tried to load json file \"%s\", but did not receive config file layout. nmax=%ld.\n",
      ll_json_file_name, (long int) nmax); 
    my_clear();
    duckdb_bind_set_error(b_info, "ERROR in LOAD JSON File run of get_config_file\n");
    return;
  }
  vpt(1, " -- After get_config_file() -- Received initial configuration file with %ld schemas and %ld known fields. \n",
    dfc->n_schemas, dfc->nfields);
  if (verbose >= 2) {
    PRINT_dfc(dfc);
  }


  //printf("Kill after Field List My Clear Kill! \n"); 
  //duckdb_bind_set_error(b_info, " Simple Kill!. \n");
  //printf("dfc game.\n");
  //int ttc = test_replace_config_file(&dfc, 2); 
  //my_clear();
  //printf("Die after MyClearKill. ttc from test was %d\n", ttc);
  //return;
  dfc->general_sep = dfc->general_sep == '\0' ? (char_sep != '\0' ? char_sep : ',') : dfc->general_sep;
  dfc->fix_sep = (dfc->fix_sep == '\0')
      ? (dfc->schemas[dfc->n_schemas-1].fixsep != '\0' ? dfc->schemas[dfc->n_schemas-1].fixsep : dfc->general_sep) 
      : dfc->fix_sep;
  fix_sep = (fix_sep == '\0') ? (dfc->fix_sep != '\0' ? dfc->fix_sep : dfc->general_sep) : fix_sep;
  if (len_default_date == 10) {
    dfc->default_date[0] = default_date[0]; dfc->default_date[1] = default_date[1]; dfc->default_date[2] = default_date[2];
  }
  vpt(0, "Before we start generate_field_list, fix_sep=\'%c\', char_sep=\'%c\', dfc->general_sep=\'%c\', dfc->fix_sep=\'%c\'. \n",
     (char) fix_sep, (char) char_sep, (char) dfc->general_sep, (char) dfc->fix_sep);
  dfl = NULL;
  dfl = generate_field_list(ll_file_name, dfc, char_sep, fix_sep, verbose-2, standard_vector_size,
    start_byte, end_byte, len_ignore_line_text > 0 ? ll_ignore_line_text : NULL, len_keep_line_text > 0 ? ll_keep_line_text : NULL);

  if (dfl == NULL) {
    vpt(0, "ERROR trying to intially pass file for types.  We read %s, We did not receive field_list for file \"%s\" and json file \"%s\"\n",
      ll_file_name, ll_file_name, ll_json_file_name); 
    my_clear();
    //  delete_config_file(&dfc,verbose); 
    duckdb_bind_set_error(b_info, "ERROR in generate_field_list read of file\n");
    return;
  }
  dfl->standard_vector_size = (int) duckdb_vector_size(); 
  vpt(1, " -- After generate_field_list(%s) received with num_used_known=%ld/%ld, num_unknown=%ld. \n", 
    ll_file_name, (long int) dfl->num_used_known_fields,(long int) dfl->n_known_fields, (long int) dfl->num_unknown);
  if ((dfl->n_known_fields <= 0) && (dfl->num_unknown <= 0)) {
    printf("ERROR ERROR ERROR ERROR ERROR df_bind.c->df_bind() fail, n_known_fields=%ld, num_unknown=%ld. n_loc_lines=%ld, n_loc_all_lines=%ld. \n",
       (long int) dfl->n_known_fields, (long int) dfl->num_unknown, (long int) dfl->n_loc_lines, (long int) dfl->n_total_lines);
    printf("ERROR df_bind.c->df_bind: note char_sep=\'%c\', fix_sep=\'%c\', dfc->general_sep=\'%c\', dfc->fix_sep=\'%c\'\n",
       (char) char_sep, (char) fix_sep, (char) dfc->general_sep, (char) dfc->fix_sep);
    printf("ERROR note that dfc->schemas[n_schemas-1=%ld].fixsep=\'%c\'\n", (long int) dfc->n_schemas-1, dfc->schemas[dfc->n_schemas-1].fixsep);
    vpt(-100, " ERROR, no known fields or unknown in field list the file read must have failed! \n");
    delete_field_list(&dfl, 2); dfl = NULL;
    dfl = generate_field_list(ll_file_name, dfc, char_sep, fix_sep, verbose+1, standard_vector_size,
      start_byte, end_byte, len_ignore_line_text > 0 ? ll_ignore_line_text : NULL, len_keep_line_text > 0 ? ll_keep_line_text : NULL);
    if (dfl == NULL) {
      duckdb_bind_set_error(b_info, " Rerun of generate_field_list made non null large data.\n"); my_clear(); return;
    }
    vpt(-100, " ERROR after error and rerun dfl->n_known_fields=%ld, dfl->num_unknown=%ld. \n",
      dfl->n_known_fields, dfl->num_unknown);
      duckdb_bind_set_error(b_info, " Rerun of generate_field_list made non null large data.\n"); my_clear();
      return;
  }

  /*
  printf("Kill after Field List My Clear Kill! \n"); 
  printf("  dfl->finish=%d. \n", dfl->finish);
  duckdb_bind_set_error(b_info, " Simple Kill!. \n");
  printf("dfc game.\n");
  int ttc = test_replace_config_file(&dfc, 2); 
  int dflofinish = dfl->finish;
  printf("dfl game.\n");
  ttc = test_replace_field_list(dfc, &dfl, 2); 
  int dflfinish = dfl->finish;
  my_clear();
  printf("Die after MyClearKill. ttc from test was %d, dflofinish=%d, dflfinish=%d\n", ttc, dflofinish, dflfinish);
  return;
   */

  if (dfl->finish <= 0) {
    vpt(0, "ERROR, dfl->finish=%d which signals we did not complete load. \n", (int) dfl->finish);
    PRINT_dfl(dfl);  
    vpt(0, " ERROR dfl->finish = %d, now freeing all data and quitting. \n", (int) dfl->finish);
    my_clear();
    //delete_config_file(&dfc, verbose);  delete_field_list(&dfl, verbose);
    duckdb_bind_set_error(b_info, "ERROR in BIND: dfl->finish generated error.\n");  
    return;
  }
  df_bind_data *df_bd = NULL;
  if (dfl->num_unknown > 0) {
    #ifdef DEBUG_MODE
      int v_add = 1;
    #else
      int v_add = 0;
    #endif
    printf("%s: We have dfl->num_unknown=%ld.  We will report what you need to populate for error case . \n", stt, dfl->num_unknown);
    vpt(v_add, "ERROR Reading file received %ld unknown fix fields please address.  File \"%s\" and json file \"%s\"\n",
      (long int) dfl->num_unknown, file_name, json_file_name);  
    PRINT_dfl(dfl);
    vpt(v_add, " ALTERNATIVE TO ERROR: We are initiating the df_bind_error_case for %ld unknown. \n", (long int) dfl->num_unknown);
    df_bd = df_bind_error_case(b_info, verbose+v_add, ll_file_name, len_file_name, 
      ll_json_file_name, len_json_file_name,dfc, dfl);
    if (df_bd == NULL) {
      my_clear();
      duckdb_bind_set_error(b_info, "ERROR in BIND cerating error set, will still fail.\n");  
    }
    duckdb_bind_set_bind_data(b_info, df_bd, destroy_df_bind_data);
    my_dbv_clear();
    vpt(0, "ERROR: WE HAVE INITIATED and are returning ready to try alternative case. \n");
    return;
  }
  if (verbose >= 2) {
    PRINT_dfl(dfl);
  }
  vpt(1, " --- Now move on to configure_column_order :::: \n");
  int success_config = configure_column_order(dfc, dfl, verbose-1);
  if ((success_config < 0) || (dfc->n_total_print_columns <= 0)) {
    vpt(0, "ERROR success config after configure column order is %ld with total_print_columns set at %ld.  File \"%s\" and json file \"%s\"\n",
      (long int) success_config, (long int) dfc->n_total_print_columns, file_name, json_file_name);  
    PRINT_dfl(dfl);  
    my_clear();
    //if (df_file_name != NULL) { duckdb_destroy_value(&df_file_name); }
    //if (df_json_file_name != NULL) { duckdb_destroy_value(&df_json_file_name); }
    //file_name=NULL;json_file_name=NULL;
    //delete_config_file(&dfc, verbose); delete_field_list(&dfl, verbose);
    duckdb_bind_set_error(b_info, " ERROR in configure_column_order, either order or allocate failed. \n");
    return;
  }
  vpt(1, "After configure_column_order().  We have received a success configuration with %ld total print_columns from file \"%s\".\n",
    (long int) dfc->n_total_print_columns, file_name);
  if (verbose >= 2) {
    printf("----------------------------------------------------------------------------------------------\n");
    printf("FPLFPLFPLFPLFPLFPLFPLFPL ---------------------------------------------------------------------\n");
    vpt(1, " Executing final_print_loc on dfc, dfl\n");
    vpt(1, " note we have total print columns =%ld with total multiplcity=%ld. \n",
      (long int) dfc->n_total_print_columns, (long int) dfc->n_total_multiplicity_columns);
    PRINT_dfl(dfl);
    PRINT_final_print_loc(dfc, dfl);
    printf("FPLFPLFPLFPLFPLFPLFPLFPL ---------------------------------------------------------------------\n");
  }

  /*
  printf("Kill after Field List My Clear Kill! \n"); 
  printf("  dfl->finish=%d. \n", dfl->finish);
  duckdb_bind_set_error(b_info, " Simple Kill!. \n");
  printf("dfc game.\n");
  int ttc = test_replace_config_file(&dfc, 2); 
  int dflofinish = dfl->finish;
  printf("dfl game.\n");
  ttc = test_replace_field_list(dfc, &dfl, 2); 
  int dflfinish = dfl->finish;
  my_clear();
  printf("Kill after Field list and configure column order, test was %d, dflofinish=%d, dflfinish=%d\n", ttc, dflofinish, dflfinish);
  return;
  */
  vpt(1, " Constructing df_bind_data object: df_bd\n");
  df_bd = NULL;
  df_bd = create_null_bind_data();
  if (df_bd == NULL) {
    vpt(-100, " Error, df_bind_data was allocated but returned NULL. \n");
    my_clear();  
    duckdb_bind_set_error(b_info, " ERROR df_bd not allocated. \n"); return;
  }
  df_bd->verbose = verbose;
  if (x_info != NULL) {
   x_info->verbose = verbose; 
  }
  df_bd->dfl = dfl; df_bd->dfc = dfc;
  df_bd->start_byte = start_byte; df_bd->end_byte = end_byte; 
  df_bd->report_bust = report_bust; df_bd->report_line = report_line;
  df_bd->len_fix35array = len_fix35array; df_bd->fix35array = fix35array;  fix35array = NULL;  len_fix35array = 0;

  //printf("copy_destroy_bind test. After completing blank creation \n");
  //copy_destroy_bind(&df_bd, "initial blank destroy");  dfl = df_bd->dfl; dfc = df_bd->dfc;
  //printf("Survived copy_destroy_bind after blank test\n\n\n\n");
  if (len_file_name > 0) {
    df_bd->file_name = malloc(sizeof(char) * (len_file_name+1));
    if (df_bd->file_name == NULL) {
      vpt(-100, " Error allocating df_bd->file_name \n");  my_clear();
      duckdb_bind_set_error(b_info, " ERROR in configure_column_order, error allocate file_name. \n");
    }
    sprintf(df_bd->file_name, "%.*s\0", len_file_name, ll_file_name);
  }
  if (len_ignore_line_text > 0) { 
    df_bd->ignore_line_text = malloc(sizeof(char) *((int) (len_ignore_line_text)+1));
    sprintf(df_bd->ignore_line_text, "%.*s\0", len_ignore_line_text, ll_ignore_line_text);
    df_bd->ignore_line_text[len_ignore_line_text] = '\0';  
  }  else {
    df_bd->ignore_line_text = NULL;
  }
  if ((len_keep_line_text > 0 )) {
    df_bd->keep_line_text = malloc(sizeof(char) *((int) (len_keep_line_text+1)));
    sprintf(df_bd->keep_line_text, "%.*s\0", len_keep_line_text, ll_keep_line_text);
    df_bd->keep_line_text[len_keep_line_text] = '\0';  
  }  else {
    df_bd->keep_line_text = NULL;
  }


  //printf("copy_destroy_bind test. After completing our creation \n");
  //copy_destroy_bind(&df_bd, "initial destroy");  dfl = df_bd->dfl; dfc = df_bd->dfc;
  //printf("Survived copy_destroy_bind first test\n\n\n\n");
  //x_info->file_name = malloc(sizeof(char) * fn_len);
  //if (x_info->file_name == NULL) { printf("ERROR trying to allocate x_info->filename of length %ld. \n", (long int) fn_len); }
  //if (x_info->file_name != NULL) { memcpy(x_info->file_name, df_bd->file_name, fn_len); }
  //
  dfc->xtra_col = ((df_bd->report_bust > 0) ? 1 : 0) + (df_bd->report_line>0 ? 1 : 0);
  vpt(1, "%s: duckdb_bind_set_cardinality to b_info. Columns=%d \n", stt, (int) dfc->n_total_multiplicity_columns + dfc->xtra_col);
  duckdb_bind_set_cardinality(b_info, (idx_t) (dfc->n_total_multiplicity_columns + dfc->xtra_col),1); 
  vpt(1, "%s: duckdb_bind_set_cardinality: set total multiplicty to %ld into b_info. \n", stt,
     (int) dfc->n_total_multiplicity_columns);
  if (dfc->mark_m_visited == NULL) {
    vpt(-100, "ERROR vpt mark_m_visited is unpopulated, we will not be able to populate schema in a loop. \n");
    copy_destroy_bind(&df_bd, "initial destroy");
    my_clear();
    duckdb_bind_set_error(b_info, "ERROR no mark_m_visited. \n");
    return;
  }
  vpt(1, "%s -- getting the schema set. \n", stt);
  duckdb_logical_type on_type;
  DF_DataType ontyp; int width = -1; int scale = -1;
  char *ptitle; char ptitle_mult[80];
  int on_multiplicity = 1;  int i_multiplicity = 0;

  int keepsf = -1; int s0f1 = 0;
  int ntmc = dfc->n_total_multiplicity_columns;
  int ntpc = dfc->n_total_print_columns;
  int mv = 0;  int fn = 0;  int pv;
  int on_ct = 0; int on_mt = 0;
  for (on_ct = 0; on_ct < ntpc; on_ct++) {
    pv = dfc->mark_visited[ntpc + on_ct];
    fn = pv >= 0 ? - 100 : -pv -1;
    vpt(2, "%s - on_ct=%ld/%ld. on_mt=%ld/%ld, pv=%ld, fn=%ld", 
      stt, (long int) on_ct, (long int) ntpc, (long int) on_mt, (long int) ntmc,
      (long int) pv, (long int) fn
    );
    if (((pv >= 0) && (pv < dfc->n_schemas)) ||  ((pv < 0) && (fn < dfc->nfields))) {
      if (pv >= 0) {
        vpt(2, ":%s with type=%s.  One Element \n",
          dfc->schemas[pv].nm, What_DF_DataType(dfc->schemas[pv].typ));
      } else {
        if (dfc->fxs == NULL) { printf("ERROR dfc->fxs is NULL! \n"); }
        if (verbose >=2 ) {
          printf(":%d", dfc->fxs[fn].field_code);
          printf(":%s", dfc->fxs[fn].nm);
          printf("  type=%s,", What_DF_DataType(dfc->fxs[fn].typ));
          printf(" maxmultiplicity=%d, ", dfc->fxs[fn].maxmultiplicity);
        }
        if (dfl->known_multiplicity== NULL) {
         printf("ERROR something wrong with dfl->known_multiplicity\n");
        }
        if (verbose >= 2) {
          printf("  multiplicity: %d or 1 \n",
            dfc->fxs[fn].maxmultiplicity >=  dfl->known_multiplicity[fn] ? dfl->known_multiplicity[fn] : dfc->fxs[fn].maxmultiplicity);
        }
      }
    } else {
      printf(" -- Error coming \n");
    }
    if ((on_mt >= ntmc) || (pv != dfc->mark_m_visited[ntmc + on_mt])) {
      vpt(-4, " ERROR: on_ct=%ld/%ld, on_mt=%ld/%ld,  we got pv=%ld, but mark_m_visited[%ld+%ld]=%ld. mark_visited[%ld+%ld]=%ld\n",
        (long int) on_ct, (long int) ntpc, (long int) on_mt, (long int) ntmc,
        (long int) pv, (long int) ntmc, (long int) on_mt, on_mt < ntmc ? (long int) dfc->mark_m_visited[ntmc + on_mt]: -100,
        (long int) ntpc, (long int) on_ct, on_ct < ntpc ? (long int) dfc->mark_visited[ntpc + on_ct]: -100);
      printf("mark_visited: \n[");
      for (int ij = 0; ij < ntpc; ij++) {  printf("[%d:%d]", dfc->mark_visited[ij], dfc->mark_visited[ntpc+ij]);
        if (ij >= ntpc-1) { printf("]\n"); } else if ( (ij+1) % 8 == 0) { printf(", \n"); } else { printf(","); }
      }
      printf("\n\nmark_m_visited: \n[");
      for (int ij = 0; ij < ntmc; ij++) {  printf("[%d:%d]", dfc->mark_m_visited[ij], dfc->mark_m_visited[ntmc+ij]);
        if (ij >= ntpc-1) { printf("]\n"); } else if ( (ij+1) % 8 == 0) { printf(", \n"); } else { printf(","); }
      }
      printf("\n");
      duckdb_bind_set_error(b_info, " mark_m_visited/mvisited travel error. \n"); 
      duckdb_bind_set_error(b_info, " mark_m_visited/mvisited travel error. \n"); 
      my_clear(); return;
    }
    if ((pv >= 0) && (pv >= dfc->n_schemas)) {
      vpt(-4, " Error mark_visited[%ld + %ld] = %d, but n_schemas = %ld. \n",
       (long int) ntpc, on_ct, pv, dfc->n_schemas);
      duckdb_bind_set_error(b_info, " mark_m_visited schemas error. \n"); 
      my_clear(); return;
    } else if ((pv < 0) && (dfc->nfields <= fn)) {
      vpt(-4, " Error mark_visited[%ld + %ld] = %d, but n_fields = %ld. \n",
       (long int) ntpc, on_ct, pv, dfc->nfields);
      duckdb_bind_set_error(b_info, " mark_m_visited fields error. \n"); 
      my_clear(); return;
    } else {
      on_multiplicity = (pv >= 0) ? 1 : 
        (dfc->fxs[fn].maxmultiplicity > dfl->known_multiplicity[fn] ? dfl->known_multiplicity[fn] :
        dfc->fxs[fn].maxmultiplicity);
      on_multiplicity = on_multiplicity > 0 ? on_multiplicity : 1;
      s0f1 = pv >= 0 ? 0 : 1;
      ontyp = pv >= 0 ? dfc->schemas[pv].typ : dfc->fxs[fn].typ;
      ptitle = pv >= 0 ? dfc->schemas[pv].nm : dfc->fxs[fn].nm;
      width = pv >= 0 ? dfc->schemas[pv].width : dfc->fxs[fn].width;
      scale = pv >= 0 ? dfc->schemas[pv].scale : dfc->fxs[fn].scale;
      vpt(2, " -- on_ct=%ld/%ld, on_mt=%ld, multip=%ld for pv=%ld,fn=%ld, %s of type %s or DDB of %s \n",
        (long int) on_ct, (long int) ntpc, (long int) on_mt, (long int) on_multiplicity, (long int) pv, (long int) fn,
        ptitle, What_DF_DataType(ontyp),
        WHAT_DDB_TYPE_STR(WHAT_DDB_TYPE(ontyp)));
      if ((ontyp == decimal185) || (ontyp==decimal184) || (ontyp==decimal153) || (ontyp==decimal154) ||
        (ontyp) == decimal_gen) {
        on_type = duckdb_create_decimal_type(width, scale);
      } else {
        on_type = duckdb_create_logical_type(WHAT_DDB_TYPE(ontyp));
      }
      for (int onm = 0; onm < on_multiplicity; onm++) {
        if (on_multiplicity > 1) {
          sprintf(ptitle_mult, "%.*s_%02d\0", (int) (strlen(ptitle) < 40 ? strlen(ptitle) : 40), ptitle, (int) onm);
          duckdb_bind_add_result_column(b_info, ptitle_mult, on_type);
        } else {
          duckdb_bind_add_result_column(b_info, ptitle, on_type);
        }
        on_mt++;
      }
      duckdb_destroy_logical_type(&on_type);
    }
  }
  if (df_bd->report_bust > 0) {
    on_type = duckdb_create_logical_type(WHAT_DDB_TYPE(str));
    duckdb_bind_add_result_column(b_info, "BustType", on_type);
    duckdb_destroy_logical_type(&on_type);
  } 

  if (df_bd->report_line > 0) {
    on_type = duckdb_create_logical_type(WHAT_DDB_TYPE(i64));
    duckdb_bind_add_result_column(b_info, "line", on_type);
    duckdb_destroy_logical_type(&on_type);
  } 
  //vpt(3, "copy_destroy_bind test. \n");
  //copy_destroy_bind(&df_bd, "Near Conclusion of df_bind");  dfl = df_bd->dfl; dfc = df_bd->dfc;
  //vpt(3, " successfuly copy_destroy_bind test.\n");
  if (verbose >= 3) {
    printf("%s --- Hey DFC and DFL tests. \n", stt);
    printf("---- Test DFC: \n");
    int ttc = test_replace_config_file(&df_bd->dfc, 2);
    printf("%s ---- DFC test returned %ld. \n", stt, ttc);
    if (ttc < 0) {
      printf("%s ERROR we can't really continue because of bad dfc. \n", stt);
      duckdb_bind_set_error(b_info, " ERROR dfc test returned Negative ttc"); 
      my_clear(); return;
    }
    ttc = test_replace_field_list(&df_bd->dfl, 2);
    printf("%s ---- DFL field test reutnred %ld.\n", stt, ttc);
    if (df_bd->dfl->ordered_known_fields == NULL) { ttc = -430503; }
    if (ttc < 0) {
      printf("%s ERROR we can't really continue because of bad dfl. ttc=%ld\n", stt, ttc);
      duckdb_bind_set_error(b_info, " ERROR dfl test returned Negative ttc"); 
      my_clear();  return;
    }
  }
  vpt(1, " after on_ct=%ld/%ld and on_mt=%ld/%ld loops we have completed. \n",
    (long int) on_ct, (long int) dfc->n_total_print_columns, (long int) on_mt, (long int) dfc->n_total_multiplicity_columns
    );
  vpt(1, " -- setting b_info and df_bd to bind data. \n");
  duckdb_bind_set_bind_data(b_info, df_bd, destroy_df_bind_data);
  vpt(1, " --  running last df_bind.c my_clear() operation; \n");

  //my_dbp_clear();
  my_dbv_clear();
  if (json_sf != NULL) { free(json_sf); json_sf=NULL; }
  //my_clear();
  vpt(1, "df_bind.c--- end bind operation. Note df_bind->verbose=%d\n", (int) df_bd->verbose);
  //printf("--- Note df_bind->verbose=%d. %s\n", (int) df_bd->verbose, stt);
  //duckdb_bind_set_error(b_info, " Looking at bind verbose. \n");
  return;
}  

df_bind_data *df_bind_error_case(duckdb_bind_info b_info, int verbose, char* ll_file_name, int len_file_name, 
  char* ll_json_file_name, int len_json_file_name,
  DF_config_file *dfc, DF_field_list *dfl) {
  char stt[600];
  sprintf(stt, "df_bind_error_case(v=%d,fn=%.*s,jfn=%.*s,nu=%ld):",
    (int) verbose, len_file_name < 20 ? len_file_name : 20,
    ll_file_name + (len_file_name < 20 ? 0 : len_file_name-20),
    len_json_file_name < 20 ? len_json_file_name : 20,
    ll_json_file_name + (len_json_file_name < 20 ? 0 : len_json_file_name-20),
    dfl->num_unknown); 
  vpt(1, " Constructing df_bind_data object: df_bd\n");
  df_bind_data *df_bd = NULL;
  df_bd = create_null_bind_data();
  if (df_bd == NULL) {
    vpt(-100, " Error, df_bind_data was allocated but returned NULL. \n");
    duckdb_bind_set_error(b_info, " ERROR df_bd not allocated. \n"); return(NULL);
  }
  df_bd->verbose = verbose;
  df_bd->dfl = dfl; df_bd->dfc = dfc;
  df_bd->start_byte = 0; df_bd->end_byte = -1;
  df_bd->keep_line_text=NULL; df_bd->ignore_line_text=NULL; df_bd->report_bust = 0;
  vpt(1, "%s: duckdb_bind_set_cardinality to b_info. Columns=%d \n", stt, (int) dfc->n_total_multiplicity_columns);
  duckdb_bind_set_cardinality(b_info, (idx_t) 4, 1);
  duckdb_logical_type on_type;
  on_type = duckdb_create_logical_type(WHAT_DDB_TYPE(i64));

  duckdb_bind_add_result_column(b_info, "unknown_field_num", on_type);
  duckdb_bind_add_result_column(b_info, "usage_count", on_type);
  duckdb_bind_add_result_column(b_info, "multiplicity", on_type);
  duckdb_bind_add_result_column(b_info, "first_line_num", on_type);
  duckdb_destroy_logical_type(&on_type);
  return(df_bd);
}
