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

void destroy_df_extra_info(void *v_df_xi) {
  printf("destroy_df_extra_info() we have now called. \n");
  df_extra_info *df_xi = (df_extra_info*) v_df_xi;
  if (df_xi == NULL) { return; }
  if (df_xi->verbose > 0) {
    printf("destroy_df_xtra_info -- starting. \n");
  }
  //if (df_xi->file_name != NULL) { free(df_xi->file_name); df_xi->file_name = NULL; }
  printf("destroy_df_extra_info() deleting df_xi now. \n");
  free(df_xi); 
  printf("destroy_df_extra_info() all concluded df_xi was non null. \n");
}
void destroy_df_bind_data(void *v_df_bd) {
  printf("destroy_df_bind_data() we have been called. \n");
  df_bind_data *df_bd = (df_bind_data*) v_df_bd;
  //duckdb_destroy_result(ta_bd->p_result);
  //duckdb_free(ta_bd->p_result);  ta_bd->p_result = NULL;
  if (df_bd->verbose > 0) {
    printf("destroy_df_bind_data INTIALIZING(%s,%s)\n",
      df_bd->dfc != NULL ? "dfc is not null" : "dfc is null",
      df_bd->dfl != NULL ? "dfl is not null" : "dfl is null");
  }
  if (df_bd->file_name != NULL) { free(df_bd->file_name); df_bd->file_name=NULL; }
  if (df_bd->dfc != NULL) { delete_config_file(&df_bd->dfc, df_bd->verbose-1); df_bd->dfc = NULL; }
  if (df_bd->dfl != NULL) { delete_field_list(&df_bd->dfl, df_bd->verbose-1); df_bd->dfl = NULL; }
  if (df_bd->verbose > 0) {
    printf("destroy_df_bind_data has been called I hope you didn't need that data. \n");
  }
  return; 
}
void duckfix_bind(duckdb_bind_info b_info) {
  char stt[500]; 
  sprintf(stt, "df_bind.c->duckfix_bind(): ");
  df_extra_info *x_info = duckdb_bind_get_extra_info((duckdb_bind_info) b_info);
  //duckdb_connection ddb_con = x_info->ddb_con;
  duckdb_value db_verbose = duckdb_bind_get_named_parameter(b_info, "verbose");
  int32_t verbose = duckdb_get_int32(db_verbose);
  if (verbose >= 1) {
    sprintf(stt, "df_bind.c->duckfix_bind(v=%ld): ", (long int) verbose);
  }
  if (verbose >= 2) {
    printf("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\n");
    sprintf(stt, "BBB duckfix_bind(v=%ld): ", (long int) verbose);
    vpt(2, " -- Init. \n");
  }

  idx_t standard_vector_size = duckdb_vector_size();
  vpt(2, " We have received standard_vector_size=%ld. looking for char_sep.\n", (long int) standard_vector_size);
  duckdb_value df_char_sep = duckdb_bind_get_named_parameter(b_info, "char_sep");
  char char_sep = ',';
  vpt(2, " We are looking to charge char_sep. \n");
  if (df_char_sep == NULL) {
    vpt(2, "-- we had df_char_sep was complete NULL.\n");
  } else if (duckdb_is_null_value(df_char_sep)) {
    vpt(2, "-- we have a null value for df_char_sep.\n");
  } else {
    vpt(2, "-- we have non null value df_char_sep, trying to get varchar from it. \n");
    char *v_char_sep = duckdb_get_varchar( (duckdb_value) df_char_sep);
    vpt(2, "-- we got a pointer and strlen(v_char_sep) = %ld. \n", strlen(v_char_sep));
    if (strlen(v_char_sep) >= 1) { 
      char_sep = v_char_sep[0]; 
    } 
    vpt(3, "duckfix_bind: Clearing v_char_sep because we think we can. \n");
    duckdb_free(v_char_sep); v_char_sep=NULL;
  }
  vpt(2, " We are looking to try and extract json_file_name. \n");
  duckdb_value df_json_file_name = duckdb_bind_get_named_parameter(b_info, "json_file_name");
  char *json_file_name = duckdb_get_varchar((duckdb_value) df_json_file_name);
  if ((json_file_name == NULL) || (strlen(json_file_name) <= 0)) {
    printf("%s -- ERROR  json_filename given is blank.\n", stt);
    duckdb_bind_set_error(b_info, "ERROR in BIND: json_file_name is NULL\n");
    return;
  }
  vpt(2, "  -- After retrieving, we have json_file_name = %s. \n", json_file_name);

  duckdb_value df_file_name = duckdb_bind_get_named_parameter(b_info, "file_name");
  char *file_name = duckdb_get_varchar(df_file_name);
  if ((file_name == NULL) || (strlen(file_name) <= 0)) {
    printf("duckfix_bind, error, file_name given is blank. json_file_name was %s\n",
     json_file_name); duckdb_free(json_file_name);
    duckdb_bind_set_error(b_info, "ERROR in BIND: file_name is NULL\n");
    return;
  }
  vpt(2, "-----------------------------------------------------------------------------------\n");
  vpt(2, " -- Step 2: Reading json_file_name=\"%s\" and generating configuration.\n", json_file_name);
  vpt(2, " -- We are ready to load file to string with json_file_name=\"%s\", file_name=\"%s\". \n",
     json_file_name, file_name);

  vpt(2, " -- Execute load_file_to_str(&json_sf, json_file_name, verbose");
  char *json_sf = NULL; // Text of complete JSON instructions. \n");
  iStr nmax =  load_file_to_str(&json_sf, json_file_name, verbose-2);
  if ((nmax <= 0) || (json_sf == NULL)) {
    vpt(0, "ERROR, tried to load json file \"%s\", but received only %ld as a result. \n",
      json_file_name, (long int) nmax);  duckdb_free(json_file_name); duckdb_free(file_name); 
    duckdb_bind_set_error(b_info, "ERROR in LOAD JSON File run of load_file_to_str(...)\n");
    return;
  }
  vpt(2, " -- We loaded json_sf, length=%ld -- Begining get_config_file. \n", strlen(json_sf));
  DF_config_file *dfc = get_config_file(json_sf, 0, nmax, verbose-3);
  free(json_sf); json_sf = NULL;
  if (dfc == NULL) {
    vpt(0, "ERROR, tried to load json file \"%s\", but did not receive config file layout. nmax=%ld.\n",
      json_file_name, (long int) nmax); duckdb_free(json_file_name); duckdb_free(file_name);
    duckdb_bind_set_error(b_info, "ERROR in LOAD JSON File run of get_config_file\n");
    return;
  }
  vpt(1, " -- After get_config_file() -- Received initial configuration file with %ld schemas and %ld known fields. \n",
    dfc->n_schemas, dfc->nfields);
  if (verbose >= 2) {
    PRINT_dfc(dfc);
  }
  DF_field_list *dfl = generate_field_list(file_name, dfc, char_sep, verbose-3, standard_vector_size);
  dfl->standard_vector_size = (int) duckdb_vector_size(); 
  if (dfl == NULL) {
    vpt(0, "ERROR trying to intially pass file for types.  We did not receive field_list for file \"%s\" and json file \"%s\"\n",
      file_name, json_file_name);  duckdb_free(file_name); duckdb_free(json_file_name);
      delete_config_file(&dfc,verbose); 
    duckdb_bind_set_error(b_info, "ERROR in generate_field_list read of file\n");
    return;
  }
  vpt(1, " -- After generate_field_list(%s) received with num_used_known=%ld/%ld, num_unknown=%ld. \n", 
    file_name, (long int) dfl->num_used_known_fields,(long int) dfl->n_known_fields, (long int) dfl->num_unknown);

  if (dfl->finish <= 0) {
    vpt(0, "ERROR, dfl->finish=%d which signals we did not complete load. \n", (int) dfl->finish);
    PRINT_dfl(dfl);  duckdb_free(file_name); duckdb_free(json_file_name);
    vpt(0, " ERROR dfl->finish = %d, now freeing all data and quitting. \n", (int) dfl->finish);
    delete_config_file(&dfc, verbose);  delete_field_list(&dfl, verbose);
    duckdb_bind_set_error(b_info, "ERROR in BIND: dfl->finish generated error.\n");  
    return;
  }
  if (dfl->num_unknown > 0) {
    vpt(0, "ERROR Reading file received %ld unknown fix fields please address.  File \"%s\" and json file \"%s\"\n",
      (long int) dfl->num_unknown, file_name, json_file_name);  
    PRINT_dfl(dfl);
    vpt(0, " ERROR received %ld unknown fields now freeing all data and quitting. \n", (long int) dfl->num_unknown);
    duckdb_free(file_name); duckdb_free(json_file_name);
    delete_config_file(&dfc, verbose);   delete_field_list(&dfl, verbose);
    duckdb_bind_set_error(b_info, "ERROR in BIND: I believe we found an invalid field list.\n");  
    return;
  }
  if (verbose >= 2) {
    PRINT_dfl(dfl);
  }
  int success_config = configure_column_order(dfc, dfl, verbose-1);
  if ((success_config < 0) || (dfc->n_total_print_columns <= 0)) {
    vpt(0, "ERROR success config after configure column order is %ld with total_print_columns set at %ld.  File \"%s\" and json file \"%s\"\n",
      (long int) success_config, (long int) dfc->n_total_print_columns, file_name, json_file_name);  
    PRINT_dfl(dfl);  duckdb_free(file_name); duckdb_free(json_file_name);
    delete_config_file(&dfc, verbose); delete_field_list(&dfl, verbose);
    duckdb_bind_set_error(b_info, " ERROR in configure_column_order, either order or allocate failed. \n");
    return;
  }
  vpt(1, "After configure_column_order().  We have received a success configuration with %ld total print_columns from file \"%s\".\n",
    (long int) dfc->n_total_print_columns, file_name);
  if (verbose >= 2) {
    printf("FPLFPLFPLFPLFPLFPLFPLFPL ---------------------------------------------------------------------\n");
    vpt(1, " Executing final_print_loc on dfc, dfl\n");
    PRINT_final_print_loc(dfc, dfl);
    printf("FPLFPLFPLFPLFPLFPLFPLFPL ---------------------------------------------------------------------\n");
  }
  vpt(1, " Constructing df_bind_data object: df_bd\n");
  df_bind_data *df_bd = duckdb_malloc(sizeof(df_bind_data));
  df_bd->verbose = verbose;  x_info->verbose = verbose; df_bd->dfl = NULL; df_bd->dfc = NULL;
  df_bd->verbose = verbose; df_bd->dfl = dfl; df_bd->dfc = dfc;
  int fn_len = strlen(file_name) +1;
  vpt(1, "Copying %s filename of length %d to df_bd->file_name pointer. \n", file_name, (long int) fn_len);
  df_bd->file_name = malloc(sizeof(char) * fn_len);
  memcpy(df_bd->file_name, file_name, fn_len);
  duckdb_free(file_name); file_name = NULL;
  //x_info->file_name = malloc(sizeof(char) * fn_len);
  //if (x_info->file_name == NULL) { printf("ERROR trying to allocate x_info->filename of length %ld. \n", (long int) fn_len); }
  //if (x_info->file_name != NULL) { memcpy(x_info->file_name, df_bd->file_name, fn_len); }
  duckdb_bind_set_cardinality(b_info, (idx_t) dfc->n_total_print_columns, 1);

  vpt(1, " Now we need to set the columns. \n");
  int on_s = loc_lowest_priority_schema_gt(dfc, -1, verbose); 
  int on_f = loc_lowest_priority_fixfield_gt(dfc,dfl, -1, verbose);
  vpt(1, " -- we start with on_S=%ld, on_f=%ld. \n", (long int) on_s, (long int) on_f);
  int p_on_s =  (int)  (((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -1);
  int p_on_f =  (int) (((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -1);
  duckdb_logical_type on_type;
  int on_loop = 0;
  DF_DataType ontyp; int width = -1; int scale = -1;
  char *ptitle;
  while ((on_s >= 0) || (on_f >= 0)) {
    vpt(2, " on loop=%ld, with on_s=%ld (p_s=%ld), on_f=%ld (p_s=%ld) \n", 
       (long int) on_loop, (long int) on_s, (long int) p_on_s, (long int) on_f, (long int) p_on_f);
    if ((on_f < 0) || (p_on_s <= p_on_f)) {
      vpt(2, "  --- We see we will move schema on_s=%ld for name = %s, type=%s. col=%ld versus goal loc=%ld, aka type = \"%s\"\n",
          (long int) on_s, dfc->schemas[on_s].nm, What_DF_DataType(dfc->schemas[on_s].typ),
          (long int) on_loop, (long int) dfc->schemas[on_s].final_loc,
          WHAT_DDB_TYPE_STR( (WHAT_DDB_TYPE(dfc->schemas[on_s].typ))) );
      ontyp = dfc->schemas[on_s].typ; width = dfc->schemas[on_s].width;  scale = dfc->schemas[on_s].scale;
      ptitle = dfc->schemas[on_s].nm;
      on_s = next_priority_schema(dfc, on_s, verbose-1);
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -1;
    } else {
      vpt(2, "  --- We see we will move field on_f=%ld for code =%ld, name = %s, type=%s.  col=%ld versus goal of %ld, aka type=\"%s\"\n",
          (long int) on_f, (long int) dfc->fxs[on_f].field_code, 
          dfc->fxs[on_f].fixtitle, What_DF_DataType(dfc->fxs[on_f].typ),
          (long int) on_loop,  dfl->final_known_print_loc[on_f],
          WHAT_DDB_TYPE_STR( (WHAT_DDB_TYPE(dfc->fxs[on_f].typ)) )
      );
      ontyp = dfc->fxs[on_f].typ; width = dfc->fxs[on_f].width;  scale = dfc->fxs[on_f].scale;
      ptitle = dfc->fxs[on_f].fixtitle;
      on_f = next_priority_fixfield(dfc, dfl, on_f, verbose-1);
      p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -1;
    }
    if ((ontyp == decimal185) || (ontyp==decimal184) || (ontyp==decimal153) || (ontyp==decimal154) ||
        (ontyp) == decimal_gen) {
        on_type = duckdb_create_decimal_type(width, scale);
    } else {
      on_type = duckdb_create_logical_type(WHAT_DDB_TYPE(ontyp));
    }
    duckdb_bind_add_result_column(b_info, ptitle, on_type);
    duckdb_destroy_logical_type(&on_type);
    on_loop++;
  }  
  vpt(1, " after %ld loops we have concluded adding columns (%ld) \n",
    (long int) on_loop, dfc->n_total_print_columns);
  vpt(1, " -- setting b_info and df_bd to bind data. \n");
  duckdb_bind_set_bind_data(b_info, df_bd, destroy_df_bind_data);
  vpt(1, " -- Now we are freeing (file_name); \n");
  if (file_name != NULL) { duckdb_free(file_name);  file_name = NULL; }
  vpt(1, " -- now we are freeing json_file_name) with duckdb_free. \n");
  if (json_file_name != NULL) { duckdb_free(json_file_name);  json_file_name = NULL; }
  vpt(1, " -- we are done with bind, free to call init. \n");
  return;
}  


