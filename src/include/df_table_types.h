/////////////////////////////////////
///
/// Duckfix table types
#pragma once


#ifndef STDIOH
#include <stdio.h>
#define STDIOH
#endif

#ifndef DUCKDBEXTENSIONH
#include "duckdb_extension.h"
#define DUCKDBEXTENSIONH 0
#endif

#ifndef DUCKFIXGENERALH
#include "include/df_general.h"
#define DUCKFIXGENERALH 0
#endif

#ifndef WHAT_DDB_TYPE
#define WHAT_DDB_TYPE( typ ) \
  ((typ == i32) ? DUCKDB_TYPE_INTEGER : \
   (typ == i64) ? DUCKDB_TYPE_BIGINT : \
   (typ == str) ? DUCKDB_TYPE_VARCHAR : \
   (typ == decimal154) ? DUCKDB_TYPE_DECIMAL : \
   (typ == decimal184) ? DUCKDB_TYPE_DECIMAL : \
   (typ == decimal153) ? DUCKDB_TYPE_DECIMAL : \
   (typ == decimal185) ? DUCKDB_TYPE_DECIMAL : \
   (typ == decimal_gen) ? DUCKDB_TYPE_DECIMAL : \
   (typ == tms) ? DUCKDB_TYPE_TIMESTAMP_MS : \
   (typ == tns) ? DUCKDB_TYPE_TIMESTAMP_NS : \
   (typ == tus) ? DUCKDB_TYPE_TIMESTAMP : \
   (typ == f64) ? DUCKDB_TYPE_DOUBLE : \
   (typ == f32) ? DUCKDB_TYPE_FLOAT : \
   (typ == fix42) ? DUCKDB_TYPE_INVALID : \
    DUCKDB_TYPE_INVALID)
#endif

#ifndef WHAT_DDB_TYPE_STR 
#define WHAT_DDB_TYPE_STR(dtyp) \
  ((dtyp == DUCKDB_TYPE_VARCHAR) ? "DUCKDB_TYPE_VARCHAR" : \
   (dtyp == DUCKDB_TYPE_BIGINT) ? "DUCKDB_TYPE_BIGINT" : \
   (dtyp == DUCKDB_TYPE_FLOAT) ? "DUCKDB_TYPE_FLOAT" : \
   (dtyp == DUCKDB_TYPE_DECIMAL) ? "DUCKDB_TYPE_DECIMAL" : \
   (dtyp == DUCKDB_TYPE_HUGEINT) ? "DUCKDB_TYPE_HUGEINT" : \
   (dtyp == DUCKDB_TYPE_INTEGER) ? "DUCKDB_TYPE_INTEGER" : \
   (dtyp == DUCKDB_TYPE_DOUBLE) ? "DUCKDB_TYPE_DOUBLE" : \
   (dtyp == DUCKDB_TYPE_ARRAY) ? "DUCKDB_TYPE_ARRAY" : \
   (dtyp == DUCKDB_TYPE_TIMESTAMP_MS) ? "DUCKDB_TYPE_TIMESTAMP_MS" : \
   (dtyp == DUCKDB_TYPE_TIMESTAMP_NS) ? "DUCKDB_TYPE_TIMESTAMP_NS" : \
   (dtyp == DUCKDB_TYPE_TIME) ? "DUCKDB_TYPE_TIME" : \
   (dtyp == DUCKDB_TYPE_INTERVAL) ? "DUCKDB_TYPE_INTERVAL" : \
   (dtyp == DUCKDB_TYPE_INTERVAL) ? "DUCKDB_TYPE_BLOB" : \
   (dtyp == DUCKDB_TYPE_INVALID) ? "DUCKDB_TYPE_INVALID" : \
   "DUCKDB_WE_DIDNT_LABEL")
#endif

typedef struct _df_extra_info {
    //duckdb_connection ddb_con;
  //char *file_name;
  int32_t verbose;
} df_extra_info;

typedef struct _df_init_data {
  int32_t verbose;
  //duckdb_connection ddb_con;
  int32_t on_chunk;
  idx_t offset;
  idx_t actual_rows_read;
  //duckdb_result *p_result;
  char *file_name;  
  FILE *fpo;
  int on_overall_line; int on_chunk_line;
  int st_buffer_loc; int end_buffer_loc;
  //duckdb_data_chunk on_in_chunk;
  //idx_t on_in_row;
  //idx_t on_in_row_count;
  //idx_t n_out_chunks;
  //idx_t n_in_chunks; 
  idx_t ion_schema;
  int32_t last_fix_num;
  char *buffer;
  int buffreads;
  iStr tbytesread;  iStr bytesread; iStr remainder;
  iStr onstr; iStr iLineEnd;
  DF_config_file *dfc;
  DF_field_list *dfl;
  duckdb_date_struct dds;
  char int_scratch[22]; // Maximum number of digits to read i64 integer

} df_init_data; 

typedef struct _df_bind_data {
  int32_t verbose;
  //duckdb_connection ddb_con;
  idx_t cardinality;
  //duckdb_result *p_result;
  char *file_name;

  DF_config_file *dfc;
  DF_field_list *dfl;
} df_bind_data;

//void RegisterAddNumbersFunction(duckdb_connection connection);
//static void test_add_manipulate_columns(duckdb_result *p_dresult);
//void register_test_add_table_function(duckdb_connection ddb_con);

// Functions for Duckdb calls.
// Duckdb package will register a table function, which will require bind/init/main combination
// See duck_fix_read.c for where these functions are installed.


// In df_bind.c
void destroy_df_extra_info(void *v_df_xi);
void destroy_df_bind_data(void *v_df_bd);
void duckfix_bind(duckdb_bind_info b_info);

// In df_init.c
void destroy_df_init_data(void *v_df_id);
void duckfix_init(duckdb_init_info i_info);

// These are in df_main.c
int add_schema_entry_to_chunk(df_init_data *df_id, char*buffer, iStr st, 
  iStr end, duckdb_data_chunk out_chunk, int verbose);
int add_fixfields_entries_to_chunk(df_init_data *df_id, char *sf, iStr fixfieldsStart, iStr fixfieldsEnd, 
  duckdb_data_chunk out_chunk, int verbose);
int add_fixfield_entry_to_chunk(df_init_data *df_id, char *sf, int fixField,  
  iStr valStart, iStr valEnd,  duckdb_data_chunk out_chunk, int verbose);
int fill_in_chunk(DF_DataType on_typ,  duckdb_vector ddbv, df_init_data *df_id,
   char*sf, iStr valStart, iStr valEnd, int verbose, int width, int scale, DF_TSType fmttyp, int nCol);
void duckfix_main_table_function(duckdb_function_info df_info, duckdb_data_chunk out_chunk);
