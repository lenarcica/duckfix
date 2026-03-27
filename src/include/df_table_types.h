/////////////////////////////////////
/// df_table_types.h
///
///  Alan Lenarcic (lenarcic@post.harvard.edu)
///  2026-02-04
///
///  GPLv2 License
///
///  This header file is used with duckdb.h connection information.
///  We need to be able to write content from configurations into specific DuckDB type columns.
///  This is only used in df_bind.c, df_init.c, df_main.c: which act as the "table function" files.
///
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
   (typ == fix2end) ? DUCKDB_TYPE_INVALID : \
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
   (dtyp == DUCKDB_TYPE_TIMESTAMP) ? "DUCKDB_TYPE_TIMESTAMP" : \
   (dtyp == DUCKDB_TYPE_TIMESTAMP_NS) ? "DUCKDB_TYPE_TIMESTAMP_NS" : \
   (dtyp == DUCKDB_TYPE_TIME) ? "DUCKDB_TYPE_TIME" : \
   (dtyp == DUCKDB_TYPE_INTERVAL) ? "DUCKDB_TYPE_INTERVAL" : \
   (dtyp == DUCKDB_TYPE_INTERVAL) ? "DUCKDB_TYPE_BLOB" : \
   (dtyp == DUCKDB_TYPE_INVALID) ? "DUCKDB_TYPE_INVALID" : \
   "DUCKDB_WE_DIDNT_LABEL")
#endif

#ifndef BUSTTYPES
#define BUSTTYPES 0
typedef enum { 
  nobust,
  bust,
  badencode,
  badencodeandbust
} DF_BustType;

#define What_DF_BustType(on_bust)  \
  ((on_bust) == nobust) ? "None" :  \
  ((on_bust) == bust) ? "Bust" :  \
  ((on_bust) == badencode) ? "BadEncode" : \
  ((on_bust) == badencodeandbust) ? "BustAndEncode" : \
  "Unknown"

#define UpdateBust(on_bust, new_bust) \
    ( (on_bust) == nobust) ? (new_bust) :  \
    ( (new_bust) == nobust) ? (on_bust) : \
    ( (on_bust) == badencodeandbust) ? badencodeandbust : \
    ((on_bust) == bust) ?  (((new_bust)==badencode) ? badencodeandbust :on_bust) : \
    ((on_bust) == badencode) ? (((new_bust)==bust) ? badencodeandbust : on_bust) : \
    on_bust

//(on_bust) )

#define UpdateBust2(on_bust,new_bust) \
 (((on_bust) == nobust) ? (new_bust) : \
  ((on_bust) == bust) ?   (((new_bust)==bust) ? bust : ((new_bust)==badencode) ? badencodeandbust : on_bust) : \
  ((on_bust) == badencode) ? (((new_bust)==nobust) ? (on_bust) : ((new_bust)==badencode) ? badencode : badencodeandbust) \
  ((on_bust) == badencodeandbust) ? badencodeandbust : badencodeandbust)
#endif
typedef struct _df_extra_info {
    //duckdb_connection ddb_con;
  //char *file_name;
  int32_t verbose;
} df_extra_info;

#define MAXINTREAD 22

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

  DF_BustType line_is_busted;
  //iStr st_line; iStr end_line;
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
  char int_scratch[MAXINTREAD]; // Maximum number of digits to read i64 integer

  char *ignore_line_text;  char *keep_line_text; 
  int64_t start_byte;  int64_t end_byte;
  int DONE;


} df_init_data; 

typedef struct _df_bind_data {
  int32_t verbose;

  short report_bust; short report_line;
  //duckdb_connection ddb_con;
  idx_t cardinality;
  //duckdb_result *p_result;
  char *file_name;

  char *ignore_line_text;  char *keep_line_text; 
  int64_t start_byte;  int64_t end_byte;
  int DONE;
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
df_bind_data *df_bind_error_case(duckdb_bind_info b_info, int verbose, char* ll_file_name, int len_file_name, 
  char* ll_json_file_name, int len_json_file_name,
  DF_config_file *dfc, DF_field_list *dfl);
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
   char*sf, iStr valStart, iStr valEnd, int verbose, int width, int scale, DF_TSType fmttyp, int nCol, char char_sep);
void duckfix_main_table_function(duckdb_function_info df_info, duckdb_data_chunk out_chunk);
