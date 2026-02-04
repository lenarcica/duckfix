//#pragma once
#ifndef DUCKDBH
#include "duckdb.h"
#define DUCKDBH 0
#endif

#ifndef DUCKDB_EXTENSIONH
#define DUCKDB_EXTENSIONH 0
#include "duckdb_extension.h"
#endif 


/*
typedef struct _df_extra_info {
  duckdb_connection ddb_con;
  //duckdb_client_context *p_context;
  int32_t verbose;
  duckdb_result *p_dresult;
} df_extra_info;

typedef struct _df_init_data {
  int32_t verbose;
  duckdb_connection ddb_con;
  idx_t offset;
  idx_t actual_rows_read;
  duckdb_result *p_result;
  duckdb_data_chunk on_in_chunk;
  idx_t on_in_row;
  idx_t on_in_row_count;
  idx_t n_out_chunks;
  idx_t n_in_chunks; 
} df_init_data; 

typedef struct _df_bind_data {
  int32_t verbose;
  duckdb_connection ddb_con;
  idx_t cardinality;
  duckdb_result *p_result;
} df_bind_data;
*/
//void destroy_df_bind_data(void *v_df_bd); 
//void destroy_df_init_data( void *v_df_id);
//void RegisterAddNumbersFunction(duckdb_connection connection);
//static void test_add_manipulate_columns(duckdb_result *p_dresult);
//void register_duckfix_table_function(duckdb_connection ddb_con);
//void register_duckfix_table_function(duckdb_connection ddb_con);
void register_duckfix_tf_01(duckdb_connection ddb_con);
void register_duckfix_development_test_reader_function(duckdb_connection ddb_con);
void register_duckfix_production_table_function(duckdb_connection ddb_con);
