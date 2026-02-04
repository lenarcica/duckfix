#pragma once

#include "duckdb_extension.h"


typedef struct _ta_extra_info {
  duckdb_connection ddb_con;
  int32_t verbose;
} ta_extra_info;

typedef struct _ta_init_data {
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
} ta_init_data; 

typedef struct _ta_bind_data {
  int32_t verbose;
  duckdb_connection ddb_con;
  idx_t cardinality;
  duckdb_result *p_result;
} ta_bind_data;

//void RegisterAddNumbersFunction(duckdb_connection connection);
//static void test_add_manipulate_columns(duckdb_result *p_dresult);
void register_test_add_table_function(duckdb_connection ddb_con);
