//#pragma once
#ifndef DUCKDBH
#include "duckdb.h"
#define DUCKDBH 0
#endif

#ifndef DUCKDB_EXTENSIONH
#include "duckdb_extension.h"
#define DUCKDB_EXTENSIONH 0
#endif 

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

