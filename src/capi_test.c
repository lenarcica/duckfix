#include "duckdb_extension.h"

#ifndef DUCKDBH
#include "../duckdb_capi/duckdb.h"
#define DUCKDBH 0
#endif
#ifndef DUCKDB_EXTENSIONH
#include "../duckdb_capi/duckdb_extension.h"
#define DUCKDB_EXTENSIONH 0
#endif 

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection ddb_con, duckdb_extension_info info, struct duckdb_extension_access *access) {
  //register_duckfix_table_function(ddb_con);
  register_duckfix_tf_01(ddb_con);
  // Return true to indicate succesful initialization
  return true;
}
