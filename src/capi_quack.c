#ifndef DUCKDB_EXTENSIONH
#define DUCKDB_EXTENSIONH 0
#include "duckdb_extension.h"
#endif
//#ifndef DUCKFIXTYPESH Not needed yet
//#include "include/duckfix_types.h"
//#define DUCKFIXTYPESH 0
//#endif
#ifndef DUCKFIXH
#define DUCKFIXH 0
#include "include/duckfix.h"
#endif
//#ifndef ANH
//#include "add_numbers.h"
//#define ANH 1
//#endif
#ifndef DDBEXT
#define DDBEXT 0
DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection ddb_con, duckdb_extension_info info, struct duckdb_extension_access *access) {
  //register_test_add_table_function(ddb_con);
  //register_duckfix_tf_01(ddb_con);
  //register_duckfix_development_test_reader_function(ddb_con);
  register_duckfix_production_table_function(ddb_con);
  //register_duckfix_test_read(ddb_con);
  // Return true to indicate succesful initialization
  return true;
}
#endif
