#include "duckdb_extension.h"
#include "duckdb.h"

#ifndef ANH
#include "add_numbers.h"
#define ANH 1
#endif

#ifndef STDIO
#include <stdio.h>
#define STDIO 1
#endif

#ifndef vprintf
#define vprintf(X, Y, ...)    \
  if (verbose >= (X)) {       \
    printf( (Y),__VA_ARGS__);  \
  }                           \
  verbose = verbose
#endif

DUCKDB_EXTENSION_EXTERN

void destroy_ta_bind_data(void *v_ta_bd) {
  ta_bind_data *ta_bd = (ta_bind_data*) v_ta_bd;
  duckdb_destroy_result(ta_bd->p_result);
  duckdb_free(ta_bd->p_result);  ta_bd->p_result = NULL;
  return; 
}
void destroy_ta_init_data( void *v_ta_id) {
  ta_init_data *ta_id = (ta_init_data*) v_ta_id;
  duckdb_destroy_result(ta_id->p_result);
  printf("destory_ta_init_data:  Reeing p_dresult now. \n");
  duckdb_free(ta_id->p_result);
  ta_id->p_result = NULL;
  ta_id->ddb_con = NULL;
  duckdb_free(ta_id);
  return;
}

void destroy_null(void *p_to_null) {
  // Do not destroy a null
  return;
}

void test_add_bind(duckdb_bind_info b_info) {
  char ta_str[] = "test_add_bind: ";
  ta_extra_info *x_info = duckdb_bind_get_extra_info((duckdb_bind_info) b_info);
  duckdb_connection ddb_con = x_info->ddb_con;
  ta_bind_data *ta_bd = duckdb_malloc(sizeof(ta_bind_data));
  duckdb_value db_verbose = duckdb_bind_get_named_parameter(b_info, "verbose");
  int32_t verbose = duckdb_get_int32(db_verbose);
  ta_bd->verbose = verbose;  x_info->verbose = verbose;
  vprintf(1,"%s STARTING\n", ta_str);

  duckdb_result *p_result = (duckdb_result*) duckdb_malloc(sizeof(duckdb_result));
  
  const char test_add_create_query[] = "CREATE OR REPLACE TABLE TEMP (x INTEGER, y INTEGER);\n"
                      "INSERT INTO TEMP VALUES (1,2);\n"
                      "INSERT INTO TEMP VALUES (2,4); INSERT into TEMP VALUE(5, NULL);\n"
                      "ALTER TABLE TEMP ADD COLUMN (z, INTEGER);";
  const char test_add_count_query[] = "SELECT cast(COUNT(*) as INTEGER) as x from TEMP";
  const char test_add_get_query[] =  "SELECT x, y, 0 as z from TEMP";
 
  vprintf(1, "%s about to launch the null query. \n", (char*) ta_str);

  duckdb_state dstate = (duckdb_state) 0;
  dstate = duckdb_query(ddb_con, (const char*) test_add_create_query, NULL);
  if (dstate == DuckDBError) {
    printf("%s ERROR, attempt to read from TEMP failed. string %s\n", ta_str, test_add_create_query); 
    duckdb_bind_set_bind_data(b_info, (void*) NULL, destroy_null);
    return;
  }

  dstate = duckdb_query(ddb_con, (const char*) test_add_count_query, p_result);
  if (dstate == DuckDBError) {
    printf("%s ERROR, attempt to read from TEMP failed. string %s\n", ta_str, test_add_count_query); 
    duckdb_bind_set_bind_data(b_info, (void*) NULL, destroy_null);
    return;
  }
  duckdb_data_chunk count_chunk = duckdb_fetch_chunk((*p_result));
  duckdb_vector colX = duckdb_data_chunk_get_vector(count_chunk, 0);
  int32_t *colX_data = (int32_t *) duckdb_vector_get_data(colX);
  idx_t cardinality = (idx_t) colX_data[0]; // Get Count of table rows of target
  ta_bd->cardinality = cardinality;
  vprintf(1, " Destroying Count Result, result was %ld \n", (long int) cardinality);
  duckdb_destroy_result(p_result);
  vprintf(1, "Freeing p_result to allocate again");
  duckdb_free(p_result);
  p_result = (duckdb_result*) duckdb_malloc(sizeof(duckdb_result)); 
  vprintf(1, "Last Query: %s \n", test_add_get_query);
  
  dstate = duckdb_query(ddb_con, (const char*) test_add_get_query, p_result);
  if (dstate == DuckDBError) {
    printf("%s ERROR, attempt to read from TEMP failed. string %s\n", ta_str, test_add_create_query); 
    duckdb_bind_set_bind_data(b_info, (void*) NULL, destroy_null);
    return;
  }
  vprintf(1, "%s -- We just ran the query. \n", ta_str);
  ta_bd->ddb_con = x_info->ddb_con;
  ta_bd->p_result = p_result;
  
  //void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type);
  duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_bind_add_result_column(b_info, "X", int_type); 
  duckdb_bind_add_result_column(b_info, "Y", int_type); 
  duckdb_bind_add_result_column(b_info, "Z", int_type); 
  duckdb_destroy_logical_type(&int_type);
  //duckdb_read_stat_init_data *data = duckdb_malloc(sizeof(duckdb_read_stat_init_data));
  //data->offset = 0;
  //data->actual_rows_read = 0;

  duckdb_bind_set_cardinality(b_info,  cardinality, (bool) 1);
  vprintf(1, "%s -- Manipulating Full Columns of result. \n", ta_str);
  //test_add_manipulate_columns(p_result);

  vprintf(1, "test_add_bind: Columns are finished manipulation. \n");
  duckdb_bind_set_bind_data(b_info, ta_bd, destroy_ta_bind_data);

  return;
}

void test_add_init(duckdb_init_info i_info) {
  char ta_str[] = "test_add_int: ";
  ta_init_data * ta_id = (ta_init_data*) duckdb_malloc(sizeof(ta_init_data));
  ta_extra_info *x_info = (ta_extra_info*) duckdb_init_get_extra_info(i_info);
  ta_bind_data *ta_bd = (ta_bind_data*) duckdb_init_get_bind_data(i_info); 
  ta_id->verbose = ta_bd->verbose;  int32_t verbose = ta_bd->verbose;

  vprintf(1, "%s  initialize \n", ta_str);
  ta_id->on_in_chunk = NULL;
  ta_id->on_in_row = 0;
  ta_id->n_out_chunks = 0;
  ta_id->n_in_chunks; 
  vprintf(1, "%s: Columns are finished manipulation. \n", ta_str);
    duckdb_init_set_init_data(i_info, ta_id, destroy_ta_init_data);
  vprintf(1, "%s, Finishing \n", ta_str);    
}
void test_add_table_function( duckdb_function_info tf_info, duckdb_data_chunk out_chunk) {
  ta_init_data *ta_id = (ta_init_data *)duckdb_function_get_init_data(tf_info);
  ta_bind_data *ta_bd = (ta_bind_data *)duckdb_function_get_bind_data(tf_info);
  int32_t verbose = ta_bd->verbose;
  char ta_str[] = "test_add_table_function: ";
  vprintf(1, "%s: experiment_add_function we ran, but what do we do?", ta_str);

  ta_extra_info *x_info = (ta_extra_info *) duckdb_function_get_extra_info(tf_info);
  vprintf(1, "%s: experiment_add_function -- we got data .", ta_str);
  if (ta_id->p_result == NULL) {
    printf("%s: ERROR -- p_dresult is NULL. ", ta_str);  return;
  }
  
  idx_t out_row_count = duckdb_data_chunk_get_size(out_chunk);
  vprintf(1, "%s: Do we even do anything because algorithms as run,"
    " out_chunk has size %ld.\n", (char*) ta_str, (long int) out_row_count);

  duckdb_vector o_col_x = duckdb_data_chunk_get_vector(out_chunk, 0);
  int32_t *o_col_x_data = (int32_t *) duckdb_vector_get_data(o_col_x);
  uint64_t *o_col_x_validity = duckdb_vector_get_validity(o_col_x);

  duckdb_vector o_col_y = duckdb_data_chunk_get_vector(out_chunk, 1);
  int32_t *o_col_y_data = (int32_t *) duckdb_vector_get_data(o_col_y);
  uint64_t *o_col_y_validity = duckdb_vector_get_validity(o_col_y);

  duckdb_vector o_col_z = duckdb_data_chunk_get_vector(out_chunk, 2);
  int32_t *o_col_z_data = (int32_t *) duckdb_vector_get_data(o_col_z);
  uint64_t *o_col_z_validity = duckdb_vector_get_validity(o_col_z);

  idx_t on_out_row = 0;
  while (on_out_row < out_row_count) {
    if (ta_id->on_in_chunk != (duckdb_data_chunk) NULL) {
      duckdb_vector i_col_x = duckdb_data_chunk_get_vector(ta_id->on_in_chunk, 0);
      int32_t *i_col_x_data = (int32_t *) duckdb_vector_get_data(i_col_x);
      uint64_t *i_col_x_validity = duckdb_vector_get_validity(i_col_x);

      duckdb_vector i_col_y = duckdb_data_chunk_get_vector(ta_id->on_in_chunk, 1);
      int32_t *i_col_y_data = (int32_t *) duckdb_vector_get_data(i_col_y);
      uint64_t *i_col_y_validity = duckdb_vector_get_validity(i_col_y);

      duckdb_vector i_col_z = duckdb_data_chunk_get_vector(ta_id->on_in_chunk, 2);
      int32_t *i_col_z_data = (int32_t *) duckdb_vector_get_data(i_col_z);
      uint64_t *i_col_z_validity = duckdb_vector_get_validity(i_col_z);
      while (ta_id->on_in_row < ta_id->on_in_row_count) {
        o_col_z_data[(idx_t) on_out_row] = i_col_z_data[ta_id->on_in_row];
        o_col_x_data[(idx_t) on_out_row] = i_col_x_data[ta_id->on_in_row];
        o_col_y_data[(idx_t) on_out_row] = i_col_y_data[ta_id->on_in_row];

        o_col_z_validity[(idx_t) on_out_row] = i_col_z_validity[ta_id->on_in_row];
        o_col_x_validity[(idx_t) on_out_row] = i_col_x_validity[ta_id->on_in_row];
        o_col_y_validity[(idx_t) on_out_row] = i_col_y_validity[ta_id->on_in_row];
        ta_id->on_in_row++;
      }
    }
    ta_id->on_in_chunk = duckdb_fetch_chunk( (*ta_bd->p_result));
    if (!(ta_id->on_in_chunk)) {
      vprintf(1, "%s: Reached a null chunk on fetch chunk %ld.\n", (char*) ta_str,
        (long int) ta_id->n_in_chunks);
        break;
    } else {
      ta_id->n_in_chunks++; ta_id->on_in_row = 0; 
      ta_id->on_in_row_count = duckdb_data_chunk_get_size(ta_id->on_in_chunk);
    }
  }
  vprintf(1, "%s: we have completed the chunk %ld.\n", (char*) ta_str, (long int) ta_id->n_out_chunks);
  ta_id->n_out_chunks++;

}

// Scalar function that adds two numbers together
static void AddNumbersTogether(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	// get the total number of rows in this chunk
	idx_t input_size = duckdb_data_chunk_get_size(input);
	// extract the two input vectors
	duckdb_vector a = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector b = duckdb_data_chunk_get_vector(input, 1);
	// get the data pointers for the input vectors (both int64 as specified by the parameter types)
	int64_t* a_data = (int64_t *)duckdb_vector_get_data(a);
	int64_t*  b_data = (int64_t *)duckdb_vector_get_data(b);
	int64_t*  result_data = (int64_t *)duckdb_vector_get_data(output);
	// get the validity vectors
	uint64_t* a_validity = duckdb_vector_get_validity(a);
	uint64_t* b_validity = duckdb_vector_get_validity(b);
	if (a_validity || b_validity) {
		// if either a_validity or b_validity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		uint64_t* result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < input_size; row++) {
			if (duckdb_validity_row_is_valid(a_validity, row) && duckdb_validity_row_is_valid(b_validity, row)) {
				// not null - do the addition
				result_data[row] = a_data[row] + b_data[row];
			} else {
				// either a or b is NULL - set the result row to NULL
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < input_size; row++) {
			result_data[row] = a_data[row] * b_data[row];
		}
	}
}

void destroy_ta_extra_info(ta_extra_info *x_info) {
  x_info->ddb_con = NULL;
  duckdb_free(x_info);
}
// Register the AddNumbersFunction
void register_test_add_table_function(duckdb_connection ddb_con) {
  // create a scalar function
  printf("register_test_add_table_function() -- Registering. \n");
  duckdb_table_function test_add_f = duckdb_create_table_function();
  
  duckdb_table_function_set_name(test_add_f, "test_add");
  ta_extra_info *x_info = duckdb_malloc(sizeof(ta_extra_info));
  x_info->ddb_con = ddb_con;
  duckdb_table_function_set_extra_info(test_add_f, x_info, destroy_ta_extra_info);


  duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  //void duckdb_table_function_add_named_parameter(duckdb_table_function table_function, const char *name, duckdb_logical_type type); 
  duckdb_table_function_add_named_parameter((duckdb_table_function) test_add_f, (const char*) "verbose", (duckdb_logical_type) int_type);
  duckdb_destroy_logical_type(&int_type);

  duckdb_table_function_set_bind(test_add_f, &test_add_bind);
  duckdb_table_function_set_init(test_add_f, &test_add_init);
  duckdb_table_function_set_function(test_add_f, &test_add_table_function);

  duckdb_state result = duckdb_register_table_function(ddb_con, test_add_f);

  duckdb_destroy_table_function(&test_add_f);
}
/*
void RegisterAddTableFunction(duckdb_connection connection) {
    // duckdb_append_data_chunk
     //AddMaterialToTemp
	duckdb_table_function function = duckdb_create_table_function();
	duckdb_scalar_function_set_name(function, "add integer _numbers_together");

	// add a two bigint parameters
	//
     duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_bind_add_result_column(b_info, "Y", int_type); 
  duckdb_bind_add_result_column(b_info, "Z", int_type); 
  duckdb_destroy_logical_type(&int_type);
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_table_function_add_parameter(function, type);

	// set the return type to bigint
	duckdb_scalar_function_set_return_type(function, type);

	duckdb_destroy_logical_type(&type);

	// set up the function
	duckdb_table_function_set_function(function, AddMaterialToTemp);
     
	// register and cleanup
	duckdb_register_table_function(connection, function);
	duckdb_destroy_table_function(&function);


}
*/
