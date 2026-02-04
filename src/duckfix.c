
#ifndef DUCKFIXLOADH
#include "include/df_load.h"
#define DUCKFIXLOADH 0
#endif

#include "duckdb_extension.h"
#include "duckdb.h"

//#ifndef ANH
//#include "add_numbers.h"
//#define ANH 1
//#endif

#ifndef STDIO
#include <stdio.h>
#define STDIO 1
#endif

#ifndef DUCKFIX_READ_FILE_H
#include "include/read_file.h"
#define DUCKFIX_READ_FILE_H 0
#endif

//#ifndef DUCKFIXTYPESH
//#include "include/duckfix_types.h"
//#define DUCKFIXTYPESH 0
//#endif


#ifndef DUCKFIXTABLETYPESH
#include "include/df_table_types.h"
#define DUCKFIXTABLETYPESH 0
#endif
#ifndef DUCKFIXH
#include "include/duckfix.h"
#define DUCKFIXH 0
#endif
#ifndef vprintf
#define vprintf(X, Y, ...)    \
  if (verbose >= (X)) {       \
    printf( (Y),__VA_ARGS__);  \
  }                           \
  verbose = verbose
#endif

DUCKDB_EXTENSION_EXTERN

void destroy_null(void *p_to_null) {
  // Do not destroy a null
  return;
}
void register_duckfix_tf_01(duckdb_connection ddb_con) {
  char stt[] = "-- register_duckfix_tf_01(): ";
  printf("----------------------------------------------------------------------------\n");
  printf("%s: We have registered duckfix_tf_01. \n", stt);
  printf("register duckfix, welcom tf_01!.\n");
  char *test_str = "\"Hello, I am a \\\\\\\"HelloStr\\\"\"\0\0";  iStr len_str = 30;
  printf(" -- Testing Quote next end for test_str = %s. \n", test_str);
  iStr jEnd = get_end_quote("register_duckfix_tf_01", test_str, 0, len_str); 
  printf(" -- Result of this was jEnd = %ld/%ld. --%.*s--, and last char of test_str[%d]=\'%c\' \n", 
    (long int) jEnd, (long int) len_str, (int) jEnd-1, test_str+1, (long int) jEnd, jEnd < 0 ? 'X' : test_str[jEnd]);

  char *test_str_bracket = "[\"Hello Str\", \"2\", 3],2,3,{1,2},[1,2]\0\0\0";  iStr len_str_bk = 37;
  printf(" -- Testing Bracket next end for test_str = %s. \n", test_str_bracket);
  iStr jEnd_bk = get_end_bracket("register_duckfix_tf_01", test_str_bracket, 0, len_str_bk); 
  printf(" -- Result of this was jEnd_bk = %ld/%ld. --[%.*s]--, and last char of test_str_braket[%d]=\'%c\' \n", 
    (long int) jEnd_bk, (long int) len_str_bk, (int) jEnd_bk-1, test_str_bracket+1, (long int) jEnd_bk, jEnd_bk < 0 ? 'X' : test_str_bracket[jEnd_bk]);


  char *test_str_brace = "{\"Hello Str\":\"2\",\"a\":[1,2,3],\"b\": {\"x\":[1,2], \"y\":\"yes\"}, \"george\":\"3\"};\0\0\0\0\0\0\0\0\0\0";  iStr len_str_bc = 72;
  printf(" -- Testing Brace next end for test_str = %.*s. \n", len_str_bc, test_str_brace);
  iStr jEnd_bc = get_end_brace("register_duckfix_tf_01", test_str_brace, 0, len_str_bc); 
  printf(" -- Result of this was jEnd_bc = %ld/%ld. --{%.*s}--, and last char of test_str_brace[%d]=\'%c\' \n", 
    (long int) jEnd_bc, (long int) len_str_bc, (int) jEnd_bc-1, test_str_brace+1, (long int) jEnd_bc-1, jEnd_bc < 0 ? 'X' : test_str_brace[jEnd_bc]);
  printf("-----------------------------------------------------------------------------\n");
}
void register_duckfix_development_test_reader_function(duckdb_connection ddb_con) {
  char stt[] = "-- register_duckfix_development_test_reader(): ";
  printf("----------------------------------------------------------------------------\n");
  printf("%s We are registering duckfix_test_read. \n", stt);
  char *sf = NULL;
  iStr nmax =  load_file_to_str(&sf, "./config_jsons/fix42.json", 2);
  printf("%s -- File is read, and length is nmax = %ld \n", stt, nmax);
  //printf("--- Printing file. \n");
  //printf("%.*s\n", nmax, sf);
  printf("--- Here is last [%ld:%ld] of sf=%.*s\n",
    nmax-5 < 0 ? 0 : nmax-5, nmax,
    nmax - (nmax-5 < 0 ? 0 : nmax-5), sf + (nmax-5 < 0 ? 0 : nmax-5));
  printf("--- That is entire file, is it useful? \n");
  printf("-----------------------------------------------------------------------------\n");
  printf("%s Let's test looking for keys in the file. \n\n", stt);
  const char name_key[] = "name\0";
  iStr find_name_ii =find_key(name_key, 4, sf, 0, nmax, 3);
  if (find_name_ii >= 0) {
    printf("%s register_duckfix_test_read::: \n", stt);
    printf("--- We found key=%.*s, presumably at %ld/%ld or --%.*s-- \n",
      4, name_key, (long int) find_name_ii,(long int) nmax,  4, sf + find_name_ii + 1);
  } else {
    printf("--- We failed to find key=%.*s. \n", 4, name_key);
  }
  printf("-------------------------------------------------------------------------------\n");
  printf("--- Testing out count_n_schemas. \n");
  int count_n_schemas = get_n_schema("test_duckfix[duckfix_c]", sf, 0, nmax, 0);
  printf("--- Our count of schemas is %ld \n", (long int) count_n_schemas);
  printf("--- HERE IS HARDEST PART READING IN DFC \n");
  DF_config_file *dfc = get_config_file(sf, 0, nmax, 1);
  printf("--- SUCCESS we populated dfc, what is it?. -- PRINTING --\n");
  PRINT_dfc(dfc);
  idx_t standard_vector_size = duckdb_vector_size();
  printf("--- About to generate file list\n");  // char_sep =','
  DF_field_list *dfl = generate_field_list("./example/ex1.csv", dfc, ',',1, (int) standard_vector_size);
  printf("--- Got File list now printing\n");
  PRINT_dfl(dfl);
  printf("--- Now configuring column order: \n");
  int success_cco = configure_column_order(dfc, dfl, 3);
  printf("--- Configure column order returned %ld. \n", success_cco);
  PRINT_final_print_loc(dfc, dfl);
  printf("\n\n\nDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD\n");
  printf("---  Well that is what we read, now deleting. \n");
  int success = delete_config_file(&dfc, 3);
  int success2 = delete_field_list(&dfl, 2);
  printf("--- Upon deleting success = %ld and dfc is %s \n",
    (long int) success, dfc == NULL ? "NULL" : "NOT NULL");
  printf("%s  Complete. \n", stt);
  printf("--------------------------------------------------------------------\n");
  return;
}
void register_duckfix_production_table_function(duckdb_connection ddb_con) {
  // create a scalar function
  //char stt[] = "-- register_duckfix_production_table_function(): ";
  //printf("%s -- Registering. \n", stt);
  duckdb_table_function duckfix_f = duckdb_create_table_function();
 
  //printf("%s -- registering a function. \n", stt); 
  duckdb_table_function_set_name(duckfix_f, "read_fixlog");
  df_extra_info *x_info = duckdb_malloc(sizeof(df_extra_info));
  //x_info->ddb_con = ddb_con;
  duckdb_table_function_set_extra_info(duckfix_f, x_info, destroy_df_extra_info);

  //printf("%s -- creating verbose parameter. \n", stt);
  duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  //void duckdb_table_function_add_named_parameter(duckdb_table_function table_function, const char *name, duckdb_logical_type type); 
  duckdb_table_function_add_named_parameter((duckdb_table_function) duckfix_f, 
    (const char*) "verbose", (duckdb_logical_type) int_type);
  duckdb_destroy_logical_type(&int_type);

  //printf("%s -- creating filename/json_filename parameters. \n", stt);
  duckdb_logical_type str_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  //void duckdb_table_function_add_named_parameter(duckdb_table_function table_function, const char *name, duckdb_logical_type type); 
  duckdb_table_function_add_named_parameter((duckdb_table_function) duckfix_f, 
    (const char*) "file_name", (duckdb_logical_type) str_type);
  duckdb_table_function_add_named_parameter((duckdb_table_function) duckfix_f, 
    (const char*) "json_file_name", (duckdb_logical_type) str_type);
  duckdb_table_function_add_named_parameter((duckdb_table_function) duckfix_f, 
    (const char*) "char_sep", (duckdb_logical_type) str_type);
  duckdb_destroy_logical_type(&str_type);

  //printf("%s -- Attaching the bind, init, main functions. \n", stt);
  duckdb_table_function_set_bind(duckfix_f, &duckfix_bind);
  duckdb_table_function_set_init(duckfix_f, &duckfix_init);
  duckdb_table_function_set_function(duckfix_f, &duckfix_main_table_function);
  //duckdb_table_function_set_function(duckfix_f, &duckfix_core_table_function);

  //printf("%s -- Registering the table function. \n", stt);
  duckdb_state result = duckdb_register_table_function(ddb_con, duckfix_f);

  //printf("%s -- Ready to destroy the table function. \n", stt);
  duckdb_destroy_table_function(&duckfix_f);
  //printf("%s -- we have a right to destroy duckfix. \n", stt);
  //printf("--------------------------------------------------------------------------\n");
  printf(" -- duckfix registered.  Call \"from read_fix_loc(verbose:=X, char_sep:=',',file_name='FILE',json_file_name='JSONFILE');\" to run. \n");
}
// Note everything below is legacy functionality to be replaced with
// 1. df_bind.c
// 2. df_init.c
// 3. df_main.c
/***************
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
  printf("REGISTER_WHY  IS THIS HERE? \n\n");
  printf("register_test_add_table_function() -- Registering. Why the heck is this registered?  This doesn't make sense.  \n");
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
***********************************/
