////////////////////////////////////////////
//  df_init.c
//
//  "Init" function (or local_init)
//
//  An Init function is called right before duckdb starts running a "main" table function
//
//  The order is
//     A. bind (df_bind.c) Orchestrate non-duckdb parts together
//     B. init (df_init.c) create data initializers (or thread specific initializers AKA "local_init")
//     C. "main" (actually unnamed or simply called a "table" function (see df_main.c)
//
//  In our organization we will have
//
//  I. Bind will open JSON schema afile and
//    i. Create "DF_config_file" object "dfc"
//    ii. Read the CSV "file_name" once over to see how often fields are mentioned
//    iii. Thus create "DF_field_lines" object "dfl"
//    iv. Identify where all of the break-line parts are so fseek() can seek new data chunk levels if needed
//    v. Then identify what columns will be printed to a DuckDB based upon remaining priority order
//    vi. Have a deletion function for Bind object that fully deletes dfc/dfl
//  II. Init will
//    i. Because C-API doesn't have thread-identifier yet, we won't implement local_init style parallelism
//    ii. Open the "file_name" for reading with a file pointer starting at zero. (fpo)
//    iii. Init the number of lines read at zero.  (on_overall_line)
//    iv. Have a deletion function for Init object that closes fpo and NULLS its references to dfc/dfl
//  III. Finally the "main" will: In Chunks
//    i. Read a line from CSV
//    ii. Attach Fix Field information to relative fields in CSV
//    iii. Write data to relevant column in DuckDB Data chunk
//    iv. Write null information when field is missing in a line
//    v. Parse Dates, Decimals, Timestamps
//
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

//#define DUCKDB_EXTENSION_EXTERN extern duckdb_ext_api_v1 duckdb_ext_api;
//Have this statement called before code using extension table functions!
DUCKDB_EXTENSION_EXTERN

void destroy_df_init_data( void *v_df_id) {
  char stt[] = "df_init.c->destroy_df_init_data(): ";
  df_init_data *df_id = (df_init_data*) v_df_id;
  //duckdb_destroy_result(ta_id->p_result);
  int verbose = df_id->verbose-1;
  if (verbose >= 2) { printf("%s: INITIATE. \n", stt); }
  if (verbose >= 2) {
    printf("INIT -Data we are apparently quitting on_line %ld/%ld (local %ld for chunk %ld). \n", 
      (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines,
      (long int) df_id->on_chunk_line, (long int) df_id->on_chunk);
    printf(" --- futher tbytesread=%ld, bytesread=%ld, remainder=%ld, for total bytes=%ld \n",
      (long int) df_id->tbytesread, (long int) df_id->bytesread, (long int) df_id->remainder,
      (long int) df_id->dfl->file_total_bytes);
    printf(" --- We have last schema = %ld/%ld, with last fix number=%ld. \n",
      (long int) df_id->ion_schema, (long int) df_id->dfc->n_schemas,
      (long int) df_id->last_fix_num);
  }
  //duckdb_free(ta_id->p_result);
  df_id->dfc = NULL; df_id->dfl = NULL;
  if (df_id->fpo != NULL) { vpt(3, " Closing and clearing fpo\n"); fclose(df_id->fpo); df_id->fpo = NULL; };
  if (df_id->file_name != NULL) { vpt(3, "freeing filename \n"); free(df_id->file_name); df_id->file_name = NULL; }
  if (df_id->buffer != NULL) { vpt(3, "freeing df_id->buffer. \n"); free(df_id->buffer); df_id->buffer = NULL; }
  //duckdb_free(df_id);
  free(df_id);
  if (verbose >= 2) { printf("%s -- we are done. \n", stt); }
  return;
}

void duckfix_init(duckdb_init_info i_info) {
  char stt[500]; 
  sprintf(stt, "df_init.c->duckfix_init(): ");
  void * v_df_bd = (void *) duckdb_init_get_bind_data(i_info);
  df_bind_data *df_bd = (df_bind_data*) v_df_bd;
  df_extra_info *x_info = (df_extra_info*) duckdb_init_get_extra_info(i_info);
  int32_t verbose = df_bd->verbose; 
  if (verbose >= 1) {
    sprintf(stt, "df_init.c->duckfix_init(v=%ld): ", (long int) verbose);
  }
  if (verbose >= 2) {
    printf("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\n");
    sprintf(stt, "III df_init.c->duckfix_init(v=%ld): ", (long int) verbose);
    vpt(2, " -- Init, initializing. \n");
  }
  if (df_bd->dfl == NULL) {
    vpt(-1, "Error: dfl not set in df_bd. \n");
    duckdb_init_set_error(i_info, "df_bd does not contain dfl. \n"); return;
  }

  if (df_bd->dfc == NULL) {
    vpt(-1, "Error: dfc not set in df_bd. \n");
    duckdb_init_set_error(i_info, "df_bd does not contain dfc. \n"); return;
  }
  df_init_data *df_id = (df_init_data*) malloc(sizeof(df_init_data));
  if (df_id == NULL) { vpt(-1, "ERROR, df_id was not allocated of size (%ld) . \n", (long int) sizeof(df_init_data));
                       duckdb_init_set_error(i_info, "ERROR in INIT: Failed to allocate init data.\n");  
                       vpt(-1, " What do we do with these errors? manually destroying data we created (but didn't attach yet!)\n ");
                       destroy_df_init_data((void*) df_id);
                       return; 
  }
  df_id->file_name = NULL; df_id->fpo = NULL; df_id->on_chunk = 0; df_id->offset=0;  df_id->buffer = NULL;
  df_id->buffer = malloc(sizeof(char) * (MAXREAD+2)); df_id->fpo = (FILE*) NULL;
  if (df_id->buffer == NULL) {
    duckdb_init_set_error(i_info, "ERROR in INIT: Failed to allocate init df_id->buffer data.\n");  
    vpt(-1, " Buffer Allocated as a fail. \n"); 
    destroy_df_init_data((void*) df_id);
    return; 
  }
  df_id->st_buffer_loc = 0; df_id->end_buffer_loc=0;  df_id->verbose = verbose; 
  df_id->actual_rows_read = 0; df_id->on_overall_line = 0; df_id->on_chunk_line = 0;
  df_id->dfl=NULL; df_id->dfc = NULL;
  df_id->dfl = df_bd->dfl; df_id->dfc = df_bd->dfc;
  // Note df_id->buffer is fixed length and created at size MAXREAD on malloc.
  df_id->tbytesread = 0; df_id->bytesread = 0; df_id->remainder = 0;  df_id->buffreads = 0;
  df_id->onstr = 0; df_id->iLineEnd = 0;  df_id->last_fix_num = -1;  df_id->ion_schema = -1;
  int len_fn = (df_bd->file_name == NULL) ? 0 : (strlen(df_bd->file_name) <= 0 ? 0 : strlen(df_bd->file_name) +1);
  if (len_fn <= 0) {
    vpt(-1, "ERROR, df_bd did not have non-null filename. \n");  duckdb_init_set_error(i_info, " No file_name supplied from bind to init");
    vpt(-1, " What do we do with these errors? manually destroying data we created (but didn't attach yet!)\n ");
    destroy_df_init_data((void*) df_id);
    return;
  } 
  vpt(2, " We have len_fn = %ld, for filename = \"%s\"\n", (long int) len_fn, df_bd->file_name);
  df_id->file_name = (char*) malloc(sizeof(char)*(len_fn+1));
  if (df_id->file_name == NULL) { duckdb_init_set_error(i_info, " FAILED to allocate space for file_name. \n");
    vpt(-1, " What do we do with these errors? manually destroying data we created (but didn't attach yet!)\n ");
    destroy_df_init_data((void*) df_id); return;
  }
  memcpy(df_id->file_name, df_bd->file_name, len_fn); 
  df_id->file_name[len_fn-1] = '\0';
  vpt(1, " Now df_id->file_name=\"%s\"\n", df_id->file_name);
  df_id->fpo = NULL; 
  vpt(1, " Trying to open df_id->file_name=\"%s\". We believe there are %ld bytes. \n", 
    (char*) df_id->file_name, (long int) df_id->dfl->file_total_bytes);
  df_id->fpo = fopen(df_id->file_name, "rt");  rewind(df_id->fpo);
  if (df_id->fpo == NULL) {
    vpt(-1, " FAILED to open file_name = \"%s\". \n", df_id->file_name);
    duckdb_init_set_error(i_info, "Failed to open the file.\n");
    vpt(-1, " What do we do with these errors? manually destroying data we created (but didn't attach yet!)\n ");
    destroy_df_init_data((void*) df_id);
    return;
  }
  vpt(1, " Trying to read data into df_id->buffer of length MAXREAD = %ld. \n", (long int) MAXREAD);
  vpt(1, " Note before this df_id->buffer[0] = \'%c\', df_id->buffer[%ld]=\'%c\'. \n",
    df_id->buffer[0], MAXREAD-1, df_id->buffer[MAXREAD-1]);
  df_id->bytesread = fread( (char*) df_id->buffer,sizeof(char),MAXREAD,df_id->fpo);  
  vpt(1, " We have read %ld/%ld (MAXREAD=%ld) bytes from fpo. \n", (long int) df_id->bytesread, (long int) df_id->dfl->file_total_bytes, (long int) MAXREAD);
  df_id->onstr = 0;  df_id->remainder=0;
  if ((ferror(df_id->fpo)) || (df_id->bytesread <= 0) || (df_id->buffer == NULL) || (df_id->buffer[0] == '\0')) {
    vpt(-1, " ISSUE Initial read of bytes read is zero, perhaps empty file! \n");
    if (ferror(df_id->fpo)) { vpt(-1, " ERROR on fpo \n"); }
    if (df_id->bytesread <= 0) { vpt(-1, " bytes read is %ld. \n", df_id->bytesread); }
    if (df_id->buffer == NULL) { vpt(-1, " df_id->buffer is NULL. \n"); }
    if (df_id->buffer[0] == '\0') { vpt(-1, "df_id->buffer[0] is \'\0\'\n"); }
    fclose(df_id->fpo); df_id->fpo = NULL; 
    duckdb_init_set_error(i_info, "Failed to get lines in first buffer from the file.\n");
    vpt(-1, " What do we do with these errors? manually destroying data we created (but didn't attach yet!)\n ");
    destroy_df_init_data((void*) df_id);
    return;
  }
  vpt(2, " Note that ferror(df_id->fpo) is %d \n", (int) ferror(df_id->fpo));
  vpt(2, " Buffer starts: buffer[0:3] = \'%c%c%c\'\n \"\"\"\n%.*s\n...\"\"\"\n",
     df_id->bytesread >= 1 ? (char) df_id->buffer[0] : 'X',
     df_id->bytesread >= 2 ? (char) df_id->buffer[1] : 'X',
     df_id->bytesread >= 3 ? (char) df_id->buffer[2] : 'X',
     df_id->bytesread < 100 ? df_id->bytesread : 100, (char*) df_id->buffer + 0);

  //vpt(3, " Another Print of it all: \"\n");
  //if (df_id->verbose >= 3) {
  //  printf("%s\n\"\n", df_id->buffer);
  //}
  if ((df_id->bytesread <= 3) ||  ((df_id->buffer[0] == ' ') && (df_id->buffer[1] == ' ') && (df_id->buffer[2] == ' ')) ||
     df_id->buffer[0] == '\0' || df_id->buffer[1] == '\0' || df_id->buffer[2] == '\0') {
    vpt(-100, " ERROR bad file read for some reason, try to adjust and fix. \n");
    duckdb_init_set_error(i_info, "Failed to get lines in first buffer from the file.\n");
    vpt(-1, " What do we do with these errors? manually destroying data we created (but didn't attach yet!)\n ");
    destroy_df_init_data((void*) df_id);
    return;
  }
  vpt(1, " We are ready to start with bytesread=%ld/%ld (MAXREAD=%ld), on_overall_line=%ld/%ld. \n",
   (long int) df_id->bytesread, (long int) df_id->dfl->file_total_bytes, (long int) MAXREAD,
   (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
  vpt(1, "Setting init data. \n");
  //dfl->line_locs[0] = onstr;  
  duckdb_init_set_init_data(i_info, df_id, destroy_df_init_data);
  vpt(1, " -- Init Data is set. I'm guessing we shouldn't touch after setting.\n");
  vpt(1, " I think we reached the end of init data activities.  On to reading the buffer (or updating it) and processing the data chunks!\n");
}
