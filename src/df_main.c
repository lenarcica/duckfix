/////////////////////////////////////////////
//  df_main.c
//
//  Alan Lenarcic (lenarcic@post.harvard.edu)
//  2026-02-04
//  GPLv2 License (Please consider writing your own code since this is really demonstration code)
//
//  "Main" Table function (actual processing of chunks) 
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
//
//
//
#ifndef DUCKFIXTABLETYPESH
#include "include/df_table_types.h"
#define DUCKFIXTABLETYPESH 0
#endif
#ifndef DUCKFIXGENERALH
#include "include/df_general.h"
#define DUCKFIXGENERALH 0
#endif

#ifndef ERRNOH
#include <errno.h>
#define ERRNOH
#endif

#define DDBUG 0

#define SETINVALID(x) \
  duckdb_vector_ensure_validity_writable(ddbv); \
  ddbv_validity = duckdb_vector_get_validity((duckdb_vector) ddbv); \
  if (ddbv_validity == NULL) {                                      \
    duckdb_vector_ensure_validity_writable(ddbv);                   \
    ddbv_validity = duckdb_vector_get_validity((duckdb_vector) ddbv); \
  }                                                                   \
  duckdb_validity_set_row_invalid(ddbv_validity, (x))

DUCKDB_EXTENSION_EXTERN

int add_schema_entry_to_chunk(df_init_data *df_id, char*buffer, iStr st, 
  iStr end, duckdb_data_chunk out_chunk, int verbose) {
  DF_config_file *dfc = df_id->dfc;  
  DF_field_list *dfl = df_id->dfl;
  DF_DataType on_typ = dfc->schemas[df_id->ion_schema].typ;
  #ifdef DDBUG
    char stt[500];
    sprintf(stt, "  df_main.c->add_schema_entry_to_chunk(v=%d,ckln=%ld,oln=%ld/%ld,i_s=%d/%d,%s,\"%.*s\"): ",
      (int) verbose, (long int) df_id->on_chunk_line, (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines,
      (int) df_id->ion_schema, (int) dfc->n_schemas, (char*) (What_DF_DataType( (on_typ) )), end-st, buffer+st);
  #endif
  #ifndef DDBUG
    char stt[] = "  df_main.c->add_schema_entry_to_chunk(): ";
  #endif
  vpt(2, " Initialize(i_s=%d/%d,%s,\"%.*s\").\n",
    (int) df_id->ion_schema, (int) dfc->n_schemas, (char*) (What_DF_DataType( (on_typ) ) ), end-st, buffer+st);
  int ii = st;
  duckdb_vector ddbv = duckdb_data_chunk_get_vector(out_chunk, dfc->schemas[df_id->ion_schema].final_loc);
  void * vddbv = (void *)duckdb_vector_get_data(ddbv);  uint64_t *ddbv_validity=NULL;
  iStr nmax = end; char *sf = buffer;  iStr end_ln = end;
  PUSH_OUT_WHITE();  st = ii;
  if ((buffer[st]=='\"') && (buffer[end] == '\"')) {
    st++; end--;
  }
  int endl = end-ii+1;  duckdb_date ddd;
  char *end_pt; int32_t load_i32; int64_t load_i64;  int dLoc, ploc, foundscale; double flt64;
  int64_t load_o64;
  if (end-st +1 <= 0) {
    SETINVALID((df_id->on_chunk_line));
    return(0);
  } 
  int an_Error = fill_in_chunk(dfc->schemas[df_id->ion_schema].typ,  ddbv, df_id,
    buffer, st, end, verbose-1, dfc->schemas[df_id->ion_schema].width, dfc->schemas[df_id->ion_schema].scale, 
    dfc->schemas[df_id->ion_schema].fmttyp, dfc->schemas[df_id->ion_schema].final_loc);
  df_id->dfc->mark_visited[dfc->schemas[df_id->ion_schema].final_loc] = 1;
  return(an_Error);
}
int add_fixfields_entries_to_chunk(df_init_data *df_id, char *sf, iStr fixfieldsStart, iStr fixfieldsEnd, 
  duckdb_data_chunk out_chunk, int verbose) {
  #ifdef DDBUG 
    char stt[500];
    sprintf(stt, "  df_main.add_fixfields_entries_to_chunk(v=%d,ckln=%ld,oln=%ld/%ld,%ld:%ld,\"%.*s...\"): ",
      (int) verbose, 
      (long int) df_id->on_chunk_line, (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines,
      (long int) fixfieldsStart,  fixfieldsEnd, (fixfieldsEnd-fixfieldsStart < 15) ? fixfieldsEnd-fixfieldsStart : 15,
      sf + fixfieldsStart); 
  #endif
  #ifndef DDBUG
    char stt[] = "  df_main.add_fixfields_entries_to_chunk(): ";
  #endif
  if (fixfieldsEnd < fixfieldsStart) {
    vpt(-3, "ERROR addfixfields_entries_to_chunk, fixfieldsStart=%ld, fixfieldsEnd=%ld.  Obvious error. \n",
      (long int) fixfieldsStart, (long int) fixfieldsEnd);
    return(-4303234);
  }
  iStr end_ln = fixfieldsEnd;
  int ii = fixfieldsStart;  iStr nmax = fixfieldsEnd;
  if (sf[ii] == '{') {
  } else {
    PUSH_OUT_WHITE();  
  }
  if (ii >= fixfieldsEnd) {
    printf("ERROR %s -- We hit ii>=fixfieldsEnd=%ld on first PUSH_OUT_WHITE. \n",
      stt, (long int) fixfieldsEnd);
    printf(" --- We started with sf[fixfieldsStart=%ld:%ld] = \"%.*s\" \n", 
      (long int) fixfieldsStart, (long int) fixfieldsEnd, fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
  }
  fixfieldsStart = ii;
  if (sf[fixfieldsStart] != '{') {
    vpt(0, "State ERROR, we do not see this field starting with a '{', we are on a \"%.*s...\",\n",
      (fixfieldsEnd-fixfieldsStart < 10) ? fixfieldsEnd-fixfieldsStart: 10, sf + fixfieldsStart);
    return(-140353);
  }
  int cntFields = 0;  int aFixNum = 0;  iStr keyStart, keyEnd; 
  int an_error;  iStr stVal, endVal; char old_end = '.';
  for (ii = fixfieldsStart+1; ii < fixfieldsEnd;ii++) {
    vpt(2, " - ii=%ld, fS,fE=[%ld,%ld], cntFields=%ld, starting with sf[%ld:%ld]=\"%.*s\"\n",
      (long int) ii, (long int) fixfieldsStart, (long int) fixfieldsEnd, (long int) cntFields,
      (long int) ii, (long int) fixfieldsEnd, fixfieldsEnd-ii, sf + ii);
    if (sf[ii] == '}') { 
      vpt(1, " -ii=%ld, fs,fE=[%ld,%ld], cntFields=%ld, we have an end point \'}\' \n",
        (long int) ii, (long int) fixfieldsStart, (long int) fixfieldsEnd, (long int) cntFields);
      break; 
    } else {
      //vpt(2, "  ---- Pushing out WHITE. \n", (long int) ii);
      PUSH_OUT_WHITE();  
      //vpt(2, "  ---- Done Pushing out WHITE, ii=%ld. \n", (long int) ii);
      if (sf[ii] != '\"') {
        vpt(0, "ERROR, We are on cntFields=%ld, but sf[%ld]=\'%c\' but expected \'\"\'\n", cntFields, ii, sf[ii]); 
        printf(" -- We do not see this field starting with a '{', we are on a \"%.*s...\",\n",
            (fixfieldsEnd-fixfieldsStart < 10) ? fixfieldsEnd-fixfieldsStart: 10, sf + fixfieldsStart);
        printf(" -- Note sf[ii-1=%ld] = \'%c\' \n", (long int) ii-1, sf[ii-1]);
        printf(" -- Note sf[fixfieldsStart=%ld] = \'%c\' \n", (long int) fixfieldsStart, sf[ii]);
        printf(" -- and in our zone sf[ii-5:ii+5] = \"%.*s\". \n",
            ((ii+5) < fixfieldsEnd ? (ii+5) : fixfieldsEnd) - (ii-5 >= 0 ? ii-5 : 0),  sf + (ii-5 >= 0 ? ii-5 : 0));
        return(-1040323);
      } else {
        vpt(2, " ---- on ii=%ld, we have a keyStart at ii+1=%ld, sf[%ld]=\'%c\'. \n", (long int) ii, (long int) ii+1,
          (long int)ii+1, sf[ii+1]);
        keyStart = ii+1;
        if (sf[ii] != '\"') { vpt(-3,"ERROR: No right before get_end_quote we have bad data. \n"); return(-1043043); }
        keyEnd = get_end_quote("add_fixfields", sf, ii, fixfieldsEnd);
        if (keyEnd < ii) { vpt(-4, "ERROR: keyEnd=%ld at the end quote, but \"add_fixfields\" had zero load. \n", (long int) keyEnd); 
          printf("ERROR so ii=%ld, fixfieldsStart=%ld, fixfieldsEnd=%ld. \n", (long int) ii, (long int) fixfieldsStart, (long int) fixfieldsEnd);
          printf("  -- note sf[ii=%ld] = \'%c\' we wanted \'\"\'\n", (long int) ii, sf[ii]);
          printf("  -- sf[fixfieldsStart=%ld:end=%ld] = \"%.*s\". \n",
             (long int) fixfieldsStart, (long int) fixfieldsEnd, fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
          return(-493023); 
        }
        vpt(2, " ---- on ii=%ld, we think the key is sf[%ld:%ld]=\"%.*s\". \n",
          (long int) ii, (long int) keyStart, (long int) keyEnd, keyEnd-keyStart, sf + keyStart);
        ii = keyEnd+1;
        PUSH_OUT_WHITE();
        if (sf[ii] == ':') {  ii++; } else {
          vpt(-10, " ---- Error on ii=%ld, we though key was sf[%ld:%ld]=\"%.*s\", did not find colon after, sf[%ld:%ld]=\"%.*s\". \n",
            (long int) ii, (long int) keyStart, (long int) keyEnd, keyEnd-keyStart, sf + keyStart,
            (long int) fixfieldsStart, (long int) fixfieldsEnd, fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
          return(-9340329);
        }
        PUSH_OUT_WHITE();
        if (sf[ii] == '\"') {
          stVal = ii+1;
          if (sf[ii] != '\"') { 
            vpt(-1," ERROR - before get_end quote, we thing sf[ii=%d]=\'%c\'. \n", (long int) ii, sf[ii]);
            return(-2034032); 
          }
          endVal = get_end_quote("add_fixfields_entries_to_chunk", sf, ii, fixfieldsEnd);
          vpt(2, " ---- on ii=%ld, key is sf[%ld:%ld]=\"%.*s\", we got get_end_quote val is sf[%ld:%ld]=\"%.*s\". \n",
            (long int) ii, (long int) keyStart, (long int) keyEnd, keyEnd-keyStart, sf + keyStart,
            (long int) stVal, (long int) endVal, endVal > stVal ? endVal-stVal : 3,
            endVal > stVal ? sf + stVal : "BAD");
          if (endVal < stVal) {
            vpt(0, "ERROR, we are on cntFields=%ld, but sf[%ld]=\'%c\' we got endVal=%ld however for sf[%d:%d]=\"%.*s\". \n",
              (long int) cntFields, (long int) ii, sf[ii], (long int) endVal, stVal-1, 
              fixfieldsEnd -stVal +1  > 20 ? stVal+20-1 : fixfieldsEnd,
              fixfieldsEnd -stVal +1  > 20 ? 20 : fixfieldsEnd-stVal+1,
              sf + stVal-1); return(-250434);
          }
        } else if ((sf[ii] == '{') || (sf[ii] == '[')) {
          vpt(0, " on cntFields=[%ld] we get sf[%ld] = \'%c\' for sf[%ld:%ld] = \"%.*s\"\n",
            (long int) cntFields, (long int) ii, sf[ii], fixfieldsStart, fixfieldsEnd, fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
          return(-1);
        } else {
          stVal = ii;  while( (ii < fixfieldsEnd) && (sf[ii] != ' ') && (sf[ii] != ',') && (sf[ii] != '}')) { ii++; }
          endVal = ii;
        } 
        vpt(2, " ---- on ii=%ld, key is sf[%ld:%ld]=\"%.*s\", we got val of field is sf[%ld:%ld]=\"%.*s\". \n",
            (long int) ii, (long int) keyStart, (long int) keyEnd, keyEnd-keyStart, sf + keyStart,
            (long int) stVal, (long int) endVal, endVal > stVal ? endVal-stVal : 3,
            endVal > stVal ? sf + stVal : "BAD");
        //stVal = get_value_bounds("add_fix_fields", sf, ii, fixFieldsEnd, verbose-1, &stVal, &endVal);
        //if (sf[stVal] == '\"') { stVal++; }
        //if ((sf[endVal] == '\"') || (sf[endVal] == '}') || (sf[endVal] == ' ')) {
        //} else if ((sf[endVal] >= '0') && (sf[endVal] <= '9')) {
        //  endVal++;
        //} else if ((sf[endVal] >= 'a') && (sf[endVal] <= 'z')) {
        //  endVal++;
        //} else if ((sf[endVal] >= 'A') && (sf[endVal] <= 'Z')) {  endVal++; }
        if ((keyEnd > fixfieldsStart) && (stVal > fixfieldsStart) && (endVal > stVal)) {
          old_end = sf[keyEnd];sf[keyEnd] = '\0';  aFixNum = atoi(sf+keyStart);  sf[keyEnd] = old_end;
          if (aFixNum > 0) {
            df_id->last_fix_num = aFixNum;
            vpt(3, " ----     We found aFixNum=%ld, for sf[%ld:%ld] val is \"%.*s\". For cntFields=%ld, call add_fixfield_entry_to_chunk.\n",
              (long int) aFixNum, (long int) stVal, (long int) endVal,  endVal-stVal, sf + stVal, (long int) cntFields);
            an_error = add_fixfield_entry_to_chunk(df_id, sf, aFixNum, stVal, endVal,  out_chunk, verbose-1);
            cntFields++;
            if (an_error < 0) {
              vpt(0, "ERROR, we are on cntFields=%ld, aFixNum=%ld, val=\"%.*s\" we get error %ld. \n",
                (long int) cntFields, (long int) aFixNum, endVal-stVal-1, sf+stVal+1, an_error);
              return(-234323);
            } else {
              vpt(3, " -- after add_fixfield_entry_to_chunk returned: %d, for fixField num=%ld, val was \"%.*s\", we move on. \n",
               (long int) an_error, (long int) aFixNum, endVal-stVal, sf + stVal);
              ii = endVal;
              if (sf[ii] == '\"') { ii++; } 
              PUSH_OUT_WHITE();
              if (sf[ii] == '}') { 
                vpt(1, " Reached end on ii=%ld, [ffS,ffE]=[%ld,%ld], found %ld fields in \"%.*s\"\n",
                  (long int) ii, (long int) fixfieldsStart, (long int) fixfieldsEnd, (long int) cntFields,
                  fixfieldsEnd -fixfieldsStart, sf + fixfieldsStart);
                return(cntFields); 
              }
              if (sf[ii] != ',') { 
                vpt(0, "ERROR on cntFields=%ld, aFixNum=%ld, sf[%ld]=\'%c\' we were " 
                  "looking for break but got sf[%ld:%ld]=\"%.*s\"\n",
                  (long int) cntFields, (long int) aFixNum, (long int) ii, sf[ii], (long int) fixfieldsStart,
                  (long int) fixfieldsEnd, (int) (fixfieldsEnd-fixfieldsStart), sf + fixfieldsStart);
                return(-55034032);
              } else {
                vpt(3, " -- so fixField=%ld for cntFields=%ld finished, found another \',\' at ii=%ld, for fS,fE=[%ld,%ld]:\"%.*s\", keep going. \n",
                  (long int) aFixNum, (long int) cntFields, (long int) ii, (long int) fixfieldsStart, (long int) fixfieldsEnd,
                  fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
              }
            }
          } else {
            vpt(0, "ERROR on cntFields=%ld, we had sf[%ld:%ld]=\"%.*s\", for sf[fs=%ld:fe=%ld]=\"%.*s\"\n",
              (long int) cntFields, keyStart, keyEnd, keyEnd-keyStart, sf+keyStart,
              fixfieldsStart, fixfieldsEnd, fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
            return(-53043023);
          }
        }
	 }
    }
    vpt(2, " --  -- continue the loop after fixField=%ld, sf[ii=%ld]=\'%c\', for fS,fE=[%ld,%ld]:\"%.*s\".  \n",
      (long int) aFixNum, (long int) ii, sf[ii], (long int) fixfieldsStart, (long int) fixfieldsEnd,
      fixfieldsEnd-fixfieldsStart, sf + fixfieldsStart);
  }
  vpt(2, "Success we found %ld total fields from [%ld:%ld]. \n", (long int) cntFields, fixfieldsStart, (long int) fixfieldsEnd);
  return(cntFields);
}
int add_fixfield_entry_to_chunk(df_init_data *df_id, char *sf, int fixField,  iStr valStart, iStr valEnd, duckdb_data_chunk out_chunk, int verbose) {
  #ifdef DDBUG
    char stt[500];
    sprintf(stt, "   add_fixfield_entry_to_chunk(v=%ld,chkln=%ld,oln=%ld/%ld,fld=%ld,val=\"%.*s\"): ",
      (long int) verbose,  (long int) df_id->on_chunk_line, (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines,
      (long int) fixField, valEnd-valStart, sf+valStart);
  #endif
  #ifndef DDBUG
  char stt[] = "   add_fixfield_entry_to_chunk(): ";
  #endif
  DF_field_list *dfl = df_id->dfl;  
  DF_config_file *dfc = df_id->dfc;
  vpt(2, " -- initiate fixField=%ld, sf[%ld:%ld] val =\"%.*s\", searching for prop_known_loc: \n",
    (long int) fixField, (long int) valStart, (long int) valEnd, valEnd-valStart, sf + valStart);
  int prop_known_loc = find_o_list(fixField, dfl->n_known_fields, dfl->ordered_known_fields); 
  vpt(2, " --  After search for prop_known_loc = %ld/%ld. \n", 
    (long int) prop_known_loc, (long int) dfl->n_known_fields);
  if ((prop_known_loc < 0) || (prop_known_loc >= dfl->n_known_fields) ||
      (dfl->ordered_known_fields[prop_known_loc] != fixField)) {
    vpt(0, " Error fixField=%ld, could not find in %ld known fiel//ds \n", (long int) fixField, 
      (long int) dfl->n_known_fields); return(-1);
  }
  vpt(2, " -- Success as fixField=%ld pkl=%ld/%ld, typ=\"%s\", fixtitle=\"%s\" for code=%ld. We think column is %ld of order_known_field=%ld\n",
    (long int) fixField, (long int) prop_known_loc, (long int) dfl->n_known_fields, 
    What_DF_DataType(dfc->fxs[prop_known_loc].typ),
    dfc->fxs[prop_known_loc].fixtitle, (long int) dfc->fxs[prop_known_loc].field_code,
    (long int) df_id->dfl->final_known_print_loc[prop_known_loc],
    (long int) df_id->dfl->ordered_known_fields[prop_known_loc]); 
  DF_DataType on_typ = dfc->fxs[prop_known_loc].typ;
  char seek_char = '\0';
  char iLocChar;  char iLC;
  DF_Fix_Field dff = dfc->fxs[prop_known_loc];
  int i_final_loc = dfl->final_known_print_loc[prop_known_loc]; 
  if (i_final_loc < 0) {
    vpt(2, " -- This column will not print, ignoring fixField=%ld with priority%ld, i_final_loc=%ld \n", 
      (long int) fixField, (long int) dfc->fxs[prop_known_loc].priority, (long int) i_final_loc);
    return(4);
  }
  vpt(2, " -- We have pulled i_final_loc, or column %ld. \n", i_final_loc);
  duckdb_vector ddbv = duckdb_data_chunk_get_vector(out_chunk, i_final_loc);
  if (verbose >= 2) {
    duckdb_logical_type ltype = duckdb_vector_get_column_type(ddbv); 
    duckdb_type dtype = duckdb_get_type_id(ltype);
    printf(" -- We got a logical type to check. \n");
    printf(" -- type for this column is type = %ld aka \"%s\" of our col %ld\n", (long int) dtype,
      WHAT_DDB_TYPE_STR(dtype), (long int) i_final_loc);
    duckdb_destroy_logical_type(&ltype);
    for (int icol = 0; icol < dfc->n_total_print_columns; icol++) {
      duckdb_vector oddbv = duckdb_data_chunk_get_vector(out_chunk, icol);
      duckdb_logical_type ltype2 = duckdb_vector_get_column_type(oddbv);
      duckdb_type dtype2 = duckdb_get_type_id(ltype2);
      printf("[col=%ld/%ld, dtype=%ld or \"%s\"] \n",
       (long int) icol, (long int) dfc->n_total_print_columns, (long int) dtype2,
       WHAT_DDB_TYPE_STR(dtype2));
      duckdb_destroy_logical_type(&ltype2);
    }
  }
  void * vddbv = (void *)duckdb_vector_get_data(ddbv);  uint64_t *ddbv_validity=NULL;
  vpt(2, " --- on fixField=%ld, sf[%ld:%ld] value is \"%.*s\", for dff->typ=\"%s\", dff.n_field_codes=%ld. pkl=%ld/%ld\n.  Code=%ld:\"%s\"\n",
      (long int) fixField, (long int) valStart, (long int) valEnd, valEnd-valStart, sf + valStart,
      What_DF_DataType(dff.typ), dff.n_field_codes, (long int) prop_known_loc, (long int) df_id->dfl->n_known_fields,
      (long int) df_id->dfc->fxs[prop_known_loc].field_code, 
      (char*) df_id->dfc->fxs[prop_known_loc].fixtitle);
  df_id->dfc->mark_visited[i_final_loc] = 1;
  if (dff.n_field_codes > 0) {
    seek_char = ((sf[valStart] == '\"') || (sf[valStart] == '\'')) ? sf[valStart + 1] : sf[valStart];
    iLocChar = LOC_CHAR(seek_char);
    if ((iLocChar >= 0) && (iLocChar < 26+26+10)) {
      vpt(3, " we have fix field %ld, with val \'%c\' which we believe is iLC=%d for \"%s\" \n",
        (long int) fixField, (char) seek_char, (int) dff.field_loc[iLocChar], 
        ((dff.field_loc[iLocChar] >= 0) && (dff.field_loc[iLocChar] < dff.n_field_codes))  ? 
         dff.field_codes_arr + dff.field_codes_loc[dff.field_loc[iLocChar]] : " ERROR_BAD_FIELD"); 
      iLC = dff.field_loc[iLocChar];
      if ((iLC >= 0) && (iLC < dff.n_field_codes)) {
        duckdb_vector_assign_string_element(ddbv, df_id->on_chunk_line, 
           dff.field_codes_arr + dff.field_codes_loc[iLC]);   // Assign String element
        vpt(3, " for field %ld, we got \'%c\':\"%s\", we have attached the string to ddbv at on_chunk_line=%ld, overall line=%ld/%ld success. \n",
          (long int) fixField, seek_char, dff.field_codes_arr + dff.field_codes_loc[iLC], df_id->on_chunk_line, df_id->on_overall_line,
          dfl->n_total_lines);
        return(2);
      } else {
        vpt(-5, " ERROR, we had a field code for field %ld, received character response \'%c\' wih LOC_CHAR=%ld for iLD=%ld/(dff.n_field_codes=%ld). \n",
          (long int) fixField, seek_char, (long int) iLocChar, (long int) iLC, (long int) dff.n_field_codes);
        printf(" -- Here are the field codes in play.\n");
        for (int ij = 0; ij < NCHARS; ij++) {
          if (dff.field_loc[ij] >= 0) {
            printf(" ---- ij=%d:\'%c\':\'%s\' \n", (int) ij,
              ij <= 10 ? '0' + ij :
              ij <= 36 ? 'a' + ij-10 :
              'A' + ij-36, dff.field_codes_arr + dff.field_codes_loc[dff.field_loc[ij]]); 
            }
        }
        printf("However seek_char=\'%c\' for LOC_CHART=%d. What do we think we did wrong? \n",
         seek_char, LOC_CHAR(seek_char));
        return(-3403053);
      }
    } else {
      vpt(0, " ERROR, iLocChar=%ld for seek_char = \'%c\' This is weird and bad. \n", (long int) iLocChar, (char) seek_char);
      return(-490320);
    } 
    return(1);
  } else {
    vpt(2, " --- We will do a fill_in_chunk for on_typ = \"%s\". prop_known_loc=%ld, print_loc=%ld/%ld.\n",  What_DF_DataType(on_typ),
      (long int) prop_known_loc, (long int) dfl->final_known_print_loc[prop_known_loc], (long int) df_id->dfc->n_total_print_columns );
    return(fill_in_chunk(on_typ, ddbv, df_id, sf, valStart, valEnd, verbose-1, dff.width, dff.scale, 
      dff.fmttyp, dfl->final_known_print_loc[prop_known_loc]));

  }
}
int fill_in_chunk(DF_DataType on_typ,  duckdb_vector ddbv, df_init_data *df_id,
   char*sf, iStr valStart, iStr valEnd, int verbose, int width, int scale, DF_TSType fmttyp, int nCol) {
  char stt[] = "    fill_in_chunk(): ";
  void * vddbv = (void *)duckdb_vector_get_data(ddbv);  uint64_t *ddbv_validity=NULL;
  int endl = valEnd-valStart;  duckdb_date ddd;
  char *end_pt; int32_t load_i32; int64_t load_i64;  int dLoc, ploc, foundscale; double flt64;
  int64_t load_o64; int64_t precision = 9;
  int vL = 0; int jj =0;
  int on_chunk_line = df_id->on_chunk_line;
  DF_config_file *dfc = df_id->dfc;
  if (sf[valStart] == '\"') { valStart++; }
  if ((sf[valEnd] == '\"') || (sf[valEnd] == '}') || (sf[valEnd] == ',') || (sf[valEnd]==' ') || (sf[valEnd]==']')) {
     vL = valEnd - valStart;
  } else {
     vL = valEnd - valStart + 1;
  }
  vpt(2, " -- nCol = %ld, Beginning with on_typ=%s, sf[%ld:%ld] = \"%.*s\" value (vL=%ld). width=%ld,scale=%ld, fmttyp=\"%s\". \n",
    (long int) nCol, What_DF_DataType(on_typ), (long int) valStart, (long int) valEnd, valEnd-valStart, sf + valStart, (long int) vL,
    (long int) width, (long int) scale, What_DF_TSType(fmttyp));
  switch (on_typ) {
    case i32 : 
      memcpy(df_id->int_scratch,  sf + valStart, vL < 10 ? vL : 10); df_id->int_scratch[vL<10?vL:10] = '\0';
      load_i32 = atoi(df_id->int_scratch);
      *(((int32_t *) vddbv) + df_id->on_chunk_line) = load_i32;
      break;
    case i64 :
      memcpy(df_id->int_scratch, sf  + valStart, vL  < 20 ? vL : 20); df_id->int_scratch[vL<20?vL:20] = '\0';
      end_pt = (df_id->int_scratch + MAXINTCHAR-1);
      errno = 0;
      load_i64 = strtoll(df_id->int_scratch, &end_pt,10); 
      if (errno != 0) {
        SETINVALID(on_chunk_line);
      } else {
        *(((int64_t *) ddbv) + df_id->on_chunk_line) = load_i64;
      }
      break;
    case f32 :
    case f64 :
      dLoc = -1;ploc = 0;
      for (jj = 0; jj < vL > 20 ? 20 : vL;jj++) { if (sf[valStart+jj] == '.') { dLoc = jj; } else { df_id->int_scratch[ploc] = sf[valStart+jj]; ploc++; }}
      if (ploc < vL-1) {
        // "   10.003, "; st= 4, dLoc =6, end=9.  (end+1 = 10);  ploc=5. 9-4=5
        vpt(0, "Error trying to record a decimal of %.*s \n", vL > 20 ? 20 : vL, sf+valStart); return(-1);
      } 
      foundscale = dLoc < 0 ? 0 : vL - dLoc;
      df_id->int_scratch[ploc] = '\0'; 
      end_pt = df_id->int_scratch + ploc;  errno = 0;
      load_i64 = strtoll(df_id->int_scratch, &end_pt, 10);
      if (foundscale > 0) {
        flt64 = (double) 1.0;
        for (jj = 0; jj < foundscale; jj++) { flt64 *= .1; }
        flt64 = flt64 * ((double) load_i64);
      } 
      if (errno != 0) {
        SETINVALID(df_id->on_chunk_line);
      } else {
         if (on_typ == f32) {
           *(((float *) vddbv) +  df_id->on_chunk_line) = (float) flt64;
         } else {
           *(((double *) vddbv) +  df_id->on_chunk_line) = (double) flt64;
         }
      }
      break;
    case decimal185 :
    case decimal184 :
    case decimal154 :
    case decimal153 :
    case decimal_gen :
      dLoc = -1; ploc = 0;
      if (vL > 20) { 
       vpt(0, "ISSUE we think a field has a length of %ld and that will be hard for decimal format. \n", (long int) vL); 
      }
      vpt(2, " -- Beginning to copy from sf[valStart=%ld:valStart+vL=%ld] = \'%.*s\', vL=%ld. \n",
        (long int) valStart, (long int) valStart + vL, vL, sf + valStart, (long int) vL);
      for (jj = 0; jj < ((vL > 20) ? 20 : vL);jj++) { 
        if (sf[valStart+jj] == '.') { 
          dLoc = jj; 
        } else { 
          df_id->int_scratch[ploc] = sf[valStart+jj]; ploc++; 
        }
      }
      if (ploc < endl-1) {
        // "   10.003, "; st= 4, dLoc =6, end=9.  (end+1 = 10);  ploc=5. 9-4=5
        vpt(0, "Error trying to record a decimal of %.*s \n", vL, sf+valStart); return(-1);
      } 
      vpt(2, " -- Note before we read decimal, dLoc=%ld, but text is %s. \n", dLoc, df_id->int_scratch);
      foundscale = dLoc < 0 ? 0 : vL - dLoc;
      df_id->int_scratch[ploc] = '\0'; 
      end_pt = df_id->int_scratch + ploc;  errno = 0;
      load_i64 = strtoll(df_id->int_scratch, &end_pt, 10);
      if (foundscale > scale) {
        for (jj = 0; jj < foundscale - scale; jj++) { load_i64 = (int64_t) load_i64/ 10; }
      } else if (foundscale < scale) {
        for (jj = 0; jj < scale- foundscale;jj++) { load_i64 = (int64_t) load_i64*10; }
      }
      if (errno != 0) {
        SETINVALID(df_id->on_chunk_line);
      } else {
         *(((int64_t *) vddbv) +  on_chunk_line) = load_i64;
      }
      vpt(2, " Note, Decimal(%ld,%ld) working buffer[%ld:%ld] = %.*s, with st,end=[%ld,%ld] we calculated dLoc=%ld, foundscale=%ld, load_i64=%ld. \n",
        (long int) width, (long int) scale, 
        (long int) valStart-2, (long int) valEnd+2, valEnd-valStart+4, sf+valStart-2, (long int) valStart, (long int) valEnd, (long int) dLoc,
        (long int) foundscale, (long int) load_i64);
      break;
    case str :
      vpt(3, " -- we have vL=%ld, assigning a string element to ddbv for on_chunk_line=%ld, value sf[%ld:%ld] = \"%.*s\". \n",
        (long int) vL,  (long int) on_chunk_line, (long int) valStart, (long int) valStart + vL, vL, sf + valStart);
      duckdb_vector_assign_string_element_len(ddbv, on_chunk_line, sf +valStart, vL);  
      break;
    case enum_date:
      memcpy(df_id->int_scratch, sf+valStart,4); df_id->int_scratch[4] = '\0';
      df_id->dds.year = atoi(df_id->int_scratch);
      memcpy(df_id->int_scratch, sf+valStart+5,2); df_id->int_scratch[2] = '\0';
      df_id->dds.month = atoi(df_id->int_scratch);
      memcpy(df_id->int_scratch, sf+valStart+8,2); df_id->int_scratch[2] = '\0';
      df_id->dds.day = atoi(df_id->int_scratch);
      ddd = duckdb_to_date(df_id->dds);
      *(((int64_t *)vddbv) + on_chunk_line) = ddd.days;
      break;
    case tms :
    case tus :
    case tns :
      switch (fmttyp) {
        case YYYYcmmcddtHHcMMcSScF :
          memcpy(df_id->int_scratch, sf+valStart,4); df_id->int_scratch[4] = '\0';
          df_id->dds.year = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+5,2); df_id->int_scratch[2] = '\0';
          df_id->dds.month = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+8,2); df_id->int_scratch[2] = '\0';
          df_id->dds.day = atoi(df_id->int_scratch);
          ddd = duckdb_to_date(df_id->dds);
          memcpy(df_id->int_scratch, sf+valStart+11,2); df_id->int_scratch[2] = '\0';
          load_i64 = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+14,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+17,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+20,vL-20); df_id->int_scratch[vL-20] = '\0';
          load_o64 = atoi(df_id->int_scratch);
          precision = (on_typ==tns) ? 9 : (on_typ==tus) ? 6 : (on_typ==tms) ? 3 : 3;
          if (vL-20 < precision) {
            for (jj = 0; jj < precision-(vL-20); jj++) { load_o64 *=10; }
          }
          precision = (on_typ==tns) ? 1000000000 : (on_typ==tus) ? 1000000 : 1000;
          load_i64 = ddd.days * ((int64_t) 24*60*60) * ((int64_t) precision)  + load_i64*((int64_t)precision) + load_o64;
          *(((int64_t *)vddbv) + on_chunk_line) = load_i64;
          break;
        case MonthcDaycYearcHHcMMcSScF :
        case MonthcDaycYear   :
          jj = 0;  if (sf[jj] == '\"') { jj++; }
          for(; jj < vL; jj++) {
            if ((sf[valStart+jj] == ' ') || (sf[valStart + jj] == '\t')) {
              df_id->int_scratch[jj] = '\0'; break;
            } else if ( 
               ((sf[valStart+jj] >= 'A') && (sf[valStart+jj] <= 'Z')) ||
               ((sf[valStart+jj] >= 'a') && (sf[valStart+jj] <= 'z')) ||
               ((sf[valStart+jj] >= '0') && (sf[valStart+jj] <= '9')) ) {
              df_id->int_scratch[jj] = sf[valStart + jj];
            } else {
              df_id->int_scratch[jj] = '\0'; break;
            }
          }
          df_id->dds.month = MATCHMONTH(df_id->int_scratch, 0, jj);
          jj++;  int oj = jj; 
          for(;jj<vL;jj++) {
            if ((sf[valStart+jj] == ',') || (sf[valStart+jj] == ' ')) { 
              df_id->int_scratch[jj-oj] = '\0'; break; 
            } else if ((sf[valStart+jj] >= '0') && (sf[valStart+jj] <= '9')) { 
              df_id->int_scratch[jj-oj] = sf[valStart+jj]; 
              if (jj-oj >= 3) { vpt(-1, "ERROR MonthcDaycYearcHHcMMcSScF, we did not like day. \"%.*s\".\n",
                vL, sf + valStart); return(-1); }
            } else {
              df_id->int_scratch[jj-oj] = '\0'; break;
            }
          }
          df_id->dds.day = atoi(df_id->int_scratch);
          for (;jj<vL;jj++) {
            if (sf[valStart +jj] == ' ') { } else { break; } 
          }
          oj = jj;
          for (;jj<vL;jj++) {
            if ((sf[valStart + jj] == 'T') || (sf[valStart+jj] == ' ') || (sf[valStart+jj] == '\0')) {
               df_id->int_scratch[jj-oj] = '\0';  break;
            } else if ((sf[valStart+jj] >= '0') && (sf[valStart+jj] <= '9')) {
               df_id->int_scratch[jj-oj] = sf[valStart+jj];
              if (jj-oj >= 5) { vpt(-1, "ERROR MonthcDaycYearcHHcMMcSScF, we did not like year. \"%.*s\".\n",
                vL, sf + valStart); return(-1); }
            }
          }
          df_id->dds.year = atoi(df_id->int_scratch);
          ddd = duckdb_to_date(df_id->dds);
          if (fmttyp == MonthcDaycYear) { 
             *(((int32_t *)vddbv) + on_chunk_line) = ddd.days;
             break; 
          }
          for (;jj < vL;jj++) { if (sf[valStart] != ' ') { break; } }
          memcpy(df_id->int_scratch, sf+valStart+jj,2); df_id->int_scratch[2] = '\0';
          load_i64 = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+jj+3,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+jj+6,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+jj+9,vL-(jj+9)); df_id->int_scratch[vL-(jj+9)] = '\0';
          load_o64 = atoi(df_id->int_scratch);
          precision = (on_typ==tns) ? 9 : (on_typ==tus) ? 6 : (on_typ==tms) ? 3 : 3;
          if (vL-20 < precision) {
            for (jj = 0; jj < precision-(vL-20); jj++) { load_o64 *=10; }
          }
          precision = (on_typ==tns) ? 1000000000 : (on_typ==tus) ? 1000000 : 1000;
          load_i64 = ddd.days * ((int64_t) 24*60*60) * ((int64_t) precision)  + load_i64*((int64_t)precision) + load_o64;
          *(((int64_t *)vddbv) + on_chunk_line) = load_i64;
          break;
        case YYYYmmddHHMMSSF :
          memcpy(df_id->int_scratch, sf+valStart,4); df_id->int_scratch[4] = '\0';
          df_id->dds.year = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+4,2); df_id->int_scratch[2] = '\0';
          df_id->dds.month = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+6,2); df_id->int_scratch[2] = '\0';
          df_id->dds.day = atoi(df_id->int_scratch);
          ddd = duckdb_to_date(df_id->dds);
          memcpy(df_id->int_scratch, sf+valStart+8,2); df_id->int_scratch[2] = '\0';
          load_i64 = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+10,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+12,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+14,vL-14); df_id->int_scratch[vL-14] = '\0';
          load_o64 = atoi(df_id->int_scratch);
          precision = (on_typ==tns) ? 9 : (on_typ==tus) ? 6 : (on_typ==tms) ? 3 : 3;
          if (vL-20 < precision) {
            for (jj = 0; jj < precision-(vL-14); jj++) { load_o64 *=10; }
          }
          precision = (on_typ==tns) ? 1000000000 : (on_typ==tus) ? 1000000 : 1000;
          load_i64 = ddd.days * ((int64_t) 24*60*60) * ((int64_t) precision)  + load_i64*((int64_t)precision) + load_o64;
          *(((int64_t *)vddbv) + on_chunk_line) = load_i64;
          break;
        case HHcMMcSScF :
          df_id->dds.year = 2025;
          df_id->dds.month = 9;
          df_id->dds.day = 8; 
          ddd = duckdb_to_date(df_id->dds);
          memcpy(df_id->int_scratch, sf+valStart+0,2); df_id->int_scratch[2] = '\0';
          load_i64 = atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+3,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+6,2); df_id->int_scratch[2] = '\0';
          load_i64 = load_i64*60 + atoi(df_id->int_scratch);
          memcpy(df_id->int_scratch, sf+valStart+9,vL-9); df_id->int_scratch[vL-9] = '\0';
          load_o64 = atoi(df_id->int_scratch);
          precision = (on_typ==tns) ? 9 : (on_typ==tus) ? 6 : (on_typ==tms) ? 3 : 3;
          if (vL-20 < precision) {
            for (jj = 0; jj < precision-(vL-9); jj++) { load_o64 *=10; }
          }
          precision = (on_typ==tns) ? 1000000000 : (on_typ==tus) ? 1000000 : 1000;
          load_i64 = ddd.days * ((int64_t) 24*60*60) * ((int64_t) precision)  + load_i64*((int64_t)precision) + load_o64;
          *(((int64_t *)vddbv) + on_chunk_line) = load_i64;
          break;
        default :
          vpt(-1, " ERROR %s TIME type is not implemented yet \n", What_DF_TSType(fmttyp));
      }
      break;
    default :
     vpt(1, " DEFAULT STATEMENT: We have not yet implemented type \"%s\", for column %ld/%ld on_chunk_line=%ld/%ld, total_line=%ld/%ld \n",
       What_DF_DataType(on_typ), (long int) nCol, df_id->dfc->n_total_print_columns, (long int) on_chunk_line, (long int) df_id->dfl->standard_vector_size,
       (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
  }
  return(1);
}
int add_line_to_chunk(duckdb_data_chunk out_chunk, 
  duckdb_function_info df_info, df_init_data *df_id, int verbose) {
  DF_field_list *dfl = df_id->dfl;
  #ifdef DDBUG
  char stt[500];
  sprintf(stt, " df_main.c->add_line_to_chunk(v=%d,chunk_ln=%d/%d,on_ln=%ld/%ld): ", 
    (int) verbose, (int) df_id->on_chunk_line, (int) dfl->standard_vector_size, 
    (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
  #endif
  #ifndef DDBUG
  char stt[] = " df_main.c->add_line_to_chunk(): ";
  #endif
  vpt(2, " Initialize: \n");
  if (verbose >= 3) {
    printf(" ---- \"%.*s\"\n",
      df_id->iLineEnd - df_id->onstr, df_id->buffer + df_id->onstr);
  }
  int ii = df_id->onstr;
  int attempt;
  iStr fieldStart; iStr fieldEnd;  int n_total_added_fields = 0;
  DF_config_file *dfc = df_id->dfc;
  char *sf = df_id->buffer;  iStr end_ln = df_id->iLineEnd;
  for (int i_pt = 0; i_pt < df_id->dfc->n_total_print_columns; i_pt++) {
    df_id->dfc->mark_visited[i_pt] = 0;
  }
  for (df_id->ion_schema = 0; df_id->ion_schema < dfc->n_schemas; df_id->ion_schema++) {
    fieldStart = ii; NEXTCHARSEP(); fieldEnd = ii;
    if ((fieldEnd-1 > fieldStart) && (sf[fieldEnd-1] == df_id->dfl->char_sep)) { fieldEnd--; }
    while((fieldEnd-1 > fieldStart) && ((sf[fieldEnd-1] == ' ') || (sf[fieldEnd-1] == '\t') || (sf[fieldEnd-1] == '\n'))) { fieldEnd--; } 
    while((fieldStart < fieldEnd-1) && ((sf[fieldStart] == ' ') || (sf[fieldStart] == '\t'))) { fieldStart++; }
    if (dfc->schemas[df_id->ion_schema].typ != fix42) {
       if ((sf[fieldStart] == '\"') && (sf[fieldEnd-1] == '\"')) {
         fieldStart++; fieldEnd--;
       }
       if (dfc->schemas[df_id->ion_schema].final_loc < 0) {
         vpt(2, "We are on ion_schmea = %ld, however final_loc=%ld so no print. \n", (long int) df_id->ion_schema,
           dfc->schemas[df_id->ion_schema].final_loc);
       } else {
         attempt = add_schema_entry_to_chunk(df_id, df_id->buffer, fieldStart, fieldEnd, out_chunk, verbose-1);
         if (attempt < 0) {
           //vpt(-1, " ERROR received %ld from last attempt to add schema entry to chunk. Trying again \n", (long int) attempt);
           //attempt = add_schema_entry_to_chunk(df_id, ion_schema, df_id->buffer, fieldStart, fieldEnd, df_id->on_chunk_line, out_chunk, verbose + 5);
           vpt(-1, "  We had received %ld from attempt to add schema entry to chunk\n", (long int) attempt);
           duckdb_function_set_error(df_info, "Attempted to Add Schema entry but failed. \n"); return(-1);
         }
       }
    } else {
       if ((sf[fieldStart] != '{') || (sf[fieldEnd-1] != '}')) {
         vpt(0, "ERROR (fix42 version) (ion_schema=%ld/%ld) on fieldStart = %ld/%ld, we had sf[%ld:%ld]=\"%.*s\" with "
            "sf[fieldStart=%ld]=\'%c\', sf[fieldEnd-1=%ld]=\'%c\'\n",  (long int) df_id->ion_schema, (long int) dfc->n_schemas,
            (long int) df_id->ion_schema, (long int) df_id->dfc->n_schemas,
            (long int) fieldStart, (long int) fieldEnd, fieldEnd-fieldStart, df_id->buffer + fieldStart,
            (long int) fieldStart, sf[fieldStart], (long int) fieldEnd-1, sf[fieldEnd-1]);
         printf(" --- ERROR note we want to be able to compare fields to \'{\' and \'}\' \n");
         return(-1014532);
       }
       //int add_fixfields_entries_to_chunk(df_init_data *df_id, char *sf, iStr fixfieldsStart, iStr fixfieldsEnd, 
       //int on_chunk_line, duckdb_data_chunk out_chunk, int verbose);
       attempt = add_fixfields_entries_to_chunk(df_id, df_id->buffer, fieldStart, fieldEnd, out_chunk, verbose-1);
       if (attempt < 0) {
         //vpt(-1, " ERROR received %ld from last attempt to add fix field entry to chunk. Trying again \n", (long int) attempt);
         //attempt = add_fixfields_entries_to_chunk(df_id, df_id->buffer, fieldStart, fieldEnd, df_id->on_chunk_line,out_chunk, verbose+5);
         vpt(-1, "  We had received %ld from attempt to add fix_field entry to chunk and tried again \n", (long int) attempt);
         duckdb_function_set_error(df_info, "Attempted to Add Fix Field entry but failed. \n"); return(-1);
       } else {
         vpt(2, " after add_fixfields_entries_to_chunk we get attempt=%ld for on_chunk_line=%ld. or line %ld/%ld \n",
           (long int) attempt, (long int) df_id->on_chunk_line, (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
       }
      //NEXTCOMMA();  
    }
    n_total_added_fields += attempt;
  }
  int on_pt;
  for (on_pt = 0; on_pt < df_id->dfc->n_total_print_columns; on_pt++) {
    if (df_id->dfc->mark_visited[on_pt] == 0) {
       duckdb_vector ddbv = duckdb_data_chunk_get_vector(out_chunk, on_pt);
       int64_t * ddbv_validity = NULL;
       SETINVALID(df_id->on_chunk_line);
    }
  }
  vpt(1, " Completed the whole line added %ld fields.\n", (long int) n_total_added_fields);
  return(1);
}
void duckfix_main_table_function(duckdb_function_info df_info, duckdb_data_chunk out_chunk) {
  df_init_data *df_id = (df_init_data *)duckdb_function_get_init_data((duckdb_function_info) df_info);
  //df_bind_data *df_bd = (df_bind_data *)duckdb_function_get_bind_data((duckdb_function_info) df_info);
  int32_t verbose = df_id->verbose;
  #ifdef DDBUG
  char stt[500];
  sprintf(stt, "M df_main.c->duckfix_main_table_function(v=%d,onc=%d,st_ln=%d/%d,tbytes=%ld/%ld): \n",
    (int) df_id->verbose, (int) df_id->on_chunk, (int) df_id->on_overall_line, (int) df_id->dfl->n_total_lines,
    (long int) df_id->tbytesread, (long int) df_id->dfl->file_total_bytes);
  #endif
  #ifndef DDBUG
  char stt[] = "df_main.c->duckfix_main_table_function(): ";
  #endif
  if (verbose >= 1) {
    printf("MMMMMMfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfmdfm\n");
    printf("%s -- Initiate with df_id->on_chunk=%ld/%ld for total lines %ld.\n", (char*) stt, 
       (long int) df_id->on_chunk,  (long int) df_id->dfl->n_loc_lines, (long int) df_id->dfl->n_total_lines); 
  }
  DF_field_list *dfl = df_id->dfl;
  iStr iLineEnd;
  int on_chunk_line = 0; df_id->on_chunk_line = 0;
  if (df_id->on_overall_line >= df_id->dfl->n_total_lines) {
    vpt(1, " --- Last loop we have on_overall_line=%ld=total=%ld.  We are on chunk %ld/%ld\n",
       (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines,
       (long int) df_id->on_chunk, (long int) df_id->dfl->n_loc_lines);
    duckdb_data_chunk_set_size(out_chunk, 0);
    return;
  }
  int add_line_error;
  char error_text[500];
  while ((df_id->tbytesread < dfl->file_total_bytes) && (df_id->bytesread > 0)) {
    if (verbose >= 3) { printf("\nMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM\n"); }
    vpt(1, " on ibuffreads=%ld  bytesread=%ld, remainder=%ld, tbytesread=%ld. \n",
      (long int) df_id->buffreads, (long int) df_id->bytesread, (long int) df_id->remainder, (long int) df_id->tbytesread);
    vpt(3, " here is the complete buffer so far: \n%.*s\nLet's begin...\n", (long int) df_id->bytesread, df_id->buffer);
    while (df_id->onstr < df_id->bytesread) {
      vpt(3, " on reading_line=%ld, onstr=%ld/%ld, tbytesread=%ld, \"%.*s...\"\n",
        (long int) df_id->on_overall_line, (long int) df_id->onstr, (long int) df_id->bytesread,  (long int) df_id->tbytesread, 30, df_id->buffer + df_id->onstr);
      while ((df_id->onstr < df_id->bytesread) && (df_id->buffer[df_id->onstr] == '\n')) {df_id->onstr++; }
      df_id->iLineEnd = get_next_newln(df_id->buffer, df_id->onstr, df_id->bytesread, verbose-2); 
      if ((df_id->iLineEnd < 0) || (df_id->iLineEnd >= df_id->bytesread)) {
        vpt(-1, " ISSUE -- we received a newline at %ld, when we started from sf[%ld:%ld] = \"%.*s\", tbytesread=%ld/%ld, overall line at %ld/%ld\n",
          (long int) df_id->iLineEnd, df_id->onstr, 
          df_id->bytesread - df_id->onstr < 200 ? df_id->bytesread : 200 + df_id->onstr,
          df_id->bytesread - df_id->onstr < 200 ? df_id->bytesread-df_id->onstr : 200,
          df_id->buffer + df_id->onstr, df_id->tbytesread, df_id->dfl->file_total_bytes,
          (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
      }
      if (df_id->iLineEnd >= df_id->bytesread) { break; }
      if (df_id->buffer[df_id->iLineEnd] != '\n') { printf("duckfix_main_table_function, iLineEnd=%ld, but buffer[%ld]=\'%c\' ? \n",
                                      (long int) df_id->iLineEnd, (long int) df_id->iLineEnd, df_id->buffer[df_id->iLineEnd]);  
         duckdb_function_set_error(df_info, "Failed to reach an end line or end of buffer"); return;
      }
      add_line_error = add_line_to_chunk(out_chunk, df_info, df_id, verbose-2);
      if (add_line_error < 0) {
        vpt(-10,"Error on update with [onstr,iLineEnd]=[%ld,%ld] for \n%.*s\nWhat went wrong?\n",
         (long int) df_id->onstr, (long int) df_id->iLineEnd, (df_id->iLineEnd)-(df_id->onstr) +1, df_id->buffer+df_id->onstr);
        sprintf(error_text, "Received add_line error = %ld. \n", (long int) add_line_error);
        duckdb_function_set_error(df_info, error_text); 
        duckdb_data_chunk_set_size(out_chunk,df_id->on_chunk+1);
        return;
      }
      vpt(2, " -- completed line %ld/%ld, or %ld/%ld, onstr=%ld, iLineEnd=%ld. \n",
        (long int) df_id->on_chunk_line, (long int) dfl->standard_vector_size,
        (long int) df_id->on_overall_line, (long int) dfl->n_total_lines, (long int) df_id->onstr,
        (long int) df_id->iLineEnd);
      if (verbose >= 2) {
        printf("MMM     ENDL (onstr=%ld, iLineEnd=%ld) -------------------------------------------------------------------\n",
          (long int) df_id->onstr, (long int) df_id->iLineEnd);
      }
      df_id->onstr = df_id->iLineEnd+1;  df_id->on_overall_line++;  on_chunk_line++;  df_id->on_chunk_line = on_chunk_line;
      if (df_id->on_chunk_line >= dfl->standard_vector_size) {
        vpt(2, "On Chunk %d/%d,  on_chunk_line=%ld/%ld, Reached vector max read position with bytesread=%ld, tbytesread=%ld",
          (int) df_id->on_chunk, (int) dfl->n_loc_lines, 
          (long int) df_id->on_chunk_line, (long int) dfl->standard_vector_size, 
          (long int) df_id->bytesread, (long int) df_id->tbytesread);
        if (verbose >= 2) {
          // Adding more text at end of above.
          printf(", remainder=%ld, onstr=%ld. df_id->on_overall_line=%ld/%ld. \n", (long int) df_id->remainder, 
            (long int) df_id->onstr, (long int) df_id->on_overall_line, (long int) dfl->n_total_lines);
        }
        duckdb_data_chunk_set_size(out_chunk, dfl->standard_vector_size);
        df_id->on_chunk++;
        return;
      } else if (df_id->on_overall_line >= df_id->dfl->n_total_lines) {
        duckdb_data_chunk_set_size(out_chunk, df_id->on_chunk_line);
        df_id->on_chunk++;
        return;
      }
    }
    vpt(1, "Buffer break, iLineEnd = %ld versus onstr=%ld. tbytesread=%ld/%ld for online=%ld/%ld. \n",
      (long int) df_id->iLineEnd, (long int) df_id->onstr, 
      (long int) df_id->tbytesread, (long int) df_id->dfl->file_total_bytes, 
      (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
    if (df_id->onstr < df_id->bytesread) {
       df_id->remainder = (df_id->bytesread-df_id->onstr);
       memcpy(df_id->buffer, df_id->buffer + df_id->onstr, sizeof(char)*df_id->remainder);
    }
    df_id->tbytesread += df_id->bytesread-df_id->remainder;
    vpt(1, "Buffer break, now trying to read %ld bytes. \n", MAXREAD-df_id->remainder);
    int new_bytes = fread(df_id->buffer+df_id->remainder,sizeof(char),MAXREAD-df_id->remainder,df_id->fpo);  df_id->onstr = 0;
    if ((new_bytes <= 0) || (ferror(df_id->fpo))) {
      vpt(-100, " We have an Error trying to reread the file. \n");
      sprintf(error_text, "ERROR on Buffer break, stt=\"%s\", iLineEnd = %ld versus onstr=%ld. tbytesread=%ld/%ld for online=%ld/%ld. \n",
      (char*) stt, (long int) df_id->iLineEnd, (long int) df_id->onstr, 
      (long int) df_id->tbytesread, (long int) df_id->dfl->file_total_bytes, 
      (long int) df_id->on_overall_line, (long int) df_id->dfl->n_total_lines);
      duckdb_function_set_error(df_info, error_text); 
      duckdb_data_chunk_set_size(out_chunk,df_id->on_chunk+1);
      return;
    }
    df_id->bytesread = new_bytes + df_id->remainder;
  }
  vpt(1, "On Chunk=%d/%d:  Reached entire end of duckfix_main_table loop with bytesread=%ld, tbytesread=%ld, ",
    (int) df_id->on_chunk, (int) dfl->n_loc_lines, (long int) df_id->bytesread, (long int) df_id->tbytesread);
  if (verbose >= 1) {
    printf("remainder=%ld, tbytesread=%ld/%ld, onstr=%ld. df_id->on_overall_line=%ld/%ld. \n",
    (long int) df_id->remainder, (long int) df_id->tbytesread, df_id->dfl->file_total_bytes,
    (long int) df_id->onstr, (long int) df_id->on_overall_line, (long int) dfl->n_total_lines);
  }
  // If we exit loop, we have read the whole file and likely no more lines to read
  if (on_chunk_line >= dfl->standard_vector_size) {
    duckdb_data_chunk_set_size(out_chunk, dfl->standard_vector_size);
  } else {
    vpt(1, "At end of loop we have on_chunk_line=%ld/%ld and bytesread=%ld. \n",  (long int) on_chunk_line, 
      (long int) dfl->standard_vector_size, (long int) df_id->bytesread);
    duckdb_data_chunk_set_size(out_chunk, on_chunk_line);
  }
  df_id->on_chunk++;
}
