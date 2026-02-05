//////////////////////////////////////////////////////////////////////////////////
// read_file.c
// 2026-02-04 - Alan Lenarcic
// License GPLv2
//
//
// Initial read of the CSV file forms a strategy for turning the data into a DuckDB table.
// Note that column order must be chosen, and all of the fix fields used must be identified.

#ifndef DUCKFIXLOADH
#include "include/df_load.h"
#define DUCKFIXLOADH 0
#endif

#ifndef DUCKFIX_READ_FILEH
#include "include/read_file.h"
#define DUCKFIX_READ_FILEH
#endif



#ifndef DFFREE 
#define DFFREE( x, cx ) \
  if ( (x) != NULL) { vpt(1, "Delete %s.\n", (cx)); free((x));  x = NULL; }

#define ALLOC_INT_ME_SIZE( mykey, sz, cs ) \
  vpt(2, " allocate %s. \n", cs);  \
  mykey = (int *) malloc(sizeof(int) * ((int) sz) ); \
  if (  (mykey) == NULL) { vpt(0, " error allocating %s. \n", cs);  delete_field_list(&dfl,2);  return(NULL); } \
  mykey[0] = 0
#endif

DF_field_list *create_blank_field_list(DF_config_file *dfc, int verbose) {
  char stt[]="create_blank_field_list(): ";
  int nfields = dfc->nfields;
  DF_field_list *dfl = (DF_field_list*) malloc(1.0*sizeof(DF_field_list));
  if (dfl == NULL) { return(NULL); }
  dfl->n_known_fields = nfields;
  dfl->num_used_known_fields = 0;
  dfl->ordered_known_fields = NULL;  
  dfl->known_usage_count = NULL;
  dfl->alloc_unknown = 0;  dfl->num_unknown = 0;
  dfl->ordered_unknown_fields = NULL;
  dfl->unknown_usage_count=NULL;
  dfl->line_unknown = NULL;
  dfl->final_known_print_loc = NULL;
  dfl->line_locs = NULL;  dfl->n_loc_lines = 0;  dfl->alloc_line_loc = 0;
  dfl->finish = 0; // Send to 1 if success
  ALLOC_INT_ME_SIZE( (dfl->ordered_known_fields) , nfields, "ordered_known_fields");
  ALLOC_INT_ME_SIZE( (dfl->known_usage_count) , nfields, "ordered_unknown_fields");
  ALLOC_INT_ME_SIZE( (dfl->ordered_unknown_fields) , nfields, "ordered_unknown_fields");

  ALLOC_INT_ME_SIZE( (dfl->final_known_print_loc) , nfields, "final_known_print_loc");

  ALLOC_INT_ME_SIZE( (dfl->line_unknown) , nfields, "ordered_unknown_fields");
  ALLOC_INT_ME_SIZE( (dfl->unknown_usage_count) , nfields, "ordered_unknown_fields");

  dfl->alloc_line_loc = nfields < 50 ? 50 : nfields;
  ALLOC_INT_ME_SIZE( (dfl->line_locs), dfl->alloc_line_loc, "line_locs"); 
  dfl->alloc_unknown = nfields;
  for (int ii = 0; ii < nfields; ii++) {
    dfl->ordered_known_fields[ii] = dfc->ordered_fields[ii];
    dfl->known_usage_count[ii] = 0; dfl->final_known_print_loc[ii] = -1;
  }
  for (int ii = 0; ii < nfields; ii++) {
    dfl->ordered_unknown_fields[ii] = -1; dfl->unknown_usage_count[ii] = -1;  dfl->line_unknown[ii] =-1;
  }
  for (int ii = 0; ii < dfl->alloc_line_loc; ii++) {
    dfl->line_locs[ii] = -1;
  }
  return(dfl);
}

int delete_field_list(DF_field_list **p_dfl, int verbose) {
  char stt[300];
  verbose = verbose -1;
  if (p_dfl[0] == NULL) { return(-1); }
  DF_field_list *dfl = p_dfl[0];
  sprintf(stt, "delete_field_list(v=%d,k=%ld,u=%ld/%ld): ",
    (int) verbose, (long int) dfl->n_known_fields, (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);
   
  DFFREE(dfl->final_known_print_loc, "final_known_print_loc"); 
  DFFREE(dfl->ordered_known_fields, "ordered_known_fields");
  DFFREE(dfl->ordered_unknown_fields, "ordered_unknown_fields");
  DFFREE(dfl->known_usage_count, "known_usage_count");
  DFFREE(dfl->unknown_usage_count, "unknown_usage_count");
  DFFREE(dfl->line_unknown, "line_unknown");
  DFFREE(dfl->line_locs, "line_locs");
  return(1);
}
int PRINT_dfl(DF_field_list *dfl) {
  printf("Printing DF_Field_list(finish=%ld: file_total_bytes=%ld): dfl with %ld known fields, %ld/%ld unknown \n",
     (long int) dfl->finish, (long int) dfl->file_total_bytes, (long int) dfl->n_known_fields, (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);
  printf("KNOWN[%ld, %ld used] --  {\n", dfl->n_known_fields, dfl->num_used_known_fields);
  printf("     ");  int n_used_known = 0;
  for (int ii = 0; ii < dfl->n_known_fields; ii++) {
    if (dfl->known_usage_count[ii] > 0) { n_used_known++; }
    printf("[%ld:%ld]", (long int) dfl->ordered_known_fields[ii], (long int) dfl->known_usage_count[ii]);
    if (ii < dfl->n_known_fields - 1) { printf(",");
      if ((ii+1) % 6 == 0) { printf("\n     "); }
    }
  }
  printf("\n   }; [%ld/%ld are populated].\n", (long int) n_used_known, (long int) dfl->n_known_fields);
  if (dfl->num_unknown == 0) {
    printf("  NO KNOWN UNKNOWN. \n"); return(0);
  } 
  printf("UNKNOWN[%ld/%ld] -- {\n", (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);
  printf("    ");
  for (int ii = 0; ii < dfl->num_unknown; ii++) {
    printf("[%ld:%ld:%ld]", (long int) dfl->ordered_unknown_fields[ii], (long int) dfl->unknown_usage_count[ii],
      (long int) dfl->line_unknown[ii]);
    if (ii < dfl->num_unknown- 1) { printf(",");
      if ((ii+1) % 6 == 0) { printf("\n     "); }
    }
  }
  printf("  }; \n");
  printf("LINE_BREAKS[%ld/%ld] -- {\n", (long int) dfl->n_loc_lines, (long int) dfl->alloc_line_loc);
  printf("    ");
  for (int ii = 0; ii < dfl->n_loc_lines; ii++) {
    printf("%ld", dfl->line_locs[ii]); 
    if (ii < dfl->n_loc_lines-1) { printf(",");
      if ((ii+1) % 20 == 0) { printf("\n     "); }
    }
  }
  printf("\n] -- With Last element [%ld] = %ld \n",
    (long int) dfl->n_loc_lines, dfl->line_locs[dfl->n_loc_lines]);
  return(5);
}
int find_o_list(int num, int nlist, int *olist) {
  if (nlist <= 0) { return(0); }
  if (num < 0) {
    printf("find_o_list: error, you supplied num=%ld. \n", num); return(-9);
  }
  //if (num == 5) {
  //  printf("\n\n\n Hey we are looking for 5 in olist! \n\n\n");
  //}
  if (num <= olist[0]) { return(0); }
  if (nlist == 1) { return(1); }
  if (num == olist[nlist-1]) { return(nlist-1); }
  if (num > olist[nlist-1]) { return(nlist); }
  if ((nlist >= 2) && (num > olist[nlist-2])) { return(nlist-1); }
  int iL =0; int iR =nlist-1;  int iM;
  int itick = 0;
  while (iL +1 < iR) {
    iM = (int) (iL + iR) / 2;  iM = iM<=iL ? iL+1 : iM; 
    if (olist[iM] == num) { return(iM); }
    if (olist[iM] < num) { 
      //if (olist[iM+1]==num) { return(iM+1); } else if (olist[iM+1] >num) { return(iM+1); } else { iL=iM+1; }
      iL = iM;
    } else {
      iR = iM;
    }
    itick++;
    if (itick >= nlist) {
      printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
      printf("EEE itick = %ld, nlist=%ld. we still have iL,iM,iR=[%d,%d,%d] for findings [%d,%d,%d]. \n",
        (long int) itick, (long int) nlist, iL, iM, iR, olist[iL], olist[iM], olist[iR]); 
      printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEe\n");
      printf("EEEEEEEEEEEEEEEEEEEEE  Bad Loop ! \n");
      return(-1);
    }
  } 
  if (olist[iL] == num) { return(iL); }
  if (olist[iR] == num) { return(iR); }
  printf("\n\n Note find_o_list failed to find itick=%ld/%ld and iL=%ld,iM=%ld,iR=%ld, for [%d,%d,%d]  Lets try another approach\n", (long int) itick, (long int) nlist,
    (long int) iL, (long int) iM, (long int) iR, olist[iL], olist[iM], olist[iR]);
  for (iR = 0; iR < nlist; iR++) {
    if (olist[iR] == num) {
       printf("  --- Woah find_o_list is broke, we found oList[%ld/%ld]=%ld, which is a win!\n", (long int) iR, (long int) nlist, (long int) num);
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("\n\n\n");
      return(-iR);
    }
  }
  printf(" I guess fine, never found %ld in the list length %ld. \n", (long int) num, (long int) nlist);
  return(iR);
}
int add_to_field_list(DF_field_list *dfl, int num, int iline, int verbose) {
   char stt[500];
   if (num < 0) {
     printf("add_to_field_list:: Fix fields are not negative, num=%ld. \n", (long int) num); return(-10);
   }
   sprintf(stt, "add_field_to_list[num=%ld,line=%ld,v=%ld]: ",
     (long int) num, (long int) iline, (long int) verbose);
   int prop_known_loc = find_o_list(num, dfl->n_known_fields, dfl->ordered_known_fields);
   if (prop_known_loc < 0) {
     vpt(0, " ERROR, prop_known_loc returned %ld.  Must be a problem with algo on search dfl->ordered_known_fields\n", (long int) prop_known_loc);
     printf("[");
     for (int ii = 0; ii < dfl->n_known_fields;ii++) {
       printf("%d", dfl->ordered_known_fields[ii]); if  (ii < dfl->n_known_fields-1) { printf(","); }
       if ((ii+1) % 25 == 0) { printf("\n"); }
     }
     printf("]\n");
     return(-10);
   }
   vpt(2,"  we found prop_known_loc=%ld. and okf[%ld] = %d \n",
     (long int) prop_known_loc,
     ((prop_known_loc >= 0) && (prop_known_loc < dfl->n_known_fields)) ? prop_known_loc : -100,
     ((prop_known_loc >= 0) && (prop_known_loc < dfl->n_known_fields)) ? dfl->ordered_known_fields[prop_known_loc] : -999);
   if ((prop_known_loc >= 0) && (prop_known_loc < dfl->n_known_fields) && dfl->ordered_known_fields[prop_known_loc] == num) {
      vpt(1, " found (num=%ld)  at %ld \n", (long int) num, (long int) prop_known_loc);
      if (dfl->known_usage_count[prop_known_loc] == 0) { dfl->num_used_known_fields++; }
      dfl->known_usage_count[prop_known_loc]++; return(1);
   }
   if (dfl->num_unknown == 0) {
     if (verbose >= 0) {
        printf("-------------------------------------------------------------------------------------------\n");
        printf("--- NOTE we found fix field=%ld at iline =%ld and it does not appear to be in the %ld known fields. \n",
          (long int) num, (long int) iline, (long int) dfl->n_known_fields);
     }
     dfl->ordered_unknown_fields[0] = num;  dfl->unknown_usage_count[0] = 1; dfl->line_unknown[0] = iline;
     dfl->num_unknown = 1;  return(2);
   }
   if (verbose >= 2) {
    printf("NEW VALUE and we have more value! [%ld] - (num unknown =%ld) ---------------------------------------------------\n", (long int) num,
      (long int) dfl->num_unknown);
   }
   int prop_unknown_loc = dfl->num_unknown;
   if (dfl->num_unknown > 0) { 
     prop_unknown_loc = find_o_list((int) num, (int) dfl->num_unknown, (int*) dfl->ordered_unknown_fields);
   } 
   if (prop_unknown_loc < 0) {
     vpt(0, "ERROR, unknown loc returned %ld.  Must be a problem with algo. \n", (long int) prop_unknown_loc);
     printf("[");
     for (int ii = 0; ii < dfl->num_unknown;ii++) {
       printf("%d", dfl->ordered_unknown_fields[ii]); if  (ii < dfl->num_unknown-1) { printf(","); }
       if ((ii+1) % 25 == 0) { printf("\n"); }
     }
     printf("]\n");
   }
   if (prop_unknown_loc ==  dfl->num_unknown) {
     dfl->ordered_unknown_fields[dfl->num_unknown] = num; 
     dfl->unknown_usage_count[dfl->num_unknown] = 1; dfl->line_unknown[dfl->num_unknown] = iline;  dfl->num_unknown++; return(3);
   }
   if ((prop_unknown_loc >= 0) && (dfl->ordered_unknown_fields[prop_unknown_loc] == num)) {
      dfl->unknown_usage_count[prop_unknown_loc]++; return(4);
   }
   if (prop_unknown_loc < 0) { prop_unknown_loc = 0; }
   if (dfl->num_unknown >= dfl->alloc_unknown-1) {
     dfl->ordered_unknown_fields = realloc(dfl->ordered_unknown_fields, dfl->alloc_unknown*2);
     dfl->unknown_usage_count = realloc(dfl->unknown_usage_count, dfl->alloc_unknown*2);
     dfl->line_unknown = realloc(dfl->line_unknown, dfl->alloc_unknown*2);
     if ((dfl->ordered_unknown_fields == NULL) || (dfl->line_unknown == NULL) ||
         (dfl->unknown_usage_count==NULL)) {
       vpt(0, "ERROR, we tried to realloc but failed with dfl->num_unknown=%ld, alloc=%ld. \n",
         (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);  return(-2);
     }
     for (int ii = dfl->alloc_unknown; ii < 2*dfl->alloc_unknown; ii++) {
       dfl->ordered_unknown_fields[ii] = -1;
       dfl->unknown_usage_count[ii] = -1;  dfl->line_unknown[ii] = -1;
     }
     dfl->alloc_unknown *=2;
   }
   for (int jj = dfl->num_unknown-1; jj >= prop_unknown_loc; jj--) {
     dfl->ordered_unknown_fields[jj+1] = dfl->ordered_unknown_fields[jj];
     dfl->line_unknown[jj+1] = dfl->line_unknown[jj]; dfl->unknown_usage_count[jj+1] = dfl->unknown_usage_count[jj];
   }
   dfl->ordered_unknown_fields[prop_unknown_loc] = num;
   dfl->line_unknown[prop_unknown_loc] = iline; dfl->unknown_usage_count[prop_unknown_loc] =1;
   dfl->num_unknown++;
   return(5);
}
int update_field_list_on_field(long int iline, int onfield, char*sf, iStr st_fld, iStr end_fld, DF_config_file *dfc, DF_field_list *dfl, int verbose) {
  char stt[200];
  sprintf(stt, "update_field_list_on_field(ln=%ld,fld=%d,[%ld:%ld],v=%d): ",
    (long int) iline, (int) onfield, (long int) st_fld, (long int) end_fld, (int) verbose);
  iStr orig_st_fld = st_fld;
  if (sf[st_fld] != '{') {
    for (;st_fld < end_fld; st_fld++) { if(sf[st_fld]=='{') { break; } }
  }
  if (sf[st_fld] != '{') {
     vpt(0, "ERROR, sf from %ld:%ld had no \'{\' start. \n", orig_st_fld, st_fld); 
     printf("---: %.*s\n", end_fld-st_fld+1, sf + st_fld);
     return(-1);
  }
  if (sf[end_fld] == '}') { end_fld++; }
  if (sf[end_fld-1] != '}') {
    for(;end_fld >st_fld; end_fld--) { if (sf[end_fld-1] == '}') { break; }}
  }
  vpt(2, "We have field list inspecting = {%.%s}. \n",
    end_fld - st_fld - 1, sf + st_fld);
  iStr iKey = 0; int cnt_keys = 0;
  iKey = get_next_key("update_field_list_on_field", sf, st_fld, end_fld, verbose-1); 
  int num = 0;  int update_code;  int nKeys = 0;
  while((iKey > 0) && (iKey <= end_fld) && (sf[iKey] != '}')) {
    if (sf[iKey] != '\"') { vpt(0, "ERROR, iKey=%ld/%ld between [%ld,%ld], cnt_keys=%ld.  We are at sf[%ld]=\'%c\'\n",
      (long int) iKey, (long int) end_fld, (long int) st_fld, (long int) end_fld, (long int) cnt_keys,
      (long int) iKey, sf[iKey]);  return(-1); 
    }
    iStr endq = get_end_quote("update_field_list_kn_field", sf, iKey, end_fld);
    if ((endq < 0) || (sf[endq] != '\"')) { vpt(0, "ERROR, iKey=%ld/%ld, endq=%ld between [%ld,%ld], cnt_keys=%ld.  We are at sf[%ld:%ld]=\"%.*s\"\n",
      (long int) iKey, (long int) end_fld, (long int) endq, (long int) st_fld, (long int) end_fld, (long int) cnt_keys,
      (long int) iKey, (long int) endq, (endq > iKey) ? endq-iKey-1 : 5, (endq > iKey) ? sf + iKey+1 : "None\0" );  return(-1); 
    }
    sf[endq] = '\0'; num = atoi(sf + iKey+1); sf[endq] = '\"';
    if (num <= 0) {
      vpt(0, "ERROR num picked out was %ld for sf[%ld:%ld] = \"%.*s\" \n", (long int) num, iKey+1, endq,
          endq - iKey-1, sf + iKey+1);   return(-1);
    }
    vpt(2, " on iKey=%ld, we determined num=%ld.  Add to field list. \n", (long int) iKey, (long int) num);
    update_code = add_to_field_list(dfl, num, iline, verbose-2);
    if (update_code < 0) {  vpt(0, "ERROR, update_code=%d for adding num=%ld?  Why ? \n", (long int) update_code, (long int) num); return(-1); }
    if (update_code != 1) { vpt(1, "Note  we received an update_code of %ld for num=%ld. \n", (long int) update_code, (long int) num); }
    iKey = get_next_key("update_field_list_on_field", sf, iKey, end_fld, verbose-2);  nKeys++;
  }
  if (verbose >=3 ) {
     vpt(3, "Finished iline=%ld, onfield=%ld.  Found %ld keys. \n", (long int) iline, (long int) onfield, (long int) nKeys);
     printf("--------------------------------------------------------------------------------------------------------------\n\n");
  }
  return(1);
}  
int update_field_list_on_line(long int iline, char*sf, iStr st_ln, iStr end_ln, DF_config_file *dfc, DF_field_list *dfl, int verbose) {
  int ii = st_ln;  int ifield = 0;
  int ion_schema = 0;
  char stt[300];
  sprintf(stt, "update_field_list_on_line(iline=%ld,v=%d,ns=%ld): ", (long int) iline, (int) verbose, (long int) dfc->n_schemas);
  vpt(2, "Here is complete line...\n%.*s\n", end_ln-st_ln, sf+st_ln);
  int attempt;
  for (ion_schema = 0; ion_schema < dfc->n_schemas; ion_schema++) {
    if (dfc->schemas[ion_schema].typ != fix42) {
       //NEXTCOMMA();  We will have dfl has assigned character separation  I suppose ",|; \t" all reasonable separators
       NEXTCHARSEP();
    } else {
      iStr fieldStart = ii;
      //NEXTCOMMA();  
      NEXTCHARSEP();
      iStr fieldEnd = ii-1;
      vpt(1, "on is=%ld/%ld, found FX between [%ld,%ld]: \"%.*s...\" \n", 
          (long int) ion_schema, (long int) dfc->n_schemas, 
          (long int) fieldStart, (long int) fieldEnd, 
          fieldEnd-fieldStart < 10 ? fieldEnd-fieldStart : 10, 
          (char*) sf + fieldStart);
      attempt = update_field_list_on_field(iline, ifield, sf, fieldStart, sf[fieldEnd] == '}' ? fieldEnd+1 : fieldEnd, dfc, dfl, verbose);
      if (attempt < 0) {
        vpt(0, " ERROR attempt = %ld for schema=%ld/%ld on iline=%ld. [st,end]=[%ld,%ld] with fieldst/end=[%ld,%ld] for:\n",
          (long int) attempt, (long int) ion_schema, (long int) dfc->n_schemas, 
          (long int) iline, (long int) st_ln, (long int) end_ln, (long int) fieldStart, (long int) fieldEnd);
        printf("---: %.*s\n", fieldEnd-fieldStart+1, sf+fieldStart);
        printf("  What went wrong? \n");  return(-1);
      }
    }
  }
  return(1);
}
iStr get_next_newln(char *sf, iStr st, iStr nmax, int verbose) {
  char stt[] = "read_file.c->get_next_newln()";
  iStr ii;
  for (ii=st; ii < nmax; ii++) {
    if (sf[ii] == '\n') {  return(ii); }
    if (sf[ii] == '\"') {
      ii = get_end_quote((char*) stt, sf, ii, nmax);
      if (ii < 0) { printf("get_next_newln(sf[st=%ld:%ld] = \"%.*s\" no exit from quote. ii=%ld. \n",
        st, nmax-st < 20 ? nmax : st + 20, nmax-st < 20 ? nmax-st : 20 , sf + st, ii);  return(nmax+1); }
    } else if (sf[ii] == '{') {
      ii = get_end_brace((char*) stt,sf, ii, nmax); 
      if (ii < 0) { printf("get_next_newln(sf[st=%ld:%ld] = \"%.*s\" no exit from get_end_brace. ii=%ld. \n",
        st, nmax-st < 20 ? nmax : 20, nmax-st < 20 ? nmax-st : 20 , sf + st, ii);  return(nmax+1); }
    } else if (sf[ii] == '[') { ii = get_end_bracket( (char*) stt,sf, ii, nmax); 
      if (ii < 0) { printf("get_next_newln(sf[st=%ld:%ld] = \"%.*s\" no exit from get_end_bracket. ii=%ld. \n",
        st, nmax-st < 20 ? nmax : st + 20, nmax-st < 20 ? nmax-st : 20, sf + st, ii);  return(nmax+1); }
    }
  }
  return(nmax);
}
DF_field_list *generate_field_list(char *tgt_filename, DF_config_file *dfc, char char_sep, int verbose, int standard_vector_size) {
  char stt[500];
  sprintf(stt, "generate_file_list(\"%.*s\",v=%ld,nf=%ld): ", 40, tgt_filename, (long int) verbose, (long int) dfc->nfields); 
  vpt(1, "  -- Welcome I hope DFC is populated. \n");
  DF_field_list *dfl = create_blank_field_list(dfc, verbose-1);
  if (dfl == NULL) { vpt(0, " -- failed to allocate dfl at beginning. \n");  return(NULL); }
  vpt(1,  " -- Opening File pointer for first time. \n");
  FILE *fpo = NULL; fpo = fopen(tgt_filename, "rt");
  if (fpo == NULL) {
    vpt(-1, " FAILED to open tgt_filename = \"%s\". \n", tgt_filename);  delete_field_list(&dfl,2 ); return(NULL);
  }
  iStr file_len = 0;
  if (fseek(fpo, 0L, SEEK_END) == 0) { 
    file_len = ftell(fpo); rewind(fpo);
  } else { 
    vpt(0, "ERROR, trying to read length of file. \n"); 
    delete_field_list(&dfl, 2); return(NULL); 
  }

  char buffer[MAXREAD];  int remainder= 0;
  long int bytesread; long int tbytesread = 0;
  bytesread = fread(buffer,sizeof(char),MAXREAD,fpo);  
  int onstr = 0; long int iline_prints=0;
  int ibuffreads = 1;
  int update_error;
  dfl->file_total_bytes = (long int) file_len;
  dfl->n_total_lines = 0;
  dfl->char_sep = char_sep;
  dfl->line_locs[0] = onstr;  dfl->line_locs[1] = bytesread; 
  while ((tbytesread < dfl->file_total_bytes) && (bytesread > 0)) {
    if (verbose >= 3) { printf("\n----------------------------------------------------\n"); }
    vpt(1, " on ibuffreads=%ld  bytesread=%ld, remainder=%ld, tbytesread=%ld. \n",
      (long int) ibuffreads, (long int) bytesread, (long int) remainder, (long int) tbytesread);
    vpt(3, " here is the complete buffer so far: \n%.*s\nLet's begin...\n", (long int) bytesread, buffer);
    while (onstr < bytesread) {
      vpt(3, " on iline=%ld, onstr=%ld/%ld, tbytesread=%ld, \"%.*s...\"\n",
        (long int) dfl->n_total_lines, (long int) onstr, (long int) bytesread,  (long int) tbytesread, 30, buffer + onstr);
      while ((onstr < bytesread) && (buffer[onstr] == '\n')) {onstr++; }
      iStr iLineEnd = get_next_newln(buffer, onstr, bytesread, verbose-2); 
      if (iLineEnd >= bytesread) { break; }
      if (buffer[iLineEnd] != '\n') { printf("generate_file_list, iLineEnd=%ld, but buffer[%ld]=\'%c\' ? \n",
                                      (long int) iLineEnd, (long int) iLineEnd, buffer[iLineEnd]);  }
      if (dfl->n_loc_lines >= dfl->alloc_line_loc - 3) {
         dfl->line_locs = realloc(dfl->line_locs, dfl->alloc_line_loc*2);
         if (dfl->line_locs == NULL) { 
           vpt(0, "ERROR trying to double line_locs length to $ld \n", (long int) (dfl->alloc_line_loc*2));
           dfl->finish = 0;
           rewind(fpo);
           fclose(fpo); return(dfl);
         }
         dfl->alloc_line_loc *= 2;
      }
      dfl->line_locs[dfl->n_loc_lines+1] = tbytesread + iLineEnd+1;
      if ((dfl->n_total_lines+1) % standard_vector_size == 0)  {
        dfl->n_loc_lines++;  dfl->line_locs[dfl->n_loc_lines+1] = tbytesread + bytesread;
      }
      update_error = update_field_list_on_line(dfl->n_total_lines, buffer, onstr, iLineEnd, dfc, dfl, verbose-2);
      if (update_error < 0) {
        printf("Error on update with [onstr,iLineEnd]=[%ld,%ld] for \n%.*s\nWhat went wrong?\n",
         (long int) onstr, (long int) iLineEnd, iLineEnd-onstr +1, buffer+onstr);
        dfl->finish = 0; rewind(fpo);
        fclose(fpo);  return(dfl);
      }
      onstr = iLineEnd+1;  dfl->n_total_lines++;
    }
    if (onstr < bytesread) {
       remainder = (bytesread-onstr);
       memcpy(buffer, buffer + onstr, sizeof(char)*remainder);
    }
    tbytesread += bytesread-remainder;
    int new_bytes = fread(buffer+remainder,1,MAXREAD-remainder,fpo);  onstr = 0;
    bytesread = new_bytes + remainder;
  }
  dfl->finish = 1;
  rewind(fpo);
  fclose(fpo);
  return(dfl);
}
iStr alter_load_file_to_str(char **out_p_sf, char *json_filename, int verbose) {
  if (200 <= get_str_l("json_filename", json_filename, 200)) {
    printf("load_file_to_str: error filename invalid. \n"); return(-1);  
  }
  char stt[500];  
  sprintf(stt, "load_file_to_str(%s,v=%ld): ", 
    json_filename, (long int) verbose);
  FILE *fpo = NULL; fpo = fopen(json_filename, "rt");
  if (fpo == NULL) { vpt(0, "ERROR FAIL to open file.\n"); return(-1); }
  iStr file_len = 0;
  if (fseek(fpo, 0L, SEEK_END) == 0) {
    file_len = ftell(fpo);
    rewind(fpo);
  } else {
    vpt(0, "ERROR trying to read length of file. \n");fclose(fpo);fpo=NULL;return(-2);
  }

  vpt(1, "  LENGTH of File is supposedly %ld \n", file_len);
  vpt(1, "  INITIATE READ of file. \n");

  char *sf =  NULL;
  //int sflen = ( (long int) sizeof(char)) * ((long int) (file_len+4));
  sf = (char*) malloc( (size_t) sizeof(char) * ((int) (file_len+4))); 
  if (sf == NULL) { out_p_sf[0] = NULL; return(-1); }
  size_t byte_count = fread(sf, sizeof(char), file_len, fpo);
  if (ferror(fpo) != 0) {
    vpt(0, "ERROR trying to read file into buffer. \n");
    free(sf); sf= NULL; fclose(fpo);  return(-3);
  }
  fclose(fpo);
  fpo = NULL;
  sf[file_len] = '\0'; sf[file_len+1] = '\0'; sf[file_len+2] = '\0';
  out_p_sf[0] = sf; sf = NULL;  return(file_len);
  return(file_len);
}
int loc_any_fixfield_at_priority(DF_config_file *dfc, DF_field_list *dfl, int nstart, int tgt_priority, int verbose) {
  if (nstart >= dfl->n_known_fields-1) { return(-1); }
  int i_f;
  for (i_f = nstart+1; i_f < dfl->n_known_fields; i_f++) {
    if ((dfl->known_usage_count[i_f] > 0) &&
        ((dfc->fxs[i_f].keep > 0) && (dfc->fxs[i_f].priority >= 0) && (dfc->fxs[i_f].priority == tgt_priority))) {
      return(i_f);
    }
  } 
  return(-1);
}
int loc_lowest_priority_fixfield_gt(DF_config_file *dfc, DF_field_list *dfl, int tgt, int verbose) {
  char stt[500];
  sprintf(stt, "lowest_priority_fixfield_gt(tgt=%ld, v=%d): ",
    (long int) tgt, (long int) verbose); 
  int i_f = -1; int loc_min = -1; int on_min = -1;
  for (i_f = 0; i_f < dfl->n_known_fields; i_f++) {
    if (dfl->known_usage_count[i_f] > 0) {
       if ((dfc->fxs[i_f].priority) >= 0 && (dfc->fxs[i_f].priority > tgt)) {
         if (loc_min < 0) {
           loc_min = i_f; on_min = dfc->fxs[i_f].priority;
         } else if (on_min > dfc->fxs[i_f].priority) {
           loc_min = i_f; on_min = dfc->fxs[i_f].priority;
         }
       }
    }
  }
  if ((loc_min >= 0) && (dfc->fxs[loc_min].priority <= tgt)) {
    printf("loc_lowest_priority_fixfield_gt: We have loc_min = %ld, but priority scored at %ld relative to %ld target?\n",
      (long int) loc_min, (long int) (dfc->fxs[loc_min].priority), (long int) tgt);
  }
  return(loc_min);
}
int next_priority_fixfield(DF_config_file *dfc, DF_field_list *dfl, int on_f, int verbose) {
  char stt[500];
  sprintf(stt, "next_priority_fixfield(after %ld): ", (long int) on_f);
  if ((on_f < 0) || (on_f >= dfc->nfields)) { return(-1); } 
  int on_val = dfc->fxs[on_f].priority;
  int other_loc = loc_any_fixfield_at_priority(dfc, dfl, on_f, on_val, verbose-1); 
  if ((other_loc >= 0) && (other_loc < dfl->n_known_fields)) { return(other_loc); }
  int next_lowest =  loc_lowest_priority_fixfield_gt(dfc, dfl,on_val+1, verbose);
  return(next_lowest);
}
int loc_any_schema_at_priority(DF_config_file *dfc, int nloc, int tgt_priority, int verbose) {
  int i_s = -1;
  if (nloc >= dfc->n_schemas-1) { return(-1); }
  for (i_s = nloc+1; i_s < dfc->n_schemas; i_s++) {
    if ((dfc->schemas[i_s].typ != fix42) && (dfc->schemas[i_s].priority == tgt_priority)) { return(i_s); } }
  return(-1);
}
int loc_lowest_priority_schema_gt(DF_config_file *dfc, int tgt, int verbose) {
  char stt[500];
  sprintf(stt, "lowest_priority_schema_gt(tgt=%ld, v=%d): ",
    (long int) tgt, (long int) verbose); 
  int i_s = -1; int loc_min = -1; int on_min = -1;
  for (i_s = 0; i_s < dfc->n_schemas; i_s++) {
    if ((dfc->schemas[i_s].priority >= 0) && (dfc->schemas[i_s].typ != fix42)){
       if (dfc->schemas[i_s].priority >= tgt) {
         if (loc_min < 0) {
           loc_min = i_s; on_min = dfc->schemas[i_s].priority;
         } else if (on_min > dfc->schemas[i_s].priority) {
           loc_min = i_s; on_min = dfc->schemas[i_s].priority;
         }
       }
    }
  }
  return(loc_min);
}
int next_priority_schema(DF_config_file *dfc, int on_s, int verbose) {
  char stt[500];
  sprintf(stt, "next_priority_schema(after %ld): ", (long int) on_s);
  if ((on_s < 0) || (on_s >= dfc->n_schemas)) { return(-1); } 
  int on_val = dfc->schemas[on_s].priority;
  int other_loc = loc_any_schema_at_priority(dfc, on_s, on_val, verbose-1); 
  if ((other_loc >= 0) && (other_loc < dfc->n_schemas)) { return(other_loc); }
  int next_lowest =  loc_lowest_priority_schema_gt(dfc, on_val+1, verbose);
  return(next_lowest);
}
int configure_column_order(DF_config_file *dfc, DF_field_list *dfl, int verbose) {
  char stt[500];
  sprintf(stt, "configure_column_order(v=%ld,ns=%ld,nfx=%ld)", (long int) verbose,
    (int) dfc->n_schemas, (int) dfl->num_used_known_fields);
  if (dfl->num_unknown > 0) {
    vpt(0, "ERROR: there are %ld unknown fix columns, please configure.\n", (long int) dfl->num_unknown);
    return(-1);
  }
  int on_s = loc_lowest_priority_schema_gt(dfc, -1, verbose); 
  int on_f = loc_lowest_priority_fixfield_gt(dfc,dfl, -1, verbose);
  int p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
  int p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
  vpt(1, " We start with min schema=%d[%d] and field=%d [loc %d, %d] \n",
    on_s, p_on_s, ((on_f >= 0) && (on_f < dfc->nfields)) ? dfl->ordered_known_fields[on_f] : -999,
    on_f, p_on_f); 
  int on_final_loc = 0;
  while ((p_on_s >= 0) || (p_on_f >= 0)) {
    if ((on_f < 0) || (p_on_s <= p_on_f)) {
      dfc->schemas[on_s].final_loc = on_final_loc;  
      on_s = next_priority_schema(dfc, on_s, verbose-1);
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
    } else {
      dfl->final_known_print_loc[on_f] = on_final_loc; 
      on_f = next_priority_fixfield(dfc, dfl, on_f, verbose-1);
      p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
    }
    on_final_loc++;
  }  
  dfc->n_total_print_columns = on_final_loc;
  dfc->mark_visited = (int*) malloc(sizeof(int)*2*on_final_loc);
  if (dfc->mark_visited == NULL) {
    vpt(-1, "ERROR -trying to allocate dfc->mark_visited resulted in fail. \n");
    return(-104);
  }
  for (int ii = 0; ii < on_final_loc*2; ii++) { dfc->mark_visited[ii] = 0; }
  // repeat, lets mark mark_visited
  on_s = loc_lowest_priority_schema_gt(dfc, -1, verbose); 
  on_f = loc_lowest_priority_fixfield_gt(dfc,dfl, -1, verbose);
  p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
  p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
  int on_pt = 0;  int error_form = 0;
  while ((p_on_s >= 0) || (p_on_f >= 0)) {
    if ((on_f < 0) || (p_on_s <= p_on_f)) {
      dfc->mark_visited[dfc->n_total_print_columns + on_pt] = on_s;
      if (dfc->schemas[on_s].final_loc != on_pt) {
        vpt(-1, " ERROR on_pt=%d/%d,  (on_s=%ld/%ld p=%ld, on_f=%ld/%ld p=%d but schema[%d].final_loc=%d \n",
          (int) on_pt, (int) on_final_loc, (long int) on_s, (long int) dfc->n_schemas, (int) p_on_s,
          (int) on_f, (int) dfl->n_known_fields,  (int) p_on_f,
          (int) on_s, dfc->schemas[on_s].final_loc);  error_form++; 
      } 
      on_s = next_priority_schema(dfc, on_s, verbose-1);
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
    } else {
      dfc->mark_visited[dfc->n_total_print_columns + on_pt] = -on_f;
      if (dfl->final_known_print_loc[on_f] != on_pt) {
        vpt(-1, " ERROR on_pt=%d/%d,  (on_s=%ld/%ld p=%ld, on_f=%ld/%ld p=%d but field[%d].final_known_print_loc=%d \n",
          (int) on_pt, (int) on_final_loc, (long int) on_s, (long int) dfc->n_schemas, (int) p_on_s,
          (int) on_f, (int) dfl->n_known_fields,  (int) p_on_f,
          (int) on_f, dfl->final_known_print_loc[on_f]);  error_form++; 
      } 
      on_f = next_priority_fixfield(dfc, dfl, on_f, verbose-1);
      p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
    }
    on_pt++;
  }  
  if (error_form > 0) {
    vpt(-1, "Calculating mark_visited failed. \n"); return(-10);
  }
  vpt(0, "Mark Visited returns all correct. \n");
  return(1);
} 

int PRINT_final_print_loc(DF_config_file *dfc, DF_field_list *dfl) {
  char stt[500];  int verbose =0;
  sprintf(stt, "PRINT_priorities(ns=%ld,nfx=%ld)",
    (int) dfc->n_schemas, (int) dfl->num_used_known_fields);
  if (dfl->num_unknown > 0) {
    vpt(0, "ERROR: there are %ld unknown fix columns, please configure.\n", (long int) dfl->num_unknown);
    return(-1);
  }
  int on_s = loc_lowest_priority_schema_gt(dfc, -1, verbose); 
  int on_f = loc_lowest_priority_fixfield_gt(dfc,dfl, -1, verbose);
  int p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -1;
  int p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -1;
  int on_pt = 0;
  printf(" --- Printing target table order [%ld columns] (start with S[%ld:%ld],F[%ld:%ld])\n", dfc->n_total_print_columns,
    (long int) on_s, (long int) p_on_s, (long int) on_f, (long int) p_on_f);
  printf(" -- mark_visited = ["); 
  for (on_pt = 0; on_pt < dfc->n_total_print_columns; on_pt++) {
    printf("%d", dfc->mark_visited[dfc->n_total_print_columns + on_pt]);
    if (on_pt < dfc->n_total_print_columns -1) { printf(","); if ((on_pt+1)%10 == 0) { printf("\n              "); } }
  }
  printf("]\n");
  printf("[    ");
  on_pt = 0;
  while ((on_s >= 0) || (on_f >= 0)) {
    if ((on_f < 0) || (p_on_s <= p_on_f)) {
      printf("[%d=%d:on_s=%d=%d,Sch:\"%s\",%s,",
        on_pt, dfc->schemas[on_s].final_loc, (int) on_s, 
        (int) dfc->mark_visited[dfc->n_total_print_columns + on_pt],
        dfc->schemas[on_s].nm, What_DF_DataType(dfc->schemas[on_s].typ));
      if ((dfc->schemas[on_s].typ == tms) || (dfc->schemas[on_s].typ == tus) || (dfc->schemas[on_s].typ == tns)) {
        printf("%s,", What_DF_TSType(dfc->schemas[on_s].fmttyp));
      }
      printf("priority=%d,fl=%d]", dfc->schemas[on_s].priority, (int) dfc->schemas[on_s].final_loc);
      on_s = next_priority_schema(dfc, on_s, verbose-1);
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -1;
    } else {
      printf("[%d=%d:on_f=%d=%d,FF=%ld,",
        on_pt, dfl->final_known_print_loc[on_f], 
        (int) on_f, (int) dfc->mark_visited[dfc->n_total_print_columns + on_pt],
        (long int) dfl->ordered_known_fields[on_f]);
      printf("\"%s\",", (char*) dfc->fxs[on_f].fixtitle);
      printf("%s,", What_DF_DataType((dfc->fxs[on_f].typ)));
      if ((dfc->fxs[on_f].typ == tms) || (dfc->fxs[on_f].typ == tus) || (dfc->fxs[on_f].typ == tns)) {
        printf("%s,", What_DF_TSType(dfc->fxs[on_f].fmttyp));
      }
      printf("priority=%d]", (int) dfc->fxs[on_f].priority);
      on_f = next_priority_fixfield(dfc, dfl, on_f, verbose-1);
      p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -1;
    }
    if (on_pt < dfc->n_total_print_columns-1) { printf(", "); 
      if ((on_pt+1) % 3 == 0) { printf("\n     "); }}
    on_pt++;
  }  
  printf("\n];\n");
  if (on_pt != dfc->n_total_print_columns) {
    vpt(0, "ERROR Potential issue, on_pt after algo = %ld, but total print columns should be %ld. \n",
       (long int) on_pt, (long int) dfc->n_total_print_columns); return(-1);
  }
  printf("\n\nEND print of column orders!\n");
  return(1);
} 


