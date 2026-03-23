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
#define DFFREE( x, lnx, cx ) \
  if (( (x) != NULL) && (lnx > 0)) {                  \
    vpt(1, "Delete %s.\n", (cx));                     \
    x[lnx-1] = x[0];                                  \
    free((x));  x = NULL;                             \
  }                                                   \
  x = NULL

#define ALLOC_INT_ME_SIZE( mykey, sz, cs ) \
  vpt(2, " allocate %s. \n", cs);  \
  mykey = (int *) malloc(sizeof(int) * ((int) sz) ); \
  if (  (mykey) == NULL) { vpt(0, " error allocating %s. \n", cs);  delete_field_list(&dfl,2);  return(NULL); } \
  mykey[0] = 0
#endif


DF_field_list *create_nulled_field_list(DF_config_file *dfc, int verbose) {
  char stt[]="create_nulled_field_list(): ";
  if (verbose >= 1) {
    printf("create_nulled_field_list, start, verbose=%d. \n", (int)verbose);
  }
  if (dfc == NULL) {
    printf("create_nulled_field_list, error, dfc supplied is NULL. \n");
  }
  int nfields = dfc->nfields;
  if (nfields <= 0) {
    printf("create_nulled_field_list: wont work becasue dfc->nfields=%ld. \n", (long int) dfc->nfields);
    return(NULL);
  }
  DF_field_list *dfl = (DF_field_list*) malloc(1.0*sizeof(DF_field_list));
  if (dfl == NULL) { return(NULL); }
  dfl->n_known_fields = nfields;
  dfl->num_used_known_fields = 0;
  dfl->ordered_known_fields = NULL;  
  dfl->known_usage_count = NULL;
  dfl->known_multiplicity=NULL; dfl->unknown_multiplicity=NULL;
  dfl->alloc_unknown = 0;  dfl->num_unknown = 0;
  dfl->ordered_unknown_fields = NULL;
  dfl->unknown_usage_count=NULL;
  dfl->line_unknown = NULL;
  dfl->final_known_print_loc = NULL; dfl->final_known_multiplicity_loc = NULL;
  dfl->line_locs = NULL;  dfl->n_loc_lines = 0;  dfl->alloc_line_loc = 0;
  dfl->n_total_lines = 0; dfl->n_total_all_lines = 0;
  dfl->finish = 0; // Send to 1 if success
  return(dfl);
}
DF_field_list *create_blank_field_list(DF_config_file *dfc, int verbose) {
  char stt[]="create_blank_field_list(): ";
  int nfields = dfc->nfields;
  if (nfields <= 0) {
    printf("create_blank_field_list: wont work becasue dfc->nfields=%ld. \n", (long int) dfc->nfields);
    return(NULL);
  }
  DF_field_list *dfl = (DF_field_list*) malloc(1.0*sizeof(DF_field_list));
  if (dfl == NULL) { return(NULL); }
  dfl->n_known_fields = nfields;
  dfl->num_used_known_fields = 0;
  dfl->ordered_known_fields = NULL;  
  dfl->known_usage_count = NULL;
  dfl->known_multiplicity=NULL; dfl->unknown_multiplicity=NULL;
  dfl->alloc_unknown = 0;  dfl->num_unknown = 0;
  dfl->ordered_unknown_fields = NULL;
  dfl->unknown_usage_count=NULL;
  dfl->line_unknown = NULL;
  dfl->final_known_print_loc = NULL; dfl->final_known_multiplicity_loc = NULL;
  dfl->line_locs = NULL;  dfl->n_loc_lines = 0;  dfl->alloc_line_loc = 0;
  dfl->n_total_lines = 0; dfl->n_total_all_lines = 0;
  dfl->finish = 0; // Send to 1 if success
  ALLOC_INT_ME_SIZE( (dfl->ordered_known_fields) , nfields, "ordered_known_fields");
  ALLOC_INT_ME_SIZE( (dfl->known_usage_count) , nfields, "ordered_unknown_fields");
  ALLOC_INT_ME_SIZE( (dfl->known_multiplicity) , nfields, "ordered_unknown_fields");
  ALLOC_INT_ME_SIZE( (dfl->ordered_unknown_fields) , nfields, "ordered_unknown_fields");

  ALLOC_INT_ME_SIZE( (dfl->final_known_print_loc) , nfields, "final_known_print_loc");
  ALLOC_INT_ME_SIZE( (dfl->final_known_multiplicity_loc) , nfields, "final_known_multiplicity_loc");

  ALLOC_INT_ME_SIZE( (dfl->line_unknown) , nfields, "ordered_unknown_fields");
  ALLOC_INT_ME_SIZE( (dfl->unknown_usage_count) , nfields, "ordered_unknown_fields");
  ALLOC_INT_ME_SIZE( (dfl->unknown_multiplicity) , nfields, "ordered_unknown_fields");

  dfl->alloc_line_loc = 4 + (nfields < 50 ? 50 : nfields);
  char mst[400];
  sprintf(mst, "line_locs size alloc_line_loc=%ld.\n", (long int) dfl->alloc_line_loc);
  ALLOC_INT_ME_SIZE( (dfl->line_locs), dfl->alloc_line_loc, mst); 
  dfl->alloc_unknown = nfields;
  for (int ii = 0; ii < nfields; ii++) {
    dfl->ordered_known_fields[ii] = dfc->ordered_fields[ii];
    dfl->known_usage_count[ii] = 0; dfl->final_known_print_loc[ii] = -1; dfl->final_known_multiplicity_loc[ii] = -1;
    dfl->known_multiplicity[ii] = 0;
  }
  for (int ii = 0; ii < nfields; ii++) {
    dfl->ordered_unknown_fields[ii] = -1; dfl->unknown_usage_count[ii] = -1;  dfl->line_unknown[ii] =-1;
    dfl->unknown_multiplicity[ii] = 0;
  }

  for (int ii = 0; ii < dfl->alloc_line_loc; ii++) {
    dfl->line_locs[ii] = -1;
  }
  dfl->num_unknown = 0;
  return(dfl);
}

int delete_field_list(DF_field_list **p_dfl, int verbose) {
  char stt[300];
  verbose = verbose -1;
  if (p_dfl[0] == NULL) { return(-1); }
  DF_field_list *dfl = p_dfl[0];
  sprintf(stt, "delete_field_list(v=%d,k=%ld,u=%ld/%ld): ",
    (int) verbose, (long int) dfl->n_known_fields, (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);
   
  DFFREE(dfl->final_known_print_loc, dfl->n_known_fields, "final_known_print_loc"); 
  DFFREE(dfl->final_known_multiplicity_loc, dfl->n_known_fields, "final_known_multiplicity_loc"); 
  DFFREE(dfl->ordered_known_fields, dfl->n_known_fields, "ordered_known_fields");
  DFFREE(dfl->ordered_unknown_fields, dfl->alloc_unknown, "ordered_unknown_fields");
  DFFREE(dfl->known_usage_count, dfl->n_known_fields, "known_usage_count");
  DFFREE(dfl->unknown_usage_count, dfl->alloc_unknown, "unknown_usage_count");
  DFFREE(dfl->line_unknown, dfl->alloc_unknown, "line_unknown");
  DFFREE(dfl->line_locs, dfl->alloc_line_loc, "line_locs");
  DFFREE(dfl->known_multiplicity, dfl->n_known_fields, "known multiplicty"); 
  DFFREE(dfl->unknown_multiplicity, dfl->alloc_unknown, "unknown multiplicty");
  return(1);
}
int PRINT_dfl(DF_field_list *dfl) {
  printf("Printing DF_Field_list(finish=%ld: file_total_bytes=%ld): dfl with %ld known fields, %ld/%ld unknown \n",
     (long int) dfl->finish, (long int) dfl->file_total_bytes, (long int) dfl->n_known_fields, (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);
  printf("KNOWN[%ld, %ld used] --  {\n", dfl->n_known_fields, dfl->num_used_known_fields);
  printf("     ");  int n_used_known = 0;
  for (int ii = 0; ii < dfl->n_known_fields; ii++) {
    if (dfl->known_usage_count[ii] > 0) { n_used_known++; }
    printf("[%ld:%ld,MX=%ld,pl%ld:m%ld]", (long int) dfl->ordered_known_fields[ii], (long int) dfl->known_usage_count[ii], (long int) dfl->known_multiplicity[ii],
     dfl->final_known_print_loc[ii], dfl->final_known_multiplicity_loc[ii]);
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
    printf("[fixfield=%ld:used %ld:%ld,MX:%ld]", (long int) dfl->ordered_unknown_fields[ii], (long int) dfl->unknown_usage_count[ii],
      (long int) dfl->line_unknown[ii], dfl->unknown_multiplicity[ii]);
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
  if ((olist[iL] < num) && (olist[iR] > num)) { return(iR); }
  printf("\n\n Note find_o_list failed to find itick=%ld/%ld and iL=%ld,iM=%ld,iR=%ld, for [%d,%d,%d]  Lets try another approach\n", (long int) itick, (long int) nlist,
    (long int) iL, (long int) iM, (long int) iR, olist[iL], olist[iM], olist[iR]);
  int nSortErrors = 0;
  for (iR = 0; iR < nlist; iR++) {
    if ((iR < nlist-1) && (olist[iR] >= olist[iR+1])) {
       printf("-- BIG ERROR COUNT we have that olist is not correclty sorted.  iR=%ld but olist[iR=%ld]=%ld >= %ld=olist[iR+1=%ld]   \n",
         (long int) iR, (long int) iR, (long int) olist[iR], (long int) olist[iR+1], (long int) iR+1);
       nSortErrors++;
    }
    if (olist[iR] == num) {
      printf("  --- Woah find_o_list is broken, we found oList[%ld/%ld]=%ld, which is a win or a fail!\n", (long int) iR, (long int) nlist, (long int) num);
      printf("  --- Note up to this point we had %ld sort errors! \n", (long int) nSortErrors);
      printf("XXX We return -iR-1. \n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
      printf("\n\n\n");
      return(-iR-1);
    }
  }
  if (nSortErrors > 0) {
    printf("Error FindOList, we didn't find the target but we had %ld sort errors ! \n", (long int) nSortErrors);
    return(-nSortErrors);
  }
  printf(" I guess fine, never found %ld in the list length %ld. \n", (long int) num, (long int) nlist);
  return(iR);
}
int add_to_field_list(DF_field_list *dfl, int num, int iline, int verbose, int multiplicity) {
   char stt[500];
   if (num <= 0) {
     printf("add_to_field_list:: Fix fields are zero or negative, num=%ld. \n", (long int) num); return(-10);
   }
   sprintf(stt, "add_to_field_list[num=%ld,line=%ld,v=%ld, multiplicity=%ld]: ",
     (long int) num, (long int) iline, (long int) verbose, (long int) multiplicity);
   int prop_known_loc = find_o_list(num, dfl->n_known_fields, dfl->ordered_known_fields);
   if (multiplicity <= 0) {
     vpt(-10, "ERROR: multiplicity given as %ld, that's not realistic. \n", (long int) multiplicity);
     return(-10);
   }
   if (prop_known_loc < 0) {
     vpt(0, " ERROR, prop_known_loc returned %ld.  Must be a problem with algo on search dfl->ordered_known_fields\n", (long int) prop_known_loc);
     vpt(0, "  Note %ld translates to %ld/%ld, which returns %ld in known fields. \n", (long int) prop_known_loc,
        (long int) (1-prop_known_loc), (long int) dfl->n_known_fields,  ((1-prop_known_loc) >= 0) && (1-prop_known_loc) < dfl->n_known_fields ?
        dfl->ordered_known_fields[1-prop_known_loc] : -99999);
     printf("[");
     for (int ii = 0; ii < dfl->n_known_fields;ii++) {
       printf("%d", dfl->ordered_known_fields[ii]); if  (ii < dfl->n_known_fields-1) { printf(","); }
       if ((ii+1) % 25 == 0) { printf("\n"); }
     }
     printf("]\n");
     return(-10032);
   }
   vpt(2,"  we found prop_known_loc=%ld/%ld. and okf[%ld] = %d \n",
     (long int) prop_known_loc, (long int) dfl->n_known_fields,
     ((prop_known_loc >= 0) && (prop_known_loc < dfl->n_known_fields)) ? prop_known_loc : -100,
     ((prop_known_loc >= 0) && (prop_known_loc < dfl->n_known_fields)) ? dfl->ordered_known_fields[prop_known_loc] : -999);
   if ((prop_known_loc >= 0) && (prop_known_loc < dfl->n_known_fields) && dfl->ordered_known_fields[prop_known_loc] == num) {
      vpt(1, " found (num=%ld)  at %ld, current multiplicity=%ld, num_used_known=%ld [%ld/%ld total known, %ld unknown] \n", (long int) num, (long int) prop_known_loc,
         (long int) multiplicity, dfl->known_usage_count[prop_known_loc], (long int) dfl->num_used_known_fields, (long int) dfl->n_known_fields, (long int) dfl->num_unknown);
      if (dfl->known_usage_count[prop_known_loc] == 0) { dfl->num_used_known_fields++; }
      if (multiplicity > dfl->known_multiplicity[prop_known_loc]) { dfl->known_multiplicity[prop_known_loc] = multiplicity; }
      dfl->known_usage_count[prop_known_loc]++; 
      return(1);
   }
   // Note if dfl->ordered_known_fields[prop_known_loc] != num, it is new number and this is location it "should be inserted to"
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
  
   if (verbose >= 2) {
     printf(" --- About to insert into ordered_unknown_fields length num_unknown=%ld, alloc_unknown=%ld. \n",
       (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);
   }
   /***************
   int sort_errors = 0;
   for (int ii = 0; ii < dfl->num_unknown -1; ii++) {
     if ((ii < dfl->num_unknown-1) && (dfl->ordered_unknown_fields[ii] >= dfl->ordered_unknown_fields[ii+1])) {
       sort_errors++;
     }
   }
   if (sort_errors > 0) {
     printf("ERROR -- ordered_unknown_fields failed to sort correctly. Errors = %ld\n [ ", (long int) sort_errors);
     for (int ii = 0; ii < dfl->num_unknown; ii++) {
       printf("%ld", dfl->ordered_unknown_fields[ii]); 
       if (ii == dfl->num_unknown-1) { printf("]\n"); } else if ((ii+1)%10 == 0) {  printf(",\n"); } else { printf(", "); }
     }
     vpt(-34030, "Correct error before this somehow? \n"); return(-4303023);
   } else {
      printf("---- No errors found before search. \n");
   }
   *************/
   int prop_unknown_loc = dfl->num_unknown;
   if (dfl->num_unknown > 0) { 
     prop_unknown_loc = find_o_list((int) num, (int) dfl->num_unknown, (int*) dfl->ordered_unknown_fields);
   }
   if ((prop_unknown_loc < 0) && (dfl->num_unknown + (prop_unknown_loc+1) >= 0) && (dfl->ordered_unknown_fields[1-prop_unknown_loc] == num)) {
     vpt(-1030, "ERROR verficiation in unknown list.  We received prop_unknown_loc=%ld/%ld "
                "translates into position %ld/%ld which is %ld in unknown fields.\n",
      (long int) prop_unknown_loc, (long int) dfl->num_unknown, (long int) 1 - prop_unknown_loc, (long int) dfl->num_unknown, num);
     vpt(-30430, "Fix find_o_list algo!\n");
   } else if (prop_unknown_loc < 0) {
     vpt(-3023, "ERROR prop_unknown_loc returned an error likely related to fail to sort. \n");
   }
   if (prop_unknown_loc < 0) {
     vpt(0, "ERROR, We were looking for num = %ld.  unknown loc returned %ld.  Must be a problem with algo (num_unknown=%ld). \n", 
       (long int) num, (long int) prop_unknown_loc, (long int) dfl->num_unknown);
     int bad_errors = 0;
     printf("[");
     for (int ii = 0; ii < dfl->num_unknown;ii++) {
       printf("%d", dfl->ordered_unknown_fields[ii]); if  (ii < dfl->num_unknown-1) { printf(","); }
       if ((ii+1) % 25 == 0) { printf("\n"); }
       if ((ii < dfl->num_unknown-1) && (dfl->ordered_unknown_fields[ii] >= dfl->ordered_unknown_fields[ii+1])) {
         bad_errors++;
       }
     }
     printf("]\n");
     printf(" --- Okay errors are also %ld so we have bad order in the loop. \n", bad_errors); return(-30323); 
   }
   if (prop_unknown_loc ==  dfl->num_unknown) {
     dfl->ordered_unknown_fields[dfl->num_unknown] = num; 
     dfl->unknown_multiplicity[dfl->num_unknown] = multiplicity;
     dfl->unknown_usage_count[dfl->num_unknown] = 1; dfl->line_unknown[dfl->num_unknown] = iline;  dfl->num_unknown++; return(3);
   }
   if ((prop_unknown_loc >= 0) && (dfl->ordered_unknown_fields[prop_unknown_loc] == num)) {
      if (dfl->unknown_multiplicity[prop_unknown_loc] < multiplicity) { 
        dfl->unknown_multiplicity[prop_unknown_loc] = multiplicity; 
      }
      dfl->unknown_usage_count[prop_unknown_loc]++; return(4);
   }
   if (prop_unknown_loc < 0) { prop_unknown_loc = 0; }
   if (dfl->num_unknown >= dfl->alloc_unknown-1) {
     dfl->ordered_unknown_fields = realloc(dfl->ordered_unknown_fields, dfl->alloc_unknown*2);
     dfl->unknown_usage_count = realloc(dfl->unknown_usage_count, dfl->alloc_unknown*2);
     dfl->line_unknown = realloc(dfl->line_unknown, dfl->alloc_unknown*2);
     dfl->unknown_multiplicity = realloc(dfl->unknown_multiplicity, dfl->alloc_unknown*2);
     if ((dfl->ordered_unknown_fields == NULL) || (dfl->line_unknown == NULL) ||
         (dfl->unknown_usage_count==NULL)) {
       vpt(0, "ERROR, we tried to realloc but failed with dfl->num_unknown=%ld, alloc=%ld. \n",
         (long int) dfl->num_unknown, (long int) dfl->alloc_unknown);  return(-2);
     }
     for (int ii = dfl->alloc_unknown; ii < 2*dfl->alloc_unknown; ii++) {
       dfl->ordered_unknown_fields[ii] = -1;
       dfl->unknown_usage_count[ii] = -1;  dfl->line_unknown[ii] = -1;
       dfl->unknown_multiplicity[ii] = 0;
     }
     dfl->alloc_unknown *=2;
   }
   for (int jj = dfl->num_unknown-1; jj >= prop_unknown_loc; jj--) {
     dfl->ordered_unknown_fields[jj+1] = dfl->ordered_unknown_fields[jj];
     dfl->unknown_multiplicity[jj+1] = dfl->unknown_multiplicity[jj];
     dfl->line_unknown[jj+1] = dfl->line_unknown[jj]; dfl->unknown_usage_count[jj+1] = dfl->unknown_usage_count[jj];
   }
   dfl->ordered_unknown_fields[prop_unknown_loc] = num;
   dfl->line_unknown[prop_unknown_loc] = iline; dfl->unknown_usage_count[prop_unknown_loc] =1;
   dfl->unknown_multiplicity[prop_unknown_loc] = multiplicity;
   dfl->num_unknown++;
   for (int jj = 0; jj < dfl->num_unknown-1; jj++) {
     if (dfl->ordered_unknown_fields[jj] >= dfl->ordered_unknown_fields[jj+1]) {
       vpt(-10, " Error, we just tried to upgrade dfl->num_unknown=%ld, but for jj=%ld we have ouf[%ld]=%ld but ouf[%ld]=%ld. \n",
         (long int) dfl->num_unknown, (long int) jj, (long int) jj, (long int) dfl->ordered_unknown_fields[jj], 
         (long int) jj+1, (long int) dfl->ordered_unknown_fields[jj+1]); 
       printf("Insertion created an error ! \n"); return(-302);
     }
   }
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
  iKey = get_first_key("update_field_list_on_field", sf, st_fld, end_fld, verbose-1); 
  int num = 0;  int update_code;  int nKeys = 0; int nEntry = 1;
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
          endq - iKey-1, sf + iKey+1);   return(-103);
    }
    vpt(2, " on iKey=%ld, we determined num=%ld.  Add to field list. \n", (long int) iKey, (long int) num);

    iStr st_v, end_v;  st_v = get_value_bounds(stt, sf, iKey, end_fld, verbose-1, &st_v, &end_v);
    if ((st_v < 0) || (st_v >= end_fld)) {
      vpt(-1030, " ERROR we were update_field_list_on_field[num=%ld] but received st_v=%ld on a value bounds assessment. iKey=%ld/%ld.\n",
        (long int) num, (long int) st_v, (long int) iKey, end_fld);  return(-4032);
    }
    nEntry = 1;
    if (num == 18) {
      for (iStr ii = st_v; ii < end_v; ii++) {
        if ((sf[ii] == dfc->general_sep) || (sf[ii] == ' ')){nEntry++;}  // Count unique entries
      }
      vpt(-10, " --- NOTE WE did an 18 update_field_list_on_filed: nEntry = %ld for sf[%ld:%ld] = |%.*s|\n",
        (long int) nEntry, (long int) st_v, (long int) end_v, end_v-st_v, sf + st_v);
    } 
    update_code = add_to_field_list(dfl, num, iline, verbose-2, nEntry);
    nKeys++;
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

int get_multi_equals_bounds(char *sf, iStr st_eq, iStr nlen, iStr*p_st_v, iStr*p_end_v, char on_eq, char on_sp) {
 // Annoying case that Data looks like "18=1 2 A B 4=32"
 // Note technically, all multi entries should be single character.
 iStr onst = st_eq;  p_end_v[0] = st_eq; p_st_v[0] = st_eq;
 if (sf[onst] == on_eq) {
 } else if ((onst > 0) && (sf[onst-1] == on_eq)) {
  onst--;
 } else if ( ((onst +1) < nlen) && (sf[onst+1] == on_eq)) {  onst++;
 } else if ((sf[onst] >= '0') && (sf[onst] <= '9')) {
   for (onst=onst+1;onst < nlen;onst++) {
      if (sf[onst] == on_eq) { break; 
      } else if (sf[onst] == on_sp) {
        p_st_v[0] = -1; p_end_v[0] = -1; return(-1);
      } else if ((sf[onst] >= '0') && (sf[onst] <= '9')) {
      } else {
        p_st_v[0] = -1; p_end_v[0] = -1; return(-1);
      }
   }
 }
 p_st_v[0] = onst+1;
 iStr ii = onst+1;
 int nEntry = 0;
 if (sf[onst] == '\"') {
   // This case: "15"="9 A B 2 0 E",  There are number of spaces + 1 entries
   p_end_v[0] = get_end_quote("get_multi_equals_bounds", sf, onst, nlen);
   if ((p_end_v[0] < 0) || (p_end_v[0] >= nlen)) {
     printf("ERROR: multi_equals_bounds, we had sf[%ld] = \'%c\' but could not find bounding end. \n",
       onst, sf[onst]);
     p_st_v[0] = -1; p_end_v[0] = -1;  return(-1);
   }
   for(ii=onst+1; ii < p_end_v[0]; ii++) {
     if (sf[onst] == on_sp) { nEntry++; }
     return(nEntry+1);
   } 
 }
 for (; ii < nlen; ii++) {
   if (sf[ii] == on_sp) {  nEntry++;  p_end_v[0] = ii;
   } else if (sf[ii] == on_eq) { return(nEntry); // Means last number read is not an entry
   } else if (sf[ii] == '\n') { p_end_v[0] = ii; return(nEntry+1); }// else keep climbing
 }
 // If we reach end of line without an eq then all answers are valid and the last member is also an answer
 
 p_end_v[0] = nlen; return(nEntry + 1);
}
// "fix2end" is not a json format it is a separated format of fix fields going on to end of a line.
int update_field_list_on_fix2end(long int iline, int onfield, char*sf, iStr st_fld, iStr end_fld, DF_config_file *dfc, DF_field_list *dfl, int verbose) {
  char stt[200];
  sprintf(stt, "update_field_list_on_fix2end(ln=%ld,fld=%d,[%ld:%ld],v=%d,fe=\'%c\'): ",
    (long int) iline, (int) onfield, (long int) st_fld, (long int) end_fld, (int) verbose, dfc->schemas[onfield].fixequal);
  iStr orig_st_fld = st_fld;
  if ((sf[st_fld] == ' ') || (sf[st_fld] == dfc->general_sep)) {
    for (;st_fld < end_fld; st_fld++) { if((sf[st_fld]!=' ') && (sf[st_fld] != dfc->general_sep)) { break; } }
  }
  if ((sf[st_fld] == ' ') || (sf[st_fld] == dfc->general_sep))  {
     vpt(0, "ERROR, sf from %ld:%ld had no \'%c\' end. \n", orig_st_fld, st_fld, dfc->general_sep); 
     printf("---: %.*s\n", end_fld-st_fld+1, sf + st_fld);
     return(-1);
  }
  if (sf[end_fld-1] == '\n') { end_fld++; }
  int end_ln = end_fld; // So we can use NEXTCHAREQ()
  vpt(2, "We have field list inspecting = {%.%s}. \n", end_fld - st_fld - 1, sf + st_fld);
  iStr iKey = 0; int cnt_keys = 0;
  iStr ii = st_fld;  iStr end_l = end_fld;
  char on_char_eq = dfc->schemas[onfield].fixequal;
  iKey = ii; 
  
  int num = 0;  int update_code;  int nKeys = 0;  iStr endq;
  char on_char_sep = dfc->general_sep;
  int nEntries = -1;  iStr st_v, end_v;

  while((iKey > 0) && (iKey <= end_fld) && (sf[iKey] != '\n')) {
    if (sf[iKey] == '\"') {
      ii = get_end_quote("update_file_list_on_fix2end", sf, iKey, end_fld); endq = ii;
      sf[endq] = '\0'; num=atoi(sf + iKey+1); sf[endq] = '\"';
      NEXTCHAREQ();
    } else {
      NEXTCHAREQ(); endq = ii;
      char dmmy = sf[endq]; sf[endq]='\0'; num=atoi(sf+iKey); sf[endq] = dmmy;
    }
    if (num <= 0) {
      vpt(-1030, "ERROR, iline=%ld, iKey=%ld/%ld, onfield=%ld, st_fld=%ld, we received num=%ld for sf[%ld:%ld]=|%.*s|. sf[%ld:%ld]=|%.*s|\n",
        (long int) iline, (long int) iKey, (long int) end_fld, (long int) onfield, (long int) st_fld,
        (long int) num, (long int) iKey, (long int) end_fld, end_fld-iKey, sf+iKey, (long int) st_fld, (long int) end_fld,
        end_fld-st_fld, sf + st_fld);
      return(-4032034);
    }
    if (num == 18) {
      nEntries = get_multi_equals_bounds(sf, endq, end_fld, &st_v, &end_v, on_char_eq, on_char_sep);
      if ((nEntries < 0) || (st_v < 0)) {
        vpt(-10, "ERROR, we tried to get multi_equals_bounds for this entry for code %ld.  st_fld=%ld, end_fld=%ld, iKey=%ld, for %.*s.\n",
          (long int) num, (long int) st_fld, (long int) end_fld, (long int) iKey, end_fld-iKey, sf + iKey);
      }
      ii = end_v;
      if ((end_v > end_fld) || (end_v < st_v)) {
        printf("get_multi_equals_bounds::: ERROR, end_v=%ld, but end_fld=%ld, st_v=%ld. \n", (long int) end_v, (long int) end_fld, (long int) st_v);
        vpt(-10, " --  ERROR I guess we will report an error. \n");
        return(-180430);
      }
      if (verbose >= 2) {
        printf("UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU\n");
      }
      vpt(1, " NOTE update_field_list_on_fix2end: we got a num=18, and nEntries=%ld. sf[%ld:%ld] = |%.*s|\n", 
        (long int) nEntries, (long int) st_v, (long int) end_v, end_v-st_v, sf + st_v);
    } else {
      nEntries = 1;  NEXTCHARSEP();
    }
    vpt(1, "   --- We have key %ld found with multiplicity %ld. Total Keys=%ld.\n", (long int) iKey, (long int) nEntries, (long int) nKeys+1);
    if (num <= 0) {
      vpt(1, " --- We have zero working field lists. num is returned as %ld.\n", (long int) num);
      return(-403023);
    }
    update_code = add_to_field_list(dfl, num, iline, verbose+1, nEntries);
    if (update_code < 0) {  vpt(0, "ERROR, update_code=%d for adding num=%ld?  Why ? \n", (long int) update_code, (long int) num); return(-1); }
    if (update_code != 1) { vpt(1, "Note  we received an update_code of %ld for num=%ld. \n", (long int) update_code, (long int) num); }
    nKeys++;
    iKey =  ii;
  }
  if (verbose >=3 ) {
     vpt(3, "Finished iline=%ld, onfield=%ld.  Found %ld keys. \n", (long int) iline, (long int) onfield, (long int) nKeys);
     printf("--------------------------------------------------------------------------------------------------------------\n\n");
  }
  return(1);
}  
int update_field_list_on_line(long int iline, char*sf, iStr st_ln, iStr end_ln, DF_config_file *dfc, DF_field_list *dfl, int verbose) {
  int ii = st_ln;  
  int ion_schema = 0;
  char stt[300];
  sprintf(stt, "update_field_list_on_line(iline=%ld,v=%d,ns=%ld): ", (long int) iline, (int) verbose, (long int) dfc->n_schemas);
  vpt(2, "Here is complete line...\n%.*s\n", end_ln-st_ln, sf+st_ln);
  int attempt;
  char on_char_sep = dfl->char_sep;
  for (; ion_schema < dfc->n_schemas; ion_schema++) {
    if ((dfc->schemas[ion_schema].typ != fix42) && (dfc->schemas[ion_schema].typ != fix2end)) {
      //NEXTCOMMA();  We will have dfl has assigned character separation  I suppose ",|; \t" all reasonable separators
      NEXTCHARSEP();
    } else if (dfc->schemas[ion_schema].typ == fix2end) {
      iStr fieldStart = ii;  iStr fieldEnd = end_ln; ii = end_ln;
      ii++;
      vpt(1, "on is=%ld/%ld, we get fix2end to end line from [%ld:%ld]: \"%.*s...\" \n",
        (long int) ion_schema, (long int) dfc->n_schemas, (long int) fieldStart, (long int) fieldEnd,
        fieldEnd-fieldStart < 20 ? fieldEnd-fieldStart : 20, (char*) sf + fieldStart);
      attempt = update_field_list_on_fix2end(iline, ion_schema, sf, fieldStart, sf[fieldEnd] == '\n' ? fieldEnd : fieldEnd, dfc, dfl, verbose);
      if (attempt < 0) {
        printf("update_field_list_on_line(iline=%ld) ERROR, attempt=%d after update_field_list_on_fix2end,ion_schema=%ld/%ld \n",
          (long int) iline, (int) attempt, (long int) ion_schema, (long int) dfc->n_schemas);
        vpt(-1020, " ERROR fix2end attempt = %ld for schema=%ld/%ld on iline=%ld. [st,end]=[%ld,%ld] with fieldst/end=[%ld,%ld] for:\n",
          (long int) attempt, (long int) ion_schema, (long int) dfc->n_schemas, 
          (long int) iline, (long int) st_ln, (long int) end_ln, (long int) fieldStart, (long int) fieldEnd);
        printf("uflf2e ---: sf[fieldStart=%ld:fieldEnd=%ld] = |%.*s|\n", (long int) fieldStart, (long int) fieldEnd, 
          fieldEnd-fieldStart+1, sf+fieldStart);
        printf("uflf2e ---: sf[st_ln=%ld:end_ln=%ld] = |%.*s|\n", (long int)st_ln, (long int)end_ln, end_ln-st_ln, sf + st_ln);
        printf("uflf2e ---:  What went wrong? \n");  return(-1);
      }
      vpt(2, " --- Finished fix2end for i_s=%ld/%ld.  Attempt generated was %ld. \n", (long int) ion_schema, (long int) dfc->n_schemas, (long int) attempt);
      return(1);
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
      attempt = update_field_list_on_field(iline, ion_schema, sf, fieldStart, sf[fieldEnd] == '}' ? fieldEnd+1 : fieldEnd, dfc, dfl, verbose);
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
      // get_next_newln(...) we don't care about not getting a fill.
    } else if (sf[ii] == '[') { ii = get_end_bracket( (char*) stt,sf, ii, nmax); 
      // get_next_newln(...) we don't care about not getting a fill.
    }
  }
  return(nmax);
}
// Two types of next newln
iStr get_next_newln_force_end(char *sf, iStr st, iStr nmax, int verbose) {
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
// Basic flip through file beginning character by beginning character, hoping to see if target string exists in a line
int confirm_txt_exists(int lntxt, const char*seektxt, const char*sf, iStr st, iStr end) {
  iStr st_i;
  for (st_i = st; st_i < end-lntxt; st_i++) {
    int hit = 1;
    for (iStr onj = 0; onj < lntxt; onj++) {
      if (sf[st_i + onj] != seektxt[onj]) { hit = 0; break; }
    }
    if (hit == 1) { return(1); }
  }
  return(0);
}
DF_field_list *generate_field_list(char *tgt_filename, DF_config_file *dfc, char char_sep, int verbose, int standard_vector_size,
  long long int start_byte, long long int end_byte, char *ignore_line_text, char *keep_line_text) {
  char stt[500];
  sprintf(stt, "generate_file_list(\"%.*s\",v=%ld,nf=%ld): ", 40, tgt_filename, (long int) verbose, (long int) dfc->nfields); 
  vpt(1, "  -- Welcome I hope DFC is populated. \n");
  DF_field_list *dfl = create_blank_field_list(dfc, verbose-1);
  if (dfl == NULL) { vpt(0, " -- failed to allocate dfl at beginning. \n");  return(NULL); }

  //printf("--- generate_field_list --- No files!. \n");
  //return(dfl);

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
 
  int len_keep_line_text = 0;  int len_ignore_line_text = 0;
  if (keep_line_text != NULL) { len_keep_line_text = strlen(keep_line_text); }
  if (ignore_line_text != NULL) { len_ignore_line_text = strlen(ignore_line_text); }

  char buffer[MAXREAD+5];  int remainder= 0;
  long int bytesread; long int tbytesread = 0; int new_bytes;

  int num_reads = 0;
  bytesread = fread(buffer,sizeof(char),MAXREAD,fpo);   num_reads++;
  int onstr = 0; long int iline_prints=0;
  int ibuffreads = 1;
  int update_error;
  dfl->file_total_bytes = (long int) file_len;
  dfl->n_total_lines = 0;  dfl->n_total_all_lines = 0;
  dfl->char_sep = char_sep;
  dfl->line_locs[0] = onstr;  dfl->line_locs[1] = bytesread; 

  int do_line = 0;
  int num_since_new = 0;
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
      if ((iLineEnd < onstr) || (iLineEnd >= bytesread))  { break; }
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
      if ((len_keep_line_text > 0) && (len_ignore_line_text == 0)) {
        do_line = confirm_txt_exists(len_keep_line_text, keep_line_text, buffer, onstr, iLineEnd);
      } else if ((len_keep_line_text == 0) && (len_ignore_line_text > 0)) {
        do_line = 1- confirm_txt_exists(len_ignore_line_text, ignore_line_text, buffer, onstr, iLineEnd);
      } else if ((len_keep_line_text > 0) && (len_ignore_line_text > 0)) {
        do_line = confirm_txt_exists(len_keep_line_text, keep_line_text, buffer, onstr, iLineEnd);
        if (do_line) {
          do_line = 1- confirm_txt_exists(len_ignore_line_text, ignore_line_text, buffer, onstr, iLineEnd);
        }
      } else {
        do_line = 1;
      }
      if (do_line > 0) {
        if ((dfl->n_total_lines+1) % standard_vector_size == 0)  {
          dfl->n_loc_lines++;  dfl->line_locs[dfl->n_loc_lines+1] = tbytesread + bytesread;
        }
        update_error = update_field_list_on_line(dfl->n_total_lines, buffer, onstr, iLineEnd, dfc, dfl, verbose-2);
        if (update_error < 0) {
          printf("ERROR: generate_field_list(onstr=%ld, iLineEnd=%ld, n_total_lines=%ld, update_error=%ld. \n", (long int) onstr,
            (long int) iLineEnd, (long int) dfl->n_total_lines, (long int) update_error);
          printf(" -- gfl -- ERROR: on update with [onstr,iLineEnd]=[%ld,%ld] for |%.*s|\n",
           (long int) onstr, (long int) iLineEnd, iLineEnd-onstr +1, buffer+onstr);
          printf(" -- gfl -- ERROR  Note: num_reads=%d. num_since_new=%d last remainder was %d\n", (int) num_reads, (int) num_since_new, (int) remainder);
          printf(" -- gfl -- ERROR buffer[onstr=%ld:iLineEnd=%ld]= |%.*s| \n", 
            (long int) onstr, (long int) iLineEnd, iLineEnd-onstr, buffer+onstr);
          printf(" -- gfl -- ERROR Last remainder was %ld, bytesread = %ld, new_bytes=%ld. \n",
            (long int) remainder, (long int) bytesread, (long int) new_bytes);
          printf(" -- gfl -- ERROR Note that tbytesread = %ld. \n", (long int) tbytesread);
          printf(" -- gfl -- ERROR current readable line n_total_lines=%ld, whereas n_total_all_lines=%ld. \n", dfl->n_total_lines,
            (long int) dfl->n_total_all_lines);
          printf(" -- gf -- Note keep_line_text[%ld]=\"%s\", ignore_line_text[%ld]=\"%s\" \n",
            (long int) len_keep_line_text, keep_line_text==NULL ? "NULL" : keep_line_text,
            (long int) len_ignore_line_text, ignore_line_text==NULL ? "NULL" : ignore_line_text);
          dfl->finish = 0; rewind(fpo);
          fclose(fpo);  return(dfl);
        }
        dfl->n_total_lines++;  dfl->n_total_all_lines++;
      } else {
        dfl->n_total_all_lines++;
      }
      onstr = iLineEnd+1;  num_since_new++;
    }
    if (onstr < 0) {
      printf("ERROR: onstr = %ld on num_reads=%ld. \n", (long int) onstr, (long int) num_reads);
      rewind(fpo); fclose(fpo); delete_field_list(&dfl, verbose); return(NULL);
    }
    if (onstr < bytesread) {
       remainder = (bytesread-onstr);
       memcpy(buffer, buffer + onstr, sizeof(char)*remainder);
    } else {
       remainder = 0;
    }
    
    #ifdef DEBUG_MODE 
    if (verbose >= 2) {
      printf("\n");
      printf(" -- gfl --  num_reads=%ld: num_since_new=%d, onstr=%d, remainder=%ld, remaining string = |%.*s| \n", (long int) num_reads, 
         (int) num_since_new, (long int) onstr, (long int) remainder,
         remainder, buffer + onstr);
    }
    #endif
    tbytesread += bytesread-remainder;
    new_bytes = fread(buffer+remainder,sizeof(char),MAXREAD-remainder,fpo);  onstr = 0;  num_reads++;
    if (new_bytes+remainder > MAXREAD) {
      printf("ERROR: onstr=%ld, num_reads=%ld, we read MAXREAD[%ld]-remainder[%ld] bytes but got %ld bytes. \n",
        (long int) onstr, (long int) num_reads, (long int) MAXREAD, (long int) remainder, (long int) new_bytes);
      rewind(fpo); fclose(fpo); delete_field_list(&dfl, verbose); return(NULL);
    }
    #ifdef DEBUG_MODE
    int find_first_endl = 0;  while((find_first_endl < new_bytes + remainder) && (buffer[find_first_endl] != '\n')) { find_first_endl++; }
    if (find_first_endl >= new_bytes + remainder) {
        vpt(-100, " ERROR: DEBUG MODE hoping to jump to end: find_first_endl didn't find anything on %ld bytes read. \n", new_bytes);
        printf("BUFFER ADD = |%.*s|\n",  new_bytes, buffer+remainder);
        printf("BUFFER REM = |%.*s|\n",  remainder, buffer);
    } 
    if (verbose >= 2) {
      if (find_first_endl < new_bytes+remainder) {
        printf(" -- gfl -- after next fread, buffer[0:%d]=|%.*s| \n",
          200 > find_first_endl ? find_first_endl : 200,
          200 > find_first_endl ? find_first_endl : 200, buffer);
      } else {
        printf(" -- glf -- no first endl found. \n");
      }
      printf(" -- gfl -- after this fread, buffer[remainder=%d:%d] = |%.*s| \n",
        remainder, remainder+40 > find_first_endl ? find_first_endl : remainder+40,
        remainder+40 > find_first_endl ? find_first_endl - remainder : 40, buffer+remainder);
    }
    #endif
    bytesread = new_bytes + remainder; num_since_new = 0;
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
    if ((dfc->schemas[i_s].typ != fix42) && (dfc->schemas[i_s].typ != fix2end) && (dfc->schemas[i_s].priority == tgt_priority)) { return(i_s); } }
  return(-1);
}
int loc_lowest_priority_schema_gt(DF_config_file *dfc, int tgt, int verbose) {
  char stt[500];
  sprintf(stt, "lowest_priority_schema_gt(tgt=%ld, v=%d): ",
    (long int) tgt, (long int) verbose); 
  int i_s = -1; int loc_min = -1; int on_min = -1;
  for (i_s = 0; i_s < dfc->n_schemas; i_s++) {
    if ((dfc->schemas[i_s].priority >= 0) && (dfc->schemas[i_s].typ != fix2end) && (dfc->schemas[i_s].typ != fix42)){
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
  int on_final_o_loc = 0; int on_final_m_loc = 0;
  int n_on_f; int n_on_s;  
  int nkeep = 0;
  while ((p_on_s >= 0) || (p_on_f >= 0)) {
    vpt(2, " -- step %ld:  on_s=[%ld,p_=%ld], on_f=[%ld,p=%ld] start: o_loc=%ld, m_loc=%ld\n",
       (long int) on_final_o_loc, (long int) on_s, (long int) p_on_s,
       (long int) on_f, (long int) p_on_f, (long int) on_final_o_loc, (long int) on_final_m_loc);
    if ((on_f < 0) || ((on_s >= 0) && (p_on_s <= p_on_f))) {
      dfc->schemas[on_s].final_o_loc = on_final_o_loc;  
      dfc->schemas[on_s].final_m_loc = on_final_m_loc;  
      n_on_s = next_priority_schema(dfc, on_s, verbose-1);
      if (n_on_s == on_s) { vpt(-10, " ERROR: n_on_s=%ld, on_s=%ld. We didn't jump correct\n", (long int) n_on_s,
         (long int) on_s);  return(-10); }
      on_s = n_on_s;
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
      on_final_m_loc++;
    } else {
      dfl->final_known_print_loc[on_f] = on_final_o_loc; 
      dfl->final_known_multiplicity_loc[on_f] = on_final_m_loc;
      n_on_f = next_priority_fixfield(dfc, dfl, on_f, verbose-1);
      if (on_f == n_on_f) { 
        vpt(-10, "ERROR: n_on_f=%ld, on_f=%ld.  This doesn't jump. \n", (long int) n_on_f, on_f);
        return(-10);
      }
      nkeep = ((dfl->known_multiplicity[on_f] < dfc->fxs[on_f].maxmultiplicity) ? dfl->known_multiplicity[on_f] :
         dfc->fxs[on_f].maxmultiplicity);  nkeep = nkeep >= 1 ? nkeep : 1;
      on_f = n_on_f;
      p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
      on_final_m_loc += nkeep;
    }
    on_final_o_loc++;
    if (on_final_o_loc >= dfc->n_schemas + dfc->nfields+1) { 
      printf("ERROR ERROR ERROR ERROR ERROR ERROR ERROR \n");
      printf("cco-%s: ERROR we have on_final_o_loc=%ld, but n_schemas=%ld, nfields=%ld. \n",
        stt, (long int) on_final_o_loc, (long int) dfc->n_schemas, dfc->nfields);
      return(-1);
    }
  }  
  dfc->n_total_print_columns = on_final_o_loc;
  dfc->n_total_multiplicity_columns = on_final_m_loc;
  vpt(1, " ---- All concluded loop, on_final_o_loc=%ld, on_final_m_loc=%ld. \n", (long int) on_final_o_loc, (long int) on_final_m_loc);
  dfc->mark_visited = (int*) malloc(sizeof(int)*2*on_final_o_loc);
  if (dfc->mark_visited == NULL) {
    vpt(-1, "ERROR -trying to allocate dfc->mark_visited resulted in fail. \n");
    return(-104);
  }
  dfc->mark_m_visited = (int*) malloc(sizeof(int)*2*on_final_m_loc);
  if (dfc->mark_m_visited == NULL) {
    vpt(-1, "ERROR -trying to allocate dfc->mark_m_visited length %ld: resulted in fail. \n", (long int) 2 *on_final_m_loc); 
    free(dfc->mark_visited); dfc->mark_visited=NULL;
    return(-104);
  }
  vpt(1, " -- Now configure the mark_visited of length %ld (on_final_loc=%ld) \n", 
     (long int) 2*on_final_o_loc,(long int) on_final_o_loc);
  for (int ii = 0; ii < on_final_o_loc*2; ii++) { dfc->mark_visited[ii] = 0; }
  for (int ii = 0; ii < on_final_m_loc*2; ii++) { dfc->mark_m_visited[ii] = 0; }
  // repeat, lets mark mark_visited
  on_s = loc_lowest_priority_schema_gt(dfc, -1, verbose); 
  on_f = loc_lowest_priority_fixfield_gt(dfc,dfl, -1, verbose);
  p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
  p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
  if (verbose >= 2) { printf("cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco-cco\n"); }
  vpt(1, " ----- cco: We start again with on_s=[%ld,p=%ld], on_f=[%ld,p=%ld] \n", 
    (long int) on_s, (long int) p_on_s, (long int) on_f, (long int) p_on_f);
  int error_form = 0;
  on_final_o_loc = 0; on_final_m_loc = 0; 
  while ((p_on_s >= 0) || (p_on_f >= 0)) {
    vpt(2, " on_final_o_loc=%ld/%ld  with on_s=[%ld,p=%ld], on_f=[%ld,p=%ld], o_loc=%ld, m_loc=%ld \n",
      (long int) on_final_o_loc, (long int) dfc->n_total_print_columns,
      (long int) on_s, (long int) p_on_s, (long int) on_f, (long int) p_on_f,
      (long int) on_final_o_loc, (long int) on_final_m_loc);
    if ((on_f < 0) || ((on_s >= 0) && (p_on_s <= p_on_f))) {
      dfc->mark_visited[dfc->n_total_print_columns + on_final_o_loc] = on_s;
      dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_final_m_loc] = on_s;
      if (dfc->schemas[on_s].final_o_loc != on_final_o_loc) {
        vpt(-1, " ERROR on_final_o_loc=%d/%d,  (on_s=%ld/%ld p=%ld, on_f=%ld/%ld p=%d but schema[%d].final_o_loc=%d, final_m_loc=%ld \n",
          (int) on_final_o_loc, (int) dfc->n_total_print_columns, (long int) on_s, (long int) dfc->n_schemas, (int) p_on_s,
          (int) on_f, (int) dfl->n_known_fields,  (int) p_on_f,
          (int) on_s, (long int) dfc->schemas[on_s].final_o_loc, 
          (long int) dfc->schemas[on_s].final_m_loc);  error_form++; 
      } 
      on_final_m_loc++;
      on_s = next_priority_schema(dfc, on_s, verbose-1);
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -999;
    } else {
      dfc->mark_visited[dfc->n_total_print_columns + on_final_o_loc] = -on_f-1;
      if (dfl->final_known_print_loc[on_f] != on_final_o_loc) {
        vpt(-1, " ERROR on_final_o_loc=%d/%d,  (on_s=%ld/%ld p=%ld, on_f=%ld/%ld p=%d but field[%d].final_known_print_loc=%d, final_multiplicity_loc=%ld\n",
          (int) on_final_o_loc, (int) dfc->n_total_print_columns, (long int) on_s, (long int) dfc->n_schemas, (int) p_on_s,
          (int) on_f, (int) dfl->n_known_fields,  (int) p_on_f,
          (int) on_f, (long int) dfl->final_known_print_loc[on_f],
          (long int) dfl->final_known_multiplicity_loc[on_f]);  error_form++; 
      } 
      nkeep = dfl->known_multiplicity[on_f] < dfc->fxs[on_f].maxmultiplicity ? dfl->known_multiplicity[on_f] : dfc->fxs[on_f].maxmultiplicity;
      nkeep = nkeep >= 1 ? nkeep : 1;
      if (nkeep <= 1) {
        dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_final_m_loc] = -on_f-1;
        on_final_m_loc++;
      } else {
        for (int ipp = 0; ipp < nkeep; ipp++) {
          dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_final_m_loc] = -on_f-1;  on_final_m_loc++;
        }
      }
      on_f = next_priority_fixfield(dfc, dfl, on_f, verbose-1);
      p_on_f =  ((on_f >= 0) && (on_f < dfc->nfields)) ? dfc->fxs[on_f].priority : -999;
    }
    on_final_o_loc++;
    if (on_final_o_loc > dfc->n_total_print_columns) { 
      printf("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR\n");
      printf("cco-%s: ERROR we have on_final_o_loc=%ld, but n_schemas=%ld, n_fields=%ld. n_total_print_columns=%ld we exceeded!\n",
        stt, (long int) on_final_o_loc, (long int) dfc->n_schemas, dfc->nfields, (long int) dfc->n_total_print_columns);
      free(dfc->mark_visited); dfc->mark_visited = NULL;
      return(-1);
    }
  } 
  if (dfc->n_total_print_columns > dfc->n_total_multiplicity_columns) {
    printf("ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR ERROR\n");
    printf("ERROR cco-%s: We have n_total_print_columns=%ld, but n_total_multiplciity_columns = %ld.  \n",
     stt, (long int) dfc->n_total_print_columns,  (long int) dfc->n_total_multiplicity_columns);
    printf("ERROR --- A fail. \n");
    free(dfc->mark_visited); dfc->mark_visited=NULL; return(-1);
  } 
  vpt(1, " We have concluded with on_final_o_loc=%ld, versus on_final_m_loc=%ld. \n", (long int) on_final_o_loc, (long int) on_final_m_loc);
  if (error_form > 0) {
    vpt(-1, "Calculating mark_visited failed. \n"); return(-10);
  }
  vpt(0, "Mark Visited returns all correct. \n");
  return(1);
} 
int clear_m_visited(DF_config_file *dfc) {
  if (dfc == NULL) { return(-10); } 
  if ((dfc->mark_m_visited == 0) || (dfc->n_total_multiplicity_columns <= 0)) { return(-5); }
  for (int ii = 0; ii < dfc->n_total_multiplicity_columns;ii++) {
    dfc->mark_m_visited[ii] = 0;
  }
  for (int ii = 0; ii < dfc->n_total_print_columns; ii++) {
    dfc->mark_visited[ii] = 0;
  }
  dfc->n_read_cols = 0;
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
  int on_pt = 0; int on_mt = 0;
  printf(" --- Printing target table order [%ld columns] (start with S[%ld:%ld],F[%ld:%ld])\n", dfc->n_total_print_columns,
    (long int) on_s, (long int) p_on_s, (long int) on_f, (long int) p_on_f);
  printf(" -- mark_m_visited = [\n"); 
  for (on_mt = 0; on_mt < dfc->n_total_multiplicity_columns; on_mt++) {
    printf("    (%d,%d)", (int) on_mt, (int) dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_mt]);
    if (on_mt < dfc->n_total_multiplicity_columns -1) { printf(",\n"); } else {printf("\n"); }
    //on_mt +=  (dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_mt] >= 0) ? 1 :
    //            ((dfc->fxs[-1-dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_mt]].maxmultiplicity <= 
    //             dfl->known_multiplicity[-1 -dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_mt]]) ?
    //             dfc->fxs[-1-dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_mt]].maxmultiplicity :
    //             dfl->known_multiplicity[-1 -dfc->mark_m_visited[dfc->n_total_multiplicity_columns + on_mt]]);
  }
  printf("]\n");
  printf("[    ");
  on_pt = 0; on_mt;
  while ((on_s >= 0) || (on_f >= 0)) {
    if ((on_f < 0) || ((on_s >= 0) && (p_on_s <= p_on_f))) {
      printf("[%d=(o_loc=%d,m_loc=%ld):on_s=%d=%d,Sch:\"%s\",%s,",
        on_pt, (int) dfc->schemas[on_s].final_o_loc, (int) dfc->schemas[on_s].final_m_loc, (int) on_s, 
        (int) dfc->mark_visited[dfc->n_total_print_columns + on_pt],
        dfc->schemas[on_s].nm, What_DF_DataType(dfc->schemas[on_s].typ));
      if ((dfc->schemas[on_s].typ == tms) || (dfc->schemas[on_s].typ == tus) || (dfc->schemas[on_s].typ == tns)) {
        printf("%s,", What_DF_TSType(dfc->schemas[on_s].fmttyp));
      }
      printf("priority=%d,fl=%d[m=%d]]", dfc->schemas[on_s].priority, 
        (int) dfc->schemas[on_s].final_o_loc, (int) dfc->schemas[on_s].final_m_loc);
      on_s = next_priority_schema(dfc, on_s, verbose-1);
      p_on_s =  ((on_s >= 0) && (on_s < dfc->n_schemas)) ? dfc->schemas[on_s].priority : -1;
    } else {
      printf("[%d=%d[m=%ld]:on_f=%d=%d,FF=%ld,",
        (int) on_pt, (int) dfl->final_known_print_loc[on_f],  (int) dfl->final_known_multiplicity_loc[on_f],
        (int) on_f, (int) dfc->mark_visited[dfc->n_total_print_columns + on_pt],
        (long int) dfl->ordered_known_fields[on_f]);
      printf("\"%s\",", (char*) dfc->fxs[on_f].fixtitle);
      printf("%s,", What_DF_DataType((dfc->fxs[on_f].typ)));
      if ((dfc->fxs[on_f].typ == tms) || (dfc->fxs[on_f].typ == tus) || (dfc->fxs[on_f].typ == tns)) {
        printf("%s,", What_DF_TSType(dfc->fxs[on_f].fmttyp));
      }
      printf("priority=%d,m_col_loc=%ld]", (int) dfc->fxs[on_f].priority,
         dfl->final_known_multiplicity_loc[on_f]);
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

#define itest(nnm, nm,ln,instr) \
  iokf = test_replace_intv(&(nm), (ln), (instr), verbose); \
  nnm = nm; nm = NULL

int test_replace_field_list(DF_config_file *dfc, DF_field_list **p_dfl, int verbose) {
  char stt[300];
  DF_field_list *dfl = p_dfl[0];
  if (p_dfl[0] == NULL) {
    printf("test_replace_field_list, field List given is already null. \n"); return(-10430);
  }
  sprintf(stt, "test_replace_field_list(known=%ld, unknown=%ld): ", 
    (long int) dfl->n_known_fields, (long int) dfl->num_unknown);
  vpt(1, "  START \n");
  if (p_dfl[0] == NULL) {
    vpt(1, " ERROR, pdfl[0] is already NULL!\n");
  }
  vpt(1, " Creating a nulled field list. \n");
  DF_field_list *ndfl = create_nulled_field_list(dfc, verbose);
  vpt(1, " ndfl created. \n");
  ndfl->char_sep = dfl->char_sep;  ndfl->n_known_fields = dfl->n_known_fields;
  int iokf;
  vpt(1, " beginning replacement and testing of all of the integer vectors. \n");
  itest(ndfl->ordered_known_fields, dfl->ordered_known_fields, dfl->n_known_fields, "ordered_known_fields");
  itest(ndfl->known_usage_count, dfl->known_usage_count, dfl->n_known_fields, "known_usage_count");
  if (dfl->known_multiplicity != NULL) {
    itest(ndfl->known_multiplicity, dfl->known_multiplicity, dfl->n_known_fields, "known_multiplicity");
  }
  if (dfl->final_known_print_loc != NULL) {
    itest(ndfl->final_known_print_loc, dfl->final_known_print_loc, dfl->n_known_fields, "final_known_print_loc");
  }
  if (dfl->final_known_multiplicity_loc != NULL) {
    itest(ndfl->final_known_multiplicity_loc, dfl->final_known_multiplicity_loc, 
      dfl->n_known_fields, "final_known_multiplicity_loc");
  }
  ndfl->alloc_unknown = dfl->alloc_unknown;  ndfl->num_unknown = dfl->num_unknown;
  itest(ndfl->ordered_unknown_fields, dfl->ordered_unknown_fields, dfl->alloc_unknown, "ordered_unknown_fields");
  itest(ndfl->unknown_usage_count, dfl->unknown_usage_count, dfl->alloc_unknown, "unknown_usage_count");
  itest(ndfl->unknown_multiplicity, dfl->unknown_multiplicity, dfl->alloc_unknown, "unknown_multiplicity");
  itest(ndfl->line_unknown, dfl->line_unknown, dfl->alloc_unknown, "line_unknown");
  ndfl->num_used_known_fields = dfl->num_used_known_fields;

  ndfl->file_total_bytes = dfl->file_total_bytes; ndfl->finish = dfl->finish;
  ndfl->standard_vector_size = dfl->standard_vector_size;
  ndfl->n_total_lines = dfl->n_total_lines;  ndfl->n_total_all_lines = dfl->n_total_all_lines;
  ndfl->alloc_line_loc = dfl->alloc_line_loc; ndfl->n_loc_lines = dfl->n_loc_lines;
  if (ndfl->line_locs != NULL) {
    itest( ndfl->line_locs, dfl->line_locs, dfl->alloc_line_loc, "line_locs");
  }
  vpt(1, "test_replace_field_list --- We are done with line_locs deleted. \n");
  delete_field_list(p_dfl, verbose);
  if (ndfl->ordered_known_fields == NULL) {
    printf("ERROR test_replace_field_list  error after that work, dfl->ordered_known_fields is null. \n");
    return(-403203);
  }
  p_dfl[0] = ndfl;
  return(1); 
}
