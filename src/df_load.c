#ifndef DUCKFIXLOADH
#include "include/df_load.h"
#define DUCKFIXLOADH 0
#endif

#include <stdlib.h>

int delete_DF_Schema(DF_Schema *dfs, int verbose) {
  vprintf(2, "Deleting schema %s. \n", dfs->nm != NULL ? dfs->nm : "UNKNOWN");
  if (dfs->nm != NULL) {
    free(dfs->nm); dfs->nm = NULL;
  }
  if (dfs->desc != NULL) { free(dfs->desc); dfs->desc = NULL; }
  return(1);
}

iStr get_str_l(char *name_str, char *ast, iStr nmax) {
  for (iStr ii = 0; ii < nmax; ii++) {
    if (ast[ii] == '\0') {
      return(ii); 
    } 
  }
  printf("get_str_l: ERROR String %s longer than nmax=%ld\n",
     name_str, (long int) nmax);
  return(-1);
}
iStr get_end_bracket(char *name_str, char *ast, iStr on_i, iStr nmax) {
  if (ast[on_i] != '[') {
    printf("get_end_bracket(%s), on_i=%ld, but it is not bracket. \n", 
      name_str, (long int) on_i);
  }
  iStr ii; iStr jj;
  for (ii = on_i + 1; ii < nmax; ii++) {
    if (ast[ii] == ']') { return(ii); }
    jj = ii;
    if (ast[ii] == '[') { jj = get_end_bracket(name_str, ast, ii,nmax) ;  
    } else if (ast[ii] == '{') { jj = get_end_brace(name_str,ast,ii,nmax);
    } else if (ast[ii] == '\"') { jj = get_end_quote(name_str,ast,ii,nmax);
    }
    if (jj < ii) { 
      printf("get_end_bracket: must have ended on an error, ast[ii=%ld]=\'%c\', jj = %ld.\n", ii, ast[ii], ast[ii]); return(jj);
    } else { ii = jj; }
  }
  printf("ERROR: get_end_bracket(%s) no next bracket at nmax=%ld. \n", name_str, nmax);
  return(-1);
}

// Note we got rid of recursion to stop dealing with broken case of a "{ [ } ]"
//    There should be no stack breakdown on 
//      "{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}"
//    However, technically, that really shouldn't happen on documents we encounter.
iStr get_end_brace(char *name_str, char *ast, iStr on_i, iStr nmax) {
  if (ast[on_i] != '{') {
    printf("get_end_brace(%s), on_i=%ld, but it is not brace. \n", name_str, (long int) on_i);
  }
  iStr ii; iStr jj;
  int max_jumps = 0;
  int n_braces = 1; int n_brackets = 0;
  for (ii = on_i + 1; ii < nmax; ii++) {
    jj = ii;
    if (ast[ii] == '}') { 
      if (n_braces == 1)  {
        if (n_brackets  > 0) { return( -104); }
        return(ii);
      } else {
        n_braces--;
      }
    } else if (ast[ii] == ']') {
       if (n_brackets <= 0) { return(-105); }
       n_brackets--;
    } else if (ast[ii] == '{') {
       n_braces++;
    } else if (ast[ii] == '[') {
       n_brackets++;
    } else if (ast[ii] == '\"') {
      jj = get_end_quote(name_str, ast, ii, nmax); 
      if (jj < ii) {
        printf("get_end_brace: must have ended on an error ast[ii=%ld]=\'%c\', jj=%ld.\n", (long int) ii, (char) ast[ii], (long int) jj);
        return(-101); 
      }
      ii = jj;
    }
  }
  printf("ERROR: get_end_brace(%s), started at on_i=%ld, no next brace at ii=%ld =nmax=%ld. n_braces=%ld, n_brackets=%ld\n", 
    name_str, (long int) on_i, (long int) ii, (long int) nmax, (long int) n_braces, (long int) n_brackets);
  printf(" We had looked for \"%.*s...\".\n",  (nmax-on_i) < 40 ? (nmax-on_i) : 40, ast + on_i);
  printf(" End of string is \"...%.*s\".\n",  (nmax-on_i) < 40 ? (nmax-on_i) : 40, ast + nmax - ((nmax-on_i) < 40 ? (nmax-on_i) : 40));
  return(-1);
}

// Our Challenge Strings will be semi
int old_str_eq(const char *mst, const char *challengestr, iStr lencstr) {
  //strncmp(mst, challengestr, lenmst);
  for (int ii = 0; ii < lencstr; ii++) {
    if (mst[ii] == '\0') {  return(-1-ii); }
    if (mst[ii] != challengestr[ii]) { return(-1-ii); }
  }
  if (mst[lencstr] == '\0') { return(0); }
  return(-lencstr);
}

//
iStr get_end_quote(char* vstr, char *in_str, iStr on_i, iStr nmax) {
  char stt[] = "get_end_quote";
  if (in_str[0] == '\0') {
    printf("get_end_quote(%s): Error, only length 0.\n", (char*) vstr); return(0);
  }
  iStr ii,jj;
  if (in_str[on_i] != '\"') {
    printf("get_end_quote(%s): ERROR this does not start with quote. \'%c\' at %ld/%ld\n",
      vstr, (char) in_str[on_i], (long int) on_i, (long int) nmax); return(-43);
  }
  for (ii = on_i+1; ii < nmax; ii++) {
    if (in_str[ii] == '\0') {
      printf("get_end_quote(%s) error reached end at ii=%ld though nlen = %ld. \n",
        vstr, (long int) ii, (long int) nmax);
    } else if (in_str[ii] == '\"') {
      //printf(" -- get_end_quote on ii=%ld/%ld, we find quote mark, with in_str[%d] = \'%c\'\n", ii, nmax, ii-1, in_str[ii-1]);
      if (in_str[ii-1] != '\\') { return(ii); 
      } else if (ii==2) {
        // We know this string was "\"... 
      } else {
        for (jj = ii-2; jj > 0; jj--) {
          if (in_str[jj] != '\\') {
            //  "  \\\"" 
            //  this means even number from ii to jj is a end quote
            if ( (ii-jj) %2 == 1) { return(ii);} else { 
             jj=-1; 
             //printf("get_end_quote, fail to find quote at ii=%ld/%ld. \n", ii, nmax);
            }
          }
        }
      }
    }
  }
  printf("get_end_quote(%s): ERROR on quote starting on_i=%ld.  We reached ii=%ld/%ld and never found end quote\n", vstr, (long int) on_i, ii, nmax);
  printf("get_end_quote(%s): Offending string is: %.*s. \n",vstr, nmax-on_i, in_str + on_i);
  return(-1);
}

///////////////////////////////////////////////////////////////////////
//  load_file_to_str()
//    
//    Lazy approach to read entire JSON configuration file in at once.
//    Our hard part is constructing the necessary read structures to properly
//    Process an arbitrarily complex part-FIX log.
//
//    Approach: Just load whole json into string, out_p_sf and return length
iStr load_file_to_str(char **out_p_sf, char *json_filename, int verbose) {
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
  int sflen = ( (long int) sizeof(char)) * ((long int) (file_len+4));
  sf = (char*) malloc( (size_t) sflen); 
  if (sf == NULL) { out_p_sf[0] = NULL; return(-1); }
  size_t byte_count = fread(sf, sizeof(char), file_len, fpo);
  if (ferror(fpo) != 0) {
    vpt(0, "ERROR trying to read file into buffer. \n");
    free(sf); sf= NULL; fclose(fpo);  return(-3);
  }
  rewind(fpo);
  fclose(fpo);
  fpo = NULL;
  sf[file_len] = '\0'; sf[file_len+1] = '\0'; sf[file_len+2] = '\0';
  out_p_sf[0] = sf; sf = NULL;  return(file_len);
  return(file_len);
}
// Perhaps overkill to have to jump to next key.
iStr get_next_key(char* assignment, char *sf, iStr on_i, iStr nmax, int verbose) {
  iStr jj; iStr end_key;
  char stt[300];  
  sprintf(stt, "get_next_key(%.*s):", 200, (char*) assignment);
  vpt(3, "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk\n");
  vpt(3, "kkk %s initiate, starting at st=%ld, with sf[%ld/%ld] = \'%c\'.  sf[%ld:%ld]=\"%.*s...\"\n", 
     (char*) stt, (long int) on_i, (long int) on_i, (long int) nmax, sf[on_i],
     (long int) on_i + 1,  (long int) on_i+21 > nmax ? nmax : on_i + 21,
     (on_i + 20 > nmax) ? nmax - on_i - 1 : on_i + 21 - (on_i+1), sf+ on_i + 1
  );
  iStr st = on_i;  iStr ii = st;
  if (sf[ii] == '{') {
    ii++;  PUSH_OUT_WHITE();
    if (sf[ii] = '\"') { return(ii); 
    } else { printf("kkk: %s we failed because we started on { but did not find a quote next. \n", stt); return(-1); }
  }
  if (sf[ii] == '\"') {
  } else {
    PUSH_TO_CHAR_WO( ("[get \'\"\']"), ('\"'));
  }
  if (sf[ii] != '\"') {
    printf("get_next_key, ii=%ld, we somehow did not reach to a quote, fail. st=%ld,nmax=%ld, sf[%ld:%ld]=%.*s\n", 
      (long int) ii, (long int) st, (long int) nmax,
      (long int) st, (long int) nmax, nmax-st, sf + st);
  }
  iStr st_key = ii;
  jj = get_end_quote("get_next_key", sf, ii, nmax);
  vpt(3, "We found that current key was sf[%ld:%ld] or --%.*s--\n",
    ii+1, (long int) jj, jj - ii - 1, sf + ii+1);
  if (jj < ii) { vpt(0, "ISSUE failed to find current key. jj=%ld versus st=%ld/%ld.\n", (long int) jj, ii, nmax); }
  if (sf[jj] != '\"') { vpt(0, "ERROR get_next_key, we got end quote but sf[jj=%ld] = \'%c\'. \n", (long int) jj, sf[jj]); }
  end_key = jj;  st = jj+1; ii = jj+1;
  PUSH_OUT_WHITE();
  if (ii >= nmax) { vpt(0, "kkk ERROR(%s): attempt to get next key ended because ii=%ld/%ld. \n", stt, (long int) ii, (long int) nmax); }
  if (sf[ii] != ':') {
    vpt(0,"kkk after key %.*s we did not find \':\' we went to: %.*s\n",
      end_key-st_key -1, sf + st_key+1, ii - st_key-1, sf+st_key+1); return(-2);
  }
  vpt(3,"kkk we found our colon at %ld. sf[%ld]=\'%c\'\n",
     sf[ii], ii, sf[ii]); 
  ii = ii+1;
  iStr end_value = -1; iStr start_value =-1;
  PUSH_OUT_WHITE();
  if ((ii >= nmax) || (ii < 0)) {
    vpt(0,"kkk ERROR(%s), on value search: we pushed out white but ii=%ld/%ld or bad! \n", stt, (long int) ii, (long int) nmax);  return(-5);
  } else if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {
    vpt(0,"kkk ERROR(%s), on value search: we pushed out white but sf[ii=%ld] = \'%c\' bad! \n", stt, (long int) ii, sf[ii]); return(-6);
  }
  vpt(2, ", after colon ii=%ld (st=%ld,max=%ld), and we have sf[ii=%ld]=\'%c\'\n",
    (long int) ii, (long int) st, (long int) nmax, (long int) ii, sf[ii]);
  if ((sf[ii] == '\"') || (sf[ii] == '{') || (sf[ii] =='[')) {
    start_value = ii;
    if (sf[ii] == '\"') {  jj = get_end_quote(stt, sf, ii, nmax); 
    } else if (sf[ii] == '{') {  jj = get_end_brace(stt, sf, ii, nmax); 
    } else if (sf[ii] == '[') {  jj = get_end_bracket(stt, sf, ii, nmax); 
    } 
    if (jj < ii) {
      vpt(0," ERROR, value location sf[ii=%ld]=\'%c\' but search led to jj=%ld/%ld. \n",
        (long int) ii, sf[ii], (long int) jj, nmax); return(-1);
    }
    end_value = jj; ii = jj+1;
  } else if (((sf[ii] >= '0') && (sf[ii] <= '9')) || (sf[ii] = '.')) {
    vpt(1, "  we are looking for an end number with sf[ii=%ld]=\'%c\'\n",  (long int) ii, sf[ii]);
    jj = get_end_number((char*) stt, sf, ii, nmax);  start_value = ii; 
    if (jj < ii) {
      vpt(0," ERROR, sf[ii=%ld]=\'%c\' but search led to jj=%ld/%ld. \n",
         (long int) ii, sf[ii], (long int) jj, nmax); return(-2);
    }
    end_value = jj;ii=jj;
  } else {
    printf("%s ERROR, looking at value sf[ii=%ld]= \'%c\' and no qualified next step. \n",
      (char*) stt, (long int) ii, sf[ii]); return(-4);
  }
  if (start_value < 0) {
    printf("ERROR ERROR ERROR get_next_key(%s) -- End, super error, we have start_value=%ld, this is invalid. \n", stt, (long int) start_value);
    printf("ERROR: get_next_key() note that we were supposed to find a key after colon but sf[st=%ld:nmax=%ld]=\n\"\"\"\n%.*s\n   \"\"\"\n",
      (long int) st, (long int) nmax, nmax-st, sf + st);  
    printf("ERROR ERROR ERROR, I wonder why start value was not good. \n");
    return(-10);
  }
  vpt(3,"kkk - we apparently started with start_value=%ld and found value at sf[%ld:%ld]=--%.*s--. \n",
    start_value, start_value+1, (long int) end_value, end_value-start_value-1, sf + start_value+1);
  // Next key will be after a comma
  vpt(3,"kkk next search ii = %ld/%ld: \'%c\'\n", (long int) ii, (long int) nmax, sf[ii]);
  PUSH_OUT_WHITE();
  if ((ii >= nmax)) {
    vpt(0, "ERROR, reached ii=%ld/%ld after whitespace removal but we didn't read a '}' at end. \n", (long int) ii, (long int) nmax);
    printf(" note that sf[(nmax-1)=%ld] = \'%c\' \n", (long int) nmax-1, sf[nmax-1]);
    if (sf[nmax-1] == '}') { printf("WEIRD: alternative exists. \n"); return(nmax-1); }
    return(nmax+1);
  }
  if (sf[ii] == '}') { return(ii); }  // End of the road here no next value.
  if (sf[ii] != ',') {
    vpt(1, " ERROR after white push out, we are at sf[ii=%ld]=\'%c\'. \n", (long int) ii, sf[ii]);
  }

  vpt(3,"kkk we are at sf[ii=%ld]=\'%c\' our comma, we go on now. \n", (long int) ii, sf[ii]);
  iStr start_next_set = ii; ii = ii+1;
  PUSH_OUT_WHITE();
  if (ii >= nmax) {
    vpt(0,"ERROR -- get_next_key(%s): Error after finding \',\' at start_next_set=%ld we could not get past white space. \n", 
      assignment, (long int) start_next_set);  return(-2);
  } else if (sf[ii] == '}') {  
    return(ii);
  } else if (sf[ii] != '\"') {
    printf("ERROR -- get_next_key(): Error after finding \',\' at start_next_set=%ld. \n   --- Expected key start '\"' but fail. \n",
      (long int) start_next_set);
    printf("   ---  We could not reach \'\"\' marker sf[%ld:%ld]=\n\"\"\"%.*s\n    \"\"\". \n", 
      (long int) start_next_set, (long int) ii+1, ii+1 - on_i, sf + on_i); 
    if (((sf[ii] >= 'a') && (sf[ii] <= 'z')) || ((sf[ii]>='A') && (sf[ii] <= 'Z'))) {
      printf("ERROR -- Aparently sf[ii=%ld]=\'%c\', we think a quotation mark is missing here. \n",
        (long int) ii, sf[ii]);
    }
    return(-3);
  }
  if (sf[ii] == '\"') { return(ii); }
  vpt(3, "issue, we worked all the way to nmax=%ld breutut did not find a new key. sf[%ld:%ld] = %.*s\n", 
        (long int) nmax,
        (long int) st, (long int) nmax, nmax-st, sf + st);
  return(nmax);
}

int get_next_comma(char* assignment, char *sf, iStr on_i, iStr nmax) {
  // Complex way to seek next comma, despite potential Brace/Bracket/Quote/Number elements in between
  char stt[300];
  sprintf(stt, "get_next_comma(%s): ", (char*) assignment);
  iStr ii = on_i; iStr jj; int verbose =0;
  for (; ii < nmax; ii++) {
    PUSH_OUT_WHITE();
    if (ii >= nmax) { 
      printf("%s:: PUSH_OUT_WHITE ERROR on_i=%ld, sf[on_i=%ld:%ld/%ld] = \"%.*s\" \n", 
        stt, (long int) on_i, (long int) on_i, (long int) nmax, (long int) nmax, nmax-on_i, sf + on_i);
      return(-2);
    } else if ((sf[ii] == ' ')  || (sf[ii] == '\t') || (sf[ii] == '\n')) {
      printf("%s:: PUSH_OUT_WHITE ERROR on_i=%ld, sf[on_i=%ld:%ld] = \"%.*s\" \n", 
        stt, (long int) on_i, (long int) on_i, (long int) ii, ii+1-on_i, sf + on_i);
      return(-2);
    }
    jj = ii;
    if (sf[ii] == '[') {
      jj = get_end_bracket("get_next_comma", sf, ii, nmax);
    } else if (sf[ii] == '{') {
      jj = get_end_brace("get_next_comma", sf, ii, nmax);
    } else if (sf[ii] == '\"') {
      jj = get_end_quote("get_next_comma", sf, ii, nmax);
    } else if (sf[ii] == ':') {
      // Next Comma might be after a colon in a dictionary
    } else if ((sf[ii] == '.') || ((sf[ii] >= '0') && (sf[ii] <= '9'))) {
      jj = get_end_number("get_next_comma", sf, ii, nmax);
    } else if (sf[ii] == ',') {
      return(ii);
    }
    if (jj < ii) {
      printf("get_next_comma(%s): we got error jj=%ld/%ld after ii=%ld. \n", assignment, (long int) jj, (long int) nmax, (long int) ii);
      return(-3);
    }
    ii = jj+1;
  }
  return(nmax);
}
int get_next_array_value(char *assignment, char*sf, iStr on_i, iStr nmax, DF_Json_Type *pjtype) {
  iStr ii;
  int cnt_comma = 0;
  for (ii = on_i; ii < nmax; ii++) {
    if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {
    } else if (sf[ii] == ',') {
      if (cnt_comma >= 1) {
        printf("ERROR: %s, we had too many commas in a row after on_i=%ld \n", assignment, (long int) on_i);
        printf("%s\n", sf + on_i);
        return(-1);
      } else { cnt_comma++; }
    } else if (sf[ii] == ']') {
      return(0);
    } else if (sf[ii] == '\"') {
      pjtype[0] = JSON_TYPE_STR; return(ii);
    } else if (sf[ii] == '{') {
      pjtype[0] = JSON_TYPE_DICT; return(ii);
    } else if (sf[ii] == '[') {
      pjtype[0] = JSON_TYPE_ARRAY; return(ii);
    } else if ((sf[ii] >= '0') && (sf[ii] <= '9')) {
      // potentially a naked number
      pjtype[0] = JSON_TYPE_NUMBER;
      return(ii);
    }
  }
  return(0);
}
iStr get_end_number(char *prstr, char* sf, iStr on_i, iStr nmax) {
  if (((sf[on_i] < '0') || (sf[on_i] > '9'))  && (sf[on_i] != '.') && (sf[on_i] != '-')) {
    printf("ERROR: %s we had sf[%ld] is not number start. is %c \n", prstr, on_i, sf[on_i]);
  }
  int nDot = sf[on_i] == '.' ? 1 : 0;  iStr ii;
  for (ii = on_i+1; ii < nmax; ii++) {
    if ((sf[ii] == '\n') || (sf[ii] == ' ') || (sf[ii] == '\t')) {
      return(ii);
    } else if ((sf[ii] == ',') || (sf[ii] == ']') || (sf[ii] == '}')) {
      return(ii);
    } else if ((sf[ii] >= '0') && (sf[ii] <= '9')) {
    } else if (sf[ii] == '.') {
      if (nDot >= 1) {
        printf("ERROR: get_end_number[%s] on ii = %ld we get second dot. \n", prstr, (long int) ii);
        return(-1);
      }
      nDot++;
    } else {
      printf("ERROR: get_end_number[%s] on ii=%ld, oni=%ld we get bad end to number = %c\n",
        prstr, (long int) ii, (long int) on_i, sf[ii]);  return(-1);
    }
  }
  printf(" ERROR: get_end_number[%s] with ii=%ld, we reached end of list. %ld \n", prstr, (long int) ii, (long int) nmax);
  return(-1);
}
int get_dict_next_value(char *assignment, char *sf, iStr on_i, 
  iStr nmax, iStr *p_st, iStr *p_end, DF_Json_Type *pjtype, int verbose) {
  iStr ii; iStr jj;
  iStr st_key; iStr end_key;
  int cnt_comma = 0;
  p_st[0] = -1; p_end[0] = -1;
  for (ii = on_i; ii < nmax; ii++) {
    if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {
    } else if (sf[ii] == '\"') { break; 
    } else if (sf[ii] == ',') {
      if (cnt_comma >= 1) {
        printf("ERROR: %s, we had too many commas in a row after on_i=%ld \n", assignment, (long int) on_i);
        printf("%s\n", sf + on_i);
        return(-1);
      } else { cnt_comma++; }
    } else if (sf[ii] == '}') {
      return(0);
    }
  }
  end_key = get_next_key(assignment, sf, ii, nmax, verbose); 
  if (end_key < ii) {
    printf("get_next_dict_value(%s), we got end_key=%ld,ii=%ld we consider get_next_key failed. \n",
      assignment, (long int) end_key, (long int) ii);  return(-3);
  }
  if (end_key >= nmax) {
    printf("get_next_dict_value(%s), we got end_key=%ld,ii=%ld we consider get_next_key failed. we had nmax=%ld\n",
      assignment, (long int) end_key, (long int) ii, (long int) nmax);  return(-3);
  }
  if (sf[end_key] == '}') {
    // No way to get to next value.  we are already at "end" of dictionary
    return(end_key);
  }
  p_st[0] = -1; p_end[0] = -1;
  for (ii = end_key + 1; ii < nmax; ii++) {
    if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {
      
    } else if (sf[ii] == ':') {
      break;
    } else {
      printf("get_next_value: %s, we failed to get next value after on_i=%ld.\n",
        sf, (long int) on_i);
    }
  }
  for (;ii < nmax;ii++) {
    if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii] == '\n')) {

    } else if (sf[ii] == '[') {
      p_st[0] = ii; pjtype[0] = JSON_TYPE_ARRAY;
      p_end[0] = get_end_bracket("get_next_value", sf, ii, nmax);
      break;
    } else if (sf[ii] == '{') {
      p_st[0] = ii; pjtype[0] = JSON_TYPE_DICT;
      p_end[0] = get_end_brace("get_next_value", sf, ii, nmax);
      break;
    } else if (sf[ii] == '\"') {
      p_st[0] = ii;  pjtype[0] = JSON_TYPE_STR;
      p_end[0] = get_end_quote("get_next_value", sf, ii, nmax);
      break;
    } else if ((sf[ii] >= '0') && (sf[ii] <= '9')) {
      p_st[0] = ii;  pjtype[0] = JSON_TYPE_NUMBER;
      p_end[0] = get_end_number("get_next_dict_value", sf, ii, nmax); // end of number is not a ", it could be a ","
      break;
    } 
  }
  if ((p_st[0] < nmax) && (p_st[0] >= 0) && (p_end[0] < nmax) && (p_end[0] >0)) {
    return(1);
  }
  return(-1);
}

// Need to do fixed
int str_eq(const char *fix_str, iStr len_fix_str, char *buff, iStr buff_st, iStr buff_end) {
  if (buff_end <= buff_st) {
    printf("str_eq:  fix_str[0:%ld] = %.*s, we have that the length given is negative, buff_st=%ld, buff_end=%ld. \n",
      (long int) len_fix_str, len_fix_str, fix_str, (long int) buff_st, (long int) buff_end);  return(0);
  }
  //printf("str_eq test: fix_str[0:%ld]=%.*s, buff[%ld:%ld] = %.*s, len_fix=%ld, len_buff_str=%ld. strncmp is %ld. \n",
  //  len_fix_str, len_fix_str, fix_str, (long int) (buff_st), (long int) (buff_end), buff_end-buff_st, buff + buff_st, len_fix_str,  buff_end-buff_st,
  //  (long int) (strncmp(fix_str, buff+buff_st, len_fix_str)) );
  if (buff_end-buff_st != len_fix_str) { return(0); }
  if (strncmp(fix_str, buff+buff_st, len_fix_str) != 0) { return(0); }
  return(1);
}
int correct_brace(char *sf, iStr *p_st, iStr *p_end, iStr nmax) {
  if ( ((sf[p_st[0]] == '{')||(sf[p_st[0]] == '\"')) && (sf[p_end[0]-1] == '}')) { return(1); }
  iStr o_st = p_st[0]; iStr o_end = p_end[0];
  if ((sf[nmax-1] != '\0') && (sf[nmax] != '\0')) { nmax++; } // Safe to move up if we aren't on end char yet of sf.
  while ( (p_st[0] < p_end[0]) && ((sf[p_st[0]] == ' ') || (sf[p_st[0]] == '\t') || (sf[p_st[0]] == '\n')) ) {
    p_st[0]++;
  }
  if ( (sf[p_st[0]] =='{') || (sf[p_st[0]] == '\"')) {
  } else {
    printf("correct_brace error, sf[p_st[0]=%ld] = \'%c\', sf[o_st=%ld,end=%ld] = (\'%c\',\'%c%c\') \n",
      (long int) p_st[0], sf[p_st[0]], (long int) o_st, (long int) o_end, sf[o_st], sf[o_end-1], o_end < nmax ? sf[o_end] : 'X');
    return(-1);
  }
  if ((p_end[0] < nmax) && (sf[p_end[0]] == '}')) { p_end[0]++; return(2); }
  if ((p_end[0] < nmax+1) && (sf[p_end[0]+1] == '}')) { p_end[0] +=2; return(3); }
  iStr on_st = p_st[0];  int cBracket = 1;
  if (sf[on_st] != '{') {
    while((on_st >= 0) && (sf[on_st] != '{') && (cBracket > 1)) {
      if (sf[on_st] == '}') { cBracket++; } else if (sf[on_st] =='{') {cBracket--; }
      on_st--;
    }
  }
  // Note this may never work becasue nested quotes and such will make things bad
  if ((on_st < 0) || (sf[on_st] != '{')) {
    printf("correct_brace error, sf[p_st[0]=%ld] = \'%c\', sf[o_st=%ld,end=%ld] = (\'%c\',\'%c%c\') we failed to solve for on_st with a \'{\' characher \n",
      (long int) p_st[0], sf[p_st[0]], (long int) o_st, (long int) o_end, sf[o_st], sf[o_end-1], o_end < nmax ? sf[o_end] : 'X', (long int) on_st,
      sf[on_st]);  return(2);
  }
  p_end[0] = get_end_brace("correct_brace", sf, on_st, nmax);
  if (p_end[0] < 0) {
    printf("correct_brace error, sf[p_st[0]=%ld] = \'%c\', sf[o_st=%ld,end=%ld] = (\'%c\',\'%c%c\') algthough sf[on_st=%ld]=%c we got p_end[0] = \'%ld\' \n",
      (long int) p_st[0], sf[p_st[0]], (long int) o_st, (long int) o_end, sf[o_st], sf[o_end-1], o_end < nmax ? sf[o_end] : 'X', (long int) on_st,
      sf[on_st], p_end[0]);
    return(-3);
  }
  if (sf[p_end[0]] == '}') { p_end[0]++; return(1); }
  if (sf[p_end[0]-1] == '}') { return(2); }
  printf("correct_brace() somehow p_end[0] = %ld but sf[%ld] = \'%c\', what gives? \n", (long int) p_end[0], (long int) p_end[0], sf[p_end[0]]);
  return(-2);
}
iStr find_key(const char *seek_key, iStr len_key, char*sf, iStr st, iStr nmax, int verbose) {
  char stt[300];
  if (verbose >= 2) {
    printf("find_key -- initiate. %s. \n", seek_key == NULL ? "SEEK KEY IS NULL" : "SEEK KEY IS NOT NULL");
  }
  if (verbose >= 3) {
    printf("find_key -- printing to stt. \n");
  }
  sprintf(stt,"find_key[%.*s,st=%ld,%ld,\'%c:%c\']::", (len_key<200?len_key:200), seek_key, (long int) st, (long int) nmax, sf[st], sf[nmax-1]);
  vpt(2, "find_key -- printed length key %ld/%ld to stt sf[st=%ld, nmax-1=%ld]=[%c,%c] \n",
      (len_key<200?len_key:200), len_key, (long int) st, (long int) nmax-1, sf[st], sf[nmax-1]);
  if (sf == NULL) { printf("%s, null string provided. \n",stt); return(-1); }
  int bc = correct_brace(sf, &st, &nmax, (iStr) nmax+0);
  if (bc <= 0) {
    vpt(0, "ERROR: find key, because bc=%ld, we don't have a chance. [%ld,%ld]\n", (long int) bc, (long int) st, (long int) nmax);
    return(-1);
  } 
  if (sf[nmax-1] != '}') {
    vpt(0, "ERROR: find key, even though bc=%ld, we didn't solve for sf[nmax-1=%ld] = \'%c\' we wanted \'}\'\n",
      (long int) bc, (long int) nmax-1, sf[nmax-1]); return(-2);
  }

  sprintf(stt,"find_key[%.*s,st=%ld,%ld,\'%c:%c\']::", (len_key<200?len_key:200), seek_key, (long int) st, (long int) nmax, sf[st], sf[nmax-1]);
  vpt(2, ":: Begin with st=%ld/%ld. \n", (long int) st, (long int) nmax);
  vpt(2, "  We begin, st=%ld/%ld on sf[%ld]=\'%c\'. \n", (long int) st, (long int) nmax, (long int) st, sf[st]);
  if ((sf[st] != '{') && (sf[st] != '\"')) { printf("%s, Issue, sf does not start with open bracket, starts with \'%c\', probably won't work. \n", stt, sf[st]); return(-2); }
  iStr ii, jj; iStr next_key = -1;
  ii = sf[st] == '\"' ? st : get_next_key(stt, sf, st, nmax, verbose-1); 
  int count_keys = 0;
  while ((ii >= 0) && (ii < nmax)) {
    if (ii < 0) {
      vpt(0, " ERROR ERROR find_key, ii now is %ld. \n", (long int) ii);
    }
    if (sf[ii] != '\"') {
      vpt(0, "ERROR: find_key, count_keys=%ld, ii=%ld/%ld, sf[ii=%ld] = \'%c\'  Not valid look. \n", (long int) count_keys,
        (long int) ii, (long int) nmax, (long int) ii, sf[ii]);  return(-2);
    } 
    jj = get_end_quote("find_key", sf, ii, nmax);
    if (jj < ii) { 
        printf("find_key, on ii=%ld/%ld, sf[%ld] = '%c' we did not find end quote. \n", (long int) ii, (long int) nmax, 
          (long int) ii, sf[ii]);  return(-1);
    }
    vpt(2, "find_key: (ii=%ld/%ld) Current key goes from sf[%ld:%ld]=\"%.*s\" \n",  (long int) ii, (long int) nmax, 
         (long int) ii+1, (long int) jj, jj - ii - 1, sf + ii+1);
    if (str_eq(seek_key, (iStr) len_key, sf, ii+1, jj)) {
        return(ii);
    } 
    vpt(2, "on sf[%ld]=\'%c\' for [ii,jj]=[%ld,%ld]/%ld, sf[%ld:%ld] = %.*s.  \n --- But not equal to key=%.*s, sf[ii=%ld]=\'%c\'moving to grab next key.\n",
            (long int) ii, sf[ii], 
            (long int) ii, (long int) jj, (long int) nmax, (long int) ii+1, (long int) jj,  
            (int) (jj-ii-1), sf + ii + 1,
            (int) len_key, (char*) seek_key, (long int) ii, sf[ii]);
    PUSH_OUT_WHITE();
    if ((ii >= nmax) || (sf[ii] == '}')) {
      return(-1);
    }
    next_key = get_next_key(stt, sf, ii, nmax, verbose-1);
    if ((next_key < ii) || (next_key >= nmax)) {
       vpt(0, "ERROR:: trying to find key after ii=%ld. sf[ii=%ld:%ld] = %.*s\n",
             (long int) ii, (long int) ii, (long int) nmax, nmax-ii, sf + ii); 
       printf(" Note we were asked to find key \"%.*s\" inside: \n\"\"\"\n%.*s\n   \"\"\"\n",
             len_key, seek_key, nmax-st, sf + st);
       return(-34032);
    } else if ((next_key < nmax) && (sf[next_key] == '}')) {
           vpt(1, ", At End point having run get_next_key, returned no more keys after ii=%ld. sf[%ld:%ld]=%.*s\n",
             (long int) ii, (long int) ii, (long int) next_key, next_key-ii+1, sf + ii);  return(next_key);
    }  
    ii = next_key; count_keys++; 
    if (count_keys  >= 10000) {
      vpt(0, " ERROR count_keys=%ld, this seems like too many. \n", (long int) count_keys);  return(-1);
    }
  } 
  return(-1);
}
iStr get_inside_value_bounds(char *assignment, char *sf, iStr key_loc, iStr nmax, int verbose, iStr *p_vst, iStr *p_vend) {
  // Note we might write this someday, but we went with a different
  // manual checkup to look at CSV values, which will largely have to be located in correctly formed JSON
  return(1);
}
iStr get_value_bounds(char* assignment, char*sf, iStr key_loc, iStr nmax, int verbose, iStr *p_vst, iStr *p_vend) {
  char stt[300];
  sprintf(stt, "get_value(%ld):", (long int) key_loc);
  iStr ii = key_loc;
  ii = get_end_quote(assignment, sf, key_loc, nmax);
  ii++;
  PUSH_OUT_WHITE();
  if (sf[ii] != ':') { printf("get_value: didn't find colon. \n"); return(-1); }
  ii++;
  PUSH_OUT_WHITE();
  if (sf[ii] == '\"') {
    p_vst[0] = ii;
    p_vend[0] = get_end_quote(assignment, sf, ii, nmax);
    return(ii);
  }
  if (sf[ii] == '{') {
    p_vst[0] = ii;
    p_vend[0] = get_end_brace(assignment, sf, ii, nmax);
    return(ii);
  }

  if (sf[ii] == '[') {
    p_vst[0] = ii;
    p_vend[0] = get_end_bracket(assignment, sf, ii, nmax);
    return(ii);
  }
  if ((sf[ii]=='-') || (sf[ii] == '.')  || ((sf[ii] >= '0') && (sf[ii] <= '9'))){
    p_vst[0] = ii;
    p_vend[0] = get_end_number(assignment, sf, ii, nmax);
    return(ii);
  }
  iStr endq = get_end_quote("get_value_bounds_error", sf, key_loc, nmax);
  if ((endq >= nmax) || (endq < key_loc)) { endq = key_loc+1; }
  printf("get_value_bounds: Failed to find a value: key_loc=%ld:(\"%.*s\"), ", 
    (long int) key_loc, key_loc-endq-1, sf + key_loc+1
  );
  printf("searched sf[%ld:%ld] :\n\"\"\"\n%.*s\n\"\"\" \n",
     (long int) key_loc, (long int) nmax, nmax - key_loc, sf + key_loc);
  return(-1); 
}
DF_Schema *create_blank_schemas(int n_schemas) {
  DF_Schema *dfs = malloc(sizeof(DF_Schema) * n_schemas);
  if (dfs == NULL) {
    printf("create_blank_schemas: failed to create %ld schemas. \n", (long int) n_schemas); 
    return(NULL); 
  }
  for (int ii = 0; ii < n_schemas; ii++) {
    dfs[ii].nm = NULL; dfs[ii].typ = UNKNOWN; dfs[ii].desc = NULL;
    dfs[ii].timestamp_format = NULL;  dfs[ii].priority = -1;
    dfs[ii].width = (char) 0; dfs[ii].scale = (char) 0;  dfs[ii].fmttyp=UNKNOWN_TS;
  }
  return(dfs);
}
int delete_schemas(int n_schemas, DF_Schema **p_schemas, int verbose) {
  char stt[300];
  sprintf(stt, "delete_schemas(n=%ld): ", n_schemas);
  vpt(2, " initiate. \n");
  DF_Schema *dfs = p_schemas[0];
  for (int ii = 0; ii < n_schemas; ii++) {
    vpt(2, " ii=%ld/%ld: working on nm/desc/timestamp. \n", (long int) ii, (long int) n_schemas);
    if (dfs[ii].nm != NULL)  { vpt(1, " ii=%ld/%ld: nm is not null. \n", (long int) ii, (long int) n_schemas); free(dfs[ii].nm); dfs[ii].nm = NULL; }
    if (dfs[ii].desc != NULL)  { free(dfs[ii].desc); dfs[ii].desc = NULL; }
    if (dfs[ii].timestamp_format != NULL) { 
      vpt(1, " ii=%ld/%ld: timestamp_format adjust, freeing now. \n", (long int) ii, (long int) n_schemas); 
      free(dfs[ii].timestamp_format); dfs[ii].timestamp_format = NULL; 
    }
  }
  vpt(2, " -- Now free dfs. \n");
  free(dfs);  p_schemas[0] = NULL;  return(1);
}
DF_config_file *new_config_file() {
  DF_config_file *dfc = malloc(sizeof(DF_config_file));
  if (dfc == NULL) {  printf("ERROR: DF_config file, failed to allocate dfc. \n");  return(NULL); }
  dfc->schemas = NULL; dfc->name = NULL; dfc->info = NULL; dfc->desc = NULL; dfc->nfields = 0;
  dfc->fxs = NULL;  dfc->mark_visited=NULL;
  return(dfc);
  //for (int ii = 0; ii < NCHARS; ii++) { field_loc[ii]=-1; } 
}
int delete_config_file(DF_config_file **pdfc, int verbose) {
  char stt[300];
  sprintf(stt, "delete_config_file(): ");
  vpt(2, "  Initiate. \n"); 
  DF_config_file *dfc = pdfc[0];
  if (dfc->name != NULL) { vpt(3, " delete name because non null. \n"); free(dfc->name); dfc->name = NULL; }
  if (dfc->info != NULL) { vpt(3, " delete info because non null. \n"); free(dfc->info); dfc->info = NULL; }
  if (dfc->desc != NULL) { vpt(3, " delete desc becaue non null. \n"); free(dfc->desc); dfc->desc = NULL; }
  if (dfc->ordered_fields != NULL) { vpt(3, "delete ordered_fields. \n"); free(dfc->ordered_fields); dfc->ordered_fields=NULL; }
  if (dfc->mark_visited != NULL) { vpt(3, "delete mark_visited. \n"); free(dfc->mark_visited); dfc->mark_visited=NULL; }
  if (dfc->schemas != NULL) {
    vpt(2, " calling delete_schemas. n=%ld\n", dfc->n_schemas);
    delete_schemas(dfc->n_schemas, &dfc->schemas, verbose-1);
  }
  free(dfc);  pdfc[0] = NULL; return(1);
}
DF_config_file *get_config_file(char *sf, iStr on_i, iStr nmax, int verbose) {
  char stt[300]; 
  sprintf(stt, "get_config_file(n=%ld,v=%ld): ", (long int) nmax, (long int) verbose);
  if (verbose >= 1) { printf("------------------------------------------------------------------------------------\n"); }
  vpt(1, " --- Initiate. \n");
  int n_schemas = get_n_schema("get_config_file", sf, 0, nmax, 0);
  vpt(1, " --- We received n_schemas = %ld. \n", n_schemas);
  if (n_schemas <= 0) {
    printf("get_config_file: ERROR no schema, no config file. \n"); return(NULL);
  }
  if ((sf[on_i] == ' ') ||  (sf[on_i] == '\n') || (sf[on_i] == '\t')) {
   for (;on_i < nmax; on_i++) {
     if (sf[on_i] == '{') { break;
     } else if ((sf[on_i] == ' ' )  || (sf[on_i] == '\t') || (sf[on_i] == '\n')) {
     } else {
       vpt(0, " ERROR on start of sf[on_i=%ld] = \'%c\' \n", (long int) on_i, sf[on_i]); return(NULL);
     }
   }
  }
  int bc = correct_brace(sf, &on_i, &nmax, nmax+0);
  if (bc < 0) {
    vpt(0, " ERROR, get_config_file returned bc=%ld on correct brace, error, on_i=%ld, nmax=%ld \n", (long int) bc, (long int) on_i, (long int) nmax);
    return(NULL);
  }
  DF_config_file *dfc = new_config_file();
  if (dfc == NULL) {  printf("ERROR: DF_config file, failed to allocate dfc. \n");  return(NULL); }
  iStr vst, vend;
  vpt(3, " -- Searching for initial whole config \"name\".  \n");
  iStr iName = find_key("name",4,sf,on_i,nmax, verbose-1);
  if (iName < 0) { printf("ERROR: note sf[nmax-1] = \'%c\' \n", (long int) nmax); }
  if (iName >= 0)  {
    vst = get_value_bounds("get_config_file", sf, iName, nmax, verbose-2, &vst, &vend);
    vpt(3, " -- We found name at iName=%ld, 1+vend-vst=%ld, sf[vst=%ld:vend=%ld]: thus: \"%.*s\" \n", 
      (long int) iName, (long int) 1 + vend -vst, (long int) vst, (long int) vend,
      vend-vst-1, sf + vst + 1);
    if (vst >= 0) {
      dfc->name = malloc(sizeof(char)*(1+vend-vst));
      if (dfc->name != NULL) { sprintf(dfc->name,"%.*s\0", vend-vst-1, sf + vst + 1); 
      } else { printf("%s: FAILED to allocate name. \n", stt); delete_config_file(&dfc, verbose); return(NULL); }
    }
  }
  
  vpt(3, " -- Searching for initial whole config \"description\". \n");
  iStr iDesc = find_key("description",11,sf,on_i,nmax,0);
  if (iDesc >= 0)  {
    vst = get_value_bounds("get_config_file", sf, iDesc, nmax, verbose-2, &vst, &vend);
    vpt(3, " --- success finding \"description\" at %ld/%ld with value sf[vst+1=%ld:vend=%ld]=\"%.*s\". \n",
      (long int) iDesc, (long int) nmax, (long int) vst + 1, (long int) vend,
      vend - vst -1, sf + vst + 1);
    if (vst >= 0) {
      dfc->desc = malloc(sizeof(char)*(1+vend-vst));
      if (dfc->desc != NULL) { sprintf(dfc->desc,"%.*s\0", vend-vst-1, sf + vst + 1); 
      } else { printf("%s: FAILED to allocate desc. \n", stt); delete_config_file(&dfc, verbose); return(NULL); } 
    }
  } else {
    vpt(3, "Note: no key \"desc\" was found in sf starting at on_i=%ld/%ld. iDesc=%ld \n", (long int) on_i, (long int) nmax, iDesc);
    
  }

  vpt(3, " -- Searching for initial whole config \"info\". \n");
  iStr iInfo = find_key("info",4,sf,on_i,nmax,0);
  if (iInfo >= 0)  {
    vst = get_value_bounds("get_config_file", sf, iInfo, nmax, verbose-2, &vst, &vend);
    if (vst >= 0) {
      dfc->info = malloc(sizeof(char)*(1+vend-vst));
      if (dfc->info != NULL) { sprintf(dfc->info,"%.*s\0", vend-vst-1, sf + vst + 1); 
      } else { printf("%s: FAILED to allocate info. \n", stt); delete_config_file(&dfc, verbose-1); return(NULL); } 
    }
  }
  iStr iSchema = find_key("schema",6,sf,on_i,nmax, 0);
  if (iSchema < 0) {
    vpt(0, " ERROR we do not find iSchema=%ld/%ld in sf from [%ld:%ld] \n",
      iSchema, nmax, on_i, nmax);
    vpt(0, " --- Let's return without doing this. \n");
    return(dfc);
  }
  //vpt(1, " TEMP End, we found iSchema at %ld, and we have n_schemas = %ld, however we will avoid right now and return early.\n",
  //  iSchema, n_schemas);
  //dfc->n_schemas = 0;
  //return(dfc);
  vst = get_value_bounds("get_config_file", sf, iSchema, nmax, verbose-2, &vst, &vend);
  if ((vst < 0) || (vend < vst)) {
    vpt(0, " ERROR we found iSchema at %ld/%ld, but vst/vend are [%ld/%ld] so failure to get value bounds. \n",
      (long int) iSchema, (long int) nmax, (long int) vst, (long int) vend);
  }
  vpt(2, "-- Success finding iSchema = %ld/%ld, value bounds vst/vend=[%ld/%ld] \n -- with sf[%ld:%ld...%ld:%ld]=\"\"\"\n%.*s...%.*s\n    \"\"\". \n",
    (long int) iSchema, (long int) nmax, (long int) vst, (long int) vend,
     (long int) vst + 1, (long int) (vst + 1 + (.5*(vend - vst-1) < 15 ? .5 * (vend-vst-1) : 15)),
     (long int) (vend - ( .5 * (vend-vst-1) < 15 ? .5 * (vend-vst-1) : 15)), (long int) vend,
     (int) (.5*(vend-vst-1) < 15 ? .5 *(vend-vst-1) : 15), sf + vst + 1, 
     (int) (.5*(vend-vst-1) < 15 ? .5 *(vend-vst-1) : 15), sf + ((int) ( vend - (.5*(vend-vst-1) < 15 ? .5 * (vend-vst-1) : 15)))
  );
  dfc->schemas = create_blank_schemas( n_schemas);   dfc->n_schemas = n_schemas;
  if (dfc->schemas == NULL) { printf("%s: FAILED to allocate schemas \n", stt); 
    delete_config_file(&dfc, verbose); return(NULL);
  }
  iStr oniS = vst;
  oniS = get_next_key("get_config_file", sf, oniS,vend, verbose-1);
  vpt(3, "------------------------------------------------------------------------------- \n");
  vpt(3, " -- Initiate Schema (iSchema=%ld/%ld)  for %ld schemas, read starting at oniS=%ld, sf[oniS=%ld:%ld] = \"%.*s...\"\n",
    (long int) iSchema, (long int) nmax, (long int) n_schemas, (long int) oniS, oniS, oniS + (nmax-oniS < 15 ? nmax-oniS: 15),
    (nmax-oniS < 15 ? nmax-oniS: 15), sf + oniS);
  for (int i_s = 0; i_s < n_schemas; i_s++) {
    iStr st0, end0;
    st0 = oniS;  end0 = get_end_quote("get_config_file", sf, st0, vend);
    if ((st0 < 0) || (end0 < st0)) {
      vpt(0, " ERROR we have i_s=%ld/%ld oniS=%ld,  for name search but st0/end0 = [%ld,%ld] \n", (long int) i_s, (long int) n_schemas,
        (long int) oniS, (long int) st0, (long int) end0);
      return(dfc);
    }
    vpt(2, " -- We are on i_s=%d/%d starting at oniS=%ld, (vst,vend)=[%ld,%ld], nm is sf[%ld:%ld] = \"%.*s\" \n",
      (int) i_s, (int) n_schemas, 
      (long int) oniS, (long int) vst, (long int) vend, (long int) st0+1, (long int) end0, end0-st0-1, sf + st0+1); 
    dfc->schemas[i_s].fmttyp = UNKNOWN_TS;
    dfc->schemas[i_s].nm = malloc(sizeof(char) * (end0-st0+1));
    sprintf(dfc->schemas[i_s].nm, "%.*s\0", end0-st0-1, sf + st0 + 1);
    iStr st_V, end_V;
    st_V = get_value_bounds("get_config_file", sf, oniS, nmax, verbose, &st_V, &end_V);
    if (st_V < 0) {
      vpt(0, " ERROR we found i_s=%d/%d: oniS=%ld, end0=%ld, but [st_V,end_V]=[%ld,%ld].\n",
       (int) i_s, (int) n_schemas, (long int) oniS, (long int) end0, (long int) st_V, (long int) end_V);
      return(dfc);
    } else {
      vpt(2, " Success finding st_V, end_V for i_s=%ld/%ld, oniS=%ld/%ld, we have sf[st_V=%ld,end_V+1=%ld] = \"%.*s\"\n",
          (long int) i_s, (long int) nmax, (long int) oniS, (long int) nmax, (long int) st_V, (long int) end_V+1,
          (long int) end_V+1-st_V, sf + st_V);
    }
    vpt(3, ": i_s=%d/%d Looking for \"desc\" where oniS=%ld/%ld but values from [%ld:%ld]. \n", 
      (int) i_s, (int) nmax, (long int) oniS,
      (long int) nmax, (long int) st_V, (long int) end_V);
    iStr desc_loc = find_key("desc",4,sf,st_V, end_V+1, 0);
    if (desc_loc < 0) {
      vpt(0, ": ERROR i_s=%ld/%ld, although we found nm at sf[oniS+1=%ld,end0=%ld] = \"%.*s\". No desc_located between [%ld,%ld].\n",
        (int) i_s, (int) n_schemas, (long int) oniS+1, (long int) end0, end0-oniS-1, sf+oniS + 1,  
        (long int) st_V, (long int) end_V);
      desc_loc = find_key("desc",4,sf,st_V, end_V+1, 3);
      vpt(0, ": ERROR i_s=%ld/%ld, although we found nm at sf[oniS+1=%ld,end0=%ld] = \"%.*s\". No desc_located between [%ld,%ld].\n",
        (int) i_s, (int) n_schemas, (long int) oniS+1, (long int) end0, end0-oniS-1, sf+oniS + 1,  
        (long int) st_V, (long int) end_V);
      return(dfc);
    }
    if (desc_loc >= 0) { 
      vpt(3, ": Found desc_loc=%ld, we will copy it's value into desc field of schmea. \n", desc_loc);
      copy_in_val_str("get_config_file", sf, desc_loc, nmax, verbose-2, &( (dfc->schemas[i_s]).desc)); 
      vpt(3, ": Success copying desc_loc=%ld value into i_s=%ld/%ld schema. \n", (long int) desc_loc,
        (long int) i_s, (long int) n_schemas);
    }
    iStr priority_loc = find_key("priority", 8, sf, st_V, end_V+1,0);
    if ((priority_loc  < st_V) || (priority_loc >= end_V)) {
      dfc->schemas[i_s].priority=-1;
    } else {
      st0 = get_value_bounds("get_config_file", sf, priority_loc, nmax, verbose-2, &st0, &end0);
      int numpriority = -1;  sf[sf[end0]=='\"' ? end0 : end0+1] = '\0'; 
      numpriority = atoi(sf+st0); sf[sf[end0]=='\0' ? end0: end0+1] = '\"';
      dfc->schemas[i_s].priority = numpriority;
    }
    iStr typ_loc = find_key("typ",3,sf,st_V,end_V+1,0);
    if (typ_loc < 0) {
      vpt(0, "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT\n");
      vpt(0, "TTT i_s = %ld/%ld, oniS=%ld/%ld failed to find typ in sf[%ld:%ld] \n", (long int) i_s, (long int) n_schemas, 
        (long int) oniS, (long int) nmax, (long int) st_V, (long int) end_V); 
      vpt(0, "TTT Trying again: \n");
      typ_loc = find_key("typ",3,sf,st_V,end_V+1,0); 
      if (typ_loc < 0) {
        vpt(0, "ERROR TTT: We don't have it typ_loc=%ld, for oniS=%ld/%ld, we looked between st_V/end_V = [%ld,%ld]\n",
          (long int) typ_loc, (long int) oniS, (long int) nmax, (long int) st_V, (long int) end_V);
      }
      return(dfc);
    }
    if (typ_loc >= 0) {
        st0 = get_value_bounds("get_config_file", sf, typ_loc, nmax, verbose-2, &st0, &end0);
        vpt(5, "Hey, for typ_loc =%ld, we got st0/end0 = sf[%ld+1:%ld]=\"%.*s\"\n",
           (long int) typ_loc, (long int) st0, (long int) end0, end0 - st0-1, sf + st0 + 1);
        vpt(5, " Does it equal i32 ? %ld .\n",
           (long int) str_eq("i32",3,sf, st0+1, end0));
        vpt(5, " Does it equal str ? %d .\n",
           (long int) str_eq("str",3,sf, st0+1, end0));
        vpt(5, " Does it equal tms ? %d .\n",
           (long int) str_eq("tms",3,sf, st0+1, end0));
        vpt(5, " Does it equal fix42 ? %d .\n",
           (long int) str_eq("fix42",5,sf, st0+1, end0));
        dfc->schemas[i_s].typ = MATCHTYPE(sf, st0, end0); 
        vpt(3, " After finding the data we have a type \"%s\" or int = %ld, we read \"%.*s\"\n", 
          What_DF_DataType(dfc->schemas[i_s].typ), (int) dfc->schemas[i_s].typ,
          end0-st0-1, sf + st0+1);
        if (dfc->schemas[i_s].typ == decimal154) { dfc->schemas[i_s].scale=4; dfc->schemas[i_s].width=15; 
        } else if (dfc->schemas[i_s].typ == decimal153) { dfc->schemas[i_s].scale=3; dfc->schemas[i_s].width=15; 
        } else if (dfc->schemas[i_s].typ == decimal184) { dfc->schemas[i_s].scale=4; dfc->schemas[i_s].width=18; 
        } else if (dfc->schemas[i_s].typ == decimal185) { dfc->schemas[i_s].scale=5; dfc->schemas[i_s].width=18; 
        } else if (dfc->schemas[i_s].typ == decimal_gen) {
          iStr width_loc = find_key("width", 5, sf, st_V, end_V, verbose-2);
          if (width_loc >= st_V) {
            st0 = get_value_bounds("schema", sf, width_loc, nmax, verbose-2, &st0, &end0);
            char OSF = sf[end0+1]; sf[end0+1] = '\0'; char a_width = atoi((char*) sf + st0); sf[end0+1] = OSF;
            dfc->schemas[i_s].width = a_width;
          }
          iStr scale_loc = find_key("scale", 5, sf, st_V, end_V, verbose-2);
          if (scale_loc >= st_V) {
            st0 = get_value_bounds("schema", sf, scale_loc, nmax, verbose-2, &st0, &end0);
            char OSF = sf[end0+1]; sf[end0+1] = '\0'; char a_scale = atoi((char*) sf + st0); sf[end0+1] = OSF;
            dfc->schemas[i_s].scale = a_scale;
          }
        }
        if ((dfc->schemas[i_s].typ == tus) || (dfc->schemas[i_s].typ == tns) || (dfc->schemas[i_s].typ == tms)) {
           vpt(2, " --- Because we found timestamp, we are searching for \"fmt\" key. \n");
           iStr fmt_loc = find_key("fmt",3,sf, st_V, end_V, verbose-2);
           if (fmt_loc < 0) {  vpt(0," i_s=%ld/%ld: Issue, we had a tus/tns found at typ_loc=%ld but no corresponding fmt. \n", 
             (long int) i_s, (long int) nmax, (long int) typ_loc);
           } else {
             st0 = get_value_bounds("get_config_file", sf, fmt_loc, nmax, verbose-2, &st0, &end0);
             if (st0 < 0) { vpt(0, " i_s = %ld/%ld: Issue, even though fmt key was found for typ_loc=%ld at %ld no value! \n",
               (long int) i_s, (long int) nmax, (long int) typ_loc, (long int) fmt_loc); 
             } else {
               dfc->schemas[i_s].timestamp_format = malloc(sizeof(char) * (end0-st0+1));
               sprintf(dfc->schemas[i_s].timestamp_format, "%.*s\0", end0-st0-1, sf + st0+1);
               vpt(3, "We copied in a timestamp format for i_s=%ld/%ld of \"%s\"\n", i_s, n_schemas, dfc->schemas[i_s].timestamp_format);

               dfc->schemas[i_s].fmttyp  = MATCHTSTYPE(sf, (st0), (end0) );
             }
           }
        }
    }
    oniS = get_next_key("get_config_file", sf, oniS,vend, verbose-2);
  }
  vpt(1, " Done reading in Schemas, can now return dfc\n");
  vpt(1, " Now populating fix_fields. \n");
  int success_fix_fields = populate_fix_fields(&dfc, sf, 0, nmax, verbose-1);
  if (success_fix_fields <= 0) {
    vpt(0, " WARNING, success_fix_fields = %ld. \n", (long int) success_fix_fields);
    printf("ERROR after populate_fix_fields --- dfc is not worth generating. \n");
    delete_config_file(&dfc, 2); dfc = NULL; return(NULL);
  } else {
    dfc->nfields = success_fix_fields;
    vpt(1, " SUCCESS Populating fix_fields for %ld. fields. \n", (long int) dfc->nfields);
  } 
  return(dfc);
}
void PRINT_dfc(DF_config_file *dfc) {
   printf("------------------------------------------------------------------------------\n");
   printf("--  Printing DFC: (dfc->n_schemas=%ld).\n", (long int) dfc->n_schemas);
   printf("-- name: \"%s\". \n", dfc->name);
   printf("-- info: \"%s\". \n", dfc->info);
   printf("-- desc: \"%s\". \n", dfc->desc); 
   if (dfc->n_schemas <= 0) {
    printf("-- NO SCHEMAS  Nothing to print after desc.\n"); 
   } else {
     for (int i_s = 0; i_s < dfc->n_schemas; i_s++) {
        printf("--  schema[%ld/%ld] = {nm=\"%s\",priority=%ld, desc=\"%s\",typ=\"%s\"",
          (long int) i_s, (long int) dfc->n_schemas,
          dfc->schemas[i_s].nm != NULL ? dfc->schemas[i_s].nm : "NULL",
          (long int) dfc->schemas[i_s].priority,
          dfc->schemas[i_s].desc != NULL ? dfc->schemas[i_s].desc : "NULL",
          (char*) ( What_DF_DataType( (dfc->schemas[i_s].typ) )) );
        if (dfc->schemas[i_s].typ == decimal_gen) {
          printf("[w=%ld,scale=%ld]", 
            (long int) dfc->schemas[i_s].width, (long int) dfc->schemas[i_s].scale);
        }
        if (dfc->schemas[i_s].timestamp_format == NULL) {
          printf(",fmt is NULL}\n");
        } else {
          printf(",fmt=\"%s\" or %s}\n", dfc->schemas[i_s].timestamp_format, 
             (char*) What_DF_TSType(dfc->schemas[i_s].fmttyp));
        }
     }
   }
   if ((dfc->fxs == NULL)  || (dfc->nfields <= 0)) {
     printf("-- NO Fix fields yet. \n");
   } else {
     for (int i_f = 0; i_f < dfc->nfields; i_f++) {
       printf("-- fix_fields[%ld/%ld] = {field_code=%ld, [keep=%ld,priority=%ld, typ=%s", 
         (long int) i_f, (long int) dfc->nfields, (long int) dfc->fxs[i_f].field_code, (long int) dfc->fxs[i_f].keep,
         (long int) dfc->fxs[i_f].priority, What_DF_DataType(dfc->fxs[i_f].typ) );
       if (dfc->fxs[i_f].typ == decimal_gen) {
         printf(",[w=%ld,scale=%ld]", (long int) dfc->fxs[i_f].width, (long int) dfc->fxs[i_f].scale);
       }
       if ((dfc->fxs[i_f].typ == tms) || (dfc->fxs[i_f].typ == tns) || (dfc->fxs[i_f].typ==tus)) {
         printf(", fmt=\"%s\"=%s],", (char*) ((dfc->fxs[i_f].fmt == NULL) ? "FMT is NULL ERROR" : dfc->fxs[i_f].fmt),
            What_DF_TSType(dfc->fxs[i_f].fmttyp ));
       } else {  printf("],"); }
       printf(" title=%s, desc=\"\"\"\n%s\n   \"\"\",\n",
           dfc->fxs[i_f].fixtitle == NULL ? "NO TITLE:: ERROR" : dfc->fxs[i_f].fixtitle, 
           dfc->fxs[i_f].desc == NULL ? "NO DESC": dfc->fxs[i_f].desc
       );
       if (dfc->fxs[i_f].n_field_codes <= 0) {
         if (dfc->fxs[i_f].typ == fix42) {
            printf("   ----  ERROR, fix42 but fxs field is null. \n");
         }
       } else {
         printf("    --- { \n    ");  int i_ff_print = 0;
         for (int i_ff = 0; i_ff < NCHARS; i_ff++) {
           if (dfc->fxs[i_f].field_loc[i_ff] >= 0) {
             //printf("dfc->fixs[i_f=%ld/%ld].field_loc[i_ff=%ld/%ld] = %ld/%ld. All char len = %ld\n",
             //  (long int) i_f, (long int) dfc->nfields, (long int) i_ff, (long int) NCHARS, (long int) dfc->fxs[i_f].field_loc[i_ff],
             //  (long int) dfc->fxs[i_f].n_field_codes, (long int) dfc->fxs[i_f].field_codes_arr[dfc->fxs[i_f].n_field_codes]);
             printf("%ld: %c", (long int) i_ff_print, (char) (i_ff < 10 ? '0' + i_ff : i_ff < 26 ? 'a' + (i_ff - 10) : 'A' + (i_ff-36)));
             iStr loc_f = dfc->fxs[i_f].field_loc[i_ff];
             iStr loc_ff = dfc->fxs[i_f].field_codes_loc[loc_f];
             //printf(" --- loc_f=%ld, loc_ff=%ld. \n", (long int) loc_f, (long int) loc_ff);
             printf("=\"%s\"",  (char*) dfc->fxs[i_f].field_codes_arr + loc_ff);
             if (i_ff_print < dfc->fxs[i_f].n_field_codes -1) { printf(","); }
             //printf("\n NOTE i_ff_print+1%5 = %d \n", (int) (i_ff_print+1) % 5);
             if (((i_ff_print+1) % 5) == 0) {  printf("\n    "); } else { printf(" "); } 
             i_ff_print++;
           }
         }
         if ((i_ff_print % 5) == 0) { printf(""); } else { printf("\n"); }
         printf("    --- }\n");
       }
     }
   }
   printf("That is complete DFC currently. \n");
}
int copy_in_val_str( char* assignment, char* sf, iStr ikey, iStr nmax, int verbose, char **ploc) {
  char stt[300];
  sprintf(stt,"copy_in_val_str(%s, ikey=%ld): ", assignment, (long int) ikey);
  iStr vst, vend;
  vpt(1, "  START with get_value_bounds(); \n");
  vst = get_value_bounds(assignment, sf, ikey, nmax, verbose, &vst, &vend);
  if (vst < 0) {
    vpt(0, " ERROR copy in val string, get_value_bounds did not return. sf[%d:%d] = \"%.%s\"\n",
       ikey, ikey + 10 < nmax ? ikey + 10 : nmax,  nmax - ikey < 0 ? nmax-ikey : 10, sf + ikey);
    return(-1);
  }
  vpt(1, "  get_value_bounds returned sf[%ld,%ld]=--%.*s--\n",
    (long int) vst, (long int) vend, vend-vst-1, sf + vst + 1);
  if ( (*ploc) != NULL) { vpt(1, " NOTE ploc is not null, need to free before we can assign and allocated. \n"); free(*ploc);  *ploc = NULL; }
  if ((vst < 0)  || (vend < 0)) {
    printf("con_in_val_str(%s): failed to find val_str: ikey=%ld/%ld\n", assignment, (long int) ikey, (long int) nmax);
    return(-1);
  }
  vpt(1, "  Trying to understand type of sf[vst=%ld] = \'%c\' \n", (long int) vst, sf[vst]);
  if ((sf[vst] != '\"')  && (sf[vst] != '.') && ((sf[vst]< '0') || (sf[vst]>'9'))) {
    printf("con_in_val_str(%s): failed to find valid copy in str: ikey=%ld/%ld sf[vst=%ld]=\'%c\'\n", 
      (char*) assignment, (long int) ikey, (long int) nmax,
      (long int) vst, sf[vst]);
    return(-1);
  }
  if ((sf[vend] == '\"') && (sf[vst] == '\"') ) {
     *ploc = malloc(sizeof(char) *(vend-vst));
     sprintf(*ploc,"%.*s\0", vend - vst-1, sf + vst + 1);
     return(1);
  }
  if ((sf[vst] == '.') || ((sf[vst] >= '0') && (sf[vst] <= '9'))) {
    if ((sf[vend] == '.') || ((sf[vend] >= '0') && (sf[vend] <= '9'))) {
      *ploc = malloc(sizeof(char)*(vend+2-vst));
      sprintf(*ploc, "%.*s\0", vend+1- vst, sf + vst); return(2);
    } else if ((sf[vend-1] == '.') || ((sf[vend-1] >= '0') && (sf[vend-1] <= '9'))) {
      *ploc = malloc(sizeof(char)*(vend+1-vst));
      sprintf(*ploc, "%.*s\0", vend- vst, sf + vst); return(3);
    } 
    printf("Something is weird, con_in_val(%s):  val is apprently %.*s", assignment, vend-vst+1, sf+vst);
    return(-3);
  } else if ((sf[vst+1] == '.') || ((sf[vst+1] >= '0') && (sf[vst+1] <= '9'))) {
    if ((sf[vend] == '.') || ((sf[vend] >= '0') && (sf[vend] <= '9'))) {
      *ploc = malloc(sizeof(char)*(vend+1-vst));
      sprintf(*ploc, "%.*s\0", vend- vst, sf + vst+1); return(2);
    } else if ((sf[vend-1] == '.') || ((sf[vend-1] >= '0') && (sf[vend-1] <= '9'))) {
      *ploc = malloc(sizeof(char)*(vend-vst));
      sprintf(*ploc, "%.*s\0", vend- vst-1, sf + vst+1); return(3);
    } 
    printf("Something is weird, con_in_val(%s):  val is apprently %.*s, with vst=%ld not inbounds", assignment, vend-vst+1, sf+vst,(long int) vst);
    return(-5);
  }
  return(-6);
}
int get_n_schema(char *assignment, char *sf, iStr on_i, iStr nmax, int verbose) {
  const char stt[] = "get_n_schemas";
  if (sf == NULL) { 
    printf("ERROR get_n_schemas, null string provided\n"); return(-1);
  }
  iStr ii = on_i;

  int end_str = 0;
  iStr loc_schemas = -1;
  iStr start_schemas = -1;
  iStr end_schemas = -1;
  vpt(1, " -- Initiate with find_key for \"schemas\", starting at on_i=%ld, sf[%ld]=\'%c\'\n", (long int) on_i,
    (long int) on_i, sf[on_i]);
  PUSH_OUT_WHITE();
  vpt(1, " -- We pushed out the white and reached ii=%ld/%ld on sf[%ld]=\'%c\'.\n",
    (long int) ii, (long int) nmax, (long int) ii, sf[ii]);
  if (sf[ii] != '{') {
    vpt(0, "ERROR, sf[%ld] first non white is \'%c\' and not \'{\' \n", (long int) ii, sf[ii]);  return(-1);
  }
  ii = find_key("schema", 6, sf, ii, nmax, verbose-1);  loc_schemas = ii;  iStr st = loc_schemas;
  if ((ii < 0) || (ii >= nmax) || (sf[ii] == '}')) {
    vpt(0, " ERROR - failed to find key \"schema\". ii=%ld/%ld. \n", (long int) ii, (long int) nmax);
    return(0);
  }
  vpt(1, "We looked for tag schemas and it returned ii = %ld. \n", (long int) ii);
  end_str = get_end_quote("get_n_schemas", sf, ii, nmax);
  if ((end_str < ii) || (end_str >= nmax) || (sf[end_str] != '\"')) { 
    vpt(0, " ERROR although find key sort of succeeded to find \"schemas\" failed to find end string. ii=%ld.",
      (long int) ii);
  }
  vpt(1, " -- We found key at ii=%ld/%ld, sf[%ld:%ld] = --%.*s-- \n",
    (long int) ii, (long int) nmax, ii+1, end_str, end_str - ii-1, sf + ii + 1);
  ii = end_str + 1; 
  PUSH_TO_CHAR_WO(("get_n_schemas[locate \':\']"), (':'));
  ii++;
  PUSH_TO_CHAR_WO(("get_n_schemas[locate \'{\']"),('{'));
  start_schemas = ii;  end_schemas = get_end_brace("get_n_schemas", sf, ii, nmax);
  if (ii >= nmax) {
    vpt(0, "ERROR, although schemas was at loc_schemas=%ld, we did not find a brace. \n", (long int) loc_schemas);
    return(-1);
  }
  if (end_schemas < start_schemas) {
    vpt(0, "ERROR: we tried to find length of schemas array but it was zero length start_schemas=%ld, end_schemas=%ld. \n", 
      (long int) start_schemas, (long int) end_schemas);  return(-1);
  }
  long int num_schemas = 0;
  ii = start_schemas + 1;
  PUSH_OUT_WHITE();
  iStr on_key = ii;
  while ((ii >= on_i) && (ii <= end_schemas) && (sf[ii] != '}')) {
    if (sf[on_key] == '}') { return(num_schemas); }
    if (sf[on_key] != '\"') {
      vpt(0, "ERROR -- We had on ii=%ld sf[%ld]=\'%c\'", (long int) ii, (long int) ii, sf[ii]);
      printf(" - this is not key quote, sf[%ld:%ld] = ", (long int) ((ii - 10) < on_i ? on_i : (ii-10)), (long int) ii+1);
      printf("--%.*s--.\n", (ii+1)  - ((ii-10) < on_i ? on_i : (ii-10)), sf + ((ii-10)<on_i ? on_i : (ii-10)));
      return(-1);
    } else {
      num_schemas++;  
      ii = get_next_key("get_n_schemas", sf, ii, end_schemas+1, verbose-1);
      if (ii < 0) {
        vpt(0, "ERROR num_schemas = %ld.  We looked for next key after on_key=%ld, (sf[%ld]=\'%c\'), but received ii=%ld\n",
          (long int) num_schemas, (long int) on_key, (long int) on_key, sf[on_key], (long int) ii);
        ii = get_next_key("get_n_schemas", sf, ii, end_schemas, 9);
        vpt(0, "ERROR num_schemas = %ld.  We looked for next key after on_key=%ld, (sf[%ld]=\'%c\'), but received ii=%ld\n",
          (long int) num_schemas, (long int) on_key, (long int) on_key, sf[on_key], (long int) ii);
        return(-1);
      }
      on_key = ii;
    }
  }
  return(num_schemas);
  vpt(1, " ISSUE, get_n_schemas[start_schemas=%ld,end_schemas=%ld], ended without ever finding schemas tag.\n",
    (long int) start_schemas, (long int) end_schemas);
  return(0);
}

int get_n_fix_fields(char *assignment, char *sf, iStr on_i, iStr nmax, int verbose) {
  const char stt[] = "get_n_fix_fields";
  if (sf == NULL) { 
    printf("ERROR get_n_fix_fields, null string provided\n"); return(-1);
  }
  iStr ii = on_i;

  int end_str = 0;
  iStr loc_fix_fields = -1;
  iStr start_fix_fields = -1;
  iStr end_fix_fields = -1;
  vpt(1, " -- Initiate with find_key for \"fix_fields\", starting at on_i=%ld, sf[%ld]=\'%c\'\n", (long int) on_i,
    (long int) on_i, sf[on_i]);
  PUSH_OUT_WHITE();
  vpt(1, " -- We pushed out the white and reached ii=%ld/%ld on sf[%ld]=\'%c\'.\n",
    (long int) ii, (long int) nmax, (long int) ii, sf[ii]);
  if (sf[ii] != '{') {
    vpt(0, "ERROR, sf[%ld] first non white is \'%c\' and not \'{\' \n", (long int) ii, sf[ii]);  return(-1);
  }
  ii = find_key("fix_fields", 10, sf, ii, nmax, verbose-1);  loc_fix_fields = ii;  iStr st = loc_fix_fields;
  if ((ii < 0) || (ii >= nmax) || (sf[ii] == '}')) {
    vpt(0, " ERROR - failed to find key \"fix_fields\". ii=%ld/%ld. \n", (long int) ii, (long int) nmax);
    return(0);
  }
  vpt(1, "We looked for tag fix_fields and it returned ii = %ld. \n", (long int) ii);
  end_str = get_end_quote("get_n_fix_fields", sf, ii, nmax);
  if ((end_str < ii) || (end_str >= nmax) || (sf[end_str] != '\"')) { 
    vpt(0, " ERROR although find key sort of succeeded to find \"schemas\" failed to find end string. ii=%ld.",
      (long int) ii);
  }
  vpt(1, " -- We found key at ii=%ld/%ld, sf[%ld:%ld] = --%.*s-- \n",
    (long int) ii, (long int) nmax, ii+1, end_str, end_str - ii-1, sf + ii + 1);
  ii = end_str + 1; 
  PUSH_TO_CHAR_WO(("get_n_fix_fields[locate \':\']"), (':'));
  ii++;
  PUSH_TO_CHAR_WO(("get_n_fix_fields[locate \'{\']"),('{'));
  start_fix_fields = ii;  end_fix_fields = get_end_brace("get_n_fix_fields", sf, ii, nmax);
  if (ii >= nmax) {
    vpt(0, "ERROR, although schemas was at loc_fix_fields=%ld, we did not find a brace. \n", (long int) loc_fix_fields);
    return(-1);
  }
  if (end_fix_fields < start_fix_fields) {
    vpt(0, "ERROR: we tried to find length of fix_fields array but it was zero length start_fix_fields=%ld, end_fix_fields=%ld. \n", 
      (long int) start_fix_fields, (long int) end_fix_fields);  return(-1);
  }
  long int num_fix_fields = 0;
  ii = start_fix_fields + 1;
  PUSH_OUT_WHITE();
  iStr on_key = ii;
  while ((ii >= on_i) && (ii <= end_fix_fields) && (sf[ii] != '}')) {
    if (sf[on_key] == '}') { return(num_fix_fields); }
    if (sf[on_key] != '\"') {
      vpt(0, "ERROR -- We had on ii=%ld sf[%ld]=\'%c\'", (long int) ii, (long int) ii, sf[ii]);
      printf(" - this is not key quote, sf[%ld:%ld] = ", (long int) ((ii - 10) < on_i ? on_i : (ii-10)), (long int) ii+1);
      printf("--%.*s--.\n", (ii+1)  - ((ii-10) < on_i ? on_i : (ii-10)), sf + ((ii-10)<on_i ? on_i : (ii-10)));
      return(-1);
    } else {
      num_fix_fields++;  
      ii = get_next_key("get_n_fix_fields", sf, ii, end_fix_fields+1, verbose-1);
      if (ii < 0) {
        vpt(0, "ERROR num_fix_fields = %ld.  We looked for next key after on_key=%ld, (sf[%ld]=\'%c\'), but received ii=%ld\n",
          (long int) num_fix_fields, (long int) on_key, (long int) on_key, sf[on_key], (long int) ii);
        ii = get_next_key("get_n_fix_fields", sf, ii, end_fix_fields, 9);
        vpt(0, "ERROR num_fix_fields = %ld.  We looked for next key after on_key=%ld, (sf[%ld]=\'%c\'), but received ii=%ld\n",
          (long int) num_fix_fields, (long int) on_key, (long int) on_key, sf[on_key], (long int) ii);
        return(-1);
      }
      on_key = ii;
    }
  }
  return(num_fix_fields);
  vpt(1, " ISSUE, get_n_fix_fields[start_fix_fields=%ld,end_fix_fields=%ld], ended without ever finding schemas tag.\n",
    (long int) start_fix_fields, (long int) end_fix_fields);
  return(0);
}
int populate_fixfield(iStr i_fst, iStr nmax, char *sf, int verbose, 
  int i_onfxs, DF_Fix_Field **p_dfs) {
  char stt[300];
  if (sf == NULL) {
    printf(" ERROR ISSUE, populate_fixfield: hey sf is NULL! \n");
  }
  if (verbose >= 1) {
    printf("populate_fixfield: start with i_fst=%ld/%ld. \n", (long int) i_fst, (long int) nmax);
  }
  if ((i_fst < 0) || (sf[i_fst] != '\"')) {
    printf("populate_fixfield(): Error at start i_onfxs=%ld, sf[i_fst=%ld] = \'%c\', something is amis. \n",
      (long int) i_onfxs, (long int) i_fst, i_fst < 0 ? 'X' : sf[i_fst]); 
    printf(" ---- Note nmax=%ld, verbose=%d, i_fst = %ld. \n", (long int) nmax, (int) verbose, (long int) i_fst);
    return(-1);
  }
  iStr i_fvals_start = i_fst;
  sprintf(stt, "populate_fixfield(i_onfxs=%ld,st=%ld): ", (long int) i_onfxs, (long int) i_fvals_start);
  iStr endq = get_end_quote(stt, sf, i_fvals_start, nmax);  char sfend_v0;
  if ((endq < 0) || (sf[endq] != '\"')) {
    vpt(0, "ERROR: populate fix field could not find endq in i_onfx=%ld, i_fvals_start=%ld/%ld.  sf[%ld:%ld...] =\"%.*s\"",
      (long int) i_onfxs, (long int) i_fvals_start, (long int) nmax, 
      (long int) i_fvals_start,  (long int) i_fvals_start + 30 > nmax ? nmax : i_fvals_start + 30,
      (i_fvals_start + 30 < nmax ? ( nmax-i_fvals_start) : 30), sf + i_fvals_start); return(-1);
  }
  sprintf(stt, "populate_fixfield(\"%.*s\", i_onfxs=%ld, st=%ld): ",
    endq-i_fvals_start-1, sf + i_fvals_start + 1, (long int) i_onfxs, (long int) i_fvals_start);
  DF_Fix_Field *dfs = p_dfs[0]; 
  sf[endq] = '\0';  int num = atoi(sf + i_fvals_start + 1);
  sf[endq] = '\"';
  dfs[i_onfxs].field_code = num;
  vpt(1, "  -- we read sf and processed a number %ld from \"%.*s\". \n",
     (long int) num, endq-1-i_fvals_start, sf + i_fvals_start + 1);
  if (num < 0) {
    vpt(-2, " error, the number we saw was %ld from \"%.*s\" for i_onfxs=%d, i_fvals_start=%ld \n",
      (long int) num, endq-1-i_fvals_start, sf + i_fvals_start + 1, (int) i_onfxs, (long int) i_fvals_start);  
  }
  sprintf(stt, "populate_fixfield(field=%d, i_onfxs=%ld, st=%ld): ",
     (int) num, (long int) i_onfxs, (long int) i_fvals_start);
  iStr st_v, end_v;
  st_v = get_value_bounds(stt, sf, i_fvals_start, nmax, verbose-1, &st_v, &end_v);
  
  if ((st_v < 0) || (sf[st_v] != '{') || (end_v < 0) || (sf[end_v] != '}')) {
    vpt(0, " ERROR we looked for values, got none: st_v=%ld, field_code=%ld, sf[%ld:%ld...] = \"%.*s\" \n",
      (long int) st_v, (long int) num, (long int) endq,
      (long int) endq+30 > nmax ? nmax : endq+30,  (endq+30 > nmax ? nmax=endq : 30), sf + endq);
    return(-1);
  }
  vpt(1, " -- We are ready to start processing values sf[st_v=%ld:end_v=%ld] = \n\"\"\"\n%.*s\n      \"\"\"\n",
    (long int) st_v, (long int) end_v, end_v-st_v + 1, sf + st_v);
  iStr st_v0, end_v0;
  iStr iKeep = find_key("keep", 4, sf, st_v, end_v+1, verbose-3); 
  if ((iKeep < 0) || (sf[iKeep] == '}')) {
    vpt(-1, " ERROR, looking at field_code=%ld values, did not find keep field. sf[%ld:%ld] = \"%.*s\" \n",
      num, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
    iKeep = find_key("keep", 4, sf, st_v, end_v, verbose+2); 
    vpt(-1, " ERROR, looking at field_code=%ld values, did not find keep field. sf[%ld:%ld] = \"%.*s\" \n",
      num, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
    vpt(-1, "We tried again, iKeep now %ld, Note sf[st_v=%ld:end_v=%ld] = \"\"\"\n%.*s\n    \"\"\"\n",
      (long int) iKeep, (long int) st_v, (long int) end_v, (int) (end_v-st_v+1), sf + st_v);
    printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
    return(-105302);
  } else {
     st_v0 = get_value_bounds(stt, sf, iKeep, end_v, verbose-1, &st_v0, &end_v0);
     if (st_v0 < 0)  {
       vpt(0, " ERROR, looking at field_code=%ld values, found keep field at %ld but still no val bounds. sf[%ld:%ld] = \"%.*s\" \n",
         num, (long int) iKeep, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
     }  else {
       int keep_num; sfend_v0 = sf[end_v0]; sf[end_v0] = '\0';  keep_num=atoi(sf + st_v0 + (sf[st_v0]=='\"' ? 1 : 0));
       sf[end_v0] = sfend_v0; 
       dfs[i_onfxs].keep = (short) keep_num;
       vpt(2, " success keep = %ld. \n", (long int) keep_num);
     }
  }
  iStr iPriority = find_key("priority", 8, sf, st_v, end_v+1, verbose-3); 
  if ((iPriority < 0) || (sf[iPriority] == '}')) {
    vpt(0, " ERROR, looking at field_code=%ld values, did not find \"priority\" field. sf[%ld:%ld] = \n\"\"\"\n%.*s\n       \"\"\" \n",
      num, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
    iPriority = find_key("priority", 8, sf, st_v, end_v, verbose+3); 
    vpt(0, " ERROR, looking at field_code=%ld values, did not find \"priority\" field. sf[%ld:%ld] = \n\"\"\"\n%.*s\n       \"\"\" \n",
      num, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
    vpt(0, "We tried again, iPriority now %ld, Note sf[st_v=%ld:end_v=%ld] = \"\"\"\n%.*s\n        \"\"\"\n",
      (long int) iPriority, (long int) st_v, (long int) end_v, (int) (end_v-st_v+1), sf + st_v);
    printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
    return(-106652);
   
  } else {
     st_v0 = get_value_bounds(stt, sf, iPriority, end_v+1, verbose-1, &st_v0, &end_v0);
     if (st_v0 < 0)  {
       vpt(0, " ERROR, looking at field_code=%ld values, found \"priority\" field at %ld but still no val bounds. sf[%ld:%ld] = \"%.*s\" \n",
         num, (long int) iPriority, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
     }  else {
       int priority_num; sfend_v0 = sf[end_v0]; sf[end_v0] = '\0';  priority_num=atoi(sf + st_v0 + (sf[st_v0]=='\"' ? 1 : 0));
       sf[end_v0] = sfend_v0; 
       dfs[i_onfxs].priority = (short) priority_num;
       vpt(2, " success, priority filled to %ld. \n", (long int) dfs[i_onfxs].priority);
     }
  }
  iStr iFixTitle = find_key("fixtitle", 8,  sf, st_v, end_v+1, verbose-2); 
  if ((iFixTitle < 0) || (sf[iFixTitle] == '}')) {
    vpt(0, " ERROR, looking at field_code=%ld values, did not find fixtitle field. sf[%ld:%ld] = \"%.*s\" \n",
      (long int) num, (long int) st_v,  (long int) (st_v + 30 > end_v ? end_v : st_v + 30),
      (int) ((st_v+30 > end_v) ? end_v-st_v : 30), (char*) (sf + st_v) );
    iFixTitle = find_key("fixtitle", 8,  sf, st_v, end_v+1, verbose+3); 
    vpt(0, " ERROR, looking at field_code=%ld values, did not find fixtitle field. sf[%ld:%ld] = \"%.*s\" \n",
      (long int) num, (long int) st_v,  (long int) (st_v + 30 > end_v ? end_v : st_v + 30),
      (int) ((st_v+30 > end_v) ? end_v-st_v : 30), (char*) (sf + st_v) );
    vpt(0, " After that search iFixTitle found at %ld. \n", iFixTitle);
    printf("Note sf[st_v=%ld:end_v=%ld] = \"\"\"\n%.*s\n    \"\"\"\n",
      (long int) st_v, (long int) end_v, (int) (end_v-st_v+1), sf + st_v);
    printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
    return(-11532);
  } else {
     st_v0 = get_value_bounds(stt, sf, iFixTitle, end_v, verbose-1, &st_v0, &end_v0);
     if (st_v0 < 0)  {
       vpt(0, " ERROR, looking at field_code=%ld values, found fixtitle field at %ld but still no val bounds. sf[%ld:%ld] = \"%.*s\" \n",
         (long int) num, (long int) iFixTitle, (long int) st_v,  (long int) (st_v + 30 > end_v ? end_v : st_v + 30), 
         (int) ((st_v+30 > end_v) ? end_v-st_v : 30), (char*) (sf + st_v) );
     } else {
       dfs[i_onfxs].fixtitle = (char*) malloc(sizeof(char)*(end_v0-st_v0));
       if (dfs[i_onfxs].fixtitle != NULL) {
         sprintf(dfs[i_onfxs].fixtitle, "%.*s\0", end_v0-st_v0-1, sf + st_v0+1);
       }
       vpt(2, " success, fixtitle filled to %s. \n", dfs[i_onfxs].fixtitle);
     }
  }
  iStr iDesc = find_key("desc", 4, sf, st_v, end_v+1, verbose-3); 
  if ((iDesc < 0) || (sf[iDesc] == '}')) {
    vpt(0, " ERROR, looking at field_code=%ld values, did not find desc field. sf[%ld:%ld] = \n\"\"\"\n%.*s\n   \"\"\" \n",
      (long int) num, (long int) st_v,  (long int) end_v, end_v-st_v+1, sf + st_v);
    iDesc = find_key("desc", 4, sf, st_v, end_v+1, verbose+1); 
    vpt(0, " ERROR, looking at field_code=%ld values, did not find desc field. sf[%ld:%ld] = \n\"\"\"\n%.*s\n   \"\"\" \n",
      (long int) num, (long int) st_v,  (long int) end_v, end_v-st_v+1, sf + st_v);
    vpt(0, " ERROR follow up, we found iDesc after second look at %ld. \n", (long int ) iDesc);
    printf(" --- Note sf[%ld:%ld]=\"\"\"\n%.*s\n    \"\"\"\n", (long int) st_v, (long int) end_v,
      (int) (end_v-st_v+1), sf + st_v);
    vpt(0, " ERROR follow up, we found iDesc after second look at %ld. \n", (long int ) iDesc);
    printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
    return(-134302);
  } else {
     st_v0 = get_value_bounds(stt, sf, iDesc, end_v, verbose-1, &st_v0, &end_v0);
     if (st_v0 < 0)  {
       vpt(0, " ERROR, looking at field_code=%ld values, found desc field at %ld but still no val bounds. sf[%ld:%ld] = \"%.*s\" \n",
         num, (long int) iDesc, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
       return(-304032);
     } else {
       dfs[i_onfxs].desc = (char*) malloc(sizeof(char)*(end_v0-st_v0));
       if (dfs[i_onfxs].desc != NULL) {
         sprintf(dfs[i_onfxs].desc, "%.*s\0", end_v0-st_v0-1, sf + st_v0+1);
         vpt(2, " success, desc filled to \"%s\".\n",  dfs[i_onfxs].desc);
       } else {
         vpt(0, "ERROR: field_code=%ld, failed to allocate %ld bytes for desc field found at %ld. \n",
           dfs[i_onfxs].field_code, (long int) end_v0-st_v0, (long int) iDesc);  return(-235032); 
       }
     }
  }
  iStr iTyp = find_key("typ", 3, sf, st_v, end_v+1, verbose-3);   DF_DataType on_typ;
  if ((iTyp < 0) || sf[iTyp] == '}') {
    vpt(0, " ERROR, looking at field_code=%ld values, did not find typ field. sf[%ld:%ld] = \"%.*s\" \n",
      num, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
    iTyp = find_key("typ", 3, sf, st_v, end_v, verbose+1);  
    vpt(0, " Did we find iTyp that time?  Now equal to %ld: sf[%ld:%ld]=\"\"\"\n%.*s\n\"\"\" \n", (long int) iTyp,
    (long int) st_v, (long int) end_v, (int) (end_v-st_v+1), sf + st_v);
    printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
    return(-304035);
  }
  if ((iTyp < 0) || (sf[iTyp] == '}')) {
  } else {
     st_v0 = get_value_bounds(stt, sf, iTyp, end_v, verbose-1, &st_v0, &end_v0);
     if (st_v0 < 0)  {
       vpt(0, " ERROR, looking at field_code=%ld values, found typ field at %ld but still no val bounds. sf[%ld:%ld] = \"%.*s\" \n",
         num, (long int) iDesc, st_v,  st_v + 30 > end_v ? end_v : st_v + 30, (st_v+30 > end_v) ? end_v-st_v : 30, sf + st_v);
     } else {
       iStr st_v0_loc = sf[st_v0] == '\"' ? st_v0 : (sf[st_v0-1] == '\"') ? st_v0-1 : st_v0;
       dfs[i_onfxs].typ = MATCHTYPE(sf, st_v0_loc, (end_v0)); on_typ = dfs[i_onfxs].typ;
       vpt(1, " we had sf[%ld:%ld]=\"%.*s\", which we map to typ=\"%s\". \n",
         (long int) (st_v0_loc+1), (long int) (end_v0), (end_v0-st_v0_loc-1), sf + st_v0_loc+1,
         What_DF_DataType( dfs[i_onfxs].typ));         
       if (dfs[i_onfxs].typ == UNKNOWN) {
         vpt(-1, " Further ERROR, sf[%ld:%ld]=\"%.*s\" mapped to UNKNOWN. \n",
           (long int) (st_v0_loc+1), (long int) end_v0, (end_v0-st_v0_loc-1), sf + st_v0_loc+1);
         printf("We have an Error trying to run MATCHTYPE \n");
         printf("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n");
         return(-304503);
       }
       if (dfs[i_onfxs].typ == decimal154) { dfs[i_onfxs].width=15; dfs[i_onfxs].scale = 4; }
       if (dfs[i_onfxs].typ == decimal153) { dfs[i_onfxs].width=15; dfs[i_onfxs].scale = 3; }
       if (dfs[i_onfxs].typ == decimal184) { dfs[i_onfxs].width=18; dfs[i_onfxs].scale = 4; }
       if (dfs[i_onfxs].typ == decimal185) { dfs[i_onfxs].width=18; dfs[i_onfxs].scale = 5; }
       if (dfs[i_onfxs].typ == decimal_gen) {
          iStr width_loc = find_key("width", 5, sf, st_v, end_v, verbose-2);
          if (width_loc >= st_v) {
            st_v0 = get_value_bounds("fxs", sf, width_loc, nmax, verbose-2, &st_v0, &end_v0);
            st_v0 = sf[st_v0] == '\"' ? st_v0+1 : ((sf[st_v0] >= '0') && (sf[st_v0] <= '9')) ? st_v0 : st_v0+1;
            end_v0 = sf[end_v0] == '\"' ? end_v0-1 : sf[end_v0] == ',' ? end_v0-1 : ((sf[end_v0] >= '0') && (sf[end_v0] <= '9')) ? end_v0: end_v0;
            char OSF = sf[end_v0+1]; sf[end_v0+1] = '\0'; char a_width = atoi((char*) sf + st_v0); sf[end_v0+1] = OSF;
            dfs[i_onfxs].width = a_width;
          } else {
            vpt(-1, "Error did not locate width got width_loc = %ld even those field %d is decimal_gen type. \n",
              (long int) width_loc, dfs[i_onfxs].field_code); return(-403230);
          }
          iStr scale_loc = find_key("scale", 5, sf, st_v, end_v, verbose-2);
          if (scale_loc >= st_v) {
            st_v0 = get_value_bounds("fxs", sf, scale_loc, nmax, verbose-2, &st_v0, &end_v0);
            st_v0 = sf[st_v0] == '\"' ? st_v0+1 : ((sf[st_v0] >= '0') && (sf[st_v0] <= '9')) ? st_v0 : st_v0+1;
            end_v0 = sf[end_v0] == '\"' ? end_v0-1 : sf[end_v0] == ',' ? end_v0-1 : ((sf[end_v0] >= '0') && (sf[end_v0] <= '9')) ? end_v0: end_v0;
            char OSF = sf[end_v0+1]; sf[end_v0+1] = '\0'; char a_scale = atoi((char*) sf + st_v0); sf[end_v0+1] = OSF;
            dfs[i_onfxs].scale = a_scale;
          }
       }
       if ((dfs[i_onfxs].typ == tms) || (dfs[i_onfxs].typ == tns) ||  (dfs[i_onfxs].typ == tus)) {
         iStr iFmt = find_key("fmt",3, sf, st_v, end_v+1, verbose);
         if ((iFmt < 0) || sf[iFmt] == '}') {
           vpt(0," ISSUE field_code=%ld, we found typ=", (long int) num);
           printf("%s", What_DF_DataType(on_typ));
           printf("but despite this, count not find fmt in \"\"\"\n%.*s\n   \"\"\".\n",
             (int) ( (end_v-st_v > 30) ? 30 : end_v-st_v), (char*) (sf + (int) st_v));
         }
         st_v0 = get_value_bounds(stt, sf, iFmt, end_v, verbose-1, &st_v0, &end_v0); 
         if (st_v0 < 0) {
           vpt(0, " ISSUE field_code=%ld, ", (long int) num);
           printf("we are on typ = \"%s\"", What_DF_DataType(on_typ));
           printf(", found fmt field at %ld.  But still no value bounds. \n", (long int) iFmt);
         } else {
           dfs[i_onfxs].fmt = (char*) malloc(sizeof(char) * (end_v0-st_v0));
           if (dfs[i_onfxs].fmt != NULL) {
             sprintf(dfs[i_onfxs].fmt, "%.*s\0", end_v0-st_v0-1, sf + st_v0+1);
           } else {  
             vpt(0, "ERROR: field_code=%ld, failed to allocate %ld bytes for fmt field found at %ld. \n",
               (long int) num, (long int)  end_v0-st_v0, iFmt); return(-1);
           }
           dfs[i_onfxs].fmttyp = MATCHTSTYPE(sf, st_v0, end_v0);
         }
       }
     }
  }
  
  vpt(1, " -- looking for encode. sf[end_v=%ld]=\'%c\'\n", (long int) end_v, sf[end_v]);
  iStr iEncode = find_key("encode", 6, sf, st_v, end_v+1, verbose-3);
  if ((iEncode >= 0) && (sf[iEncode] != '}')) {
    vpt(1, " -- Note encode was successfully found at %ld between [%ld,%ld] \n", (long int) iEncode, (long int) st_v, (long int) end_v);
    int success_encode = populate_encode(stt, dfs+i_onfxs, i_onfxs, sf, st_v, end_v+1, verbose-2);
    if (success_encode < 0) {
      vpt(0, "ERROR: ifield=%ld: field_code=%ld: We have an error %ld on encode. \nsf[%ld:%ld]=\"\"\"\n%.*s\n   \"\"\"\n", 
        (long int) i_onfxs,
        (long int) dfs[i_onfxs].field_code, (long int) success_encode, 
        (long int) st_v, (long int) end_v, (int) (end_v-st_v+1), (char*) (sf + st_v));  return(-1);
    }
  } else {
    if (verbose >= 1) {
      vpt(1, "-- Encode not found ifield=%ld, field_code=%ld,", (long int) i_onfxs, (long int) dfs[i_onfxs].field_code);
      printf("\nsf[%ld:%ld]=\"\"\"\n", (long int) st_v, (long int) end_v);
      if (sf[end_v] != '}') { printf(" ----- Hey weird, end_V=%ld but sf[end_v=%ld] =\'%c\' \n",
        (long int) end_v, (long int) end_v, sf[end_v]); }
      printf("%.*s\n    \"\"\"\n", (int) (end_v-st_v+1), (char*) (sf + st_v));
    }
  }
  vpt(1, " --- All concluded with ifield =%ld, field_code=%ld. \n",
      (long int) i_onfxs, (long int) dfs[i_onfxs].field_code);
  return(1);
}
int populate_encode(char *assignment, DF_Fix_Field*dff, int i_onfxs, char*sf, iStr i_fvals_start, iStr nmax, int verbose) {
  char stt[300];
  sprintf(stt, "populate_encode(%.*s, i_onfxs=%ld,st=%ld): ", 30, assignment, (long int) i_onfxs, (long int) i_fvals_start);
  iStr iEncode = find_key("encode", 6, sf, i_fvals_start, nmax, verbose-1);
  if (iEncode < 0) {
   vpt(0, "ERROR we never found \"encode\" within the sf limits.  sf[%ld:%ld] = \"\"\"%.*s\n    \"\"\"\n",
     (long int) i_fvals_start, (long int) nmax, nmax - i_fvals_start, sf + i_fvals_start);
   printf("NOTE THIS IS STUPID AND SHOULDN'T HAPPEN, We already found it once! \n");
   return(-10);
  }
  vpt(1, " start, with iEncode found at %ld between [%ld,%ld] \n", (long int) iEncode, (long int) i_fvals_start, (long int) nmax);
  if ((iEncode < 0) || (iEncode > nmax)) {
    vpt(0, " ERROR: populate encode failed iEncode returned %ld. i_onfxs=%ld, Searching from i_fvals_start=%ld, sf=\"%.*s...\"\n",
      (long int) iEncode, (long int) i_onfxs, (long int) i_fvals_start, 50, (char*) (sf + i_fvals_start) ); return(-1);
  }
  iStr st_v, end_v;
  st_v = get_value_bounds(stt, sf, iEncode, nmax, verbose-1, &st_v, &end_v);
  iStr n_keys = 0;  iStr n_vals_char = 0;
  iStr st_v0, end_v0;
  iStr i_on_key = -1;
  i_on_key = get_next_key(stt, sf, st_v, end_v+1, verbose-1);
  if (i_on_key < 0) {
    vpt(0, "ERROR: get_next key on first key with iEncode=%ld, still no keys. sf[%ld:%ld...] = \"%.*s...\"\n",
      (long int) iEncode, (long int) st_v, 
      (long int) (end_v+1 < (long int) st_v + 30 ? end_v + 1 : st_v + 30),
      (int) ((end_v + 1) < st_v + 30 ? end_v+1 - st_v : 30), (char*) (sf + st_v));  return(-1);
  }
  vpt(2, " starting to calculate keys i_onkey=%ld between vals[%ld,%ld], here we go. \n", 
    (long int) i_on_key, (long int) st_v, (long int) end_v);
  while ((i_on_key >= 0) && (i_on_key < end_v)) {
    st_v0 = get_value_bounds(stt, sf, i_on_key, end_v+1, verbose-1, &st_v0, &end_v0);
    n_keys++; n_vals_char += end_v0-st_v0;
    vpt(3, "key %ld, total chars %ld, value string is \"%.*s\". \n", (long int) n_keys, (long int) n_vals_char,
      end_v0 - st_v0-1, sf + st_v0+1); 
    i_on_key = get_next_key(stt, sf, i_on_key, end_v+1, verbose-1);
    if ((i_on_key < 0) || (i_on_key >= nmax) || (sf[i_on_key] == '}')) { i_on_key = end_v+1; }
  }
  vpt(2, " we found %ld keys, for total of %ld characters.  Allocating loc and arr\n", (long int) n_keys, n_vals_char);
  dff->n_field_codes = n_keys;
  dff->field_codes_loc = (iStr*) malloc(sizeof(iStr) * (1+n_keys));
  if (dff->field_codes_loc == NULL) {
    vpt(0, "ERROR: failed to allocate field_codes loc of size %ld \n", (long int) n_keys);  return(-1);
  }
  dff->field_codes_arr = (char*) malloc(sizeof(char) * (1+n_vals_char));
  if (dff->field_codes_arr ==NULL) {
    vpt(0, " ERROR: field_codes_arr is NULL, n_vals_char was %ld for %ld keys. \n", (long int) n_vals_char, (long int) n_keys);
  }
  dff->field_codes_loc[n_keys] = n_vals_char;  // Last element is total length of vector.
  i_on_key = get_next_key(stt, sf, st_v, end_v+1, verbose-1);
  int n_keys2 = 0; int n_vals_char2 = 0;
  dff->field_codes_arr[n_vals_char] = '\0';
  vpt(1, " -- Beginning to insert data into key. \n");
  //vpt(1, " Note that dff->field_codes_loc[%ld] = %ld \n", (long int) n_keys, (long int) dff->field_codes_loc[n_keys]);
  //dff->field_codes_arr[0] = 'Y'; dff->field_codes_arr[1] = 'O'; dff->field_codes_arr[2] = '\0';
  //vpt(1, " -- Funny town: dff->field_codes_arr=\"%s\"\n", dff->field_codes_arr);
  while(i_on_key < end_v) {
    char on_char = sf[i_on_key+1];
    st_v0 = get_value_bounds(stt, sf, i_on_key, end_v+1, verbose-1, &st_v0, &end_v0);
    dff->field_loc[LOC_CHAR(on_char)]  = n_keys2;
    dff->field_codes_loc[n_keys2] = n_vals_char2;
    //vpt(1, " -- i_on_key=%ld/%ld, n_keys2=%ld/%ld, on_char=\'%c\' located at %ld write \"%.*s\" to %ld/%ld \n",
    //  (long int) i_on_key, (long int) end_v, (long int) n_keys2, (long int) n_keys,
    //  on_char, (long int) LOC_CHAR(on_char), end_v0-st_v0-1, sf + st_v0 + 1,
    //   (long int) n_vals_char2, (long int) n_vals_char);
    for (iStr iip = 0; iip < end_v0-st_v0-1; iip++) {
      dff->field_codes_arr[n_vals_char2 + iip] = sf[ st_v0 + 1 + iip];
    }
    dff->field_codes_arr[n_vals_char2 + end_v0-st_v0-1] = '\0';
    //printf(" WRITE WAS DONE, end_v0-st_v0=%ld \n", (long int) end_v0-st_v0);
    //printf("0th char = \'%c\' \n", dff->field_codes_arr[n_vals_char2]);
    //printf("1th char = \'%c\' \n", dff->field_codes_arr[1+n_vals_char2]);
    //printf("%ldth char = \'%c\' \n", (end_v0-st_v0), dff->field_codes_arr[n_vals_char2 + end_v0-st_v0]);
    //printf("String is \"%s\" \n", dff->field_codes_arr + n_vals_char2);
    //sprintf(dff->field_codes_arr + n_vals_char2, "%.*s\0", end_v0-st_v0-1, sf + st_v0+1);
    dff->field_codes_arr[n_vals_char2+end_v0-st_v0-1] = '\0';
    //vpt(1, " -- i_on_key=%ld/%ld, n_keys2=%ld/%ld, on_char=\'%c\' located at %ld. \n",
    //  (long int) i_on_key, (long int) end_v, (long int) n_keys2, (long int) n_keys,
    //  on_char, LOC_CHAR(on_char));
    //printf("  With field found=\"%s\" \n", (char*) dff->field_codes_arr + n_vals_char2);
    //printf("  Field Codes Loc[%ld] = %ld \n", (long int) n_keys2, (long int) dff->field_codes_loc[n_keys2]);
    n_keys2++; n_vals_char2 += end_v0-st_v0;
    i_on_key = get_next_key(stt, sf, i_on_key, end_v+1, verbose-1);
    if (sf[i_on_key] == '}') { i_on_key = end_v+1; }
  }
  dff->field_codes_arr[n_vals_char] = '\0';
  vpt(1, " We have successfully filled dff fields with n_keys=%ld:%ld, n_vals_char=%ld:%ld \n", n_keys, n_keys2, n_vals_char, n_vals_char2);
  return(n_keys);
}
DF_Fix_Field *create_blank_fix_fields(int n_fix_fields, int verbose) {
  char stt[300];
  sprintf(stt, "create_blank_fix_fields(n=%ld): ", (long int) n_fix_fields);
  DF_Fix_Field* fxs = NULL;
  vpt(1, " Allocating initial field structure. \n");
  fxs = malloc(sizeof(DF_Fix_Field)*n_fix_fields);
  if (fxs == NULL) {
    vpt(0, " ERROR failed to allocate fxs. \n"); return(NULL);
  }
  for (int ii = 0; ii < n_fix_fields; ii++) {
    fxs[ii].field_code = -1; fxs[ii].keep = -1; fxs[ii].fixtitle = NULL;
    fxs[ii].typ = UNKNOWN;   fxs[ii].desc = NULL;  fxs[ii].fmt = NULL;
    for (int jj =0; jj < NCHARS; jj++) { fxs[ii].field_loc[jj] = -1; }
    fxs[ii].field_codes_loc = NULL; fxs[ii].field_codes_arr = NULL; fxs[ii].n_field_codes = 0;
    fxs[ii].width = (char) 0; fxs[ii].scale = (char) 0;  fxs[ii].fmttyp = UNKNOWN_TS;
  }
  vpt(1, " create_blank_fix_fields.  Returning blanked fields \n");
  return(fxs);
}
int delete_fix_fields(int n_fix_fields, DF_Fix_Field **p_fxs, int verbose) {
  char stt[300];
  sprintf(stt, "delete_fix_fields(%d): ", (int) n_fix_fields);
  DF_Fix_Field *fxs = p_fxs[0];
  if (fxs == NULL) { 
    vpt(0, " ERROR: fxs supplied is already NULL"); return(-1);
  }
  for (int ii = 0; ii < n_fix_fields; ii++) {
    //if (fxs[ii].nm != NULL) {
    //  vpt(2, " Free fxs[ii=%ld/%ld].nm. \n", (long int) ii, (long int) n_fix_fields);
    //  free(fxs[ii].nm); fxs[ii].nm = NULL;
    //}
    if (fxs[ii].fixtitle != NULL) {
      vpt(3, " Free fxs[ii=%ld/%ld][%d].fixtitle. \n", (long int) ii, (long int) n_fix_fields, fxs[ii].field_code);
      free(fxs[ii].fixtitle); fxs[ii].fixtitle = NULL;
    }
    if (fxs[ii].fmt != NULL) {
      vpt(3, " Free fxs[ii=%ld/%ld][%d].fmt. \n", (long int) ii, (long int) n_fix_fields, fxs[ii].field_code);
      free(fxs[ii].fmt); fxs[ii].fmt = NULL;
    }
    if (fxs[ii].desc != NULL) {
      vpt(3, " Free fxs[ii=%ld/%ld].desc. \n", (long int) ii, (long int) n_fix_fields);
      free(fxs[ii].desc); fxs[ii].desc = NULL;
    }
    if (fxs[ii].field_codes_loc != NULL) {
      vpt(3, " Free fxs[ii=%ld/%ld].free field codes. \n", (long int) ii, (long int) n_fix_fields);
      free(fxs[ii].field_codes_loc);  fxs[ii].field_codes_loc = NULL;
    }
    if (fxs[ii].field_codes_arr != NULL) {
      vpt(3, " Free fxs[ii=%ld/%ld].free field codes. \n", (long int) ii, (long int) n_fix_fields);
      free(fxs[ii].field_codes_arr);  fxs[ii].field_codes_arr = NULL;
    }
  }
  return(1);
}
int comp_int(const void*a, const void*b) {
   int ia = *((int*)a); int ib=*((int*)b);
   return(ia-ib);
}
int *order_unique_fix_fields(char* assignment, char*sf, iStr on_i, iStr nmax, int verbose, iStr FieldKey, int n_fix_fields) {
  char stt[300];
  sprintf(stt, "order_unique_fix_fields(\"%.*s\",nf=%ld,v=%ld): ", 30, assignment, (long int) n_fix_fields, (long int) verbose); 
  iStr st_v, end_v;
  st_v = get_value_bounds("fix_fields", sf, FieldKey, nmax, verbose-1, &st_v, &end_v); 
  int *unique_fix_fields = (int*) malloc(sizeof(int) * n_fix_fields);
  if (unique_fix_fields == NULL) {  vpt(0, "ERROR failed to allocate unique fix fields. \n");  return(NULL); }
  iStr onFieldKey;
  onFieldKey =get_next_key("order_unique_fix_fields", sf, st_v, end_v, verbose-1);
  if ((onFieldKey < 0) || (onFieldKey >= end_v)) { 
    printf("df_load.c->order_unique_fix_fields: we were given st_v,end_v=[%ld,%ld] but found no key initially. \n",
      (long int) st_v, (long int) end_v);
    vpt(0, "ERROR onFieldKey initial is NULL! \n"); free(unique_fix_fields); return(NULL); 
  }
  int onikey = 0; 
  for (;onikey < n_fix_fields; onikey++) {
     iStr endq = get_end_quote(stt, sf, onFieldKey, end_v);  char sfend_v0;
     if (endq < 0) { vpt(0, "ERROR endq=%ld for onFieldKey=%ld for onikey=%ld \n", (long int) endq, (long int) onFieldKey, (long int) onikey);
                     free(unique_fix_fields); return(NULL); } 
     sf[endq] = '\0'; int num = atoi(sf + onFieldKey + 1);
     sf[endq] = '\"';
     unique_fix_fields[onikey] = num;
     if (onikey < n_fix_fields -1) {
       onFieldKey =get_next_key("order_unique_fix_fields", sf, onFieldKey, end_v, verbose-1);
     }
  }
  qsort(unique_fix_fields, n_fix_fields, sizeof(int), comp_int);
  return(unique_fix_fields);
}
int populate_fix_fields(DF_config_file **p_dfc, char*sf, iStr on_i, iStr nmax, int verbose) {
  char stt[300];
  iStr iField = find_key("fix_fields",10, sf,on_i, nmax, verbose-1);  
  if (iField < 0) {
    vpt(0, " ERROR no fix_fields field found from %ld:%ld, sf[%ld]=\'%c\' \n",  (long int) on_i, (long int) nmax, (long int) on_i, sf[on_i]);
    return(-1);
  }
  sprintf(stt, "populate_fix_fields(iField=%ld/%ld): ", (long int) iField, (long int) nmax);
  vpt(1, " INITIATE \n");
  DF_config_file *dfc = p_dfc[0];
  int n_fix_fields = get_n_fix_fields("populate_fix_fields", sf, on_i, nmax, verbose-1);
  vpt(2, " -- We get n_fix_fields = %ld. \n", (long int) n_fix_fields);  
  if (n_fix_fields <= 0) { vpt(0, " n_fix_fields=%ld. \n", (long int) n_fix_fields); return(-1); }
  int *ordered_fields = order_unique_fix_fields("populate_fix_fields", sf, on_i, nmax, verbose-1, iField, n_fix_fields);
  vpt(2, " -- Here are the ordered fields. \n");
  if (verbose >= 1) {
    printf("     [");  for (int ij = 0; ij < n_fix_fields; ij++) { 
      printf("%d", ordered_fields[ij]);  if (ij < n_fix_fields-1) { printf(", "); } 
      if ((ij+1)%10 == 0) {printf("\n       "); }
    }
    printf("  ]\n");
  }
  if (ordered_fields == NULL) { vpt(0, "ERROR, ordered_fields returned NULL, bad fields! \n");  return(-1);  }
  dfc->ordered_fields = ordered_fields;
  dfc->nfields = n_fix_fields;
  if (dfc->fxs != NULL) {
    vpt(0, " ERROR we need to clear fxs in dfc. \n");  free(dfc->fxs); dfc->fxs = NULL; 
  }
  dfc->fxs = create_blank_fix_fields(n_fix_fields, verbose - 1);
  if (dfc->fxs == NULL) {
    vpt(0, " ERROR we got fxs is null on create. \n"); return(-1);
  }
  int i_field = 0;
  iStr st_v, end_v;
  st_v = get_value_bounds("fix_fields", sf, iField, nmax, verbose-1, &st_v, &end_v); 
  if ((st_v < 0) || (sf[st_v] != '{')) {
    vpt(0,  " ERROR we tried to get bounds for fix fields but failed. \n");
    delete_fix_fields(n_fix_fields, &(dfc->fxs), verbose); return(-1);
  }
  iStr adder = 0;
  if (end_v == get_end_brace("get_n_fix_fields", sf, st_v, nmax)) {
     adder=1;
  }
  //vpt(1, " We received the fields, where they are: \n\"\"\"\n%.*s\n    \"\"\"\n",
  //  end_v-st_v, sf + st_v);
  int success_field = 0;  
  iStr iFieldLoc = -1;
  char FIELD[30]; int ncf = 0;
  for (i_field = 0; i_field < n_fix_fields; i_field++) {
    int seek_field = dfc->ordered_fields[i_field];
    sprintf(FIELD, "%d\0", (int) seek_field); 
    for (ncf = 0; ncf < 30; ncf++) { if (FIELD[ncf] == '\0') { break; }}
    iFieldLoc = find_key(FIELD,ncf,sf, st_v, end_v, verbose-3); 
    if ((iFieldLoc < 0) || (iFieldLoc >= end_v))  {
      vpt(-2, "ERROR i_field=%d/%d, seek=%d=\'%s\', iFieldLoc returned %ld for sf[%ld:%ld]\n",
        (int) i_field, (int) n_fix_fields, (int) seek_field, FIELD, iFieldLoc, (long int) st_v, (long int) end_v); 
      printf(" Why do you think we failed?");
      printf("----------------------------------------------------------------------------------------------------\n\n\n");
      return(-1); 
    }
    //vpt(0, "AT LOCATION of Field i_field=%d/%d, seek=%d=\'%.*s\', iFieldLoc returned %ld for sf[%ld:%ld] = {\n%.*s\n}\n",
    //    (int) i_field, (int) n_fix_fields, (int) seek_field, ncf, FIELD, (long int) iFieldLoc);
    //printf("---------------------------------------------------------------------------------------------------------\n\n\n");
    //return(-1);
    success_field = populate_fixfield((iStr) iFieldLoc, (iStr) end_v, (char*) sf, (int) verbose-2,
        (int) i_field, (DF_Fix_Field**) &(dfc->fxs));
    if (success_field < 0) {
        vpt(-2,  " ERROR populate_fixfield returned: %ld: after populating i_field=%ld/%ld, starting at FieldLoc=%ld. \n", 
          (long int) success_field, (long int) i_field, (long int) n_fix_fields, (long int) iFieldLoc); 
        // delete_fix_fields(n_fix_fields, &(dfc->fxs), verbose); 
        return(-1);
    }
    vpt(3, " -- i_field=%ld/%ld.  iFieldLoc=%ld/%ld, seek=%ld, success.\n",
      (long int) i_field, (long int) n_fix_fields, (long int) iFieldLoc, (long int) nmax,
      seek_field); 
  }
  vpt(2, " SUCCESS: populated %ld fix fields. \n", (long int) n_fix_fields);
  return(n_fix_fields);
}
