#pragma once

#ifndef vprintf
#define vprintf(X, Y, ...)    \
  if (verbose >= (X)) {       \
    printf( (Y),__VA_ARGS__); \
  }                           \
  verbose = verbose

#define vpt(X, Y, ...) \
  if (verbose >= (X)) {        \
    printf("%s", stt);         \
    printf( (Y),__VA_ARGS__);  \
  }                            \
  verbose = verbose
#endif

#ifndef ISTRH
#define ISTRH 0
typedef long int iStr;
#endif

#define NCHARS (26*2+10)
#ifndef LOC_CHAR
#define LOC_CHAR(on_char) \
  ((on_char >= '0' & on_char <= '9') ? (int) (on_char - '0') : \
  (on_char >= 'a' & on_char <= 'z') ? (int) (on_char - 'a' + 10) : \
  (on_char >= 'A' & on_char <= 'Z') ? (int) (on_char - 'A' + 26 + 10) : -1)
#endif

#ifndef JSONTYPES
#define JSONTYPES 0
typedef enum {
 JSON_TYPE_NUMBER,
 JSON_TYPE_QUOTE,
 JSON_TYPE_ARRAY,
 JSON_TYPE_STR,
 JSON_TYPE_DICT
} DF_Json_Type; 
#endif

#ifndef DFDATATYPES
#define DFDATATYPES 0
typedef enum {
  str,
  i32,
  i64,
  decimal153, decimal154, 
  decimal185, decimal184,
  fix42,
  tms,
  tns,
  tus,
  UNKNOWN
} DF_DataType;
#endif

