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

// Maximum characters, 2^16 for a File buffer read (hopefully longer than any line in a document)
#ifndef MAXREAD
#define MAXREAD 65536 
#endif

#ifndef MAXINTCHAR
#define MAXINTCHAR 20
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
  decimal_gen,
  enum_date,
  fix42,
  f32,
  f64,
  tms,
  tns,
  tus,
  UNKNOWN
} DF_DataType;
#endif

typedef enum{
  YYYYcmmcddtHHcMMcSScF,
  YYYYmmddHHMMSSF,
  YYcmmcddtHHcMMcSScF,
  HHcMMcSScF,
  HHMMSSF,
  YYYYcmmcdd,
  YYYYmmdd,
  MonthcDaycYear,
  MonthcDaycYearcHHcMMcSScF,
  MonthcDaycYearcHHcMMcSS,
  YearcMonthcDay,
  YearcMonthcDaycHHcMMcSScF,
  YearcMonthcDaycHHcMMcSS,
  UNKNOWN_TS
} DF_TSType;

#define MATCHMONTH(sf, st_v0, end_v0) \
 ((str_eq("Jan", 3, sf, (st_v0), end_v0)) ? 1 : \
  (str_eq("jan", 3, sf, (st_v0), end_v0)) ? 1 : \
  (str_eq("JAN", 3, sf, (st_v0), end_v0)) ? 1 : \
  (str_eq("January", 7, sf, (st_v0), end_v0)) ? 1 : \
  (str_eq("JANUARY", 7, sf, (st_v0), end_v0)) ? 1 : \
  (str_eq("feb", 3, sf, (st_v0), end_v0)) ? 2 : \
  (str_eq("Feb", 3, sf, (st_v0), end_v0)) ? 2 : \
  (str_eq("FEB", 3, sf, (st_v0), end_v0)) ? 2 : \
  (str_eq("February", 8, sf, (st_v0), end_v0)) ? 2 : \
  (str_eq("FEBRUARY", 8, sf, (st_v0), end_v0)) ? 2 : \
  (str_eq("mar", 3, sf, (st_v0), end_v0)) ? 3 : \
  (str_eq("Mar", 3, sf, (st_v0), end_v0)) ? 3 : \
  (str_eq("MAR", 3, sf, (st_v0), end_v0)) ? 3 : \
  (str_eq("MARCH", 5, sf, (st_v0), end_v0)) ? 3 : \
  (str_eq("March", 5, sf, (st_v0), end_v0)) ? 3 : \
  (str_eq("apr", 3, sf, (st_v0), end_v0)) ? 4 : \
  (str_eq("Apr", 3, sf, (st_v0), end_v0)) ? 4 : \
  (str_eq("APR", 3, sf, (st_v0), end_v0)) ? 4 : \
  (str_eq("APRIL", 5, sf, (st_v0), end_v0)) ? 4 : \
  (str_eq("April", 5, sf, (st_v0), end_v0)) ? 4 : \
  (str_eq("may", 3, sf, (st_v0), end_v0)) ? 5 : \
  (str_eq("May", 3, sf, (st_v0), end_v0)) ? 5 : \
  (str_eq("MAY", 3, sf, (st_v0), end_v0)) ? 5 : \
  (str_eq("jun", 3, sf, (st_v0), end_v0)) ? 6 : \
  (str_eq("Jun", 3, sf, (st_v0), end_v0)) ? 6 : \
  (str_eq("JUN", 3, sf, (st_v0), end_v0)) ? 6 : \
  (str_eq("June", 4, sf, (st_v0), end_v0)) ? 6 : \
  (str_eq("JUNE", 4, sf, (st_v0), end_v0)) ? 6 : \
  (str_eq("jul", 3, sf, (st_v0), end_v0)) ? 7 : \
  (str_eq("Jul", 3, sf, (st_v0), end_v0)) ? 7 : \
  (str_eq("JUL", 3, sf, (st_v0), end_v0)) ? 7 : \
  (str_eq("July", 4, sf, (st_v0), end_v0)) ? 7 : \
  (str_eq("JULY", 4, sf, (st_v0), end_v0)) ? 7 : \
  (str_eq("aug", 3, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("Aug", 3, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("AUG", 3, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("August", 6, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("AUGUST", 6, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("aug", 3, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("Aug", 3, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("AUG", 3, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("August", 6, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("AUGUST", 6, sf, (st_v0), end_v0)) ? 8 : \
  (str_eq("sep", 3, sf, (st_v0), end_v0)) ? 9 : \
  (str_eq("Sep", 3, sf, (st_v0), end_v0)) ? 9 : \
  (str_eq("SEP", 3, sf, (st_v0), end_v0)) ? 9 : \
  (str_eq("September", 9, sf, (st_v0), end_v0)) ? 9 : \
  (str_eq("SEPTEMBER", 9, sf, (st_v0), end_v0)) ? 9 : \
  (str_eq("oct", 3, sf, (st_v0), end_v0)) ? 10 : \
  (str_eq("Oct", 3, sf, (st_v0), end_v0)) ? 10 : \
  (str_eq("OCT", 3, sf, (st_v0), end_v0)) ? 10 : \
  (str_eq("October", 7, sf, (st_v0), end_v0)) ? 10 : \
  (str_eq("OCTOBER", 7, sf, (st_v0), end_v0)) ? 10 : \
  (str_eq("nov", 3, sf, (st_v0), end_v0)) ? 11 : \
  (str_eq("Nov", 3, sf, (st_v0), end_v0)) ? 11 : \
  (str_eq("NOV", 3, sf, (st_v0), end_v0)) ? 11 : \
  (str_eq("November", 8, sf, (st_v0), end_v0)) ? 11 : \
  (str_eq("NOVEMBER", 8, sf, (st_v0), end_v0)) ? 11 : \
  (str_eq("dec", 3, sf, (st_v0), end_v0)) ? 12 : \
  (str_eq("Dec", 3, sf, (st_v0), end_v0)) ? 12 : \
  (str_eq("DEC", 3, sf, (st_v0), end_v0)) ? 12 : \
  (str_eq("December", 8, sf, (st_v0), end_v0)) ? 12 : \
  (str_eq("DECEMBER", 8, sf, (st_v0), end_v0)) ? 12 : \
  -1)
/*
/*
#ifndef DEF_TS_str 
char strpY_pm_pd_pH_pM_pS_pf[] = "%Y-%m-%d %H:%M:%S.%f";
char strpY_pm_pd_pH_pM_pS_pF[] = "%Y-%m-%d %H:%M:%S.%F";
char strpY_pm_pdTpH_pM_pS_pf[] = "%Y-%m-%d %H:%M:%S.%f";
char strpY_pm_pdTpH_pM_pS_pF[] = "%Y-%m-%d %H:%M:%S.%F";
char strpH_pM_pS_pf[] = "%H:%M:%S.%f";
char strpH_pM_pS_pF[] = "%H:%M:%S.%F";

  (str_eq(strpH_pM_pS_pf, 11, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
  (str_eq(strpH_pM_pS_pF, 11, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
#define DEF_TS_str 0
#endif

  (str_eq(strpY_pm_pd_pH_pM_pS_pf, 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq(strpY_pm_pdTpH_pM_pS_pf, 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
*/
#define MATCHTSTYPE(sf, st_v0, end_v0) \
 ((str_eq("YYYY-mm-ddTHH:MM:SS.F", 21, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY-mm-ddTHH:MM:SS.f", 21, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY.mm.ddDHH:MM:SS.F", 21, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY-mm-dd HH:MM:SS.f", 21, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY.mm.dd HH:MM:SS.F", 21, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY-mm-ddTHH:MM:SS.FFF", 23, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY-mm-ddTHH:MM:SS.FFFFFF", 26, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YYYY-mm-ddTHH:MM:SS.FFFFFFFFF", 29, sf, (st_v0+1), end_v0)) ? YYYYcmmcddtHHcMMcSScF : \
  (str_eq("YY-mm-ddTHH:MM:SS.FFFFFFFFF", 27, sf, (st_v0+1), end_v0)) ? YYcmmcddtHHcMMcSScF : \
  (str_eq("YY-mm-ddTHH:MM:SS.F", 19, sf, (st_v0+1), end_v0)) ? YYcmmcddtHHcMMcSScF : \
  (str_eq("YY.mm.ddTHH:MM:SS.F", 19, sf, (st_v0+1), end_v0)) ? YYcmmcddtHHcMMcSScF : \
  (str_eq("YY.mm.ddDHH:MM:SS.F", 19, sf, (st_v0+1), end_v0)) ? YYcmmcddtHHcMMcSScF : \
  (str_eq("YY.MM.DDTHH:MM:SS.F", 19, sf, (st_v0+1), end_v0)) ? YYcmmcddtHHcMMcSScF : \
  (str_eq("YY.MM.DDDHH:MM:SS.F", 19, sf, (st_v0+1), end_v0)) ? YYcmmcddtHHcMMcSScF : \
  (str_eq("%%Y-%%m-%%dT%%H:%%M:%%S.%%F", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%%Y-%%m-%%d %%H:%%M:%%S.%%F", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%%Y-%%m-%%d %%H:%%M:%%S.%%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%%Y-%%m-%%d %%H:%%m:%%s.%%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%%Y-%%m-%%d %%H:%%m:%%s.%%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y-%m-%d %H:%m:%s.%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y-%m-%d %H:%M:%S.%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y-%m-%d %H:%M:%S.%F", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y-%M-%D %H:%M:%S.%F", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y-%m-%dT%H:%m:%s.%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y.%m.%dD%H:%m:%s.%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%Y.%M.%DD%H:%m:%s.%f", 20, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%%Y-%%m-%%d %%H:%%M:%%S.%%f", 27, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("%%Y-%%m-%%dT%%H:%%M:%%S.%%f", 27, sf, (st_v0+1), (end_v0))) ? YYYYcmmcddtHHcMMcSScF :  \
  (str_eq("YYYYmmddHHMMSSF", 15, sf, (st_v0+1), (end_v0))) ? YYYYmmddHHMMSSF : \
  (str_eq("%%Y%%m%%d%%H%%M%%S%%F", 14, sf, (st_v0+1), (end_v0))) ? YYYYmmddHHMMSSF : \
  (str_eq("%%H%%M%%S%%F", 8, sf, (st_v0+1), (end_v0))) ? HHMMSSF : \
  (str_eq("%%H:%%M:%%S.%%F", 11, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
  (str_eq("%H:%M:%S.%f", 11, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
  (str_eq("%%H%%M%%S%%f", 8, sf, (st_v0+1), (end_v0))) ? HHMMSSF : \
  (str_eq("%%H:%%M:%%S.%%f", 11, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
  (str_eq("%%H:%%M:%%S.%%f", 15, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
  (str_eq("%%H:%%M:%%S.%%F", 15, sf, (st_v0+1), (end_v0))) ? HHcMMcSScF : \
  (str_eq("YYYY-mm-dd",10,sf, (st_v0+1), (end_v0))) ? YYYYcmmcdd : \
  (str_eq("%%Y-%%m-%%d",8, sf, (st_v0+1), (end_v0))) ? YYYYcmmcdd : \
  (str_eq("%%Y%%m%%d",6, sf, (st_v0+1), end_v0)) ? YYYYmmdd : \
  (str_eq("YYYYmmdd",8, sf, (st_v0+1), end_v0)) ? YYYYmmdd : \
  (str_eq("Month Day, Year",15,sf,(st_v0+1), end_v0)) ? MonthcDaycYear :\
  (str_eq("Month Day, Year HH:MM:SS.f",26,sf,(st_v0+1), end_v0)) ? MonthcDaycYearcHHcMMcSScF : \
  (str_eq("Month Day, Year HH:MM:SS.F",26,sf,(st_v0+1), end_v0)) ? MonthcDaycYearcHHcMMcSScF : \
  (str_eq("Month Day, Year %H:%M:%S.%f",27,sf,(st_v0+1), end_v0)) ? MonthcDaycYearcHHcMMcSScF : \
  (str_eq("Month Day, Year %H:%M:%S.%F",27,sf,(st_v0+1), end_v0)) ? MonthcDaycYearcHHcMMcSScF : \
  (str_eq("Month Day, Year %H:%M:%S",25,sf,(st_v0+1), end_v0)) ? MonthcDaycYearcHHcMMcSS : \
  (str_eq("Year Month Day",14,sf,(st_v0+1), end_v0)) ? YearcMonthcDay : \
  (str_eq("Year Month Day HH:MM:SS.f",25,sf,(st_v0+1), end_v0)) ? YearcMonthcDaycHHcMMcSScF : \
  (str_eq("Year Month Day HH:MM:SS.F",25,sf,(st_v0+1), end_v0)) ? YearcMonthcDaycHHcMMcSScF : \
  (str_eq("Year Month Day HH:MM:SS",23,sf,(st_v0+1), end_v0)) ? YearcMonthcDaycHHcMMcSS : \
  UNKNOWN_TS)

#define What_DF_TSType(on_typ)  \
 (((on_typ) == YYYYcmmcddtHHcMMcSScF) ? "YYYY-mm-dd_HH:MM:SS.F" :  \
  ((on_typ) == YYYYmmddHHMMSSF) ? "YYYYmmddHHMMSSF" :  \
  ((on_typ) == YYYYcmmcdd) ? "YYYY-mm-dd" : \
  ((on_typ) == YYYYmmdd) ? "YYYYmmdd" : \
  ((on_typ) == HHcMMcSScF) ? "HH:MM:SS.F" : \
  ((on_typ) == HHMMSSF) ? "HHMMSSF" : \
  ((on_typ) == UNKNOWN_TS) ? "LABELLED_UNKNOWN" : \
  "NOT_RECOGNIZED_TS")

#ifndef MATCHTYPE 
#define MATCHTYPE(sf, st_v0, end_v0) \
   (str_eq("i32",3,sf, (st_v0+1), end_v0)) ?  i32 :  \
   (str_eq("str",3,sf, (st_v0+1), end_v0)) ? str :   \
   (str_eq("Str",3,sf, (st_v0+1), end_v0)) ? str :   \
   (str_eq("i64",3,sf, (st_v0+1), end_v0)) ? i64 :   \
   (str_eq("float",5,sf,(st_v0+1), end_v0)) ? f64: \
   (str_eq("f64",3,sf,(st_v0+1), end_v0)) ? f64: \
   (str_eq("f32",3,sf,(st_v0+1), end_v0)) ? f32: \
   (str_eq("date",4,sf,(st_v0+1), end_v0)) ? enum_date: \
   (str_eq("enum_date",9,sf,(st_v0+1), end_v0)) ? enum_date: \
   (str_eq("decimal153",10,sf, (st_v0+1), end_v0)) ? decimal153 : \
   (str_eq("decimal185",10,sf, (st_v0+1), end_v0)) ? decimal185 : \
   (str_eq("decimal154",10,sf, (st_v0+1), end_v0)) ? decimal153 : \
   (str_eq("decimal153",10,sf, (st_v0+1), end_v0)) ? decimal153 : \
   (str_eq("decimal185",10,sf, (st_v0+1), end_v0)) ? decimal185 : \
   (str_eq("decimal154",10,sf, (st_v0+1), end_v0)) ? decimal153 : \
   (str_eq("decimal184",10,sf, (st_v0+1), end_v0)) ? decimal185 : \
   (str_eq("Decimal(15,3)",13,sf, (st_v0+1), end_v0)) ? decimal153 : \
   (str_eq("Decimal(15,3)",13,sf, (st_v0+1), end_v0)) ? decimal153 : \
   (str_eq("Decimal(18,5)",13,sf, (st_v0+1), end_v0)) ? decimal185 : \
   (str_eq("Decimal(18,4)",13,sf, (st_v0+1), end_v0)) ? decimal184 : \
   (str_eq("Decimal(15,4)",13,sf, (st_v0+1), end_v0)) ? decimal154 : \
   (str_eq("decimal",7,sf, (st_v0+1), end_v0)) ? decimal_gen : \
   (str_eq("Decimal",7,sf, (st_v0+1), end_v0)) ? decimal_gen : \
   (str_eq("fix42",5,sf, (st_v0+1), end_v0)) ? fix42 : \
   (str_eq("tus",3,sf, (st_v0+1), end_v0)) ? tus : \
   (str_eq("tns",3,sf, (st_v0+1), end_v0)) ? tns : \
   (str_eq("tms",3,sf, (st_v0+1), end_v0)) ? tms : \
   UNKNOWN
#endif

#ifndef What_DF_DataType
#define What_DF_DataType(on_typ)  \
  ((on_typ) == str) ? "str" :  \
  ((on_typ) == i32) ? "i32" :  \
  ((on_typ) == i64) ? "i64" : \
  ((on_typ) == f64) ? "f64" : \
  ((on_typ) == f32) ? "f32" : \
  ((on_typ) == enum_date) ? "date" : \
  ((on_typ) == decimal153) ? "Decimal(15,3)" : \
  ((on_typ) == decimal185) ? "Decimal(18,5)" : \
  ((on_typ) == decimal154) ? "Decimal(15,4)" : \
  ((on_typ) == decimal184) ? "Decimal(18,4)" : \
  ((on_typ) == decimal_gen) ? "Decimal_General" : \
  ((on_typ) == fix42) ? "fix42" : \
  ((on_typ) == tms) ? "tms" : \
  ((on_typ) == tns) ? "tns" : \
  ((on_typ) == tus) ? "tus" : \
  ((on_typ) == UNKNOWN) ? "LABELLED_UNKNOWN" : \
  "NOT_FOUND"
#endif

#ifndef DFSCHEMA
#define DFSCHEMA 0
typedef struct _DFSchema {
  char *nm;
  char *desc;
  DF_DataType typ;
  char *timestamp_format; DF_TSType fmttyp;
  char width,scale; // Only really necessary for Decimals
  int priority;
  int final_loc;
} DF_Schema;
#endif

#ifndef DFFIXFIELD
#define DFFIXFIELD 0
typedef struct _DF_Fix_Field {
  int field_code;
  short keep; int priority;
  char* fixtitle; char *desc;
  DF_DataType typ; 
  char *fmt; DF_TSType fmttyp; char width, scale;
  char field_loc[NCHARS];
  int n_field_codes;
  iStr *field_codes_loc;
  char *field_codes_arr; 
} DF_Fix_Field;

typedef struct _DF_config_file {
  char *name;
  char *desc; 
  char *info;
  char *exampleline;
  int n_schemas;
  DF_Schema *schemas;
  int nfields;
  int *ordered_fields;
  DF_Fix_Field *fxs;
  int n_total_print_columns;
  int *mark_visited;
} DF_config_file; 
#endif

#ifndef DFFIELDLIST
#define DFFIELDLIST 0
typedef struct _DF_field_list {
  char char_sep;
  int n_known_fields;
  int *ordered_known_fields;  // min and max are at both ends
  int *known_usage_count;
  int *final_known_print_loc;
  int alloc_unknown;
  int num_unknown;
  int *ordered_unknown_fields;
  int *unknown_usage_count;
  int *line_unknown;
  int num_used_known_fields;

  long int file_total_bytes;
  int finish;
  int standard_vector_size;
  int n_total_lines;
  int alloc_line_loc;
  int n_loc_lines;
  int *line_locs;
} DF_field_list;
#endif


#ifndef PUSH_OUT_WHITE
#define PUSH_OUT_WHITE()   \
  if ((sf[ii] == '{') || (sf[ii]=='[') || (sf[ii]=='}')) {             \
  } else {                                                             \
    for (;ii < nmax; ii++) {                                           \
      if ((sf[ii] == ' ') || (sf[ii] == '\t') || (sf[ii]=='\n')) {     \
      } else {                                                         \
        break;                                                         \
      }                                                                \
    }                                                                  \
  }                                                                    \
  if (ii >= nmax) {                                                    \
    vpt(0, "ERROR: in df_general.h "                                   \
      "PUSH_OUT_WHITE(), over NMAX at ii=%ld/%ld. \n",                 \
      (long int) ii,                                                   \
      (long int) nmax);                                                \
  }                                                                    \
  ii+=0
#endif

#ifndef NEXTCOMMA
#define NEXTCOMMA() \
for(;ii < end_ln; ii++) { \
  if (sf[ii] == '\n') { break;  \
  } else if (sf[ii] == ',') { break;  \
  } else if ((sf[ii] == ' ') || (sf[ii] == '\t')) { \
  } else if (sf[ii]=='\"') { \
    ii = get_end_quote("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='[') { \
    ii =  get_end_bracket("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='{') { \
    ii =  get_end_brace("nextcomma", sf, ii, end_ln); \
  } \
} \
ii++ 
#endif

#ifndef NEXTCHARSEP
#define NEXTCHARSEP() \
for(;ii < end_ln; ii++) { \
  if (sf[ii] == '\n') { break; \
  } else if (sf[ii] == dfl->char_sep) { break;  \
  } else if ((sf[ii] == ' ') || (sf[ii] == '\t')) { \
  } else if (sf[ii]=='\"') { \
    ii = get_end_quote("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='[') { \
    ii =  get_end_bracket("nextcomma",sf,ii,end_ln); \
  } else if (sf[ii]=='{') { \
    ii =  get_end_brace("nextcomma", sf, ii, end_ln); \
  } \
} \
ii++ 
#endif
