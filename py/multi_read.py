#################################################################################
## multi_read.py
##
##  guidance for multiple files.
##
##  Corpuses of log files can span multiple files for multiple customer connections.
##
## 1. Use a utility to list all files in directory and subdirectories.
##     LLMs and other python programs can create a list_from_walk.
## 2. Use Threads as prefered technique with duckdb.
##     DuckDB can open multiple local threads with "cursor" command.
##     This way DuckdB will not open in a single directory and conflict with itself.
## 3. Use Parquet saves, even though it is likely Log files will be different schema,.
##     It is not likely that logs will have uniform column/message format.
##     For large logs, it may be necessary to read in thousands of files to find all 
##     unique fields under use.
## 
##
##
##
import numpy as np; import duckdb; import pandas as pd; import polars as pl; import pyarrow as pa;
from logging import logger;
default_compression_level = 19;
"""
json_file_name = "c:/users/alanj/Dropbox/ddb/duckfix/config_jsons/fix42_t2.json";
"""

df_types = ['str','Str','STR',
            'i64','i32','int',
            'f64','f32','float','double',
            'tms','tns','tus',
            'date','DATE','Date',
            'decimal','Decimal','dec','DEC','DECIMAL','DEC','decimal_gen','decimal_general','dec_gen','Decimal_general',
            'Decimal(18,4)','decimal(18,4)','DECIMAL(18,4)','DECIMAL(18,3)','DECIMAL(18,5)','Decimal(18,5)','decimal(18,5)','dec(18,5)',
            'Decimal(15,3)','Decimal(15,4)','decimal(15,3)','DECIMAL(15,3)','DECIMAL(15,4)',
            'UNKNOWN','Unknown'];
ddf_types = ['VARCHAR','VARCHAR','VARCHAR',
             'BIGINT','INTEGER','INTEGER',
             'DOUBLE','FLOAT','FLOAT','DOUBLE',
             'TIMESTAMP_MS','TIMESTAMP_NS','TIMESTAMP',
             'DATE','DATE','DATE'] +  \
             (['DECIMAL'] * 10) + \
             (['DECIMAL'] * 8) + \
             (['DECIMAL'] * 5) +  \
             ['UNKNOWN','UNKNOWN']
mmtch = dict(zip(df_types, ddf_types));

def read_in_fields(json_file_name:str="") :
  import json; import polars as pl; import pandas as pd;
  import numpy as np; import copy;
  with open(json_file_name, 'r') as data_json:
     dtdir = json.load(data_json);
  dirfields = dtdir['fix_fields'];
  pd_b_df = pd.DataFrame.from_dict(dirfields, orient='index').reset_index();
  cols = np.array(pd_b_df.columns); cols[0] = 'tag'; pd_b_df.columns = cols;
  pddf = pd_b_df[['tag','nm','fixtitle','typ','width','scale','fmt']]
  pddf['width'] = pddf['width'].astype(np.float64);
  pddf['scale'] = pddf['scale'].astype(np.float64);
  pldf = pl.DataFrame(pddf);
  APT = copy.copy(pldf['typ'].to_pandas().replace(mmtch)).to_numpy()
  ixd = [ix for ix in range(len(APT)) if APT[ix] == 'DECIMAL'];
  for ii in ixd :
    APT[ii] = 'DECIMAL(' + str(int((pldf['width'].to_list())[ii])) + "," + str(int((pldf['scale'].to_list()[ii]))) + ")";
  pldf['ddb_typ'] = APT;
  return(pldf);

def string_fill_cols(nm_cols, pldf) :
   nm_ix = [ix for ix in range(len(pldf)) if (pldf['nm'].to_list()[ix]) in nm_cols];
   if (len(nm_ix) <= 0) :
     return("");
   rt = [ 'cast(NULL as ' + pldf['ddb_typ'].to_list()[ix] + ") as " + pldf['nm'].to_list()[ix] for ix in nm_ix];
   return(", " + ", ".join(rt));

class df_info_class :
  """
  df_info_class: Information for multifile parallel extraction for fix.
 
  Users can supply a unique svdir/svfile/smfile directory for any fix35keep/ignore_line_text/keep_line_text filters. 

  A Logger will try to track runtimes if helpful and print error logs.
  """
  def __init__(self, file_list:list[str] = None, json_file_name:str="", files_dir = "",
               verbose:int=0, keep_line_text:str="", ignore_line_text:str="",
               svdir:str="./", svfile:str="logfix", smfile:str="smfix", logdir ="logs",
               keep_line_text:str="", ignore_line_text:str="", fix35keep:str|None:None,
               fix_sep:str='1', char_sep=",", report_bust:int=0, report_line:int=0,
               compression_level:int=19, nthreads:int=-1,
               oth_entry_str: str=""
               partition_by_date:bool=False, partition_others:list[str]|None = None,
               default_date:str|None="2000-01-01", compression_level:int=default_compression_level) :
    self.verbose=verbose;  self.file_list=file_list; self.json_file_name = json_file_name;
    self.keep_line_text=keep_line_text; self.ignore_line_text=ignore_line_text;
    self.svdir = svdir; self.svfile=svfile; self.smfile=smfile;  self.fix35keep = fix35keep;
    self.fix_sep=fix_sep;  self.char_sep = char_sep; self.files_dir = files_dir if files_dir is not None else "";
    self.svdir = svdir; self.logdir = logdir;
    self.svfile =svfile; self.smfile=smfile;  self.compression_level=compression_level;
    self.nthreads=nthreads;
    self.partition_by_date=partition_by_date;  self.partition_others=partition_others;  self.default_date=default_date;
    self.oth_entry_str = oth_entry_str;
    self.compression_level = int(compression_level);
    os.makedirs(self.svdir, exist_ok=True); os.makedirs(self.svdir + "/success", exist_ok=True); os.makedirs(self.svdir + "/fail", exist_ok=True);
    os.makedirs(self.svdir + "/summary", exist_ok=True);
    if (self.verbose >= 1) :
      print("df_info_class: generating a logger to logdir");
      self.logger =  logger(self.logdir);
    else :
      self.logger=None;

  def emit_sql(ii: int|None = None, file_name: str | None = None) :
    if ((ii is not None) and (isinstance(ii, int)) and (ii >= 0) and (self.file_list is not None) and \
        (len(self.file_list) > ii)) :
      file_name = file_list[ii];
    elif (isinstance(file_name, str) and (len(file_name) > 0)) :
      pass;
    rtt = "from read_fix(file_name:='" + file_name + "', \n" + \
          "   json_file_name:='" + self.json_file_name + "' \n" + \
    if (isinstance(self.verbose, int)) :
      rtt = rtt + ",    verbose:=" + str(self.verbose); 
    if (isinstance(self.keep_line_text, str) and (len(self.keep_line_text) > 0)) :
      rtt = rtt + ",    keep_line_text:='" + self.keep_line_text + "'\n" + \
    if (isinstance(self.ignore_line_text,str) and (len(self.ignore_line_text) > 0)) :
      rtt = rtt + ",    ignore_line_text:='" + self.ignore_line_text + "'\n" + \ 
    if (isinstance(self.fix35keep,str) and (len(self.fix35keep) > 0)) :
      rtt = rtt + ",    fix35keep:='" + self.fix35keep + "'\n" + \ 
    if (isinstance(self.fix_sep,str) and (len(self.fix_sep) > 0)) :
      rtt = rtt + ",    fix_sep:='" + self.fix_sep + "'\n" + \ 
    if (isinstance(self.char_sep,str) and (len(self.char_sep) > 0)) :
      rtt = rtt + ",    char_sep:='" + self.char_sep + "'\n" + \ 
    rtt = rtt + ")\n";
    return(emit_sql);

def MultiFile(ii:int, local_con, info_class:df_info_class, ithread:int=-1, default_date:str|None=None) :
  from pathlib import Path;  import time;
  if (default_date is not None) and (len(default_date) > 0) :
    info_class.default_date = default_date;
  default_date = None if ((info_class.default_date is None) or (len(info_class.default_date) <= 0)) else info_class.default_date;
  onfile = info_class.file_list[ii];
  full_path = Path(onfile); filename = full_path.name; subdir = full_path.parent;
  ENDStr = "ERRORFAIL";
  compression_text = "(FORMAT PARQUET, compression ZSTD, COMPRESSION_LEVEL " + str(info_class.compression_level) + ")";
  parts = [] if (self.partition_others is None) else list(self.partition_others);
  parts = (['subdir'] if (self.partition_subdir==True) else []) + parts;
  parts = (['dt'] if self.partition_by_date ==True else []) + parts;
  
  import copy;  oth_parts = copy.copy(parts); dtB = ""; dtS = "";
  if 'dt' == parts[0] :
    dtB = "/dt=" + info_class.default_date
    os.makedirs(info_class_svdir + "/fail/" + dtB, exist_ok=True);
    os.makedirs(info_class_svdir + "/success/" + dtB, exist_ok=True);
    oth_parts = [parts[ix] for ix in range(len(parts)) if ix != 0 ];
  if 'subdir' == oth_parts[0] :
    dtS = "/subdir=" subdir;
    os.makedirs(info_class_svdir + "/fail/" + dtB + dtS, exist_ok=True);
    os.makedirs(info_class_svdir + "/success/" + dtB + dtS, exist_ok=True);
  time_00 = time.time();
  try :
    OutTab = local_con.sql("create or replace table ontab_" + str(ii) " as \n" + \
                         "   select *, '" + filename + "' as file, '" + subdir + "' as subdir  \n" + \
                         ("" if ((info_class.oth_entry_str is None) or (info_class.oth_entry_str in [""," ","  ","   "]))  \
                             else (", " + info_class.oth_entry_str)) + \
                         ("" if (default_date is None) else (", CAST('" + default_date +"' as DATE) as dt")) + \
                         "   " + info_class.emit_sql(ii) + "; from on_tab_" + str(ii) + ";");
    if (OutTab.columns[0] == 'unknown_field_num') :

      OutTab = OutTab.with_columns([pl.lit(filename).alias('file'), pl.lit(subdir).alias('subdir')]);
      OutTab.write_parquet(info_class.svdir + "/fail/fail_" + info_tab.svfile + "_" + str(ii) + ".parquet");
      ENDStr = "FAIL";
    else :
      DSQL = "COPY ontab_" + str(ii) + " TO \n" + \
             " '" + info_class.svdir + "/success/" + dtB + dtS + "/success_" + str(ii) + ".parquet' \n" + \
             compression_text;
      local_con.sql(DSQL);
      SumTab = local_con.sql("select *, '" + filename + "' as file, '" + subdir + "' as parent, " + str(ii) " as filei from " + \
                         "(summarize ontab_" + str(ii) + ")").pl();
      DSumSQL = "COPY (select * from SumTab)  TO \n" + \
                " '" + info_class.svdir + "/summ" + dtB + dtS + "/summ_" + str(ii) + ".parquet" + \
                compression_text;
      local_con.sql(DSumSQL)
      ENDStr = "SUCCESS";
    local_con.sql("drop table ontab_" + str(ii) + ";");
  except Exception as e :
    info_class.logger.error(" We have an Error on thread " + str(ithread) + ",on file ii=" + str(ii) + ":" + filename + "," + subdir + ",e=" + str(e));
  time_END = time.time();
  if (info_class.verbose >= 1) :
    LTT = ("MultiFile(ii=" + str(ii) + ", Thread=" + str(ithread) + "/" + str(info_tab.nthreads) + "): " + \
           " -- complete file " + str(ii) + ", " + ENDStr + " on " + filename + ", [RESULT-LEN: " + str(len(OutTab)) + "]," + \
           " [tm-File_" + str(ii) + ": " + str(time_END-time_00) + " sec]");
    if (info_class.logger is not None) :
      info_class.logger.info(LTT);
    print(LTT); 
 
def MultiThread(global_con, info_class:df_info_class|None = None) :
  import time;
  local_con = global_con.local_cursor(); 
  ithread = local_con.thread;
  FLN = len(info_class.file_list)
  doII = [x for x in range(len(FLN)) if ( (x%nthreads)-ithread) == 0 ]
  time_00 = time.time();
  for ii in doII :
    MultiFile(ii, local_con, info_class, ithread=ithread)
  if (info_class.verbose >= 1) :
    LTT = ("MultiThread[Thread=" + str(ithread) + "] -- completed. [tm-MultiThread=" + str(timeEND-time00) + " sec]");
    if (info_class.logger is not None) :
      info_class.logger.info(LTT); 
    print(LTT); 
  local_con.close(); 



def GlobalExtract(file_list:list[str],json_file_name:str="", files_dir="", local_con=None, verbose:int=1, 
  keep_line_text:str|None=None, ignore_line_text:str|None,fix35keep:str|None=None, svdir:str="", svfile:str="logfix", smfile:str="sumfix"
  report_bust:str|None=0, report_line:str|None=0, fix_sep:str|None='1',char_sep:str|None=',',nthreadss=8, logdir="") :
  import duckdb;
  from pathlib import Path;
  logdir = logdir if ((logdir is not None) and (logdir != "")) else (svdir + "/log");
  info_class = df_info_class(file_list=file_list, json_file_name=json_file_name, files_dir=files_dir,
      verbose=verbose, keep_line_text=keep_line_text, report_line=report_line, report_bust=report_bust,
      ignore_line_text=ignore_line_text, fix35keep=fix35keep, fix_sep=fix_sep,char_sep=char_sep, 
      svdir=svdir, logdir=logdir,
      svfile=svfile, smfile=smfile, svdir=svdir, nthreads=nthreads);
  global_con = duckdb.connect();
  threads = [];
  for ii in range(nthreads) :
    threads.appen(Thread([MultiThread, (global_con, info_class)));
  if (verbose >= 1) :
    print("------------------------------------------------------------------------");
    print("GlobalExtract() initiating with " + str(nthreads) + " threads.");
  for ii in range(nthreads) :
    thread[ii].start();
  for ii in range(nthreads) :
    thread[ii].join();
  if (verbose >= 1) :
    print("GlobalExtrac() all threads completed.");
    print("------------------------------------------------------------------------");
