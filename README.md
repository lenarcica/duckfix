# "duckfix": a DuckDB library for processing FIX-message triggered logs
- 2026-02-03
- Alan Lenarcic
- lenarcic@post.harvard.edu
- License GPLv2 (to be honest you should rewrite this code if it is useful to you)


The following is a library designed to act as a framework for reading arbitrary FIX-message ("Financial Information eXchange") logs (https://en.wikipedia.org/wiki/Financial_Information_eXchange). 
These messages are typically sent and read between financial servers, and then recorded in a variety of proprietary formats in cold storage logs.


We consider logs of a format:
```
col1,col2,fixcol1,fixcol2,col5,...
DEAL_1,SEND_2,{"5":"N"},{"8":10.3,"10":"Account","9":"Y"},10:30:43.0323,...
DEAL_1,SEND_3,{"5":"Y"},{"10":"Hello,"10":"Other","9":"Y"},10:40:43.0323,...
... 
```

That is, each line is a mixture of CSV information and Fix log material.  FIX messages might be contained in one or more JSON structures, or they may be appended to the end of the line and separated
with a separate "fix_sep" separator character, as opposed to the comma used to separate the informational columns.  The role of a DuckDB package is to be able
to ingest these logs into a DuckDB table, to export into parquet, and join logs together in a more reasoanable message-type separated format.

Users supply fix configuration with a "config_jsons/fix_X.json" file.  Information about user-specific FIX fields will need to be entered here, as well as information about the rest of the log schmea, as well as information about separators and fix-encoding which can hopefully be adjusted to adapt to multiple log format types.

The goal is to read a CSV of fix logs quickly into DuckDB using a DuckDB compiled table function

# I. Compiling

1. Copy in extension-ci-tools library into folder.  Also copy "duckdb.h" and "duckdb_extension.h" into ```duckdb_capi``` folder.
2. ```make configure``` gets Python and Duckdb ready.
3. ```make debug```
4. ```make release```

  Currently "make debug" can be run from the home directory, assuming duckdb is installed to windows environment, or alternatively, active in an anaconda environment.

  The compiler will require "duckdb.h", "duckdb_extension.h" be copied into an assistant directory ```duckdb_capi```.  A clone of "extension-ci-tools" from github will
  have information for make files.   That said, ```ninja``` and other utilities, at least in Windows package processing, are unnecessary, as this is a simple package
  with only C files included.

  We rely on a makefile included in this package, as well as CMakeLists.txt, which should name all critical "src/*.c" files used in the package.  After testing,
  the other ```make test_debug``` and ```make release``` will be implemented.

# II. To run in Duckdb
 
  If you have duckdb, you need to uninstall any existing previous version of the package, and then run duckdb in "unsigned" mode so it can
  reinstall this user-comoiled extension.  We can test on "ex1.csv", or we can use the "py/fake_data.py" file to create a larger "ex_rand.csv" file.

  Here is a sample set of code to copy.  Note, copy starting at "del" inside a shell prompt, and have it delete the duckdb existing packages installed
  (say version 1.4.3 if thats what you have), and then turn on duckdb in --unsigned mode.  After that it reinstalls the duckfix extension, loads it, and
  then does a "from read_fixlog()" table function call, acting on "/example/ex1.csv" or others you might want.


# III. Test and running

Compiled extension for duckfix should result in the "./build/debug" or "./build/release" folder.

```
--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'DESIRED EXTENSIONS'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex1 as select * from read_fixlog(verbose := 0, char_sep := ',', fix_sep='1', 
    report_bust:=1, report_line:=1, file_name := './example/ex1.csv', json_file_name:= 'config_jsons/fix42.json');
```


As we wee above, the "del" run in shell simply deletes existing duckdb installed packages.  Then we install our target extension.  LOAD command loads the package.

The single duckdb ```table_function``` is called ```read_fixlog```, and takes a number of optional and required entries.
- ```file_name```: required name of the csv/log you want to read
- ```json_file_name```: the required name of the json configuration file with FIX field information (we read this first before parsing log)
- ```verbose```: Integer level, if you want to see a lot of debug information
- ```char_sep```: Separator single character for the CSV columns  (note this can be set in the json)
- ```fix_sep```: Alternate separator inside FIX section (note the "=/:" equals sign can also be assigned in configuration json.
- ```report_bust```: marks which lines we read were supposedly busted
- ```report_line```: report original line number
- ```keep_line_text```: a String, searches the line and only keeps lines with certain text, such ass "MSGTYPE=EXECUTE"
- ```ignore_line_text```: a String, ignore all lines that have this text.

# III. Informational JSON format
  See example in ```example/fix42.json```.  Here we supply a schema for the table, naming the columns and giving types.  Users need only supply Fix format (here example 4.20) codes, and
intended code encodings (in case those exist).  Fix message elements can be strings, single-character codes, decimals, timestamps, etc.  

  The algorithm decodes the fix messages and creates a single duckdb table of the desired columns.  This can be saved as a duckdb SQL table or saved further as a CSV.

  JSONs are read by the file "df_load.c".  Before fully reading a target CSV, we walk through the target CSV observing that all of the fix message code numbers are properly explained in fix42.json information file.  If not, we do not have a functionality to read the file until the identified columns are included with descriptors.   

  Note, the resultant table does not have to contain all of the data in the FIX log.  By setting a negative "priority" score to any column, the resultant table will ignore that column of data.  Higher number priority score data goes in the back (further rightward) of the table column list.

# IV. Fake data example
  The python code in "py/fake_data.py" shows how to read the informational JSON and create a csv of fake data, suitable for testing the algorithm.
