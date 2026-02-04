# "duckfix": a DuckDB library for processing fix logs
- 2026-02-03
- Alan Lenarcic
- lenarcic@post.harvard.edu

The following is a library designed to act as a framework for reading arbitrary fix logs.

Chiefly, we expect fix messages to be a mix of multiple messages associated with a csv line.  Users supply fix configuration with a "config_jsons/fix_X.json" file.

The goal is to read a CSV of fix logs quickly into DuckDB using a DuckDB compiled table function

# I. Compiling
  Currently "make debug" can be run, assuming duckdb is installed to windows environment, or alternatively, active in an anaconda environment.

  The compiler will require "duckdb.h", "duckdb_extension.h" be copied into an assistant directory.  A clone of "extension-ci-tools" from github will
  have information for make files.

  We rely on a makefile in this package, as well as CMakeLists.txt, which should name all critical "src/*.c" files used in the package.

# II. To run in Duckdb
 
  If you have duckdb, you need to uninstall any existing previous version of the package, and then run duckdb in "unsigned" mode so it can
  reinstall this user-comoiled extension.  We can test on "ex1.csv", or we can use the "py/fake_data.py" file to create a larger "ex_rand.csv" file.

  Here is a sample set of code to copy.  Note, copy starting at "del" inside a shell prompt, and have it delete the duckdb existing packages installed
  (say version 1.4.3 if thats what you have), and then turn on duckdb in --unsigned mode.  After that it reinstalls the duckfix extension, loads it, and
  then does a "from read_fixlog()" table function call, acting on "/example/ex1.csv" or others you might want.
```
--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
from read_fixlog(verbose := 0, char_sep := ',', file_name := './example/ex1.csv', json_file_name:= 'config_jsons/fix42.json');
```
As we wee above, the "del" run in shell simply deletes existing duckdb installed packages.  Then we install our target.

