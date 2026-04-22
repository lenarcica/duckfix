--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex1 as select * from read_fixlog(verbose := 0, char_sep := ',', file_name := './example/ex1.csv', json_file_name:= 'config_jsons/fix42.json');


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table exfmt2 as select * from read_fixlog(verbose := 4, char_sep := ' ', file_name := './example/ex_fmt2.csv', json_file_name:= 'config_jsons/fix42_t2.json', default_date='2026-01-02'); from exfmt2;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex2 as select * from read_fixlog(verbose := 2, char_sep := ' ', file_name := './example/ex_random2.csv', json_file_name:= 'config_jsons/fix42_t2.json'); from ex2;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/release/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex2 as select * from read_fixlog(verbose := 0, char_sep := ' ', file_name := './example/ex_random2.csv', json_file_name:= 'config_jsons/fix42_t2.json',
   default_date:='2026-02-01'); from ex2;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex2A as select * from read_fixlog(verbose := 2, char_sep := ' ', report_bust:= 1, file_name := './example/ex_random2.csv', json_file_name:= 'config_jsons/fix42_t2.json', keep_line_text:='{A:}' ); from ex2A;



--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex3A as select * from read_fixlog(verbose := 1, char_sep := ' ', file_name := './example/ex_random3.csv', json_file_name:= 'config_jsons/fix42_t2.json', keep_line_text:='{A:}' ); from ex3A;


del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex4 as select * from read_fixlog(verbose := 1, char_sep := ' ', fix_sep:='|', report_bust:=1, report_line:=1, file_name := './example/ex_random4small.csv', json_file_name:= 'config_jsons/fix42_t4.json' ); from ex4;

del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex4A as select * from read_fixlog(verbose := 1, char_sep := ' ', fix_sep='|', report_bust:= 1, report_line:= 1, file_name := './example/ex_random4.csv', json_file_name:= 'config_jsons/fix42_t4.json', keep_line_text:='{A:}' ); from ex4A;



--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex3m as select * from read_fixlog(verbose := 1, char_sep := ' ', file_name := './example/ex_random3.csv', json_file_name:= 'config_jsons/fix42_miss.json', keep_line_text:='{A:}' ); from ex3m;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex3 as select * from read_fixlog(verbose := 0, char_sep := ' ', file_name := './example/ex_random3.csv', json_file_name:= 'config_jsons/fix42_t2.json'); from ex3;

--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/release/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex3 as select * from read_fixlog(verbose := 0, char_sep := ' ', file_name := './example/ex_random3.csv', json_file_name:= 'config_jsons/fix42_t2.json'); from ex3;

--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex2A as select * from read_fixlog(verbose := 2, char_sep := ' ', file_name := './example/ex_random2.csv', json_file_name:= 'config_jsons/fix42_t2.json', ignore_line_text:='{A:}' ); from ex2A;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex2s as select * from read_fixlog(verbose := 0, char_sep := ' ', file_name := './example/ex_random2testshort.csv', json_file_name:= 'config_jsons/fix42_t2.json'); from ex2s;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table ex2 as select * from read_fixlog(verbose := 4, char_sep := ' ', file_name := './example/ex_random2.csv', json_file_name:= 'config_jsons/fix42_t2.json'); from ex2;


--del c:\users\alanj\ddb\v1.4.3\windows_amd64\duckfix* & duckdb --unsigned
SET extension_directory = 'c:/users/alanj/ddb'; 
INSTALL './build/debug/duckfix.duckdb_extension';
LOAD duckfix;
-- Now to run the function you declared:
create or replace table exT as select * from read_fixlog(verbose := 0, char_sep := ' ', file_name := './example/ex_randomT.csv', json_file_name:= 'config_jsons/fix42_t4.json',
  default_date='2026-04-20'); from exT;

As we wee above, the "del" run in shell simply deletes existing duckdb installed packages. Then we install our target.

III. Informational JSON format
See example in example/fix42.json. Here we supply a schema for the table, naming the columns and giving types. Users need only supply Fix format (here example 4.20) codes, and intended code encodings (in case those exist). Fix message elements can be strings, single-character codes, decimals, timestamps, etc.

The algorithm decodes the fix messages and creates a single duckdb table of the desired columns. This can be saved as a duckdb SQL table or saved further as a CSV.
