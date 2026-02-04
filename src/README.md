# src ".c" files
This is a first working version of duckfix, and for clarity's sake we need to clean these up a bit.  

1. df_load.c -- Loads the initial configuration json
2. read_file.c -- Uses configuration json to do first pass assessing csv of choice.
3. df_bind.c -- Bind operation of table function (step 1 of a table function)
4. df_init.c -- Init operation of table function (step 2)
5. df_main.c -- Main Table function operating on chunks of Duckdb Output table
6. duckfix.c -- Script for registering table function.
