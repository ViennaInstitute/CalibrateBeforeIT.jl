
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)

import CalibrateBeforeIT as CBit

## Set some parameters for the data-downloading process
global const eurostat_path = "data/010_eurostat_tables"
mkpath(eurostat_path)
conn = CBit.DuckDB.DBInterface.connect(CBit.DuckDB.DB)

##------------------------------------------------------------
## Step 1: Append the three Figaro tables into one. Eurostat splits the Figaro
## IO tables into three separate tables. For us to query them efficiently, we
## append them onto each other and save them again as a .parquet file.
result_figaro = CBit.combine_tables(eurostat_path;
    conn=conn, skip_if_missing=false)
@info "FIGARO combination result: $(result_figaro.output_file)"


##------------------------------------------------------------
## For une_rt_a, keep only non-missing entries
sqlquery = """
COPY (
SELECT * FROM '$(pqfile("une_rt_a"))'
WHERE value IS NOT NULL
)
TO '$(pqfile("une_rt_a"))' (FORMAT parquet)
"""
execute(conn,sqlquery);

sqlquery = """
COPY (
WITH table1 as (
SELECT * FROM '$(pqfile("une_rt_a"))'
WHERE value IS NOT NULL
), table2 as (
SELECT * FROM '$(pqfile("une_rt_a_h"))'
WHERE value IS NOT NULL
)
SELECT * FROM table1
UNION
SELECT * FROM table2
WHERE NOT EXISTS (
   SELECT 1
   FROM table1
   WHERE table1.time = table2.time
   AND table1.freq = table2.freq
   AND table1.age = table2.age
   AND table1.unit = table2.unit
   AND table1.sex = table2.sex
   AND table1.geo = table2.geo
))
TO '$(pqfile("une_rt_a"))' (FORMAT parquet)
"""
res_une_rt_a = execute_debug(conn,sqlquery);


##------------------------------------------------------------
## For une_rt_q, keep only non-missing entries
sqlquery = """
COPY (
SELECT * FROM '$(pqfile("une_rt_q"))'
WHERE value IS NOT NULL
)
TO '$(pqfile("une_rt_q"))' (FORMAT parquet)
"""
execute(conn,sqlquery);

sqlquery = """
COPY (
WITH table1 as (
SELECT * FROM '$(pqfile("une_rt_q"))'
WHERE value IS NOT NULL
), table2 as (
SELECT * FROM '$(pqfile("une_rt_q_h"))'
WHERE value IS NOT NULL
)
SELECT * FROM table1
UNION
SELECT * FROM table2
WHERE NOT EXISTS (
   SELECT 1
   FROM table1
   WHERE table1.time = table2.time
   AND table1.freq = table2.freq
   AND table1.age = table2.age
   AND table1.unit = table2.unit
   AND table1.sex = table2.sex
   AND table1.geo = table2.geo
   AND table1.s_adj = table2.s_adj
))
TO '$(pqfile("une_rt_q"))' (FORMAT parquet)
"""
res_une_rt_q = execute_debug(conn,sqlquery);

##------------------------------------------------------------
## Step 2: Create bd_9ac_l_form_a64 from bd_9ac_l_form_r2
success, rows = CBit.create_business_demographic_a64_data("bd_9ac_l_form_r2",
    eurostat_path, conn)
success, rows = CBit.create_business_demographic_a64_data("bd_l_form",
    eurostat_path, conn)
@info "Step 4 completed" success rows


##------------------------------------------------------------
## Step 3: Create sbs_na_sca_a64 from sbs_na_sca_r2
success, rows = CBit.create_enterprise_statistics_a64_data("sbs_na_sca_r2",
    eurostat_path, conn)
success, rows = CBit.create_enterprise_statistics_a64_data("sbs_ovw_act",
    eurostat_path, conn)
@info "Step 5 completed" success rows


# test = "SELECT * FROM '$(pqfile("bd_9ac_l_form_a64"))' LIMIT 10;"
# DBInterface.execute(conn, test)

## After this step, we have the necessary data stored on disk!
