
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
## Step 2: Append the "historical" annual (i.e. une_rt_a_h) and "current" annual
## (i.e. une_rt_a) unemployment timeseries to each other. The same for the
## quarterly timeseries (une_rt_q_h and une_rt_q).
result_une_rt_a = CBit.unify_unemployment_rate_sources("une_rt_a", conn)
result_une_rt_q = CBit.unify_unemployment_rate_sources("une_rt_q", conn)
@info "Step 2 completed" result_une_rt_a result_une_rt_q


##------------------------------------------------------------
## Step 3: Create bd_9ac_l_form_a64 from bd_9ac_l_form_r2
success, rows = CBit.create_business_demographic_a64_data("bd_9ac_l_form_r2",
    eurostat_path, conn)
success, rows = CBit.create_business_demographic_a64_data("bd_l_form",
    eurostat_path, conn)
@info "Step 3 completed" success rows


##------------------------------------------------------------
## Step 4: Create sbs_na_sca_a64 from sbs_na_sca_r2
success, rows = CBit.create_enterprise_statistics_a64_data("sbs_na_sca_r2",
    eurostat_path, conn)
success, rows = CBit.create_enterprise_statistics_a64_data("sbs_ovw_act",
    eurostat_path, conn)
@info "Step 4 completed" success rows

# test = "SELECT * FROM '$(pqfile("bd_9ac_l_form_a64"))' LIMIT 10;"
# DBInterface.execute(conn, test)

## After this step, we have the necessary data stored on disk!
