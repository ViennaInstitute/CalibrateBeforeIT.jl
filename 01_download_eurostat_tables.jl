
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)

import CalibrateBeforeIT as CBit

## Set some parameters for the data-downloading process
global const eurostat_path = "data/010_eurostat_tables"
mkpath(eurostat_path)
conn = CBit.DuckDB.DBInterface.connect(CBit.DuckDB.DB)


##------------------------------------------------------------
## Step 1: Download all Eurostat tables and save as .parquet files
all_eurostat_table_ids = CBit.get_eurostat_table_ids()

println(all_eurostat_table_ids)

# Test on a single table
result = CBit.download_to_parquet("sbs_na_sca_r2", eurostat_path; use_cached_tsv=false)
println(result)

# Download all tables
for table_id in all_eurostat_table_ids
    @info table_id
    CBit.download_to_parquet(table_id, eurostat_path; use_cached_tsv=true)
end


##------------------------------------------------------------
## Step 2: Save the NACE64.csv (industry classification table) as parquet too
table_id = "nace64"
csv_filename = "data/$(table_id).csv"
nace64_table = CBit.CSV.read(csv_filename, CBit.DataFrame;
                             delim = ",")
CBit.write_table(joinpath(eurostat_path, "$(table_id).parquet"),
            nace64_table, format = :parquet)


##------------------------------------------------------------
## Step 3: Append the three Figaro tables into one. Eurostat splits the Figaro
## IO tables into three separate tables. For us to query them efficiently, we
## append them onto each other and save them again as a .parquet file.
result_figaro = CBit.combine_figaro_tables(eurostat_path; conn=conn, skip_if_missing=false)
@info "FIGARO combination result: $(result_figaro.output_file)"


##------------------------------------------------------------
## Step 4: Create bd_9ac_l_form_a64 from bd_9ac_l_form_r2
success, rows = CBit.create_business_demographic_a64_data("bd_9ac_l_form_r2",
    eurostat_path, conn)
success, rows = CBit.create_business_demographic_a64_data("bd_l_form",
    eurostat_path, conn)
@info "Step 4 completed" success rows


##------------------------------------------------------------
## Step 5: Create sbs_na_sca_a64 from sbs_na_sca_r2
success, rows = CBit.create_enterprise_statistics_a64_data("sbs_na_sca_r2",
    eurostat_path, conn)
success, rows = CBit.create_enterprise_statistics_a64_data("sbs_ovw_act",
    eurostat_path, conn)
@info "Step 5 completed" success rows


# test = "SELECT * FROM '$(pqfile("bd_9ac_l_form_a64"))' LIMIT 10;"
# DBInterface.execute(conn, test)


## After this step, we have the necessary data stored on disk!
