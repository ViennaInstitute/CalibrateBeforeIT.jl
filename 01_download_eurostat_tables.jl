
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

