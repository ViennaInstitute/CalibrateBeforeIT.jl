
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)

using Pkg
Pkg.activate(Base.current_project())
using Revise
using CSV
using DataFrames
using Downloads
using QuackIO
using DuckDB
using Tables

includet("src/utils.jl")
includet("src/import_eurostat.jl")

## Set some parameters for the data-downloading process
global const save_path = "data/010_eurostat_tables"
mkpath(save_path)


##------------------------------------------------------------
## Step 1: Download all Eurostat tables and save as .parquet files
all_eurostat_table_ids = readlines("docs/00_table.txt");
for table_id in all_eurostat_table_ids
    @info table_id
    download_to_parquet(table_id, save_path)
end


##------------------------------------------------------------
## Step 2: Save the NACE64.csv (industry classification table) as parquet too
table_id = "nace64"
csv_filename = "data/$(table_id).csv"
nace64_table = CSV.read(csv_filename, DataFrame;
                        delim = ",")
write_table(joinpath(save_path, "$(table_id).parquet"),
            nace64_table, format = :parquet)


##------------------------------------------------------------
## Step 3: Append the three Figaro tables into one. Eurostat splits the Figaro
## IO tables into three separate tables. For us to query them efficiently, we
## append them onto each other and save them again as a .parquet file.
conn = DBInterface.connect(DuckDB.DB)
sqlquery = "COPY " *
    "(SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii1.parquet"))' UNION ALL " *
    "SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii2.parquet"))' UNION ALL " *
    " SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii3.parquet"))') " *
    "TO '$(joinpath(save_path, "naio_10_fcp_ii.parquet"))' (FORMAT parquet)"
DBInterface.execute(conn, sqlquery);

## After this step, we have the necessary data stored on disk!
