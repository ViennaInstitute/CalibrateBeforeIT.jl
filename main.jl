
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)

using Pkg
Pkg.activate(Base.current_project())
using Revise, CSV, DataFrames, Downloads, QuackIO

includet("src/utils.jl")
includet("src/import_eurostat.jl")
includet("src/import_figaro_data.jl")
using .Utils
using .Import_Eurostat
using .Import_Figaro_Data

save_path = "data/010_eurostat_tables"
all_eurostat_table_ids = readlines("docs/001_table.txt");


## Step 1: Download all Eurostat tables and save as parquet
for table_id in all_eurostat_table_ids
    @info table_id
    download_to_parquet(table_id, save_path)
end


## Step 2: Save the NACE64 csv as parquet too
table_id = "nace64"
csv_filename = "data/$(table_id).csv"
nace64_table = CSV.read(csv_filename, DataFrame;
                        delim = ",")

write_table(joinpath(save_path, "$(table_id).parquet"),
            nace64_table, format = :parquet)


## Step 3: Append the three Figaro tables into one

## Step 4: Import the input-output (Figaro) data
geo = country = "AT"

start_calibration_year = 2010;
end_calibration_year = 2022; # the last year for which there are IO tables
all_years = collect(start_calibration_year:end_calibration_year)
number_years = end_calibration_year - start_calibration_year + 1;
number_quarters = number_years*4;
number_sectors = 62;

at_figaro = import_figaro_data(geo, all_years, number_sectors, number_years)
