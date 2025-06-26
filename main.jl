
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
includet("src/import_figaro_data.jl")
includet("src/import_data.jl")
includet("src/import_calibration_data.jl")
# using .Utils
# using .Import_Eurostat
# using .Import_Figaro_Data

## Set some parameters for the data-downloading process
global const save_path = "data/010_eurostat_tables"
mkpath(save_path)


# ## Step 1: Download all Eurostat tables and save as parquet
# all_eurostat_table_ids = readlines("docs/001_table.txt");
# for table_id in all_eurostat_table_ids
#     @info table_id
#     download_to_parquet(table_id, save_path)
# end


# ## Step 2: Save the NACE64 csv as parquet too
# table_id = "nace64"
# csv_filename = "data/$(table_id).csv"
# nace64_table = CSV.read(csv_filename, DataFrame;
#                         delim = ",")

# write_table(joinpath(save_path, "$(table_id).parquet"),
#             nace64_table, format = :parquet)

# function pqfile(table_id)
#     joinpath(save_path, "$(table_id).parquet")
# end


# ## Step 3: Append the three Figaro tables into one
# conn = DBInterface.connect(DuckDB.DB)
# sqlquery = "COPY " *
#     "(SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii1.parquet"))' UNION ALL " *
#     "SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii2.parquet"))' UNION ALL " *
#     " SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii3.parquet"))') " *
#     "TO '$(joinpath(save_path, "naio_10_fcp_ii.parquet"))' (FORMAT parquet)"
# DBInterface.execute(conn, sqlquery);

## After this step, we have the necessary data stored on disk!


## Step 4: Import the input-output (Figaro) data

## Set some parameters necessary to carry out the calibration functions
geo = country = "AT"
start_calibration_year = 2010;
end_calibration_year = 2022; # the last year for which there are IO tables
all_years = collect(start_calibration_year:end_calibration_year)
number_years = end_calibration_year - start_calibration_year + 1;
number_quarters = number_years*4;
number_sectors = 62;

at_figaro = import_figaro_data(geo, save_path,
                               all_years, number_sectors, number_years)



## Step 5: "Import data": GDP, GVA, Consumption time series
start_year = 1996
end_year = 2024

at_data = import_data(geo, start_year, end_year)


## Step 6: "Import calibration data"
at_calibration_data = import_calibration_data(geo, start_calibration_year, end_calibration_year)


## Step 7: "Import EA data"
##
## OR: seems to be the same as import_data(), but with geo == "EA19". TODO check


# ## Step 8: Calculate the initial conditions and parameters

# ## NOTE: code does not run yet

# ## Save calibration data into such a struct
# using Dates
# struct CalibrationData
#     calibration::Dict{String, Any}
#     figaro::Dict{String, Any}
#     data::Dict{String, Any}
#     ea::Dict{String, Any}
#     max_calibration_date::DateTime
#     estimation_date::DateTime
# end

# calibration_date = DateTime(start_year, 03, 31);
# max_calibration_date = DateTime(2020, 12, 31)
# estimation_date = DateTime(1996, 12, 31)

# country_calibration = CalibrationData(
#     at_calibration_data,
#     at_figaro,
#     at_data,
#     ea, ## OR: is not computed yet
#     max_calibration_date, estimation_date)

# using BeforeIT
# parameters, initial_conditions =
#     BeforeIT.get_params_and_initial_conditions(country_calibration,
#                                                calibration_date; scale = 1/10000);
