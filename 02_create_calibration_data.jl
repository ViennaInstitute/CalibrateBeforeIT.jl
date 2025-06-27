
## This file is meant to be a script to help developing the whole process and to
## show the usual application

## To run this script, 01_download_eurostat_tables.jl must be run first. This
## script asummes that the directory /data/010_eurostat_tables is populated with
## {table_id}.parquet files, where {table_id} is the Eurostat table id.

cd(@__DIR__)

using Pkg
Pkg.activate(Base.current_project())
using Revise
using DataFrames
using QuackIO
using DuckDB
using Tables

includet("src/utils.jl")
includet("src/import_figaro_data.jl")
includet("src/import_data.jl")
includet("src/import_calibration_data.jl")

## Set some parameters for the data-downloading process
global const save_path = "data/010_eurostat_tables"
mkpath(save_path)



##------------------------------------------------------------
## Set some parameters necessary to carry out the calibration functions
geo = country = "IE"
start_calibration_year = 2010;
end_calibration_year = 2022; # the last year for which there are IO tables
all_years = collect(start_calibration_year:end_calibration_year)
number_years = end_calibration_year - start_calibration_year + 1;
number_quarters = number_years*4;
number_sectors = 62;


##------------------------------------------------------------
## Step 4: "Import figaro": Input-Output data and other indicators
ctry_figaro = import_figaro_data(geo, save_path,
                                 all_years, number_sectors, number_years)


##------------------------------------------------------------
## Step 5: "Import data": GDP, GVA, Consumption time series
start_year = 1996
end_year = 2024

ctry_data = import_data(geo, start_year, end_year)


##------------------------------------------------------------
## Step 6: "Import calibration data"
ctry_calibration_data = import_calibration_data(geo,
                                                start_calibration_year,
                                                end_calibration_year)


##------------------------------------------------------------
## Step 7: "Import EA data"

## This step does the same queries as step 5 with import_data(), but with geo ==
## "EA19". Only difference is that with EA19, unemployment rates are not used
ea_data = import_data("EA19", start_year, end_year)



##------------------------------------------------------------
## Step 8: Calculate the initial conditions and parameters

## NOTE: code does not run yet

## Save calibration data into such a struct
using Dates
struct CalibrationData
    calibration::Dict{String, Any}
    figaro::Dict{String, Any}
    data::Dict{String, Any}
    ea::Dict{String, Any}
    max_calibration_date::DateTime
    estimation_date::DateTime
end

calibration_date = DateTime(start_year, 03, 31);
max_calibration_date = DateTime(2020, 12, 31)
estimation_date = DateTime(1996, 12, 31)

country_calibration = CalibrationData(
    ctry_calibration_data,
    ctry_figaro,
    ctry_data,
    ea_data,
    max_calibration_date, estimation_date)

using BeforeIT
parameters, initial_conditions =
    BeforeIT.get_params_and_initial_conditions(country_calibration,
                                               calibration_date; scale = 1/10000);
