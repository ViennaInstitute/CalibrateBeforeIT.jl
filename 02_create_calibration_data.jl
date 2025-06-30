
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
using Dates
using StatsBase ## only for cov in get_params_and_initial_conditions

includet("src/utils.jl")
includet("src/import_figaro_data.jl")
includet("src/import_data.jl")
includet("src/import_calibration_data.jl")
includet("src/get_params_and_initial_conditions.jl")

## Set some parameters for the data-downloading process
global const save_path = "data/010_eurostat_tables"
mkpath(save_path)

## Save calibration data into such a struct
struct CalibrationData
    calibration::Dict{String, Any}
    figaro::Dict{String, Any}
    data::Dict{String, Any}
    ea::Dict{String, Any}
    max_calibration_date::DateTime
    estimation_date::DateTime
end

##------------------------------------------------------------
## Step 4: "Import EA data"

## For `import_data`:
start_year = 1996
end_year = 2024

## For `import_figaro_data`:
start_calibration_year = 2010;
end_calibration_year = 2022; # the last year for which there are IO tables
number_years = end_calibration_year - start_calibration_year + 1;
number_quarters = number_years*4;
number_sectors = 62;
## all countries:
## geo=["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]
## countries excluding the non-working ones:
## geo=["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT", "LV", "NL", "PL", "PT", "RO", "SI", "SK"]
## countries whose data is available until 2023 (additionally excludes DK, HR)
all_countries=["AT", "BE", "BG", "CY", "CZ", "DE", "EE", "EL", "ES", "FI", "FR", "HU", "IE", "IT", "LV", "NL", "PL", "PT", "RO", "SI", "SK"]


## This step does the same queries as step 5 with import_data(), but with geo ==
## "EA19". Only difference is that with EA19, unemployment rates are not used
ea_data = import_data("EA19", start_year, end_year)

##------------------------------------------------------------
## Set some parameters necessary to carry out the calibration functions
geo = country = "AT"
for geo in all_countries
    @info geo

    ##------------------------------------------------------------
    ## Step 5: "Import figaro": Input-Output data and other indicators
    ctry_figaro = import_figaro_data(geo, start_calibration_year, end_calibration_year,
                                     number_sectors, number_years)


    ##------------------------------------------------------------
    ## Step 6: "Import data": GDP, GVA, Consumption time series
    ctry_data = import_data(geo, start_year, end_year)


    ##------------------------------------------------------------
    ## Step 7: "Import calibration data"
    ctry_calibration_data = import_calibration_data(geo,
                                                    start_calibration_year,
                                                    end_calibration_year,
                                                    number_sectors,
                                                    ctry_figaro)


    ##------------------------------------------------------------
    ## Step 8: Calculate the initial conditions and parameters
    calibration_date = DateTime(start_calibration_year, 03, 31);
    max_calibration_date = DateTime(2020, 12, 31)
    estimation_date = DateTime(1996, 12, 31)

    calibration_object = CalibrationData(
        ctry_calibration_data,
        ctry_figaro,
        ctry_data,
        ea_data,
        max_calibration_date, estimation_date)

    parameters, initial_conditions =
        get_params_and_initial_conditions(calibration_object,
                                          calibration_date; scale = 1/10000);
end
