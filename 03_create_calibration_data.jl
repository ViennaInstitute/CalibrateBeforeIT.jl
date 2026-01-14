
## This file is meant to be a script to help developing the whole process and to
## show the usual application

## To run this script, 01_download_eurostat_tables.jl must be run first. This
## script asummes that the directory /data/010_eurostat_tables is populated with
## {table_id}.parquet files, where {table_id} is the Eurostat table id.

cd(@__DIR__)

import CalibrateBeforeIT as CBit
using JLD2
using Dates: year, month

## Set some parameters for the data-downloading process. `eurostat_path` is already
## set to a default directory value, which can be re-set here:
# CBit.eurostat_path = "data/010_eurostat_tables"
mkpath(CBit.eurostat_path)
mkpath(CBit.calibration_output_path)

## Save calibration data into such a struct (TODO should be sourced from
## BeforeIT.jl)
struct CalibrationData
    calibration::Dict{String, Any}
    figaro::Dict{String, Any}
    data::Dict{String, Any}
    ea::Dict{String, Any}
    max_calibration_date::CBit.DateTime
    estimation_date::CBit.DateTime
end

##------------------------------------------------------------
## Step 4: "Import EA data"

## For `import_data`:
start_year = 1996
end_year = 2024

## For `import_figaro_data`:
max_calibration_date = CBit.DateTime(2020, 12, 31)
estimation_date = CBit.DateTime(1996, 12, 31)
number_sectors = 62;

# ## all countries:
# all_countries = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

# ## countries excluding the non-working ones:
# HR: 2010-2012
# MT
# BG excluded temporarily due to data issue (0-element vector error)
all_countries = ["AT", "BE", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]
#all_countries = ["NL"]
## This step does the same queries as step 5 with import_data(), but with geo ==
## "EA19". Only difference is that with EA19, unemployment rates are not used
ea_data = CBit.import_data("EA19", start_year, end_year)


##------------------------------------------------------------
## Set some parameters necessary to carry out the calibration functions
geo = country = "HR"
for geo in all_countries
    @info geo

    start_calibration_year, end_calibration_year = CBit.get_minmax_calibration_years(geo)

    ##------------------------------------------------------------
    @info "Step 5: 'Import figaro': Input-Output data and other indicators"
    ctry_figaro = CBit.import_figaro_data(geo, start_calibration_year,
        end_calibration_year,
        number_sectors)


    ##------------------------------------------------------------
    @info "Step 6: 'Import data': GDP, GVA, Consumption time series"
    if geo in ["MT"]
        global start_year = 2000
        global estimation_date = CBit.DateTime(start_year, 12, 31)
    end
    ctry_data = CBit.import_data(geo, start_year, end_year)


    ##------------------------------------------------------------
    @info "Step 7: 'Import calibration data'"
    ctry_calibration_data = CBit.import_calibration_data(geo,
        start_calibration_year,
        end_calibration_year,
        number_sectors,
        ctry_figaro)

    ## Create calibration object (same for all quarters)
    calibration_object = CalibrationData(
        ctry_calibration_data,
        ctry_figaro,
        ctry_data,
        ea_data,
        max_calibration_date, estimation_date)

    ## Save the calibration object
    mkpath("$(CBit.calibration_output_path)/$(geo)")
    jldsave("$(CBit.calibration_output_path)/$(geo)/calibration_object.jld2";
        calibration_object = calibration_object)

    ##------------------------------------------------------------
    @info "Step 8: Calculate the initial conditions and parameters"

    # Determine first quarter with valid data (some countries have missing early quarters)
    key_quarterly_vars = ["firm_cash_quarterly", "household_cash_quarterly", "bank_equity_quarterly"]
    first_valid_idx = 1
    for k in key_quarterly_vars
        if haskey(ctry_calibration_data, k)
            v = ctry_calibration_data[k]
            idx = findfirst(!ismissing, v)
            if idx !== nothing
                first_valid_idx = max(first_valid_idx, idx)
            end
        end
    end

    # Convert index to year/quarter
    first_valid_date = CBit.num2date(ctry_calibration_data["quarters_num"][first_valid_idx])
    first_valid_year = year(first_valid_date)
    first_valid_quarter = ceil(Int, month(first_valid_date) / 3)

    if first_valid_year > start_calibration_year || (first_valid_year == start_calibration_year && first_valid_quarter > 1)
        @info "  --> $(geo): Starting calibration from $(first_valid_year)Q$(first_valid_quarter) (earlier quarters have missing data)"
    end

    for calibration_year in start_calibration_year:end_calibration_year
        for calibration_quarter in 1:4
            # Skip quarters before data becomes available
            if calibration_year < first_valid_year ||
               (calibration_year == first_valid_year && calibration_quarter < first_valid_quarter)
                continue
            end

            calibration_month = calibration_quarter * 3
            calibration_date = CBit.DateTime(calibration_year, calibration_month,
                calibration_month in [3, 12] ? 31 : 30);

            parameters, initial_conditions =
                CBit.get_params_and_initial_conditions(calibration_object,
                    calibration_date; scale = 1/1000);

            # @info "Calibrated $(geo) $(calibration_year)Q$(calibration_quarter)"

            ## Save the parameters and initial conditions
            jldsave("$(CBit.calibration_output_path)/$(geo)/$(calibration_year)Q$(calibration_quarter)_parameters_initial_conditions.jld2";
                parameters = parameters,
                initial_conditions = initial_conditions)
        end
    end
end
