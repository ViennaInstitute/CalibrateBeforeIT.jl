
## This file is meant to be a script to help developing the whole process and to
## show the usual application

## To run this script, 01_download_eurostat_tables.jl must be run first. This
## script asummes that the directory /data/010_eurostat_tables is populated with
## {table_id}.parquet files, where {table_id} is the Eurostat table id.

cd(@__DIR__)

import CalibrateBeforeIT as CBit

## Set some parameters for the data-downloading process. `eurostat_path` is already
## set to a default directory value, which can be re-set here:
# CBit.eurostat_path = "data/010_eurostat_tables"
mkpath(CBit.eurostat_path)

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

# start_calibration_year = 2010;
# end_calibration_year = 2021; # the last year for which there are IO tables
# number_quarters = number_years*4;

## all countries:
# all_countries=["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

## countries excluding the non-working ones:
# not_working=["BG", "CY", "EE", "ES", "FR", "HR", "HU", "IE", "LT", "LU", "MT", "PL", "PT", "RO", "SE", ]
all_countries=["AT", "BE", "CZ", "DE", "DK", "EL", "FI", "IT", "LV", "NL", "SI", "SK"]


## This step does the same queries as step 5 with import_data(), but with geo ==
## "EA19". Only difference is that with EA19, unemployment rates are not used
ea_data = CBit.import_data("EA19", start_year, end_year)

##------------------------------------------------------------
## Set some parameters necessary to carry out the calibration functions
geo = country = "AT"
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
    ctry_data = CBit.import_data(geo, start_year, end_year)


    ##------------------------------------------------------------
    @info "Step 7: 'Import calibration data'"
    ctry_calibration_data = CBit.import_calibration_data(geo,
                                                         start_calibration_year,
                                                         end_calibration_year,
                                                         number_sectors,
                                                         ctry_figaro)


    ##------------------------------------------------------------
    @info "Step 8: Calculate the initial conditions and parameters"
    for calibration_year in start_calibration_year:end_calibration_year
    # for calibration_year in 2024:2024
        for calibration_quarter in [3, 6, 9, 12]
            @info "Calibrating $(geo) year=$(calibration_year) month=$(calibration_quarter)"
            calibration_date = CBit.DateTime(calibration_year, calibration_quarter,
                                             calibration_quarter in [3, 12] ? 31 : 30);

            calibration_object = CalibrationData(
                ctry_calibration_data,
                ctry_figaro,
                ctry_data,
                ea_data,
                max_calibration_date, estimation_date)

            parameters, initial_conditions =
                CBit.get_params_and_initial_conditions(calibration_object,
                                                       calibration_date; scale = 1/10000);

            # ## Helper code to generate "reference" objects to be used in the tests:
            # using JLD2
            # jldsave("test/$(geo)_2010Q1_calibration_object.jld2";
            #         reference_calibration_object=calibration_object)
            # jldsave("test/$(geo)_2010Q1_parameters_initial_conditions.jld2";
            #         reference_parameters=parameters,
            #         reference_initial_conditions=initial_conditions)

            ## TODO save the objects them somewhere
        end
    end
end

# import BeforeIT as Bit
# using Plots

# model = Bit.Model(parameters, initial_conditions)
# T = 20
# Bit.run!(model, T)

# p1 = plot(model.data.real_gdp, title = "gdp", titlefont = 10)
# p2 = plot(model.data.real_household_consumption, title = "household cons.", titlefont = 10)
# p3 = plot(model.data.real_government_consumption, title = "gov. cons.", titlefont = 10)
# p4 = plot(model.data.real_capitalformation, title = "capital form.", titlefont = 10)
# p5 = plot(model.data.real_exports, title = "exports", titlefont = 10)
# p6 = plot(model.data.real_imports, title = "imports", titlefont = 10)
# p7 = plot(model.data.wages, title = "wages", titlefont = 10)
# p8 = plot(model.data.euribor, title = "euribor", titlefont = 10)
# p9 = plot(model.data.nominal_gdp ./ model.data.real_gdp, title = "gdp deflator", titlefont = 10)

# plot(p1, p2, p3, p4, p5, p6, p7, p8, p9, layout = (3, 3), legend = false)
