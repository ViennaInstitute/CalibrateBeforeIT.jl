#!/usr/bin/env julia
# Script to regenerate NL calibration object with wages_by_sector field

cd(@__DIR__)

import CalibrateBeforeIT as CBit
using JLD2

struct CalibrationData
    calibration::Dict{String, Any}
    figaro::Dict{String, Any}
    data::Dict{String, Any}
    ea::Dict{String, Any}
    max_calibration_date::CBit.DateTime
    estimation_date::CBit.DateTime
end

geo = "NL"
start_year = 1996
end_year = 2024
max_calibration_date = CBit.DateTime(2020, 12, 31)
estimation_date = CBit.DateTime(1996, 12, 31)
number_sectors = 62

println("Regenerating NL calibration object with wages_by_sector...")

# Step 5: Import figaro
start_calibration_year, end_calibration_year = CBit.get_minmax_calibration_years(geo)
println("Calibration years: $start_calibration_year - $end_calibration_year")

ctry_figaro = CBit.import_figaro_data(geo, start_calibration_year, end_calibration_year, number_sectors)
println("Figaro imported. Keys: $(length(keys(ctry_figaro)))")

# Step 6: Import data
ctry_data = CBit.import_data(geo, start_year, end_year)
println("Data imported. Keys: $(length(keys(ctry_data)))")

ea_data = CBit.import_data("EA19", start_year, end_year)
println("EA data imported. Keys: $(length(keys(ea_data)))")

# Step 7: Import calibration data (this now includes wages_by_sector!)
ctry_calibration_data = CBit.import_calibration_data(geo, start_calibration_year, end_calibration_year, number_sectors, ctry_figaro)
println("Calibration data imported.")
println("Keys: $(collect(keys(ctry_calibration_data)))")

# Check if wages_by_sector is present
if haskey(ctry_calibration_data, "wages_by_sector")
    wages_size = size(ctry_calibration_data["wages_by_sector"])
    println("wages_by_sector present! Size: $wages_size")
else
    println("ERROR: wages_by_sector NOT present!")
end

# Create and save calibration object
calibration_object = CalibrationData(
    ctry_calibration_data,
    ctry_figaro,
    ctry_data,
    ea_data,
    max_calibration_date, estimation_date)

mkpath("$(CBit.calibration_output_path)/$(geo)")
jldsave("$(CBit.calibration_output_path)/$(geo)/calibration_object.jld2"; calibration_object = calibration_object)
println("Calibration object saved to $(CBit.calibration_output_path)/$(geo)/calibration_object.jld2")
