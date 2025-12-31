#!/usr/bin/env julia
# Regenerate all 40 quarters of OCM calibration output
# Saves to NEW folder to avoid overwriting existing files

cd(@__DIR__)

import CalibrateBeforeIT as CBit
using JLD2, Dates

println("=" ^ 60)
println("Regenerating OCM Reference Files (2010Q1 - 2019Q4)")
println("=" ^ 60)

# Load calibration object
calibration_path = "data/020_calibration_output/NL/calibration_object.jld2"
println("Loading calibration object from: $calibration_path")
calibration_object = load(calibration_path, "calibration_object")

# Output paths - NEW folder to avoid overwriting
base_output = "/Users/steven/Github/Optimal-ABM/data/parameters_initial_conditions/netherlands_households_own_firms_regenerated"
params_output = joinpath(base_output, "parameters")
ic_output = joinpath(base_output, "initial_conditions")

# Create output directories
mkpath(params_output)
mkpath(ic_output)

println("\nOutput directories:")
println("  Parameters: $params_output")
println("  Initial Conditions: $ic_output")
println()

# Generate all quarters from 2010Q1 to 2019Q4
global quarters_generated = 0
global quarters_failed = 0

for year in 2010:2019
    for quarter in 1:4
        # Quarter end date (last day of quarter)
        month = quarter * 3
        day = month in [3, 12] ? 31 : 30
        calibration_date = DateTime(year, month, day)

        quarter_name = "$(year)Q$(quarter)"
        print("Generating $quarter_name... ")

        try
            params, initial_conditions = CBit.get_params_and_initial_conditions_netherlands_ocm(
                calibration_object,
                calibration_date
            )

            # Save to JLD2 files (params and initial_conditions are Dicts with String keys)
            save(joinpath(params_output, "$quarter_name.jld2"), params)
            save(joinpath(ic_output, "$quarter_name.jld2"), initial_conditions)

            println("✓")
            global quarters_generated += 1
        catch e
            println("✗ ERROR: $e")
            global quarters_failed += 1
        end
    end
end

println()
println("=" ^ 60)
println("COMPLETE: $quarters_generated quarters generated, $quarters_failed failed")
println("Output saved to: $base_output")
println("=" ^ 60)
