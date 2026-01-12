import CalibrateBeforeIT as CBit

using Test
using JLD2

@testset "Testing get_params_and_initial_conditions function" begin

    # Use NL calibration object for all tests
    nl_calibration_path = joinpath(dirname(@__DIR__),
                                   "data", "020_calibration_output", "NL",
                                   "calibration_object.jld2")

    if !isfile(nl_calibration_path)
        @test_skip "NL calibration object not found - please ensure calibration has been run"
    else
        @info "Testing with NL calibration object from: $nl_calibration_path"
        calibration_object = load(nl_calibration_path, "calibration_object")

        # Test multiple quarters to ensure consistency
        test_quarters = [
            (CBit.DateTime(2010, 03, 31), "2010Q1"),
            (CBit.DateTime(2015, 06, 30), "2015Q2"),
            (CBit.DateTime(2020, 12, 31), "2020Q4"),
        ]

        for (calibration_date, quarter_str) in test_quarters
            @testset "Testing quarter: $quarter_str" begin

                # Generate parameters and initial conditions
                parameters, initial_conditions =
                    CBit.get_params_and_initial_conditions(calibration_object,
                                                           calibration_date; scale = 1/1000)

                # Load reference for this quarter
                reference_file = joinpath(dirname(@__DIR__),
                                         "data", "020_calibration_output", "NL",
                                         "$(quarter_str)_parameters_initial_conditions.jld2")

                if isfile(reference_file)
                    reference_params = load(reference_file, "parameters")
                    reference_initial = load(reference_file, "initial_conditions")

                    # Test all parameters match
                    for key in keys(parameters)
                        @test isapprox(reference_params[key],
                                     parameters[key], atol = 1e-6, rtol = 1e-6)
                    end

                    # Test all initial conditions match
                    for key in keys(initial_conditions)
                        @test isapprox(reference_initial[key],
                                     initial_conditions[key], atol = 1e-6, rtol = 1e-6)
                    end
                else
                    @test_skip "Reference file not found for $quarter_str"
                end
            end
        end
    end
end