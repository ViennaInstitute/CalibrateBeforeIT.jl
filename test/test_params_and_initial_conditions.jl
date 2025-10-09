
import CalibrateBeforeIT as CBit

using Test
using JLD2

for geo in ["AT"]

    @testset "Testing get_params_and_initial_conditions function: $(geo), 2010" begin

        @info "Testing get_params_and_initial_conditions function: $(geo), 2010"

        ## Save calibration data into such a struct
        struct CalibrationData
            calibration::Dict{String, Any}
            figaro::Dict{String, Any}
            data::Dict{String, Any}
            ea::Dict{String, Any}
            max_calibration_date::CBit.DateTime
            estimation_date::CBit.DateTime
        end

        reference_calibration_object =
            load(joinpath(@__DIR__,
                "data", "reference_calibration",
                "$(geo)_2010Q1_calibration_object.jld2"),
                "reference_calibration_object")
        (reference_parameters, reference_initial_conditions) =
            load(joinpath(@__DIR__,
                "data", "reference_calibration",
                "$(geo)_2010Q1_parameters_initial_conditions.jld2"),
                "reference_parameters", "reference_initial_conditions")
        calibration_date = CBit.DateTime(2010, 03, 31);

        parameters, initial_conditions =
            CBit.get_params_and_initial_conditions(reference_calibration_object,
                                                   calibration_date; scale = 1/10000);

        for key in keys(parameters)
            @test isapprox(reference_parameters[key],
                           parameters[key], atol = 1e-6, rtol = 1e-6)
        end

        for key in keys(initial_conditions)
            @test isapprox(reference_initial_conditions[key],
                           initial_conditions[key], atol = 1e-6, rtol = 1e-6)
        end
    end

end
