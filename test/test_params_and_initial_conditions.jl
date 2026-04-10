import CalibrateBeforeIT as CBit

using Test
using JLD2

const BEFOREIT_AVAILABLE = try
    import BeforeIT as Bit
    true
catch
    false
end

for geo in ["AT"]

    @testset "Testing get_params_and_initial_conditions function: $(geo), 2010" begin

        if !BEFOREIT_AVAILABLE
            @info "BeforeIT.jl not available, skipping get_params_and_initial_conditions test"
            @test_skip "BeforeIT.jl required for get_params_and_initial_conditions test"
        else
            import BeforeIT as Bit

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
                    "calibration_object")
            (reference_parameters, reference_initial_conditions) =
                load(joinpath(@__DIR__,
                    "data", "reference_calibration",
                    "$(geo)_2010Q1_parameters_initial_conditions.jld2"),
                    "parameters", "initial_conditions")
            calibration_date = CBit.DateTime(2010, 03, 31);

            parameters, initial_conditions =
                Bit.get_params_and_initial_conditions(reference_calibration_object,
                                                       calibration_date; scale = 1/10000);

            # Test keys that exist in both reference and new output
            common_param_keys = intersect(keys(parameters), keys(reference_parameters))
            for key in common_param_keys
                @test isapprox(reference_parameters[key],
                               parameters[key], atol = 1e-6, rtol = 1e-6)
            end

            common_ic_keys = intersect(keys(initial_conditions), keys(reference_initial_conditions))
            for key in common_ic_keys
                @test isapprox(reference_initial_conditions[key],
                               initial_conditions[key], atol = 1e-6, rtol = 1e-6)
            end
        end

    end

end