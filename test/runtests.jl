
import CalibrateBeforeIT as CBit

using Test

@testset "CalibrateBeforeIT.jl Tests" begin

    @testset "Download Function Tests" begin
        include("test_download_function.jl")
    end

    @testset "Library Functions Tests" begin
        include("test_library_functions.jl")
    end

    @testset "Utils Tests" begin
        include("test_utils.jl")
    end

    @testset "NACE Rev.2 to NACE64 Conversion Tests" begin
        include("test_r2_to_nace64_conversion.jl")
    end

    @testset "Calibration Tests" begin
        include("test_params_and_initial_conditions.jl")
    end

    # Parameter and Initial Condition Constraint Tests
    # These tests validate that calibrated data satisfies economic and mathematical constraints
    # required for stable BeforeIT.jl model execution

    @testset "Parameter Constraints Tests" begin
        include("test_parameter_constraints.jl")
    end

    @testset "Sectoral Constraints Tests" begin
        include("test_sectoral_constraints.jl")
    end

    @testset "Initial Conditions Constraints Tests" begin
        include("test_initial_conditions_constraints.jl")
    end

    @testset "SFC Accounting Identity Tests" begin
        include("test_sfc_identities.jl")
    end

end
