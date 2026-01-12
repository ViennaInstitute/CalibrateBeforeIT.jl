
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

    @testset "Netherlands OCM Calibration Tests" begin
        include("test_params_and_initial_conditions_netherlands_ocm.jl")
    end

    @testset "Calibration Data Tests" begin
        include("test_calibration_data.jl")
    end

    # Only run validation tests if calibration output exists
    if isdir(joinpath(dirname(@__DIR__), "data", "020_calibration_output"))
        @testset "Calibration Validation Tests" begin
            include("test_calibration_validation.jl")
        end
    else
        @info "Skipping calibration validation tests (no calibration output found)"
    end

end
