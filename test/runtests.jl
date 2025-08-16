
import CalibrateBeforeIT as CBit

using Test

@testset "CalibrateBeforeIT.jl Tests" begin

    @testset "Download Function Tests" begin
        include("test_download_function.jl")
    end

    @testset "Calibration Tests" begin
        include("params_and_initial_conditions.jl")
    end

end
