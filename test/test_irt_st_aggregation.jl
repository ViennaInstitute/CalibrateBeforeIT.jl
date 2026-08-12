using Test
using CalibrateBeforeIT

@testset "IRT_ST Aggregation Tests" begin

    @testset "_monthly_to_quarterly_mean: basic" begin
        # 3 months, one quarter
        monthly = [1.0, 2.0, 3.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0]
    end

    @testset "_monthly_to_quarterly_mean: multiple quarters" begin
        # 6 months, two quarters
        monthly = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0, 5.0]
    end

    @testset "_monthly_to_quarterly_mean: length not multiple of 3 drops remainder" begin
        monthly = [1.0, 2.0, 3.0, 4.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0]
    end

    @testset "_in_sample_interp: no missing" begin
        v = [1.0, 2.0, 3.0, 4.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 3.0, 4.0]
    end

    @testset "_in_sample_interp: interior missing" begin
        # missing at index 2, between 1.0 and 3.0 -> 2.0
        v = Union{Missing,Float64}[1.0, missing, 3.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 3.0]
    end

    @testset "_in_sample_interp: leading missing clamps to first observed" begin
        v = Union{Missing,Float64}[missing, missing, 3.0, 4.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [3.0, 3.0, 3.0, 4.0]
    end

    @testset "_in_sample_interp: trailing missing clamps to last observed" begin
        v = Union{Missing,Float64}[1.0, 2.0, missing, missing]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 2.0, 2.0]
    end

    @testset "_in_sample_interp: all missing returns all missing" begin
        v = Union{Missing,Float64}[missing, missing]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test all(ismissing, out)
    end

    @testset "_gap_fill_quarterly: reported preserved" begin
        reported   = Union{Missing,Float64}[5.0, missing, 7.0]
        aggregated = [2.0, 4.0, 6.0]
        out = CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
        @test out == [5.0, 4.0, 7.0]
    end

    @testset "_gap_fill_quarterly: all missing reported -> all aggregated" begin
        reported   = Union{Missing,Float64}[missing, missing, missing]
        aggregated = [2.0, 4.0, 6.0]
        out = CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
        @test out == [2.0, 4.0, 6.0]
    end

    @testset "_gap_fill_quarterly: lengths must match" begin
        reported   = Union{Missing,Float64}[5.0, missing]
        aggregated = [2.0, 4.0, 6.0]
        @test_throws DimensionMismatch CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
    end
end
