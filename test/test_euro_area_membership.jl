using Test
using CalibrateBeforeIT
using Dates

@testset "Euro Area Membership Tests" begin

    @testset "Founding members (1999-01-01)" begin
        for geo in ["AT", "BE", "DE", "ES", "FI", "FR", "IE", "IT", "LU", "NL", "PT"]
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(1999, 1, 1)) == true
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(1998, 12, 31)) == false
        end
    end

    @testset "Late joiners" begin
        @test CalibrateBeforeIT.is_euro_area_member("EL", Date(2000, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("EL", Date(2001, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("SI", Date(2007, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("CY", Date(2008, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("MT", Date(2008, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("SK", Date(2009, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("EE", Date(2011, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("LV", Date(2014, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("LT", Date(2015, 1, 1)) == true
    end

    @testset "HR joined 2023" begin
        @test CalibrateBeforeIT.is_euro_area_member("HR", Date(2022, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("HR", Date(2023, 1, 1)) == true
    end

    @testset "BG joins 2026 (outside current calibration window)" begin
        @test CalibrateBeforeIT.is_euro_area_member("BG", Date(2025, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("BG", Date(2026, 1, 1)) == true
    end

    @testset "Non-EA countries (current window <= 2023-12-31)" begin
        for geo in ["CZ", "DK", "HU", "PL", "RO", "SE"]
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(2023, 12, 31)) == false
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(2024, 12, 31)) == false
        end
        @test CalibrateBeforeIT.is_euro_area_member("BG", Date(2023, 12, 31)) == false
    end

    @testset "Unknown geo returns false" begin
        @test CalibrateBeforeIT.is_euro_area_member("XX", Date(2024, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("EA", Date(2024, 12, 31)) == false
    end

    @testset "EURO_AREA_JOIN_DATES is exported and complete" begin
        @test haskey(CalibrateBeforeIT.EURO_AREA_JOIN_DATES, "AT")
        @test length(CalibrateBeforeIT.EURO_AREA_JOIN_DATES) == 21
    end
end
