"""
Test script for the library functions in CalibrateBeforeIT.jl
Tests the core utility functions and data processing capabilities.
"""

using Test
import CalibrateBeforeIT as CBit

@testset "Library Functions Tests" begin

    @testset "Eurostat Table Management" begin
        # Test get_eurostat_table_ids function
        table_ids = CBit.get_eurostat_table_ids()

        @test isa(table_ids, Vector{String})
        @test length(table_ids) == 35
        @test "naio_10_fcp_ii1" in table_ids
        @test "naio_10_fcp_ii2" in table_ids
        @test "naio_10_fcp_ii3" in table_ids
        @test "naio_10_fcp_ii4" in table_ids
        @test "nama_10_gdp" in table_ids

        # Test that the function returns a copy (not the original)
        table_ids_copy = CBit.get_eurostat_table_ids()
        push!(table_ids_copy, "test_table")
        original_ids = CBit.get_eurostat_table_ids()
        @test length(original_ids) == 35  # Should not be modified
    end

    @testset "FIGARO Data Processing" begin
        # Test combine_tables function with invalid directory (skip_if_missing=true)
        result = CBit.combine_tables("nonexistent_directory")
        @test result === nothing

        # Test combine_tables function with invalid directory (skip_if_missing=false)
        @test_throws ArgumentError CBit.combine_tables("nonexistent_directory"; skip_if_missing=false)

        # Test with valid directory but missing files (skip_if_missing=true)
        temp_dir = mktempdir()
        result = CBit.combine_tables(temp_dir)
        @test result === nothing

        # Test with valid directory but missing files (skip_if_missing=false)
        @test_throws ArgumentError CBit.combine_tables(temp_dir; skip_if_missing=false)

        # Test parameter validation
        @test_throws ArgumentError CBit.combine_tables(temp_dir; input_tables=String[], skip_if_missing=false)

        # Clean up
        rm(temp_dir)
    end

    @testset "Exception Handling" begin
        # Test custom exception types
        @test CBit.DownloadError("test") isa CBit.DownloadError
        @test CBit.ProcessingError("test") isa CBit.ProcessingError

        # Test that they display properly
        io = IOBuffer()
        Base.showerror(io, CBit.DownloadError("test message"))
        @test String(take!(io)) == "DownloadError: test message"

        io = IOBuffer()
        Base.showerror(io, CBit.ProcessingError("test message"))
        @test String(take!(io)) == "ProcessingError: test message"
    end

    @testset "import_data: non-EA euribor is country-specific and dense" begin
        # DK is non-EA and has complete IRT_M3 in irt_st_q (no gap-fill needed)
        # This test is skipped if the eurostat data dir is not populated.
        eurostat_dir = CBit.eurostat_path
        if !isfile(joinpath(eurostat_dir, "irt_st_q.parquet"))
            @info "Skipping non-EA euribor test: irt_st_q.parquet not present"
        else
            data = CBit.import_data("DK", 2018, 2018)
            n_quarters = 4  # 2018 Q1..Q4
            @test length(data["euribor"]) == n_quarters
            @test all(!ismissing, data["euribor"])

            # DK should NOT equal the EA rate (DK is non-EA and has its own IBOR)
            ea_data = CBit.import_data("EA", 2018, 2018)
            @test data["euribor"] != ea_data["euribor"]

            # Sanity: the DK vector must be aligned with quarters_num (same length)
            @test length(data["euribor"]) == length(data["quarters_num"])
        end
    end
end
