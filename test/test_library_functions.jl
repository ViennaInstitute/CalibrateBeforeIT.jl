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
        @test length(table_ids) == 27
        @test "naio_10_fcp_ii1" in table_ids
        @test "naio_10_fcp_ii2" in table_ids
        @test "naio_10_fcp_ii3" in table_ids
        @test "naio_10_fcp_ii4" in table_ids
        @test "nama_10_gdp" in table_ids

        # Test that the function returns a copy (not the original)
        table_ids_copy = CBit.get_eurostat_table_ids()
        push!(table_ids_copy, "test_table")
        original_ids = CBit.get_eurostat_table_ids()
        @test length(original_ids) == 27  # Should not be modified
    end

    @testset "FIGARO Data Processing" begin
        # Test combine_figaro_tables function with invalid directory (skip_if_missing=true)
        result = CBit.combine_figaro_tables("nonexistent_directory")
        @test result === nothing

        # Test combine_figaro_tables function with invalid directory (skip_if_missing=false)
        @test_throws ArgumentError CBit.combine_figaro_tables("nonexistent_directory"; skip_if_missing=false)

        # Test with valid directory but missing files (skip_if_missing=true)
        temp_dir = mktempdir()
        result = CBit.combine_figaro_tables(temp_dir)
        @test result === nothing

        # Test with valid directory but missing files (skip_if_missing=false)
        @test_throws ArgumentError CBit.combine_figaro_tables(temp_dir; skip_if_missing=false)

        # Test parameter validation
        @test_throws ArgumentError CBit.combine_figaro_tables(temp_dir; input_tables=String[], skip_if_missing=false)

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
end
