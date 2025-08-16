"""
CI-friendly tests for CalibrateBeforeIT.jl
These tests run quickly and don't require large data downloads.
"""

using Test
import CalibrateBeforeIT as CBit

@testset "CI Tests" begin
    
    @testset "Package Loading" begin
        @test CBit isa Module
        @test hasmethod(CBit.get_eurostat_table_ids, ())
        @test hasmethod(CBit.combine_figaro_tables, (String,))
    end
    
    @testset "Eurostat Table Management" begin
        table_ids = CBit.get_eurostat_table_ids()
        
        @test isa(table_ids, Vector{String})
        @test length(table_ids) == 24
        @test "naio_10_fcp_ii1" in table_ids
        @test "nama_10_gdp" in table_ids
        
        # Test that function returns a copy
        table_ids_copy = CBit.get_eurostat_table_ids()
        push!(table_ids_copy, "test_table")
        original_ids = CBit.get_eurostat_table_ids()
        @test length(original_ids) == 24
    end
    
    @testset "Error Handling" begin
        # Test custom exception types
        @test CBit.DownloadError("test") isa CBit.DownloadError
        @test CBit.ProcessingError("test") isa CBit.ProcessingError
        
        # Test error display
        io = IOBuffer()
        Base.showerror(io, CBit.DownloadError("test message"))
        @test String(take!(io)) == "DownloadError: test message"
    end
    
    @testset "FIGARO Function Parameters" begin
        # Test that the function handles missing directories gracefully
        result = CBit.combine_figaro_tables("nonexistent_directory")
        @test result === nothing
        
        # Test parameter validation
        @test_throws ArgumentError CBit.combine_figaro_tables("nonexistent_directory"; 
                                                             input_tables=String[], 
                                                             skip_if_missing=false)
    end
end
