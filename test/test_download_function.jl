"""
    test_download_function.jl

Minimal test for the download_to_parquet function.
"""

using Test
import CalibrateBeforeIT as CBit

@testset "download_to_parquet function" begin
    
    # Test with the specific table we've been using
    table_id = "nama_10_an6"
    save_path = "data/010_eurostat_tables"
    
    @testset "Basic functionality" begin
        # Test that function returns the expected structure
        result = CBit.download_to_parquet(table_id, save_path; use_cached_tsv=true)
        
        # Check return value structure
        @test haskey(result, :tsv_path)
        @test haskey(result, :parquet_path)
        @test haskey(result, :rows_processed)
        @test haskey(result, :columns_processed)
        @test haskey(result, :download_time)
        @test haskey(result, :processing_time)
        @test haskey(result, :metadata)
        
        # Check that files exist
        @test isfile(result.tsv_path)
        @test isfile(result.parquet_path)
        
        # Check that data was processed
        @test result.rows_processed > 0
        @test result.columns_processed > 0
        
        # Check timing is reasonable
        @test result.download_time >= 0
        @test result.processing_time > 0
        
        # Check metadata structure
        @test haskey(result.metadata, :table_id)
        @test result.metadata.table_id == table_id
        
        println("✅ Processed $(result.rows_processed) rows with $(result.columns_processed) columns")
        println("   Download: $(round(result.download_time, digits=2))s, Processing: $(round(result.processing_time, digits=2))s")
    end
    
end
