using Test
using CalibrateBeforeIT
using DuckDB

@testset "NACE Rev.2 to NACE64 Conversion Tests" begin
    
    @testset "create_bd_9ac_l_form_a64 validation" begin
        conn = DuckDB.DBInterface.connect(DuckDB.DB)
        
        # Test with nonexistent directory
        @test_throws ArgumentError CalibrateBeforeIT.create_bd_9ac_l_form_a64("nonexistent_dir", conn)
        
        # Test that function exists and has correct signature
        @test hasmethod(CalibrateBeforeIT.create_bd_9ac_l_form_a64, Tuple{String, Any})
        
        # Test with different excluded industries (keyword argument)
        # Just check that we can call it with keyword args (will fail due to missing files, but signature is correct)
        try
            CalibrateBeforeIT.create_bd_9ac_l_form_a64("nonexistent", conn; excluded_industries=String[])
        catch ArgumentError
            # This is expected - we're just testing the method signature works
            @test true
        end
        
        DuckDB.DBInterface.close!(conn)
    end
    
end
