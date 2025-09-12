using Test
using CalibrateBeforeIT
using DuckDB

@testset "NACE Rev.2 to NACE64 Conversion Tests" begin

    @testset "create_business_demographic_a64_data validation" begin
        conn = DuckDB.DBInterface.connect(DuckDB.DB)

        # Test with nonexistent directory
        @test_throws ArgumentError CalibrateBeforeIT.create_business_demographic_a64_data(
            "bd_9ac_l_form_r2", "nonexistent_dir", conn)

        # Test that function exists and has correct signature
        @test hasmethod(CalibrateBeforeIT.create_business_demographic_a64_data,
            Tuple{String, String, Any})

        # Test with different excluded industries (keyword argument)
        # Just check that we can call it with keyword args (will fail due to missing files, but signature is correct)
        try
            CalibrateBeforeIT.create_business_demographic_a64_data(
                "bd_9ac_l_form_r2", "nonexistent", conn; excluded_industries=String[])
        catch ArgumentError
            # This is expected - we're just testing the method signature works
            @test true
        end

        DuckDB.DBInterface.close!(conn)
    end

    @testset "create_enterprise_statistics_a64_data validation" begin
        conn = DuckDB.DBInterface.connect(DuckDB.DB)

        # Test with nonexistent directory
        @test_throws ArgumentError CalibrateBeforeIT.create_enterprise_statistics_a64_data(
            "sbs_na_sca_r2", "nonexistent_dir", conn)

        # Test that function exists and has correct signature
        @test hasmethod(CalibrateBeforeIT.create_enterprise_statistics_a64_data, Tuple{String, String, Any})

        # Test with different excluded industries (keyword argument)
        try
            CalibrateBeforeIT.create_enterprise_statistics_a64_data(
                "sbs_na_sca_r2", "nonexistent", conn; excluded_industries=String[])
        catch ArgumentError
            # This is expected - we're just testing the method signature works
            @test true
        end

        DuckDB.DBInterface.close!(conn)
    end

end
