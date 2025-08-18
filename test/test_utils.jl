using Test
using CalibrateBeforeIT
using Dates

@testset "Utils Tests" begin
    
    @testset "Linear Interpolation and Extrapolation Tests" begin
        # Example data
        y = [50.4700, 47.9040, 46.9640, 39.5860, 34.9000, 41.9340,
             47.2220, 55.4940, 56.5050, 65.1820, 64.8450]
        x = 3:13
        xi = 1:13

        # Expected result
        expected_yi = [55.602, 53.036, 50.4700, 47.9040, 46.9640, 39.5860, 34.9000, 41.9340,
             47.2220, 55.4940, 56.5050, 65.1820, 64.8450]

        # Test if the output matches the expected result
        @test CalibrateBeforeIT.linear_interp_extrap(x, y, xi) ≈ expected_yi

        # Additional test cases can be added here
        # For example, testing with different input ranges or edge cases
    end

    @testset "DateTime Conversion" begin
        @test CalibrateBeforeIT.date2num(DateTime(2010, 12, 31)) == 734503
        @test CalibrateBeforeIT.date2num(2010, 12, 31) == 734503
        @test CalibrateBeforeIT.date2num_yearly(2010:2022) ==
            [734503, 734868, 735234, 735599, 735964, 736329, 736695,
             737060, 737425, 737790, 738156, 738521, 738886]
    end

end
