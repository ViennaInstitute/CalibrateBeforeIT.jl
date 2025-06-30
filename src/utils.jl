# module Utils
# export pqfile, execute
# using DuckDB, Tables
using Test

function pqfile(table_id)
    joinpath(save_path, "$(table_id).parquet")
end

function execute(conn, query)
    res = values(columntable(DBInterface.execute(conn, query)))[1]
    return res
end

function execute(conn, query, dims)
    raw_res = execute(conn, query)
    res = reshape(raw_res, dims)
    return res
end

using Interpolations

function linear_interp_extrap(x, y, xi)
    # Create linear interpolation object
    itp = linear_interpolation(x, y, extrapolation_bc = Line())

    # Evaluate the interpolation/extrapolation at points xi
    return itp(xi)
end

# # Example usage:
# x = [50.4700, 47.9040, 46.9640, 39.5860, 34.9000, 41.9340,
#      47.2220, 55.4940, 56.5050, 65.1820, 64.8450]
# y = 3:13
# # y = collect(1:11)
# xi = 1:13  # Points to interpolate/extrapolate
# yi = linear_interp_extrap(x, y, xi)
# println(yi)

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
    @test linear_interp_extrap(x, y, xi) ≈ expected_yi

    # Additional test cases can be added here
    # For example, testing with different input ranges or edge cases
end


date2num(d::Dates.DateTime) = Int64(Dates.value(d - MATLAB_EPOCH) / (1000 * 60 * 60 * 24))
date2num(year::Int64, month::Int64, day::Int64) = date2num(DateTime(year, month, day))
date2num_yearly(years_range::UnitRange) = [date2num(this_year, 12, 31) for this_year in years_range]
date2num_quarterly(years_range::UnitRange) = reduce(vcat, [[date2num(this_year, 3, 31), date2num(this_year, 6, 30), date2num(this_year, 9, 30), date2num(this_year, 12, 31)] for this_year in years_range])

# inverse function of the above
const MATLAB_EPOCH = Dates.DateTime(-1, 12, 31)
num2date(n::Number) = MATLAB_EPOCH + Dates.Millisecond(round(Int64, n * 1000 * 60 * 60 * 24))

@testset "DateTime Conversion" begin
    @test date2num(DateTime(2010, 12, 31)) == 734503
    @test date2num(2010, 12, 31) == 734503
    @test date2num(start_calibration_year:end_calibration_year, 12, 31) ==
        [734503, 734868, 735234, 735599, 735964, 736329, 736695,
         737060, 737425, 737790, 738156, 738521, 738886]
end


# end
