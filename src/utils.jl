# module Utils
# export pqfile, execute
# using DuckDB, Tables

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

using Test
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


# end
