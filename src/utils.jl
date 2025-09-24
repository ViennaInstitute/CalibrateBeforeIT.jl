# module Utils
# export pqfile, execute
# using DuckDB, Tables

# File path utilities
function pqfile(table_id::String)::String
    # Generate parquet file path from table ID
    joinpath(eurostat_path, "$(table_id).parquet")
end

# Database query utilities
function execute(conn, query::String)::Vector
    # Execute SQL query and return first column as vector
    res = values(columntable(DBInterface.execute(conn, query)))[1]
    return res
end

# Database query utilities -- debug version
function execute_debug(conn, query::String)::DataFrame
    # Execute SQL query and return a DataFrame
    res = DataFrame(columntable(DBInterface.execute(conn, sqlquery)))
    return res
end

function execute(conn, query::String, dims::Tuple{Vararg{Int}})::Array
    # Execute SQL query and reshape result to specified dimensions
    raw_res = execute(conn, query)
    res = reshape(raw_res, dims)
    return res
end

function extract_years(conn, table_id::String, start_calibration_year::Int64)::Vector{Int64}
    sqlquery = "SELECT DISTINCT time FROM '$(pqfile(table_id))' ORDER BY time"
    res_years = try
        parse.(Int64, execute(conn, sqlquery))
    catch e
        error("Failed to parse years from the database: $e")
    end
    filter!(x -> x >= start_calibration_year, res_years)
    return res_years
end

using Interpolations

# Interpolation utilities
function linear_interp_extrap(x::Vector, y::Vector, xi::Vector)::Vector
    # Create linear interpolation object with extrapolation
    itp = linear_interpolation(x, y, extrapolation_bc = Line())

    # Evaluate the interpolation/extrapolation at points xi
    return itp(xi)
end

using Dates

# Constants for date conversion
const MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24

# Date conversion utilities (MATLAB-compatible)
date2num(d::Dates.DateTime)::Int64 = Int64(Dates.value(d - MATLAB_EPOCH) / MILLISECONDS_PER_DAY)
date2num(year::Int64, month::Int64, day::Int64)::Int64 = date2num(DateTime(year, month, day))
date2num_yearly(years_range::UnitRange{Int64})::Vector{Int64} = [date2num(this_year, 12, 31) for this_year in years_range]
date2num_quarterly(years_range::UnitRange{Int64})::Vector{Int64} = reduce(vcat, [[date2num(this_year, 3, 31), date2num(this_year, 6, 30), date2num(this_year, 9, 30), date2num(this_year, 12, 31)] for this_year in years_range])

create_year_array_str(all_years::Vector)::String = join(["'$(year)'" for year in all_years], ", ")

# MATLAB epoch for date conversions
const MATLAB_EPOCH = Dates.DateTime(-1, 12, 31)
# Convert MATLAB date number back to DateTime
num2date(n::Number)::Dates.DateTime = MATLAB_EPOCH + Dates.Millisecond(round(Int64, n * MILLISECONDS_PER_DAY))

# end
