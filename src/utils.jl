# Constants for date conversion
const MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24

# MATLAB epoch for date conversions
const MATLAB_EPOCH = Dates.DateTime(-1, 12, 31)


# File path utilities
function pqfile(table_id::String)::String
    # Generate parquet file path from table ID
    joinpath(eurostat_path, "$(table_id).parquet")
end

# Database query utilities
function execute(conn, query::String)::Vector
    res = values(columntable(DBInterface.execute(conn, query)))[1]
    return res
end

function execute(conn, query::String, dims::Tuple{Vararg{Int}})::Array
    # Execute SQL query and reshape result to specified dimensions
    raw_res = execute(conn, query)
    res = reshape(raw_res, dims)
    return res
end

# Query the `value` column of a Eurostat table for one geo and the periods in
# `times_str`; `filters` are additional SQL conditions
function query_values(conn, table_id::String, geo, times_str::String, filters::String;
                      order::String = "time")::Vector
    execute(conn, "SELECT value FROM '$(pqfile(table_id))' WHERE time IN ($(times_str)) AND geo='$(geo)' AND $(filters) ORDER BY $(order)")
end

# Database query utilities -- debug version
function execute_debug(conn, query::String)::DataFrame
    # Execute SQL query and return a DataFrame
    res = DataFrame(columntable(DBInterface.execute(conn, query)))
    return res
end

function extract_years(conn, table_id::String,
                       start_year::Int64 = typemin(Int64),
                       end_year::Int64 = typemax(Int64))::Vector{Int64}
    sqlquery = "SELECT DISTINCT time FROM '$(pqfile(table_id))' ORDER BY time"
    res_years = try
        parse.(Int64, execute(conn, sqlquery))
    catch e
        error("Failed to parse years from the database: $e")
    end
    filter!(x -> x >= start_year && x <= end_year, res_years)
    return res_years
end

# NACE64 industries that are not part of the model
const EXCLUDED_NACE64 = ["L68A", "T", "U"]

# Quoted, comma-separated list for SQL `IN (...)` clauses
sql_list(items)::String = join(["'$(item)'" for item in items], ", ")

# SQL lists of the years and of the year-quarters ("2010-Q1") in `years`, as
# found in the `time` column of annual and quarterly Eurostat tables
sql_years_quarters(years::UnitRange{Int64}) =
    (sql_list(years), sql_list(["$(year)-Q$(quarter)" for year in years for quarter in 1:4]))

# Interpolation utilities
function linear_interp_extrap(x, y, xi)::Vector
    # Create linear interpolation object with extrapolation
    itp = linear_interpolation(x, y, extrapolation_bc = Line())

    # Evaluate the interpolation/extrapolation at points xi
    return itp(xi)
end

# Date conversion utilities (MATLAB-compatible)
date2num(d::Dates.DateTime)::Int64 = Int64(Dates.value(d - MATLAB_EPOCH) / MILLISECONDS_PER_DAY)
date2num(year::Int64, month::Int64, day::Int64)::Int64 = date2num(DateTime(year, month, day))
date2num_yearly(years_range::UnitRange{Int64})::Vector{Int64} = [date2num(this_year, 12, 31) for this_year in years_range]
date2num_quarterly(years_range::UnitRange{Int64})::Vector{Int64} = reduce(vcat, [[date2num(this_year, 3, 31), date2num(this_year, 6, 30), date2num(this_year, 9, 30), date2num(this_year, 12, 31)] for this_year in years_range])

# Convert MATLAB date number back to DateTime
num2date(n::Number)::Dates.DateTime = MATLAB_EPOCH + Dates.Millisecond(round(Int64, n * MILLISECONDS_PER_DAY))

