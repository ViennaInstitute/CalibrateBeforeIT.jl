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

using Dates

date2num(d::Dates.DateTime) = Int64(Dates.value(d - MATLAB_EPOCH) / (1000 * 60 * 60 * 24))
date2num(year::Int64, month::Int64, day::Int64) = date2num(DateTime(year, month, day))
date2num_yearly(years_range::UnitRange) = [date2num(this_year, 12, 31) for this_year in years_range]
date2num_quarterly(years_range::UnitRange) = reduce(vcat, [[date2num(this_year, 3, 31), date2num(this_year, 6, 30), date2num(this_year, 9, 30), date2num(this_year, 12, 31)] for this_year in years_range])

# inverse function of the above
const MATLAB_EPOCH = Dates.DateTime(-1, 12, 31)
num2date(n::Number) = MATLAB_EPOCH + Dates.Millisecond(round(Int64, n * 1000 * 60 * 60 * 24))

# end
