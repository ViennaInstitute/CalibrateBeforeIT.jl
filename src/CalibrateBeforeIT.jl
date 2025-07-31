module CalibrateBeforeIT

using CSV
using Downloads
using QuackIO
using DuckDB
using Tables
using DataFrames
using Dates
using StatsBase ## only for cov in get_params_and_initial_conditions


include("utils.jl")
include("import_eurostat.jl")
include("import_figaro_data.jl")
include("import_data.jl")
include("import_calibration_data.jl")
include("get_params_and_initial_conditions.jl")

global save_path = "data/010_eurostat_tables"

end # module CalibrateBeforeIT
