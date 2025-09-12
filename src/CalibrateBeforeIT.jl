module CalibrateBeforeIT

using CSV
using Downloads
using QuackIO
using DuckDB
using Tables
using DataFrames
using Dates
using StatsBase ## only for cov in get_params_and_initial_conditions

# Eurostat table IDs required for calibration
const ALL_EUROSTAT_TABLE_IDS = [
    "naio_10_fcp_ii1",
    "naio_10_fcp_ii2",
    "naio_10_fcp_ii3",
    "naio_10_fcp_ii4",
    "nama_10_gdp",
    "namq_10_gdp",
    "irt_st_q",
    "irt_st_a",
    "nama_10_pe",
    "namq_10_pe",
    "une_rt_q",
    "une_rt_a",
    "nama_10_a10",
    "namq_10_a10",
    "nama_10_a64",
    "nama_10_nfa_st",
    "nasq_10_f_bs",
    "gov_10q_ggdebt",
    "gov_10a_main",
    "nasa_10_nf_tr",
    "gov_10a_exp",
    "nama_10_an6",
    "nama_10_a64_e",
    "sbs_na_sca_r2",
    "sbs_ovw_act",
    "bd_9ac_l_form_r2",
    "bd_l_form"
]

"""
    get_eurostat_table_ids()

Returns the list of all Eurostat table IDs required for calibration.

# Returns
- `Vector{String}`: Array of Eurostat table identifiers

# Example
```julia
import CalibrateBeforeIT as CBit
table_ids = CBit.get_eurostat_table_ids()
println("Number of tables: ", length(table_ids))
```
"""
function get_eurostat_table_ids()
    return copy(ALL_EUROSTAT_TABLE_IDS)
end

global save_path = "data/010_eurostat_tables"

include("utils.jl")
include("import_eurostat.jl")
include("import_figaro_data.jl")
include("import_data.jl")
include("import_calibration_data.jl")
include("get_params_and_initial_conditions.jl")
include("r2_to_nace64_conversion.jl")

end # module CalibrateBeforeIT
