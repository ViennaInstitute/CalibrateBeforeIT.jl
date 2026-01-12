module CalibrateBeforeIT

using CSV
using Downloads
using QuackIO
using DuckDB
using Tables
using DataFrames
using Dates
using JLD2
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
    "une_rt_q_h",
    "une_rt_a",
    "une_rt_a_h",
    "nama_10_a10",
    "namq_10_a10",
    "nama_10_a64",
    "nama_10_nfa_st",
    "nasq_10_f_bs",
    "gov_10q_ggdebt",
    "gov_10a_main",
    "nasa_10_nf_tr",
    "nasq_10_nf_tr",
    "gov_10q_ggnfa",
    "gov_10a_exp",
    "nama_10_an6",
    "nama_10_a64_e",
    "sbs_na_sca_r2",
    "sbs_ovw_act",
    "bd_9ac_l_form_r2",
    "bd_l_form",
    "cens_11an_r2"  # Census data for direct unemployed/inactive counts
]

# NOTE: Agriculture sector (A01, A02, A03) firm counts are not available in SBS tables.
# Current approach uses division-level or economy-wide employee/firm ratios as fallback.
#
# FUTURE ALTERNATIVE: Add Farm Structure Survey (FSS) tables:
#   - "ef_m_farmleg"  # Farm indicators by legal status, size, type
#   - "ef_kvftaa"     # Number of farms by utilized agricultural area
#
# However, FSS provides number of HOLDINGS (physical farms), not FIRMS (legal entities):
#   - Holdings ≠ Firms: One company may own multiple holdings; one farmer may operate
#     a holding without formal business registration
#   - No NACE sector breakdown (A01/A02/A03) in FSS tables
#   - Would require conceptual mapping between agricultural holdings and business firms
#
# For now, the division-level fallback provides sufficient accuracy given that
# agriculture represents a small share of most EU economies' total employment.

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

global eurostat_path = "data/010_eurostat_tables"
global calibration_output_path = "data/020_calibration_output"

include("utils.jl")
include("utils_firm_imputation.jl")
include("import_eurostat.jl")
include("import_figaro_data.jl")
include("import_data.jl")
include("import_calibration_data.jl")
include("get_params_and_initial_conditions.jl")
include("get_params_and_initial_conditions_netherlands_ocm.jl")
include("r2_to_nace64_conversion.jl")

end # module CalibrateBeforeIT
