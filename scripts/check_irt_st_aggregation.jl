## One-time consistency check: compare reported quarterly IRT_M3 (irt_st_q_raw)
## against monthly-aggregated IRT_M3 (irt_st_m aggregated to quarterly) for
## non-EA countries. Produces a per-country line plot (reported quarterly in
## red, aggregated quarterly in blue) for visual inspection of imputation
## quality. Also writes a CSV of the overlap comparison.
##
## Run once, commit the outputs to docs/irt_st_consistency/, cite in the
## working paper. NOT part of the calibration pipeline.

cd(@__DIR__)
cd("..")
using Pkg
Pkg.activate(".")

using DuckDB
using Dates
using Tables
using DataFrames
using CSV
using Statistics
using Plots

import CalibrateBeforeIT as CBit

const OUT_DIR = joinpath(@__DIR__, "..", "docs", "irt_st_consistency")
mkpath(OUT_DIR)

const NON_EA_GEOS = ["BG", "CZ", "DK", "HU", "PL", "RO", "SE"]
const START_YEAR, END_YEAR = 1996, 2024

conn = DBInterface.connect(DuckDB.DB())

# Build month and quarter grids
all_months = [string(y, "-", lpad(string(m), 2, '0')) for y in START_YEAR:END_YEAR for m in 1:12]
all_quarters = [string(y, "-Q", q) for y in START_YEAR:END_YEAR for q in 1:4]
months_str = join(["'$m'" for m in all_months], ",")
quarters_str = join(["'$q'" for q in all_quarters], ",")

# Numeric x-axis for plotting (year + fractional quarter)
quarter_x = Float64[]
for q in all_quarters
    y, qq = parse.(Int, split(q, "-Q"))
    push!(quarter_x, y + (qq - 1) / 4.0)
end

rows = []  # (geo, time, reported, aggregated, abs_diff, rel_diff) on overlap

for geo in NON_EA_GEOS
    # Monthly
    sql = "SELECT time, value FROM '$(CBit.pqfile("irt_st_m"))' WHERE geo='$(geo)' AND int_rt='IRT_M3' AND time IN ($(months_str)) ORDER BY time"
    m_raw = CBit.execute_debug(conn, sql)
    m_dict = Dict{String, Float64}()
    for r in eachrow(m_raw)
        if !ismissing(r.value) && r.value !== nothing
            m_dict[r.time] = Float64(r.value)
        end
    end
    if isempty(m_dict)
        @warn "No monthly IRT_M3 data for geo='$(geo)' — skipping"
        continue
    end
    monthly_vec = Vector{Union{Missing,Float64}}(undef, length(all_months))
    for (i, m) in enumerate(all_months)
        monthly_vec[i] = haskey(m_dict, m) ? m_dict[m] : missing
    end
    monthly_interp = CBit._in_sample_interp(monthly_vec)
    quarterly_agg = CBit._monthly_to_quarterly_mean(monthly_interp)

    # Reported quarterly (from the raw, pre-gap-fill file)
    sql = "SELECT time, value FROM '$(CBit.pqfile("irt_st_q_raw"))' WHERE geo='$(geo)' AND int_rt='IRT_M3' AND time IN ($(quarters_str)) ORDER BY time"
    q_raw = CBit.execute_debug(conn, sql)
    q_dict = Dict{String, Union{Missing,Float64}}()
    for r in eachrow(q_raw)
        q_dict[r.time] = ismissing(r.value) || r.value === nothing ? missing : Float64(r.value)
    end

    # Build reported quarterly series (only where reported exists; missing otherwise)
    reported_x = Float64[]
    reported_y = Float64[]
    for (i, q) in enumerate(all_quarters)
        if haskey(q_dict, q) && !ismissing(q_dict[q])
            push!(reported_x, quarter_x[i])
            push!(reported_y, q_dict[q])
        end
    end

    # Overlap comparison rows (reported quarterly vs aggregated where both exist)
    for (i, q) in enumerate(all_quarters)
        if haskey(q_dict, q) && !ismissing(q_dict[q])
            reported = q_dict[q]
            aggregated = quarterly_agg[i]
            abs_diff = abs(reported - aggregated)
            rel_diff = abs_diff / max(abs(reported), 1e-12)
            push!(rows, (geo=geo, time=q, reported=reported,
                         aggregated=aggregated, abs_diff=abs_diff,
                         rel_diff=rel_diff))
        end
    end

    # Per-country plot: reported quarterly (red) + aggregated quarterly (blue)
    p = plot(quarter_x, quarterly_agg;
        seriestype=:line, color=:blue, linewidth=1.5,
        label="aggregated quarterly (from irt_st_m)",
        xlabel="year", ylabel="3-month rate (%)",
        title="$(geo) IRT_M3: reported quarterly vs aggregated quarterly",
        legend=:topright, size=(1000, 500))
    if !isempty(reported_x)
        plot!(p, reported_x, reported_y;
            seriestype=:line, color=:red, linewidth=1.5,
            label="reported quarterly (irt_st_q_raw)")
    end
    savefig(p, joinpath(OUT_DIR, "$(geo)_irt_m3_comparison.png"))
    println("Wrote plot for $(geo) (reported: $(length(reported_x)) quarters)")
end

# Write CSV of the overlap comparison
df = DataFrame(rows)
csv_path = joinpath(OUT_DIR, "overlap_comparison.csv")
CSV.write(csv_path, df)
println("Wrote $(length(rows)) rows to $csv_path")