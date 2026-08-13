## One-time consistency check: compare reported monthly IRT_M3 (irt_st_m)
## against monthly-aggregated IRT_M3 (irt_st_m aggregated to quarterly) for
## non-EA countries. Produces a per-country line plot (reported monthly in red,
## aggregated quarterly in blue) for visual inspection of imputation quality.
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

# Numeric x-axes for plotting (year + fractional month/quarter)
month_x = Float64[]
for (i, m) in enumerate(all_months)
    y, mm = parse.(Int, split(m, "-"))
    push!(month_x, y + (mm - 1) / 12.0)
end
quarter_x = Float64[]
for (i, q) in enumerate(all_quarters)
    y, qq = parse.(Int, split(q, "-Q"))
    push!(quarter_x, y + (qq - 1) / 4.0)
end

rows = []  # (geo, time, reported, aggregated, abs_diff, rel_diff) on overlap

# Detect whether irt_st_q.parquet has already been gap-filled by Step 5
# (in which case the reported-quarterly vs aggregated-quarterly comparison is
# circular for the filled quarters). Heuristic: HU originally has NO reported
# quarterly after 2015-Q1, so a value at 2015-Q2 indicates gap-filling already
# ran. The plots (monthly red vs aggregated blue) remain valid regardless.
hu_check = CBit.execute_debug(conn,
    "SELECT value FROM '$(CBit.pqfile("irt_st_q"))' WHERE geo='HU' AND int_rt='IRT_M3' AND time='2015-Q2'")
already_filled = nrow(hu_check) > 0 && !ismissing(hu_check.value[1]) && hu_check.value[1] !== nothing
if already_filled
    @warn "irt_st_q.parquet appears already gap-filled (HU has a value at 2015-Q2). " *
          "The overlap CSV comparison will be circular for filled quarters. " *
          "Run this script BEFORE 02_preprocess Step 5 for a clean pre-fill comparison. " *
          "The per-country plots (monthly vs aggregated) remain valid."
end

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

    # Reported quarterly
    sql = "SELECT time, value FROM '$(CBit.pqfile("irt_st_q"))' WHERE geo='$(geo)' AND int_rt='IRT_M3' AND time IN ($(quarters_str)) ORDER BY time"
    q_raw = CBit.execute_debug(conn, sql)
    q_dict = Dict{String, Union{Missing,Float64}}()
    for r in eachrow(q_raw)
        q_dict[r.time] = ismissing(r.value) || r.value === nothing ? missing : Float64(r.value)
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

    # Per-country plot: reported monthly (red) + aggregated quarterly (blue)
    reported_monthly_x = Float64[]
    reported_monthly_y = Float64[]
    for (i, m) in enumerate(all_months)
        if haskey(m_dict, m)
            push!(reported_monthly_x, month_x[i])
            push!(reported_monthly_y, m_dict[m])
        end
    end

    p = plot(reported_monthly_x, reported_monthly_y;
        seriestype=:line, color=:red, linewidth=1.5,
        label="reported monthly (irt_st_m)",
        xlabel="year", ylabel="3-month rate (%)",
        title="$(geo) IRT_M3: reported monthly vs aggregated quarterly",
        legend=:topright, size=(1000, 500))
    plot!(p, quarter_x, quarterly_agg;
        seriestype=:line, color=:blue, linewidth=1.5,
        label="aggregated quarterly (mean of 3 months, interp)")
    savefig(p, joinpath(OUT_DIR, "$(geo)_irt_m3_comparison.png"))
    println("Wrote plot for $(geo)")
end

# Write CSV of the overlap comparison
df = DataFrame(rows)
csv_path = joinpath(OUT_DIR, "overlap_comparison.csv")
CSV.write(csv_path, df)
println("Wrote $(length(rows)) rows to $csv_path")
