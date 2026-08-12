## One-time consistency check: compare reported quarterly IRT_M3 (irt_st_q)
## against monthly-aggregated IRT_M3 (irt_st_m aggregated to quarterly) for
## non-EA countries, on the overlap window where both exist.
##
## Run once, commit the outputs to docs/irt_st_consistency/, cite in the
## working paper. NOT part of the calibration pipeline.

cd(@__DIR__)
using Pkg
Pkg.activate("..")

using DuckDB
using Dates
using Tables
using DataFrames
using CSV
using Statistics

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

rows = []  # (geo, time, reported, aggregated, abs_diff, rel_diff)

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
end

# Write CSV
df = DataFrame(rows)
csv_path = joinpath(OUT_DIR, "overlap_comparison.csv")
CSV.write(csv_path, df)
println("Wrote $(length(rows)) rows to $csv_path")

# Summary
open(joinpath(OUT_DIR, "summary.md"), "w") do io
    println(io, "# irt_st aggregation consistency check")
    println(io)
    println(io, "Compares reported quarterly IRT_M3 (`irt_st_q`) against monthly-aggregated")
    println(io, "IRT_M3 (`irt_st_m`, mean of 3 calendar months) for non-EA countries, on the")
    println(io, "overlap window where both exist. Aggregation method verified against the EA")
    println(io, "aggregate (mean |diff| ~0.0002 over 2018-2021).")
    println(io)
    println(io, "Generated: $(Dates.now())")
    println(io)
    println(io, "## Per-geo summary")
    println(io)
    println(io, "| geo | n_overlap | mean |diff| | median |diff| | max |diff| | % within 1e-4 |")
    println(io, "|-----|----------:|-----------:|-------------:|-----------:|--------------:|")
    for geo in NON_EA_GEOS
        sub = filter(:geo => ==(geo), df)
        if nrow(sub) == 0
            println(io, "| $geo | 0 | — | — | — | — |")
            continue
        end
        n = nrow(sub)
        mean_abs = mean(sub.abs_diff)
        med_abs = median(sub.abs_diff)
        max_abs = maximum(sub.abs_diff)
        within = count(<(1e-4), sub.abs_diff)
        pct = 100.0 * within / n
        println(io, "| $geo | $n | $(round(mean_abs, digits=6)) | $(round(med_abs, digits=6)) | $(round(max_abs, digits=6)) | $(round(pct, digits=1))% |")
    end
    println(io)
    println(io, "## Interpretation")
    println(io)
    println(io, "If the monthly-aggregated values match the reported quarterly within rounding")
    println(io, "(mean |diff| < 1e-3, >95% within 1e-4), the monthly source is a trustworthy")
    println(io, "fill for the post-2015Q1 gap in irt_st_q for BG/CZ/HU/PL/RO.")
end
println("Wrote summary to $(joinpath(OUT_DIR, "summary.md"))")
