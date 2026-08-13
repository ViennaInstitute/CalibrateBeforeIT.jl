"""
    _monthly_to_quarterly_mean(monthly::AbstractVector) -> Vector{Float64}

Aggregate a flat monthly series to quarterly via arithmetic mean of each
3-calendar-month block. If `length(monthly)` is not a multiple of 3 the
trailing 1-2 months are dropped (caller is responsible for passing a
full-month-grid vector).
"""
function _monthly_to_quarterly_mean(monthly::AbstractVector)
    n_quarters = length(monthly) ÷ 3
    out = Vector{Float64}(undef, n_quarters)
    for q in 1:n_quarters
        i = 3 * (q - 1) + 1
        out[q] = (monthly[i] + monthly[i + 1] + monthly[i + 2]) / 3.0
    end
    return out
end

"""
    _in_sample_interp(v::AbstractVector{<:Union{Missing,Real}}) -> Vector{Union{Missing,Float64}}

Linearly interpolate interior missing values; clamp leading/trailing missing
to the nearest observed value (no extrapolation). If all values are missing,
return the input unchanged.
"""
function _in_sample_interp(v::AbstractVector{<:Union{Missing,Real}})
    n = length(v)
    out = Vector{Union{Missing,Float64}}(undef, n)
    for i in 1:n
        out[i] = v[i]
    end

    observed = findall(!ismissing, v)
    if isempty(observed)
        return out  # all missing -> leave as-is
    end

    # Leading: clamp to first observed
    first_obs = observed[1]
    if first_obs > 1
        for i in 1:(first_obs - 1)
            out[i] = v[first_obs]
        end
    end

    # Interior: linear interpolation between surrounding observed points
    for k in 2:length(observed)
        a = observed[k - 1]
        b = observed[k]
        if b - a > 1
            ya = Float64(v[a])
            yb = Float64(v[b])
            for i in (a + 1):(b - 1)
                t = (i - a) / (b - a)
                out[i] = ya + t * (yb - ya)
            end
        end
    end

    # Trailing: clamp to last observed
    last_obs = observed[end]
    if last_obs < n
        for i in (last_obs + 1):n
            out[i] = v[last_obs]
        end
    end

    return out
end

"""
    _gap_fill_quarterly(reported, aggregated) -> Vector{Float64}

Merge a reported quarterly series with a monthly-aggregated quarterly series:
where `reported` is non-missing, keep it; where missing, take `aggregated`.
Both vectors must have the same length.
"""
function _gap_fill_quarterly(reported::AbstractVector{Union{Missing,Float64}},
                             aggregated::AbstractVector)
    if length(reported) != length(aggregated)
        throw(DimensionMismatch(
            "reported (len=$(length(reported))) and aggregated (len=$(length(aggregated))) must have the same length"))
    end
    out = Vector{Float64}(undef, length(reported))
    for i in eachindex(reported)
        if ismissing(reported[i])
            out[i] = Float64(aggregated[i])
        else
            out[i] = Float64(reported[i])
        end
    end
    return out
end

"""
    aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year, geos, int_rt="IRT_M3")

For each geo in `geos`, produce a gap-filled `irt_st_q.parquet` from the
monthly-aggregated `int_rt` values: read `irt_st_m`, left-join to the full
month grid (`start_year..end_year` x 12), in-sample linear-interpolate missing
months, aggregate to quarterly via mean-of-3-months, then merge — reported
quarterly (from `irt_st_q_raw.parquet`) preserved, only missing quarters filled
from the monthly aggregate. Rows in `irt_st_q_raw.parquet` for geos NOT in
`geos` (e.g. EA) are copied through unchanged.

The function reads from `irt_st_q_raw.parquet` (the as-downloaded reported
quarterly) and writes the gap-filled result to `irt_st_q.parquet` via DuckDB
`COPY ... TO`. If `irt_st_q.parquet` already exists it is overwritten.

If `irt_st_m.parquet` or `irt_st_q_raw.parquet` does not exist, the function
warns and returns without writing `irt_st_q.parquet`.
"""
function aggregate_irt_st_monthly_to_quarterly(conn;
                                                start_year::Int,
                                                end_year::Int,
                                                geos::Vector{String},
                                                int_rt::String="IRT_M3")
    m_file = pqfile("irt_st_m")
    q_raw_file = pqfile("irt_st_q_raw")
    q_file = pqfile("irt_st_q")

    if !isfile(m_file)
        @warn "irt_st_m.parquet not found at $m_file — skipping gap-fill of irt_st_q"
        return nothing
    end
    if !isfile(q_raw_file)
        @warn "irt_st_q_raw.parquet not found at $q_raw_file — cannot gap-fill"
        return nothing
    end
    if isempty(geos)
        @warn "aggregate_irt_st_monthly_to_quarterly called with empty geos — nothing to do"
        return nothing
    end

    # Full month grid as "YYYY-MM" strings, in order
    all_months = String[]
    for y in start_year:end_year, m in 1:12
        push!(all_months, string(y, "-", lpad(string(m), 2, '0')))
    end
    # Full quarter grid as "YYYY-Qq" strings, in order
    all_quarters = String[]
    for y in start_year:end_year, q in 1:4
        push!(all_quarters, string(y, "-Q", q))
    end
    months_str = join(["'$m'" for m in all_months], ",")
    quarters_str = join(["'$q'" for q in all_quarters], ",")

    # Gap-filled rows accumulated across all geos
    filled_rows = []  # Vector of NamedTuples (freq, int_rt, geo, time, value)
    filled_geos = String[]  # only geos actually gap-filled (rows of these get dropped+rewritten)

    for geo in geos
        # 1. Read monthly for this geo
        sql = "SELECT time, value FROM '$m_file' WHERE geo='$(geo)' AND int_rt='$(int_rt)' AND time IN ($(months_str)) ORDER BY time"
        m_raw = execute_debug(conn, sql)  # DataFrame with columns time, value
        # Build a Dict time -> value (drop missing)
        m_dict = Dict{String, Float64}()
        for row in eachrow(m_raw)
            v = row.value
            if !ismissing(v) && v !== nothing
                m_dict[row.time] = Float64(v)
            end
        end

        # GUARD: if no monthly data at all, skip this geo (leave irt_st_q unchanged)
        if isempty(m_dict)
            @warn "No monthly $(int_rt) data for geo='$(geo)' in irt_st_m — leaving irt_st_q unchanged for this geo"
            continue
        end

        # 2. Left-join to full month grid
        monthly_vec = Vector{Union{Missing,Float64}}(undef, length(all_months))
        for (i, m) in enumerate(all_months)
            monthly_vec[i] = haskey(m_dict, m) ? m_dict[m] : missing
        end

        # 3. In-sample interpolation
        monthly_interp = _in_sample_interp(monthly_vec)

        # 4. Aggregate to quarterly
        quarterly_agg = _monthly_to_quarterly_mean(monthly_interp)

        # 5. Read reported quarterly for this geo (from the raw, pre-gap-fill file)
        sql = "SELECT time, value FROM '$q_raw_file' WHERE geo='$(geo)' AND int_rt='$(int_rt)' AND time IN ($(quarters_str)) ORDER BY time"
        q_raw = execute_debug(conn, sql)
        q_dict = Dict{String, Union{Missing,Float64}}()
        for row in eachrow(q_raw)
            v = row.value
            q_dict[row.time] = ismissing(v) || v === nothing ? missing : Float64(v)
        end
        reported = Vector{Union{Missing,Float64}}(undef, length(all_quarters))
        for (i, q) in enumerate(all_quarters)
            reported[i] = haskey(q_dict, q) ? q_dict[q] : missing
        end

        # 6. Gap-fill
        filled = _gap_fill_quarterly(reported, quarterly_agg)

        for (i, q) in enumerate(all_quarters)
            push!(filled_rows, (freq="Q", int_rt=int_rt, geo=geo, time=q, value=filled[i]))
        end
        push!(filled_geos, geo)
    end

    # 7. Write irt_st_q.parquet (gap-filled) from irt_st_q_raw.parquet (reported):
    #    a. keep all rows from raw that are NOT (geo in filled_geos AND int_rt)
    #       within the grid's time range; geos with no monthly data are NOT in
    #       filled_geos, so their rows are copied through unchanged
    #    b. write kept rows + filled_rows to irt_st_q.parquet via COPY
    if isempty(filled_geos)
        @warn "No geos were gap-filled (none had monthly data) — copying irt_st_q_raw to irt_st_q unchanged"
        DBInterface.execute(conn, "COPY (SELECT * FROM '$q_raw_file') TO '$q_file' (FORMAT parquet)")
        return nothing
    end
    geo_filter = join(["(geo='$(g)' AND int_rt='$(int_rt)')" for g in filled_geos], " OR ")
    keep_sql = "SELECT freq, int_rt, geo, time, value FROM '$q_raw_file' WHERE NOT ($(geo_filter)) OR time NOT IN ($(quarters_str))"

    # Create a temp table with the filled rows, then COPY the union
    tmp_table = "tmp_irt_st_filled_$(abs(hash((geos, int_rt, start_year, end_year))))"
    DBInterface.execute(conn, "CREATE OR REPLACE TABLE $(tmp_table) (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
    if !isempty(filled_rows)
        values_clause = join(
            ["('Q','$(int_rt)','$(r.geo)','$(r.time)',$(r.value))" for r in filled_rows],
            ",")
        DBInterface.execute(conn, "INSERT INTO $(tmp_table) VALUES $(values_clause)")
    end
    union_sql = "$(keep_sql) UNION ALL SELECT freq, int_rt, geo, time, value FROM $(tmp_table)"

    tmp_out = q_file * ".tmp"
    try
        DBInterface.execute(conn, "COPY ($(union_sql)) TO '$(tmp_out)' (FORMAT parquet)")
        mv(tmp_out, q_file; force=true)
    finally
        DBInterface.execute(conn, "DROP TABLE $(tmp_table)")
        isfile(tmp_out) && rm(tmp_out; force=true)
    end
    @info "aggregate_irt_st_monthly_to_quarterly: gap-filled $(length(filled_geos)) geo(s) for int_rt='$(int_rt)'; wrote $q_file (from $q_raw_file)"
    return nothing
end
