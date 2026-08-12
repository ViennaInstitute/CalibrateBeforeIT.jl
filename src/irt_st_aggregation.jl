using Dates
using Interpolations

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
    _in_sample_interp(v::AbstractVector{Union{Missing,Float64}}) -> Vector{Union{Missing,Float64}}

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
