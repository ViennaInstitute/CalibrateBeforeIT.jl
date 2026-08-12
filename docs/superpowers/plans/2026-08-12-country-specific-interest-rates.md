# Country-specific short-term interest rates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Feed each non-EA EU country its own 3-month short rate (national IBOR) into `data["euribor"]` instead of the EA Euribor, so `r_bar`, the Taylor-rule input `a1`, and `r_bar_series` in BeforeIT.jl are country-specific.

**Architecture:** Add `irt_st_m` (monthly short rates) to the download list. Preprocess: for each non-EA geo, aggregate monthly IRT_M3 to quarterly (mean of 3 calendar months), in-sample linear-interpolating missing months, and **gap-fill** `irt_st_q.parquet` (reported quarterly preserved, missing quarters filled from the monthly aggregate). Add a time-varying `is_euro_area_member(geo, date)` helper. In `import_data.jl`, dispatch the euribor query to `geo='EA'` for EA members and to `geo='$(geo)'` for non-EA. A separate one-time script validates the monthly-vs-quarterly consistency and writes the evidence to `docs/irt_st_consistency/`.

**Tech Stack:** Julia 1.11+, DuckDB (parquet queries), Interpolations.jl (linear interp), Test.jl. Spec: `docs/superpowers/specs/2026-08-12-country-specific-interest-rates-design.md`.

**Branch:** `feat/country-specific-rates` (already created and spec committed).

---

## File Structure

**New files:**
- `src/euro_area_membership.jl` — `EURO_AREA_JOIN_DATES` Dict, `is_euro_area_member(geo, date)`.
- `src/irt_st_aggregation.jl` — `aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year, geos, int_rt)`, plus helpers `_monthly_to_quarterly_mean`, `_gap_fill_quarterly`.
- `scripts/check_irt_st_aggregation.jl` — one-time consistency check (not in pipeline).
- `test/test_euro_area_membership.jl` — unit tests for the helper.
- `test/test_irt_st_aggregation.jl` — unit tests for monthly→quarterly + gap-fill on synthetic data.

**Modified files:**
- `src/CalibrateBeforeIT.jl` — add `"irt_st_m"` to `ALL_EUROSTAT_TABLE_IDS`; include the two new modules; export the new public functions.
- `src/import_data.jl:375-381` — euribor block dispatches by EA membership.
- `02_preprocess_raw_eurostat_tables.jl` — new Step 5.
- `test/runtests.jl` — include the two new test files.

**Unchanged:** `BeforeIT.jl`, `03_create_calibration_data.jl`, all existing tests, reference fixtures.

---

## Task 1: Add `irt_st_m` to the download list

**Files:**
- Modify: `src/CalibrateBeforeIT.jl:34` (add `"irt_st_m"` near `irt_st_q`/`irt_st_a`)

- [ ] **Step 1: Edit the table list**

In `src/CalibrateBeforeIT.jl`, find the block:
```julia
    "nama_10_gdp",
    "namq_10_gdp",
    "irt_st_q",
    "irt_st_a",
```
Add `"irt_st_m"` immediately after `"irt_st_q"`:
```julia
    "nama_10_gdp",
    "namq_10_gdp",
    "irt_st_q",
    "irt_st_m",
    "irt_st_a",
```

- [ ] **Step 2: Verify the export is unchanged**

`get_eurostat_table_ids()` (line ~79) returns `copy(ALL_EUROSTAT_TABLE_IDS)`, so the new ID is automatically exported. No code change needed.

- [ ] **Step 3: Sanity-check the module still loads**

Run:
```bash
julia --project=. -e 'import CalibrateBeforeIT as CBit; println(length(CBit.get_eurostat_table_ids()))'
```
Expected: prints a number one greater than before (was 32, should now be 33).

- [ ] **Step 4: Commit**

```bash
git add src/CalibrateBeforeIT.jl
git commit -m "Add irt_st_m to ALL_EUROSTAT_TABLE_IDS

Monthly short-term interest rates; needed to gap-fill irt_st_q for
non-EA countries (BG/CZ/HU/PL/RO) that stopped reporting quarterly
after 2015Q1."
```

---

## Task 2: `is_euro_area_member` helper (TDD)

**Files:**
- Create: `src/euro_area_membership.jl`
- Create: `test/test_euro_area_membership.jl`
- Modify: `src/CalibrateBeforeIT.jl` (include + export)
- Modify: `test/runtests.jl` (include the test file)

- [ ] **Step 1: Write the failing test**

Create `test/test_euro_area_membership.jl`:
```julia
using Test
using CalibrateBeforeIT
using Dates

@testset "Euro Area Membership Tests" begin

    @testset "Founding members (1999-01-01)" begin
        for geo in ["AT", "BE", "DE", "ES", "FI", "FR", "IE", "IT", "LU", "NL", "PT"]
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(1999, 1, 1)) == true
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(1998, 12, 31)) == false
        end
    end

    @testset "Late joiners" begin
        @test CalibrateBeforeIT.is_euro_area_member("EL", Date(2000, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("EL", Date(2001, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("SI", Date(2007, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("CY", Date(2008, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("MT", Date(2008, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("SK", Date(2009, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("EE", Date(2011, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("LV", Date(2014, 1, 1)) == true
        @test CalibrateBeforeIT.is_euro_area_member("LT", Date(2015, 1, 1)) == true
    end

    @testset "HR joined 2023" begin
        @test CalibrateBeforeIT.is_euro_area_member("HR", Date(2022, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("HR", Date(2023, 1, 1)) == true
    end

    @testset "BG joins 2026 (outside current calibration window)" begin
        @test CalibrateBeforeIT.is_euro_area_member("BG", Date(2025, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("BG", Date(2026, 1, 1)) == true
    end

    @testset "Non-EA countries (current window <= 2023-12-31)" begin
        for geo in ["CZ", "DK", "HU", "PL", "RO", "SE"]
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(2023, 12, 31)) == false
            @test CalibrateBeforeIT.is_euro_area_member(geo, Date(2024, 12, 31)) == false
        end
        @test CalibrateBeforeIT.is_euro_area_member("BG", Date(2023, 12, 31)) == false
    end

    @testset "Unknown geo returns false" begin
        @test CalibrateBeforeIT.is_euro_area_member("XX", Date(2024, 12, 31)) == false
        @test CalibrateBeforeIT.is_euro_area_member("EA", Date(2024, 12, 31)) == false
    end

    @testset "EURO_AREA_JOIN_DATES is exported and complete" begin
        @test haskey(CalibrateBeforeIT.EURO_AREA_JOIN_DATES, "AT")
        @test length(CalibrateBeforeIT.EURO_AREA_JOIN_DATES) == 20
    end
end
```

- [ ] **Step 2: Add the test to runtests.jl**

In `test/runtests.jl`, add inside the outer `@testset` block, before the closing `end`:
```julia
    @testset "Euro Area Membership Tests" begin
        include("test_euro_area_membership.jl")
    end
```

- [ ] **Step 3: Run the test to verify it fails**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: FAIL — `is_euro_area_member` not defined (UndefVarError or similar).

- [ ] **Step 4: Write the implementation**

Create `src/euro_area_membership.jl`:
```julia
using Dates

"""
    EURO_AREA_JOIN_DATES

Dict mapping EU country codes (String) to the Date they joined the euro area.
Used by [`is_euro_area_member`](@ref) to determine, for a given reference date,
which countries were EA members (and thus should use the EA Euribor as the
short-term interest rate) versus non-EA (which use their own national IBOR).

Source: ECB euro changeover timeline.
"""
const EURO_AREA_JOIN_DATES = Dict{String, Date}(
    "AT" => Date(1999, 1, 1),
    "BE" => Date(1999, 1, 1),
    "DE" => Date(1999, 1, 1),
    "ES" => Date(1999, 1, 1),
    "FI" => Date(1999, 1, 1),
    "FR" => Date(1999, 1, 1),
    "IE" => Date(1999, 1, 1),
    "IT" => Date(1999, 1, 1),
    "LU" => Date(1999, 1, 1),
    "NL" => Date(1999, 1, 1),
    "PT" => Date(1999, 1, 1),
    "EL" => Date(2001, 1, 1),
    "SI" => Date(2007, 1, 1),
    "CY" => Date(2008, 1, 1),
    "MT" => Date(2008, 1, 1),
    "SK" => Date(2009, 1, 1),
    "EE" => Date(2011, 1, 1),
    "LV" => Date(2014, 1, 1),
    "LT" => Date(2015, 1, 1),
    "HR" => Date(2023, 1, 1),
    "BG" => Date(2026, 1, 1),
)

"""
    is_euro_area_member(geo::String, date::Date) -> Bool

Return `true` if `geo` was a euro-area member on or before `date`.

For the current calibration window (`max_calibration_date = 2023-12-31`,
`end_year = 2024`), called with `date = Date(end_year, 12, 31)` this returns
`true` for the 20 EA members (incl. HR, joined 2023) and `false` for the 7
non-EA EU members (BG, CZ, DK, HU, PL, RO, SE). Future-proof: BG flips to
`true` automatically once `date >= Date(2026, 1, 1)`.
"""
is_euro_area_member(geo::String, date::Date) =
    haskey(EURO_AREA_JOIN_DATES, geo) && EURO_AREA_JOIN_DATES[geo] <= date
```

- [ ] **Step 5: Include and export in the main module**

In `src/CalibrateBeforeIT.jl`:

1. In the `export` line (line ~13), add `is_euro_area_member, EURO_AREA_JOIN_DATES`. The line currently reads:
```julia
export download_and_extract_zenodo_data, get_eurostat_table_ids,
    combine_tables, pqfile, execute, execute_debug, extract_years,
    linear_interp_extrap, unify_unemployment_rate_sources,
    get_valid_calibration_quarters
```
Append `is_euro_area_member, EURO_AREA_JOIN_DATES` to the end of the export list.

2. After the line `include("utils.jl")` (line ~86), add:
```julia
include("euro_area_membership.jl")
```

- [ ] **Step 6: Run the test to verify it passes**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: the "Euro Area Membership Tests" testset passes. (Other tests should still pass.)

- [ ] **Step 7: Commit**

```bash
git add src/euro_area_membership.jl src/CalibrateBeforeIT.jl test/test_euro_area_membership.jl test/runtests.jl
git commit -m "Add is_euro_area_member helper with join-date Dict

Time-varying EA membership: AT..PT (1999), EL (2001), SI (2007),
CY/MT (2008), SK (2009), EE (2011), LV (2014), LT (2015), HR (2023),
BG (2026). For the current calibration window (end_year=2024) returns
true for 20 EA members incl. HR and false for 7 non-EA incl. BG."
```

---

## Task 3: Monthly→quarterly aggregation helper (TDD, synthetic)

**Files:**
- Create: `src/irt_st_aggregation.jl`
- Create: `test/test_irt_st_aggregation.jl`
- Modify: `src/CalibrateBeforeIT.jl` (include + export)
- Modify: `test/runtests.jl` (include the test file)

This task implements the **pure** aggregation/gap-fill logic (no I/O, no DuckDB) so it can be unit-tested on synthetic data. Task 4 wires it into the parquet I/O.

- [ ] **Step 1: Write the failing test**

Create `test/test_irt_st_aggregation.jl`:
```julia
using Test
using CalibrateBeforeIT
using Dates

@testset "IRT_ST Aggregation Tests" begin

    @testset "_monthly_to_quarterly_mean: basic" begin
        # 3 months, one quarter
        monthly = [1.0, 2.0, 3.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0]
    end

    @testset "_monthly_to_quarterly_mean: multiple quarters" begin
        # 6 months, two quarters
        monthly = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0, 5.0]
    end

    @testset "_monthly_to_quarterly_mean: length not multiple of 3 drops remainder" begin
        monthly = [1.0, 2.0, 3.0, 4.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0]
    end

    @testset "_in_sample_interp: no missing" begin
        v = [1.0, 2.0, 3.0, 4.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 3.0, 4.0]
    end

    @testset "_in_sample_interp: interior missing" begin
        # missing at index 2, between 1.0 and 3.0 -> 2.0
        v = Union{Missing,Float64}[1.0, missing, 3.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 3.0]
    end

    @testset "_in_sample_interp: leading missing clamps to first observed" begin
        v = Union{Missing,Float64}[missing, missing, 3.0, 4.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [3.0, 3.0, 3.0, 4.0]
    end

    @testset "_in_sample_interp: trailing missing clamps to last observed" begin
        v = Union{Missing,Float64}[1.0, 2.0, missing, missing]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 2.0, 2.0]
    end

    @testset "_in_sample_interp: all missing returns all missing" begin
        v = Union{Missing,Float64}[missing, missing]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test all(ismissing, out)
    end

    @testset "_gap_fill_quarterly: reported preserved" begin
        reported   = Union{Missing,Float64}[5.0, missing, 7.0]
        aggregated = [2.0, 4.0, 6.0]
        out = CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
        @test out == [5.0, 4.0, 7.0]
    end

    @testset "_gap_fill_quarterly: all missing reported -> all aggregated" begin
        reported   = Union{Missing,Float64}[missing, missing, missing]
        aggregated = [2.0, 4.0, 6.0]
        out = CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
        @test out == [2.0, 4.0, 6.0]
    end

    @testset "_gap_fill_quarterly: lengths must match" begin
        reported   = Union{Missing,Float64}[5.0, missing]
        aggregated = [2.0, 4.0, 6.0]
        @test_throws DimensionMismatch CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
    end
end
```

- [ ] **Step 2: Add the test to runtests.jl**

In `test/runtests.jl`, inside the outer `@testset`, add:
```julia
    @testset "IRT_ST Aggregation Tests" begin
        include("test_irt_st_aggregation.jl")
    end
```

- [ ] **Step 3: Run the test to verify it fails**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: FAIL — `_monthly_to_quarterly_mean` not defined.

- [ ] **Step 4: Write the implementation**

Create `src/irt_st_aggregation.jl`:
```julia
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
function _in_sample_interp(v::AbstractVector{Union{Missing,Float64}})
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
```

- [ ] **Step 5: Include and export in the main module**

In `src/CalibrateBeforeIT.jl`:

1. Add to the `export` line: `_monthly_to_quarterly_mean, _in_sample_interp, _gap_fill_quarterly, aggregate_irt_st_monthly_to_quarterly`. (The `_`-prefixed helpers are exported only for testing; this matches the existing pattern where `linear_interp_extrap` is exported.)

2. After `include("euro_area_membership.jl")`, add:
```julia
include("irt_st_aggregation.jl")
```

- [ ] **Step 6: Run the test to verify it passes**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: "IRT_ST Aggregation Tests" testset passes.

- [ ] **Step 7: Commit**

```bash
git add src/irt_st_aggregation.jl src/CalibrateBeforeIT.jl test/test_irt_st_aggregation.jl test/runtests.jl
git commit -m "Add monthly->quarterly aggregation and gap-fill helpers

_monthly_to_quarterly_mean: arithmetic mean of 3 calendar months
(matches Eurostat's derivation, verified against EA irt_st_q).
_in_sample_interp: linear interpolation of interior missing months,
clamp leading/trailing to nearest observed (no extrapolation).
_gap_fill_quarterly: preserve reported quarterly, fill missing from
the monthly aggregate."
```

---

## Task 4: Parquet I/O wrapper — `aggregate_irt_st_monthly_to_quarterly`

**Files:**
- Modify: `src/irt_st_aggregation.jl` (add the I/O function)
- Modify: `test/test_irt_st_aggregation.jl` (add an integration-style test using a temp DuckDB)

This task adds the function that reads `irt_st_m.parquet` and `irt_st_q.parquet`, applies the helpers from Task 3, and rewrites `irt_st_q.parquet` with gap-filled non-EA rows. The test builds tiny in-memory parquet files via DuckDB and verifies the round-trip.

- [ ] **Step 1: Write the failing test**

Append to `test/test_irt_st_aggregation.jl`, inside the outer `@testset`:
```julia
    @testset "aggregate_irt_st_monthly_to_quarterly: end-to-end on synthetic parquet" begin
        using DuckDB
        using Tables

        # Set up a temp directory and a temp eurostat_path
        tmpdir = mktempdir()
        # We override CalibrateBeforeIT.eurostat_path so pqfile() resolves here
        original_path = CalibrateBeforeIT.eurostat_path
        CalibrateBeforeIT.eurostat_path = tmpdir

        try
            conn = DBInterface.connect(DuckDB.DB())

            # Build irt_st_m.parquet: monthly IRT_M3 for a fake geo "ZZ"
            # 2018 Q1: months 1,2,3 with values 1,2,3 -> quarterly mean 2.0
            # 2018 Q2: months 4,5,6 with values 4,missing,6 -> interp 5 -> mean 5.0
            # (no quarterly row reported for Q2 -> gap-fill should pick 5.0)
            months = ["2018-01", "2018-02", "2018-03",
                      "2018-04", "2018-05", "2018-06"]
            mvals  = [1.0, 2.0, 3.0, 4.0, missing, 6.0]
            rows = [(freq="M", int_rt="IRT_M3", geo="ZZ",
                     time=m, value=v) for (m, v) in zip(months, mvals)]
            # write via DuckDB COPY of an in-memory table
            DBInterface.execute(conn, "CREATE TABLE m (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
            DBInterface.execute(conn, "INSERT INTO m VALUES $(join(["('M','IRT_M3','ZZ','$(m)',$(ismissing(v) ? "NULL" : v))" for (m,v) in zip(months,mvals)], ","))")
            DBInterface.execute(conn, "COPY m TO '$(tmpdir)/irt_st_m.parquet' (FORMAT parquet)")

            # Build irt_st_q.parquet: reported quarterly for ZZ at Q1 only (5.0)
            qrows = [(freq="Q", int_rt="IRT_M3", geo="ZZ", time="2018-Q1", value=5.0),
                     (freq="Q", int_rt="IRT_M3", geo="ZZ", time="2018-Q2", value=missing)]
            DBInterface.execute(conn, "CREATE TABLE q (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
            DBInterface.execute(conn, "INSERT INTO q VALUES ('Q','IRT_M3','ZZ','2018-Q1',5.0),('Q','IRT_M3','ZZ','2018-Q2',NULL)")
            # Add an EA row to verify it is preserved untouched
            DBInterface.execute(conn, "INSERT INTO q VALUES ('Q','IRT_M3','EA','2018-Q1',-0.5)")
            DBInterface.execute(conn, "COPY q TO '$(tmpdir)/irt_st_q.parquet' (FORMAT parquet)")

            # Run the gap-fill for geo ZZ over 2018..2018
            CalibrateBeforeIT.aggregate_irt_st_monthly_to_quarterly(
                conn; start_year=2018, end_year=2018, geos=["ZZ"], int_rt="IRT_M3")

            # Read back irt_st_q.parquet for ZZ
            res = CalibrateBeforeIT.execute(conn,
                "SELECT time, value FROM '$(tmpdir)/irt_st_q.parquet' WHERE geo='ZZ' AND int_rt='IRT_M3' ORDER BY time")
            # Q1 reported=5.0 preserved; Q2 gap-filled from monthly mean (4+5+6)/3=5.0
            @test res[1] == (time="2018-Q1", value=5.0)
            @test res[2] == (time="2018-Q2", value=5.0)

            # EA row preserved
            ea = CalibrateBeforeIT.execute(conn,
                "SELECT value FROM '$(tmpdir)/irt_st_q.parquet' WHERE geo='EA' AND int_rt='IRT_M3' AND time='2018-Q1'")
            @test ea[1][1] == -0.5
        finally
            CalibrateBeforeIT.eurostat_path = original_path
        end
    end
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: FAIL — `aggregate_irt_st_monthly_to_quarterly` not defined (or method not found).

- [ ] **Step 3: Write the implementation**

Append to `src/irt_st_aggregation.jl`:
```julia
"""
    aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year, geos, int_rt="IRT_M3")

For each geo in `geos`, gap-fill `irt_st_q.parquet` with monthly-aggregated
`int_rt` values: read `irt_st_m`, left-join to the full month grid
(`start_year..end_year` x 12), in-sample linear-interpolate missing months,
aggregate to quarterly via mean-of-3-months, then merge — reported quarterly
preserved, only missing quarters filled from the monthly aggregate. Rows in
`irt_st_q.parquet` for geos NOT in `geos` (e.g. EA) are left untouched.

The function rewrites `irt_st_q.parquet` in place: it reads all current rows,
drops the rows for `geos` x `int_rt`, and writes the concatenation of (untouched
rows) + (gap-filled rows) back via DuckDB `COPY ... TO`.

If `irt_st_m.parquet` does not exist, the function warns and returns without
modifying `irt_st_q.parquet`.
"""
function aggregate_irt_st_monthly_to_quarterly(conn;
                                                start_year::Int,
                                                end_year::Int,
                                                geos::Vector{String},
                                                int_rt::String="IRT_M3")
    m_file = pqfile("irt_st_m")
    q_file = pqfile("irt_st_q")

    if !isfile(m_file)
        @warn "irt_st_m.parquet not found at $m_file — skipping gap-fill of irt_st_q"
        return nothing
    end
    if !isfile(q_file)
        @warn "irt_st_q.parquet not found at $q_file — cannot gap-fill"
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

    for geo in geos
        # 1. Read monthly for this geo
        sql = "SELECT time, value FROM '$m_file' WHERE geo='$(geo)' AND int_rt='$(int_rt)' AND time IN ($(months_str)) ORDER BY time"
        m_raw = execute(conn, sql)  # Vector of (time, value) NamedTuples
        # Build a Dict time -> value (drop missing)
        m_dict = Dict{String, Float64}()
        for row in m_raw
            v = row.value
            if !ismissing(v) && v !== nothing
                m_dict[row.time] = Float64(v)
            end
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

        # 5. Read reported quarterly for this geo
        sql = "SELECT time, value FROM '$q_file' WHERE geo='$(geo)' AND int_rt='$(int_rt)' AND time IN ($(quarters_str)) ORDER BY time"
        q_raw = execute(conn, sql)
        q_dict = Dict{String, Union{Missing,Float64}}()
        for row in q_raw
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
    end

    # 7. Rewrite irt_st_q.parquet:
    #    a. read all rows NOT in (geos x int_rt) -> keep
    #    b. write kept rows + filled_rows back to a temp parquet, then move
    geo_filter = join(["(geo='$(g)' AND int_rt='$(int_rt)')" for g in geos], " OR ")
    keep_sql = "SELECT freq, int_rt, geo, time, value FROM '$q_file' WHERE NOT ($(geo_filter))"

    # Create a temp table with the filled rows, then COPY the union
    tmp_table = "tmp_irt_st_filled_$(abs(hash(geos, int_rt, start_year, end_year)))"
    DBInterface.execute(conn, "CREATE OR REPLACE TABLE $(tmp_table) (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
    if !isempty(filled_rows)
        values_clause = join(
            ["('Q','$(int_rt)','$(r.geo)','$(r.time)',$(r.value))" for r in filled_rows],
            ",")
        DBInterface.execute(conn, "INSERT INTO $(tmp_table) VALUES $(values_clause)")
    end
    union_sql = "$(keep_sql) UNION ALL SELECT freq, int_rt, geo, time, value FROM $(tmp_table)"

    tmp_out = q_file * ".tmp"
    DBInterface.execute(conn, "COPY ($(union_sql)) TO '$(tmp_out)' (FORMAT parquet)")
    DBInterface.execute(conn, "DROP TABLE $(tmp_table)")

    mv(tmp_out, q_file; force=true)
    @info "aggregate_irt_st_monthly_to_quarterly: gap-filled $(length(geos)) geo(s) for int_rt='$(int_rt)' in $q_file"
    return nothing
end
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: the new "end-to-end on synthetic parquet" test passes. If `eurostat_path` is a `global` that cannot be reassigned from the test (because it's a module-level non-const), see the note in Step 5.

- [ ] **Step 5: Handle the `eurostat_path` override (if needed)**

If the test in Step 1 fails because `CalibrateBeforeIT.eurostat_path` cannot be reassigned (it's declared `global` but not `const` at `src/CalibrateBeforeIT.jl:83`), one of two fixes:

(a) Prefer: change the test to construct the file paths explicitly and call a lower-level variant. Simpler: keep the test as written; `global x = "..."` is reassignable in Julia from `Main` but NOT from inside a module's `@testset`. If reassignment fails, instead pass the `eurostat_path` by temporarily wrapping the call with a `let`-block that uses `Base.eval` to set it. Cleanest: refactor `aggregate_irt_st_monthly_to_quarterly` to accept an optional `eurostat_path=pqfile`-style override. **However**, to keep scope minimal and match the spec, prefer this: in the test, do
```julia
Core.eval(CalibrateBeforeIT, :(eurostat_path = $(tmpdir)))
```
and restore with
```julia
Core.eval(CalibrateBeforeIT, :(eurostat_path = $(original_path)))
```
inside the `try/finally`. This works regardless of `const`-ness because the declaration is `global`, not `const`.

(b) Fallback: if (a) doesn't work, change `src/CalibrateBeforeIT.jl:83` from `global eurostat_path = "data/010_eurostat_tables"` to `const eurostat_path = Ref("data/010_eurostat_tables")` and update `pqfile` to dereference. This is a larger change — only do it if (a) fails.

- [ ] **Step 6: Commit**

```bash
git add src/irt_st_aggregation.jl test/test_irt_st_aggregation.jl
git commit -m "Add aggregate_irt_st_monthly_to_quarterly parquet I/O

Reads irt_st_m for each non-EA geo, left-joins to the full month grid,
in-sample-interpolates missing months, aggregates to quarterly via
mean-of-3-months, and gap-fills irt_st_q.parquet (reported quarterly
preserved, missing quarters filled from the monthly aggregate). EA
rows and other int_rt rows in irt_st_q untouched. Rewrites the parquet
in place via DuckDB COPY of (kept rows UNION ALL filled rows)."
```

---

## Task 5: Wire the preprocessing step into `02_preprocess_raw_eurostat_tables.jl`

**Files:**
- Modify: `02_preprocess_raw_eurostat_tables.jl` (add Step 5)

- [ ] **Step 1: Add Step 5**

In `02_preprocess_raw_eurostat_tables.jl`, after Step 4 (line ~47) and before the final `## After this step...` comment, add:
```julia


##------------------------------------------------------------
## Step 5: Gap-fill irt_st_q with monthly-aggregated IRT_M3 for non-EA countries
##
## Non-EA EU countries (BG, CZ, DK, HU, PL, RO, SE) need their own 3-month short
## rate, not the EA Euribor. Eurostat stopped deriving irt_st_q for 5 of these
## (BG/CZ/HU/PL/RO) after 2015Q1; the monthly source irt_st_m is complete, so we
## aggregate it to quarterly (mean of 3 months) and gap-fill irt_st_q in place.
## Reported quarterly values are preserved; only missing quarters are filled.
start_year_irt = 1996
end_year_irt = 2024
non_ea_geos = ["BG", "CZ", "DK", "HU", "PL", "RO", "SE"]
CBit.aggregate_irt_st_monthly_to_quarterly(conn;
    start_year=start_year_irt, end_year=end_year_irt,
    geos=non_ea_geos, int_rt="IRT_M3")
@info "Step 5 completed: irt_st_q gap-filled for non-EA countries"
```

- [ ] **Step 2: Verify the script still parses**

Run (does not execute, just compiles):
```bash
julia --project=. -e 'include("02_preprocess_raw_eurostat_tables.jl")' 2>&1 | rtk head -20
```
Expected: it will start running (the script is not wrapped in a function). Kill it after the first `@info` lines appear; the goal is only to confirm there are no syntax errors. If it errors immediately with a syntax/UndefVar error, fix before proceeding.

- [ ] **Step 3: Commit**

```bash
git add 02_preprocess_raw_eurostat_tables.jl
git commit -m "Wire irt_st_q gap-fill into preprocessing (Step 5)

For the 7 non-EA EU countries, aggregate irt_st_m IRT_M3 to quarterly
and gap-fill irt_st_q. Reported quarterly preserved; missing quarters
(e.g. CZ/HU/PL/RO/BG post-2015Q1) filled from the monthly aggregate."
```

---

## Task 6: Update `import_data.jl` euribor block to dispatch by EA membership

**Files:**
- Modify: `src/import_data.jl:375-381`
- Modify: `test/test_library_functions.jl` (add a regression test for a non-EA country)

- [ ] **Step 1: Write the failing test**

First check `test/test_library_functions.jl` for the existing pattern, then add a regression test. The test verifies that for a non-EA country with complete data (DK), `import_data` returns a dense, non-missing `data["euribor"]` aligned with `quarters_num`, and that the value differs from EA.

Append to `test/test_library_functions.jl` inside its outer `@testset`:
```julia
    @testset "import_data: non-EA euribor is country-specific and dense" begin
        # DK is non-EA and has complete IRT_M3 in irt_st_q (no gap-fill needed)
        # This test is skipped if the eurostat data dir is not populated.
        eurostat_dir = CalibrateBeforeIT.eurostat_path
        if !isfile(joinpath(eurostat_dir, "irt_st_q.parquet"))
            @info "Skipping non-EA euribor test: irt_st_q.parquet not present"
        else
            data = CalibrateBeforeIT.import_data("DK", 2018, 2018)
            n_quarters = 4  # 2018 Q1..Q4
            @test length(data["euribor"]) == n_quarters
            @test all(!ismissing, data["euribor"])

            # DK should NOT equal the EA rate (DK is non-EA and has its own IBOR)
            ea_data = CalibrateBeforeIT.import_data("EA", 2018, 2018)
            @test data["euribor"] != ea_data["euribor"]

            # Sanity: the DK vector must be aligned with quarters_num (same length)
            @test length(data["euribor"]) == length(data["quarters_num"])
        end
    end
```

- [ ] **Step 2: Run the test to verify it fails (or is skipped)**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Two cases:
- If `irt_st_q.parquet` is not present locally, the test is skipped — proceed to Step 3 anyway (the test is a guard for real data; it will run in CI/local with data).
- If present, the test FAILS because `import_data("DK", ...)` currently returns the EA euribor (hardcoded), so `data["euribor"] == ea_data["euribor"]`.

- [ ] **Step 3: Edit the euribor block**

In `src/import_data.jl`, replace lines 375-381:
```julia
    ## 3-month euribor
    sqlquery="SELECT value FROM '$(pqfile("irt_st_q"))' WHERE time IN ($(quarters_str)) AND geo='EA' AND int_rt='IRT_M3' ORDER BY time"
    data["euribor"]=0.01*execute(conn,sqlquery);

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("irt_st_a"))' WHERE time IN ($(years_str)) AND geo='EA' AND int_rt='IRT_M3' ORDER BY time"
    data["euribor_yearly"]=0.01*execute(conn,sqlquery);
```
with:
```julia
    ## 3-month short rate: euribor (geo='EA') for EA members, national IBOR
    ## (geo='$(geo)') for non-EA members. For non-EA, irt_st_q must have been
    ## gap-filled by aggregate_irt_st_monthly_to_quarterly (02_preprocess, Step 5).
    ea_member = is_euro_area_member(geo, Date(end_year, 12, 31))
    euribor_geo = ea_member ? "EA" : geo
    sqlquery="SELECT value FROM '$(pqfile("irt_st_q"))' WHERE time IN ($(quarters_str)) AND geo='$(euribor_geo)' AND int_rt='IRT_M3' ORDER BY time"
    data["euribor"]=0.01*execute(conn,sqlquery);

    # Annual (kept for symmetry; currently unused downstream)
    sqlquery="SELECT value FROM '$(pqfile("irt_st_a"))' WHERE time IN ($(years_str)) AND geo='$(euribor_geo)' AND int_rt='IRT_M3' ORDER BY time"
    data["euribor_yearly"]=0.01*execute(conn,sqlquery);
```

- [ ] **Step 4: Verify the edit is syntactically valid**

Run:
```bash
julia --project=. -e 'import CalibrateBeforeIT as CBit; println(CBit.is_euro_area_member("DK", Date(2024,12,31)))'
```
Expected: prints `false`.

- [ ] **Step 5: Run the tests**

Run:
```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: all tests pass (or the new test is skipped if data is absent). The existing `AT_2010Q1` fixture tests must still pass — AT is EA and its path is unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/import_data.jl test/test_library_functions.jl
git commit -m "Dispatch euribor query by EA membership in import_data

EA members keep geo='EA' (Euribor). Non-EA members use geo='$(geo)'
(national IBOR), which is now dense after 02_preprocess Step 5 gap-fills
irt_st_q from irt_st_m. Fixes: all EU countries previously calibrated
against the EA rate; non-EA countries now use their own short rate."
```

---

## Task 7: One-time consistency check script

**Files:**
- Create: `scripts/check_irt_st_aggregation.jl`

This script is **not** part of the pipeline. It is run once manually; its outputs (`docs/irt_st_consistency/overlap_comparison.csv` and `summary.md`) are committed as evidence for the working paper.

- [ ] **Step 1: Write the script**

Create `scripts/check_irt_st_aggregation.jl`:
```julia
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
    m_raw = CBit.execute(conn, sql)
    m_dict = Dict{String, Float64}()
    for r in m_raw
        if !ismissing(r.value) && r.value !== nothing
            m_dict[r.time] = Float64(r.value)
        end
    end
    monthly_vec = Vector{Union{Missing,Float64}}(undef, length(all_months))
    for (i, m) in enumerate(all_months)
        monthly_vec[i] = haskey(m_dict, m) ? m_dict[m] : missing
    end
    monthly_interp = CBit._in_sample_interp(monthly_vec)
    quarterly_agg = CBit._monthly_to_quarterly_mean(monthly_interp)

    # Reported quarterly
    sql = "SELECT time, value FROM '$(CBit.pqfile("irt_st_q"))' WHERE geo='$(geo)' AND int_rt='IRT_M3' AND time IN ($(quarters_str)) ORDER BY time"
    q_raw = CBit.execute(conn, sql)
    q_dict = Dict{String, Union{Missing,Float64}}()
    for r in q_raw
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

using Statistics
using CSV
```

Note: the `using Statistics` and `using CSV` at the bottom should be moved to the top with the other `using` statements before running. The script as written lists them at the bottom for clarity of the diff; the implementer should hoist them to the top of the file.

- [ ] **Step 2: Run the script once (requires `irt_st_m.parquet` downloaded)**

If `irt_st_m.parquet` is not yet downloaded, first run `01b_download_eurostat_tables.jl` (or just download `irt_st_m`). Then:
```bash
julia --project=. scripts/check_irt_st_aggregation.jl
```
Expected: writes `docs/irt_st_consistency/overlap_comparison.csv` and `summary.md`.

- [ ] **Step 3: Inspect the summary**

Read `docs/irt_st_consistency/summary.md`. Confirm:
- For DK and SE: the overlap is the full window (1996Q1–2023Q4), mean |diff| should be near 0 (DK/SE quarterly is derived from the same monthly source).
- For CZ/HU/PL/RO: overlap is 1996Q1–2015Q1, mean |diff| should be < 1e-3.
- For BG: overlap is 1998Q1–2015Q1 (BG monthly starts 1998Q1).

If the discrepancies are larger than 1e-3 for any country, stop and report — the aggregation method may need a country-specific adjustment before proceeding.

- [ ] **Step 4: Commit the script and its outputs**

```bash
git add scripts/check_irt_st_aggregation.jl docs/irt_st_consistency/
git commit -m "Add one-time irt_st aggregation consistency check + outputs

Compares reported quarterly IRT_M3 vs monthly-aggregated (mean of 3
months) for non-EA countries on the overlap window. Outputs
(overlap_comparison.csv, summary.md) are run-once evidence for the
working paper: they justify using irt_st_m to fill the post-2015Q1 gap
in irt_st_q for BG/CZ/HU/PL/RO. The script is not part of the pipeline."
```

---

## Task 8: Final verification

- [ ] **Step 1: Run the full test suite**

```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```
Expected: all testsets pass. New testsets: "Euro Area Membership Tests", "IRT_ST Aggregation Tests", and the new "import_data: non-EA euribor is country-specific and dense" (or skipped if no data locally).

- [ ] **Step 2: Confirm no `BeforeIT.jl` changes**

```bash
cd ~/.julia/dev/BeforeIT && rtk git status
```
Expected: clean (no modifications). If anything is modified, revert — this feature must not touch BeforeIT.

- [ ] **Step 3: Review the full diff**

```bash
cd /home/reitero/Projects/Julia/CalibrateBeforeIT.jl && rtk git diff master..HEAD --stat
```
Expected files changed:
- `src/CalibrateBeforeIT.jl`
- `src/euro_area_membership.jl` (new)
- `src/irt_st_aggregation.jl` (new)
- `src/import_data.jl`
- `02_preprocess_raw_eurostat_tables.jl`
- `scripts/check_irt_st_aggregation.jl` (new)
- `docs/irt_st_consistency/` (new)
- `test/test_euro_area_membership.jl` (new)
- `test/test_irt_st_aggregation.jl` (new)
- `test/test_library_functions.jl`
- `test/runtests.jl`
- `docs/superpowers/specs/2026-08-12-country-specific-interest-rates-design.md` (new, already committed)

- [ ] **Step 4: Final commit (if any leftover)**

If all tasks above are already committed individually, no further commit is needed. Otherwise:
```bash
git add -A
git commit -m "Complete country-specific interest rates feature"
```

---

## Self-Review notes (post-write)

**Spec coverage:**
- D1 (in-sample interp): Task 3 `_in_sample_interp` clamps edges — covered.
- D2 (time-varying helper): Task 2 `is_euro_area_member` + join-date Dict — covered.
- D3 (preprocess into irt_st_q): Tasks 4 + 5 — covered.
- D4 (gap-fill, not overwrite): Task 3 `_gap_fill_quarterly`, Task 4 end-to-end — covered.
- D5 (one-time consistency check): Task 7 — covered.
- D6 (Date(end_year,12,31) reference): Task 6 — covered.
- D7 (Approach 1): Task 4 rewrites in place via DuckDB COPY UNION ALL — covered.
- Caveats: `linear_interp_extrap` edge behavior — Task 3 uses a custom `_in_sample_interp` (clamp) instead of the extrapolating `linear_interp_extrap`, so the caveat is resolved, not relied upon.
- Caveat: DuckDB read→concat→rewrite — Task 4 follows the `combine_tables` pattern (`COPY (union) TO`); verified against `src/import_eurostat.jl:448`.

**Type consistency:**
- `_monthly_to_quarterly_mean(monthly::AbstractVector)` — used in Task 4 and Task 7 with `Vector{Union{Missing,Float64}}` (interp output) and `Vector{Float64}` (Task 7). Both are `AbstractVector` — OK.
- `_in_sample_interp` returns `Vector{Union{Missing,Float64}}` — consumed by `_monthly_to_quarterly_mean` which does arithmetic; `missing` would propagate. **Invariant**: `_in_sample_interp` returns no interior `missing` (only all-missing input returns all-missing). In the all-missing case, `_monthly_to_quarterly_mean` would error on `missing + x`. **Guard**: in Task 4, if a geo has zero monthly data, `_in_sample_interp` returns all-missing and `_monthly_to_quarterly_mean` errors. Add a guard in Task 4 before calling: if `all(ismissing, monthly_vec)`, `@warn` and skip that geo (leave its quarterly untouched). **Add this to Task 4 Step 3** — see the inline note below.

**Task 4 Step 3 amendment (add guard):**
Before the `# 2. Left-join to full month grid` block in `aggregate_irt_st_monthly_to_quarterly`, add:
```julia
        if isempty(m_dict)
            @warn "No monthly IRT_M3 data for geo='$(geo)' in irt_st_m — leaving irt_st_q unchanged for this geo"
            continue
        end
```
This handles the all-missing case and the spec's "zero monthly data" error-handling requirement.