# Design Spec: Country-specific short-term interest rates for non-EA countries

## Problem

All 27 EU countries currently start the calibration with the same central-bank
interest rate. Root cause: `src/import_data.jl:376,380` hardcodes `geo='EA'` in
the euribor query for every country, so every country receives the EA 3-month
Euribor series. Non-EA countries (BG, CZ, DK, HU, PL, RO, SE) should use their
own short-term rate.

Complication: 5 of 7 non-EA countries (BG, CZ, HU, PL, RO) have **no** quarterly
data after 2015Q1 in `irt_st_q`/`irt_st_a` — Eurostat stopped deriving the
quarterly/annual national series after 2015Q1 while the monthly source
(`irt_st_m`) continued. Only DK and SE are complete in `irt_st_q`.

## Goal

Feed each non-EA country its own 3-month short rate (national IBOR) into
`data["euribor"]`, so `r_bar`, the Taylor-rule input `a1`, and `r_bar_series`
(all in BeforeIT.jl) are country-specific. EA members keep `geo='EA'`.

## Established facts (evidence)

1. **Aggregation method**: Eurostat derives quarterly `IRT_M3` from monthly as
   the arithmetic mean of the 3 calendar months in the quarter. Cross-checked
   against EA (complete in both `irt_st_m` and `irt_st_q`): mean |diff| ~0.0002
   over 2018-2021; end-of-quarter-month rejected (mean |diff| ~0.096).
   [Subagent cross-check, 144 quarters 1999-2022.]
2. **`irt_st_m` is complete** for HU (1996-01 → 2026-06, scattered `m` flags
   only), CZ, PL, RO confirmed via the Eurostat API post-2015; BG via the
   Statistics Explained article. No separate "reformed rate" table exists —
   reformed national IBORs (PRIBOR/WIBOR/BUBOR/ROBOR/...) continue under the
   same `IRT_M3` code in `irt_st_m`.
3. **`data["euribor"]` consumers** (BeforeIT.jl, read-only):
   `calibration.jl:245` (r_bar), `:489` (a1), `:637` (r_bar_series);
   `_calibration_steady_state.jl:65,243`. All index
   `data["euribor"][T_calibration_exo]` where `T_calibration_exo` is a positional
   index into the full `quarters_num` grid → requires `data["euribor"]` to be
   dense and aligned 1:1 with `data["quarters_num"]`.
4. **`data["euribor_yearly"]` is loaded but never consumed** anywhere in
   BeforeIT.jl or CalibrateBeforeIT.jl. (Dead code; keep for symmetry, not
   removing.)
5. **Current `WHERE time IN (...)` query has no left-join**: a non-EA country
   with gaps returns a *shorter* vector → misindexes silently. The fix must
   guarantee a dense, aligned vector.
6. **EA membership** (current window, `max_calibration_date = 2023-12-31`):
   20 EA members incl. HR (joined 2023-01-01); 7 non-EA incl. BG (joins
   2026-01-01, outside window). No EA-membership list exists in the code today.

## Decisions (made during brainstorming)

- **D1 — Missing-quarter handling**: in-sample linear interpolation of missing
  months before aggregation. EA countries unaffected (no gaps).
- **D2 — EA membership**: time-varying helper with a `Dict{String, Date}` of
  join dates, queried as-of `Date(end_year, 12, 31)`.
- **D3 — Aggregation location**: preprocessing into `irt_st_q.parquet` (single
  canonical quarterly file); `import_data.jl` only ever touches `irt_st_q`.
- **D4 — Fill mode**: gap-fill, not overwrite. Reported quarterly values are
  preserved; only missing quarters receive the monthly-aggregated value.
- **D5 — Consistency check**: separate one-time script (not in the pipeline).
  Run once, commit outputs as evidence for the working paper.
- **D6 — `import_data` signature**: unchanged. Membership reference date =
  `Date(end_year, 12, 31)` computed inside `import_data`.
- **D7 — Approach 1**: gap-fill existing `irt_st_q.parquet` in place during
  preprocessing.

## Architecture

### New files

1. `src/euro_area_membership.jl` — `EURO_AREA_JOIN_DATES` Dict and
   `is_euro_area_member(geo, date)`.
2. `src/irt_st_aggregation.jl` —
   `aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year, geos, int_rt)`.
3. `scripts/check_irt_st_aggregation.jl` — one-time consistency check; outputs
   to `docs/irt_st_consistency/`.
4. `test/test_euro_area_membership.jl`, `test/test_irt_st_aggregation.jl` — new
   tests.

### Modified files

1. `src/CalibrateBeforeIT.jl` — add `"irt_st_m"` to `ALL_EUROSTAT_TABLE_IDS`;
   `include` the two new modules; export `is_euro_area_member`,
   `EURO_AREA_JOIN_DATES`, `aggregate_irt_st_monthly_to_quarterly`.
2. `02_preprocess_raw_eurostat_tables.jl` — new Step 5 calling
   `aggregate_irt_st_monthly_to_quarterly` for the 7 non-EA geos.
3. `src/import_data.jl` — euribor block (`:375-381`) dispatches by
   `is_euro_area_member(geo, Date(end_year, 12, 31))`; uses `geo='EA'` for EA,
   `geo='$(geo)'` for non-EA (now gap-filled).

### Unchanged

- `BeforeIT.jl` — no changes. `data["euribor"]` consumed as a dense aligned
  vector, which the new production guarantees.
- `03_create_calibration_data.jl` — loop structure unchanged;
  `import_data("EA19", ...)` at `:50` stays as-is (separate concern from
  euribor; the EA19-vs-EA20 question is flagged but out of scope here).
- All existing tests.

## Data flow

```
01b_download_eurostat_tables.jl
  └─ downloads irt_st_m.parquet (NEW) + irt_st_q, irt_st_a

02_preprocess_raw_eurostat_tables.jl   (NEW Step 5)
  └─ aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year,
            geos=["BG","CZ","DK","HU","PL","RO","SE"], int_rt="IRT_M3")
       per geo:
         ├─ read irt_st_m monthly (geo, IRT_M3)
         ├─ left-join to full month grid (start_year..end_year × 12)
         ├─ in-sample linear-interpolate missing months
         ├─ group by quarter → mean of 3 months → monthly-aggregated quarterly
         ├─ read irt_st_q reported quarterly (geo, IRT_M3)
         ├─ gap-fill: reported where present, else monthly-aggregated
         └─ write gap-filled non-EA rows back into irt_st_q.parquet
            (read all rows → concat gap-filled non-EA → rewrite; EA rows unchanged)

03_create_calibration_data.jl   (unchanged loop)
  └─ import_data(geo, start_year, end_year)
       euribor_geo = is_euro_area_member(geo, Date(end_year,12,31)) ? "EA" : geo
       SELECT ... FROM irt_st_q WHERE geo='$(euribor_geo)' AND int_rt='IRT_M3' ...
       → data["euribor"] (dense, aligned with quarters_num)
```

**Invariant after preprocessing**: `irt_st_q.parquet` has a dense `IRT_M3`
series for every non-EA geo across the full grid → `WHERE time IN (...)`
returns a vector aligned 1:1 with `data["quarters_num"]`.

## Components

### `src/euro_area_membership.jl`

```julia
using Dates

const EURO_AREA_JOIN_DATES = Dict{String, Date}(
    "AT" => Date(1999,1,1), "BE" => Date(1999,1,1), "DE" => Date(1999,1,1),
    "ES" => Date(1999,1,1), "FI" => Date(1999,1,1), "FR" => Date(1999,1,1),
    "IE" => Date(1999,1,1), "IT" => Date(1999,1,1), "LU" => Date(1999,1,1),
    "NL" => Date(1999,1,1), "PT" => Date(1999,1,1),
    "EL" => Date(2001,1,1),
    "SI" => Date(2007,1,1),
    "CY" => Date(2008,1,1), "MT" => Date(2008,1,1),
    "SK" => Date(2009,1,1),
    "EE" => Date(2011,1,1),
    "LV" => Date(2014,1,1),
    "LT" => Date(2015,1,1),
    "HR" => Date(2023,1,1),
    "BG" => Date(2026,1,1),
)

is_euro_area_member(geo::String, date::Date) =
    haskey(EURO_AREA_JOIN_DATES, geo) && EURO_AREA_JOIN_DATES[geo] <= date
```

- For `Date(2024,12,31)` (current `end_year=2024`): true for 20 (incl. HR),
  false for 7 (incl. BG). Correct.
- Future-proof: BG flips to EA automatically when `end_year >= 2026`.

### `src/irt_st_aggregation.jl`

```julia
"""
    aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year, geos, int_rt="IRT_M3")

For each geo in `geos`, gap-fill irt_st_q.parquet with monthly-aggregated
IRT_M3: read irt_st_m, left-join to the full month grid, in-sample
linear-interpolate missing months, aggregate to quarterly via mean-of-3-months,
then merge — reported quarterly preserved, only missing quarters filled
from the monthly aggregate. EA rows in irt_st_q untouched.
"""
function aggregate_irt_st_monthly_to_quarterly(conn; start_year, end_year,
                                               geos, int_rt="IRT_M3")
    # 1. build full month grid strings "YYYY-MM"
    # 2. for each geo:
    #    a. query irt_st_m: SELECT value WHERE geo=geo AND int_rt=int_rt ORDER BY time
    #    b. left-join to month grid → Vector{Union{Missing,Float64}}
    #    c. linear_interp_extrap on present values (in-sample; no extrapolation
    #       beyond first/last present month — clamp to nearest if needed at edges)
    #    d. group months by quarter, mean of 3 → quarterly Vector{Float64}
    #    e. query irt_st_q reported: SELECT value WHERE geo=geo AND int_rt=int_rt
    #       AND time IN (quarters) ORDER BY time
    #    f. gap-fill: for each quarter, reported if present else monthly-aggregated
    # 3. read all existing rows of irt_st_q.parquet
    # 4. drop non-EA geos' IRT_M3 rows; concat gap-filled non-EA rows
    # 5. rewrite irt_st_q.parquet (COPY TO, same pattern as combine_tables)
end
```

- Reuses `pqfile`, `execute` (`utils.jl`), `linear_interp_extrap` (`utils.jl:63`).
- No new dependencies.

### `src/import_data.jl` change (euribor block, `:375-381`)

```julia
## 3-month short rate (euribor for EA; national IBOR for non-EA)
ea_member = is_euro_area_member(geo, Date(end_year, 12, 31))
euribor_geo = ea_member ? "EA" : geo
sqlquery="SELECT value FROM '$(pqfile("irt_st_q"))' WHERE time IN ($(quarters_str)) AND geo='$(euribor_geo)' AND int_rt='IRT_M3' ORDER BY time"
data["euribor"]=0.01*execute(conn,sqlquery);

sqlquery="SELECT value FROM '$(pqfile("irt_st_a"))' WHERE time IN ($(years_str)) AND geo='$(euribor_geo)' AND int_rt='IRT_M3' ORDER BY time"
data["euribor_yearly"]=0.01*execute(conn,sqlquery);
```

- `data["euribor_yearly"]` kept for symmetry (still unused downstream).

### `02_preprocess_raw_eurostat_tables.jl` — new Step 5

```julia
##------------------------------------------------------------
## Step 5: Gap-fill irt_st_q with monthly-aggregated IRT_M3 for non-EA countries
non_ea_geos = ["BG", "CZ", "DK", "HU", "PL", "RO", "SE"]
# (or derive: [g for g in ALL_EU if !is_euro_area_member(g, Date(end_year,12,31))])
CBit.aggregate_irt_st_monthly_to_quarterly(conn;
    start_year=1996, end_year=2024, geos=non_ea_geos, int_rt="IRT_M3")
@info "Step 5 completed: irt_st_q gap-filled for non-EA countries"
```

### `scripts/check_irt_st_aggregation.jl` (one-time, not in pipeline)

- For each non-EA geo, for each quarter where **both** reported quarterly and
  monthly-aggregated exist (overlap window):
  - `abs_diff = |reported_q - aggregated_q|`,
    `rel_diff = abs_diff / |reported_q|`.
- Outputs:
  - `docs/irt_st_consistency/overlap_comparison.csv` (per-observation)
  - `docs/irt_st_consistency/summary.md` (per-geo: n_overlap, mean/median/max
    abs_diff, % within 1e-4, outliers)
- Run once; commit outputs as evidence; cite in the working paper alongside
  the EA cross-check (already done) proving the aggregation method.

## Error handling

- If `irt_st_m.parquet` is missing → `@warn` and skip gap-fill (EA countries
  still work; non-EA fall back to the current short vector — i.e. the pre-fix
  behavior for those geos). Do not crash the pipeline.
- If a non-EA country has **zero** monthly data for the whole window → `@warn`
  and leave its quarterly as-is (will be short; will fail downstream at
  `T_calibration_exo` with a clear out-of-bounds, same as today).
- Interpolation edges: if the first/last months are missing, clamp to the
  nearest present value (no extrapolation beyond the observed range — matches
  `linear_interp_extrap`'s existing behavior; verify in implementation).

## Testing

- `test/test_euro_area_membership.jl` (new): `is_euro_area_member("HR",
  Date(2022,12,31)) == false`; `("HR", Date(2023,1,1)) == true`;
  `("BG", Date(2025,12,31)) == false`; `("AT", Date(1998,12,31)) == false`;
  `("AT", Date(1999,1,1)) == true`; unknown geo → false.
- `test/test_irt_st_aggregation.jl` (new): synthetic monthly input → expected
  quarterly mean (e.g. months [1,2,3] → 2.0); missing-month interpolation
  (months [1, missing, 3] → interp 2 → mean 2.0); gap-fill preserves reported
  quarterly when present (reported=5.0 at Q1, monthly-agg=2.0 → result 5.0).
- `test/test_library_functions.jl` (extend): regression — for a non-EA country
  with complete data (DK), `import_data("DK", 1996, 2024)` returns
  `data["euribor"]` of length `number_quarters`, all non-missing, and
  `data["euribor"][T]` matches `irt_st_q` DK IRT_M3 (after gap-fill = reported,
  since DK is complete).
- Reference fixture `AT_2010Q1` is EA and unaffected — no change.
- Existing tests: no changes; all should still pass.

## Branch & commits

- Branch `feat/country-specific-rates` off `master`.
- Logical commit order (may squash):
  1. Add `"irt_st_m"` to `ALL_EUROSTAT_TABLE_IDS`.
  2. Add `src/euro_area_membership.jl` + tests.
  3. Add `src/irt_st_aggregation.jl` + tests.
  4. Add Step 5 to `02_preprocess_raw_eurostat_tables.jl`.
  5. Update `import_data.jl` euribor block to dispatch by EA membership.
  6. Add `scripts/check_irt_st_aggregation.jl` + run-once outputs committed as
     evidence.
- No `BeforeIT.jl` changes.

## Out of scope (flagged, not done)

- **EA19 → EA20 for 2023**: the `ea_data = import_data("EA19", ...)` at
  `03_create_calibration_data.jl:50` is technically stale for calibration dates
  >= 2023-01-01 (HR should be in the EA aggregate). Separate correctness issue
  from the euribor fix; not touched here.
- **`data["euribor_yearly"]` removal**: dead code; kept for symmetry to
  minimize scope. Could be removed in a follow-up.
- **Extending `get_valid_calibration_quarters` to check euribor**: not needed
  since the new production guarantees density, but could be added as
  defense-in-depth later.

## Caveats / unverified (to confirm during implementation)

- `linear_interp_extrap`'s exact edge behavior (clamp vs extrapolate) — verify
  in `src/utils.jl:63` before relying on it for the first/last months.
- The DuckDB read→concat→rewrite pattern for `irt_st_q.parquet` must preserve
  column order/types; verify against `combine_tables` in `src/import_eurostat.jl`.
- The `end_year` used in Step 5 (`02_*`) should match the `end_year` used in
  `03_create_calibration_data.jl` (currently 2024); confirm they're consistent
  or pass explicitly.