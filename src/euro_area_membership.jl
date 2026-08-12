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
