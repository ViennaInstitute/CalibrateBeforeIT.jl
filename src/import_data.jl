## National accounts aggregates (na_item => name used in the data keys) for
## which nominal and real levels, deflators and growth rates are imported
const NATIONAL_ACCOUNTS_AGGREGATES = [
    "B1GQ" => "gdp",
    "B1G" => "gva",
    "P31_S14_S15" => "household_consumption",
    "P3_S13" => "government_consumption",
    "P3" => "final_consumption",
    "P5G" => "capitalformation",
    "P51G" => "fixed_capitalformation",
    "P6" => "exports",
    "P7" => "imports",
]

function import_data(geo, start_year, end_year)
    conn = DBInterface.connect(DuckDB.DB)
    number_years = end_year - start_year + 1;
    number_quarters = number_years * 4;
    years_str, quarters_str = sql_years_quarters(start_year:end_year)

    data = Dict()

    ## Set these date numbers
    data["years_num"] = date2num_yearly(start_year:end_year)
    data["quarters_num"] = date2num_quarterly(start_year:end_year)

    ## Annual and quarterly (seasonally and calendar adjusted) frequency
    frequencies = (("nama_10_gdp", "nama_10_a10", years_str, "", ""),
                   ("namq_10_gdp", "namq_10_a10", quarters_str, "_quarterly", " AND s_adj='SCA'"))


    ## GDP, GVA, consumption, capital formation, exports and imports time
    ## series, e.g. "nominal_gdp", "real_gdp", "gdp_deflator", "real_gdp_growth",
    ## "gdp_deflator_growth" and "nominal_gdp_growth" (+ "_quarterly")
    for (na_item, name) in NATIONAL_ACCOUNTS_AGGREGATES,
        (table, _, times_str, suffix, s_adj) in frequencies

        series(unit) = query_values(conn, table, geo, times_str,
            "na_item='$(na_item)' AND unit='$(unit)'$(s_adj)")
        data["nominal_$(name)$(suffix)"] = series("CP_MEUR")
        data["real_$(name)$(suffix)"] = series("CLV10_MEUR")
        data["$(name)_deflator$(suffix)"] = 0.01*series("PD10_EUR")
        data["real_$(name)_growth$(suffix)"] = 0.01*series("CLV_PCH_PRE")
        data["$(name)_deflator_growth$(suffix)"] = 0.01*series("PD_PCH_PRE_EUR")
        data["nominal_$(name)_growth$(suffix)"] =
            data["real_$(name)_growth$(suffix)"] + data["$(name)_deflator_growth$(suffix)"]
    end


    ## 3-month short rate: euribor (geo='EA') for EA members and EA aggregates
    ## (EA19/EA20/EA21), national IBOR (geo='$(geo)') for non-EA members. For
    ## non-EA, irt_st_q must have been gap-filled by
    ## aggregate_irt_st_monthly_to_quarterly (02_preprocess, Step 5).
    ea_member = is_euro_area_member(geo, Date(end_year, 12, 31))
    euribor_geo = (ea_member || geo in ("EA", "EA19", "EA20", "EA21")) ? "EA" : geo
    data["euribor"]=0.01*query_values(conn, "irt_st_q", euribor_geo, quarters_str, "int_rt='IRT_M3'")

    # Annual (kept for symmetry; currently unused downstream)
    data["euribor_yearly"]=0.01*query_values(conn, "irt_st_a", euribor_geo, years_str, "int_rt='IRT_M3'")


    ## Compensation of employees, wages and operating surplus

    s_adj_namq_10_gdp = "SCA"
    ## Some countries do not report time series in the "seasonally and calendar
    ## adjusted" (= SCA) way that we would need. For these countries, we fall
    ## back to the "seasonally adjusted" (= SA) series.
    if geo in ["DE", "FR"]
        s_adj_namq_10_gdp = "SA"
    end
    for (na_item, name) in ["D1" => "compensation_employees", "D11" => "wages",
                            "B2A3G" => "operating_surplus"]
        data[name] = query_values(conn, "nama_10_gdp", geo, years_str,
            "na_item='$(na_item)' AND unit='CP_MEUR'")
        data["$(name)_quarterly"] = query_values(conn, "namq_10_gdp", geo, quarters_str,
            "na_item='$(na_item)' AND unit='CP_MEUR' AND s_adj='$(s_adj_namq_10_gdp)'")
    end


    ## Employed

    # Annual
    data["employed"] = query_values(conn, "nama_10_pe", geo, years_str,
        "na_item='EMP_DC' AND unit='THS_PER'")

    if geo != "EA19"
        ## Unemployment rate
        data["unemployment_rate_quarterly"]=0.01*query_values(conn, "une_rt_q", geo, quarters_str,
            "unit='PC_ACT' AND age='Y15-74' AND s_adj='SA' AND sex='T'")

        # Annual
        data["unemployment_rate"]=0.01*query_values(conn, "une_rt_a", geo, years_str,
            "unit='PC_ACT' AND age='Y15-74' AND sex='T'")

        # Pad the unemployment rate series to the number of years/quarters
        data["unemployment_rate"]=[repeat([missing], (number_years - length(data["unemployment_rate"])))...,
                                   data["unemployment_rate"]...];
        data["unemployment_rate_quarterly"]=[repeat([missing], (number_quarters - length(data["unemployment_rate_quarterly"])))...,
                                             data["unemployment_rate_quarterly"]...];
    end


    ## Sectoral GVA (one column per NACE 10 industry)

    for (_, table, times_str, suffix, s_adj) in frequencies
        series(unit) = reshape(query_values(conn, table, geo, times_str,
            "nace_r2 NOT IN ('C', 'TOTAL') AND na_item='B1G' AND unit='$(unit)'$(s_adj)";
            order = "nace_r2, time"), :, 10)
        data["nominal_nace10_gva$(suffix)"] = series("CP_MEUR")
        data["real_nace10_gva$(suffix)"] = series("CLV10_MEUR")
        data["nace10_gva_deflator$(suffix)"] = 0.01*series("PD10_EUR")
        data["real_nace10_gva_growth$(suffix)"] = 0.01*series("CLV_PCH_PRE")
        data["nace10_gva_deflator_growth$(suffix)"] = 0.01*series("PD_PCH_PRE_EUR")
    end

    ## NOTE: the annual series adds the aggregate GVA deflator growth, the
    ## quarterly one the sectoral deflator growth
    data["nominal_nace10_gva_growth"]=data["real_nace10_gva_growth"] .+ data["gva_deflator_growth"];
    data["nominal_nace10_gva_growth_quarterly"]=data["real_nace10_gva_growth_quarterly"]+data["nace10_gva_deflator_growth_quarterly"];

    ## The MATLAB original padded series that were one period too short with a
    ## leading NaN. Not replicated here; fail loudly instead:
    for (key, value) in data
        data_length = length(value)
        if (data_length == length(data["years_num"]) - 1) || (data_length == length(data["quarters_num"]) - 1)
            println(key, ": ", length(value))
            error("Series $(key) has length $(length(value)) which is one too short!")
        end
    end

    return data
end
