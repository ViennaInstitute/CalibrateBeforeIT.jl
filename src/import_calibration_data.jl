## Countries whose sectoral capital consumption is distributed according to
## the EU20 structure (no usable industry-level data)
const CAPITAL_CONSUMPTION_FROM_EU20 = ["FR", "IE", "LT", "LU", "MT", "PL", "SE"]
const EU20 = ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "DE", "EL", "HU", "IT", "LV", "NL", "PT", "RO", "SK", "SI", "ES"]

## Countries with industry-level data on fixed assets and dwellings; all
## others are distributed according to the EU7 structure
const SECTORAL_FIXED_ASSETS = ["AT", "BG", "CZ", "EL", "FI", "LV", "SK"] # OR [2025-05-06 Di]: removed DK
const EU7 = ["AT", "CZ", "DK", "EL", "FI", "LV", "SK"]

function import_calibration_data(geo, start_calibration_year, end_calibration_year,
                                 number_sectors, figaro)
    conn = DBInterface.connect(DuckDB.DB)

    number_years=end_calibration_year-start_calibration_year + 1;
    number_quarters=number_years*4;
    years_str, quarters_str = sql_years_quarters(start_calibration_year:end_calibration_year)

    calibration_data = Dict()

    ## Sector x year matrices from NACE64 tables, for `geo` or summed over `geos`
    nace64_filter = "nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND nace_r2 NOT IN ($(sql_list(EXCLUDED_NACE64)))"
    sectoral(table, filters) = execute(conn,
        "SELECT value FROM '$(pqfile(table))' WHERE $(nace64_filter) AND time IN ($(years_str)) AND geo='$(geo)' AND $(filters) ORDER BY time, nace_r2",
        (number_sectors, number_years))
    sectoral_sum(table, filters, geos) = execute(conn,
        "SELECT sum(value) FROM '$(pqfile(table))' WHERE $(nace64_filter) AND time IN ($(years_str)) AND geo IN ($(sql_list(geos))) AND $(filters) GROUP BY time, nace_r2 ORDER BY time, nace_r2",
        (number_sectors, number_years))

    ## Quarterly financial balance sheets, annual non-financial transactions
    ## and annual government finance statistics
    f_bs(sector, na_item, finpos) = query_values(conn, "nasq_10_f_bs", geo, quarters_str,
        "unit='MIO_EUR' AND sector='$(sector)' AND na_item='$(na_item)' AND finpos='$(finpos)'")
    nf_tr(sector, na_item, direct) = query_values(conn, "nasa_10_nf_tr", geo, years_str,
        "unit='CP_MEUR' AND sector='$(sector)' AND na_item='$(na_item)' AND direct='$(direct)'")
    gov_main(na_item) = query_values(conn, "gov_10a_main", geo, years_str,
        "unit='MIO_EUR' AND sector='S13' AND na_item='$(na_item)'")

    ## Quarterly series that are not available for all countries. If missing,
    ## the variable is not created (or left empty) and get_params_and_initial_conditions
    ## falls back to the annual series.
    function quarterly_if_available!(key, table, filters, description, fallback)
        try
            calibration_data[key]=query_values(conn, table, geo, quarters_str, filters);
            if length(calibration_data[key]) == 0
                throw(ErrorException("Empty result"))
            end
        catch e
            @warn "  --> $(geo): Quarterly $(description) data not available ($(typeof(e))), $(fallback)"
        end
    end

    ## Set these date numbers
    calibration_data["years_num"] = date2num_yearly(start_calibration_year:end_calibration_year)
    calibration_data["quarters_num"] = date2num_quarterly(start_calibration_year:end_calibration_year)

    ## time series
    if geo in CAPITAL_CONSUMPTION_FROM_EU20
        calibration_data["capital_consumption"]=query_values(conn, "nama_10_a64", geo, years_str,
            "nace_r2='TOTAL' AND unit = 'CP_MEUR' AND na_item='P51C'");
        calibration_data["nace64_capital_consumption_eu20"]=sectoral_sum("nama_10_a64", "unit = 'CP_MEUR' AND na_item='P51C'", EU20);
        calibration_data["nominal_nace64_output_eu20"]=sectoral_sum("nama_10_a64", "unit = 'CP_MEUR' AND na_item='P1'", EU20);
    else
        calibration_data["nace64_capital_consumption"]=sectoral("nama_10_a64", "unit = 'CP_MEUR' AND na_item='P51C'");
        calibration_data["nominal_nace64_output"]=sectoral("nama_10_a64", "unit = 'CP_MEUR' AND na_item='P1'");
    end

    calibration_data["wages"]=query_values(conn, "nama_10_a64", geo, years_str,
        "nace_r2='TOTAL' AND unit = 'CP_MEUR' AND na_item='D11'");

    # Sectoral wages (D11) for employers_social_contributions calculation
    calibration_data["wages_by_sector"]=sectoral("nama_10_a64", "unit = 'CP_MEUR' AND na_item='D11'");

    # Impute missing sectoral wages using economy-wide wages/compensation ratio
    wages_by_sector = calibration_data["wages_by_sector"]
    compensation_employees = figaro["compensation_employees"]
    if any(ismissing.(wages_by_sector))
        for yr_idx in 1:size(wages_by_sector, 2)
            missing_mask = ismissing.(wages_by_sector[:, yr_idx])
            if any(missing_mask)
                known_wages = collect(skipmissing(wages_by_sector[:, yr_idx]))
                known_comp = compensation_employees[.!missing_mask, yr_idx]
                if !isempty(known_wages) && sum(known_comp) > 0
                    wages_ratio = sum(known_wages) / sum(known_comp)
                    for (i, is_missing) in enumerate(missing_mask)
                        if is_missing
                            wages_by_sector[i, yr_idx] = compensation_employees[i, yr_idx] * wages_ratio
                        end
                    end
                    @info "  --> $(geo): Imputed $(sum(missing_mask)) missing wages_by_sector values for year index $(yr_idx) using ratio $(round(wages_ratio, digits=3))"
                end
            end
        end
        calibration_data["wages_by_sector"] = wages_by_sector
    end

    calibration_data["firm_cash_quarterly"]=f_bs("S11", "F2", "ASS");
    calibration_data["firm_debt_quarterly"]=f_bs("S11", "F4", "LIAB");

    # Consolidated F4 loans (co_nco='CO') exclude intra-sector transactions
    # (intercompany loans within multinationals). Annual only, so we compute
    # the CO/NCO ratio and interpolate it to quarterly frequency.
    try
        firm_debt_consolidated = query_values(conn, "nasa_10_f_bs", geo, years_str,
            "unit='MIO_EUR' AND sector='S11' AND na_item='F4' AND finpos='LIAB' AND co_nco='CO'")
        firm_debt_nonconsolidated = query_values(conn, "nasa_10_f_bs", geo, years_str,
            "unit='MIO_EUR' AND sector='S11' AND na_item='F4' AND finpos='LIAB' AND co_nco='NCO'")
        if length(firm_debt_consolidated) == length(firm_debt_nonconsolidated) == number_years
            consolidation_ratio = firm_debt_consolidated ./ firm_debt_nonconsolidated
            # Interpolate annual ratio to quarterly using year-end date numbers
            consolidation_ratio_quarterly = linear_interp_extrap(
                calibration_data["years_num"],
                consolidation_ratio,
                calibration_data["quarters_num"]
            )
            calibration_data["firm_debt_consolidation_ratio_quarterly"] = consolidation_ratio_quarterly
        else
            @warn "Consolidated F4 length mismatch for $geo (CO=$(length(firm_debt_consolidated)), NCO=$(length(firm_debt_nonconsolidated)), years=$(number_years)), skipping consolidation"
        end
    catch e
        @warn "Consolidated F4 data not available for $geo: $e"
    end

    calibration_data["household_cash_quarterly"]=f_bs("S14_S15", "F2", "ASS");

    all_monetary_institutions_equity = f_bs("S121_S122_S123", "F5", "LIAB")
    centralbank_equity = f_bs("S121", "F5", "LIAB")
    if size(centralbank_equity, 1) == number_quarters
        calibration_data["bank_equity_quarterly"] = all_monetary_institutions_equity - centralbank_equity;
    else
        calibration_data["bank_equity_quarterly"] = all_monetary_institutions_equity;
    end

    calibration_data["government_debt_quarterly"]=query_values(conn, "gov_10q_ggdebt", geo, quarters_str,
        "unit='MIO_EUR' AND sector='S13' AND na_item='GD'");

    calibration_data["government_deficit"]=gov_main("B9");

    # Try quarterly government deficit first, fall back to annual if missing
    quarterly_if_available!("government_deficit_quarterly", "gov_10q_ggnfa",
        "unit='MIO_EUR' AND sector='S13' AND na_item='B9' AND s_adj='NSA'",
        "government deficit", "will use annual data")

    calibration_data["property_income"]=nf_tr("S14_S15", "D4", "RECV");

    # D4 PAID: Property income paid by households (for net property income calculation)
    calibration_data["property_income_paid"]=nf_tr("S14_S15", "D4", "PAID");

    # Use B2A3G (Gross Operating Surplus/Mixed Income) instead of B2A3N (Net)
    # Gross includes capital consumption, which is needed for ESA-consistent disposable income
    calibration_data["mixed_income"]=nf_tr("S14_S15", "B2A3G", "RECV");

    calibration_data["firm_interest"]=nf_tr("S11", "D41", "PAID");

    # Try quarterly first, fall back to annual/4 approximation if missing
    quarterly_if_available!("firm_interest_quarterly", "nasq_10_nf_tr",
        "unit='CP_MEUR' AND sector='S11' AND na_item='D41' AND direct='PAID' AND s_adj='NSA'",
        "firm interest", "will use annual data with timescale conversion")

    calibration_data["corporate_tax"]=nf_tr("S11", "D51", "PAID");

    calibration_data["capital_taxes"]=gov_main("D91REC");

    if geo in ["EE", "SE"]
        ## convert missing entries to zeros
        calibration_data["capital_taxes"][ismissing.(calibration_data["capital_taxes"])] .= 0.0
    end

    calibration_data["interest_government_debt"]=gov_main("D41PAY");

    # Try quarterly first, fall back to annual/4 approximation if missing
    quarterly_if_available!("interest_government_debt_quarterly", "gov_10q_ggnfa",
        "unit='MIO_EUR' AND sector='S13' AND na_item='D41PAY' AND s_adj='NSA'",
        "government interest", "will use annual data with timescale conversion")

    calibration_data["unemployment_benefits"]=query_values(conn, "gov_10a_exp", geo, years_str,
        "unit='MIO_EUR' AND sector='S13' AND cofog99='GF1005' AND na_item='TE'");
    calibration_data["pension_benefits"]=query_values(conn, "gov_10a_exp", geo, years_str,
        "unit='MIO_EUR' AND sector='S13' AND cofog99='GF1002' AND na_item='TE'");

    # D62 RECV from household sector - captures ALL social benefits including private pensions
    # (gov_10a_main D62PAY only captures government-paid benefits, missing private pension funds)
    calibration_data["social_benefits"]=nf_tr("S14_S15", "D62", "RECV");

    # D61 PAID from household sector - captures ALL social contributions including private pensions
    # (gov_10a_main D61REC only captures government-received contributions, missing private pension funds)
    calibration_data["social_contributions"]=nf_tr("S14_S15", "D61", "PAID");

    # D7: Other current transfers (replaces government budget residual for ESA consistency)
    calibration_data["other_current_transfers_recv"]=nf_tr("S14_S15", "D7", "RECV");
    calibration_data["other_current_transfers_paid"]=nf_tr("S14_S15", "D7", "PAID");

    # B6G: Gross Disposable Income (ESA standard) for verification
    calibration_data["gross_disposable_income_esa"]=nf_tr("S14_S15", "B6G", "RECV");

    # B8G: Gross Saving (ESA standard) for savings rate verification
    calibration_data["gross_saving_esa"]=nf_tr("S14_S15", "B8G", "RECV");

    calibration_data["income_tax"]=gov_main("D5REC");

    calibration_data["gross_fixed_capitalformation"]=query_values(conn, "nama_10_an6", geo, years_str,
        "asset10='N11G' AND unit='CP_MEUR'");
    calibration_data["gross_capitalformation_dwellings"]=query_values(conn, "nama_10_an6", geo, years_str,
        "asset10='N111G' AND unit='CP_MEUR'");

    # Census data for unemployed/inactive (point-in-time fallback)
    # Census data is typically from 2011, not a time series
    for (key, aggregate, wstatus, description) in (("unemployed_census", "value", "UNE", "unemployed"),
                                                   ("inactive_census", "sum(value)", "INAC", "inactive"))
        try
            sqlquery="SELECT $(aggregate) FROM '$(pqfile("cens_11an_r2"))' WHERE geo='$(geo)' AND wstatus='$(wstatus)' AND sex='T' AND nace_r2='TOTAL' AND unit='NR' AND age='TOTAL'"
            result = execute(conn,sqlquery);
            if length(result) > 0 && !ismissing(result[1])
                calibration_data[key]=result[1];
            end
        catch e
            @warn "  --> $(geo): Census $(description) count not available ($(typeof(e)))"
        end
    end

    sqlquery="SELECT 1e3*value FROM '$(pqfile("nama_10_pe"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='THS_PER' AND na_item='POP_NC' ORDER BY time"
    calibration_data["population"]=execute(conn,sqlquery);

    sqlquery="SELECT 1e3*value FROM '$(pqfile("nama_10_a64_e"))' WHERE $(nace64_filter) AND time IN ($(years_str)) AND geo='$(geo)' AND unit = 'THS_PER' AND na_item='SAL_DC' ORDER BY time, nace_r2"
    calibration_data["employees"]=execute(conn,sqlquery);
    calibration_data["employees"]=reshape(calibration_data["employees"],(number_sectors,Int64(length(calibration_data["employees"])/number_sectors)));
    calibration_data["employees"]=coalesce.(calibration_data["employees"],100);

    ## sbs_ovw_act is a prolongation of the sbs_na_sca_r2 dataset, and bd_l_form
    ## is that of bd_9ac_l_form, so we use these two datasets to extend the
    ## timeframe of firm population data.

    ## Query which years are in respective data sources (filtered to calibration range)
    sbs_na_sca_r2_years = extract_years(conn, "sbs_na_sca_r2_a64", start_calibration_year, end_calibration_year);
    sbs_ovw_act_years = extract_years(conn, "sbs_ovw_act_a64", start_calibration_year, end_calibration_year);
    bd_9ac_l_form_r2_years = extract_years(conn, "bd_9ac_l_form_r2_a64", start_calibration_year, end_calibration_year);
    bd_l_form_years = extract_years(conn, "bd_l_form_a64", start_calibration_year, end_calibration_year);

    ## Make sure that only extract data up until the last year that is available
    ## in both datasets
    latest_year_fully_available = min(maximum(bd_l_form_years), maximum(sbs_ovw_act_years))
    filter!(x -> x <= latest_year_fully_available, bd_l_form_years)
    filter!(x -> x <= latest_year_fully_available, sbs_ovw_act_years)

    # Create variable `sbs_na_sca_r2_years_str` that only contains those years
    # that are not in sbs_ovw_act
    sbs_na_sca_r2_years = setdiff(sbs_na_sca_r2_years, sbs_ovw_act_years)
    sbs_ovw_act_years_str = sql_list(sbs_ovw_act_years)
    sbs_na_sca_r2_years_str = sql_list(sbs_na_sca_r2_years)

    ## For bd_* datasets, we do it the other way round: we source all years from
    ## bd_9ac_* first, then take the rest from bd_l_form.
    bd_l_form_years = setdiff(bd_l_form_years, bd_9ac_l_form_r2_years)
    bd_l_form_years_str = sql_list(bd_l_form_years)
    bd_9ac_l_form_r2_years_str = sql_list(bd_9ac_l_form_r2_years)

    ## Select only the relevant years from each of the datasets and append them
    ## to each other
    nace_r2_industries_subset = "('K64','K65','K66','P','Q86','Q87_Q88','R90-R92','R93','S94','S95','S96')"
    sqlquery= """
    SELECT value FROM (
    SELECT time, nace_r2, value AS value FROM '$(pqfile("sbs_na_sca_r2_a64"))'
    WHERE time IN ($(sbs_na_sca_r2_years_str)) AND geo='$(geo)'
    AND indic_sb='V11110' AND nace_r2 NOT IN $(nace_r2_industries_subset)
    UNION
    SELECT time, nace_r2, value AS value FROM '$(pqfile("sbs_ovw_act_a64"))'
    WHERE time IN ($(sbs_ovw_act_years_str)) AND geo='$(geo)'
    AND indic_sbs='ENT_NR' AND nace_r2 NOT IN $(nace_r2_industries_subset)
    UNION
    SELECT time, nace_r2, value AS value FROM '$(pqfile("bd_9ac_l_form_r2_a64"))'
    WHERE time IN ($(bd_9ac_l_form_r2_years_str)) AND geo='$(geo)'
    AND indic_sb='V11910' AND leg_form='TOTAL' AND nace_r2 IN $(nace_r2_industries_subset)
    UNION
    SELECT time, nace_r2, value AS value FROM '$(pqfile("bd_l_form_a64"))'
    WHERE time IN ($(bd_l_form_years_str)) AND geo='$(geo)'
    AND indic_sbs='ENT_NR' AND leg_form='TOTAL' AND nace_r2 IN $(nace_r2_industries_subset)
    ) foo ORDER BY time, nace_r2
    """

    calibration_data["firms"]=execute(conn,sqlquery);
    firm_years = sort(collect(union(union(sbs_na_sca_r2_years, sbs_ovw_act_years),
        union(bd_9ac_l_form_r2_years, bd_l_form_years))))
    number_firm_years = length(firm_years)
    calibration_data["firms"]=reshape(calibration_data["firms"],(number_sectors,number_firm_years));

    ## Farm Structure Survey for A01 (agriculture) - provides farm counts for survey years
    try
        firm_years_str = sql_list(firm_years)
        sqlquery = """
        SELECT time, SUM(value) as farms
        FROM '$(pqfile("ef_m_farmleg"))'
        WHERE geo='$(geo)'
          AND statinfo='TOTAL'
          AND unit='HLD'
          AND leg_form='TOTAL'
          AND so_eur='TOTAL'
          AND uaarea='TOTAL'
          AND time IN ($(firm_years_str))
        GROUP BY time
        ORDER BY time
        """
        farm_survey = DBInterface.execute(conn, sqlquery) |> DataFrame

        if nrow(farm_survey) > 1
            farm_survey = filter(row -> !ismissing(row.farms), farm_survey)

            if nrow(farm_survey) > 1
                survey_year_indices = [findfirst(==(y), firm_years) for y in farm_survey.time]
                survey_year_indices = filter(!isnothing, survey_year_indices)

                if length(survey_year_indices) > 1
                    survey_values = Float64.(farm_survey.farms[1:length(survey_year_indices)])
                    all_firm_year_indices = collect(1:number_firm_years)

                    interpolated_farms = max.(1, round.(linear_interp_extrap(
                        survey_year_indices,
                        survey_values,
                        all_firm_year_indices)))

                    calibration_data["firms"][1, :] = interpolated_farms
                    @info " --> $(geo): Used Farm Structure Survey for A01 agriculture ($(length(survey_year_indices)) survey points)"
                end
            end
        end
    catch e
        @warn " --> $(geo): Farm Structure Survey data not available for A01 ($(typeof(e))), will use fallback imputation"
    end

    ## Impute industries with some missing entries using linear interpolation
    industries_with_missings = 0 .< dropdims(sum(ismissing.(calibration_data["firms"]), dims = 2), dims = 2) .< (number_firm_years - 3)
    indizes_with_missing = findall(!iszero, industries_with_missings)
    all_time_indizes = collect(1:number_firm_years)
    for industry_index in indizes_with_missing
        industry_values = calibration_data["firms"][industry_index, :]
        nonmissing_entries = all_time_indizes[.!ismissing.(industry_values)]
        @info " --> $(geo): Imputing nr of firms in industry $(industry_index): nr missing = $(sum(ismissing.(industry_values)))"
        imputed_industry_values = max.(1, round.(linear_interp_extrap(
            nonmissing_entries,
            Float64.(industry_values[nonmissing_entries]),
            all_time_indizes)))
        calibration_data["firms"][industry_index, :] = imputed_industry_values
    end

    ## Find industries that still have missings (# missings > (number_years -
    ## 3)): these industries will be imputed by assuming an average number of
    ## employees and estimating the number of firms from the number of employees
    industries_with_missings = 0 .< dropdims(sum(ismissing.(calibration_data["firms"]), dims = 2), dims = 2)
    indizes_with_missing = findall(!iszero, industries_with_missings)
    indizes_with_missing_str = sql_list(indizes_with_missing)
    sqlquery = "select nace from '$(pqfile("nace64"))' where id in ($(indizes_with_missing_str))"
    missing_industries = execute(conn, sqlquery)

    if length(missing_industries) > 0
        @warn " --> $(geo): Nr firms in industries $(missing_industries) are still missing (will be imputed with avg nr of workers)"
    end

    ## Use sector-specific average employees per firm to estimate firm counts
    sector_avg_employees = Dict(
        1 => 5,    # A01 Agriculture
        2 => 5,    # A02 Forestry
        3 => 3,    # A03 Fishing
        41 => 50,  # K64 Financial services
        42 => 20,  # K65 Insurance
        43 => 15,  # K66 Financial auxiliaries
    )
    default_avg_employees = 10

    firms_matrix = calibration_data["firms"]
    employees_subset = calibration_data["employees"][:, 1:number_firm_years]
    for sector_idx in 1:number_sectors
        for year_idx in 1:number_firm_years
            if ismissing(firms_matrix[sector_idx, year_idx])
                avg_emp = get(sector_avg_employees, sector_idx, default_avg_employees)
                emp_count = employees_subset[sector_idx, year_idx]
                firms_matrix[sector_idx, year_idx] = max(1, round(emp_count / avg_emp))
            end
        end
    end
    calibration_data["firms"] = firms_matrix


    output=dropdims(sum(figaro["intermediate_consumption"], dims=1), dims=1) +
           figaro["taxes_products"] + figaro["taxes_production"] +
           figaro["compensation_employees"] + figaro["operating_surplus"];#+capital_consumption;
    if geo in CAPITAL_CONSUMPTION_FROM_EU20
        capital_consumption_structure = calibration_data["nace64_capital_consumption_eu20"] ./ calibration_data["nominal_nace64_output_eu20"] .* output
        calibration_data["capital_consumption"]=calibration_data["capital_consumption"]' .*
                                                capital_consumption_structure ./
                                                sum(capital_consumption_structure, dims = 1);
    else
        calibration_data["capital_consumption"]=calibration_data["nace64_capital_consumption"]./calibration_data["nominal_nace64_output"].*output;
    end

    if geo in SECTORAL_FIXED_ASSETS
        calibration_data["fixed_assets"]=sectoral("nama_10_nfa_st", "asset10='N11N' AND unit='CRC_MEUR'");
        calibration_data["dwellings"]=sectoral("nama_10_nfa_st", "asset10='N111N' AND unit='CRC_MEUR'");
    else
        ## For some countries, there is insufficient industry-level data on
        ## fixed assets and dwellings, only country totals. For these countries,
        ## we assume that fixed assets have the same structure as the EU7 and
        ## distribute their total fixed assets accordingly. Total dwellings are
        ## all assigned to the real estate industry.
        calibration_data["fixed_assets"]=query_values(conn, "nama_10_nfa_st", geo, years_str,
            "nace_r2 = 'TOTAL' AND asset10='N11N' AND unit='CRC_MEUR'");
        calibration_data["dwellings"]=query_values(conn, "nama_10_nfa_st", geo, years_str,
            "nace_r2 = 'TOTAL' AND asset10='N111N' AND unit='CRC_MEUR'");

        calibration_data["fixed_assets_eu7"]=sectoral_sum("nama_10_nfa_st", "asset10='N11N' AND unit='CRC_MEUR'", EU7);
        calibration_data["dwellings_eu7"]=sectoral_sum("nama_10_nfa_st", "asset10='N111N' AND unit='CRC_MEUR'", EU7);
        calibration_data["nominal_nace64_output_eu7"]=sectoral_sum("nama_10_a64", "unit = 'CP_MEUR' AND na_item='P1'", EU7);

        fixed_assets_structure = (calibration_data["fixed_assets_eu7"]-calibration_data["dwellings_eu7"]) ./ calibration_data["nominal_nace64_output_eu7"].*output
        fixed_assets_other_than_dwellings=(calibration_data["fixed_assets"]-calibration_data["dwellings"])' .*
                                          fixed_assets_structure ./
                                          sum(fixed_assets_structure, dims = 1);
        dwellings=zeros(Union{Missing, Float64},
            size(fixed_assets_other_than_dwellings));
        dwellings[44,:]=calibration_data["dwellings"];
        calibration_data["fixed_assets"]=fixed_assets_other_than_dwellings+dwellings;
        calibration_data["dwellings"]=dwellings;
    end

    return calibration_data
end
