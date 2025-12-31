# Firm imputation helper functions are defined in utils_firm_imputation.jl

function import_calibration_data(geo, start_calibration_year, end_calibration_year,
                                 number_sectors, figaro)
    conn = DBInterface.connect(DuckDB.DB)

    all_years = collect(start_calibration_year:end_calibration_year)
    number_years=end_calibration_year-start_calibration_year + 1;
    number_quarters=number_years*4;
    ## Create a year-quarter vector and string for creating a data.frame and for SQL
    ## queries
    years_str = create_year_array_str(all_years)
    all_quarters = ["Q1", "Q2", "Q3", "Q4"]
    quarters_vec = ["$(year)-$(quarter)" for year in all_years for quarter in all_quarters]
    quarters_str = create_year_array_str(quarters_vec)

    calibration_data = Dict()

    ## Set these date numbers
    calibration_data["years_num"] = date2num_yearly(start_calibration_year:end_calibration_year)
    calibration_data["quarters_num"] = date2num_quarterly(start_calibration_year:end_calibration_year)

    ## time series
    if geo in ["FR", "IE", "LT", "LU", "MT", "PL", "SE"]
        sqlquery="SELECT value FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2='TOTAL' AND time IN ($(years_str)) AND unit = 'CP_MEUR' AND na_item='P51C' AND geo='$(geo)' ORDER BY time, nace_r2"
        calibration_data["capital_consumption"]=execute(conn,sqlquery);

        sqlquery="SELECT sum(value) FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'CP_MEUR' AND na_item='P51C' AND geo IN ('AT' ,'BE' ,'BG' ,'HR' ,'CY' ,'CZ' ,'DK' ,'EE' ,'FI' ,'DE' ,'EL' ,'HU' ,'IT' ,'LV' ,'NL' ,'PT' ,'RO' ,'SK' ,'SI' ,'ES') GROUP BY time, nace_r2 ORDER BY time, nace_r2"
        calibration_data["nace64_capital_consumption_eu20"]=execute(conn,sqlquery, (number_sectors,number_years));

        sqlquery="SELECT sum(value) FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'CP_MEUR' AND na_item='P1' AND geo IN ('AT' ,'BE' ,'BG' ,'HR' ,'CY' ,'CZ' ,'DK' ,'EE' ,'FI' ,'DE' ,'EL' ,'HU' ,'IT' ,'LV' ,'NL' ,'PT' ,'RO' ,'SK' ,'SI' ,'ES') GROUP BY time, nace_r2 ORDER BY time, nace_r2"
        calibration_data["nominal_nace64_output_eu20"]=execute(conn,sqlquery,(number_sectors,number_years));
    else
        sqlquery="SELECT value FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'CP_MEUR' AND na_item='P51C' AND geo='$(geo)' ORDER BY time, nace_r2"
        calibration_data["nace64_capital_consumption"]=execute(conn,sqlquery,(number_sectors,number_years));

        sqlquery="SELECT value FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'CP_MEUR' AND na_item='P1' AND geo='$(geo)' ORDER BY time, nace_r2"
        calibration_data["nominal_nace64_output"]=execute(conn,sqlquery,(number_sectors,number_years));
    end

    sqlquery="SELECT value FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2='TOTAL' AND time IN ($(years_str)) AND unit = 'CP_MEUR' AND na_item='D11' AND geo='$(geo)' ORDER BY time, nace_r2"
    calibration_data["wages"]=execute(conn,sqlquery);

    # Sectoral wages (D11 = wages and salaries) - needed for employers_social_contributions calculation
    sqlquery="SELECT value FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'CP_MEUR' AND na_item='D11' AND geo='$(geo)' ORDER BY time, nace_r2"
    calibration_data["wages_by_sector"]=execute(conn,sqlquery,(number_sectors,number_years));

    # sqlquery="SELECT value FROM '$(pqfile("nasa_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S11' AND na_item='F2' AND finpos='ASS' AND co_nco='NCO' ORDER BY time"
    # calibration_data["firm_cash"]=execute(conn,sqlquery);

    # old:
    # sqlquery="SELECT value, time FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S11' AND na_item='F2' AND finpos='ASS' ORDER BY time"
    # calibration_data["firm_cash_quarterly"]=fetch_(conn,sqlquery,calibration_data["quarters_num"]);
    # new:
    sqlquery="SELECT value FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S11' AND na_item='F2' AND finpos='ASS' ORDER BY time"
    calibration_data["firm_cash_quarterly"]=execute(conn,sqlquery);

    # sqlquery="SELECT value FROM '$(pqfile("nasa_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S11' AND na_item='F4' AND finpos='LIAB' AND co_nco='NCO' ORDER BY time"
    # calibration_data["firm_debt"]=execute(conn,sqlquery);

    # old:
    # sqlquery="SELECT value, time FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S11' AND na_item='F4' AND finpos='LIAB' ORDER BY time"
    # calibration_data["firm_debt_quarterly"]=fetch_(conn,sqlquery,calibration_data["quarters_num"]);
    # new:
    sqlquery="SELECT value FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S11' AND na_item='F4' AND finpos='LIAB' ORDER BY time"
    calibration_data["firm_debt_quarterly"]=execute(conn,sqlquery);

    # sqlquery="SELECT value FROM '$(pqfile("nasa_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S14_S15' AND na_item='F2' AND finpos='ASS' AND co_nco='NCO' ORDER BY time"
    # calibration_data["household_cash"]=execute(conn,sqlquery);

    # old:
    # sqlquery="SELECT value, time FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S14_S15' AND na_item='F2' AND finpos='ASS' ORDER BY time"
    # calibration_data["household_cash_quarterly"]=fetch_(conn,sqlquery,calibration_data["quarters_num"]);
    # new:
    sqlquery="SELECT value FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S14_S15' AND na_item='F2' AND finpos='ASS' ORDER BY time"
    calibration_data["household_cash_quarterly"]=execute(conn,sqlquery);

    # sqlquery="SELECT value FROM '$(pqfile("nasa_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S121_S122_S123' AND na_item='F5' AND finpos='LIAB' AND co_nco='NCO' ORDER BY time"
    # calibration_data["bank_equity"]=execute(conn,sqlquery);

    # old:
    # sqlquery="SELECT value, time FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S121_S122_S123' AND na_item='F5' AND finpos='LIAB' ORDER BY time"
    # calibration_data["bank_equity_quarterly"]=fetch_(conn,sqlquery,calibration_data["quarters_num"]);
    # new:
    sqlquery="SELECT value FROM '$(pqfile("nasq_10_f_bs"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S121_S122_S123' AND na_item='F5' AND finpos='LIAB' ORDER BY time"
    calibration_data["bank_equity_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10q_ggdebt"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='GD' ORDER BY time"
    calibration_data["government_debt_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='B9' ORDER BY time"
    calibration_data["government_deficit"]=execute(conn,sqlquery);

    # Try quarterly government deficit first, fall back to annual if missing
    try
        sqlquery="SELECT value FROM '$(pqfile("gov_10q_ggnfa"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='B9' AND s_adj='NSA' ORDER BY time"
        calibration_data["government_deficit_quarterly"]=execute(conn,sqlquery);
        if length(calibration_data["government_deficit_quarterly"]) == 0
            throw(ErrorException("Empty result"))
        end
    catch e
        @warn "  --> $(geo): Quarterly government deficit data not available ($(typeof(e))), will use annual data"
        # Don't create quarterly variable - will fall back to annual in get_params_and_initial_conditions.jl
    end

    sqlquery="SELECT value FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector='S14_S15' AND na_item='D4' AND direct='RECV' ORDER BY time"
    calibration_data["property_income"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector='S14_S15' AND na_item='B2A3N' AND direct='RECV' ORDER BY time"
    calibration_data["mixed_income"]=execute(conn,sqlquery);

    sqlquery="SELECT sum(value) FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector IN ('S11','S12') AND na_item='D41' AND direct='PAID' GROUP BY time ORDER BY time"
    calibration_data["firm_interest"]=execute(conn,sqlquery);

    # Try quarterly first, fall back to annual/4 approximation if missing
    try
        sqlquery="SELECT sum(value) FROM '$(pqfile("nasq_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='CP_MEUR' AND sector IN ('S11','S12') AND na_item='D41' AND direct='PAID' AND s_adj='NSA' GROUP BY time ORDER BY time"
        calibration_data["firm_interest_quarterly"]=execute(conn,sqlquery);
        if length(calibration_data["firm_interest_quarterly"]) == 0
            throw(ErrorException("Empty result"))
        end
    catch e
        @warn "  --> $(geo): Quarterly firm interest data not available ($(typeof(e))), will use annual data with timescale conversion"
        # Don't create quarterly variable - will fall back to annual in get_params_and_initial_conditions.jl
    end

    sqlquery="SELECT sum(value) FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector IN ('S11','S12') AND na_item='D51' AND direct='PAID' GROUP BY time ORDER BY time"
    calibration_data["corporate_tax"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D91REC' ORDER BY time"
    calibration_data["capital_taxes"]=execute(conn,sqlquery);

    if geo in ["EE", "SE"]
        ## convert missing entries to zeros
        calibration_data["capital_taxes"][ismissing.(calibration_data["capital_taxes"])] .= 0.0
    end

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D41PAY' ORDER BY time"
    calibration_data["interest_government_debt"]=execute(conn,sqlquery);

    # Try quarterly first, fall back to annual/4 approximation if missing
    try
        sqlquery="SELECT value FROM '$(pqfile("gov_10q_ggnfa"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D41PAY' AND s_adj='NSA' ORDER BY time"
        calibration_data["interest_government_debt_quarterly"]=execute(conn,sqlquery);
        if length(calibration_data["interest_government_debt_quarterly"]) == 0
            throw(ErrorException("Empty result"))
        end
    catch e
        @warn "  --> $(geo): Quarterly government interest data not available ($(typeof(e))), will use annual data with timescale conversion"
        # Don't create quarterly variable - will fall back to annual in get_params_and_initial_conditions.jl
    end

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_exp"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND cofog99='GF1005' AND na_item='TE' ORDER BY time"
    calibration_data["unemployment_benefits"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_exp"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND cofog99='GF1002' AND na_item='TE' ORDER BY time"
    calibration_data["pension_benefits"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D62PAY' ORDER BY time"
    calibration_data["social_benefits"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D61REC' ORDER BY time"
    calibration_data["social_contributions"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D5REC' ORDER BY time"
    calibration_data["income_tax"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_an6"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND asset10='N11G' AND unit='CP_MEUR' ORDER BY time"
    calibration_data["gross_fixed_capitalformation"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_an6"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND asset10='N111G' AND unit='CP_MEUR' ORDER BY time"
    calibration_data["gross_capitalformation_dwellings"]=execute(conn,sqlquery);

    # Direct counts of unemployed and inactive from census (for OCM calibration)
    # Census data is point-in-time (typically 2011), not a time series
    try
        sqlquery="SELECT value FROM '$(pqfile("cens_11an_r2"))' WHERE geo='$(geo)' AND wstatus='UNE' AND sex='T' AND nace_r2='TOTAL' AND unit='NR' AND age='TOTAL'"
        result = execute(conn,sqlquery);
        if length(result) > 0 && !ismissing(result[1])
            calibration_data["unemployed_census"]=result[1];
        end
    catch e
        @warn "  --> $(geo): Census unemployed count not available ($(typeof(e)))"
    end

    try
        sqlquery="SELECT sum(value) FROM '$(pqfile("cens_11an_r2"))' WHERE geo='$(geo)' AND wstatus='INAC' AND sex='T' AND nace_r2='TOTAL' AND unit='NR' AND age='TOTAL'"
        result = execute(conn,sqlquery);
        if length(result) > 0 && !ismissing(result[1])
            calibration_data["inactive_census"]=result[1];
        end
    catch e
        @warn "  --> $(geo): Census inactive count not available ($(typeof(e)))"
    end

    sqlquery="SELECT 1e3*value FROM '$(pqfile("nama_10_pe"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='THS_PER' AND na_item='POP_NC' ORDER BY time"
    calibration_data["population"]=execute(conn,sqlquery);

    # [calibration_data["firms"],calibration_data["employees"]]=number_of_agents(conn,geo,start_calibration_year:end_calibration_year);

    sqlquery="SELECT 1e3*value FROM '$(pqfile("nama_10_a64_e"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') and time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'THS_PER' AND na_item='SAL_DC' ORDER BY time, nace_r2"
    calibration_data["employees"]=execute(conn,sqlquery);
    calibration_data["employees"]=reshape(calibration_data["employees"],(number_sectors,Int64(length(calibration_data["employees"])/number_sectors)));
    calibration_data["employees"]=coalesce.(calibration_data["employees"],100);

    ## sbs_ovw_act is a prolongation of the sbs_na_sca_r2 dataset, and bd_l_form
    ## is that of bd_9ac_l_form, so we use these two datasets to extend the
    ## timeframe of firm population data.

    ## Query which years are in respective data sources
    sbs_na_sca_r2_years = extract_years(conn, "sbs_na_sca_r2_a64", start_calibration_year);
    sbs_ovw_act_years = extract_years(conn, "sbs_ovw_act_a64", start_calibration_year);
    bd_9ac_l_form_r2_years = extract_years(conn, "bd_9ac_l_form_r2_a64", start_calibration_year);
    bd_l_form_years = extract_years(conn, "bd_l_form_a64", start_calibration_year);

    # CRITICAL: Limit all year lists to calibration range BEFORE SQL queries
    # extract_years() only filters >= start_year, NOT <= end_year
    # Without this, SQL queries fetch data beyond end_calibration_year, causing reshape errors
    filter!(y -> y <= end_calibration_year, sbs_na_sca_r2_years)
    filter!(y -> y <= end_calibration_year, sbs_ovw_act_years)
    filter!(y -> y <= end_calibration_year, bd_9ac_l_form_r2_years)
    filter!(y -> y <= end_calibration_year, bd_l_form_years)

    ## Make sure that only extract data up until the last year that is available
    ## in both datasets
    latest_year_fully_available = min(maximum(bd_l_form_years), maximum(sbs_ovw_act_years))
    filter!(x -> x <= latest_year_fully_available, bd_l_form_years)
    filter!(x -> x <= latest_year_fully_available, sbs_ovw_act_years)

    # Create variable `sbs_na_sca_r2_years_str` that only contains those years
    # that are not in sbs_ovw_act
    sbs_na_sca_r2_years = setdiff(sbs_na_sca_r2_years, sbs_ovw_act_years)
    sbs_ovw_act_years_str = create_year_array_str(sbs_ovw_act_years)
    sbs_na_sca_r2_years_str = create_year_array_str(sbs_na_sca_r2_years)

    ## For bd_* datasets, we do it the other way round: we source all years from
    ## bd_9ac_* first, then take the rest from bd_l_form.
    bd_l_form_years = setdiff(bd_l_form_years, bd_9ac_l_form_r2_years)
    bd_l_form_years_str = create_year_array_str(bd_l_form_years)
    bd_9ac_l_form_r2_years_str = create_year_array_str(bd_9ac_l_form_r2_years)

    ## Select only the relevant years from each of the datasets and append them
    ## to each other
    nace_r2_industries_subset = "('K64','K65','K66','P','Q86','Q87_Q88','R90-R92','R93','S94','S95','S96')"
    # "SELECT time, nace_r2, value FROM (
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
    # Track ACTUAL firm years for proper alignment with employee data
    firm_years = sort(union(union(sbs_na_sca_r2_years, sbs_ovw_act_years),
        union(bd_9ac_l_form_r2_years, bd_l_form_years)))

    # CRITICAL: Filter to only years within calibration range
    # extract_years() only filters >= start_year, NOT <= end_year
    # This prevents firm_years from extending beyond all_years, which would cause
    # firm_year_indices to contain 'nothing' values and crash
    firm_years = filter(y -> start_calibration_year <= y <= end_calibration_year, firm_years)

    number_firm_years = length(firm_years)
    calibration_data["firms"]=reshape(calibration_data["firms"],(number_sectors,number_firm_years));

    # Create mapping from firm years to employee matrix column indices
    # employees matrix has columns for all_years, firms matrix only for firm_years
    firm_year_indices = [findfirst(==(y), all_years) for y in firm_years]

    ## Get all industries whose number of missing entries is more than 0 and
    ## less than the number of years minus 3 (so there are at least some entries
    ## which we can use for imputation). We use linear interpolation to impute,
    ## but log the values before and exponentiate them afterwards (also to avoid
    ## predicting negative values). Finally, we assume that there is at least
    ## one firm in every industry.
    industries_with_missings = 0 .< dropdims(sum(ismissing.(calibration_data["firms"]), dims = 2), dims = 2) .< (number_firm_years - 3)
    indizes_with_missing = findall(!iszero, industries_with_missings)
    all_time_indizes = collect(1:number_firm_years)
    for industry_index in indizes_with_missing
        industry_values = calibration_data["firms"][industry_index, :]
        nonmissing_entries = all_time_indizes[.!ismissing.(industry_values)]
        @info " --> $(geo): Imputing nr of firms in industry $(industry_index): nr missing = $(sum(ismissing.(industry_values)))"
        imputed_industry_values = max.(round.(exp.(linear_interp_extrap(nonmissing_entries,
            log.(industry_values[nonmissing_entries]),
            all_time_indizes))), 1)
        calibration_data["firms"][industry_index, :] = imputed_industry_values
    end

    ## Find industries that still have missings (# missings > (number_years -
    ## 3)): these industries will be imputed using a hierarchical fallback strategy:
    ## 1. Sector-specific ratios from peer EU countries
    ## 2. Division-level average from other sectors in same NACE division
    ## 3. Economy-wide average if no division data available
    industries_with_missings = 0 .< dropdims(sum(ismissing.(calibration_data["firms"]), dims = 2), dims = 2)
    indizes_with_missing = findall(!iszero, industries_with_missings)
    indizes_with_missing_str = create_year_array_str(indizes_with_missing)
    sqlquery = "select nace from '$(pqfile("nace64"))' where id in ($(indizes_with_missing_str))"
    missing_industries = execute(conn, sqlquery)

    if length(missing_industries) > 0
        @warn " --> $(geo): Nr firms in industries $(missing_industries) are still missing (will be imputed)"
    end

    ## Calculate economy-wide fallback ratio from sectors that DO have data
    ## (instead of using arbitrary fixed value of 10)
    non_missing_mask = .!ismissing.(calibration_data["firms"])
    economy_wide_avg_employees_per_firm = if any(non_missing_mask)
        total_employees = sum(calibration_data["employees"][:, firm_year_indices][non_missing_mask])
        total_firms = sum(calibration_data["firms"][non_missing_mask])
        total_firms > 0 ? total_employees / total_firms : 10.0
    else
        10.0  # Ultimate fallback if no firm data at all
    end

    ## For each industry with missing firm data, try peer country approach first
    for (idx, industry_index) in enumerate(indizes_with_missing)
        nace_code = missing_industries[idx]

        # Try to get sector-specific ratio from peer countries
        peer_ratio = calculate_sector_employees_per_firm_from_peers(
            conn, nace_code, geo, years_str, number_years
        )

        if !isnothing(peer_ratio)
            # Use peer country sector-specific ratio
            employees_this_sector = calibration_data["employees"][industry_index, firm_year_indices]
            calibration_data["firms"][industry_index, 1:number_firm_years] = round.(employees_this_sector / peer_ratio)
            @info "  --> $(geo): Sector $(nace_code) using peer average: $(round(peer_ratio, digits=1)) employees/firm"
        else
            # Try division-level average as intermediate fallback
            division_ratio = calculate_division_employees_per_firm(
                conn, nace_code, geo,
                calibration_data["employees"],
                calibration_data["firms"],
                number_sectors, number_firm_years,
                firm_year_indices
            )

            if !isnothing(division_ratio)
                # Use division-level average
                employees_this_sector = calibration_data["employees"][industry_index, firm_year_indices]
                calibration_data["firms"][industry_index, 1:number_firm_years] = round.(employees_this_sector / division_ratio)
                division = string(first(filter(isletter, nace_code)))
                @info "  --> $(geo): Sector $(nace_code) using division $(division) average: $(round(division_ratio, digits=1)) employees/firm"
            else
                # Fall back to economy-wide average for this country
                employees_this_sector = calibration_data["employees"][industry_index, firm_year_indices]
                calibration_data["firms"][industry_index, 1:number_firm_years] = round.(employees_this_sector / economy_wide_avg_employees_per_firm)

                # NOTE: For agriculture sectors (A01-A03), firm counts are not collected in
                # Eurostat SBS tables. An alternative would be to use FSS (Farm Structure Survey)
                # tables like ef_m_farmleg for number of agricultural holdings, but this has
                # conceptual issues (holdings ≠ firms). See comment in CalibrateBeforeIT.jl
                # for full explanation of FSS alternative.

                @warn "  --> $(geo): Sector $(nace_code) using economy-wide fallback: $(round(economy_wide_avg_employees_per_firm, digits=1)) employees/firm (no division data available)"
            end
        end
    end


    output=dropdims(sum(figaro["intermediate_consumption"], dims=1), dims=1) +
           figaro["taxes_products"] + figaro["taxes_production"] +
           figaro["compensation_employees"] + figaro["operating_surplus"];#+capital_consumption;
    if geo in ["FR", "IE", "LT", "LU", "MT", "PL", "SE"]
        calibration_data["capital_consumption"]=calibration_data["capital_consumption"]' .*
                                                (calibration_data["nace64_capital_consumption_eu20"] ./ calibration_data["nominal_nace64_output_eu20"] .* output) ./
                                                sum(calibration_data["nace64_capital_consumption_eu20"] ./ calibration_data["nominal_nace64_output_eu20"] .* output,
                                                dims = 1);
    else
        calibration_data["capital_consumption"]=calibration_data["nace64_capital_consumption"]./calibration_data["nominal_nace64_output"].*output;
    end

    if geo in ["AT", "BG", "CZ", "EL", "FI", "LV", "SK"] # OR [2025-05-06 Di]: removed DK
        sqlquery="SELECT value FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND geo='$(geo)' AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND asset10='N11N' AND unit='CRC_MEUR' ORDER BY time, nace_r2"
        calibration_data["fixed_assets"]=execute(conn,sqlquery,(number_sectors,number_years));

        sqlquery="SELECT value FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND geo='$(geo)' AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND asset10='N111N' AND unit='CRC_MEUR' ORDER BY time, nace_r2"
        calibration_data["dwellings"]=execute(conn,sqlquery,(number_sectors,number_years));
    else
        ## For some countries, there is insufficient industry-level data on
        ## fixed assets and dwellings, only country totals. For these countries,
        ## we assume that fixed assets have the same structure as the EU7 and
        ## distribute their total fixed assets accordingly. Total dwellings are
        ## all assigned to the real estate industry.
        sqlquery="SELECT value FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 = 'TOTAL' AND geo='$(geo)' AND time IN ($(years_str)) AND asset10='N11N' AND unit='CRC_MEUR' ORDER BY time"
        calibration_data["fixed_assets"]=execute(conn,sqlquery);

        sqlquery="SELECT value FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 = 'TOTAL' AND geo='$(geo)' AND time IN ($(years_str)) AND asset10='N111N' AND unit='CRC_MEUR' ORDER BY time"
        calibration_data["dwellings"]=execute(conn,sqlquery);

        sqlquery="SELECT sum(value) FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND geo IN ('AT', 'CZ', 'DK', 'EL', 'FI', 'LV', 'SK') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND asset10='N11N' AND unit='CRC_MEUR' GROUP BY time, nace_r2 ORDER BY time, nace_r2"
        calibration_data["fixed_assets_eu7"]=execute(conn,sqlquery,(number_sectors,number_years));

        sqlquery="SELECT sum(value) FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND geo IN ('AT', 'CZ', 'DK', 'EL', 'FI', 'LV', 'SK') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND asset10='N111N' AND unit='CRC_MEUR' GROUP BY time, nace_r2 ORDER BY time, nace_r2"
        calibration_data["dwellings_eu7"]=execute(conn,sqlquery,(number_sectors,number_years));

        sqlquery="SELECT sum(value) FROM '$(pqfile("nama_10_a64"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'CP_MEUR' AND na_item='P1' AND geo IN ('AT', 'CZ', 'DK', 'EL', 'FI', 'LV', 'SK') GROUP BY time, nace_r2 ORDER BY time, nace_r2"
        calibration_data["nominal_nace64_output_eu7"]=execute(conn,sqlquery,(number_sectors,number_years));

        fixed_assets_other_than_dwellings=(calibration_data["fixed_assets"]-calibration_data["dwellings"])' .*
                                          ((calibration_data["fixed_assets_eu7"]-calibration_data["dwellings_eu7"]) ./ calibration_data["nominal_nace64_output_eu7"].*output) ./
                                          sum((calibration_data["fixed_assets_eu7"]-calibration_data["dwellings_eu7"]) ./ calibration_data["nominal_nace64_output_eu7"].*output,
                                              dims = 1);
        dwellings=zeros(Union{Missing, Float64},
            size(fixed_assets_other_than_dwellings));
        dwellings[44,:]=calibration_data["dwellings"];
        calibration_data["fixed_assets"]=fixed_assets_other_than_dwellings+dwellings;
        calibration_data["dwellings"]=dwellings;
    end

    return calibration_data
end
