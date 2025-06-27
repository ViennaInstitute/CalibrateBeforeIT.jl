

function import_calibration_data(geo, start_calibration_year, end_calibration_year,
                                 number_sectors)
    conn = DBInterface.connect(DuckDB.DB)

    all_years = collect(start_calibration_year:end_calibration_year)
    number_years=end_calibration_year-start_calibration_year + 1;
    number_quarters=number_years*4;
    years_str = join(["'$(year)'" for year in all_years], ", ")
    ## Create a year-quarter vector and string for creating a data.frame and for SQL
    ## queries
    all_quarters = ["Q1", "Q2", "Q3", "Q4"]
    quarters_vec = ["$(year)-$(quarter)" for year in all_years for quarter in all_quarters]
    quarters_str = join(["'$(yearquarter)'" for yearquarter in quarters_vec], ",")

    calibration_data = Dict()

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

    if geo in ["AT", "CZ", "EL", "FI", "LV", "SK"] # OR [2025-05-06 Di]: removed DK
        sqlquery="SELECT value FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND geo='$(geo)' AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND asset10='N11N' AND unit='CRC_MEUR' ORDER BY time, nace_r2"
        calibration_data["fixed_assets"]=execute(conn,sqlquery,(number_sectors,number_years));

        sqlquery="SELECT value FROM '$(pqfile("nama_10_nfa_st"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') AND geo='$(geo)' AND time IN ($(years_str)) AND nace_r2 NOT IN ('T', 'U', 'L68A') AND asset10='N111N' AND unit='CRC_MEUR' ORDER BY time, nace_r2"
        calibration_data["dwellings"]=execute(conn,sqlquery,(number_sectors,number_years));
    else
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
    end

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

    # sqlquery="SELECT value FROM '$(pqfile("gov_10q_ggnfa"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='B9' AND s_adj='NSA' ORDER BY time"
    # calibration_data["government_deficit_quarterly"]=execute(conn,sqlquery);

    # sqlquery="SELECT value FROM '$(pqfile("nasq_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='CP_MEUR' AND sector='S13' AND na_item='B9' AND direct='PAID' AND s_adj='NSA' ORDER BY time"
    # calibration_data["government_deficit_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector='S14_S15' AND na_item='D4' AND direct='RECV' ORDER BY time"
    calibration_data["property_income"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector='S14_S15' AND na_item='B2A3N' AND direct='RECV' ORDER BY time"
    calibration_data["mixed_income"]=execute(conn,sqlquery);

    sqlquery="SELECT sum(value) FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector IN ('S11','S12') AND na_item='D41' AND direct='PAID' GROUP BY time ORDER BY time"
    calibration_data["firm_interest"]=execute(conn,sqlquery);

    # sqlquery="SELECT sum(value) FROM '$(pqfile("nasq_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='CP_MEUR' AND sector IN ('S11','S12') AND na_item='D41' AND direct='PAID' AND s_adj='NSA' GROUP BY time ORDER BY time"
    # calibration_data["firm_interest_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT sum(value) FROM '$(pqfile("nasa_10_nf_tr"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='CP_MEUR' AND sector IN ('S11','S12') AND na_item='D51' AND direct='PAID' GROUP BY time ORDER BY time"
    calibration_data["corporate_tax"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D91REC' ORDER BY time"
    calibration_data["capital_taxes"]=execute(conn,sqlquery);

    # sqlquery="SELECT value FROM '$(pqfile("gov_10q_ggnfa"))' WHERE geo='$(geo)' AND time IN ($(quarters_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D41PAY' AND s_adj='NSA' ORDER BY time"
    # calibration_data["interest_government_debt_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("gov_10a_main"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='MIO_EUR' AND sector='S13' AND na_item='D41PAY' ORDER BY time"
    calibration_data["interest_government_debt"]=execute(conn,sqlquery);

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

    # sqlquery="SELECT value FROM '$(pqfile("cens_11an_r2"))' WHERE geo='$(geo)' AND wstatus='UNE' AND sex='T' AND nace_r2='TOTAL' AND unit='NR' AND age='TOTAL'"
    # calibration_data["unemployed"]=execute(conn,sqlquery);

    # sqlquery="SELECT sum(value) FROM '$(pqfile("cens_11an_r2"))' WHERE geo='$(geo)' AND wstatus='INAC' AND sex='T' AND nace_r2='TOTAL' AND unit='NR' AND age='TOTAL'"
    # calibration_data["inactive"]=execute(conn,sqlquery);

    sqlquery="SELECT 1e3*value FROM '$(pqfile("nama_10_pe"))' WHERE geo='$(geo)' AND time IN ($(years_str)) AND unit='THS_PER' AND na_item='POP_NC' ORDER BY time"
    calibration_data["population"]=execute(conn,sqlquery);

    # [calibration_data["firms"],calibration_data["employees"]]=number_of_agents(conn,geo,start_calibration_year:end_calibration_year);

    sqlquery="SELECT 1e3*value FROM '$(pqfile("nama_10_a64_e"))' WHERE nace_r2 IN (SELECT nace FROM '$(pqfile("nace64"))') and time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('T', 'U', 'L68A') AND unit = 'THS_PER' AND na_item='SAL_DC' ORDER BY time, nace_r2"
    calibration_data["employees"]=execute(conn,sqlquery);
    calibration_data["employees"]=reshape(calibration_data["employees"],(number_sectors,Int64(length(calibration_data["employees"])/number_sectors)));
    calibration_data["employees"]=coalesce.(calibration_data["employees"],100);

    ## TODO fix: sbs_na_sca_a64 file not found
    # sqlquery="SELECT value FROM (" *
    #           "SELECT time, nace_r2, value AS value FROM '$(pqfile("sbs_na_sca_a64"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND indic_sb='V11110' AND nace_r2 NOT IN ('K64','K65','K66','P','Q86','Q87_Q88','R90-R92','R93','S94','S95','S96') " *
    #           "UNION " *
    #           "SELECT time, nace_r2, value AS value FROM '$(pqfile("bd_9ac_l_form_a64"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND indic_sb='V11910' AND leg_form='TOTAL' AND nace_r2 IN ('K64','K65','K66','P','Q86','Q87_Q88','R90-R92','R93','S94','S95','S96')" *
    #           ") foo ORDER BY time, nace_r2"
    # #     'UNION " ...
    # #     'SELECT time, nace_r2, 1e2*value AS value FROM '$(pqfile("nama_10_a64_e"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND unit = 'THS_PER' AND na_item='SAL_DC' AND nace_r2 IN ('A01','A02','A03','O') " ...
    # calibration_data["firms"]=execute(conn,sqlquery);
    # calibration_data["firms"]=reshape(calibration_data["firms"],62,length(calibration_data["firms"])/62);
    # calibration_data["firms"](isnan(calibration_data["firms"]))=round(calibration_data["employees"](isnan(calibration_data["firms"]))/10);

    if geo=="BE"
        calibration_data["firms"][10,6]=10;
    end

    if geo=="EE"
        calibration_data["capital_taxes"]=zeros(size(calibration_data["corporate_tax"]));
    elseif geo=="SE"
        calibration_data["capital_taxes"][6:7]=0;
    end

    # ## TODO fix: load needed figaro data and check 'squeeze' function in Matlab
    # load("../data/020_figaro_data/',char(geos(g)),'.mat'],'figaro');
    # # output=squeeze(sum(figaro.intermediate_consumption))+figaro.taxes_products+figaro.taxes_production+figaro.compensation_employees+figaro.operating_surplus;#+capital_consumption;
    # if geo in ["FR", "IE", "LT", "LU", "MT", "PL", "SE"]
    #     calibration_data["capital_consumption"]=calibration_data["capital_consumption"]'.*(calibration_data["nace64_capital_consumption_eu20"]./calibration_data["nominal_nace64_output_eu20"].*output)./sum(calibration_data["nace64_capital_consumption_eu20"]./calibration_data["nominal_nace64_output_eu20"].*output);
    # else
    #     calibration_data["capital_consumption"]=calibration_data["nace64_capital_consumption"]./calibration_data["nominal_nace64_output"].*output;
    # end

    # if !(geo in ["AT", "CZ", "EL", "FI", "LV", "SK"])  # OR [2025-05-06 Di]: removed DK
    #     fixed_assets_other_than_dwellings=(calibration_data["fixed_assets"]-calibration_data["dwellings"])'.*((calibration_data["fixed_assets_eu7"]-calibration_data["dwellings_eu7"])./calibration_data["nominal_nace64_output_eu7"].*output)./sum((calibration_data["fixed_assets_eu7"]-calibration_data["dwellings_eu7"])./calibration_data["nominal_nace64_output_eu7"].*output);
    #     dwellings=zeros(size(fixed_assets_other_than_dwellings));
    #     dwellings[44,:]=calibration_data["dwellings"];
    #     calibration_data["fixed_assets"]=fixed_assets_other_than_dwellings+dwellings;
    #     calibration_data["dwellings"]=dwellings;
    # end

    # save("../data/040_calibration_input_data/', char(geos(g)),'.mat'],'calibration_data');
    # clear calibration_data;
    return calibration_data
end
