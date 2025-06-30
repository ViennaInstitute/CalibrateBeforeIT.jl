# module Import_Data

function import_data(geo, start_year, end_year)
    conn = DBInterface.connect(DuckDB.DB)
    all_years = collect(start_year:end_year)

    number_years = end_year - start_year + 1;
    number_quarters = number_years * 4;
    years_str = join(["'$(year)'" for year in all_years], ", ")
    ## Create a year-quarter vector and string for creating a data.frame and for SQL
    ## queries
    all_quarters = ["Q1", "Q2", "Q3", "Q4"]
    quarters_vec = ["$(year)-$(quarter)" for year in all_years for quarter in all_quarters]
    quarters_str = join(["'$(yearquarter)'" for yearquarter in quarters_vec], ",")

    data = Dict()

    ## Set these date numbers
    data["years_num"] = date2num_yearly(start_year:end_year)
    data["quarters_num"] = date2num_quarterly(start_year:end_year)


    ## GDP time series
    ##
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_gdp"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_gdp"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='PD10_EUR' ORDER BY time"
    data["gdp_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_gdp_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["gdp_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_gdp_growth"]=data["real_gdp_growth"]+data["gdp_deflator_growth"];

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_gdp_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_gdp_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["gdp_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_gdp_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1GQ' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["gdp_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_gdp_growth_quarterly"]=data["real_gdp_growth_quarterly"]+data["gdp_deflator_growth_quarterly"];


    ## GVA time series
    ##
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_gva"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_gva"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='PD10_EUR' ORDER BY time"
    data["gva_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_gva_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["gva_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_gva_growth"]=data["real_gva_growth"]+data["gva_deflator_growth"];

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_gva_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_gva_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["gva_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_gva_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B1G' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["gva_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_gva_growth_quarterly"]=data["real_gva_growth_quarterly"]+data["gva_deflator_growth_quarterly"];


    ## Household Consumption Time Series

    # Annual

    #Take from '$(pqfile("IO"))' tables to get correct number
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_household_consumption"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_household_consumption"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='PD10_EUR' ORDER BY time"
    data["household_consumption_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_household_consumption_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["household_consumption_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_household_consumption_growth"]=data["real_household_consumption_growth"]+data["household_consumption_deflator_growth"];

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_household_consumption_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_household_consumption_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["household_consumption_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_household_consumption_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P31_S14_S15' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["household_consumption_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_household_consumption_growth_quarterly"]=data["real_household_consumption_growth_quarterly"]+data["household_consumption_deflator_growth_quarterly"];


    ## Government Consumption Time Series

    # Annual

    #Take from '$(pqfile("IO"))' tables to get correct number
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_government_consumption"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_government_consumption"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='PD10_EUR' ORDER BY time"
    data["government_consumption_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_government_consumption_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["government_consumption_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_government_consumption_growth"]=data["real_government_consumption_growth"]+data["government_consumption_deflator_growth"];

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_government_consumption_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_government_consumption_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["government_consumption_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_government_consumption_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3_S13' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["government_consumption_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_government_consumption_growth_quarterly"]=data["real_government_consumption_growth_quarterly"]+data["government_consumption_deflator_growth_quarterly"];


    ## Final Consumption Time Series

    # Annual

    #Take from '$(pqfile("IO"))' tables to get correct number
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_final_consumption"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_final_consumption"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3' AND unit='PD10_EUR' ORDER BY time"
    data["final_consumption_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_final_consumption_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P3' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["final_consumption_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_final_consumption_growth"]=data["real_final_consumption_growth"]+data["final_consumption_deflator_growth"];

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_final_consumption_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_final_consumption_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["final_consumption_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_final_consumption_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P3' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["final_consumption_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_final_consumption_growth_quarterly"]=data["real_final_consumption_growth_quarterly"]+data["final_consumption_deflator_growth_quarterly"];


    ## Capital Formation Time Series


    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_capitalformation"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_capitalformation"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='PD10_EUR' ORDER BY time"
    data["capitalformation_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_capitalformation_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["capitalformation_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_capitalformation_growth"]=data["real_capitalformation_growth"]+data["capitalformation_deflator_growth"];

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_capitalformation_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_capitalformation_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["capitalformation_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_capitalformation_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P5G' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["capitalformation_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_capitalformation_growth_quarterly"]=data["real_capitalformation_growth_quarterly"]+data["capitalformation_deflator_growth_quarterly"];


    ## Fixed Capital Formation Time Series


    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_fixed_capitalformation"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_fixed_capitalformation"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='PD10_EUR' ORDER BY time"
    data["fixed_capitalformation_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_fixed_capitalformation_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["fixed_capitalformation_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_fixed_capitalformation_growth"]=data["real_fixed_capitalformation_growth"]+data["fixed_capitalformation_deflator_growth"];

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_fixed_capitalformation_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_fixed_capitalformation_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["fixed_capitalformation_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_fixed_capitalformation_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P51G' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["fixed_capitalformation_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_fixed_capitalformation_growth_quarterly"]=data["real_fixed_capitalformation_growth_quarterly"]+data["fixed_capitalformation_deflator_growth_quarterly"]


    ## Exports Time Series - Take from '$(pqfile("IO"))' tables, all growth and deflator rates from nama_10_gdp, calculate quarterly exports then!

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P6' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_exports"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P6' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_exports"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P6' AND unit='PD10_EUR' ORDER BY time"
    data["exports_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P6' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_exports_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P6' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["exports_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_exports_growth"]=data["real_exports_growth"]+data["exports_deflator_growth"];

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P6' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_exports_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P6' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_exports_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P6' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["exports_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P6' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_exports_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P6' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["exports_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_exports_growth_quarterly"]=data["real_exports_growth_quarterly"]+data["exports_deflator_growth_quarterly"];


    ## Imports Time Series


    # Annual
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P7' AND unit='CP_MEUR' ORDER BY time"
    data["nominal_imports"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P7' AND unit='CLV10_MEUR' ORDER BY time"
    data["real_imports"] = execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P7' AND unit='PD10_EUR' ORDER BY time"
    data["imports_deflator"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P7' AND unit='CLV_PCH_PRE' ORDER BY time"
    data["real_imports_growth"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='P7' AND unit='PD_PCH_PRE_EUR' ORDER BY time"
    data["imports_deflator_growth"]=0.01*execute(conn,sqlquery);

    data["nominal_imports_growth"]=data["real_imports_growth"]+data["imports_deflator_growth"];

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P7' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["nominal_imports_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P7' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY time"
    data["real_imports_quarterly"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P7' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY time"
    data["imports_deflator_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P7' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY time"
    data["real_imports_growth_quarterly"]=0.01*execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='P7' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY time"
    data["imports_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);

    data["nominal_imports_growth_quarterly"]=data["real_imports_growth_quarterly"]+data["imports_deflator_growth_quarterly"];


    ## 3-month euribor
    sqlquery="SELECT value FROM '$(pqfile("irt_st_q"))' WHERE time IN ($(quarters_str)) AND geo='EA' AND int_rt='IRT_M3' ORDER BY time"
    data["euribor"]=0.01*execute(conn,sqlquery);

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("irt_st_a"))' WHERE time IN ($(years_str)) AND geo='EA' AND int_rt='IRT_M3' ORDER BY time"
    data["euribor_yearly"]=0.01*execute(conn,sqlquery);


    ## Compensation of employees

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='D1' AND unit='CP_MEUR' ORDER BY time"
    data["compensation_employees"] = execute(conn,sqlquery);

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='D1' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["compensation_employees_quarterly"]=execute(conn,sqlquery);


    ## Wages

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='D11' AND unit='CP_MEUR' ORDER BY time"
    data["wages"] = execute(conn,sqlquery);

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='D11' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["wages_quarterly"]=execute(conn,sqlquery);


    ## Operating surplus

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("nama_10_gdp"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='B2A3G' AND unit='CP_MEUR' ORDER BY time"
    data["operating_surplus"] = execute(conn,sqlquery);

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_gdp"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='B2A3G' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY time"
    data["operating_surplus_quarterly"]=execute(conn,sqlquery);


    ## Employed

    # Annual
    sqlquery="SELECT value FROM '$(pqfile("nama_10_pe"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND na_item='EMP_DC' AND unit='THS_PER' ORDER BY time"
    data["employed"] = execute(conn,sqlquery);

    # Quarterly
    sqlquery="SELECT value FROM '$(pqfile("namq_10_pe"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND na_item='EMP_DC' AND unit='THS_PER' AND s_adj='SCA' ORDER BY time"
    data["employed_quarterly"]=execute(conn,sqlquery);


    if geo != "EA19"
        ## Unemployment rate
        # sqlquery="SELECT value FROM '$(pqfile("une_rt_q_h"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND unit='PC_ACT' AND age='Y15-74' AND s_adj='SA' AND sex='T' ORDER BY time"
        sqlquery="SELECT value FROM '$(pqfile("une_rt_q"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND unit='PC_ACT' AND age='Y15-74' AND s_adj='SA' AND sex='T' ORDER BY time"
        data["unemployment_rate_quarterly"]=0.01*execute(conn,sqlquery);

        # Annual
        # sqlquery="SELECT value FROM '$(pqfile("une_rt_a_h"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND unit='PC_ACT' AND age='Y15-74' AND sex='T' ORDER BY time"
        sqlquery="SELECT value FROM '$(pqfile("une_rt_a"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND unit='PC_ACT' AND age='Y15-74' AND sex='T' ORDER BY time"
        data["unemployment_rate"]=0.01*execute(conn,sqlquery);

        # Pad the unemployment rate series to the number of years/quarters
        data["unemployment_rate"]=[repeat([missing], (number_years - length(data["unemployment_rate"])))...,
                                   data["unemployment_rate"]...];
        data["unemployment_rate_quarterly"]=[repeat([missing], (number_quarters - length(data["unemployment_rate_quarterly"])))...,
                                             data["unemployment_rate_quarterly"]...];
    end


    ## Sectoral GVA

    sqlquery="SELECT value FROM '$(pqfile("nama_10_a10"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND unit = 'CP_MEUR' AND na_item='B1G' order by nace_r2, time"
    data["nominal_nace10_gva"]=execute(conn,sqlquery)
    data["nominal_nace10_gva"]=reshape(data["nominal_nace10_gva"], (Int64(length(data["nominal_nace10_gva"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("nama_10_a10"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND unit = 'CLV10_MEUR' AND na_item='B1G' order by nace_r2, time"
    data["real_nace10_gva"]=execute(conn,sqlquery);
    data["real_nace10_gva"]=reshape(data["real_nace10_gva"], (Int64(length(data["real_nace10_gva"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("nama_10_a10"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND unit = 'PD10_EUR' AND na_item='B1G' order by nace_r2, time"
    data["nace10_gva_deflator"]=0.01*execute(conn,sqlquery);
    data["nace10_gva_deflator"]=reshape(data["nace10_gva_deflator"],(Int64(length(data["nace10_gva_deflator"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("nama_10_a10"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND unit = 'CLV_PCH_PRE' AND na_item='B1G' order by nace_r2, time"
    data["real_nace10_gva_growth"]=0.01*execute(conn,sqlquery);
    data["real_nace10_gva_growth"]=reshape(data["real_nace10_gva_growth"], (Int64(length(data["real_nace10_gva_growth"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("nama_10_a10"))' WHERE time IN ($(years_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND unit = 'PD_PCH_PRE_EUR' AND na_item='B1G' order by nace_r2, time"
    data["nace10_gva_deflator_growth"]=0.01*execute(conn,sqlquery);
    data["nace10_gva_deflator_growth"]=reshape(data["nace10_gva_deflator_growth"],(Int64(length(data["nace10_gva_deflator_growth"])/10),10));

    data["nominal_nace10_gva_growth"]=data["real_nace10_gva_growth"] .+ data["gva_deflator_growth"];

    sqlquery="SELECT value FROM '$(pqfile("namq_10_a10"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND na_item='B1G' AND unit='CP_MEUR' AND s_adj='SCA' ORDER BY nace_r2, time"
    data["nominal_nace10_gva_quarterly"]=execute(conn,sqlquery);
    data["nominal_nace10_gva_quarterly"]=reshape(data["nominal_nace10_gva_quarterly"],(Int64(length(data["nominal_nace10_gva_quarterly"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("namq_10_a10"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND na_item='B1G' AND unit='CLV10_MEUR' AND s_adj='SCA' ORDER BY nace_r2, time"
    data["real_nace10_gva_quarterly"]=execute(conn,sqlquery);
    data["real_nace10_gva_quarterly"]=reshape(data["real_nace10_gva_quarterly"],(Int64(length(data["real_nace10_gva_quarterly"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("namq_10_a10"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND na_item='B1G' AND unit='PD10_EUR' AND s_adj='SCA' ORDER BY nace_r2, time"
    data["nace10_gva_deflator_quarterly"]=0.01*execute(conn,sqlquery);
    data["nace10_gva_deflator_quarterly"]=reshape(data["nace10_gva_deflator_quarterly"],(Int64(length(data["nace10_gva_deflator_quarterly"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("namq_10_a10"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND na_item='B1G' AND unit='CLV_PCH_PRE' AND s_adj='SCA' ORDER BY nace_r2, time"
    data["real_nace10_gva_growth_quarterly"]=0.01*execute(conn,sqlquery);
    data["real_nace10_gva_growth_quarterly"]=reshape(data["real_nace10_gva_growth_quarterly"],(Int64(length(data["real_nace10_gva_growth_quarterly"])/10),10));

    sqlquery="SELECT value FROM '$(pqfile("namq_10_a10"))' WHERE time IN ($(quarters_str)) AND geo='$(geo)' AND nace_r2 NOT IN ('C', 'TOTAL') AND na_item='B1G' AND unit='PD_PCH_PRE_EUR' AND s_adj='SCA' ORDER BY nace_r2, time"
    data["nace10_gva_deflator_growth_quarterly"]=0.01*execute(conn,sqlquery);
    data["nace10_gva_deflator_growth_quarterly"]=reshape(data["nace10_gva_deflator_growth_quarterly"],(Int64(length(data["nace10_gva_deflator_growth_quarterly"])/10),10));

    data["nominal_nace10_gva_growth_quarterly"]=data["real_nace10_gva_growth_quarterly"]+data["nace10_gva_deflator_growth_quarterly"];

    ##
    ## Adjust length of time series
    ##

    # #     TODO
    # fields=fieldnames(data);
    # for l=1:numel(fields)
    #     eval(['p=length(data.',fields{l},');']);
    #     if p == length(data.quarters_num)-1 || p == length(data.years_num)-1
    #         eval(['data.',fields{l},'=[NaN*zeros(1,size(data.',fields{l},',2));data.',fields{l},"']);
    #     end
    # end
    for (key, value) in data
        data_length = length(value)
        if (data_length == length(data["years_num"]) - 1) || (data_length == length(data["quarters_num"]) - 1)
            println(key, ": ", length(value))
            error("Series $(key) has length $(length(value)) which is one too short!")
        end
    end

    return data
end

# end
