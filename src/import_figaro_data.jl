# module Import_Figaro_Data
# export import_figaro_data
# using DuckDB
# using DataFrames
# using Tables
# using ..Utils


function import_figaro_data(geo,
                            start_calibration_year, end_calibration_year,
                            number_sectors, number_years)
    conn = DBInterface.connect(DuckDB.DB)
    all_years = collect(start_calibration_year:end_calibration_year)
    years_str = join(["'$(year)'" for year in all_years], ", ")

    figaro = Dict()

    # % time series
    # sqlquery=['SELECT sum(value) FROM naio_10_fcp_ii1 WHERE c_dest=',geo,' AND ind_ava IN (SELECT nace_figaro FROM nace64 WHERE nace NOT IN (''L68A'', ''T'', ''U'')) AND ind_use IN (SELECT nace_figaro FROM nace64 WHERE nace NOT IN (''L68A'', ''T'', ''U'')) AND time IN (',years_str,') GROUP BY time, ind_use, ind_ava ORDER BY time, ind_use, ind_ava'];
    # figaro.intermediate_consumption=fetch(conn,sqlquery,'DataReturnFormat','numeric');
    # figaro.intermediate_consumption=reshape(figaro.intermediate_consumption,number_sectors,number_sectors,number_years);

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND ind_use IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_use, ind_ava ORDER BY time, ind_use, ind_ava";
    figaro["intermediate_consumption"]=execute(conn,sqlquery, (number_sectors, number_sectors, number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND ind_use IN ('P3_S14', 'P3_S15') AND time IN ($(years_str)) GROUP BY time, ind_ava ORDER BY time, ind_ava";
    figaro["household_consumption"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND ind_use='P51G' AND time IN ($(years_str)) GROUP BY time, ind_ava ORDER BY time, ind_ava";
    figaro["fixed_capitalformation"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND ind_use='P5M' AND time IN ($(years_str)) GROUP BY time, ind_ava ORDER BY time, ind_ava";
    figaro["inventory_changes"]=execute(conn,sqlquery, (number_sectors,number_years));

    figaro["capitalformation"]=figaro["fixed_capitalformation"]+figaro["inventory_changes"];

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_orig!='$(geo)' and c_dest='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_ava ORDER BY time, ind_ava";
    figaro["imports"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_orig='$(geo)' and c_dest!='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_ava ORDER BY time, ind_ava";
    figaro["exports"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND ind_use='P3_S13' AND time IN ($(years_str)) GROUP BY time, ind_ava ORDER BY time, ind_ava";
    figaro["government_consumption"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava='B2A3G' AND ind_use IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_use ORDER BY time, ind_use";
    figaro["operating_surplus"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava='D1' AND ind_use IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_use ORDER BY time, ind_use";
    figaro["compensation_employees"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava='D29X39' AND ind_use IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_use ORDER BY time, ind_use";
    figaro["taxes_production"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava='D21X31' AND ind_use IN (SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ('L68A', 'T', 'U')) AND time IN ($(years_str)) GROUP BY time, ind_use ORDER BY time, ind_use";
    figaro["taxes_products"]=execute(conn,sqlquery, (number_sectors,number_years));

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_use IN ('P3_S14','P3_S15') AND ind_ava='D21X31' AND time IN ($(years_str)) GROUP BY time ORDER BY time";
    figaro["taxes_products_household"]=execute(conn,sqlquery);

    sqlquery="SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_use IN ('P51G','P5M') AND ind_ava='D21X31' AND time IN ($(years_str)) GROUP BY time ORDER BY time";
    figaro["taxes_products_capitalformation"]=execute(conn,sqlquery);

    sqlquery="SELECT value FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_use='P3_S13' AND ind_ava='D21X31' AND time IN ($(years_str)) ORDER BY time";
    figaro["taxes_products_government"]=execute(conn,sqlquery);

    # figaro["taxes_products_export"]=zeros(size(figaro["taxes_products_household"]));

    # figaro["wages"]=0.825*figaro["compensation_employees"];

    # figaro["capital_consumption"]=0.1*(squeeze(sum(figaro["intermediate_consumption"],1))+figaro["operating_surplus"]+figaro["compensation_employees"]+figaro["taxes_production"]+figaro["taxes_products"]);

    # figaro["operating_surplus"]=figaro["operating_surplus"]-figaro["capital_consumption"];

    if geo=="IE"
        figaro["compensation_employees"][3,:]=linear_interp_extrap(3:number_years,figaro["compensation_employees"][3,3:number_years],1:number_years);
        figaro["compensation_employees"][57,:]=linear_interp_extrap(3:number_years,figaro["compensation_employees"][57,3:number_years],1:number_years);
        # figaro["compensation_employees"][10,:]=linear_interp_extrap([1 2 3 4 6 7],figaro["compensation_employees"][10,[1 2 3 4 6 7]],1:7);
    end
    return figaro
end


# end
