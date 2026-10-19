## SQL subquery selecting the FIGARO codes of the NACE64 industries in the model
nace64_figaro_sql() =
    "(SELECT nace_figaro FROM '$(pqfile("nace64"))' WHERE nace NOT IN ($(sql_list(EXCLUDED_NACE64))))"

function get_minmax_calibration_years(geo)
    conn = DBInterface.connect(DuckDB.DB)
    sectors = nace64_figaro_sql()
    filters = "c_dest='$(geo)' AND ind_ava IN $(sectors) AND ind_use IN $(sectors)"
    min_year = parse(Int64, execute(conn, "SELECT min(time) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE $(filters)")[1])
    max_year = parse(Int64, execute(conn, "SELECT max(time) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE $(filters)")[1])
    return (min_year, max_year)
end

function import_figaro_data(geo,
                            start_calibration_year, end_calibration_year,
                            number_sectors)
    conn = DBInterface.connect(DuckDB.DB)
    number_years = end_calibration_year - start_calibration_year + 1;
    years_str = sql_list(start_calibration_year:end_calibration_year)
    sectors = nace64_figaro_sql()

    ## Sum of the FIGARO values matching `filters`, grouped and ordered by `by`
    figaro_sum(filters, by) = "SELECT sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE $(filters) AND time IN ($(years_str)) GROUP BY $(by) ORDER BY $(by)"
    ## Sector x year matrices by product (ind_ava) or by industry (ind_use)
    by_product(filters) = execute(conn, figaro_sum("$(filters) AND ind_ava IN $(sectors)", "time, ind_ava"),
                                  (number_sectors, number_years))
    by_industry(filters) = execute(conn, figaro_sum("$(filters) AND ind_use IN $(sectors)", "time, ind_use"),
                                   (number_sectors, number_years))

    figaro = Dict()

    ## For debugging: Add more columns to the SELECT statment, parse everything
    ## to a DataFrame to be able to query and inspect that dataframe:
    # sqlquery="SELECT time, ind_use, ind_ava, sum(value) FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_ava IN $(sectors) AND ind_use IN $(sectors) AND time IN ($(years_str)) GROUP BY time, ind_use, ind_ava ORDER BY time, ind_use, ind_ava";
    # res = DataFrame(columntable(DBInterface.execute(conn, sqlquery)))
    # combine(groupby(res, [:time]), nrow => :count) ## number of rows by 'time'

    figaro["intermediate_consumption"]=execute(conn,
        figaro_sum("c_dest='$(geo)' AND ind_ava IN $(sectors) AND ind_use IN $(sectors)", "time, ind_use, ind_ava"),
        (number_sectors, number_sectors, number_years));

    figaro["household_consumption"]=by_product("c_dest='$(geo)' AND ind_use IN ('P3_S14', 'P3_S15')");
    figaro["fixed_capitalformation"]=by_product("c_dest='$(geo)' AND ind_use='P51G'");
    figaro["inventory_changes"]=by_product("c_dest='$(geo)' AND ind_use='P5M'");
    figaro["capitalformation"]=figaro["fixed_capitalformation"]+figaro["inventory_changes"];
    figaro["imports"]=by_product("c_orig!='$(geo)' AND c_dest='$(geo)'");
    figaro["exports"]=by_product("c_orig='$(geo)' AND c_dest!='$(geo)'");
    figaro["government_consumption"]=by_product("c_dest='$(geo)' AND ind_use='P3_S13'");

    figaro["operating_surplus"]=by_industry("c_dest='$(geo)' AND ind_ava='B2A3G'");
    figaro["compensation_employees"]=by_industry("c_dest='$(geo)' AND ind_ava='D1'");
    figaro["taxes_production"]=by_industry("c_dest='$(geo)' AND ind_ava='D29X39'");
    figaro["taxes_products"]=by_industry("c_dest='$(geo)' AND ind_ava='D21X31'");

    figaro["taxes_products_household"]=execute(conn,
        figaro_sum("c_dest='$(geo)' AND ind_use IN ('P3_S14','P3_S15') AND ind_ava='D21X31'", "time"));

    # Household consumption TOTAL at purchaser prices (sum of all ind_ava)
    # This equals ESA P3 and should be used for psi calculation
    # Includes NACE sectors + D21X31 (taxes) + OP_RES + OP_NRES (negative adjustments)
    figaro["household_consumption_total"]=execute(conn,
        figaro_sum("c_dest='$(geo)' AND ind_use IN ('P3_S14','P3_S15')", "time"));

    figaro["taxes_products_capitalformation"]=execute(conn,
        figaro_sum("c_dest='$(geo)' AND ind_use IN ('P51G','P5M') AND ind_ava='D21X31'", "time"));

    sqlquery="SELECT value FROM '$(pqfile("naio_10_fcp_ii"))' WHERE c_dest='$(geo)' AND ind_use='P3_S13' AND ind_ava='D21X31' AND time IN ($(years_str)) ORDER BY time";
    figaro["taxes_products_government"]=execute(conn,sqlquery);

    if geo=="IE"
        figaro["compensation_employees"][3,:]=linear_interp_extrap(3:number_years,figaro["compensation_employees"][3,3:number_years],1:number_years);
        figaro["compensation_employees"][57,:]=linear_interp_extrap(3:number_years,figaro["compensation_employees"][57,3:number_years],1:number_years);
    end
    return figaro
end
