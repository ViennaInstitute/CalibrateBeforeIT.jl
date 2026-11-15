function unify_unemployment_rate_sources(table_id::String,
    conn)

    table_id_new_data = table_id
    table_id_old_data = "$(table_id)_h"

    if table_id_new_data == "une_rt_a"
        quarterly_adjustment = ""
    else
        quarterly_adjustment = " AND table1.s_adj = table2.s_adj"
    end

    # Append the "historical" data (with id ending in "_h") to the "current"
    # time series to have one long-running time series. Only non-missing
    # entries of both sources are kept:
    sqlquery = """
    COPY (
    WITH table1 as (
    SELECT * FROM '$(pqfile(table_id_new_data))'
    WHERE value IS NOT NULL
    ), table2 as (
    SELECT * FROM '$(pqfile(table_id_old_data))'
    WHERE value IS NOT NULL
    )
    SELECT * FROM table1
    UNION
    SELECT * FROM table2
    WHERE NOT EXISTS (
    SELECT 1
    FROM table1
    WHERE table1.time = table2.time
    AND table1.freq = table2.freq
    AND table1.age = table2.age
    AND table1.unit = table2.unit
    AND table1.sex = table2.sex
    AND table1.geo = table2.geo
    $(quarterly_adjustment)
    ))
    TO '$(pqfile(table_id_new_data))' (FORMAT parquet)
    """
    res_query = DBInterface.execute(conn,sqlquery);

    return res_query
end
