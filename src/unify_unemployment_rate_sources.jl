using DuckDB

function unify_unemployment_rate_sources(table_id::String,
    conn)

    table_id_new_data = table_id
    table_id_old_data = "$(table_id)_h"

    # For the new data source (e.g., une_rt_a), keep only non-missing entries
    sqlquery = """
    COPY (
    SELECT * FROM '$(pqfile(table_id_new_data))'
    WHERE value IS NOT NULL
    )
    TO '$(pqfile(table_id_new_data))' (FORMAT parquet)
    """
    DBInterface.execute(conn,sqlquery);

    if table_id_new_data == "une_rt_a"
        quarterly_adjustment = ""
    else
        quarterly_adjustment = " AND table1.s_adj = table2.s_adj"
    end

    # Append the "historical" data (with id ending in "_h") to the "current"
    # time series to have one long-running time series:
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

# execute(conn, "SELECT COUNT(*) FROM '$(pqfile("une_rt_a"))'")
# execute(conn, "SELECT COUNT(*) FROM '$(pqfile("une_rt_a_h"))'")

# execute(conn, "SELECT COUNT(*) FROM '$(pqfile("une_rt_q"))'")
# execute(conn, "SELECT COUNT(*) FROM '$(pqfile("une_rt_q_h"))'")
