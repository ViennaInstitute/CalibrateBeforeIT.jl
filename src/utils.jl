module Utils

export pqfile, execute

using DuckDB, Tables

function pqfile(table_id)
    joinpath(save_path, "$(table_id).parquet")
end

function execute(conn, query)
    res = values(columntable(DBInterface.execute(conn, query)))[1]
    return res
end

function execute(conn, query, dims)
    raw_res = execute(conn, query)
    res = reshape(raw_res, dims)
    return res
end


end
