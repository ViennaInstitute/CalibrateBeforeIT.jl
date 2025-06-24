
cd(@__DIR__)

# using Pkg
# Pkg.activate(Base.current_project())

using CSV, DataFrames, Downloads, QuackIO

save_path = "../data/010_eurostat_tables"

tbl1 = read_parquet(DataFrame, joinpath(save_path, "naio_10_fcp_ii1.parquet"))
tbl2 = read_parquet(DataFrame, joinpath(save_path, "naio_10_fcp_ii2.parquet"))
tbl3 = read_parquet(DataFrame, joinpath(save_path, "naio_10_fcp_ii3.parquet"))

tbl = reduce(vcat, [tbl1, tbl2, tbl3])

write_table(joinpath(save_path, "naio_10_fcp_ii.parquet"),
            tbl, format = :parquet)
