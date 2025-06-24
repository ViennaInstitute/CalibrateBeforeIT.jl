
# cd(@__DIR__)

# using Pkg
# Pkg.activate(Base.current_project())

# using QuackIO

# save_path = "../data/010_eurostat_tables"
# # all_eurostat_table_ids = readlines("001_table.txt");
# # table_id = "namq_10_pe"
# # parquet_file = joinpath(save_path, "$(table_id).parquet")

## Compute some more parameters based on the above parameters
##------------------------------------------------------------
# sqlquery="SELECT value FROM '$(parquet_file)' WHERE time IN ($(years_str)) AND geo='EA' AND int_rt='IRT_M3' ORDER BY time";
# # sqlquery = "SELECT value FROM irt_st_a WHERE time IN ($(years_str)) AND country='EA' AND na_item='B1GQ' AND unit       ='CP_MEUR' AND s_adj   ='SCA' ORDER BY time"
