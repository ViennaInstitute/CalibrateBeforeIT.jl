
# cd(@__DIR__)

# using CSV, DataFrames, Downloads

# save_path = "../data/010_eurostat_tables"
# table_id = "nace64"
# csv_filename = joinpath(save_path, "$(table_id).csv")
# nace64_table = CSV.read(csv_filename, DataFrame;
#                         delim = ",")

# write_table(joinpath(save_path, "$(table_id).parquet"),
#             nace64_table, format = :parquet)
