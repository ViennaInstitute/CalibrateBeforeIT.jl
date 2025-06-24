module Import_Eurostat

export remove_flags_parse_to_float, transform_columns, download_to_parquet

using CSV, DataFrames, Downloads, QuackIO

# Function to process each element, remove Eurostat flags and try to convert it
# to Float64
function remove_flags_parse_to_float(value)
    if ismissing(value)
        return missing
    elseif isnothing(value)
        return missing
    else
        # Remove all non-digit characters and convert to Float64
        # filtered_value = replace(value, r"[^0-9|.]" => "")
        # filtered_value = replace(value, r" +[a-z]+" => "")
        filtered_value = replace(value, r":? +[^0-9|.]+$" => "")
        filtered_value = replace(filtered_value, r" +" => "")
        parsed_value = tryparse(Float64, filtered_value)
        return isnothing(parsed_value) ? missing : parsed_value
    end
end
# remove_flags_parse_to_float("13.344 b")

# Function to apply to each column of the DataFrame
function transform_columns(df, cols)
    for col in cols
        ## TODO would probably to be safer to apply to all columns inheriting
        ## from String or so
        if eltype(df[!, col]) <: Union{Missing, AbstractString}
            transform!(df, col => ByRow(remove_flags_parse_to_float) => col)
        end
    end
    return df
end

# table_id = "irt_st_a"
# save_path = "../data/010_eurostat_tables"
function download_to_parquet(table_id, save_path)
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/$(table_id)/?format=TSV&compressed=false"
    tsv_filename = joinpath(save_path, "$(table_id).tsv")
    http_response = Downloads.download(url, tsv_filename)

    raw_table = CSV.read(tsv_filename, DataFrame;
                         delim = "\t",
                         missingstring = ["", ":", ": ", ": b", ": bc",
                                          ": c", ": cd", ": d", ": e",
                                          ": n", ": m", ": p", ": z"])
    # show(describe(raw_table, :eltype); allrows = true)

    ## delete extraoneaus whitespace at the end
    rename!(x -> replace(x, r" +$" => ""), raw_table)

    ## The year columns are all columns except the very first
    year_cols = names(raw_table)[2:end]

    ## Apply the cleaning function to all year columns
    cleaned_table = transform_columns(raw_table, year_cols)
    cleaned_table = identity.(cleaned_table)
    # show(describe(cleaned_table, :eltype); allrows = true)
    # cleaned_table[isnothing.(cleaned_table[!, "2020"]), "2020"]

    # x = cleaned_table[!, "2014-Q1"]
    # # cleaned_table[isnothing.(x), "2014-Q1"]
    # raw_table[isnothing.(x), "2014-Q1"]

    ## Split the first column into multiple columns
    col1 = names(raw_table)[1]
    col1new = replace(col1, "\\TIME_PERIOD" => "")
    new_cols = split(col1new, ",")
    split_table = transform(cleaned_table,
                            Symbol(col1) => ByRow(x -> split(x, ",")) => Symbol.(new_cols))
    select!(split_table, Not(Symbol(col1)))

    ## Convert the wide table to long format by stacking. Rename the 'variable'
    ## column to a proper 'time' column
    long_table = stack(split_table, Not(Symbol.(new_cols)))
    rename!(long_table, :variable => :time)
    # transform!(long_table, :variable => ByRow(x -> tryparse(Int64, x)) => :time)
    # describe(long_table, :eltype)

    ## write parquet
    write_table(joinpath(save_path, "$(table_id).parquet"),
                long_table, format = :parquet)
end


    end
