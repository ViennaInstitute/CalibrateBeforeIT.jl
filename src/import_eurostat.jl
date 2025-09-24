# module Import_Eurostat
# export remove_flags_parse_to_float, transform_columns, download_to_parquet
# using CSV
# using DataFrames
# using Downloads
# using QuackIO

using Dates  # For now() function in metadata


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

"""
    download_to_parquet(table_id::String, eurostat_path::String;
                       use_cached_tsv::Bool = false,
                       timeout::Int = 300,
                       retry_attempts::Int = 3) -> NamedTuple

Download a Eurostat table and convert it to Parquet format.

Downloads data from the Eurostat API, processes it to clean Eurostat-specific
formatting flags, and saves it as a Parquet file for efficient querying.

# Arguments
- `table_id::String`: Eurostat table identifier (e.g., "nama_10_gdp")
- `eurostat_path::String`: Directory path where files will be saved
- `use_cached_tsv::Bool`: If true, skip download if TSV file already exists (default: false)
- `timeout::Int`: Download timeout in seconds (default: 300)
- `retry_attempts::Int`: Number of download retry attempts (default: 3)

# Returns
- `NamedTuple`: Contains paths and processing summary
  - `tsv_path::String`: Path to downloaded TSV file
  - `parquet_path::String`: Path to created Parquet file
  - `rows_processed::Int`: Number of data rows processed
  - `columns_processed::Int`: Number of columns processed
  - `download_time::Float64`: Download time in seconds
  - `processing_time::Float64`: Processing time in seconds

# Throws
- `ArgumentError`: If table_id is invalid or eurostat_path cannot be created
- `DownloadError`: If download fails after all retry attempts
- `ProcessingError`: If data processing fails

# Example
```julia
result = download_to_parquet("nama_10_gdp", "data/eurostat")
println("Downloaded ", result.rows_processed, " rows to ", result.parquet_path)
```
"""
function download_to_parquet(table_id::String, eurostat_path::String;
                            use_cached_tsv::Bool = false,
                            timeout::Int = 300,
                            retry_attempts::Int = 3)

    # Input validation
    validate_download_inputs(table_id, eurostat_path, timeout, retry_attempts)

    # Ensure save directory exists
    try
        mkpath(eurostat_path)
    catch e
        throw(ArgumentError("Cannot create save directory '$eurostat_path': $e"))
    end

    # File paths
    tsv_filename = joinpath(eurostat_path, "$(table_id).tsv")
    parquet_filename = joinpath(eurostat_path, "$(table_id).parquet")

    # Download phase
    download_time = @elapsed begin
        if !use_cached_tsv || !isfile(tsv_filename)
            download_eurostat_tsv(table_id, tsv_filename, timeout, retry_attempts)
        else
            @info "=> Using cached TSV file: $tsv_filename"
        end
    end

    # Processing phase
    processing_time = @elapsed begin
        processed_data = process_eurostat_tsv(tsv_filename, table_id)
    end

    # Save as Parquet
    try
        write_table(parquet_filename, processed_data.table, format = :parquet)
        @info "  -> Successfully created Parquet file: $parquet_filename"
    catch e
        throw(ProcessingError("Failed to write Parquet file '$parquet_filename': $e"))
    end

    # Return summary
    return (
        tsv_path = tsv_filename,
        parquet_path = parquet_filename,
        rows_processed = nrow(processed_data.table),
        columns_processed = ncol(processed_data.table),
        download_time = download_time,
        processing_time = processing_time,
        metadata = processed_data.metadata
    )
end

"""
    validate_download_inputs(table_id, eurostat_path, timeout, retry_attempts)

Validate inputs for download_to_parquet function.
"""
function validate_download_inputs(table_id::String, eurostat_path::String, timeout::Int, retry_attempts::Int)
    # Validate table_id
    if isempty(strip(table_id))
        throw(ArgumentError("table_id cannot be empty"))
    end

    # Check for invalid characters in table_id (basic validation)
    if !occursin(r"^[a-zA-Z0-9_]+$", table_id)
        throw(ArgumentError("table_id '$table_id' contains invalid characters (only letters, numbers, and underscores allowed)"))
    end

    # Validate eurostat_path
    if isempty(strip(eurostat_path))
        throw(ArgumentError("eurostat_path cannot be empty"))
    end

    # Validate timeout
    if timeout <= 0
        throw(ArgumentError("timeout must be positive (got $timeout)"))
    end

    # Validate retry_attempts
    if retry_attempts < 0
        throw(ArgumentError("retry_attempts must be non-negative (got $retry_attempts)"))
    end
end

"""
    download_eurostat_tsv(table_id, tsv_filename, timeout, retry_attempts)

Download TSV file from Eurostat API with retry logic.
"""
function download_eurostat_tsv(table_id::String, tsv_filename::String, timeout::Int, retry_attempts::Int)
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/$(table_id)/?format=TSV&compressed=false"

    @info "=> Downloading Eurostat table: $table_id"
    @info "=> URL: $url"

    last_error = nothing

    for attempt in 1:(retry_attempts + 1)
        try
            if attempt > 1
                @info "  -> Download attempt $attempt/$(retry_attempts + 1) for table $table_id"
            end

            # Download with timeout
            Downloads.download(url, tsv_filename; timeout = timeout)

            # Verify file was created and has content
            if !isfile(tsv_filename)
                throw(DownloadError("Downloaded file does not exist: $tsv_filename"))
            end

            file_size = filesize(tsv_filename)
            if file_size == 0
                throw(DownloadError("Downloaded file is empty: $tsv_filename"))
            end

            @info "  -> Successfully downloaded table $table_id ($(round(file_size/1024, digits=1)) KB)"
            return

        catch e
            last_error = e
            @warn "Download attempt $attempt failed for table $table_id: $e"

            # Clean up partial download
            if isfile(tsv_filename)
                try
                    rm(tsv_filename)
                catch cleanup_error
                    @warn "Could not clean up partial download: $cleanup_error"
                end
            end

            # Wait before retry (exponential backoff)
            if attempt <= retry_attempts
                wait_time = 2^(attempt - 1)
                @info "  -> Waiting $(wait_time) seconds before retry..."
                sleep(wait_time)
            end
        end
    end

    # All attempts failed
    throw(DownloadError("Failed to download table '$table_id' after $(retry_attempts + 1) attempts. Last error: $last_error"))
end

"""
    process_eurostat_tsv(tsv_filename, table_id) -> NamedTuple

Process downloaded Eurostat TSV file into clean tabular format.
"""
function process_eurostat_tsv(tsv_filename::String, table_id::String)
    @info "=> Processing TSV file: $tsv_filename"

    # Read raw TSV file
    local raw_table  # Declare variable in outer scope
    try
        raw_table = CSV.read(tsv_filename, DataFrame;
                            delim = "\t",
                            missingstring = ["", ":", ": ", ": b", ": bc",
                                           ": c", ": cd", ": d", ": e",
                                           ": n", ": m", ": p", ": z"])

        if nrow(raw_table) == 0
            throw(ProcessingError("TSV file is empty or could not be parsed: $tsv_filename"))
        end

        @info "  -> Read $(nrow(raw_table)) rows and $(ncol(raw_table)) columns from TSV"

    catch e
        throw(ProcessingError("Failed to read TSV file '$tsv_filename': $e"))
    end

    # Clean column names (remove trailing whitespace)
    try
        rename!(x -> replace(x, r" +$" => ""), raw_table)
    catch e
        throw(ProcessingError("Failed to clean column names: $e"))
    end

    # Identify year/time columns (all except first)
    if ncol(raw_table) < 2
        throw(ProcessingError("Table must have at least 2 columns (got $(ncol(raw_table)))"))
    end

    year_cols = names(raw_table)[2:end]
    @info "=> Processing $(length(year_cols)) time series columns"

    # Apply data cleaning to year columns
    local cleaned_table  # Declare variable in outer scope
    try
        cleaned_table = transform_columns(raw_table, year_cols)
        cleaned_table = identity.(cleaned_table)
    catch e
        throw(ProcessingError("Failed to clean data values: $e"))
    end

    # Split first column into separate dimension columns
    col1 = names(raw_table)[1]

    local split_table, new_cols  # Declare variables in outer scope
    try
        col1new = replace(col1, "\\TIME_PERIOD" => "")
        new_cols = split(col1new, ",")

        if isempty(new_cols) || any(isempty.(strip.(new_cols)))
            throw(ProcessingError("Invalid column structure in first column: '$col1'"))
        end

        @info "=> Splitting dimension column into: $(join(new_cols, ", "))"

        # Create a safer splitting function that ensures consistent array lengths
        n_expected_cols = length(new_cols)
        safe_split = x -> begin
            parts = split(x, ",")
            # Pad with empty strings if too few parts, truncate if too many
            if length(parts) < n_expected_cols
                parts = vcat(parts, fill("", n_expected_cols - length(parts)))
            elseif length(parts) > n_expected_cols
                parts = parts[1:n_expected_cols]
            end
            return parts
        end

        split_table = transform(cleaned_table,
                               Symbol(col1) => ByRow(safe_split) => Symbol.(new_cols))
        select!(split_table, Not(Symbol(col1)))

    catch e
        throw(ProcessingError("Failed to split dimension columns: $e"))
    end

    # Convert from wide to long format
    local long_table  # Declare variable in outer scope
    try
        long_table = stack(split_table, Not(Symbol.(new_cols)))
        rename!(long_table, :variable => :time)

        @info "  -> Converted to long format: $(nrow(long_table)) rows"

    catch e
        throw(ProcessingError("Failed to convert to long format: $e"))
    end

    # Create metadata
    metadata = (
        table_id = table_id,
        original_dimensions = new_cols,
        time_columns = year_cols,
        processed_at = now(),
        source_file = tsv_filename
    )

    return (table = long_table, metadata = metadata)
end

# Custom exception types for better error handling
struct DownloadError <: Exception
    message::String
end

struct ProcessingError <: Exception
    message::String
end

Base.showerror(io::IO, e::DownloadError) = print(io, "DownloadError: ", e.message)
Base.showerror(io::IO, e::ProcessingError) = print(io, "ProcessingError: ", e.message)

"""
    combine_figaro_tables(eurostat_path::String;
                         conn=nothing,
                         input_tables=["naio_10_fcp_ii1", "naio_10_fcp_ii2", "naio_10_fcp_ii3"],
                         output_table="naio_10_fcp_ii",
                         skip_if_missing=true)

Combine the three FIGARO Input-Output tables into a single table.

Eurostat splits the FIGARO IO tables into three separate tables by time periods:
- naio_10_fcp_ii1: 2010-2013
- naio_10_fcp_ii2: 2014-2017
- naio_10_fcp_ii3: 2018-2021
- naio_10_fcp_ii4: 2022 onwards

This function appends them into one table for efficient querying.

# Arguments
- `eurostat_path::String`: Directory containing the Parquet files
- `conn`: DuckDB connection (optional, will create if not provided)
- `input_tables::Vector{String}`: Names of input tables to combine (default: FIGARO tables)
- `output_table::String`: Name of the combined output table (default: "naio_10_fcp_ii")
- `skip_if_missing::Bool`: If true, return nothing when files are missing; if false, throw error (default: true)

# Returns
- `NamedTuple`: Information about the combination operation, or `nothing` if skipped due to missing files

# Example
```julia
import CalibrateBeforeIT as CBit
eurostat_path = "data/010_eurostat_tables"
result = CBit.combine_figaro_tables(eurostat_path)
if result !== nothing
    println("Combined ", length(result.input_tables), " tables into ", result.output_file)
else
    println("Combination skipped - files not ready")
end
```
"""
function combine_figaro_tables(eurostat_path::String;
                              conn=nothing,
                              input_tables=["naio_10_fcp_ii1", "naio_10_fcp_ii2",
                                            "naio_10_fcp_ii3", "naio_10_fcp_ii4"],
                              output_table="naio_10_fcp_ii",
                              skip_if_missing=true)

    @info "=> Combining FIGARO tables: $(join(input_tables, ", ")) → $output_table"

    # Validate inputs
    if isempty(input_tables)
        throw(ArgumentError("At least one input table must be specified"))
    end

    if !isdir(eurostat_path)
        if skip_if_missing
            @warn "Could not combine FIGARO tables (directory does not exist): $eurostat_path"
            @info "Skipping FIGARO combination - ensure save directory exists and input tables are downloaded"
            return nothing
        else
            throw(ArgumentError("Save path directory does not exist: $eurostat_path"))
        end
    end

    # Check that all input files exist
    missing_files = String[]
    input_files = String[]
    for table in input_tables
        file_path = joinpath(eurostat_path, "$(table).parquet")
        if !isfile(file_path)
            push!(missing_files, file_path)
        else
            push!(input_files, file_path)
        end
    end

    if !isempty(missing_files)
        if skip_if_missing
            @warn "Could not combine FIGARO tables (some files not found): $(join(missing_files, ", "))"
            @info "Skipping FIGARO combination - ensure input tables are downloaded first"
            return nothing
        else
            throw(ArgumentError("Missing input files: $(join(missing_files, ", "))"))
        end
    end

    # Create database connection if not provided
    local_conn = false
    if conn === nothing
        conn = DuckDB.DBInterface.connect(DuckDB.DB())
        local_conn = true
    end

    output_file = joinpath(eurostat_path, "$(output_table).parquet")

    try
        # Build SQL query for combining tables
        union_clauses = ["SELECT * FROM '$file'" for file in input_files]
        union_query = join(union_clauses, " UNION ALL ")

        sqlquery = "COPY ($union_query) TO '$output_file' (FORMAT parquet)"

        @info "=> Executing SQL query to combine tables..."
        start_time = time()

        DuckDB.DBInterface.execute(conn, sqlquery)

        processing_time = time() - start_time

        # Verify output file was created
        if !isfile(output_file)
            throw(ProcessingError("Output file was not created: $output_file"))
        end

        @info "  -> ✅ Successfully combined $(length(input_tables)) tables in $(round(processing_time, digits=2))s"
        @info "=> Output: $output_file"

        return (
            input_tables = input_tables,
            input_files = input_files,
            output_table = output_table,
            output_file = output_file,
            processing_time = processing_time,
            query = sqlquery
        )

    catch e
        error_msg = "Failed to combine FIGARO tables: $e"
        @error error_msg
        throw(ProcessingError(error_msg))

    finally
        # Close connection if we created it
        if local_conn && conn !== nothing
            DuckDB.DBInterface.close!(conn)
        end
    end
end

# end
