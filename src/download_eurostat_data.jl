"""
    download_eurostat_data.jl

Functions for downloading and processing Eurostat data tables.
"""

using DuckDB

"""
    download_all_eurostat_tables(table_list_file::String, save_path::String; 
                                use_cached_tsv::Bool=false, timeout::Int=300, retry_attempts::Int=3)

Download all Eurostat tables listed in the specified file and save as Parquet files.

# Arguments
- `table_list_file::String`: Path to file containing list of Eurostat table IDs (one per line)
- `save_path::String`: Directory path where Parquet files will be saved
- `use_cached_tsv::Bool`: Whether to use cached TSV files if they exist (default: false)
- `timeout::Int`: Download timeout in seconds (default: 300)
- `retry_attempts::Int`: Number of retry attempts per download (default: 3)

# Returns
- `NamedTuple`: Download summary with results and statistics

# Example
```julia
results = download_all_eurostat_tables("docs/00_table.txt", "data/010_eurostat_tables")
println("Successfully downloaded $(length(results.successful)) tables")
```
"""
function download_all_eurostat_tables(table_list_file::String, save_path::String; 
                                     use_cached_tsv::Bool=false, timeout::Int=300, retry_attempts::Int=3)
    # Ensure save directory exists
    mkpath(save_path)
    
    # Read table IDs from file
    if !isfile(table_list_file)
        throw(ArgumentError("Table list file not found: $table_list_file"))
    end
    
    all_eurostat_table_ids = readlines(table_list_file)
    
    # Filter out empty lines and comments
    all_eurostat_table_ids = filter(x -> !isempty(strip(x)) && !startswith(strip(x), "#"), all_eurostat_table_ids)
    
    if isempty(all_eurostat_table_ids)
        throw(ArgumentError("No valid table IDs found in $table_list_file"))
    end
    
    @info "Starting download of $(length(all_eurostat_table_ids)) Eurostat tables to $save_path"
    
    successful_downloads = Dict{String, Any}()
    failed_downloads = Dict{String, String}()
    total_rows = 0
    total_download_time = 0.0
    total_processing_time = 0.0
    
    for (i, table_id) in enumerate(all_eurostat_table_ids)
        table_id = strip(table_id)
        
        try
            @info "[$i/$(length(all_eurostat_table_ids))] Downloading table: $table_id"
            
            result = download_to_parquet(table_id, save_path; 
                                       use_cached_tsv=use_cached_tsv, 
                                       timeout=timeout, 
                                       retry_attempts=retry_attempts)
            
            successful_downloads[table_id] = result
            total_rows += result.rows_processed
            total_download_time += result.download_time
            total_processing_time += result.processing_time
            
            @info "✅ $table_id: $(result.rows_processed) rows, $(round(result.download_time, digits=1))s download, $(round(result.processing_time, digits=1))s processing"
            
        catch e
            error_msg = string(e)
            failed_downloads[table_id] = error_msg
            @warn "❌ Failed to download table $table_id: $error_msg"
        end
    end
    
    # Summary
    success_count = length(successful_downloads)
    total_count = length(all_eurostat_table_ids)
    
    @info "Download summary: $success_count/$total_count tables successful"
    @info "Total data processed: $total_rows rows"
    @info "Total time: $(round(total_download_time, digits=1))s download + $(round(total_processing_time, digits=1))s processing"
    
    return (
        successful = successful_downloads,
        failed = failed_downloads,
        summary = (
            total_tables = total_count,
            successful_count = success_count,
            failed_count = length(failed_downloads),
            total_rows = total_rows,
            total_download_time = total_download_time,
            total_processing_time = total_processing_time,
            success_rate = success_count / total_count
        )
    )
end

"""
    convert_nace64_to_parquet(nace_csv_path::String, save_path::String)

Convert NACE64 classification CSV file to Parquet format.

# Arguments
- `nace_csv_path::String`: Path to NACE64 CSV file
- `save_path::String`: Directory where Parquet file will be saved

# Returns
- `String`: Path to the created Parquet file
"""
function convert_nace64_to_parquet(nace_csv_path::String, save_path::String)
    if !isfile(nace_csv_path)
        throw(ArgumentError("NACE64 CSV file not found: $nace_csv_path"))
    end
    
    mkpath(save_path)
    
    @info "Converting NACE64 CSV to Parquet format"
    nace64_table = CSV.read(nace_csv_path, DataFrame; delim=",")
    
    output_path = joinpath(save_path, "nace64.parquet")
    write_table(output_path, nace64_table, format=:parquet)
    
    @info "NACE64 converted to: $output_path"
    return output_path
end

"""
    combine_figaro_tables(save_path::String)

Combine the three separate Figaro input-output tables into a single Parquet file.

# Arguments
- `save_path::String`: Directory containing the separate Figaro Parquet files

# Returns
- `String`: Path to the combined Parquet file
"""
function combine_figaro_tables(save_path::String)
    conn = DBInterface.connect(DuckDB.DB)
    
    # Check that all required files exist
    required_files = ["naio_10_fcp_ii1.parquet", "naio_10_fcp_ii2.parquet", "naio_10_fcp_ii3.parquet"]
    for file in required_files
        file_path = joinpath(save_path, file)
        if !isfile(file_path)
            throw(ArgumentError("Required Figaro file not found: $file_path"))
        end
    end
    
    @info "Combining Figaro tables into single file"
    
    output_path = joinpath(save_path, "naio_10_fcp_ii.parquet")
    
    sqlquery = """
    COPY (
        SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii1.parquet"))' 
        UNION ALL 
        SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii2.parquet"))' 
        UNION ALL 
        SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii3.parquet"))'
    ) TO '$output_path' (FORMAT parquet)
    """
    
    try
        DBInterface.execute(conn, sqlquery)
        @info "Combined Figaro tables saved to: $output_path"
        return output_path
    catch e
        @error "Failed to combine Figaro tables: $e"
        rethrow(e)
    finally
        DBInterface.close!(conn)
    end
end

"""
    create_business_data_aggregations(save_path::String)

Create aggregated business data tables from detailed NACE classifications.

# Arguments
- `save_path::String`: Directory containing the source Parquet files

# Returns
- `Vector{String}`: Paths to created aggregated files
"""
function create_business_data_aggregations(save_path::String)
    conn = DBInterface.connect(DuckDB.DB)
    created_files = String[]
    
    try
        # Helper function for file paths
        pqfile(table_id) = joinpath(save_path, "$(table_id).parquet")
        
        @info "Creating business data aggregations"
        
        # Create bd_9ac_l_form_a64 from bd_9ac_l_form_r2
        output_file1 = pqfile("bd_9ac_l_form_a64")
        sqlquery1 = create_bd_9ac_l_form_a64_query(pqfile)
        DBInterface.execute(conn, sqlquery1)
        push!(created_files, output_file1)
        @info "Created: bd_9ac_l_form_a64.parquet"
        
        # Create sbs_na_sca_a64 from sbs_na_sca_r2
        output_file2 = pqfile("sbs_na_sca_a64")
        sqlquery2 = create_sbs_na_sca_a64_query(pqfile)
        DBInterface.execute(conn, sqlquery2)
        push!(created_files, output_file2)
        @info "Created: sbs_na_sca_a64.parquet"
        
        return created_files
        
    catch e
        @error "Failed to create business data aggregations: $e"
        rethrow(e)
    finally
        DBInterface.close!(conn)
    end
end

# Helper functions for complex SQL queries
function create_bd_9ac_l_form_a64_query(pqfile_func)
    return """
    COPY (WITH sbs AS ( 
       WITH nace64 AS (SELECT * FROM '$(pqfile_func("nace64"))')
       SELECT nace, geo, indic_sb, leg_form, time, sum(value) AS value FROM '$(pqfile_func("bd_9ac_l_form_r2"))' 
       JOIN nace64 ON nace_r2::text ~ nace64.regex 
       WHERE nace_r2 NOT IN (SELECT nace FROM nace64) AND nace NOT IN (SELECT nace_r2 FROM '$(pqfile_func("bd_9ac_l_form_r2"))') AND nace_r2 !~ '^([A-Z][0-9][0-9][0-9])' 
       GROUP BY nace, geo, indic_sb, leg_form, time 
       UNION 
       SELECT nace, geo, indic_sb, leg_form, time, value FROM '$(pqfile_func("bd_9ac_l_form_r2"))' 
       JOIN nace64 ON nace_r2=nace64.nace 
    ), foo AS ( 
       WITH sbs_geo AS (SELECT DISTINCT geo FROM '$(pqfile_func("bd_9ac_l_form_r2"))'), 
       sbs_indic AS (SELECT DISTINCT indic_sb FROM '$(pqfile_func("bd_9ac_l_form_r2"))'), 
       sbs_leg AS (SELECT DISTINCT leg_form FROM '$(pqfile_func("bd_9ac_l_form_r2"))'), 
       sbs_time AS (SELECT DISTINCT time FROM '$(pqfile_func("bd_9ac_l_form_r2"))'), 
       nace64 AS (SELECT * FROM '$(pqfile_func("nace64"))') 
       SELECT nace64.nace, sbs_geo.geo, sbs_indic.indic_sb, sbs_leg.leg_form, sbs_time.time FROM nace64, sbs_geo, sbs_indic, sbs_leg, sbs_time 
    ) 
    SELECT foo.indic_sb, foo.leg_form, foo.nace AS nace_r2, foo.geo, foo.time, value FROM foo 
    LEFT JOIN sbs ON foo.nace=sbs.nace AND foo.geo=sbs.geo AND foo.indic_sb=sbs.indic_sb AND foo.leg_form=sbs.leg_form AND foo.time=sbs.time 
    WHERE foo.nace NOT IN ('L68A','T','U') 
    ORDER BY foo.nace 
    ) TO '$(pqfile_func("bd_9ac_l_form_a64"))' (FORMAT parquet);
    """
end

function create_sbs_na_sca_a64_query(pqfile_func)
    return """
    COPY (WITH sbs AS ( 
       WITH nace64 AS (SELECT * FROM '$(pqfile_func("nace64"))')
       SELECT nace, geo, indic_sb, time, sum(value) AS value FROM '$(pqfile_func("sbs_na_sca_r2"))' 
       JOIN nace64 ON nace_r2::text ~ nace64.regex 
       WHERE nace_r2 NOT IN (SELECT nace FROM nace64) AND nace NOT IN (SELECT nace_r2 FROM '$(pqfile_func("sbs_na_sca_r2"))') AND nace NOT IN ('O','P','Q','T','U') 
       GROUP BY nace, geo, indic_sb, time 
       UNION 
       SELECT nace, geo, indic_sb, time, value FROM '$(pqfile_func("sbs_na_sca_r2"))' 
       JOIN nace64 ON nace_r2=nace64.nace 
    ), foo AS ( 
       WITH sbs_geo AS (SELECT DISTINCT geo FROM '$(pqfile_func("sbs_na_sca_r2"))'), 
       sbs_indic AS (SELECT DISTINCT indic_sb FROM '$(pqfile_func("sbs_na_sca_r2"))'), 
       sbs_time AS (SELECT DISTINCT time FROM '$(pqfile_func("sbs_na_sca_r2"))'), 
       nace64 AS (SELECT * FROM '$(pqfile_func("nace64"))') 
       SELECT nace64.nace, sbs_geo.geo, sbs_indic.indic_sb, sbs_time.time FROM nace64, sbs_geo, sbs_indic, sbs_time 
    ) 
    SELECT foo.nace AS nace_r2, foo.geo, foo.indic_sb, foo.time, value FROM foo 
    LEFT JOIN sbs ON foo.nace=sbs.nace AND foo.geo=sbs.geo AND foo.indic_sb=sbs.indic_sb AND foo.time=sbs.time 
    WHERE foo.nace NOT IN ('L68A','T','U') 
    ORDER BY foo.nace 
    ) TO '$(pqfile_func("sbs_na_sca_a64"))' (FORMAT parquet)
    """
end

"""
    download_and_process_eurostat_data(;
        table_list_file::String = "docs/00_table.txt",
        nace_csv_path::String = "data/nace64.csv", 
        save_path::String = "data/010_eurostat_tables",
        use_cached_tsv::Bool = false
    )

Complete pipeline to download and process all required Eurostat data.

# Keyword Arguments
- `table_list_file::String`: Path to file with Eurostat table IDs
- `nace_csv_path::String`: Path to NACE64 classification CSV
- `save_path::String`: Output directory for processed data
- `use_cached_tsv::Bool`: Whether to reuse existing TSV files

# Returns
- `NamedTuple`: Summary of processing results

# Example
```julia
results = download_and_process_eurostat_data()
```
"""
function download_and_process_eurostat_data(;
    table_list_file::String = "docs/00_table.txt",
    nace_csv_path::String = "data/nace64.csv", 
    save_path::String = "data/010_eurostat_tables",
    use_cached_tsv::Bool = false,
    timeout::Int = 300,
    retry_attempts::Int = 3
)
    @info "Starting complete Eurostat data download and processing pipeline"
    
    # Step 1: Download all Eurostat tables
    download_results = download_all_eurostat_tables(table_list_file, save_path; 
                                                   use_cached_tsv=use_cached_tsv,
                                                   timeout=timeout,
                                                   retry_attempts=retry_attempts)
    
    # Step 2: Convert NACE64 to Parquet
    nace_output = convert_nace64_to_parquet(nace_csv_path, save_path)
    
    # Step 3: Combine Figaro tables
    figaro_output = combine_figaro_tables(save_path)
    
    # Step 4: Create business data aggregations
    business_outputs = create_business_data_aggregations(save_path)
    
    results = (
        download_results = download_results,
        nace_output = nace_output,
        figaro_output = figaro_output,
        business_outputs = business_outputs,
        summary = (
            total_download_files = download_results.summary.successful_count,
            total_failed_downloads = download_results.summary.failed_count,
            total_processed_rows = download_results.summary.total_rows,
            total_files_created = download_results.summary.successful_count + 1 + 1 + length(business_outputs),
            download_success_rate = download_results.summary.success_rate,
            total_download_time = download_results.summary.total_download_time,
            total_processing_time = download_results.summary.total_processing_time
        )
    )
    
    @info "Eurostat data processing complete:"
    @info "  📊 Successfully downloaded: $(results.summary.total_download_files) tables"
    @info "  ❌ Failed downloads: $(results.summary.total_failed_downloads) tables"
    @info "  📈 Total rows processed: $(results.summary.total_processed_rows)"
    @info "  📁 Total files created: $(results.summary.total_files_created)"
    @info "  ⏱️  Total time: $(round(results.summary.total_download_time + results.summary.total_processing_time, digits=1))s"
    @info "  ✅ Success rate: $(round(results.summary.download_success_rate * 100, digits=1))%"
    
    return results
end
