using Downloads
using p7zip_jll

# Zenodo download and extraction utilities
"""
Downloads the zip file from Zenodo and extracts its contents.

# Arguments
- `zip_file`: Name of the zip file to download
- `url`: URL to download from
- `extract_to`: Directory where contents should be extracted (default: "data/010_eurostat_tables")

# Returns
- `String`: Path to the extraction directory
"""
function download_and_extract_zenodo_data(
    zip_file::String = "$(ZENODO_ZIP_FILENAME).zip",
    url::String = ZENODO_URL,
    extract_to::String = "data/010_eurostat_tables")

    # Create extraction directory if it doesn't exist
    if !isdir(extract_to)
        mkpath(extract_to)
        println("Created directory: $extract_to")
    end

    # Download the zip file
    if !isfile(zip_file)
        println("Downloading $zip_file from Zenodo...")
        try
            Downloads.download(url, zip_file)
            println("Download completed: $zip_file")
        catch e
            if isa(e, Downloads.RequestError) && e.response.status == 403
                error("Access forbidden (403). This could mean:
                1. The Zenodo record is not published yet (still a draft)
                2. The record ID or file name is incorrect
                3. You don't have permission to access this record
\nPlease check the URL: $url")
            elseif isa(e, Downloads.RequestError) && e.response.status == 404
                error("Record not found (404). The Zenodo record ID may be incorrect.\n\nPlease check the URL: $url")
            else
                rethrow(e)
            end
        end
    else
        println("Zip file already exists: $zip_file")
    end

    # Extract the zip file using p7zip_jll (cross-platform)
    println("Extracting $zip_file...")
    try
        # Use 7z to extract with full paths (-y = assume yes, -o = output directory)
        run(`$(p7zip()) x -y -o$(extract_to) $(zip_file)`)
        println("Extraction completed successfully!")
    catch e
        error("Failed to extract zip file: $e")
    end

    return extract_to
end



