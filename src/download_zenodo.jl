using Downloads
using p7zip_jll

# Zenodo download and extraction utilities
"""
Downloads the zip file from Zenodo and extracts its contents.

# Arguments
- `zip_file`: Name of the zip file to download
- `url`: URL to download from
"""
function download_and_extract_zenodo_data(
    zip_file::String = "$(ZENODO_ZIP_FILENAME).zip",
    url::String = ZENODO_URL)

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
        # Preserve the archive's data/010_eurostat_tables prefix by extracting
        # relative to the working directory, as the former unzip call did.
        run(`$(p7zip()) x -y $(zip_file)`)
        println("Extraction completed successfully!")
    catch e
        error("Failed to extract zip file: $e")
    end
end



