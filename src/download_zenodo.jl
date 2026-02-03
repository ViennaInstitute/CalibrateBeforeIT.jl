using Downloads
using ZipFile

# Zenodo download and extraction utilities
"""
Downloads the zip file from Zenodo and extracts its contents.

# Arguments
- `zip_file`: Name of the zip file to download
- `url`: URL to download from
- `extract_to`: Directory where contents should be extracted

# Returns
- `String`: Path to the extraction directory
"""
function download_and_extract_zenodo_data(zip_file::String = ZENODO_ZIP_FILE,
    url::String = ZENODO_URL,
    extract_to::String = zenodo_path)

    # Create extraction directory if it doesn't exist
    if !isdir(extract_to)
        mkpath(extract_to)
        println("Created directory: $extract_to")
    end

    # Download the zip file
    zip_path = joinpath(extract_to, zip_file)
    if !isfile(zip_path)
        println("Downloading $zip_file from Zenodo...")
        try
            Downloads.download(url, zip_path)
            println("Download completed: $zip_path")
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
        println("Zip file already exists: $zip_path")
    end

    # Extract the zip file using ZipFile
    println("Extracting $zip_file to $extract_to...")

    # Open zip file
    r = ZipFile.Reader(zip_path)
    try
        for f in r.files
            # Skip valid directories (indicated by trailing /) to avoid errors
            if endswith(f.name, "/")
                continue
            end

            full_path = joinpath(extract_to, f.name)

            # Ensure directory exists
            mkpath(dirname(full_path))

            # Write file content
            open(full_path, "w") do io
                write(io, read(f))
            end
        end
    finally
        close(r)
    end

    println("Extraction completed successfully!")

    return extract_to
end
