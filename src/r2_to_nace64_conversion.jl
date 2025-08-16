"""
Module for converting NACE Rev.2 classification data to NACE64 format.

This module handles the complex process of mapping industry classification codes
from the detailed NACE Rev.2 system to the simplified 64-industry framework
used by economic models like BeforeIT.jl.
"""

using DuckDB

"""
    create_bd_9ac_l_form_a64(save_path::String, conn; 
                            excluded_industries::Vector{String} = ["L68A", "T", "U"])

Convert business demography data from NACE Rev.2 to NACE64 classification.

Creates `bd_9ac_l_form_a64.parquet` from `bd_9ac_l_form_r2.parquet` by:
1. Mapping NACE Rev.2 codes to NACE64 using regex patterns
2. Aggregating values where multiple Rev.2 codes map to one NACE64 code  
3. Creating complete dimensional grid (industry×geo×indicator×legal_form×time)
4. Filling gaps with NULL values for missing combinations

# Arguments
- `save_path::String`: Directory containing input/output Parquet files
- `conn`: DuckDB database connection
- `excluded_industries::Vector{String}`: NACE64 industries to exclude from output

# Files Required
- `$(save_path)/nace64.parquet`: Industry classification mapping table
- `$(save_path)/bd_9ac_l_form_r2.parquet`: Input business demography data

# Files Created  
- `$(save_path)/bd_9ac_l_form_a64.parquet`: Output data in NACE64 format

# Returns
- `(success::Bool, rows_processed::Int)`: Processing status and row count

# Example
```julia
using DuckDB
conn = DBInterface.connect(DuckDB.DB)
save_path = "data/010_eurostat_tables"
success, rows = create_bd_9ac_l_form_a64(save_path, conn)
```
"""
function create_bd_9ac_l_form_a64(save_path::String, conn; 
                                  excluded_industries::Vector{String} = ["L68A", "T", "U"])
    
    # Input validation
    nace64_file = joinpath(save_path, "nace64.parquet")
    input_file = joinpath(save_path, "bd_9ac_l_form_r2.parquet")
    output_file = joinpath(save_path, "bd_9ac_l_form_a64.parquet")
    
    if !isfile(nace64_file)
        throw(ArgumentError("NACE64 mapping file not found: $nace64_file"))
    end
    
    if !isfile(input_file)
        throw(ArgumentError("Input file not found: $input_file"))
    end
    
    try
        # Build the SQL query
        sqlquery = """
        COPY (WITH sbs AS ( 
            WITH nace64 AS (SELECT * FROM '$nace64_file')
            SELECT nace, geo, indic_sb, leg_form, time, sum(value) AS value 
            FROM '$input_file' 
            JOIN nace64 ON nace_r2::text ~ nace64.regex 
            WHERE nace_r2 NOT IN (SELECT nace FROM nace64) 
              AND nace NOT IN (SELECT nace_r2 FROM '$input_file') 
              AND nace_r2 !~ '^([A-Z][0-9][0-9][0-9])' 
            GROUP BY nace, geo, indic_sb, leg_form, time 
            UNION 
            SELECT nace, geo, indic_sb, leg_form, time, value 
            FROM '$input_file' 
            JOIN nace64 ON nace_r2=nace64.nace 
        ), foo AS ( 
            WITH sbs_geo AS (SELECT DISTINCT geo FROM '$input_file'), 
                 sbs_indic AS (SELECT DISTINCT indic_sb FROM '$input_file'), 
                 sbs_leg AS (SELECT DISTINCT leg_form FROM '$input_file'), 
                 sbs_time AS (SELECT DISTINCT time FROM '$input_file'), 
                 nace64 AS (SELECT * FROM '$nace64_file') 
            SELECT nace64.nace, sbs_geo.geo, sbs_indic.indic_sb, sbs_leg.leg_form, sbs_time.time 
            FROM nace64, sbs_geo, sbs_indic, sbs_leg, sbs_time 
        ) 
        SELECT foo.indic_sb, foo.leg_form, foo.nace AS nace_r2, foo.geo, foo.time, value 
        FROM foo 
        LEFT JOIN sbs ON foo.nace=sbs.nace AND foo.geo=sbs.geo AND foo.indic_sb=sbs.indic_sb 
                      AND foo.leg_form=sbs.leg_form AND foo.time=sbs.time 
        WHERE foo.nace NOT IN ($(join("'" .* excluded_industries .* "'", ","))) 
        ORDER BY foo.nace 
        ) TO '$output_file' (FORMAT parquet);
        """
        
        # Execute the query
        DBInterface.execute(conn, sqlquery)
        
        # Count output rows for verification
        count_query = "SELECT COUNT(*) as count FROM '$output_file'"
        result = DBInterface.execute(conn, count_query)
        rows_processed = first(result).count
        
        @info "Successfully created bd_9ac_l_form_a64.parquet" rows_processed
        
        return (true, rows_processed)
        
    catch e
        @error "Failed to create bd_9ac_l_form_a64" error=e
        return (false, 0)
    end
end
