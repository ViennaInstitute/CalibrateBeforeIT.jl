
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)

import CalibrateBeforeIT as CBit

## Set some parameters for the data-downloading process
global const save_path = "data/010_eurostat_tables"
mkpath(save_path)
conn = CBit.DuckDB.DBInterface.connect(CBit.DuckDB.DB)


##------------------------------------------------------------
## Step 1: Download all Eurostat tables and save as .parquet files
all_eurostat_table_ids = CBit.get_eurostat_table_ids()

println(all_eurostat_table_ids)

# Test on a single table
result = CBit.download_to_parquet("nama_10_an6", save_path)
println(result)

# Download all tables
for table_id in all_eurostat_table_ids
    @info table_id
    CBit.download_to_parquet(table_id, save_path; use_cached_tsv=true)
end


##------------------------------------------------------------
## Step 2: Save the NACE64.csv (industry classification table) as parquet too
table_id = "nace64"
csv_filename = "data/$(table_id).csv"
nace64_table = CBit.CSV.read(csv_filename, CBit.DataFrame;
                             delim = ",")
CBit.write_table(joinpath(save_path, "$(table_id).parquet"),
            nace64_table, format = :parquet)


##------------------------------------------------------------
## Step 3: Append the three Figaro tables into one. Eurostat splits the Figaro
## IO tables into three separate tables. For us to query them efficiently, we
## append them onto each other and save them again as a .parquet file.
sqlquery = "COPY " *
    "(SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii1.parquet"))' UNION ALL " *
    "SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii2.parquet"))' UNION ALL " *
    " SELECT * FROM '$(joinpath(save_path, "naio_10_fcp_ii3.parquet"))') " *
    "TO '$(joinpath(save_path, "naio_10_fcp_ii.parquet"))' (FORMAT parquet)"
CBit.DuckDB.DBInterface.execute(conn, sqlquery);


## Step 4: Create bd_9ac_l_form_a64 from bd_9ac_l_form_r2
sqlquery = "COPY (WITH sbs AS ( " *
    "   WITH nace64 AS (SELECT * FROM '$(CBit.pqfile("nace64"))')" *
    "   SELECT nace, geo, indic_sb, leg_form, time, sum(value) AS value FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))' " *
    "   JOIN nace64 ON nace_r2::text ~ nace64.regex " *
    "   WHERE nace_r2 NOT IN (SELECT nace FROM nace64) AND nace NOT IN (SELECT nace_r2 FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))') AND nace_r2 !~ '^([A-Z][0-9][0-9][0-9])' " *
    "   GROUP BY nace, geo, indic_sb, leg_form, time " *
    "   UNION " *
    "   SELECT nace, geo, indic_sb, leg_form, time, value FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))' " *
    "   JOIN nace64 ON nace_r2=nace64.nace " *
    "), foo AS ( " *
    "   WITH sbs_geo AS (SELECT DISTINCT geo FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))'), " *
    "   sbs_indic AS (SELECT DISTINCT indic_sb FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))'), " *
    "   sbs_leg AS (SELECT DISTINCT leg_form FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))'), " *
    "   sbs_time AS (SELECT DISTINCT time FROM '$(CBit.pqfile("bd_9ac_l_form_r2"))'), " *
    "   nace64 AS (SELECT * FROM '$(CBit.pqfile("nace64"))') " *
    "   SELECT nace64.nace, sbs_geo.geo, sbs_indic.indic_sb, sbs_leg.leg_form, sbs_time.time FROM nace64, sbs_geo, sbs_indic, sbs_leg, sbs_time " *
    ") " *
    "SELECT foo.indic_sb, foo.leg_form, foo.nace AS nace_r2, foo.geo, foo.time, value FROM foo " *
    "LEFT JOIN sbs ON foo.nace=sbs.nace AND foo.geo=sbs.geo AND foo.indic_sb=sbs.indic_sb AND foo.leg_form=sbs.leg_form AND foo.time=sbs.time " *
    "WHERE foo.nace NOT IN ('L68A','T','U') " *
    "ORDER BY foo.nace " *
    ") TO '$(CBit.pqfile("bd_9ac_l_form_a64"))' (FORMAT parquet);"
CBit.DuckDB.DBInterface.execute(conn, sqlquery)

## Step 5: Create sbs_na_sca_a64 from sbs_na_sca_r2
sqlquery = "COPY (WITH sbs AS ( " *
    "   WITH nace64 AS (SELECT * FROM '$(CBit.pqfile("nace64"))')" *
    "   SELECT nace, geo, indic_sb, time, sum(value) AS value FROM '$(CBit.pqfile("sbs_na_sca_r2"))' " *
    "   JOIN nace64 ON nace_r2::text ~ nace64.regex " *
    "   WHERE nace_r2 NOT IN (SELECT nace FROM nace64) AND nace NOT IN (SELECT nace_r2 FROM '$(CBit.pqfile("sbs_na_sca_r2"))') AND nace NOT IN ('O','P','Q','T','U') " *
    "   GROUP BY nace, geo, indic_sb, time " *
    "   UNION " *
    "   SELECT nace, geo, indic_sb, time, value FROM '$(CBit.pqfile("sbs_na_sca_r2"))' " *
    "   JOIN nace64 ON nace_r2=nace64.nace " *
    "), foo AS ( " *
    "   WITH sbs_geo AS (SELECT DISTINCT geo FROM '$(CBit.pqfile("sbs_na_sca_r2"))'), " *
    "   sbs_indic AS (SELECT DISTINCT indic_sb FROM '$(CBit.pqfile("sbs_na_sca_r2"))'), " *
    "   sbs_time AS (SELECT DISTINCT time FROM '$(CBit.pqfile("sbs_na_sca_r2"))'), " *
    "   nace64 AS (SELECT * FROM '$(CBit.pqfile("nace64"))') " *
    "   SELECT nace64.nace, sbs_geo.geo, sbs_indic.indic_sb, sbs_time.time FROM nace64, sbs_geo, sbs_indic, sbs_time " *
    ") " *
    "SELECT foo.nace AS nace_r2, foo.geo, foo.indic_sb, foo.time, value FROM foo " *
    "LEFT JOIN sbs ON foo.nace=sbs.nace AND foo.geo=sbs.geo AND foo.indic_sb=sbs.indic_sb AND foo.time=sbs.time " *
    "WHERE foo.nace NOT IN ('L68A','T','U') " *
    "ORDER BY foo.nace " *
    ") TO '$(CBit.pqfile("sbs_na_sca_a64"))' (FORMAT parquet)"
CBit.DuckDB.DBInterface.execute(conn, sqlquery)


# test = "SELECT * FROM '$(pqfile("bd_9ac_l_form_a64"))' LIMIT 10;"
# DBInterface.execute(conn, test)


## After this step, we have the necessary data stored on disk!
