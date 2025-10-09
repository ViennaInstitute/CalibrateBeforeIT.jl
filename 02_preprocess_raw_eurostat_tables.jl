
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)

import CalibrateBeforeIT as CBit

## Set some parameters for the data-downloading process
global const eurostat_path = "data/010_eurostat_tables"
mkpath(eurostat_path)
conn = CBit.DuckDB.DBInterface.connect(CBit.DuckDB.DB)
