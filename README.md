Calibration scripts for the BeforeIT.jl ABM for the 27 EU member countries
====

This repository contains scripts that can be used to generate initial
conditions and parameters for the
[BeforeIT.jl](https://github.com/bancaditalia/BeforeIT.jl) agent-based
model.

The scripts are based on the Matlab and PostgreSQL scripts used in
[Poledna et
al.](https://www.sciencedirect.com/science/article/pii/S0014292122001891)
(already extended to work for all EU27 countries) and were kindly
shared with us. To ease the accessability of this work, we translated
the scripts to Julia. The only prerequisite for this repository is
Julia, there are no outside (technical) dependencies.

# Instantiating the project

Either run the script `00_instantiate_project.jl` in the root folder.
Alternatively, go to `models/MultiIndustry_ABM/`, start julia and
type:

``` julia
]activate .
instantiate
```

This will install all needed packages and their dependencies.

Aside: `]` enters the "pkg" mode where you can make changes to the
needed package dependencies by this project. Press `backspace` to
return to normal "julia" mode. Equivalently, `?` is for help mode, `;`
for shell mode.

These commands (or this script) are only necessary when you are
instantiating the project for the first time, or packages have been
updated (done only rarely).

# Example usage

## Step 1: download and store Eurostat tables as .parquet files

Run script `01_download_eurostat_tables.jl`. Reserve at least 3GB of
disk space and allow the script to run one or two hours. The following
steps are carried out:

1. Download of the necessary Eurostat tables, convert to long format
   and save as `.parquet` files in the `data/` directory.
2. Write a predefined NACE Rev.2 industry classification to disk.
3. Input-output coefficients are separated into three tables
   (2010-2014, 2015-2019, 2020 onwards). Because we need a
   time-series, we append the three tables into one.
4. Firm counts are not available through just one table: We have to
   extract the necessary data items and write them to disk for later
   easier querying.
   
After that, the `data/` directory is populated with the `.parquet` files
that we then need for second script.

## Step 2: generate the calibration input data for a given country

Run script `02_create_calibration_data.jl`. In the script, one can
specify the country, year ranges, etc that are parameters to the
calibration process. The following steps are carried out:

1. Import data for the EA19 country aggregate. This is used as an
   imputation device for countries that lack certain time series.
2. Then, for a given country:
   1. Import Figaro (input-output) data
   2. Import GDP, GVA, imports, exports time series
   3. Import data series for initial stocks and flows
3. From the three data sources and the EA19 data, initial conditions
   and parameters for a given point in time are generated. These
   initial conditions and parameters serve as input data for the
   model.


# References

Poledna, S., Miess, M. G., Hommes, C., & Rabitsch, K. (2023). Economic forecasting with an agent-based model. European Economic Review, 151, 104306. https://doi.org/10.1016/j.euroecorev.2022.104306
