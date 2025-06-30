Calibration scripts for the BeforeIT.jl ABM for the 27 EU member countries
====

# Instantiating the project

Either run the script `00_instantiate_project.jl` in the root folder.
Alternatively, go to `models/MultiIndustry_ABM/`, start julia and
type:

``` julia
]activate .
instantiate
```

Aside: `]` enters the "pkg" mode where you can make changes to the
needed package dependencies by this project. Press `backspace` to
return to normal "julia" mode. Equivalently, `?` is for help mode, `;`
for shell mode.

These commands (or this script) are only necessary when you are
instantiating the project for the first time, or packages have been
updated (done only rarely).

# Example usage

## Step 1: download and store Eurostat tables as .parquet files

See and run script `01_download_eurostat_tables.jl`.

## Step 2: generate the calibration input data for a given country

See and run script `02_create_calibration_data.jl`.
