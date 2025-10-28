
## This file is meant to be a script to help developing the whole process and to
## show the usual application

cd(@__DIR__)
using Revise
using Pkg
Pkg.activate(".")
using DuckDB, Tables, DataFrames, JLD2

import CalibrateBeforeIT as CBit

## Download and extract the .zip file from Zenodo
CBit.download_and_extract_zenodo_data()
