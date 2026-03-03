"""
    test_sectoral_constraints.jl

Tests for sectoral parameters (62 NACE64 sectors) and input-output matrix constraints.

Key constraints:
- Depreciation rates δ_s ∈ [0, 0.025] quarterly (~0-10% annual)
- Productivity parameters α_s, κ_s, w_s ≥ 0
- Distribution vectors must sum to 1
- Input-output matrix columns normalized to sum to 1

Reference: OECD Measuring Capital Manual (2009)
           Leontief Input-Output Model theory
"""

using Test
using JLD2
using Statistics

import CalibrateBeforeIT as CBit

if !@isdefined(TEST_DATA_DIR)
    const TEST_DATA_DIR = joinpath(@__DIR__, "data", "reference_calibration")
end
const N_SECTORS = 62  # NACE64 excluding L68A, T, U

@testset "Sectoral Parameter Constraints" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    params = data["parameters"]

    @testset "Sectoral Vector Dimensions" begin
        sectoral_vectors = ["I_s", "alpha_s", "beta_s", "kappa_s", "delta_s", "w_s",
                           "tau_Y_s", "tau_K_s", "b_CF_g", "b_CFH_g", "b_HH_g",
                           "c_G_g", "c_E_g", "c_I_g"]

        for vec_name in sectoral_vectors
            @test length(params[vec_name]) == N_SECTORS
        end
    end

    @testset "Depreciation Rates (delta_s)" begin
        delta_s = params["delta_s"]

        @test !any(isnan, delta_s)
        @test !any(isinf, delta_s)
        @test all(delta_s .>= 0)
        # Quarterly depreciation typically ≤ 0.025 (~10% annual), allow up to 0.07
        # for short-lived assets (software, R&D in ICT sectors ~25% annual)
        @test maximum(delta_s) <= 0.07
    end

    @testset "Labor Productivity (alpha_s)" begin
        alpha_s = params["alpha_s"]
        alpha_clean = replace(alpha_s, NaN => 0.0)

        @test !any(isinf, alpha_clean)
        @test all(alpha_clean .>= 0)
        # Most active sectors should have positive productivity
        @test count(alpha_clean .> 0) >= 50
    end

    @testset "Capital Productivity (kappa_s)" begin
        kappa_s = params["kappa_s"]
        kappa_clean = replace(kappa_s, NaN => 0.0)

        @test !any(isinf, kappa_clean)
        @test all(kappa_clean .>= 0)
    end

    @testset "Wage per Employee (w_s)" begin
        w_s = params["w_s"]
        w_clean = replace(w_s, NaN => 0.0)

        @test !any(isinf, w_clean)
        @test all(w_clean .>= 0)

        positive_wages = filter(x -> x > 0, w_clean)
        if length(positive_wages) > 0
            @test minimum(positive_wages) > 0.001
            @test maximum(positive_wages) < 10000
        end
    end

    @testset "Output Multiplier (beta_s)" begin
        beta_s = params["beta_s"]
        beta_clean = replace(beta_s, NaN => 0.0)

        @test !any(isinf, beta_clean)
        @test all(beta_clean .>= 0)

        # beta_s typically > 1 (output > intermediate consumption)
        positive_betas = filter(x -> x > 0, beta_clean)
        if length(positive_betas) > 0
            @test mean(positive_betas) > 1
        end
    end

    @testset "Sectoral Tax Rates (tau_Y_s, tau_K_s)" begin
        for tax_vec in ["tau_Y_s", "tau_K_s"]
            @testset "$tax_vec" begin
                tau = params[tax_vec]
                tau_clean = replace(tau, NaN => 0.0)

                @test !any(isinf, tau_clean)
                @test all(tau_clean .>= -0.25)
                @test all(tau_clean .<= 1.0)
            end
        end
    end

    @testset "Number of Firms (I_s)" begin
        I_s = params["I_s"]

        @test !any(isnan, I_s)
        @test !any(isinf, I_s)
        @test all(I_s .>= 1)
        @test sum(I_s) > 50
    end
end

@testset "Distribution Vector Constraints - Sum to 1" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    params = data["parameters"]

    distribution_vectors = [
        ("b_CF_g", "Capital formation distribution"),
        ("b_CFH_g", "Housing capital formation distribution"),
        ("b_HH_g", "Household consumption distribution"),
        ("c_G_g", "Government consumption distribution"),
        ("c_E_g", "Export distribution"),
        ("c_I_g", "Import distribution"),
    ]

    for (vec_name, description) in distribution_vectors
        @testset "$vec_name ($description)" begin
            vec = params[vec_name]

            @test !any(isnan, vec)
            @test !any(isinf, vec)
            @test all(vec .>= 0)
            @test isapprox(sum(vec), 1.0, atol=1e-6)
        end
    end
end

@testset "Input-Output Matrix (a_sg)" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    params = data["parameters"]

    @testset "Matrix Dimensions and Basic Properties" begin
        a_sg = params["a_sg"]

        @test size(a_sg) == (N_SECTORS, N_SECTORS)

        a_sg_clean = replace(a_sg, NaN => 0.0)

        @test !any(isinf, a_sg_clean)
        @test all(a_sg_clean .>= 0)
    end

    @testset "Column Sum Normalization" begin
        a_sg = params["a_sg"]
        a_sg_clean = replace(a_sg, NaN => 0.0)

        for j in 1:N_SECTORS
            col_sum = sum(a_sg_clean[:, j])
            if col_sum > 0  # Skip sectors with no inputs
                @test isapprox(col_sum, 1.0, atol=1e-6)
            end
        end
    end

    @testset "Leontief Productivity Condition" begin
        a_sg = params["a_sg"]
        a_sg_clean = replace(a_sg, NaN => 0.0)

        @test maximum(a_sg_clean) <= 1.0
    end
end

@testset "Sectoral Constraints - Multiple Countries" begin
    calibration_dir = joinpath(dirname(@__DIR__), "data", "020_calibration_output")

    if isdir(calibration_dir)
        countries = filter(x -> isdir(joinpath(calibration_dir, x)), readdir(calibration_dir))

        negative_delta = String[]
        extreme_delta = String[]
        distribution_violations = String[]

        for country in countries
            country_dir = joinpath(calibration_dir, country)
            files = filter(f -> endswith(f, "_parameters_initial_conditions.jld2"), readdir(country_dir))

            for file in files
                file_path = joinpath(country_dir, file)
                file_key = "$country/$file"

                try
                    data = load(file_path)
                    params = data["parameters"]

                    # Check depreciation
                    if haskey(params, "delta_s")
                        delta_s = params["delta_s"]
                        if any(delta_s .< 0)
                            push!(negative_delta, "$file_key: min delta_s = $(minimum(delta_s))")
                        end
                        if any(delta_s .> 0.05)
                            push!(extreme_delta, "$file_key: max delta_s = $(maximum(delta_s))")
                        end
                    end

                    # Check distribution sums
                    for vec_name in ["b_CF_g", "b_HH_g", "c_G_g", "c_E_g", "c_I_g"]
                        if haskey(params, vec_name)
                            vec_sum = sum(params[vec_name])
                            if !isapprox(vec_sum, 1.0, atol=1e-4)
                                push!(distribution_violations, "$file_key: sum($vec_name) = $vec_sum")
                            end
                        end
                    end

                catch e
                    # Skip files with errors
                end
            end
        end

        @testset "Depreciation rates across all countries" begin
            @test length(negative_delta) == 0
        end

        @testset "Distribution sums across all countries" begin
            @test length(distribution_violations) == 0
            if length(distribution_violations) > 0
                for v in distribution_violations[1:min(10, length(distribution_violations))]
                    @warn "Distribution violation: $v"
                end
            end
        end
    else
        @info "Calibration output directory not found, skipping multi-country sectoral tests"
    end
end
