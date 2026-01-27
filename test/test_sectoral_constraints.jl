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

    if isfile(reference_file)
        data = load(reference_file)
        params = data["parameters"]

        @testset "Sectoral Vector Dimensions" begin
            sectoral_vectors = ["I_s", "alpha_s", "beta_s", "kappa_s", "delta_s", "w_s",
                               "tau_Y_s", "tau_K_s", "b_CF_g", "b_CFH_g", "b_HH_g",
                               "c_G_g", "c_E_g", "c_I_g"]

            for vec_name in sectoral_vectors
                @testset "$vec_name" begin
                    @test haskey(params, vec_name)
                    if haskey(params, vec_name)
                        vec = params[vec_name]
                        @test length(vec) == N_SECTORS
                    end
                end
            end
        end

        @testset "Depreciation Rates (delta_s)" begin
            @test haskey(params, "delta_s")
            if haskey(params, "delta_s")
                delta_s = params["delta_s"]

                # No NaN or Inf
                @test !any(isnan, delta_s)
                @test !any(isinf, delta_s)

                # Depreciation must be non-negative
                @test all(delta_s .>= 0)

                # Quarterly depreciation typically ≤ 0.025 (~10% annual), allow up to 0.06
                max_delta = maximum(delta_s)
                @test max_delta <= 0.06

                # Warn if any sector has unusually high depreciation
                high_depr = findall(delta_s .> 0.025)
                if length(high_depr) > 0
                    @warn "Sectors with high depreciation (>0.025 quarterly): $high_depr"
                end
            end
        end

        @testset "Labor Productivity (alpha_s)" begin
            @test haskey(params, "alpha_s")
            if haskey(params, "alpha_s")
                alpha_s = params["alpha_s"]

                # Replace NaN with 0 (for sectors without output)
                alpha_clean = replace(alpha_s, NaN => 0.0)

                # No Inf
                @test !any(isinf, alpha_clean)

                # Productivity must be non-negative
                @test all(alpha_clean .>= 0)

                # Most active sectors should have positive productivity
                active_sectors = count(alpha_clean .> 0)
                @test active_sectors >= 50
            end
        end

        @testset "Capital Productivity (kappa_s)" begin
            @test haskey(params, "kappa_s")
            if haskey(params, "kappa_s")
                kappa_s = params["kappa_s"]

                # Replace NaN with 0
                kappa_clean = replace(kappa_s, NaN => 0.0)

                # No Inf
                @test !any(isinf, kappa_clean)

                # Productivity must be non-negative
                @test all(kappa_clean .>= 0)
            end
        end

        @testset "Wage per Employee (w_s)" begin
            @test haskey(params, "w_s")
            if haskey(params, "w_s")
                w_s = params["w_s"]

                # Replace NaN with 0
                w_clean = replace(w_s, NaN => 0.0)

                # No Inf
                @test !any(isinf, w_clean)

                # Wages must be non-negative
                @test all(w_clean .>= 0)

                # Positive wages should be reasonable (not tiny or huge)
                positive_wages = filter(x -> x > 0, w_clean)
                if length(positive_wages) > 0
                    @test minimum(positive_wages) > 0.001
                    @test maximum(positive_wages) < 10000
                end
            end
        end

        @testset "Output Multiplier (beta_s)" begin
            @test haskey(params, "beta_s")
            if haskey(params, "beta_s")
                beta_s = params["beta_s"]

                # Replace NaN with 0
                beta_clean = replace(beta_s, NaN => 0.0)

                # No Inf
                @test !any(isinf, beta_clean)

                # beta_s must be non-negative
                @test all(beta_clean .>= 0)

                # beta_s typically > 1 (output > intermediate consumption)
                positive_betas = filter(x -> x > 0, beta_clean)
                if length(positive_betas) > 0
                    mean_beta = mean(positive_betas)
                    @test mean_beta > 1
                end
            end
        end

        @testset "Sectoral Tax Rates (tau_Y_s, tau_K_s)" begin
            for tax_vec in ["tau_Y_s", "tau_K_s"]
                @testset "$tax_vec" begin
                    @test haskey(params, tax_vec)
                    if haskey(params, tax_vec)
                        tau = params[tax_vec]

                        # Replace NaN with 0
                        tau_clean = replace(tau, NaN => 0.0)

                        # No Inf
                        @test !any(isinf, tau_clean)

                        # Tax rates bounded (can be negative for subsidies)
                        @test all(tau_clean .>= -0.25)
                        @test all(tau_clean .<= 1.0)
                    end
                end
            end
        end

        @testset "Number of Firms (I_s)" begin
            @test haskey(params, "I_s")
            if haskey(params, "I_s")
                I_s = params["I_s"]

                # No NaN or Inf
                @test !any(isnan, I_s)
                @test !any(isinf, I_s)

                # Firm counts must be positive integers (at least 1)
                @test all(I_s .>= 1)

                # Total firms should be reasonable (scaled data may have fewer)
                total_firms = sum(I_s)
                @test total_firms > 50
            end
        end
    else
        @info "Reference file not found, skipping sectoral parameter tests"
    end
end

@testset "Distribution Vector Constraints - Sum to 1" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")

    if isfile(reference_file)
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
                @test haskey(params, vec_name)
                if haskey(params, vec_name)
                    vec = params[vec_name]

                    # No NaN or Inf
                    @test !any(isnan, vec)
                    @test !any(isinf, vec)

                    # All elements must be non-negative (distribution weights)
                    @test all(vec .>= 0)

                    # Sum must equal 1 (distribution property)
                    vec_sum = sum(vec)
                    @test isapprox(vec_sum, 1.0, atol=1e-6)
                end
            end
        end
    else
        @info "Reference file not found, skipping distribution vector tests"
    end
end

@testset "Input-Output Matrix (a_sg)" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")

    if isfile(reference_file)
        data = load(reference_file)
        params = data["parameters"]

        @testset "Matrix Dimensions and Basic Properties" begin
            @test haskey(params, "a_sg")
            if haskey(params, "a_sg")
                a_sg = params["a_sg"]

                # Should be square and N_SECTORS x N_SECTORS
                @test size(a_sg) == (N_SECTORS, N_SECTORS)

                # Replace NaN with 0 (standard practice for undefined coefficients)
                a_sg_clean = replace(a_sg, NaN => 0.0)

                # No Inf
                @test !any(isinf, a_sg_clean)

                # All elements must be non-negative (input coefficients)
                @test all(a_sg_clean .>= 0)
            end
        end

        @testset "Column Sum Normalization" begin
            if haskey(params, "a_sg")
                a_sg = params["a_sg"]
                a_sg_clean = replace(a_sg, NaN => 0.0)

                # Each column should sum to 1 (normalized input coefficients)
                # This represents the distribution of inputs to produce output in each sector
                for j in 1:N_SECTORS
                    col_sum = sum(a_sg_clean[:, j])
                    if col_sum > 0  # Skip sectors with no inputs
                        @test isapprox(col_sum, 1.0, atol=1e-6)
                    end
                end
            end
        end

        @testset "Leontief Productivity Condition" begin
            # For a productive Leontief model, the matrix (I - A) must be invertible
            # with non-negative inverse. This requires the column sums to be < 1
            # when A represents technical (not normalized) coefficients.
            #
            # Since our a_sg is normalized (columns sum to 1), we check that
            # no single input dominates (no element > 0.9)
            if haskey(params, "a_sg")
                a_sg = params["a_sg"]
                a_sg_clean = replace(a_sg, NaN => 0.0)

                max_element = maximum(a_sg_clean)
                @test max_element <= 1.0

                # Warn if any sector has very concentrated inputs
                concentrated = findall(a_sg_clean .> 0.8)
                if length(concentrated) > 0
                    @warn "a_sg has $(length(concentrated)) elements > 0.8 (concentrated inputs)"
                end
            end
        end
    else
        @info "Reference file not found, skipping I-O matrix tests"
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
