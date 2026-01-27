"""
    test_parameter_constraints.jl

Tests for validating that calibrated parameters satisfy economic and mathematical constraints.
These constraints ensure the BeforeIT.jl model will run stably and produce economically meaningful results.

Reference: Based on SFC (Stock-Flow Consistent) modeling principles and agent-based macroeconomics theory.
"""

using Test
using JLD2
using Statistics
using LinearAlgebra

import CalibrateBeforeIT as CBit

# Load reference calibration for testing
if !@isdefined(TEST_DATA_DIR)
    const TEST_DATA_DIR = joinpath(@__DIR__, "data", "reference_calibration")
end

@testset "Parameter Constraints Tests" begin

    # Load a sample calibration file
    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")

    @testset "Reference file exists" begin
        @test isfile(reference_file)
    end

    if isfile(reference_file)
        data = load(reference_file)
        params = data["parameters"]

        @testset "Behavioral Parameters - Housing Investment Propensity" begin
            # psi_H (housing investment propensity) must be positive and small
            @test haskey(params, "psi_H")
            psi_H = params["psi_H"]
            @testset "psi_H non-negative (got $psi_H)" begin
                @test psi_H >= 0
            end
            @testset "psi_H < 0.3 typical (got $psi_H)" begin
                @test psi_H < 0.3
            end
        end

        @testset "Behavioral Parameters - Dividend Payout" begin
            # theta_DIV (dividend payout ratio) must be in (0, 1)
            @test haskey(params, "theta_DIV")
            theta_DIV = params["theta_DIV"]
            @testset "theta_DIV positive (got $theta_DIV)" begin
                @test theta_DIV > 0
            end
            @testset "theta_DIV < 1 (got $theta_DIV)" begin
                @test theta_DIV < 1
            end
        end

        @testset "Behavioral Parameters - Bank Markup" begin
            # mu (bank markup over risk-free rate) must be positive
            @test haskey(params, "mu")
            mu = params["mu"]
            @testset "mu positive (got $mu)" begin
                @test mu > 0
            end
            @testset "mu < 0.1 quarterly typical (got $mu)" begin
                @test mu < 0.1
            end
        end

        @testset "Tax Rate Parameters - Bounds" begin
            tax_params = ["tau_INC", "tau_FIRM", "tau_VAT", "tau_SIF", "tau_SIW",
                          "tau_EXPORT", "tau_CF", "tau_G"]

            for tax_param in tax_params
                @testset "$tax_param exists and bounded" begin
                    @test haskey(params, tax_param)
                    if haskey(params, tax_param)
                        tau = params[tax_param]
                        # Tax rates should be in [0, 1], with possible small negative for subsidies
                        @test tau >= -0.1
                        @test tau <= 1.0

                        # Flag unusually high tax rates (> 60%)
                        if tau > 0.6
                            @warn "$tax_param is unusually high: $tau"
                        end
                    end
                end
            end

            # tau_FIRM can be negative (if losses > profits), but should be bounded
            @testset "tau_FIRM not extremely negative" begin
                if haskey(params, "tau_FIRM")
                    @test params["tau_FIRM"] >= -0.5
                end
            end
        end

        @testset "Fixed Parameters" begin
            # These parameters are typically hardcoded in BeforeIT.jl
            @testset "theta ≈ 0.05" begin
                if haskey(params, "theta")
                    @test isapprox(params["theta"], 0.05, atol=0.01)
                end
            end

            @testset "zeta ≈ 0.03" begin
                if haskey(params, "zeta")
                    @test isapprox(params["zeta"], 0.03, atol=0.01)
                end
            end

            @testset "zeta_LTV ≈ 0.6" begin
                if haskey(params, "zeta_LTV")
                    @test isapprox(params["zeta_LTV"], 0.6, atol=0.1)
                end
            end

            @testset "zeta_b ≈ 0.5" begin
                if haskey(params, "zeta_b")
                    @test isapprox(params["zeta_b"], 0.5, atol=0.1)
                end
            end
        end

        @testset "Model Dimensions" begin
            # G (number of sectors) should be 62 for NACE64 (excluding L68A, T, U)
            @testset "G = 62 sectors" begin
                if haskey(params, "G")
                    @test params["G"] == 62
                end
            end

            # T_prime should be positive
            @testset "T_prime > 0" begin
                if haskey(params, "T_prime")
                    @test params["T_prime"] > 0
                end
            end

            # Population counts should be positive
            for pop_param in ["H_act", "H_inact", "J", "L"]
                @testset "$pop_param > 0" begin
                    if haskey(params, pop_param)
                        @test params[pop_param] > 0
                    end
                end
            end
        end

        @testset "No NaN or Inf Values" begin
            scalar_params = ["psi_H", "theta_DIV", "mu", "tau_INC", "tau_FIRM",
                            "tau_VAT", "tau_SIF", "tau_SIW", "tau_EXPORT", "tau_CF", "tau_G",
                            "theta", "zeta", "zeta_LTV", "zeta_b", "r_G", "theta_UB"]

            for param in scalar_params
                @testset "$param not NaN/Inf" begin
                    if haskey(params, param)
                        val = params[param]
                        @test !isnan(val)
                        @test !isinf(val)
                    end
                end
            end
        end
    end
end

