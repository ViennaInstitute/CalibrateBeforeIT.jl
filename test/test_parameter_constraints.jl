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

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    params = data["parameters"]

    @testset "Behavioral Parameters - Housing Investment Propensity" begin
        psi_H = params["psi_H"]
        @test psi_H >= 0
        @test psi_H < 0.3
    end

    @testset "Behavioral Parameters - Dividend Payout" begin
        theta_DIV = params["theta_DIV"]
        @test theta_DIV > 0
        @test theta_DIV < 1.5
    end

    @testset "Behavioral Parameters - Bank Markup" begin
        mu = params["mu"]
        @test mu > 0
        @test mu < 0.1
    end

    @testset "Tax Rate Parameters - Bounds" begin
        tax_params = ["tau_INC", "tau_FIRM", "tau_VAT", "tau_SIF", "tau_SIW",
                      "tau_EXPORT", "tau_CF", "tau_G"]

        for tax_param in tax_params
            @testset "$tax_param bounded" begin
                tau = params[tax_param]
                @test tau >= -0.1
                @test tau <= 1.0
            end
        end

        # tau_FIRM can be negative (if losses > profits), but should be bounded
        @test params["tau_FIRM"] >= -0.5
    end

    @testset "Fixed Parameters" begin
        @test isapprox(params["theta"], 0.05, atol=0.01)
        @test isapprox(params["zeta"], 0.03, atol=0.01)
        @test isapprox(params["zeta_LTV"], 0.6, atol=0.1)
        @test isapprox(params["zeta_b"], 0.5, atol=0.1)
    end

    @testset "Model Dimensions" begin
        @test params["G"] == 62
        @test params["T_prime"] > 0

        for pop_param in ["H_act", "H_inact", "J", "L"]
            @test params[pop_param] > 0
        end
    end

    @testset "No NaN or Inf Values" begin
        scalar_params = ["psi_H", "theta_DIV", "mu", "tau_INC", "tau_FIRM",
                        "tau_VAT", "tau_SIF", "tau_SIW", "tau_EXPORT", "tau_CF", "tau_G",
                        "theta", "zeta", "zeta_LTV", "zeta_b", "r_G", "theta_UB"]

        for param in scalar_params
            val = params[param]
            @test !isnan(val)
            @test !isinf(val)
        end
    end

    @testset "Budget Constraint (psi + psi_H < 1)" begin
        psi = params["psi"]
        psi_H = params["psi_H"]
        total = psi + psi_H
        @test total < 1.0
        @test 1 - total > 0  # savings rate positive
    end

    @testset "Tax Revenue Consistency" begin
        tax_sum = sum(params[t] for t in ["tau_INC", "tau_SIF", "tau_SIW", "tau_VAT"])
        @test tax_sum < 1.5
    end
end
