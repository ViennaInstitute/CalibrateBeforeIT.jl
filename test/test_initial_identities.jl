"""
    test_initial_identities.jl

Tests for the initial accounting-identity gate that
03_create_calibration_data.jl runs on every generated bundle.

A saved bundle (parameters + initial conditions) must close the
model's initial accounting identities. The gate catches quarters
whose calibration would send more income through the model than the
calibration data support (e.g. unfloored bank profit in the
theta_DIV/tau_FIRM profit pools, or a clamped negative benefits sum),
which would otherwise surface only as a broken expenditure identity
in the simulation.
"""

import CalibrateBeforeIT as CBit

using Test
using JLD2

# Only run these tests if BeforeIT.jl is available
const BEFOREIT_AVAILABLE = try
    import BeforeIT as Bit
    true
catch
    false
end

@testset "Initial accounting identities" begin

    if !BEFOREIT_AVAILABLE
        @info "BeforeIT.jl not available, skipping initial identity tests"
        @test_skip "BeforeIT.jl required for initial identity tests"
    else
        import BeforeIT as Bit

        reference_file = joinpath(
            @__DIR__, "data", "reference_calibration",
            "AT_2010Q1_parameters_initial_conditions.jld2"
        )
        data = load(reference_file)
        parameters = data["parameters"]
        initial_conditions = data["initial_conditions"]

        @testset "A consistent calibration closes the identities" begin
            income_prod, exp_nominal, exp_real =
                CBit.get_initial_accounting_identities(parameters, initial_conditions)
            @info "Income-production diff = $income_prod, " *
                "expenditure (nominal) diff = $exp_nominal, " *
                "expenditure (real) diff = $exp_real"
            @test isapprox(income_prod, 0.0, atol = 1e-7)
            @test isapprox(exp_nominal, 0.0, atol = 1e-7)
            @test isapprox(exp_real, 0.0, atol = 1e-7)
        end

        @testset "Over-distributed firm profits are detected" begin
            # Same failure channel as the 2024Q1 bundles: an inflated
            # theta_DIV sends more dividends to households than disposable
            # income supports, breaking the initial expenditure identity.
            violated = merge(
                parameters,
                Dict("theta_DIV" => parameters["theta_DIV"] * 1.10),
            )
            income_prod, exp_nominal, exp_real =
                CBit.get_initial_accounting_identities(violated, initial_conditions)
            @info "Violated calibration: income-production diff = $income_prod, " *
                "expenditure (nominal) diff = $exp_nominal, " *
                "expenditure (real) diff = $exp_real"
            @test abs(exp_nominal) > 1e-7
            @test abs(exp_real) > 1e-7
        end
    end
end
