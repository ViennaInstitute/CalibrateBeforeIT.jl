"""
    test_sfc_identities.jl

Tests for Stock-Flow Consistent (SFC) accounting identities.

These tests verify that calibrated parameters and initial conditions produce
economically consistent results when used with BeforeIT.jl.

Key SFC principles:
1. Flow consistency: All income flows are accounted for
2. Stock consistency: Stocks evolve consistent with flows
3. Stock-flow consistency: Integrating flows gives correct stock changes
4. Double-entry bookkeeping: Every asset is someone's liability

Reference: https://en.wikipedia.org/wiki/Stock-flow_consistent_model
           BeforeIT.jl accounting_identities.jl tests
"""

using Test
using JLD2

# Only run these tests if BeforeIT.jl is available
const BEFOREIT_AVAILABLE = try
    import BeforeIT as Bit
    true
catch
    false
end

if !@isdefined(TEST_DATA_DIR)
    const TEST_DATA_DIR = joinpath(@__DIR__, "data", "reference_calibration")
end

@testset "SFC Accounting Identities" begin

    if !BEFOREIT_AVAILABLE
        @info "BeforeIT.jl not available, skipping SFC identity tests"
        @test_skip "BeforeIT.jl required for SFC tests"
    else
        import BeforeIT as Bit

        reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
        data = load(reference_file)
        params = data["parameters"]
        init_conds = data["initial_conditions"]

        @testset "Model Initialization" begin
            # Test that calibrated data can initialize a BeforeIT model
            model = nothing
            try
                model = Bit.Model(params, init_conds)
                @test model !== nothing
            catch e
                @error "Model initialization failed" exception=e
                @test false
            end

            if model !== nothing
                @testset "Single Step Accounting Identities" begin
                    # Run one step
                    Bit.step!(model; parallel=false)
                    Bit.collect_data!(model)

                    # Use BeforeIT's accounting identity functions
                    income_prod, exp_nominal, exp_real = Bit.get_accounting_identities(model.data)
                    cb_balance, bank_balance = Bit.get_accounting_identity_banks(model)

                    @testset "Income-Production identity" begin
                        @info "Income-Production identity diff = $income_prod"
                        @test isapprox(income_prod, 0.0, atol=1e-6)
                    end

                    @testset "Expenditure identity (nominal)" begin
                        @info "Expenditure identity (nominal) diff = $exp_nominal"
                        @test isapprox(exp_nominal, 0.0, atol=1e-6)
                    end

                    @testset "Expenditure identity (real)" begin
                        @info "Expenditure identity (real) diff = $exp_real"
                        @test isapprox(exp_real, 0.0, atol=1e-6)
                    end

                    @testset "Central bank identity" begin
                        @info "Central bank identity diff = $cb_balance"
                        @test isapprox(cb_balance, 0.0, atol=1e-6)
                    end

                    @testset "Bank balance sheet" begin
                        @info "Bank balance sheet diff = $bank_balance"
                        @test isapprox(bank_balance, 0.0, atol=1e-6)
                    end
                end

                @testset "Multi-Step Stability" begin
                    # Run multiple steps and check identities hold
                    model2 = Bit.Model(params, init_conds)

                    for t in 1:5
                        Bit.step!(model2; parallel=false)
                        Bit.collect_data!(model2)

                        @testset "Period $t" begin
                            # Use BeforeIT's accounting identity functions
                            income_prod, exp_nominal, exp_real = Bit.get_accounting_identities(model2.data)
                            cb_balance, bank_balance = Bit.get_accounting_identity_banks(model2)

                            @test isapprox(income_prod, 0.0, atol=1e-5)
                            @test isapprox(exp_nominal, 0.0, atol=1e-5)
                            @test isapprox(exp_real, 0.0, atol=1e-5)
                            @test isapprox(cb_balance, 0.0, atol=1e-5)
                            @test isapprox(bank_balance, 0.0, atol=1e-5)
                        end
                    end
                end
            end
        end
    end
end

@testset "SFC Constraints - Multiple Countries" begin
    calibration_dir = joinpath(dirname(@__DIR__), "data", "020_calibration_output")

    if isdir(calibration_dir) && BEFOREIT_AVAILABLE
        import BeforeIT as Bit

        countries = filter(x -> isdir(joinpath(calibration_dir, x)), readdir(calibration_dir))

        model_failures = String[]
        identity_results = Dict{String, NamedTuple}()

        # Test ALL countries
        for country in countries
            country_dir = joinpath(calibration_dir, country)
            files = filter(f -> endswith(f, "_parameters_initial_conditions.jld2"), readdir(country_dir))

            if length(files) > 0
                file_path = joinpath(country_dir, files[1])

                try
                    data = load(file_path)
                    params = data["parameters"]
                    init_conds = data["initial_conditions"]

                    # Apply minimum productivity floor for zero-productivity sectors
                    # (matches fix in get_params_and_initial_conditions.jl for new calibrations)
                    # This is needed for existing saved data that predates the fix
                    MIN_PRODUCTIVITY = 1e-6
                    params["alpha_s"] = max.(params["alpha_s"], MIN_PRODUCTIVITY)
                    params["kappa_s"] = max.(params["kappa_s"], MIN_PRODUCTIVITY)

                    model = Bit.Model(params, init_conds)
                    Bit.step!(model; parallel=false)
                    Bit.collect_data!(model)

                    # Check ALL accounting identities
                    income_prod, exp_nominal, exp_real = Bit.get_accounting_identities(model.data)
                    cb_balance, bank_balance = Bit.get_accounting_identity_banks(model)

                    identity_results[country] = (
                        income_prod = income_prod,
                        exp_nominal = exp_nominal,
                        exp_real = exp_real,
                        cb_balance = cb_balance,
                        bank_balance = bank_balance
                    )

                catch e
                    push!(model_failures, "$country: $e")
                end
            end
        end

        @testset "Model initialization across countries" begin
            if length(model_failures) > 0
                for v in model_failures
                    @warn "Model failure: $v"
                end
            end
            @test length(model_failures) == 0
        end

        # Test each identity across all countries
        tolerance = 1e-4

        @testset "Income-Production identity" begin
            failures = [(c, r.income_prod) for (c, r) in identity_results if abs(r.income_prod) > tolerance]
            if length(failures) > 0
                @info "Income-Production failures:" failures
            end
            @test length(failures) == 0
        end

        @testset "Expenditure identity (nominal)" begin
            failures = [(c, r.exp_nominal) for (c, r) in identity_results if abs(r.exp_nominal) > tolerance]
            if length(failures) > 0
                @info "Expenditure (nominal) failures:" failures
            end
            @test length(failures) == 0
        end

        @testset "Expenditure identity (real)" begin
            failures = [(c, r.exp_real) for (c, r) in identity_results if abs(r.exp_real) > tolerance]
            if length(failures) > 0
                @info "Expenditure (real) failures:" failures
            end
            @test length(failures) == 0
        end

        @testset "Central bank identity" begin
            failures = [(c, r.cb_balance) for (c, r) in identity_results if abs(r.cb_balance) > tolerance]
            if length(failures) > 0
                @info "Central bank failures:" failures
            end
            @test length(failures) == 0
        end

        @testset "Bank balance sheet" begin
            failures = [(c, r.bank_balance) for (c, r) in identity_results if abs(r.bank_balance) > tolerance]
            if length(failures) > 0
                @info "Bank balance sheet failures:" failures
            end
            @test length(failures) == 0
        end

        # Summary
        @info "Tested $(length(identity_results)) countries, $(length(model_failures)) initialization failures"

    elseif !BEFOREIT_AVAILABLE
        @info "BeforeIT.jl not available, skipping multi-country SFC tests"
    else
        @info "Calibration output directory not found, skipping multi-country SFC tests"
    end
end
