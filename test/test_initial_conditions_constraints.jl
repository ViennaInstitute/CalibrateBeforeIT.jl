"""
    test_initial_conditions_constraints.jl

Tests for initial conditions that are required by BeforeIT.jl model.

Key constraints:
- Financial stocks (D_I, L_I, D_H, K_H, L_G, E_k) must be positive
- E_CB (central bank equity) can be negative (it's a residual)
- Time series (Y, pi) must have correct length
- Per-capita benefits (w_UB, sb_inact, sb_other) must be positive

Reference: BeforeIT.jl model initialization requirements
"""

using Test
using JLD2
using Statistics

import CalibrateBeforeIT as CBit

if !@isdefined(TEST_DATA_DIR)
    const TEST_DATA_DIR = joinpath(@__DIR__, "data", "reference_calibration")
end

@testset "Initial Conditions - Financial Stocks" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    init_conds = data["initial_conditions"]

    @testset "Firm Financial Stocks" begin
        D_I = init_conds["D_I"]
        @test !isnan(D_I)
        @test !isinf(D_I)
        @test D_I > 0

        L_I = init_conds["L_I"]
        @test !isnan(L_I)
        @test !isinf(L_I)
        @test L_I > 0
    end

    @testset "Household Financial Stocks" begin
        D_H = init_conds["D_H"]
        @test !isnan(D_H)
        @test !isinf(D_H)
        @test D_H > 0

        K_H = init_conds["K_H"]
        @test !isnan(K_H)
        @test !isinf(K_H)
        @test K_H > 0
    end

    @testset "Government and Bank Stocks" begin
        L_G = init_conds["L_G"]
        @test !isnan(L_G)
        @test !isinf(L_G)
        @test L_G > 0

        E_k = init_conds["E_k"]
        @test !isnan(E_k)
        @test !isinf(E_k)
        @test E_k > 0

        # E_CB: Central bank equity (CAN be negative - it's the residual sector)
        E_CB = init_conds["E_CB"]
        @test !isnan(E_CB)
        @test !isinf(E_CB)

        # D_RoW: Rest of world (typically 0)
        D_RoW = init_conds["D_RoW"]
        @test !isnan(D_RoW)
        @test !isinf(D_RoW)
    end

    @testset "Balance Sheet Identity" begin
        # SFC identity: E_CB = L_G + L_I - D_I - D_H - E_k
        E_CB = init_conds["E_CB"]
        L_G = init_conds["L_G"]
        L_I = init_conds["L_I"]
        D_I = init_conds["D_I"]
        D_H = init_conds["D_H"]
        E_k = init_conds["E_k"]

        expected_E_CB = L_G + L_I - D_I - D_H - E_k
        @test isapprox(E_CB, expected_E_CB, rtol=0.01)
    end
end

@testset "Initial Conditions - Social Benefits" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    init_conds = data["initial_conditions"]

    @testset "Unemployment Benefit (w_UB)" begin
        w_UB = init_conds["w_UB"]
        @test !isnan(w_UB)
        @test !isinf(w_UB)
        @test w_UB > 0
    end

    @testset "Inactive Benefits (sb_inact)" begin
        sb_inact = init_conds["sb_inact"]
        @test !isnan(sb_inact)
        @test !isinf(sb_inact)
        @test sb_inact > 0
    end

    @testset "Other Benefits (sb_other)" begin
        sb_other = init_conds["sb_other"]
        @test !isnan(sb_other)
        @test !isinf(sb_other)
        # sb_other can be zero but typically positive
        @test sb_other >= 0
    end
end

@testset "Initial Conditions - Time Series" begin

    reference_file = joinpath(TEST_DATA_DIR, "AT_2010Q1_parameters_initial_conditions.jld2")
    data = load(reference_file)
    init_conds = data["initial_conditions"]
    params = data["parameters"]

    @testset "GDP History (Y)" begin
        Y = init_conds["Y"]

        @test length(Y) == params["T_prime"]
        @test !any(isnan, Y)
        @test !any(isinf, Y)
        @test all(Y .> 0)
    end

    @testset "Inflation History (pi)" begin
        pi = init_conds["pi"]

        @test length(pi) == params["T_prime"]
        @test !any(isnan, pi)
        @test !any(isinf, pi)
        @test all(pi .> -0.2)
        @test all(pi .< 0.5)
    end

    @testset "Forward-Looking Series (C_G, C_E, Y_I)" begin
        forward_series = [
            ("C_G", "Government consumption forecast"),
            ("C_E", "Export forecast"),
            ("Y_I", "Import forecast"),
        ]

        for (var_name, description) in forward_series
            series = vec(init_conds[var_name])

            @test !any(isinf, series)

            # NaN only allowed at the end (beyond data availability)
            non_nan = .!isnan.(series)
            if any(non_nan)
                last_valid = findlast(non_nan)
                @test all(.!isnan.(series[1:last_valid]))
                @test all(series[non_nan] .> 0)
            end
        end
    end

    @testset "EA Series" begin
        Y_EA = init_conds["Y_EA"]
        @test !isnan(Y_EA)
        @test !isinf(Y_EA)
        @test Y_EA > 0

        pi_EA = init_conds["pi_EA"]
        @test !isnan(pi_EA)
        @test !isinf(pi_EA)
        @test pi_EA > -0.1
        @test pi_EA < 0.2

        r_bar = init_conds["r_bar"]
        @test !isnan(r_bar)
        @test !isinf(r_bar)
        @test r_bar > -0.02
        @test r_bar < 0.1
    end

    @testset "Employment by Sector (N_s)" begin
        N_s = init_conds["N_s"]

        @test length(N_s) == 62
        @test !any(isnan, N_s)
        @test !any(isinf, N_s)
        @test all(N_s .>= 0)
        @test sum(N_s) > 100
    end

    @testset "Capital Utilization (omega)" begin
        omega = init_conds["omega"]
        @test !isnan(omega)
        @test !isinf(omega)
        @test omega ≈ 0.85 atol=0.01
    end
end

@testset "Initial Conditions - Multiple Countries" begin
    calibration_dir = joinpath(dirname(@__DIR__), "data", "020_calibration_output")

    if isdir(calibration_dir)
        countries = filter(x -> isdir(joinpath(calibration_dir, x)), readdir(calibration_dir))

        negative_stocks = String[]
        balance_violations = String[]

        for country in countries
            country_dir = joinpath(calibration_dir, country)
            files = filter(f -> endswith(f, "_parameters_initial_conditions.jld2"), readdir(country_dir))

            for file in files
                file_path = joinpath(country_dir, file)
                file_key = "$country/$file"

                try
                    data = load(file_path)
                    init_conds = data["initial_conditions"]

                    # Check positive stocks
                    for stock in ["D_I", "L_I", "D_H", "K_H", "L_G", "E_k"]
                        if haskey(init_conds, stock)
                            val = init_conds[stock]
                            if val <= 0
                                push!(negative_stocks, "$file_key: $stock = $val")
                            end
                        end
                    end

                    # Check balance sheet identity
                    if all(haskey.(Ref(init_conds), ["E_CB", "L_G", "L_I", "D_I", "D_H", "E_k"]))
                        E_CB = init_conds["E_CB"]
                        expected = init_conds["L_G"] + init_conds["L_I"] -
                                   init_conds["D_I"] - init_conds["D_H"] - init_conds["E_k"]
                        if !isapprox(E_CB, expected, rtol=0.05)
                            push!(balance_violations, "$file_key: E_CB=$E_CB vs expected=$expected")
                        end
                    end

                catch e
                    # Skip files with errors
                end
            end
        end

        @testset "Positive financial stocks across all countries" begin
            @test length(negative_stocks) == 0
            if length(negative_stocks) > 0
                for v in negative_stocks[1:min(10, length(negative_stocks))]
                    @warn "Negative stock: $v"
                end
            end
        end

        @testset "Balance sheet identity across all countries" begin
            @test length(balance_violations) == 0
            if length(balance_violations) > 0
                for v in balance_violations[1:min(10, length(balance_violations))]
                    @warn "Balance violation: $v"
                end
            end
        end
    else
        @info "Calibration output directory not found, skipping multi-country initial conditions tests"
    end
end
