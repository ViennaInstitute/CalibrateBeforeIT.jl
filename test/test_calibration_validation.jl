#!/usr/bin/env julia

"""
Calibration Validation Script

Performs comprehensive sanity checks on calibrated parameters and initial conditions:
1. Parameter range checks
2. Accounting identity checks
3. Time series properties
4. Cross-sectional reasonableness
5. Financial stock-flow consistency
6. Diagnostic visualizations
7. Summary report
"""

import CalibrateBeforeIT as CBit
using JLD2
using Test
using Plots
using Printf
using Statistics
using LinearAlgebra

"""
Load calibration object and extract parameters/initial conditions for a specific quarter.

# Arguments
- `geo::String`: Country code (e.g., "NL", "AT")
- `calibration_year::Int`: Year to validate (e.g., 2020)
- `calibration_quarter::Int`: Quarter to validate (1-4)

# Returns
- Tuple of (params, initial_conditions, calibration_object) for the specified quarter
"""
function load_calibration_for_validation(geo::String, calibration_year::Int, calibration_quarter::Int)
    # Load calibration object
    calibration_path = joinpath(dirname(@__DIR__), "data", "020_calibration_output",
                                geo, "calibration_object.jld2")

    if !isfile(calibration_path)
        error("Calibration object not found: $calibration_path\nPlease run calibration first.")
    end

    calibration_object = load(calibration_path, "calibration_object")

    # Calculate calibration date
    calibration_month = calibration_quarter * 3
    calibration_date = CBit.DateTime(calibration_year, calibration_month,
        calibration_month in [3, 12] ? 31 : 30)

    # Get parameters and initial conditions
    parameters, initial_conditions = CBit.get_params_and_initial_conditions(
        calibration_object, calibration_date; scale = 1/1000
    )

    return parameters, initial_conditions, calibration_object
end

function validate_parameter_ranges(params, G)
    """Check that all parameters are in economically reasonable ranges"""

    println("\n" * "="^80)
    println("1. PARAMETER RANGE CHECKS")
    println("="^80)

    errors = String[]
    warnings = String[]

    # Tax rates should be in [0, 1]
    if haskey(params, "tau_Y_s")
        if any(params["tau_Y_s"] .< 0)
            push!(errors, "Negative product tax rates detected!")
        end
        if any(params["tau_Y_s"] .> 1)
            push!(warnings, "Product tax rates > 100% detected")
        end
        println("  ✓ Product tax rates: [$(round(minimum(params["tau_Y_s"]), digits=4)), $(round(maximum(params["tau_Y_s"]), digits=4))]")
    end

    if haskey(params, "tau_K_s")
        if any(params["tau_K_s"] .< 0)
            push!(errors, "Negative capital tax rates detected!")
        end
        if any(params["tau_K_s"] .> 1)
            push!(warnings, "Capital tax rates > 100% detected")
        end
        println("  ✓ Capital tax rates: [$(round(minimum(params["tau_K_s"]), digits=4)), $(round(maximum(params["tau_K_s"]), digits=4))]")
    end

    # Interest rates should be small positive
    if haskey(params, "r_bar")
        if params["r_bar"] < 0
            push!(errors, "Negative baseline interest rate!")
        elseif params["r_bar"] > 0.2
            push!(warnings, "Very high baseline interest rate: $(params["r_bar"])")
        end
        println("  ✓ Baseline interest rate: $(round(params["r_bar"], digits=4))")
    end

    if haskey(params, "r_G")
        if params["r_G"] < 0
            push!(errors, "Negative government interest rate!")
        elseif params["r_G"] > 0.2
            push!(warnings, "Very high government interest rate: $(params["r_G"])")
        end
        println("  ✓ Government interest rate: $(round(params["r_G"], digits=4))")
    end

    if haskey(params, "mu")
        if params["mu"] < -0.1
            push!(warnings, "Very negative risk premium: $(params["mu"])")
        elseif params["mu"] > 0.1
            push!(warnings, "Very high risk premium: $(params["mu"])")
        end
        println("  ✓ Risk premium (mu): $(round(params["mu"], digits=4))")
    end

    # Depreciation rates in [0, 0.3] (0% to 30% annual)
    if haskey(params, "delta_s")
        if any(params["delta_s"] .< 0)
            push!(errors, "Negative depreciation rates detected!")
        end
        if any(params["delta_s"] .> 0.3)
            push!(warnings, "Very high depreciation rates (>30%) detected")
        end
        println("  ✓ Depreciation rates: [$(round(minimum(params["delta_s"]), digits=4)), $(round(maximum(params["delta_s"]), digits=4))]")
    end

    # AR(1) coefficients should be stationary: |alpha| < 1
    for param in ["alpha_G", "alpha_E", "alpha_I"]
        if haskey(params, param)
            if abs(params[param]) >= 1
                push!(errors, "Non-stationary AR(1) coefficient: $param = $(params[param])")
            end
            println("  ✓ AR(1) coefficient $param: $(round(params[param], digits=4))")
        end
    end

    # Adjustment speeds should be positive
    if haskey(params, "delta_S_s")
        if any(params["delta_S_s"] .<= 0)
            push!(errors, "Non-positive inventory adjustment speeds detected!")
        end
        println("  ✓ Inventory adjustment speeds: [$(round(minimum(params["delta_S_s"]), digits=4)), $(round(maximum(params["delta_S_s"]), digits=4))]")
    end

    # Tax rates (should be in [0, 1] mostly)
    println("\n  📊 Tax Rates:")
    tax_params = ["tau_INC", "tau_FIRM", "tau_VAT", "tau_SIF", "tau_SIW", "tau_EXPORT", "tau_CF", "tau_G"]
    tax_labels = ["Income tax", "Corporate tax", "VAT", "Employer social insurance",
                   "Worker social insurance", "Export tax", "Capital formation tax", "Gov consumption tax"]

    for (i, param) in enumerate(tax_params)
        if haskey(params, param)
            val = params[param]
            if val < 0 || val > 1
                push!(warnings, "$param outside [0, 1]: $val")
            end

            # Special case for export tax
            if param == "tau_EXPORT"
                if val == 0
                    println("  ℹ️  $(tax_labels[i]) ($param): $(round(val, digits=4)) (no export taxes)")
                else
                    println("  ✓ $(tax_labels[i]) ($param): $(round(val, digits=4))")
                end
            else
                println("  ✓ $(tax_labels[i]) ($param): $(round(val, digits=4))")
            end
        end
    end

    # Total tax burden check
    if all(haskey(params, p) for p in ["tau_INC", "tau_SIF", "tau_SIW"])
        total_tax_burden = params["tau_INC"] + params["tau_SIF"] + params["tau_SIW"]
        if total_tax_burden > 0.8
            push!(warnings, "Very high total tax burden: $(round(total_tax_burden, digits=3))")
        end
        println("  ✓ Total tax burden: $(round(total_tax_burden, digits=3)) (tau_INC + tau_SIF + tau_SIW)")
    end

    # Behavioral parameters
    println("\n  📊 Behavioral Parameters:")

    if haskey(params, "psi")
        psi = params["psi"]
        if psi < 0 || psi > 1
            push!(errors, "Consumption propensity (psi) outside [0, 1]: $psi")
        end
        println("  ✓ Consumption propensity (psi): $(round(psi, digits=4))")
    end

    if haskey(params, "psi_H")
        psi_H = params["psi_H"]
        if psi_H < 0 || psi_H > 1
            push!(errors, "Housing investment propensity (psi_H) outside [0, 1]: $psi_H")
        end
        println("  ✓ Housing investment propensity (psi_H): $(round(psi_H, digits=4))")
    end

    # Check psi + psi_H <= 1.0
    if haskey(params, "psi") && haskey(params, "psi_H")
        total_spending = params["psi"] + params["psi_H"]
        if total_spending > 1.0
            push!(warnings, "Total spending propensity > 1.0: psi + psi_H = $(round(total_spending, digits=4))")
        end
        println("  ✓ Total spending propensity (psi + psi_H): $(round(total_spending, digits=4))")
    end

    if haskey(params, "theta_DIV")
        theta_DIV = params["theta_DIV"]
        if theta_DIV < 0 || theta_DIV > 1
            push!(errors, "Dividend payout ratio (theta_DIV) outside [0, 1]: $theta_DIV")
        end
        println("  ✓ Dividend payout ratio (theta_DIV): $(round(theta_DIV, digits=4))")
    end

    if haskey(params, "theta_UB")
        theta_UB = params["theta_UB"]
        if theta_UB < 0 || theta_UB > 1
            push!(errors, "Unemployment replacement rate (theta_UB) outside [0, 1]: $theta_UB")
        end
        if theta_UB > 0.8
            push!(warnings, "Very high unemployment replacement rate: $(round(theta_UB, digits=3))")
        end
        println("  ✓ Unemployment replacement rate (theta_UB): $(round(theta_UB, digits=4))")
    end

    if haskey(params, "theta")
        println("  ✓ Theta parameter: $(round(params["theta"], digits=4))")
    end

    # Bank/credit parameters
    bank_params = ["zeta", "zeta_LTV", "zeta_b"]
    if any(haskey(params, p) for p in bank_params)
        print("  ✓ Bank parameters:")
        for param in bank_params
            if haskey(params, param)
                val = params[param]
                if param in ["zeta_LTV", "zeta_b"] && (val < 0 || val > 1)
                    push!(warnings, "$param outside [0, 1]: $val")
                end
                print(" $param=$(round(val, digits=3))")
            end
        end
        println()
    end

    # Taylor rule parameters
    println("\n  📊 Taylor Rule Parameters:")

    if haskey(params, "rho")
        rho = params["rho"]
        if rho < 0 || rho > 1
            push!(warnings, "Interest rate smoothing (rho) outside [0, 1]: $rho")
        end
        println("  ✓ Interest rate smoothing (rho): $(round(rho, digits=4))")
    end

    if haskey(params, "r_star")
        r_star = params["r_star"]
        if r_star < -0.01 || r_star > 0.02
            push!(warnings, "Equilibrium rate (r_star) unusual: $(round(r_star, digits=5))")
        end
        println("  ✓ Equilibrium rate (r_star): $(round(r_star, digits=5))")
    end

    if haskey(params, "xi_pi")
        xi_pi = params["xi_pi"]
        if xi_pi <= 0
            push!(errors, "Inflation response (xi_pi) must be positive (Taylor principle)")
        end
        println("  ✓ Inflation response (xi_pi): $(round(xi_pi, digits=4))")
    end

    if haskey(params, "xi_gamma")
        xi_gamma = params["xi_gamma"]
        if xi_gamma <= 0
            push!(warnings, "Output response (xi_gamma) should typically be positive: $(round(xi_gamma, digits=4))")
        end
        println("  ✓ Output response (xi_gamma): $(round(xi_gamma, digits=4))")
    end

    if haskey(params, "pi_star")
        pi_star = params["pi_star"]
        annual_pi_star = (1 + pi_star)^4 - 1
        if pi_star < 0.003 || pi_star > 0.008
            push!(warnings, "Inflation target unusual: $(round(annual_pi_star*100, digits=2))% annual")
        end
        println("  ✓ Inflation target (pi_star): $(round(pi_star, digits=5)) ($(round(annual_pi_star*100, digits=1))% annual)")
    end

    # EA AR(1) process parameters
    println("\n  📊 AR(1) Process Parameters:")

    ar_processes = [
        ("EA inflation", "alpha_pi_EA", "beta_pi_EA", "sigma_pi_EA"),
        ("EA output", "alpha_Y_EA", "beta_Y_EA", "sigma_Y_EA"),
        ("Government", "alpha_G", "beta_G", "sigma_G"),
        ("Exports", "alpha_E", "beta_E", "sigma_E"),
        ("Imports", "alpha_I", "beta_I", "sigma_I")
    ]

    for (label, alpha_name, beta_name, sigma_name) in ar_processes
        if haskey(params, beta_name) && haskey(params, sigma_name)
            beta_val = params[beta_name]
            sigma_val = params[sigma_name]

            if sigma_val <= 0
                push!(errors, "$label process: sigma must be positive")
            end
            if sigma_val > 0.1
                push!(warnings, "$label process: very high volatility (sigma = $(round(sigma_val, digits=4)))")
            end

            # Get alpha if available (already checked earlier for G, E, I)
            if haskey(params, alpha_name)
                alpha_val = params[alpha_name]
                println("  ✓ $label: alpha=$(round(alpha_val, digits=3)), beta=$(round(beta_val, digits=3)), sigma=$(round(sigma_val, digits=4))")
            else
                println("  ✓ $label: beta=$(round(beta_val, digits=3)), sigma=$(round(sigma_val, digits=4))")
            end
        end
    end

    # Print results
    if !isempty(errors)
        println("\n  ❌ ERRORS:")
        for err in errors
            println("    - $err")
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    end

    if isempty(errors) && isempty(warnings)
        println("\n  ✅ All parameter ranges look good!")
    end

    return isempty(errors)
end

function validate_production_parameters(params, initial_conditions, calibration_data)
    """Check production parameters (labor/capital productivity, wages, I-O matrix)

    IMPORTANT: Some parameters are affected by the scale factor used in calibration.
    - alpha_s (labor productivity) = output / scaled_employees → inflated by 1/scale
    - w_s (wages) = wages / scaled_employees → inflated by 1/scale
    - beta_s, kappa_s, a_sg are ratios → unaffected by scaling
    """

    println("\n" * "="^80)
    println("2. PRODUCTION PARAMETER CHECKS")
    println("="^80)

    errors = String[]
    warnings = String[]

    # Infer the scale factor from employee data
    if haskey(initial_conditions, "N_s") && haskey(calibration_data, "employees")
        inferred_scale = sum(initial_conditions["N_s"]) / sum(calibration_data["employees"][:, end])
        println("  ℹ️  Inferred scale factor: $(round(inferred_scale, digits=5)) (employees scaled by 1/$(round(Int, 1/inferred_scale)))")
    else
        inferred_scale = 1.0
        push!(warnings, "Could not infer scale factor, assuming scale=1.0")
    end

    # alpha_s: Labor productivity (SCALE-DEPENDENT)
    if haskey(params, "alpha_s")
        alpha_s = params["alpha_s"]

        # Check positivity
        if any(alpha_s .<= 0)
            push!(errors, "Non-positive labor productivity detected!")
        end

        # Scale-adjusted values (true million EUR/employee/quarter)
        alpha_s_true = alpha_s * inferred_scale

        # Check reasonable ranges (values are in million EUR, so 0.001 = 1K EUR)
        low_productivity = alpha_s_true .< 0.001  # Less than 1K EUR/employee/quarter
        high_productivity = alpha_s_true .> 0.500  # More than 500K EUR/employee/quarter

        if any(low_productivity)
            n_low = sum(low_productivity)
            push!(warnings, "$n_low sector(s) with very low labor productivity (<1,000 EUR/quarter)")
        end
        if any(high_productivity)
            n_high = sum(high_productivity)
            push!(warnings, "$n_high sector(s) with very high labor productivity (>500,000 EUR/quarter)")
        end

        println("  ✓ Labor productivity (alpha_s):")
        println("    Raw: [$(round(minimum(alpha_s), digits=0)), $(round(maximum(alpha_s), digits=0))] million EUR/scaled-employee/quarter")
        println("    True: [$(round(minimum(alpha_s_true)*1000, digits=0))K, $(round(maximum(alpha_s_true)*1000, digits=0))K] EUR/employee/quarter, mean: $(round(mean(alpha_s_true)*1000, digits=0))K")
    end

    # w_s: Wages per employee (SCALE-DEPENDENT)
    if haskey(params, "w_s")
        w_s = params["w_s"]

        # Check positivity
        if any(w_s .<= 0)
            push!(errors, "Non-positive wages detected!")
        end

        # Scale-adjusted values (true million EUR/employee/quarter)
        w_s_true = w_s * inferred_scale

        # Check reasonable ranges (values are in million EUR, so 0.002 = 2K EUR)
        low_wage = w_s_true .< 0.002  # Less than 2K EUR/quarter (~ min wage)
        high_wage = w_s_true .> 0.100  # More than 100K EUR/quarter

        if any(low_wage)
            n_low = sum(low_wage)
            push!(warnings, "$n_low sector(s) with very low wages (<2,000 EUR/quarter)")
        end
        if any(high_wage)
            n_high = sum(high_wage)
            push!(warnings, "$n_high sector(s) with very high wages (>100,000 EUR/quarter)")
        end

        println("  ✓ Wages per employee (w_s):")
        println("    Raw: [$(round(minimum(w_s), digits=0)), $(round(maximum(w_s), digits=0))] million EUR/scaled-employee/quarter")
        println("    True: [$(round(minimum(w_s_true)*1000, digits=0))K, $(round(maximum(w_s_true)*1000, digits=0))K] EUR/employee/quarter, mean: $(round(mean(w_s_true)*1000, digits=0))K")
    end

    # beta_s: Output multiplier (SCALE-INDEPENDENT)
    if haskey(params, "beta_s")
        beta_s = params["beta_s"]

        # Check if output exceeds intermediate consumption
        low_multiplier = beta_s .< 1.0
        if any(low_multiplier)
            n_low = sum(low_multiplier)
            push!(errors, "$n_low sector(s) with output < intermediate consumption (beta_s < 1.0)")
        end

        # Check for unusually high value added
        high_multiplier = beta_s .> 20.0
        if any(high_multiplier)
            n_high = sum(high_multiplier)
            push!(warnings, "$n_high sector(s) with very high output multiplier (>20)")
        end

        println("  ✓ Output multiplier (beta_s): [$(round(minimum(beta_s), digits=2)), $(round(maximum(beta_s), digits=2))], mean: $(round(mean(beta_s), digits=2))")
    end

    # kappa_s: Capital productivity (SCALE-INDEPENDENT)
    if haskey(params, "kappa_s")
        kappa_s = params["kappa_s"]

        # Check positivity
        if any(kappa_s .<= 0)
            push!(errors, "Non-positive capital productivity detected!")
        end

        # Check reasonable ranges
        low_capital = kappa_s .< 0.01
        high_capital = kappa_s .> 5.0

        if any(low_capital)
            n_low = sum(low_capital)
            push!(warnings, "$n_low sector(s) with very low capital productivity (<0.01)")
        end
        if any(high_capital)
            n_high = sum(high_capital)
            push!(warnings, "$n_high sector(s) with very high capital productivity (>5.0)")
        end

        println("  ✓ Capital productivity (kappa_s): [$(round(minimum(kappa_s), digits=3)), $(round(maximum(kappa_s), digits=3))], mean: $(round(mean(kappa_s), digits=3))")
    end

    # a_sg: Input-output coefficient matrix (SCALE-INDEPENDENT)
    if haskey(params, "a_sg")
        a_sg = params["a_sg"]
        G = size(a_sg, 1)

        # Check all elements in [0, 1]
        if any(a_sg .< 0) || any(a_sg .> 1)
            push!(errors, "Input-output coefficients outside [0, 1] range!")
        end

        # Check column sums (should be ~1.0)
        col_sums = sum(a_sg, dims=1)[:]
        valid_cols = abs.(col_sums .- 1.0) .< 0.05
        n_valid = sum(valid_cols)

        if n_valid < G
            push!(warnings, "$(G - n_valid) sector(s) with input-output column sum ≠ 1.0 (tolerance ±0.05)")
        end

        # Check for zero columns (sectors with no intermediate inputs)
        zero_cols = sum(a_sg, dims=1)[:] .== 0
        n_zero = sum(zero_cols)

        println("  ✓ Input-output matrix (a_sg): $n_valid/$G sectors have valid column sums")
        if n_zero > 0
            println("  ℹ️  $n_zero sector(s) with zero columns (no intermediate inputs)")
        end
    end

    # Print summary
    if !isempty(errors)
        println("\n  ❌ ERRORS:")
        for err in errors
            println("    - $err")
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    end

    if isempty(errors) && isempty(warnings)
        println("\n  ✅ All production parameters validated (scale-adjusted)!")
    end

    return isempty(errors)
end

function validate_distribution_vectors(params)
    """Check that distribution vectors sum to 1.0 and are properly normalized"""

    println("\n" * "="^80)
    println("3. DISTRIBUTION VECTOR CHECKS")
    println("="^80)

    errors = String[]
    warnings = String[]

    # Helper function to validate a distribution vector
    function check_distribution(vec, name, tolerance=0.001)
        # Handle both Vector and Matrix types
        if isa(vec, Matrix)
            vec = vec[:, 1]  # Take first column if matrix
        end

        # Check all elements in [0, 1]
        if any(vec .< 0) || any(vec .> 1)
            push!(errors, "$name has elements outside [0, 1]")
        end

        # Check sum
        vec_sum = sum(vec)
        if abs(vec_sum - 1.0) > tolerance
            push!(errors, "$name doesn't sum to 1.0 (sum = $(round(vec_sum, digits=5)))")
        else
            println("  ✓ $name: sum = $(round(vec_sum, digits=5))")
        end

        # Count zeros
        n_zeros = sum(vec .== 0)
        if n_zeros > 0
            println("    ℹ️  $n_zeros sectors with zero shares")
        end

        # Check concentration
        max_share = maximum(vec)
        if max_share > 0.5
            max_idx = argmax(vec)
            println("    ℹ️  Concentrated in sector $max_idx ($(round(max_share*100, digits=1))%)")
        end

        # Return top 3 sectors if needed
        if name == "Household consumption (b_HH_g)"
            sorted_idx = sortperm(vec, rev=true)
            top3 = sorted_idx[1:min(3, length(sorted_idx))]
            println("    Top 3 sectors: ", join(["$i ($(round(vec[i]*100, digits=1))%)" for i in top3], ", "))
        end

        return vec_sum
    end

    # Check each distribution vector
    dist_vectors = [
        ("b_CF_g", "Capital formation (b_CF_g)"),
        ("b_CFH_g", "Housing investment (b_CFH_g)"),
        ("b_HH_g", "Household consumption (b_HH_g)"),
        ("c_G_g", "Government consumption (c_G_g)"),
        ("c_E_g", "Export distribution (c_E_g)"),
        ("c_I_g", "Import distribution (c_I_g)")
    ]

    for (param_name, label) in dist_vectors
        if haskey(params, param_name)
            check_distribution(params[param_name], label)
        end
    end

    # Print summary
    if !isempty(errors)
        println("\n  ❌ ERRORS:")
        for err in errors
            println("    - $err")
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    end

    if isempty(errors) && isempty(warnings)
        println("\n  ✅ All distribution vectors sum to 1.0!")
    end

    return isempty(errors)
end

function validate_covariance_matrix(params)
    """Check covariance matrix is valid (symmetric, positive semi-definite)"""

    println("\n" * "="^80)
    println("4. COVARIANCE MATRIX CHECKS")
    println("="^80)

    errors = String[]
    warnings = String[]

    if !haskey(params, "C")
        println("  ℹ️  No covariance matrix (C) found in parameters")
        return true
    end

    C = params["C"]

    # Check it's a matrix
    if !isa(C, Matrix)
        push!(errors, "Covariance matrix C is not a Matrix type")
        return false
    end

    # Check dimensions
    n_dim = size(C, 1)
    if size(C, 2) != n_dim
        push!(errors, "Covariance matrix C is not square: $(size(C))")
        return false
    end

    println("  ℹ️  Covariance matrix dimensions: $(n_dim)×$(n_dim)")

    # Check symmetry
    if maximum(abs.(C - C')) > 1e-10
        push!(errors, "Covariance matrix C is not symmetric")
    else
        println("  ✓ Matrix C is symmetric")
    end

    # Check positive semi-definite (all eigenvalues >= 0)
    eigenvals = eigvals(C)
    if any(eigenvals .< -1e-10)  # Allow small numerical errors
        push!(errors, "Covariance matrix C is not positive semi-definite (negative eigenvalues)")
        println("    Eigenvalues: ", eigenvals)
    else
        println("  ✓ Matrix C is positive semi-definite")
        println("    Eigenvalues: [", join([round(λ, digits=6) for λ in eigenvals], ", "), "]")
    end

    # Print diagonal elements (variances)
    println("  ✓ Diagonal elements (variances): [", join([round(C[i,i], digits=6) for i in 1:n_dim], ", "), "]")

    # Print off-diagonal correlations
    if n_dim > 1
        correlations = Float64[]
        for i in 1:(n_dim-1)
            for j in (i+1):n_dim
                if C[i,i] > 0 && C[j,j] > 0
                    corr = C[i,j] / sqrt(C[i,i] * C[j,j])
                    push!(correlations, corr)
                end
            end
        end
        if !isempty(correlations)
            println("  ℹ️  Off-diagonal correlations: [", join([round(ρ, digits=2) for ρ in correlations], ", "), "]")
        end
    end

    # Print summary
    if !isempty(errors)
        println("\n  ❌ ERRORS:")
        for err in errors
            println("    - $err")
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    end

    if isempty(errors) && isempty(warnings)
        println("\n  ✅ Covariance matrix is valid!")
    end

    return isempty(errors)
end

function validate_accounting_identities(params, initial_conditions, calibration_data, figaro, G)
    """Check key accounting identities hold"""

    println("\n" * "="^80)
    println("5. ACCOUNTING IDENTITY CHECKS")
    println("="^80)

    errors = String[]

    # Get final year data
    T = size(figaro["intermediate_consumption"], 3)
    IC = figaro["intermediate_consumption"][:, :, end]

    # GDP identity: Y = C + I + G + X - M
    household_consumption = sum(figaro["household_consumption"][:, end])
    capitalformation = sum(figaro["capitalformation"][:, end])
    government_consumption = sum(figaro["government_consumption"][:, end])
    exports = sum(figaro["exports"][:, end])
    imports = sum(figaro["imports"][:, end])

    calculated_gdp = household_consumption + capitalformation + government_consumption + exports - imports

    # Get nominal GDP from initial conditions if available
    if haskey(initial_conditions, "Y")
        nominal_gdp = initial_conditions["Y"][1]  # First value
        relative_error = abs(calculated_gdp - nominal_gdp) / nominal_gdp

        if relative_error < 0.02
            println("  ✓ GDP identity (C+I+G+X-M): $(round(relative_error*100, digits=2))% error")
        else
            println("  ⚠️  GDP identity error: $(round(relative_error*100, digits=2))%")
            push!(errors, "GDP identity discrepancy > 2%")
        end
    end

    # Output identity: Output = IC + Value Added Components
    output = dropdims(sum(IC, dims=1), dims=1) +
             figaro["taxes_products"][:, end] +
             figaro["taxes_production"][:, end] +
             figaro["compensation_employees"][:, end] +
             figaro["operating_surplus"][:, end]

    println("  ✓ Output calculated from production side")

    # Check no negative outputs
    if any(output .< 0)
        push!(errors, "Negative output values detected!")
    end

    # Government budget (rough check)
    if haskey(calibration_data, "government_deficit")
        govt_deficit = calibration_data["government_deficit"][end]
        println("  ℹ️  Government deficit: $(round(govt_deficit, digits=2)) million EUR")
    end

    if isempty(errors)
        println("\n  ✅ All accounting identities check out!")
    else
        println("\n  ❌ ERRORS:")
        for err in errors
            println("    - $err")
        end
    end

    return isempty(errors)
end

function validate_time_series_properties(initial_conditions)
    """Check time series have reasonable statistical properties"""

    println("\n" * "="^80)
    println("6. TIME SERIES PROPERTY CHECKS")
    println("="^80)

    warnings = String[]

    # Check growth rates
    for var in ["gamma", "gamma_G", "gamma_E", "gamma_I"]
        if haskey(initial_conditions, var)
            values = initial_conditions[var]
            clean_values = values[.!isnan.(values)]

            if length(clean_values) > 0
                μ = mean(clean_values)
                σ = std(clean_values)
                min_val = minimum(clean_values)
                max_val = maximum(clean_values)

                # Check for extreme values
                if any(abs.(clean_values) .> 0.5)
                    push!(warnings, "$var has quarterly growth > 50%")
                end

                # Check for outliers (>3 std from mean)
                outliers = abs.(clean_values .- μ) .> 3*σ
                if any(outliers)
                    push!(warnings, "$var has $(sum(outliers)) outliers (>3σ)")
                end

                println(@sprintf("  ✓ %-10s: mean=%6.2f%%, std=%5.2f%%, range=[%6.2f%%, %6.2f%%]",
                                 var, μ*100, σ*100, min_val*100, max_val*100))
            end
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    else
        println("\n  ✅ Time series properties look reasonable!")
    end

    return true
end

function validate_cross_sectional(params, calibration_data, figaro, G)
    """Check cross-sectional distributions are reasonable"""

    println("\n" * "="^80)
    println("7. CROSS-SECTIONAL REASONABLENESS CHECKS")
    println("="^80)

    warnings = String[]

    # Output shares
    output = dropdims(sum(figaro["intermediate_consumption"][:, :, end], dims=1), dims=1) +
             figaro["taxes_products"][:, end] +
             figaro["taxes_production"][:, end] +
             figaro["compensation_employees"][:, end] +
             figaro["operating_surplus"][:, end]

    output_shares = output ./ sum(output)

    # No sector should dominate
    max_share = maximum(output_shares)
    if max_share > 0.5
        push!(warnings, "One sector accounts for $(round(max_share*100, digits=1))% of output")
    end
    println("  ✓ Largest sector share: $(round(max_share*100, digits=1))%")

    # Wage share of value added
    total_output = sum(output)
    total_IC = sum(figaro["intermediate_consumption"][:, :, end])
    value_added = total_output - total_IC

    if haskey(figaro, "compensation_employees")
        wage_share = sum(figaro["compensation_employees"][:, end]) / value_added

        if wage_share < 0.4 || wage_share > 0.8
            push!(warnings, "Unusual wage share of VA: $(round(wage_share*100, digits=1))%")
        end
        println("  ✓ Wage share of value added: $(round(wage_share*100, digits=1))%")

        # Capital share complement
        capital_share = sum(figaro["operating_surplus"][:, end]) / value_added
        println("  ✓ Capital share of value added: $(round(capital_share*100, digits=1))%")
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    else
        println("\n  ✅ Cross-sectional distributions look reasonable!")
    end

    return true
end

function validate_firm_employment(calibration_data, params, initial_conditions, G)
    """Check firm and employment data are reasonable"""

    println("\n" * "="^80)
    println("8. FIRM & EMPLOYMENT REASONABLENESS CHECKS")
    println("="^80)

    warnings = String[]

    if haskey(calibration_data, "employees") && haskey(calibration_data, "firms")
        employees = calibration_data["employees"]
        firms = calibration_data["firms"]

        # Get last year
        T_emp = size(employees, 2)
        T_firms = size(firms, 2)

        # Check average firm sizes
        for s in 1:G
            emp_val = employees[s, end]
            firm_val = firms[s, end]

            if !ismissing(emp_val) && !ismissing(firm_val) && firm_val > 0
                avg_size = emp_val / firm_val

                if avg_size < 1
                    push!(warnings, "Sector $s: avg firm size < 1 employee ($(round(avg_size, digits=2)))")
                elseif avg_size > 10000
                    push!(warnings, "Sector $s: avg firm size > 10,000 employees ($(round(avg_size, digits=0)))")
                end
            end
        end

        total_employment = sum(skipmissing(employees[:, end]))
        total_firms = sum(skipmissing(firms[:, end]))
        economy_avg = total_employment / total_firms

        println("  ✓ Total employment: $(round(total_employment, digits=0))")
        println("  ✓ Total firms: $(round(total_firms, digits=0))")
        println("  ✓ Economy-wide average firm size: $(round(economy_avg, digits=1)) employees")

        # Check against population if available
        if haskey(calibration_data, "population")
            pop = calibration_data["population"][end]
            employment_rate = total_employment / pop

            if employment_rate > 0.8
                push!(warnings, "Employment rate > 80% of population ($(round(employment_rate*100, digits=1))%)")
            end
            println("  ✓ Employment / population ratio: $(round(employment_rate*100, digits=1))%")
        end
    end

    # Check I_s consistency with scaled firms
    if haskey(params, "I_s") && haskey(initial_conditions, "N_s") && haskey(calibration_data, "firms")
        I_s = params["I_s"]
        firms_data = calibration_data["firms"][:, end]

        # Infer scale from employment data
        if haskey(calibration_data, "employees")
            employees_data = calibration_data["employees"][:, end]
            N_s = initial_conditions["N_s"]
            inferred_scale = sum(N_s) / sum(employees_data)

            # Check sector-by-sector match
            scaled_firms = firms_data * inferred_scale
            max_diff = maximum(abs.(I_s - scaled_firms))

            if max_diff > 0.5
                push!(warnings, "I_s doesn't match scaled firms (max diff: $(round(max_diff, digits=1)))")
            else
                println("  ✓ I_s parameter consistent with scaled firm data (max diff: $(round(max_diff, digits=2)))")
            end
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    else
        println("\n  ✅ Firm and employment data look reasonable!")
    end

    return true
end

function validate_financial_stocks(calibration_data, params)
    """Check financial stock-flow consistency"""

    println("\n" * "="^80)
    println("10. FINANCIAL STOCK-FLOW CONSISTENCY CHECKS")
    println("="^80)

    warnings = String[]

    # Firm debt and interest
    if haskey(calibration_data, "firm_debt_quarterly") && haskey(calibration_data, "firm_interest_quarterly")
        debt = calibration_data["firm_debt_quarterly"][end]
        interest = calibration_data["firm_interest_quarterly"][end]

        if debt > 0
            implied_rate = interest / debt

            if implied_rate < 0
                push!(warnings, "Negative implied interest rate on firm debt")
            elseif implied_rate > 0.05
                push!(warnings, "Very high quarterly interest rate: $(round(implied_rate*100, digits=2))%")
            end

            println("  ✓ Firm debt: $(round(debt, digits=0)) million EUR")
            println("  ✓ Firm interest: $(round(interest, digits=0)) million EUR")
            println("  ✓ Implied quarterly rate: $(round(implied_rate*100, digits=3))%")
        end
    end

    # Government debt and interest
    if haskey(calibration_data, "government_debt_quarterly") && haskey(calibration_data, "interest_government_debt_quarterly")
        debt = calibration_data["government_debt_quarterly"][end]
        interest = calibration_data["interest_government_debt_quarterly"][end]

        if debt > 0
            implied_rate = interest / debt

            if implied_rate < 0
                push!(warnings, "Negative implied interest rate on government debt")
            elseif implied_rate > 0.05
                push!(warnings, "Very high quarterly govt interest rate: $(round(implied_rate*100, digits=2))%")
            end

            println("  ✓ Government debt: $(round(debt, digits=0)) million EUR")
            println("  ✓ Government interest: $(round(interest, digits=0)) million EUR")
            println("  ✓ Implied quarterly rate: $(round(implied_rate*100, digits=3))%")
        end
    end

    if !isempty(warnings)
        println("\n  ⚠️  WARNINGS:")
        for warn in warnings
            println("    - $warn")
        end
    else
        println("\n  ✅ Financial stock-flow consistency looks good!")
    end

    return true
end

function create_diagnostic_plots(params, initial_conditions, calibration_data, figaro, G)
    """Create diagnostic visualization plots"""

    println("\n" * "="^80)
    println("11. GENERATING DIAGNOSTIC PLOTS")
    println("="^80)

    plots_dir = joinpath(@__DIR__, "validation_plots")
    mkpath(plots_dir)

    # A. Time series plots
    if haskey(initial_conditions, "gamma")
        p1 = plot(initial_conditions["gamma"], title="Growth Rates",
                  ylabel="Quarterly growth", xlabel="Quarter", label="GDP", legend=:best)

        if haskey(initial_conditions, "gamma_G")
            plot!(p1, initial_conditions["gamma_G"], label="Government")
        end
        if haskey(initial_conditions, "gamma_E")
            plot!(p1, initial_conditions["gamma_E"], label="Exports")
        end
        if haskey(initial_conditions, "gamma_I")
            plot!(p1, initial_conditions["gamma_I"], label="Imports")
        end

        savefig(p1, joinpath(plots_dir, "growth_rates.png"))
        println("  ✓ Saved growth_rates.png")
    end

    # B. Parameter distributions
    if haskey(params, "tau_Y_s")
        p2 = histogram(params["tau_Y_s"], title="Product Tax Rates by Sector",
                       xlabel="Tax rate", ylabel="Number of sectors", legend=false)
        savefig(p2, joinpath(plots_dir, "tax_rates.png"))
        println("  ✓ Saved tax_rates.png")
    end

    if haskey(params, "delta_s")
        p3 = histogram(params["delta_s"], title="Depreciation Rates by Sector",
                       xlabel="Depreciation rate", ylabel="Number of sectors", legend=false)
        savefig(p3, joinpath(plots_dir, "depreciation_rates.png"))
        println("  ✓ Saved depreciation_rates.png")
    end

    # C. Firm size distribution
    if haskey(calibration_data, "employees") && haskey(calibration_data, "firms")
        firm_sizes = calibration_data["employees"][:, end] ./ calibration_data["firms"][:, end]
        valid_sizes = firm_sizes[.!ismissing.(firm_sizes) .&& (firm_sizes .> 0)]

        if length(valid_sizes) > 0
            p4 = histogram(valid_sizes, title="Firm Size Distribution",
                           xlabel="Employees per firm", ylabel="Number of sectors", legend=false)
            savefig(p4, joinpath(plots_dir, "firm_sizes.png"))
            println("  ✓ Saved firm_sizes.png")
        end
    end

    # D. Production parameter plots (scale-adjusted)
    # Infer scale factor for adjustments
    if haskey(initial_conditions, "N_s") && haskey(calibration_data, "employees")
        inferred_scale = sum(initial_conditions["N_s"]) / sum(calibration_data["employees"][:, end])

        # D1. Labor productivity (scale-adjusted, in thousands EUR)
        if haskey(params, "alpha_s")
            alpha_s_true_k = params["alpha_s"] * inferred_scale * 1000  # Convert to thousands EUR
            p5 = histogram(alpha_s_true_k, title="Labor Productivity by Sector (scale-adjusted)",
                           xlabel="Thousands EUR/employee/quarter", ylabel="Number of sectors", legend=false)
            savefig(p5, joinpath(plots_dir, "labor_productivity.png"))
            println("  ✓ Saved labor_productivity.png")
        end

        # D2. Wages per employee (scale-adjusted, in thousands EUR)
        if haskey(params, "w_s")
            w_s_true_k = params["w_s"] * inferred_scale * 1000  # Convert to thousands EUR
            p6 = histogram(w_s_true_k, title="Wages per Employee by Sector (scale-adjusted)",
                           xlabel="Thousands EUR/employee/quarter", ylabel="Number of sectors", legend=false)
            savefig(p6, joinpath(plots_dir, "wages_per_employee.png"))
            println("  ✓ Saved wages_per_employee.png")
        end
    end

    # D3. Output multiplier
    if haskey(params, "beta_s")
        p7 = histogram(params["beta_s"], title="Output/Intermediate Consumption Ratio",
                       xlabel="Output multiplier", ylabel="Number of sectors", legend=false)
        savefig(p7, joinpath(plots_dir, "output_multiplier.png"))
        println("  ✓ Saved output_multiplier.png")
    end

    # D4. Capital productivity
    if haskey(params, "kappa_s")
        p8 = histogram(params["kappa_s"], title="Capital Productivity by Sector",
                       xlabel="Output/Capital ratio", ylabel="Number of sectors", legend=false)
        savefig(p8, joinpath(plots_dir, "capital_productivity.png"))
        println("  ✓ Saved capital_productivity.png")
    end

    # D5. Input-output matrix heatmap
    if haskey(params, "a_sg")
        p9 = heatmap(params["a_sg"],
                     title="Input-Output Coefficient Matrix",
                     xlabel="Supplying sector", ylabel="Using sector",
                     color=:viridis, clims=(0, 1))
        savefig(p9, joinpath(plots_dir, "input_output_matrix.png"))
        println("  ✓ Saved input_output_matrix.png")
    end

    # E. NEW: Aggregate tax rates bar chart
    tax_params = [("tau_INC", "Income"), ("tau_FIRM", "Corporate"), ("tau_VAT", "VAT"),
                  ("tau_SIF", "Employer SI"), ("tau_SIW", "Worker SI"),
                  ("tau_EXPORT", "Export"), ("tau_CF", "Capital Form"), ("tau_G", "Gov Cons")]
    tax_labels = String[]
    tax_values = Float64[]
    for (param, label) in tax_params
        if haskey(params, param)
            push!(tax_labels, label)
            push!(tax_values, params[param])
        end
    end
    if !isempty(tax_values)
        p10 = bar(tax_labels, tax_values,
                  title="Aggregate Tax Rates",
                  xlabel="Tax type", ylabel="Rate",
                  legend=false, ylims=(0, maximum(tax_values)*1.1))
        savefig(p10, joinpath(plots_dir, "aggregate_tax_rates.png"))
        println("  ✓ Saved aggregate_tax_rates.png")
    end

    # F. NEW: Distribution vectors comparison
    dist_vectors = [("b_CF_g", "Cap Form"), ("b_CFH_g", "Housing"),
                    ("b_HH_g", "HH Cons"), ("c_G_g", "Gov Cons"),
                    ("c_E_g", "Exports"), ("c_I_g", "Imports")]
    dist_labels = String[]
    dist_data = []
    for (param, label) in dist_vectors
        if haskey(params, param)
            push!(dist_labels, label)
            vec = params[param]
            if isa(vec, Matrix)
                vec = vec[:, 1]
            end
            push!(dist_data, vec)
        end
    end
    if !isempty(dist_data)
        # Show top 5 sectors for each distribution
        p11 = plot(layout=(length(dist_data), 1), size=(800, 300*length(dist_data)))
        for (i, (vec, label)) in enumerate(zip(dist_data, dist_labels))
            sorted_idx = sortperm(vec, rev=true)[1:min(5, length(vec))]
            bar!(p11, string.(sorted_idx), vec[sorted_idx],
                 title="$label Distribution (Top 5 Sectors)",
                 xlabel="Sector", ylabel="Share",
                 legend=false, subplot=i)
        end
        savefig(p11, joinpath(plots_dir, "distribution_vectors.png"))
        println("  ✓ Saved distribution_vectors.png")
    end

    # G. NEW: Covariance matrix heatmap (if exists)
    if haskey(params, "C")
        C = params["C"]
        if isa(C, Matrix) && size(C, 1) > 1
            p12 = heatmap(C,
                          title="Covariance Matrix",
                          xlabel="Variable", ylabel="Variable",
                          color=:RdBu, aspect_ratio=1)
            savefig(p12, joinpath(plots_dir, "covariance_matrix.png"))
            println("  ✓ Saved covariance_matrix.png")
        end
    end

    println("\n  ✅ Diagnostic plots saved to '$plots_dir/'")

    return true
end

function print_calibration_summary(params, initial_conditions, calibration_data, figaro, G)
    """Print comprehensive calibration summary"""

    println("\n" * "="^80)
    println("12. CALIBRATION SUMMARY REPORT")
    println("="^80)

    # Key aggregates
    println("\n📊 Key Aggregates (Final Year):")

    T = size(figaro["intermediate_consumption"], 3)

    household_consumption = sum(figaro["household_consumption"][:, end])
    capitalformation = sum(figaro["capitalformation"][:, end])
    government_consumption = sum(figaro["government_consumption"][:, end])
    exports = sum(figaro["exports"][:, end])
    imports = sum(figaro["imports"][:, end])
    gdp = household_consumption + capitalformation + government_consumption + exports - imports

    println(@sprintf("  GDP: %.0f million EUR", gdp))
    println(@sprintf("    Consumption (C): %.0f (%.1f%%)", household_consumption, household_consumption/gdp*100))
    println(@sprintf("    Investment (I): %.0f (%.1f%%)", capitalformation, capitalformation/gdp*100))
    println(@sprintf("    Government (G): %.0f (%.1f%%)", government_consumption, government_consumption/gdp*100))
    println(@sprintf("    Exports (X): %.0f (%.1f%%)", exports, exports/gdp*100))
    println(@sprintf("    Imports (M): %.0f (%.1f%%)", imports, imports/gdp*100))

    if haskey(calibration_data, "employees")
        total_emp = sum(skipmissing(calibration_data["employees"][:, end]))
        println(@sprintf("  Total Employment: %.0f persons", total_emp))
    end

    if haskey(calibration_data, "firms")
        total_firms = sum(skipmissing(calibration_data["firms"][:, end]))
        println(@sprintf("  Total Firms: %.0f", total_firms))
    end

    # Key parameters
    println("\n📈 Key Parameters:")

    for param in ["r_bar", "r_G", "mu", "alpha_G", "beta_G", "alpha_E", "alpha_I"]
        if haskey(params, param)
            println(@sprintf("  %-10s: %.4f", param, params[param]))
        end
    end

    # Growth rate summary
    if haskey(initial_conditions, "gamma")
        println("\n📉 Average Quarterly Growth Rates:")

        for var in ["gamma", "gamma_G", "gamma_E", "gamma_I"]
            if haskey(initial_conditions, var)
                values = initial_conditions[var]
                clean_values = values[.!isnan.(values)]
                if length(clean_values) > 0
                    μ = mean(clean_values)
                    println(@sprintf("  %-10s: %.2f%%", var, μ*100))
                end
            end
        end
    end

    println("\n" * "="^80)
end

# Main validation function
function validate_calibration(geo::String, calibration_year::Int, calibration_quarter::Int=4)
    """
    Run complete validation suite on calibration results for a specific quarter.

    # Arguments
    - `geo`: Country code (e.g., "NL", "AT")
    - `calibration_year`: Year to validate (e.g., 2020)
    - `calibration_quarter`: Quarter to validate (1-4), defaults to Q4

    # Returns
    - `true` if all checks pass, `false` otherwise
    """

    println("\n" * "="^80)
    println("VALIDATING CALIBRATION FOR $(geo) $(calibration_year)Q$(calibration_quarter)")
    println("="^80)

    # Load calibration results
    println("\nLoading calibration data...")

    params, initial_conditions, calibration_object = load_calibration_for_validation(
        geo, calibration_year, calibration_quarter
    )

    # Extract calibration_data and figaro from calibration_object
    calibration_data = calibration_object.calibration
    figaro = calibration_object.figaro

    G = length(params["w_s"])

    # Run all validation checks
    checks = Dict()

    checks["parameter_ranges"] = validate_parameter_ranges(params, G)
    checks["production_parameters"] = validate_production_parameters(params, initial_conditions, calibration_data)
    checks["distribution_vectors"] = validate_distribution_vectors(params)
    checks["covariance_matrix"] = validate_covariance_matrix(params)
    checks["accounting_identities"] = validate_accounting_identities(params, initial_conditions, calibration_data, figaro, G)
    checks["time_series"] = validate_time_series_properties(initial_conditions)
    checks["cross_sectional"] = validate_cross_sectional(params, calibration_data, figaro, G)
    checks["firm_employment"] = validate_firm_employment(calibration_data, params, initial_conditions, G)
    checks["financial_stocks"] = validate_financial_stocks(calibration_data, params)
    checks["diagnostic_plots"] = create_diagnostic_plots(params, initial_conditions, calibration_data, figaro, G)

    # Print summary
    print_calibration_summary(params, initial_conditions, calibration_data, figaro, G)

    # Final verdict
    all_passed = all(values(checks))

    println("\n" * "="^80)
    if all_passed
        println("✅ VALIDATION PASSED - All checks completed successfully!")
    else
        println("⚠️  VALIDATION COMPLETED WITH WARNINGS - Review output above")
    end
    println("="^80 * "\n")

    return all_passed
end

# Run validation if script is executed directly
if abspath(PROGRAM_FILE) == @__FILE__
    if length(ARGS) < 2
        println("Usage: julia test/test_calibration_validation.jl GEO YEAR [QUARTER]")
        println("Example: julia test/test_calibration_validation.jl NL 2020 4")
        println("         julia test/test_calibration_validation.jl NL 2020    # defaults to Q4")
        exit(1)
    end

    geo = ARGS[1]
    calibration_year = parse(Int, ARGS[2])
    calibration_quarter = length(ARGS) >= 3 ? parse(Int, ARGS[3]) : 4

    success = validate_calibration(geo, calibration_year, calibration_quarter)
    exit(success ? 0 : 1)
end
