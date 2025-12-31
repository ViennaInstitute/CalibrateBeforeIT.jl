"""
Netherlands-specific OCM (Optimal Consumption Model) calibration function.

This variant implements the model where households directly own firm equity,
as opposed to the standard BeforeIT model where K_H represents physical dwellings.

Key differences from standard calibration:
- Mixed income is added to compensation_employees (self-employed treated as employees)
- Operating surplus is NOT reduced by capital consumption
- K_H = firm equity (D_I + sum(fixed_assets) - L_I), not sum(dwellings)
- H_act = sum(employees) + unemployed (excludes firm owners)
- tau_INC denominator excludes property_income and mixed_income
- theta_DIV numerator uses property_income only (excludes mixed_income)
- Additional outputs: nu, scale, unscaled values, full time series, index_prices
"""

import BeforeIT as Bit

function get_params_and_initial_conditions_netherlands_ocm(
    calibration_object,
    calibration_date;
    scale = 0.001,
    composite_rk_path::Union{String, Nothing} = nothing
)
    calibration_data = calibration_object.calibration
    figaro = calibration_object.figaro
    data = calibration_object.data
    ea = calibration_object.ea
    max_calibration_date = calibration_object.max_calibration_date
    estimation_date = calibration_object.estimation_date

    # Calculate GDP deflator from levels
    data["gdp_deflator_quarterly"] = data["nominal_gdp_quarterly"] ./ data["real_gdp_quarterly"]
    ea["gdp_deflator_quarterly"] = ea["nominal_gdp_quarterly"] ./ ea["real_gdp_quarterly"]

    # Time indices (using CalibrateBeforeIT.jl Vector convention)
    T_calibration = findall(
        calibration_data["years_num"] .== date2num(DateTime(year(min(calibration_date, max_calibration_date)), 12, 31)),
    )[1]
    T_calibration_quarterly = findall(calibration_data["quarters_num"] .== date2num(calibration_date))[1]
    T_estimation_exo = findall(data["quarters_num"] .== date2num(estimation_date))[1]
    T_calibration_exo = findall(data["quarters_num"] .== date2num(calibration_date))[1]
    T_calibration_exo_max = length(data["quarters_num"])

    # Extract base data from FIGARO IO tables
    intermediate_consumption = figaro["intermediate_consumption"][:, :, T_calibration]
    G = size(intermediate_consumption)[1]
    S = G
    household_consumption = figaro["household_consumption"][:, T_calibration]
    fixed_capitalformation = figaro["fixed_capitalformation"][:, T_calibration]
    exports = figaro["exports"][:, T_calibration]
    compensation_employees = figaro["compensation_employees"][:, T_calibration]
    operating_surplus = figaro["operating_surplus"][:, T_calibration]
    government_consumption = figaro["government_consumption"][:, T_calibration]
    taxes_production = figaro["taxes_production"][:, T_calibration]
    taxes_products = figaro["taxes_products"][:, T_calibration]
    taxes_products_household = figaro["taxes_products_household"][T_calibration]
    taxes_products_fixed_capitalformation = figaro["taxes_products_capitalformation"][T_calibration]
    taxes_products_government = figaro["taxes_products_government"][T_calibration]

    # Taxes on exports - use FIGARO data if available, otherwise zero
    taxes_products_export = if haskey(figaro, "taxes_products_export") && length(figaro["taxes_products_export"]) >= T_calibration
        figaro["taxes_products_export"][T_calibration]
    else
        0.0
    end

    # Extract calibration data
    household_cash_quarterly = calibration_data["household_cash_quarterly"][T_calibration_quarterly]
    property_income = calibration_data["property_income"][T_calibration]
    mixed_income = calibration_data["mixed_income"][T_calibration]
    firm_cash_quarterly = calibration_data["firm_cash_quarterly"][T_calibration_quarterly]
    firm_debt_quarterly = calibration_data["firm_debt_quarterly"][T_calibration_quarterly]
    government_debt_quarterly = calibration_data["government_debt_quarterly"][T_calibration_quarterly]
    social_benefits = calibration_data["social_benefits"][T_calibration]
    unemployment_benefits = calibration_data["unemployment_benefits"][T_calibration]
    pension_benefits = calibration_data["pension_benefits"][T_calibration]
    corporate_tax = calibration_data["corporate_tax"][T_calibration]
    wages_by_sector = calibration_data["wages_by_sector"][:, T_calibration]  # Sectoral wages (D11)
    social_contributions = calibration_data["social_contributions"][T_calibration]
    income_tax = calibration_data["income_tax"][T_calibration]
    capital_taxes = calibration_data["capital_taxes"][T_calibration]
    bank_equity_quarterly = calibration_data["bank_equity_quarterly"][T_calibration_quarterly]
    government_deficit = calibration_data["government_deficit"][T_calibration]
    firms = calibration_data["firms"][:, T_calibration]
    employees = calibration_data["employees"][:, T_calibration]
    capital_consumption = calibration_data["capital_consumption"][:, T_calibration]

    # Check if quarterly data available
    has_quarterly_firm_interest = haskey(calibration_data, "firm_interest_quarterly")
    has_quarterly_govt_interest = haskey(calibration_data, "interest_government_debt_quarterly")
    has_quarterly_govt_deficit = haskey(calibration_data, "government_deficit_quarterly")

    firm_interest_quarterly = if has_quarterly_firm_interest
        calibration_data["firm_interest_quarterly"][T_calibration_quarterly]
    else
        calibration_data["firm_interest"][T_calibration]
    end

    interest_government_debt_quarterly = if has_quarterly_govt_interest
        calibration_data["interest_government_debt_quarterly"][T_calibration_quarterly]
    else
        calibration_data["interest_government_debt"][T_calibration]
    end

    government_deficit_quarterly = if has_quarterly_govt_deficit
        calibration_data["government_deficit_quarterly"][T_calibration_quarterly]
    else
        government_deficit  # Use annual value (will be scaled by timescale later)
    end

    # Get unemployed and inactive counts
    # Prefer census counts if available, otherwise calculate from rates
    if haskey(calibration_data, "unemployed_census")
        unemployed_raw = calibration_data["unemployed_census"]
    else
        unemployment_rate_quarterly = data["unemployment_rate_quarterly"][T_calibration_exo]
        unemployed_raw = matlab_round((unemployment_rate_quarterly * sum(employees)) / (1 - unemployment_rate_quarterly))
    end

    if haskey(calibration_data, "inactive_census")
        inactive_raw = calibration_data["inactive_census"]
    else
        population = calibration_data["population"][T_calibration]
        inactive_raw = population - sum(max.(max.(1, firms), employees)) - unemployed_raw - sum(max.(1, firms)) - 1
    end

    # Interest rate
    r_bar = (data["euribor"][T_calibration_exo] .+ 1.0) .^ (1.0 / 4.0) .- 1
    omega = 0.85

    # Save original compensation_employees before OCM adjustment (needed for employers_social_contributions)
    compensation_employees_original = copy(compensation_employees)

    # =========================================================================
    # OCM KEY DIFFERENCE #1: Add mixed income to compensation
    # =========================================================================
    # Self-employed income is distributed proportionally across sectors
    income_of_self_employed_per_sector = mixed_income * (compensation_employees ./ sum(compensation_employees))
    compensation_employees = compensation_employees + income_of_self_employed_per_sector

    # Zero out sector-level product taxes (same as standard calibration)
    taxes_products = zeros(eltype(taxes_products), size(taxes_products))

    # Calculate output from accounting identity
    # OCM KEY DIFFERENCE #2: capital_consumption is NOT added here
    intermediate_consumption = max.(0, intermediate_consumption)
    output =
        sum(intermediate_consumption, dims = 1)' .+ taxes_products .+ taxes_production .+ compensation_employees .+
        operating_surplus .+ capital_consumption
    output = output[:, 1]

    # Fixed assets handling
    if size(calibration_data["fixed_assets"])[1] == G &&
        size(calibration_data["dwellings"])[1] == G
        fixed_assets = calibration_data["fixed_assets"][:, T_calibration]
        dwellings = calibration_data["dwellings"][:, T_calibration]
        fixed_assets_other_than_dwellings = fixed_assets - dwellings
    else
        fixed_assets_total = calibration_data["fixed_assets"][T_calibration]
        dwellings_total = calibration_data["dwellings"][T_calibration]
        fixed_assets_eu7 = calibration_data["fixed_assets_eu7"][:, T_calibration]
        dwellings_eu7 = calibration_data["dwellings_eu7"][:, T_calibration]
        nominal_nace64_output_eu7 = calibration_data["nominal_nace64_output_eu7"][:, T_calibration]
        fixed_assets_other_than_dwellings =
            (fixed_assets_total - dwellings_total) * ((fixed_assets_eu7 - dwellings_eu7) ./ nominal_nace64_output_eu7 .* output) /
            sum((fixed_assets_eu7 - dwellings_eu7) ./ nominal_nace64_output_eu7 .* output, dims = 1)
        dwellings = zeros(G)
        dwellings[44] = dwellings_total  # Real estate sector
        fixed_assets = fixed_assets_other_than_dwellings + dwellings
    end

    # =========================================================================
    # OCM KEY DIFFERENCE #3: Do NOT subtract capital_consumption from operating_surplus
    # =========================================================================
    # Standard BeforeIT: operating_surplus = operating_surplus - capital_consumption
    # OCM: Keep operating_surplus as-is (commented out in original)

    # Calculate employers' social contributions (D1 - D11 = D12)
    # Use original compensation_employees (before OCM adjustment) minus sectoral wages
    # This matches the Optimal-ABM approach: comp_emp - wages by sector
    employers_social_contributions = compensation_employees_original - wages_by_sector

    # Gross capital formation for dwellings
    gross_capitalformation_dwellings = calibration_data["gross_capitalformation_dwellings"][T_calibration]
    fixed_capitalformation = Bit.pos(fixed_capitalformation)

    taxes_products_capitalformation_dwellings =
        gross_capitalformation_dwellings *
        (1 - 1 / (1 + taxes_products_fixed_capitalformation / sum(fixed_capitalformation)))

    # Timescale: ratio of quarterly GDP to annual FIGARO output
    timescale =
        data["nominal_gdp_quarterly"][T_calibration_exo] / (
            sum(
                compensation_employees .+ operating_surplus .+ capital_consumption .+ taxes_production .+
                taxes_products,
            ) .+ taxes_products_household .+ taxes_products_capitalformation_dwellings .+ taxes_products_government .+
            taxes_products_export
        )

    # Apply timescale conversion for annual data
    if !has_quarterly_firm_interest
        firm_interest_quarterly = timescale * firm_interest_quarterly
    end
    if !has_quarterly_govt_interest
        interest_government_debt_quarterly = timescale * interest_government_debt_quarterly
    end
    if !has_quarterly_govt_deficit
        government_deficit_quarterly = timescale * government_deficit_quarterly
    end

    # Capital formation calculations
    capitalformation_dwellings = zeros(G)  # OCM: zeroed out
    fixed_capital_formation_other_than_dwellings = fixed_capitalformation - capitalformation_dwellings
    exports = Bit.pos(exports)

    imports = Bit.pos(
        sum(intermediate_consumption, dims = 2) +
        household_consumption +
        government_consumption +
        fixed_capital_formation_other_than_dwellings * sum(capital_consumption) /
        sum(fixed_capital_formation_other_than_dwellings) +
        capitalformation_dwellings +
        exports - output,
    )
    reexports = Bit.neg(
        sum(intermediate_consumption, dims = 2) +
        household_consumption +
        government_consumption +
        fixed_capital_formation_other_than_dwellings * sum(capital_consumption) /
        sum(fixed_capital_formation_other_than_dwellings) +
        capitalformation_dwellings +
        exports - output,
    )

    # Social contributions and wages
    household_social_contributions = social_contributions - sum(employers_social_contributions)
    wages = compensation_employees * (1 - sum(employers_social_contributions) / sum(compensation_employees))
    household_income_tax = income_tax - corporate_tax

    # Other net transfers (government budget identity)
    other_net_transfers = Bit.pos(
        sum(taxes_products_household) +
        sum(taxes_products_capitalformation_dwellings) +
        sum(taxes_products_export) +
        sum(taxes_products) +
        sum(taxes_production) +
        sum(employers_social_contributions) +
        household_social_contributions +
        household_income_tax +
        corporate_tax +
        capital_taxes - social_benefits - sum(government_consumption) - interest_government_debt_quarterly / timescale -
        government_deficit_quarterly / timescale,
    )

    # Disposable income
    disposable_income =
        sum(wages) + property_income + social_benefits + other_net_transfers -
        household_social_contributions - household_income_tax - capital_taxes

    # Pre-scaling values (for OCM output)
    w_s_unscaled = timescale * wages ./ employees
    w_UB_unscaled = timescale * unemployment_benefits / unemployed_raw
    sb_inact_unscaled = timescale * pension_benefits / inactive_raw
    sb_other_unscaled = timescale * (social_benefits + other_net_transfers - unemployment_benefits - pension_benefits) / (sum(employees) + unemployed_raw + inactive_raw)

    # Scale number of firms and employees
    firms = max.(1, matlab_round.(scale * firms))
    employees = max.(firms, matlab_round.(scale * employees))
    inactive = max.(1, matlab_round.(scale * inactive_raw))
    unemployed = max.(1, matlab_round.(scale * unemployed_raw))

    # Sector parameters
    I_s = firms
    alpha_s = timescale * output ./ employees
    beta_s = output ./ sum(intermediate_consumption, dims = 1)'
    kappa_s = timescale * output ./ fixed_assets_other_than_dwellings / omega
    delta_s = timescale * capital_consumption ./ fixed_assets_other_than_dwellings / omega
    replace!(delta_s, NaN => 0.0)
    w_s = timescale * wages ./ employees
    tau_Y_s = taxes_products ./ output
    tau_K_s = taxes_production ./ output
    b_CF_g = fixed_capital_formation_other_than_dwellings / sum(fixed_capital_formation_other_than_dwellings)
    b_CFH_g = zeros(G)  # OCM: zeroed out for dwellings
    b_HH_g = household_consumption / sum(household_consumption)
    a_sg = intermediate_consumption ./ sum(intermediate_consumption, dims = 1)
    replace!(a_sg, NaN => 0.0)
    c_G_g = government_consumption / sum(government_consumption)
    c_E_g = (exports - reexports) / sum(exports - reexports)
    c_I_g = imports / sum(imports)

    # Structural parameters
    T_prime = T_calibration_exo - T_estimation_exo + 1
    T = 12
    T_max = T - max(0, T_calibration_exo + T - T_calibration_exo_max)

    # =========================================================================
    # OCM KEY DIFFERENCE #4: H_act excludes firms
    # =========================================================================
    # Standard BeforeIT: H_act = sum(employees) + unemployed + sum(firms) + 1
    # OCM: H_act = sum(employees) + unemployed
    H_act = sum(employees) + unemployed
    H_inact = inactive

    J = matlab_round(sum(firms) / 4)
    L = matlab_round(sum(firms) / 2)

    mu = firm_interest_quarterly / firm_debt_quarterly - r_bar

    # =========================================================================
    # OCM KEY DIFFERENCE #5: tau_INC denominator excludes property/mixed income
    # =========================================================================
    # Standard BeforeIT: (sum(wages) + property_income + mixed_income - household_social_contributions)
    # OCM: (sum(wages) - household_social_contributions)
    tau_INC =
        (household_income_tax + capital_taxes) /
        (sum(wages) - household_social_contributions)

    tau_FIRM =
        timescale * corporate_tax / (
            sum(
                Bit.pos(
                    timescale * operating_surplus -
                    firm_interest_quarterly * fixed_assets_other_than_dwellings /
                    sum(fixed_assets_other_than_dwellings) +
                    r_bar * firm_cash_quarterly * Bit.pos(operating_surplus) /
                    sum(Bit.pos(operating_surplus)),
                ),
            ) + firm_interest_quarterly - r_bar * (firm_debt_quarterly - bank_equity_quarterly)
        )

    tau_VAT = taxes_products_household / sum(household_consumption)
    tau_SIF = sum(employers_social_contributions) / sum(wages)
    tau_SIW = household_social_contributions / sum(wages)
    tau_EXPORT = sum(taxes_products_export) / sum(exports - reexports)
    tau_CF = 0.0  # OCM: zeroed out
    tau_G = sum(taxes_products_government) / sum(government_consumption)
    psi = (sum(household_consumption) + sum(taxes_products_household)) / disposable_income
    psi_H = 0.0  # OCM: zeroed out

    # =========================================================================
    # OCM KEY DIFFERENCE #6: theta_DIV uses property_income only
    # =========================================================================
    # Standard BeforeIT: timescale * (mixed_income + property_income) / (...)
    # OCM: timescale * property_income / (...)
    theta_DIV =
        timescale * property_income / (
            sum(
                Bit.pos(
                    timescale * operating_surplus -
                    firm_interest_quarterly * fixed_assets_other_than_dwellings /
                    sum(fixed_assets_other_than_dwellings) +
                    r_bar * firm_cash_quarterly * Bit.pos(operating_surplus) /
                    sum(Bit.pos(operating_surplus)),
                ),
            ) + firm_interest_quarterly - r_bar * (firm_debt_quarterly - bank_equity_quarterly) -
            timescale * corporate_tax
        )

    r_G = interest_government_debt_quarterly / government_debt_quarterly
    theta_UB = 0.55 * (1 - tau_INC) * (1 - tau_SIW)
    theta = 0.05
    zeta = 0.03
    zeta_LTV = 0.6
    zeta_b = 0.5
    nu = 1 / (30 * 4)  # Death rate (quarterly, ~30 year lifespan)

    # AR(1) estimation for EA economy
    alpha_pi_EA, beta_pi_EA, sigma_pi_EA, epsilon_pi_EA = Bit.estimate_for_calibration_script(
        diff(log.(ea["gdp_deflator_quarterly"][(T_estimation_exo - 1):T_calibration_exo])),
    )
    alpha_Y_EA, beta_Y_EA, sigma_Y_EA, epsilon_Y_EA =
        Bit.estimate_for_calibration_script(log.(ea["real_gdp_quarterly"][T_estimation_exo:T_calibration_exo]))

    # Taylor rule estimation
    a1 = (data["euribor"][T_estimation_exo:T_calibration_exo] .+ 1) .^ (1 / 4) .- 1
    a2 = exp.(diff(log.(ea["gdp_deflator_quarterly"][(T_estimation_exo - 1):T_calibration_exo]))) .- 1
    a3 = exp.(diff(log.(ea["real_gdp_quarterly"][(T_estimation_exo - 1):T_calibration_exo]))) .- 1
    rho, r_star, xi_pi, xi_gamma, pi_star = Bit.estimate_taylor_rule(a1, a2, a3)

    # AR(1) estimation for Government, Exports, Imports
    G_est =
        timescale * sum(government_consumption) .*
        data["real_government_consumption_quarterly"][T_estimation_exo:T_calibration_exo] ./
        data["real_government_consumption_quarterly"][T_calibration_exo]
    G_est = log.(G_est)

    E_est =
        timescale * sum(exports - reexports) .* data["real_exports_quarterly"][T_estimation_exo:T_calibration_exo] ./
        data["real_exports_quarterly"][T_calibration_exo]
    E_est = log.(E_est)

    I_est =
        timescale * sum(imports) .* data["real_imports_quarterly"][T_estimation_exo:T_calibration_exo] ./
        data["real_imports_quarterly"][T_calibration_exo]
    I_est = log.(I_est)

    alpha_G, beta_G, sigma_G, epsilon_G = Bit.estimate_for_calibration_script(G_est)
    alpha_E, beta_E, sigma_E, epsilon_E = Bit.estimate_for_calibration_script(E_est)
    alpha_I, beta_I, sigma_I, epsilon_I = Bit.estimate_for_calibration_script(I_est)

    C = cov([epsilon_Y_EA epsilon_E epsilon_I])

    # Parameters dictionary
    params = Dict(
        "T" => T,
        "T_max" => T_max,
        "S" => S,
        "G" => G,
        "H_act" => H_act,
        "H_inact" => H_inact,
        "J" => J,
        "L" => L,
        "tau_INC" => tau_INC,
        "tau_FIRM" => tau_FIRM,
        "tau_VAT" => tau_VAT,
        "tau_SIF" => tau_SIF,
        "tau_SIW" => tau_SIW,
        "tau_EXPORT" => tau_EXPORT,
        "tau_CF" => tau_CF,
        "tau_G" => tau_G,
        "theta_UB" => theta_UB,
        "psi" => psi,
        "psi_H" => psi_H,
        "theta_DIV" => theta_DIV,
        "theta" => theta,
        "mu" => mu,
        "r_G" => r_G,
        "zeta" => zeta,
        "zeta_LTV" => zeta_LTV,
        "zeta_b" => zeta_b,
        "I_s" => I_s,
        "alpha_s" => alpha_s,
        "beta_s" => reshape(beta_s, :, 1),
        "kappa_s" => kappa_s,
        "delta_s" => delta_s,
        "w_s" => w_s,
        "w_s_unscaled" => w_s_unscaled,
        "tau_Y_s" => tau_Y_s,
        "tau_K_s" => tau_K_s,
        "b_CF_g" => reshape(b_CF_g, :, 1),
        "b_CFH_g" => reshape(b_CFH_g, :, 1),
        "b_HH_g" => b_HH_g,
        "c_G_g" => c_G_g,
        "c_E_g" => reshape(c_E_g, :, 1),
        "c_I_g" => reshape(c_I_g, :, 1),
        "a_sg" => a_sg,
        "T_prime" => T_prime,
        "alpha_pi_EA" => alpha_pi_EA,
        "beta_pi_EA" => beta_pi_EA,
        "sigma_pi_EA" => sigma_pi_EA,
        "alpha_Y_EA" => alpha_Y_EA,
        "beta_Y_EA" => beta_Y_EA,
        "sigma_Y_EA" => sigma_Y_EA,
        "rho" => rho,
        "r_star" => r_star,
        "xi_pi" => xi_pi,
        "xi_gamma" => xi_gamma,
        "pi_star" => pi_star,
        "alpha_G" => alpha_G,
        "beta_G" => beta_G,
        "sigma_G" => sigma_G,
        "alpha_E" => alpha_E,
        "beta_E" => beta_E,
        "sigma_E" => sigma_E,
        "alpha_I" => alpha_I,
        "beta_I" => beta_I,
        "sigma_I" => sigma_I,
        "nu" => nu,
        "C" => C,
        "scale" => scale
    )

    # Initial conditions
    N_s = employees
    D_I = firm_cash_quarterly
    L_I = firm_debt_quarterly
    w_UB = timescale * unemployment_benefits / unemployed
    sb_inact = timescale * pension_benefits / inactive

    # =========================================================================
    # OCM KEY DIFFERENCE #7: sb_other denominator excludes firms
    # =========================================================================
    # Standard BeforeIT: (sum(employees) + unemployed + inactive + sum(firms) + 1)
    # OCM: (sum(employees) + unemployed + inactive)
    sb_other =
        timescale * (social_benefits + other_net_transfers - unemployment_benefits - pension_benefits) /
        (sum(employees) + unemployed + inactive)

    D_H = household_cash_quarterly

    # =========================================================================
    # OCM KEY DIFFERENCE #8: K_H = firm equity, not dwellings
    # =========================================================================
    # Standard BeforeIT: K_H = sum(dwellings)
    # OCM: K_H = E_i = D_I + sum(fixed_assets) - L_I (firm equity)
    E_i = D_I + sum(fixed_assets) - L_I
    K_H = E_i

    L_G = government_debt_quarterly
    E_k = bank_equity_quarterly
    E_CB = L_G + L_I - D_I - D_H - E_k
    D_RoW = 0.0

    # Capital tax rate (OCM addition)
    tau_K = capital_taxes / K_H

    # Time series for initial conditions
    Y =
        timescale * sum(output) .* data["real_gdp_quarterly"][T_estimation_exo:T_calibration_exo] ./
        data["real_gdp_quarterly"][T_calibration_exo]
    pi = diff(log.(data["gdp_deflator_quarterly"][(T_estimation_exo - 1):T_calibration_exo]))
    Y_EA = ea["real_gdp_quarterly"][T_calibration_exo]
    pi_EA = ea["gdp_deflator_quarterly"][T_calibration_exo] / ea["gdp_deflator_quarterly"][T_calibration_exo - 1] - 1

    C_G = [
        timescale *
        sum(government_consumption) *
        data["real_government_consumption_quarterly"][T_estimation_exo:min(
            T_calibration_exo + T,
            T_calibration_exo_max,
        )] / data["real_government_consumption_quarterly"][T_calibration_exo]
        fill(NaN, max(0, T_calibration_exo + T - T_calibration_exo_max), 1)
    ]
    C_E = [
        timescale *
        sum(exports - reexports) *
        data["real_exports_quarterly"][T_estimation_exo:min(T_calibration_exo + T, T_calibration_exo_max)] /
        data["real_exports_quarterly"][T_calibration_exo]
        fill(NaN, max(0, T_calibration_exo + T - T_calibration_exo_max), 1)
    ]
    Y_I = [
        timescale *
        sum(imports) *
        data["real_imports_quarterly"][T_estimation_exo:min(T_calibration_exo + T, T_calibration_exo_max)] /
        data["real_imports_quarterly"][T_calibration_exo]
        fill(NaN, max(0, T_calibration_exo + T - T_calibration_exo_max), 1)
    ]

    C_G = vcat(C_G...)
    C_E = vcat(C_E...)
    Y_I = vcat(Y_I...)

    # Full time series (OCM addition)
    C_G_full =
        timescale *
        sum(government_consumption) *
        data["real_government_consumption_quarterly"][T_estimation_exo:T_calibration_exo_max] /
        data["real_government_consumption_quarterly"][T_calibration_exo]
    C_E_full =
        timescale *
        sum(exports - reexports) *
        data["real_exports_quarterly"][T_estimation_exo:T_calibration_exo_max] /
        data["real_exports_quarterly"][T_calibration_exo]
    Y_I_full =
        timescale *
        sum(imports) *
        data["real_imports_quarterly"][T_estimation_exo:T_calibration_exo_max] /
        data["real_imports_quarterly"][T_calibration_exo]

    r_bar_series = (data["euribor"][T_estimation_exo:min(T_calibration_exo + T, T_calibration_exo_max)] .+ 1.0) .^ (1.0 / 4.0) .- 1

    # Load index prices from CSV if path provided
    # Expected format: Date (YYYY-MM-DD), Open, High, Low, Close, Adj Close, Volume
    index_prices = if !isnothing(composite_rk_path) && isfile(composite_rk_path)
        df_composite = CSV.read(composite_rk_path, DataFrame)

        # Parse Date column (format: YYYY-MM-DD)
        df_composite[!, :ParsedDate] = Date.(df_composite.Date)

        # Filter data before calibration date
        df_filtered = df_composite[df_composite.ParsedDate .< Date(calibration_date), :]

        # Use Adj Close as the index price
        Vector{Float64}(df_filtered[!, "Adj Close"])
    else
        Float64[]  # Empty array if no path provided
    end

    # Initial conditions dictionary
    initial_conditions = Dict(
        "D_I" => D_I,
        "L_I" => L_I,
        "omega" => omega,
        "w_UB" => w_UB,
        "w_UB_unscaled" => w_UB_unscaled,
        "sb_inact" => sb_inact,
        "sb_inact_unscaled" => sb_inact_unscaled,
        "sb_other" => sb_other,
        "sb_other_unscaled" => sb_other_unscaled,
        "D_H" => D_H,
        "K_H" => K_H,
        "L_G" => L_G,
        "E_k" => E_k,
        "E_CB" => E_CB,
        "D_RoW" => D_RoW,
        "N_s" => N_s,
        "Y" => Y,
        "pi" => pi,
        "Y_EA" => Y_EA,
        "pi_EA" => pi_EA,
        "r_bar" => r_bar,
        "C_G" => C_G,
        "C_E" => C_E,
        "Y_I" => Y_I,
        "C_G_full" => C_G_full,
        "C_E_full" => C_E_full,
        "Y_I_full" => Y_I_full,
        "disposable_income" => disposable_income,
        "household_consumption" => psi * disposable_income,
        "r_bar_series" => r_bar_series,
        "index_prices" => index_prices
    )

    return params, initial_conditions
end
