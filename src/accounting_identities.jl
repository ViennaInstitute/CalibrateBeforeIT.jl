## Initial accounting-identity check for calibration bundles.

import BeforeIT as Bit

"""
    get_initial_accounting_identities(parameters, initial_conditions)

Construct the BeforeIT model implied by `parameters` and
`initial_conditions` and return the three accounting-identity
residuals of its initialization snapshot:

1. income vs production accounting,
2. nominal expenditure accounting (`GDP = C + G + I + X - M`),
3. real expenditure accounting.

A fresh model records exactly one observation (the initialization), so the
residuals are those of the initial quarter. A bundle whose residuals are not
zero to floating point sends a different amount of income through the model
than the calibration data support and breaks the initial expenditure
identity of the simulation.

Calibration scripts should refuse to save a bundle whose residuals deviate
from zero beyond rounding noise (e.g. `abs(r) > 1e-7`), the same bound the
model suite asserts on shipped bundles.
"""
function get_initial_accounting_identities(parameters, initial_conditions)
    model = Bit.Model(parameters, initial_conditions)
    return Bit.get_accounting_identities(model.data)
end
