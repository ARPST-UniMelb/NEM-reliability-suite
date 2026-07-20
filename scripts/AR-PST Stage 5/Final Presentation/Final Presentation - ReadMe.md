# Final Presentation

Scripts to create the plots for the final presentation on 20th of July 2026. All figures are saved to the `figures/` subfolder. Most figures are exported in incremental steps (`... - 0.png`, `... - 1.png`, ...) so the results can be revealed step by step on a slide; the `... - legend.png` versions are used to label the figures manually.

## Structure

Each script covers one part of the presentation and can be run independently:

| Script | Content | Simulation cases used |
| --- | --- | --- |
| `B - Storage.jl` | NEM-wide reliability risk (average annual USE) for the three storage operation cases (A1: high energy, A2: derated energy, A3: economic operation) | `baseVPP` |
| `C - DER - 1 DSP.jl` | Effect of demand-side participation on reliability risk, with and without DSP | `baseVPP`, `baseHalfDR` |
| `C - DER - 2 weather years.jl` | Histograms of load shedding events (2040) for the DER cases across two weather reference years | `base`, `baseEV`, `baseVPP`, `baseDRnoVPP` |
| `D - Risk profile - 1 tail.jl` | Average annual USE and tail risk (CVaR 95%) across all weather reference years (2011–2023, POE 10/50) | `baseVPP` |
| `D - Risk profile - 2 profile.jl` | Duration vs. magnitude profile of load shedding events | `baseVPP` |
| `E - Resilience.jl` | Heatwave resilience: capacity deratings during the heatwave week and annual USE per derated component group | `base`, `baseVPP` + heatwave resilience events |

The scripts follow a common pattern: load the shared functions (`functions/all_functions.jl`), read the simulation results with `create_summary()`, and export the figures with the plot sections separated by `#%%` cells.

## How to get the results

1. Set up and activate the repository environment as described in the [main ReadMe](../../../README.md).
2. Run the simulations for the cases listed above with `Run workflow.jl` in the folder [`Final Report`](../Final%20Report) (follow the explanations in the tutorials, e.g. `tutorials/AR-PST Final Report - Tutorial.ipynb`). The scripts read the simulation outputs from the results drive (`base_path`, default `Z://`).
3. Run a script from a Julia REPL with the repository environment active, either as a whole or cell by cell (`#%%` cells, e.g. in VS Code). The figures are written to `figures/`.
