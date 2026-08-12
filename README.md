# NEM Reliability Suite - AR-PST Stage 5

Data and reliability studies for the Australian National Electricity Market (NEM), developed for Stage 5 of the [AR-PST initiative](https://www.csiro.au/en/research/technology-space/energy/Electricity-transition/AR-PST/Stage-5).

This repository contains some sample data, as well as tutorials and scripts to perform reliability studies with [PISP.jl](https://github.com/ARPST-UniMelb/PISP.jl), [PRASNEM.jl](https://github.com/ARPST-UniMelb/PRASNEM.jl), [SchedNEM.jl](https://github.com/ARPST-UniMelb/SchedNEM.jl) and [SiennaNEM.jl](https://github.com/ARPST-UniMelb/SiennaNEM.jl).

> [!CAUTION]
> The current version is functional and has been extensively tested; however, bugs or other issues may still arise. We would greatly appreciate any feedback or bug reports submitted via https://github.com/ARPST-UniMelb/NEM-reliability-suite/issues
>

> [!NOTE]
> If you are using this or the related repositories for your work, please cite the final report of AR-PST Stage 5:
> 
> T. Kopka, M. Yasirroni, P. Apablaza, B. Moya, S. Mhanna, and P. Mancarella, “Resource Adequacy, Risk, and Resilience in Low-Carbon Energy System Planning: Methods, Tools, and Metrics,” Australian Research in Power Systems Transition (AR-PST), Jun. 2026. [Online]. Available: https://www.csiro.au/en/research/technology-space/energy/Electricity-transition/AR-PST/Stage-5


## Getting started

First, clone the project by executing the following instruction in your command line.

```sh
git clone "https://github.com/ARPST-UniMelb/NEM-reliability-suite"
```

Then start a Julia REPL within the folder and activate and instantiate the local environment (*note that this requires ```julia 1.11+```*):

```julia
using Pkg
Pkg.activate(".")
Pkg.instantiate()
```

All code snippets below assume the REPL is running in the repository root folder.

Now we can start collecting the public ISP data with `PISP`. Note that this requires an active internet connection and may take some time.

```julia
using PISP

# Set some parameters (see all parameters below)
reference_trace = 4006 #Use 4006 for the reference trace of the ODP. 
poe             = 10 # Probability of exceedance (POE) for demand
target_years    = [2030, 2031]

PISP.build_ISP24_datasets(
    downloadpath = joinpath(pwd(), "data", "pisp-downloads"),
    poe          = poe,
    reftrace     = reference_trace,
    years        = target_years,
    output_root  = joinpath(pwd(), "data", "pisp-datasets"),
    write_csv    = true,
    write_arrow  = true,  # using arrow will make SiennaNEM faster
    scenarios    = [1,2,3]
)
```

Now we have the dataset available in the specified folder. We can therefore now use `PRASNEM` to run adequacy studies.

```julia
using PRASNEM
using Dates

# Create PRAS file
tyear             = target_years[1]
scenario          = 2
start_dt          = DateTime("$tyear-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt            = DateTime("$tyear-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
input_folder      = joinpath(pwd(), "data", "pisp-datasets","out-ref$reference_trace-poe$poe", "csv")
timeseries_folder = "schedule-$tyear" # subfolder of input_folder with the timeseries data
output_folder     = joinpath(pwd(), "data", "pras-files")
sys_pras          = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder, scenario=scenario) # More optional parameters available (see below)

# Run adequacy study using PRAS
shortfall = PRASNEM.run_pras_study(sys_pras);
```

If more advanced adequacy studies are desired, using PRAS directly is advised. See examples in the folder `tutorials/`.

To understand the system operation in detail, we utilise `SiennaNEM` to run system scheduling.

```julia
using SiennaNEM

using PowerSimulations

using Dates
using HiGHS

horizon                  = Hour(24)
interval                 = Hour(24)
simulation_output_folder = joinpath(pwd(), "data", "sienna-files")
simulation_name          = "ref$(reference_trace)-poe$(poe)-tyear$(tyear)-s$(scenario)"
simulation_steps         = 2  # number of rolling horizon steps
file_format              = "arrow"
input_folder_arrow       = joinpath(pwd(), "data", "pisp-datasets","out-ref$reference_trace-poe$poe", file_format)
timeseries_folder_arrow  = joinpath(input_folder_arrow, "schedule-$tyear")

data       = SiennaNEM.get_data(
    input_folder_arrow, timeseries_folder_arrow; file_format=file_format
)
sys_sienna = SiennaNEM.create_system!(data);
SiennaNEM.add_ts!(
    sys_sienna, data;
    horizon  = horizon,  # horizon of each time slice that will be used in the study
    interval = interval,  # interval within each time slice, not the resolution of the time series
    scenario = scenario,  # scenario number, integer
)

template_uc = SiennaNEM.build_problem_base_uc()
sim         = SiennaNEM.run_simulation(
    template_uc, sys_sienna;
    simulation_folder     = simulation_output_folder,
    simulation_name       = simulation_name,
    simulation_steps      = simulation_steps,
    decision_model_kwargs = (
        optimizer=optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01),
    ),
)
results = SimulationResults(sim)
```

An interactive example with `PRASNEM.jl` and `SchedNEM.jl` can be found in the interactive Jupyter Notebook `tutorials/NEM-reliability-suite - Example.ipynb`. A tutorial for the full adequacy-assessment workflow used in the AR-PST final report is in `tutorials/AR-PST Final Report - Tutorial.ipynb`.

## Optional parameters

### PISP.build_ISP24_datasets()

There are multiple parameters that can be adjusted when generating the dataset from the public ISP24 datafiles:

| Parameter           | Default       | Description                                                                                                                        |
| ------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
|downloadpath|"../../data-download"| Path where all files from AEMO's website will be downloaded and extracted
|download_from_AEMO|true| Whether to download files from AEMO's website
|poe|10| Probability of exceedance (POE) for demand: 10% or 50%
|reftrace|4006| Reference weather year trace: select among 2011 - 2023 or 4006 (trace for the ODP)
|years|[2025]| Calendar years for which to build the time-varying schedules: select among 2025 - 2050
|output_name|"out"| Output folder name
|output_root|nothing| Output folder root
|write_csv|true| Whether to write CSV files
|write_arrow|true|Whether to write Arrow files
|scenarios|[1,2,3]|Scenarios to include in the output: 1 for "Progressive Change", 2 for "Step Change", 3 for "Green Energy Exports"

### PRASNEM.create_pras_system()

There are multiple optional parameters that can be adjusted when creating the pras system:

| Parameter           | Default       | Description                                                                                                                        |
| ------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| output_folder       | ""            | Folder to save the PRAS file. If empty, the PRAS file is not saved.                                                                |
| regions_selected    | collect(1:12) | Array of region IDs to include (needs to be in ascending order). Empty array for copperplate model.                                |
| scenario            | 2             | ISP scenario to use (1: "Progressive Change", 2: "Step Change", 3: "Green Energy Exports")                                        |
| gentech_excluded    | []            | Array of generator technologies to exclude (can be fuel or technology, e.g. "Coal", "RoofPV", ...)                                 |
| alias_excluded      | []            | Array of generator/storage/DER aliases to exclude (e.g. "GSTONE1")                                                                 |
| investment_filter   | [0]           | Array indicating which assets to include based on investment status (if investment candidate or not)                               |
| active_filter       | [1]           | Array indicating which assets to include based on their active status                                                              |
| line_alias_included | []            | Array of line aliases to include even if they would be filtered out due to investment/active status                                |
| DER_parameters      | `PRASNEM.get_DER_parameters()` | Dict with DER scenario, as defined in `PRASNEM.get_DER_parameters()` function.                                    |
| hydro_parameters    | `PRASNEM.get_hydro_parameters()` | Dict with hydro parameters (e.g. initial SOC of reservoirs, reservoir sizes, efficiencies).                     |
