# NEM Reliability Suite
Data and reliability studies for the Australian National Electricity Market (NEM).

This repository contains some sample data, as well as tutorials and scripts to perform reliability studies with [PISP.jl](https://github.com/ARPST-UniMelb/PISP.jl), [PRASNEM.jl](https://github.com/ARPST-UniMelb/PRASNEM.jl) and [SiennaNEM.jl](https://github.com/ARPST-UniMelb/SiennaNEM.jl).


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

Now we can start collecting the public ISP data with ```PISP```. Note that this requires an active internet connection and may take some time.
```julia
using PISP

# Set some parameters (see all parameters below)
reference_trace = 4006 #Use 4006 for the reference trace of the ODP. 
poe             = 10 # Probability of exceedance (POE) for demand
target_years    = [2030, 2031]

PISP.build_ISP24_datasets(
    downloadpath = joinpath(@__DIR__, "..", "data", "pisp-downloads"),
    poe          = poe,
    reftrace     = reference_trace,
    years        = target_years,
    output_root  = joinpath(@__DIR__, "..", "data", "pisp-datasets"),
    write_csv    = true,
    write_arrow  = false,
    scenarios    = [1,2,3])

```

Now we have the dataset available in the specified folder. We can therefore now use ```PRASNEM``` to run adequacy studies.
```julia
using PRASNEM
using Dates

# Create PRAS file
tyear             = target_years[1]
scenario          = 2
start_dt          = DateTime("$tyear-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt            = DateTime("$tyear-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
input_folder      = joinpath(@__DIR__, "..", "data", "pisp-datasets","out-ref$reference_trace-poe$poe", "csv")
timeseries_folder = joinpath(input_folder, "schedule-$tyear")
output_folder     = joinpath(@__DIR__, "..", "data", "pras-files")
sys_pras          = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder, scenario=scenario) # More optional parameters available (see below)

# Run adequacy study using PRAS
shortfall = PRASNEM.run_pras_study(sys_pras);
```
If more advanced adequacy studies are desired, using PRAS directly is advised. See examples in the folder ```\tutorials```.

To understand the system operation in detail, we utilise ```SiennaNEM``` to run system scheduling.
```julia
using SiennaNEM
using HiGHS

horizon                  = Hour(36)
interval                 = Hour(24)
simulation_output_folder = joinpath(@__DIR__, "..", "data", "sienna-files")
simulation_name          = "ref$reference_trace-poe$poe-tyear$tyear-s$scenario"

data       = SiennaNEM.get_data(input_folder, timeseries_folder);
sys_sienna = SiennaNEM.create_system!(data);
SiennaNEM.add_ts!(
    sys_sienna, data;
    horizon       = horizon,  # horizon of each time slice that will be used in the study
    interval      = interval,  # interval within each time slice, not the resolution of the time series
    scenario_name = scenario_name,  # scenario number
);

template_uc = SiennaNEM.build_problem_base_uc();
results     = SiennaNEM.run_decision_model_loop(
                                template_uc, sys_sienna;
                                simulation_folder     = simulation_output_folder,
                                simulation_name       = simulation_name,
                                decision_model_kwargs = (optimizer=optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01),),)

```


## Optional parameters

### PISP.build_ISP24_datasets()
There are multiple parameters that can be adjusted when generating the dataset from the public ISP24 datafiles:
| Parameter           | Default       | Description                                                                                                                        |
| ------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
|downloadpath|"../../data-download"| Path where all files from AEMO's website will be downloaded and extracted
|download_from_AEMO|true| Whether to download files from AEMO's website
|poe|10| Probability of exceedance (POE) for demand: 10% or 50%
|reftrace|2011| Reference weather year trace: select among 2011 - 2023 or 4006 (trace for the ODP)
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
| scenario            | 2             | ISP scenario to use (1: progressive change, 2: step change, 3: green hydrogen exports)                                             |
| gentech_excluded    | []            | Array of generator technologies to exclude (can be fuel or technology, e.g. "Coal", "RoofPV", ...)                                 |
| alias_excluded      | []            | Array of generator/storage/DER aliases to exclude (e.g. "GSTONE1")                                                                 |
| investment_filer    | [0]           | Array indicating which assets to include based on investment status (if investment candidate or not)                               |
| active_filter       | [1]           | Array indicating which assets to include based on their active status                                                              |
| line_alias_included | []            | Array of line aliases to include even if they would be filtered out due to investment/active status                                |
| weather_folder      | ""            | Folder with weather data timeseries to use (no capacities are read from here, only normalised timeseries for demand, VRE, and DSP). Inflows are considered in full (not normalised).|

## Advanced results
The source code to obtain these results can be found in the folder ``/tutorials``.

**Distribution of USE events of the NEM in 2028**
![](./scripts/AR-PST%20Interim%20Presentation/figures/PRASNEM_2_USE_Event_Size_Histogram_2028.png)

**ISP 2024 Step Change Scenario Adequacy Levels**
![](./scripts/AR-PST%20Interim%20Presentation/figures/PRASNEM_3_ISP_Step_Change_adequacy.png)

**Adequacy in 2030 with/without selected transmission expansion**
![](./scripts/AR-PST%20Interim%20Presentation/figures/PRASNEM_4_maps.png)
