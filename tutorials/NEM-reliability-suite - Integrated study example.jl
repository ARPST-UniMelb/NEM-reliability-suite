# If not already done, activate and instantiate the local environment before running this script:
# using Pkg; Pkg.activate("."); Pkg.instantiate()

# Initialising all dependencies and packages
using PISP
using PRASNEM
using Dates
using SiennaNEM
using HiGHS
using Dates
using JuMP

# =======================================================
# Generating PISP datasets for the 2024 ISP
# =======================================================

# Set parameters (see all parameters below)
reference_trace = 4006  # Use 4006 for the reference trace of the ODP
poe             = 10    # Probability of exceedance (POE) for demand
target_years    = [2030, 2031]

# Generate PISP datasets for `target_years`, based on the reference trace 4006 (ODP of the 2024 ISP) for 10% POE demand
PISP.build_ISP24_datasets(
    downloadpath = joinpath(@__DIR__, "..", "data", "pisp-downloads"),
    poe          = poe,
    reftrace     = reference_trace,
    years        = target_years,
    output_root  = joinpath(@__DIR__, "..", "data", "pisp-datasets"),
    write_csv    = true,
    write_arrow  = false,
    scenarios    = [1,2,3])

# =======================================================
# Studying system adequacy with PRASNEM
# =======================================================
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
shortfall         = PRASNEM.run_pras_study(sys_pras)

# =======================================================
# Studying system operations with SiennaNEM
# =======================================================
horizon                  = Hour(36)
interval                 = Hour(24)
simulation_output_folder = joinpath(@__DIR__, "..", "data", "sienna-files")
simulation_name          = "ref$reference_trace-poe$poe-tyear$tyear-s$scenario"

data       = SiennaNEM.get_data(input_folder, timeseries_folder);
sys_sienna = SiennaNEM.create_system!(data);
SiennaNEM.add_ts!(
    sys_sienna, data;
    horizon=horizon,         # Horizon of each time slice that will be used in the study
    interval=interval,       # Interval within each time slice, not the resolution of the time series
    scenario_name=scenario,  # Scenario number
);

template_uc = SiennaNEM.build_problem_base_uc();
results = SiennaNEM.run_decision_model_loop(
    template_uc, sys_sienna;
    simulation_folder=simulation_output_folder,
    simulation_name=simulation_name,
    decision_model_kwargs=(optimizer=JuMP.optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01, "log_to_console" => true),),)