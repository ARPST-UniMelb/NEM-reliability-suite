using SiennaNEM

using PowerSimulations

using Dates
using HiGHS

reference_trace = 4006
poe = 10
tyear = 2030
scenario = 1

horizon = Hour(48)
interval = Hour(24)
simulation_output_folder = joinpath(@__DIR__, "..", "data", "sienna-files")
simulation_name = "ref$reference_trace-poe$poe-tyear$tyear-s$scenario"
simulation_steps = 2  # number of rolling horizon steps
problem_name = "UC"

file_format = "arrow"
input_folder_arrow = joinpath(@__DIR__, "..", "data", "pisp-datasets", "out-ref$reference_trace-poe$poe", file_format)
timeseries_folder_arrow = joinpath(input_folder_arrow, "schedule-$tyear")

optimizer = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01)

data = SiennaNEM.get_data(
    input_folder_arrow, timeseries_folder_arrow; file_format=file_format
)
sys_sienna = SiennaNEM.create_system!(data);
SiennaNEM.add_ts!(
    sys_sienna, data;
    horizon=horizon,  # horizon of each time slice that will be used in the study
    interval=interval,  # interval within each time slice, not the resolution of the time series
    scenario=scenario,  # scenario number, integer
);

template_uc = SiennaNEM.build_problem_base_uc()
simulation = SiennaNEM.run_simulation(
    template_uc, sys_sienna;
    simulation_folder=simulation_output_folder,
    simulation_name=simulation_name,
    simulation_steps=simulation_steps,
    decision_model_kwargs=(
        optimizer=optimizer,
        name=problem_name,
    ),
)
simulation_results = SimulationResults(simulation)
decision_problem_results = get_decision_problem_results(
    simulation_results, problem_name
)
