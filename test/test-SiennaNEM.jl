using Revise
using SiennaNEM
using PowerSimulations
using HiGHS
using Dates

system_data_dir = "data/arrow"
ts_data_dir     = joinpath(system_data_dir, "schedule-1w")
scenario_name   = 1

data = read_system_data(system_data_dir);
read_ts_data!(data, ts_data_dir);
add_tsf_data!(data, scenario_name=scenario_name);
update_system_data_bound!(data);

sys = create_system!(data);
add_ts!(sys, data, scenario_name=scenario_name);

template_uc = SiennaNEM.build_problem_base_uc();
solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01);

hours = Hour(24)
problem = DecisionModel(template_uc, sys; optimizer=solver, horizon=hours)
@time build!(problem; output_dir=mktempdir())
@time solve!(problem)
res = OptimizationProblemResults(problem)