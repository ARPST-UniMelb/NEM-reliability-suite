

using JuMP
using Plots
using Dates
using Gurobi
using SchedNEM
using PRAS
using PRASNEM

include("../../../../functions/all_functions.jl");


#%%


reference_year = 2019
poe = 10
target_year = 2025
nsamples = 100
seed = 2
case = "base"

# File paths (adjust as necessary)
base_folder_pisp = joinpath("Z:/", "pisp-datasets", case)
base_folder_pras = joinpath("Z:/", "pras-files", case)
base_folder_schedules = joinpath("Z:/", "schedules", case)
base_folder_results = joinpath("Z:/", "results", case)

pisp_input_folder = joinpath(base_folder_pisp, "out-ref$(reference_year)-poe$(poe)", "csv")
pras_folder = joinpath(base_folder_pras, "out-ref$(reference_year)-poe$(poe)")
timeseries_folder = "schedule-$(target_year)"
schedules_folder = joinpath(base_folder_schedules, "out-ref$(reference_year)-poe$(poe)")


#%%
# First run assess_adequacy to create all the relevant files

# Create PRAS system
sys = PRAS.SystemModel(joinpath(pras_folder, "2025-01-01_to_2025-12-31_s2_all_regions.pras"))
PRASNEM.updateStorageOutageDerating!(sys)
sys_original = deepcopy(sys)

#%%
# Get schedule
res = SchedNEM.read_schedule(joinpath(schedules_folder, "ty$target_year","$target_year-ref$(reference_year)-poe$(poe)-s2.h5"))

# Update the system with the schedule (e.g. embedd storage, DR, update unit commitment)
sys_assessment = deepcopy(sys)
PRASNEM.updateDRExpectationDispatch!(sys_assessment, res)
PRASNEM.updateStorageExpectationDispatch!(sys_assessment, res)
PRASNEM.updateUnitCommitment!(sys_assessment, res)

# And calculate critical periods
sf_expectation, genAv, lineAv, = assess(sys_assessment, SequentialMonteCarlo(samples=100, seed=2), ShortfallSamples(), GeneratorAvailability(), LineAvailability())

#%%

# Choose the critical period to look at (selected because it still may have load shedding after the reoptimisation)
idx_start = 4633

# First just look at the original system schedule
DER_parameters = PRASNEM.get_DER_parameters()

m = SchedNEM.build_operation_model(sys_original; input_folder="Z:/pisp-datasets/base/out-ref2011-poe10/csv", 
   optimiser=Gurobi.Optimizer(),
   DER_parameters=DER_parameters)

initial_soc_stor, initial_soc_genstor, 
        p_gen_initial, gon_initial, stup_before, 
        shdw_before, gen_fail_before = SchedNEM.get_system_parameters(res, idx_start, 48, m[:genOpDetails], ones(size(res.gon)))  

SchedNEM.update_model_parameters!(m, sys_original, idx_start; 
    initial_soc_stor=initial_soc_stor, initial_soc_genstor=initial_soc_genstor, 
    p_gen_initial=p_gen_initial, gon_initial=gon_initial, stup_before=stup_before, 
    shdw_before=shdw_before, gen_fail_before=gen_fail_before)

optimize!(m)
res_orig_schedule = SchedNEM.get_results(m)

SchedNEM.plot_timeseries_results(m, sys_original; region=[5,6,7,8])
title!("Pre-dispatch schedule 2025 | NSW")
xlabs = Dates.format.(DateTime.(sys_original.timestamps[idx_start:idx_start+47]), "dd/mm HH:MM")
xticks!(1:12:48, xlabs[1:12:end])
plot!(leftmargin=5Plots.mm, dpi=300, size=(800,400))
savefig(joinpath(@__DIR__, "figures", "A3-pre_dispatch_schedule.png"))

#%%

# Plot the offline capacity

cap_offline = res.p_gen[:, idx_start:idx_start+47, :] .* (1 .- genAv.available[:, idx_start:idx_start+47, 81])

idxs_coal = findall(x -> occursin("coal", lowercase(x)), sys_original.generators.categories)
idxs_gas = findall(x -> lowercase(x) in ["ocgt", "ccgt"], sys_original.generators.categories)
idxs_nsw = vcat(sys.region_gen_idxs[[5,6,7,8]]...)

agg_cap_offline = hcat(sum(cap_offline[intersect(idxs_coal, idxs_nsw), :, :], dims=1)[:], sum(cap_offline[intersect(idxs_gas, idxs_nsw), :, :], dims=1)[:]) 


x = vcat(repeat(0.5:1.0:47, inner=2)[2:end], 47 + 0.5)
y_pos = hcat([repeat(agg_cap_offline[:,i], inner=2) for i in axes(agg_cap_offline,2)]...)

areaplot(x, - y_pos ./ 1e3, fillalpha=0.8, lw=0, c=[:black :grey], label=["Coal" "Gas"], palette=:Spectral_11, dpi=500,
    legend=:outerright)
xticks!(1:12:48, xlabs[1:12:end])
ylabel!("Unavailable capacity [GW]")
title!("Unavailable scheduled capacity in NSW")
plot!(leftmargin=5Plots.mm, size=(800,400))
savefig(joinpath(@__DIR__, "figures", "A3-offline_capacity.png"))


#%%

sample = 81
# Now the system with outages
initial_soc_stor, initial_soc_genstor, 
        p_gen_initial, gon_initial, stup_before, 
        shdw_before, gen_fail_before = SchedNEM.get_system_parameters(res, idx_start, 48, m[:genOpDetails], genAv.available[:, :, sample])  

m_expectation = SchedNEM.build_operation_model(sys_original; input_folder="Z:/pisp-datasets/base/out-ref2011-poe10/csv", 
   optimiser=Gurobi.Optimizer(),
   DER_parameters=DER_parameters)

SchedNEM.update_model_parameters!(m_expectation, sys_original, idx_start; 
    initial_soc_stor=initial_soc_stor, initial_soc_genstor=initial_soc_genstor, 
    p_gen_initial=p_gen_initial, gon_initial=gon_initial, stup_before=stup_before, 
    shdw_before=shdw_before, gen_fail_before=gen_fail_before)

# Set the results to the pre-dispatch schedule
@constraint(m_expectation, m_expectation[:gon] .<= res.gon[:, idx_start:idx_start+47])
@constraint(m_expectation, m_expectation[:e_stor] .== value.(m[:e_stor]))
@constraint(m_expectation, m_expectation[:e_genstor] .== value.(m[:e_genstor]))

# And add failures
SchedNEM.updateGenAvailabilityFullHorizon!(m_expectation, genAv.available[:, :, sample], idx_start)
SchedNEM.updateLineAvailabilityFullHorizon!(m_expectation, sys_original, idx_start, lineAv.available[:, :, sample])

optimize!(m_expectation)
@assert is_solved_and_feasible(m_expectation)

SchedNEM.plot_timeseries_results(m_expectation, sys_original; region=[5,6,7,8])
title!("Schedule with original schedule | NSW")
xlabs = Dates.format.(DateTime.(sys_original.timestamps[idx_start:idx_start+47]), "dd/mm HH:MM")
xticks!(1:12:48, xlabs[1:12:end])
plot!(leftmargin=5Plots.mm, dpi=300, size=(800,400))
savefig(joinpath(@__DIR__, "figures", "A3-expectation_schedule.png"))



#%%

sample = 81
# Now the system with outages
initial_soc_stor, initial_soc_genstor, 
        p_gen_initial, gon_initial, stup_before, 
        shdw_before, gen_fail_before = SchedNEM.get_system_parameters(res, idx_start, 48, m[:genOpDetails], genAv.available[:, :, sample])  

SchedNEM.update_model_parameters!(m, sys_original, idx_start; 
    initial_soc_stor=initial_soc_stor, initial_soc_genstor=initial_soc_genstor, 
    p_gen_initial=p_gen_initial, gon_initial=gon_initial, stup_before=stup_before, 
    shdw_before=shdw_before, gen_fail_before=gen_fail_before)

# Fix all the dispatch
SchedNEM.updateGenAvailabilityFullHorizon!(m, genAv.available[:, :, sample], idx_start)
SchedNEM.updateLineAvailabilityFullHorizon!(m, sys_original, idx_start, lineAv.available[:, :, sample])

optimize!(m)
assert_is_solved_and_feasible(m)

SchedNEM.plot_timeseries_results(m, sys_original; region=[5,6,7,8])
title!("Redispatched schedule 2025 | NSW")
xlabs = Dates.format.(DateTime.(sys_original.timestamps[idx_start:idx_start+47]), "dd/mm HH:MM")
xticks!(1:12:48, xlabs[1:12:end])
plot!(leftmargin=5Plots.mm, dpi=300, size=(800,400))
savefig(joinpath(@__DIR__, "figures", "A3-redispatch_schedule.png"))

