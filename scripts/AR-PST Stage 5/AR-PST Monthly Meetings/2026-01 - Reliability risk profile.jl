# Systematically assessing the impact of imperfect storage operation on the reliability risk profile


#%% Initialise all dependencies and packages

#using SchedNEM NOTE: SchedNEM is not yet publicly available, so this code will not run without access to the SchedNEM package. Please contact the author for access if needed.
using PISP
using PRASNEM
using PRAS
using Plots
using CSV
using DataFrames
using Dates
using Gurobi
using Statistics

#%% Define parameters

# Simulation parameters
nsamples = 10000
target_year = 2030
poe = 10
reference_year = 4006 #2011:1:2023

add_lines = PRASNEM.get_added_lines_per_year(2) # Get actionable and committed lines (Scenario 2 - Step Change)

# File paths (adjust as necessary)
input_folder_base = joinpath("F:","PhD Data", "pisp-datasets")
output_folder_base = joinpath("F:","PhD Data", "pras-files")
data_folder_base = joinpath("./scripts/AR-PST Monthly Meetings/data/2026-01")

# Further parameters
Niterations = round(Int, nsamples / 100)

#%% Run adequacy assessments with different storage operation approaches, and save the results

# ==========================  Create the PRAS system ==========================

input_folder = joinpath(input_folder_base, "out-ref$(reference_year)-poe$(poe)", "csv")
output_folder = joinpath(output_folder_base, "out-ref$(reference_year)-poe$(poe)")
timeseries_folder = "schedule-$(target_year)"

sys = PRASNEM.create_pras_system(
    DateTime("$(target_year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS"),
    DateTime("$(target_year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS"),
    input_folder, timeseries_folder; line_alias_included=add_lines[target_year], output_folder=output_folder)

sys.attrs["additional_offset_DispatchProblem"] = "12" # So that PRAS is always charging/discharging even if storage is far away

# ==========================  Approach 0: System without storage ==========================
sys_no_stor = deepcopy(sys)
sys_no_stor.storages.energy_capacity .= 0.0
sys_no_stor.demandresponses.borrow_capacity .= 0.0

# ==========================  Approach 1: Greedy operation (default) ==========================
# (No changes needed, as this is the default operation in PRASNEM)

# ==========================  Approach 2: AEMO energy capacity derating ========================== 
sys_stor_derated = deepcopy(sys)
sys_stor_derated = PRASNEM.updateEnergyDerating(sys_stor_derated)

# ==========================  Approach 3: Compare with expecation dispatch ========================== 
println("Calculating the storage dispatch with SchedNEM for reference year $(reference_year)...")
m = SchedNEM.build_operation_model(sys; optimisation_window=48, move_forward=24, input_folder=input_folder, optimiser=Gurobi.Optimizer())
res = SchedNEM.run_operation_model(m, sys; output_folder_schedule=output_folder)
sys_stor_fixed = deepcopy(sys)
sys_stor_fixed = PRASNEM.updateExpectationDispatch(sys_stor_fixed, res; include_genstorage=false)

# ==========================  Running the adequacy assessments ==========================
println("Running adequacy assessment for system without storage for reference year $(reference_year)...")

# Split in multiple iterations to avoid RAM issues
for i in 1:1:Niterations
    println("     Sample batch $i/$Niterations...")
    simspec = SequentialMonteCarlo(samples=100, seed=i);
    resultspecs = (ShortfallSamples(), StorageEnergySamples(),);

    # 0: No storage and no DR
    #sfsamples_nostor, sesamples_nostor, = assess(sys_no_stor, simspec, resultspecs...)
    #df_nostor = PRASNEM.get_all_event_details(sfsamples_nostor; sesamples=sesamples_nostor, sys=sys_no_stor)
    #CSV.write("$(data_folder_base)/$(target_year)_nostor_ref$(reference_year)_poe$(poe)_$i.csv", df_nostor)
    df_nostor = CSV.read("$(data_folder_base)/$(target_year)_nostor_ref$(reference_year)_poe$(poe)_$i.csv", DataFrame)

    # 1: Greedy storage operation
    sfsamples_greedy, sesamples_greedy, = assess(sys, simspec, resultspecs...)
    df_greedy = PRASNEM.get_all_event_details(sfsamples_greedy; sesamples=sesamples_greedy, sys=sys, df_nostor=df_nostor) # Use the sys storage capacity here to compare to potential full capacity
    CSV.write("$(data_folder_base)/$(target_year)_greedy_ref$(reference_year)_poe$(poe)_$i.csv", df_greedy)

    # 2: Derated storage operation
    sfsamples_derated, sesamples_derated, = assess(sys_stor_derated, simspec, resultspecs...)
    df_derated = PRASNEM.get_all_event_details(sfsamples_derated; sesamples=sesamples_derated, sys=sys, df_nostor=df_nostor)
    CSV.write("$(data_folder_base)/$(target_year)_derated_ref$(reference_year)_poe$(poe)_$i.csv", df_derated)

    # 3: Expectation dispatch
    simspec = SequentialMonteCarlo(samples=100, seed=i);
    sfsamples_expectation,  = assess(sys_stor_fixed, simspec, ShortfallSamples()) # Only need shortfall samples here, as we will get storage energy from the operation model
    df_expectation = PRASNEM.get_all_event_details(sfsamples_expectation; sys=sys_stor_fixed, df_nostor=df_nostor)
    # and add storage energy before event and during critical period from the schedule
    res_total_soc = sum(res.stor_energy, dims=1) ./ sum(sys.storages.energy_capacity, dims=1)
    df_expectation.storages_energy_before = res_total_soc[1, max.(df_expectation.start_index .- 1,1)]
    df_expectation.storages_energy_start_critical_period = res_total_soc[1, max.(df_expectation.start_critical_index .- 1, 1)]
    CSV.write("$(data_folder_base)/$(target_year)_expectation_ref$(reference_year)_poe$(poe)_$i.csv", df_expectation)
end


#%% ========================== Analysing the data ==========================

# Read in all the data and combine into a single DataFrame for analysis

df_greedy_all = DataFrame()
df_derated_all = DataFrame()
df_expectation_all = DataFrame()

# Select Niterations if only a subset is needed for analysis
#Niterations = 100

for i in 1:1:Niterations
    df_greedy = CSV.read("$(data_folder_base)/$(target_year)_greedy_ref$(reference_year)_poe$(poe)_$i.csv", DataFrame)
    df_greedy[!,:reference_year] .= reference_year
    df_greedy[!,:sample] .= df_greedy.sample .+ round(Int, (i .- 1) .* nsamples ./ Niterations)
    append!(df_greedy_all, df_greedy)

    df_derated = CSV.read("$(data_folder_base)/$(target_year)_derated_ref$(reference_year)_poe$(poe)_$i.csv", DataFrame)
    df_derated[!,:reference_year] .= reference_year
    df_derated[!,:sample] .= df_derated.sample .+ round(Int, (i .- 1) .* nsamples ./ Niterations)
    append!(df_derated_all, df_derated)

    df_expectation = CSV.read("$(data_folder_base)/$(target_year)_expectation_ref$(reference_year)_poe$(poe)_$i.csv", DataFrame)
    df_expectation[!,:reference_year] .= reference_year
    df_expectation[!,:sample] .= df_expectation.sample .+ round(Int, (i .- 1) .* nsamples ./ Niterations)
    append!(df_expectation_all, df_expectation)
end


#%% Create scatter plots comparing unserved energy per event against total storage energy before the event

scatter(df_greedy_all.sum, df_greedy_all.storages_energy_start_critical_period,
    ylabel="Total storage energy before event\n[Normalised]", xlabel="Unserved energy per event [MWh]",
    label = "Greedy",  
    markersize=4, legend=:bottomright)
ylims!(0.0,1.1)
xlims!(1, maximum(vcat(df_greedy_all.sum, df_derated_all.sum, df_expectation_all.sum)) + 10)
plot!(xscale=:log10, yscale=:identity)
#savefig("./scripts/AR-PST Monthly Meetings/figures/2026-01_storage_operation_scatter1.png")

scatter(df_greedy_all.sum, df_greedy_all.storages_energy_start_critical_period,
    ylabel="Total storage energy before event\n[Normalised]", xlabel="Unserved energy per event [MWh]",
    label = "Greedy",  
    markersize=4, legend=:bottomright)
scatter!(df_derated_all.sum, df_derated_all.storages_energy_start_critical_period,  
    label = "Derated",
    markersize=4)
ylims!(0.0,1.1)
xlims!(1, maximum(vcat(df_greedy_all.sum, df_derated_all.sum, df_expectation_all.sum)) + 10)
plot!(xscale=:log10, yscale=:identity)
#savefig("./scripts/AR-PST Monthly Meetings/figures/2026-01_storage_operation_scatter2.png")


scatter(df_greedy_all.sum, df_greedy_all.storages_energy_start_critical_period,
    ylabel="Total storage energy before event\n[Normalised]", xlabel="Unserved energy per event [MWh]",
    label = "Greedy",  
    markersize=4, legend=:bottomright)
scatter!(df_derated_all.sum, df_derated_all.storages_energy_start_critical_period,  
    label = "Derated",
    markersize=4)
scatter!(df_expectation_all.sum, df_expectation_all.storages_energy_start_critical_period,  
    label = "Market-Based",
    markersize=4)
ylims!(0.0,1.1)
xlims!(1, maximum(vcat(df_greedy_all.sum, df_derated_all.sum, df_expectation_all.sum)) + 10)
plot!(xscale=:log10, yscale=:identity)
#savefig("./scripts/AR-PST Monthly Meetings/figures/2026-01_storage_operation_scatter3.png")


#%%

# Calculate the metrics
nsamples = Niterations * 100
all_use_samples = zeros(3,nsamples)
all_lolh_samples = zeros(3,nsamples)

for i in 1:nsamples
    all_use_samples[1,i] = sum(df_greedy_all.sum[df_greedy_all.sample .== i])
    all_use_samples[2,i] = sum(df_derated_all.sum[df_derated_all.sample .== i])
    all_use_samples[3,i] = sum(df_expectation_all.sum[df_expectation_all.sample .== i])
    all_lolh_samples[1,i] = sum(df_greedy_all.length[df_greedy_all.sample .== i])
    all_lolh_samples[2,i] = sum(df_derated_all.length[df_derated_all.sample .== i])
    all_lolh_samples[3,i] = sum(df_expectation_all.length[df_expectation_all.sample .== i])
end

println("Greedy: ", mean(all_use_samples[1, :]), " | Derated: ", mean(all_use_samples[2, :]), " | Expectation: ", mean(all_use_samples[3, :]))


#%%
subset_idxs = 1:size(all_use_samples, 2) # Use all samples here, but can also select a subset if desired (e.g., 1:1000)

EUE_greedy = mean(all_use_samples[1, subset_idxs])
EUE_derated = mean(all_use_samples[2, subset_idxs])
EUE_expectation = mean(all_use_samples[3, subset_idxs])

bar(["Greedy", "Derated", "Market-Based"], [EUE_greedy, EUE_derated, EUE_expectation],
    ylabel="EUE [MWh]", xlabel="",
    legend=false, color=[1, 2, 3], dpi=300, size=(400,300))
#savefig("./scripts/AR-PST Monthly Meetings/figures/2026-01_storage_operation_eue.png")

LOLH_greedy = mean(all_lolh_samples[1, subset_idxs])
LOLH_derated = mean(all_lolh_samples[2, subset_idxs])
LOLH_expectation = mean(all_lolh_samples[3, subset_idxs])

bar(["Greedy", "Derated", "Market-Based"], [LOLH_greedy, LOLH_derated, LOLH_expectation],
    ylabel="LOLH [h/year]", xlabel="",
    legend=false, color=[1, 2, 3], dpi=300, size=(400,300))
#savefig("./scripts/AR-PST Monthly Meetings/figures/2026-01_storage_operation_lolh.png")

VaR_greedy = quantile(all_use_samples[1, subset_idxs], 0.95)
VaR_derated = quantile(all_use_samples[2, subset_idxs], 0.95)
VaR_expectation = quantile(all_use_samples[3, subset_idxs], 0.95)

idxs_var_greedy = findall(all_use_samples[1, subset_idxs] .>= VaR_greedy)
idxs_var_derated = findall(all_use_samples[2, subset_idxs] .>= VaR_derated)
idxs_var_expectation = findall(all_use_samples[3, subset_idxs] .>= VaR_expectation)

CVaR_greedy = mean(all_use_samples[1, subset_idxs][idxs_var_greedy])
CVaR_derated = mean(all_use_samples[2, subset_idxs][idxs_var_derated])
CVaR_expectation = mean(all_use_samples[3, subset_idxs][idxs_var_expectation])

bar(["Greedy", "Derated", "Market-Based"], [CVaR_greedy, CVaR_derated, CVaR_expectation],
    ylabel="CVaR-95% [MWh]", xlabel="",
    legend=false, color=[1, 2, 3], dpi=300, size=(400,300))
#savefig("./scripts/AR-PST Monthly Meetings/figures/2026-01_storage_operation_cvar.png")