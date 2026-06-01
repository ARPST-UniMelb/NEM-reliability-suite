


using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../../functions/all_functions.jl"));


#%%
target_year = 2040
ref = 2011
poe = 10
case = "baseVPP"
pras_folder = joinpath("Z:/", "pras-files", case)
pras_filenames = readdir(joinpath(pras_folder, readdir(pras_folder)[1]))
schedules_folder = "Z:/schedules/$case/"


sys = PRAS.SystemModel(joinpath(pras_folder, "out-ref$(ref)-poe$(poe)", pras_filenames[findfirst(x -> occursin("$(target_year)-", x), pras_filenames)]))
schedule = SchedNEM.read_schedule(joinpath(schedules_folder, "out-ref$(ref)-poe$(poe)", "ty$(target_year)", "$target_year-ref$(ref)-poe$(poe)-s2.h5"))

PRASNEM.updateStorageOutageDerating!(sys)

sys_derated = deepcopy(sys)
PRASNEM.updateEnergyDerating!(sys_derated)

simspecs = SequentialMonteCarlo(samples=100, seed=1)
resspecs = (Shortfall(), StorageEnergy(), GeneratorStorageEnergy())
sf, se, gse  = assess(sys, simspecs, resspecs...)
sf_derated, se_derated, gse_derated  = assess(sys_derated, simspecs, resspecs...)

#%%


existing_stor_idxs = findall(x -> x > 0, minimum(sys.storages.energy_capacity, dims=2)[:])

area2region_map = PRASNEM.get_region_area_map(;rev=true)
all_x = collect(1:size(sys.storages.energy_capacity, 1))

lab_pos = zeros(5)
for i in 1:5
   all_regions_after_i = vcat([area2region_map[j] for j in collect(i:5)]...)
   all_stors_in_region = vcat(sys.region_stor_idxs[all_regions_after_i]...)
   all_x[all_stors_in_region] .+= 2

   all_stors_in_current_region = vcat(sys.region_stor_idxs[area2region_map[i]]...)
   lab_pos[i] = mean(all_x[all_stors_in_current_region])
end

bar(all_x, (mean(se.energy_mean, dims=2)[:] ./ maximum(sys.storages.energy_capacity, dims=2)[:] .* 100)[existing_stor_idxs], 
        label="A1: High Energy", fillalpha=1.0, lw=0, lc=1, legend=:bottomright)
bar!(all_x, (mean(se_derated.energy_mean, dims=2)[:] ./ maximum(sys.storages.energy_capacity, dims=2)[:] .* 100)[existing_stor_idxs], label="A2: Derated Energy", 
   fillalpha=1.0, lw=0, lc=2)
bar!(all_x, (mean(schedule.stor_energy, dims=2)[:] ./ maximum(sys.storages.energy_capacity, dims=2)[:] .* 100)[existing_stor_idxs], label="A3: Economic Operation", 
fillalpha=1.0, lw=0, lc=3, dpi=600, size=(900, 400),
bottommargin=5Plots.mm, leftmargin=5Plots.mm, rightmargin=5Plots.mm, topmargin=5Plots.mm)


xlims!(1, all_x[end] + 2)
xlabel!("Storage units per state")
ylabel!("Average state of charge [%]")
title!("Storage energy levels | Planning year $(target_year)")
ylims!(0, 100)
xticks!(lab_pos, ["QLD","NSW","VIC","TAS","SA"])

savefig(joinpath(@__DIR__, "figures", "6-Storage_energy_levels.png"))

#%%
# Charging and discharging 

#%% Charging/discharging power over time of day

idx_relevant = 1:366 #days

ch_dch = (se.energy_mean .- hcat(zeros(size(se.energy_mean[:,1])),se.energy_mean[:, 1:end-1])) .* sys.storages.discharge_efficiency
ch_dch_derated = (se_derated.energy_mean .- hcat(zeros(size(se_derated.energy_mean[:,1])),se_derated.energy_mean[:, 1:end-1])) .* sys.storages.discharge_efficiency

ch_greedy = reshape(sum(ch_dch[:,idx_q1], dims=1)[:] ./ sum(sys.storages.discharge_capacity[:,idx_q1], dims=1)[:] .* 100, 24, :)'
ch_derated = reshape(sum(ch_dch_derated[:,idx_q1], dims=1)[:] ./ sum(sys.storages.discharge_capacity[:,idx_q1], dims=1)[:] .* 100, 24, :)'
ch_rolling = reshape(sum(schedule.stor_discharging .- schedule.stor_charging, dims=1)[idx_q1] ./ sum(sys.storages.discharge_capacity[:,idx_q1], dims=1)[:] .* 100, 24, :)'


iqr_greedy_up = [quantile(ch_greedy[idx_relevant, i], 0.75) for i in 1:24]
iqr_greedy_low = [quantile(ch_greedy[idx_relevant, i], 0.25) for i in 1:24]

iqr_derated_up = [quantile(ch_derated[idx_relevant, i], 0.75) for i in 1:24]
iqr_derated_low = [quantile(ch_derated[idx_relevant, i], 0.25) for i in 1:24]

iqr_rolling_up = [quantile(ch_rolling[idx_relevant, i], 0.75) for i in 1:24]
iqr_rolling_low = [quantile(ch_rolling[idx_relevant, i], 0.25) for i in 1:24]

plot(median(ch_greedy[idx_relevant, :], dims=1)[:], label="A1: High Energy", alpha=1.0, lw=2, legend=:topleft, color=1)
plot!(iqr_greedy_up, label="", fillalpha=0.2, lw=0, color=1, fillrange=iqr_greedy_low)
plot!(median(ch_derated[idx_relevant, :], dims=1)[:], label="A2: Derated Energy", fillalpha=0.0, lw=2, color=2)
plot!(iqr_derated_up, label="", fillalpha=0.2, lw=0, color=2, fillrange=iqr_derated_low)
plot!(median(ch_rolling[idx_relevant, :], dims=1)[:], label="A3: Economic Operation", 
   fillalpha=1.0, lw=2, fillstyle = :/, color=3)
plot!(iqr_rolling_up, label="", fillalpha=0.2, lw=0, color=3, fillrange=iqr_rolling_low, dpi=300)
xlabel!("Time of day")
xticks!(1:6:24, string.(collect(0:6:23)))
ylabel!("Charge/Discharge [% of power capacity]")
title!("Average storage operation | Planning year 2040")

savefig(joinpath(@__DIR__, "figures", "6-storage_operation_time_of_day.png"))


