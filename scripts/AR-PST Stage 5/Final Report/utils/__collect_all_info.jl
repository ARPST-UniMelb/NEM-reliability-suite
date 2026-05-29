

using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../../functions/all_functions.jl"));

#%%

target_years = [2025, 2030, 2035, 2040]
ref_poe_scen_sets = [(ref, poe, 2) for ref in collect(2011:2023) for poe in [10, 50]]
samples = 100


r = create_summary("baseVPP";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

#%%

case = "baseVPP"

pras_folder = "Z:/pras-files/$case/"
pras_filenames = readdir(joinpath(pras_folder, "out-ref2011-poe10"))

schedules_folder = "Z:/schedules/$case/"
simspecs = SequentialMonteCarlo(samples=3, seed=1234)
resspecs = (StorageEnergy(),)

all_vre_production = zeros(length(target_years), length(ref_poe_scen_sets), 8784)
all_roof_pv_production = zeros(length(target_years), length(ref_poe_scen_sets), 8784)
all_demand = zeros(length(target_years), length(ref_poe_scen_sets), 8784)

all_mean_shortfalls = zeros(length(target_years), length(ref_poe_scen_sets), 8784)
all_mean_shortfalls_derated = zeros(length(target_years), length(ref_poe_scen_sets), 8784)
all_mean_shortfalls_greedy = zeros(length(target_years), length(ref_poe_scen_sets), 8784)

all_socs = zeros(length(target_years), length(ref_poe_scen_sets), 8784)
all_socs_derated = zeros(length(target_years), length(ref_poe_scen_sets), 8784)
all_socs_greedy = zeros(length(target_years), length(ref_poe_scen_sets), 8784)

all_vpp_operation = zeros(length(target_years), length(ref_poe_scen_sets), 8784)

for (i, (ref, poe, scen)) in enumerate(ref_poe_scen_sets)
   println("Processing reference year $ref, POE $poe...")
   for target_year in target_years

      # Get the VRE margin time series
      sys = PRAS.SystemModel(joinpath(pras_folder, "out-ref$(ref)-poe$(poe)", pras_filenames[findfirst(x -> occursin("$(target_year)-12-31", x), pras_filenames)]))
      sys = PRASNEM.updateStorageOutageDerating!(sys)

      roofpv_idxs = findall(x -> x == "RoofPV", sys.generators.categories)
      all_roof_pv_production[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(sys.generators.capacity[roofpv_idxs, :], dims=1)[:]

      all_demand[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(sys.regions.load, dims=1)[:]

      vre_idxs = findall(x -> x in ["Wind", "LargePV"], sys.generators.categories)
      all_vre_production[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(sys.generators.capacity[vre_idxs, :], dims=1)[:]


      # Get the storage state of charge
      all_max_energy = copy(sum(sys.storages.energy_capacity, dims=1)[:]) # Before derating, for normalisation of SOC
      schedule = SchedNEM.read_schedule(joinpath(schedules_folder, "out-ref$(ref)-poe$(poe)", "ty$(target_year)", "$target_year-ref$(ref)-poe$(poe)-s2.h5"))
      all_socs[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(schedule.stor_energy, dims=1)[:] ./ all_max_energy
      all_vpp_operation[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(schedule.stor_discharging[findall(sys.storages.categories .== "VPP"), :], dims=1)[:] .- sum(schedule.stor_charging[findall(sys.storages.categories .== "VPP"), :], dims=1)[:]

      se_greedy, = assess(sys, simspecs, resspecs...)
      all_socs_greedy[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(se_greedy.energy_mean, dims=1)[:] ./ all_max_energy

      sys = PRASNEM.updateEnergyDerating!(sys)
      se,  = assess(sys, simspecs, resspecs...)
      all_socs_derated[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= sum(se.energy_mean, dims=1)[:] ./ all_max_energy

      # And get the shortfalls
      res = mean(sum(SchedNEM.readSfMatrix(joinpath("Z:/results/$case/", "out-ref$(ref)-poe$(poe)", "ty$(target_year)", "sf_samples_reoptimised_s2_batch1.csv")), dims=1), dims=3)[:]
      res_derated = mean(sum(SchedNEM.readSfMatrix(joinpath("Z:/results/$case/", "out-ref$(ref)-poe$(poe)", "ty$(target_year)", "sf_samples_derated_s2_batch1.csv")), dims=1), dims=3)[:]
      res_greedy = mean(sum(SchedNEM.readSfMatrix(joinpath("Z:/results/$case/", "out-ref$(ref)-poe$(poe)", "ty$(target_year)", "sf_samples_greedy_s2_batch1.csv")), dims=1), dims=3)[:]

      all_mean_shortfalls[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= res
      all_mean_shortfalls_derated[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= res_derated
      all_mean_shortfalls_greedy[findfirst(target_years .== target_year), i, 1:length(sys.timestamps)] .= res_greedy # Placeholder until we have the greedy shortfalls
   end
end
