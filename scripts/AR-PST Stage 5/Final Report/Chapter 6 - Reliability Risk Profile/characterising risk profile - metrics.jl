

using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));

#%%


target_years = [2025, 2030, 2035, 2040]
samples = 100
case = "baseVPP"
storage_case = "reoptimised"

pras_folder = joinpath("Z:/", "pras-files", case)
pras_filenames = readdir(joinpath(pras_folder, readdir(pras_folder)[1]))

results_folder = "Z:/results/$case/"

ref_poe_scen_sets = [(yr, poe, 2) for yr in collect(2011:2023) for poe in [10, 50]]

poe_weights = Dict(10 => 0.304, 50 => 0.392)
#poe_weights = Dict(10 => 1.0, 50 => 1.0) # For equal weighting of the two POEs, for now

NEUE_per_area = zeros(5, length(target_years))
NEUE_per_ref_year = zeros(round(Int, length(ref_poe_scen_sets)/2), length(target_years))

for (tyi, target_year) in enumerate(target_years)

   pras_filename = pras_filenames[findfirst(x -> occursin("$(target_year)-01-01_to_$(target_year)-12-31", x), pras_filenames)]
   all_sf = zeros(5,length(ref_poe_scen_sets))
   all_load = zeros(5,length(ref_poe_scen_sets))
   all_weights = zeros(length(ref_poe_scen_sets))

   for (k, ref_poe_scen_set) in enumerate(ref_poe_scen_sets)
      println("Processing reference year $(ref_poe_scen_set[1]) and POE $(ref_poe_scen_set[2])...")
      filename_output = joinpath(results_folder, "out-ref$(ref_poe_scen_set[1])-poe$(ref_poe_scen_set[2])", "ty$(target_year)", "sf_samples_$(storage_case)_s2_batch1.csv")
      if !isfile(filename_output)
         @error "Output file $filename_output not found. Skipping any further batches of this case."
         break
      end

      sf_area = SchedNEM.eensAreaFromSfMatrix(filename_output)

      filename_pras = joinpath(pras_folder, "out-ref$(ref_poe_scen_set[1])-poe$(ref_poe_scen_set[2])", pras_filename)
      sys = PRAS.SystemModel(filename_pras)

      all_load_temp = sum(sys.regions.load, dims=2)[:]
      area2region = PRASNEM.get_region_area_map(;rev=true)

      for i in 1:5
         all_sf[i, k] = sf_area[i]
         all_load[i, k] = sum(all_load_temp[area2region[i]])
      end
      all_weights[k] = poe_weights[ref_poe_scen_set[2]]
   end

   println("        Calculating weighted NEUE for target year $target_year...")
   all_sf_area_weighted = all_sf .* all_weights' ./ all_load .* 1e6

   all_sf_overall_weighted = sum(all_sf, dims=1)[:] .* all_weights ./ sum(all_load, dims=1)[:] .* 1e6

   # Calculate the total NEUE per area and per reference year (weighted)
   refs = [r[1] for r in ref_poe_scen_sets]
   NEUE_per_area_per_ref_year = zeros(5, length(unique(refs)))
   for (i, ref) in enumerate(unique(refs))
      NEUE_per_area_per_ref_year[:, i] = sum(all_sf_area_weighted[:, refs .== ref], dims=2)[:]
      NEUE_per_ref_year[i, tyi] = sum(all_sf_overall_weighted[refs .== ref])
   end

   NEUE_per_area[:, tyi] = mean(NEUE_per_area_per_ref_year, dims=2)[:]
end

#%%

labs = ["QLD" "NSW" "VIC" "TAS" "SA"]
c = ["#9B2242" "#A1DAE7" "#7C7FAB" "#A8D088" "#E56A54"]

groupedbar(NEUE_per_area' ./ 1e4, label=labs, c = c, size=(500,300))
plot!([0.75,4.25], [20,20] ./ 1e4, c=:black, lw=1, ls=:dash, label="Reliability Standard")
xlabel!("Planning Year")
xticks!(1:length(target_years), string.(target_years), dpi=300)
ylims!(0, 0.006)
xlims!(0.5, length(target_years) + 0.5)
ylabel!("Annual average USE [%]")
title!("Base case | A3: Economic Operation")
savefig(joinpath(@__DIR__, "figures", "7-NEUE_per_area.png"))

#%%

cs = ["#6A2A7A" "#982040" "#40C0A8" "#F0C860" "#88D070" "#5868B0" "#A0D8E8" "#D8D880" "#E07058" "#A01840" "#A850A8" "#E8E8E8" "#E8A0A8" "#70A0D8"]
labs = string.(unique([r[1] for r in ref_poe_scen_sets])')
#cs = collect(1:2:25)'
groupedbar(NEUE_per_ref_year' / 1e4, label=labs, legend=:topleft, c = cs, size=(500,300))
plot!([0.75,4.25], [20,20] ./ 1e4, c=:black, lw=1, ls=:dash, label="Reliability Standard")
xlabel!("Planning Years")
xticks!(1:length(target_years), string.(target_years), dpi=300)
ylims!(0, 0.006)
xlims!(0.5, length(target_years) + 0.5)
ylabel!("Annual average USE [%]")
title!("Base case | A3: Economic Operation")
savefig(joinpath(@__DIR__, "figures", "7-NEUE_per_ref_year.png"))


#%%

ref_poe_scen_sets = [(ref, poe, 2) for ref in collect(2011:2023) for poe in [10, 50]]

r_greedy = create_summary("baseVPP"; 
   samples=100, 
   storage_case="greedy", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   apply_demand_weights=true, 
   )

r_derated = create_summary("baseVPP"; 
   samples=100, 
   storage_case="derated", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   apply_demand_weights=true, 
   )

r = create_summary("baseVPP"; 
   samples=100, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   apply_demand_weights=true, 
   )


#%%

#%% =====================================================================================================================

function calculate_weighted_metrics(events::DataFrame; target_years=[2025, 2030, 2035, 2040], total_number_of_samples=3900)

   samples_per_year = 100

   all_freq_mean = zeros(length(target_years))
   all_freq_cvar = zeros(length(target_years))

   all_lolh_mean = zeros(length(target_years))
   all_lolh_cvar = zeros(length(target_years))

   all_use_mean = zeros(length(target_years))
   all_use_cvar = zeros(length(target_years))

   # First count the number of events per year, reference year, POE, and sample for each case
   events.normalised_magnitude_mwh = events.magnitude_mwh ./ events.total_load_mwh * 1e2
   events_combined = combine(groupby(events, [:year, :ref_year, :poe, :sample, :poe_weight]), nrow => :count, :normalised_magnitude_mwh => sum => :total_normalised_magnitude_mwh, :duration_hrs => sum => :total_duration_hrs)

   for group in groupby(events_combined, :year)
      ty = group.year[1]

      df_temp = DataFrame(group)
      additional_zero_df = DataFrame(year=[], ref_year=[], poe=[], sample=[], poe_weight=[], count=[], total_normalised_magnitude_mwh=[], total_duration_hrs=[])

      # Add all the samples that had no events for this year, with count = 0, total_normalised_magnitude_mwh = 0, and total_duration_hrs = 0
      for group2 in groupby(df_temp, [:ref_year, :poe])
         for sample in setdiff(1:samples_per_year, group2.sample)
            push!(additional_zero_df, [ty, group2.ref_year[1], group2.poe[1], sample, group2.poe_weight[1], 0, 0.0, 0.0])
         end
      end

      df_temp = vcat(df_temp, additional_zero_df)

      temp = combine(groupby(df_temp, [:poe_weight]), :count => mean, :total_normalised_magnitude_mwh => mean, :total_duration_hrs => mean)

      all_freq_mean[target_years .== ty] .= sum(temp.count_mean .* temp.poe_weight)
      all_lolh_mean[target_years .== ty] .= sum(temp.total_duration_hrs_mean .* temp.poe_weight)
      all_use_mean[target_years .== ty] .= sum(temp.total_normalised_magnitude_mwh_mean .* temp.poe_weight)

      # Frequency
      all_freq_temp = vcat(df_temp.count, zeros(total_number_of_samples - nrow(df_temp))) # Add zeros for the samples with no events
      var = quantile(all_freq_temp, 0.95)
      all_freq_cvar[target_years .== ty] .= mean(all_freq_temp[all_freq_temp .>= var])

      # LOLH
      all_lolh_temp = vcat(df_temp.total_duration_hrs, zeros(total_number_of_samples - nrow(df_temp))) # Add zeros for the samples with no events
      var = quantile(all_lolh_temp, 0.95)
      all_lolh_cvar[target_years .== ty] .= mean(all_lolh_temp[all_lolh_temp .>= var])

      # USE
      all_use_temp = vcat(df_temp.total_normalised_magnitude_mwh, zeros(total_number_of_samples - nrow(df_temp))) # Add zeros for the samples with no events
      var = quantile(all_use_temp, 0.95)
      all_use_cvar[target_years .== ty] .= mean(all_use_temp[all_use_temp .>= var])
   end

   return (NEUE=all_use_mean, NCVAR=all_use_cvar, LOLH=all_lolh_mean, LOLH_CVAR=all_lolh_cvar, LOLF=all_freq_mean, LOLF_CVAR=all_freq_cvar)
end

metrics_greedy = calculate_weighted_metrics(r_greedy.events)
metrics_derated = calculate_weighted_metrics(r_derated.events)
metrics_reoptimised = calculate_weighted_metrics(r.events)

#%%

plot(res_greedy.NEUE ./ 1e4)
plot(metrics_greedy.NEUE)
plot!(metrics_derated.NEUE)
plot!(metrics_reoptimised.NEUE)


#%% ========================================================================================
# Metrics with 
target_years = [2025, 2030, 2035, 2040]
res = unstack(r.metrics, :metric, :value)
res_derated = unstack(r_derated.metrics, :metric, :value)
res_greedy = unstack(r_greedy.metrics, :metric, :value)

kwargs = (lw=2, dpi=300, size=(500,300), legend=:outertop)

p1 = plot(res_greedy.NEUE ./ 1e4, markershape=:diamond, label="A1: High energy", c=1; kwargs...)
p1 = plot!(res_derated.NEUE ./ 1e4, markershape=:square, label="A2: Derated energy", c=2; kwargs...)
p1 = plot!(res.NEUE ./ 1e4, markershape=:circle, label="A3: Economic operation", c=3,
   ylabel="Average annual USE [%]",
   leftmargin=5Plots.mm; kwargs...)
p1 = plot!([0.9,4.1], [20,20] ./ 1e4, c=:black, lw=1, ls=:dash, label="Reliability Standard")
xlabel!("Planning years")
xticks!(1:length(target_years), string.(target_years))

p2 = plot(res_greedy.NCVAR95 ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, ls=:dash; kwargs...)
p2 = plot!(res_derated.NCVAR95 ./ 1e4, markershape=:square, label="A2: Derated energy", c=2, ls=:dash;  kwargs...)
p2 = plot!(res.NCVAR95 ./ 1e4, markershape=:circle, label="A3: Economic operation", c=3, ls=:dash,
   ylabel="CVaR(95%) of average USE [%]"; kwargs...)
xlabel!("Planning years")
xticks!(1:length(target_years), string.(target_years))

plot(p1, p2, layout=(1,2), title="", size=(900,400), link=:all, bottommargin=5Plots.mm, leftmargin=5Plots.mm, 
   rightmargin=5Plots.mm, topmargin=5Plots.mm)


savefig(joinpath(@__DIR__, "figures", "7-NEUE_and_CVaR95_NEUE.png"))


#%% Loss of load hours
target_years = [2025, 2030, 2035, 2040]
res = unstack(r.metrics, :metric, :value)
res_derated = unstack(r_derated.metrics, :metric, :value)
res_greedy = unstack(r_greedy.metrics, :metric, :value)





kwargs = (lw=2, dpi=300, size=(500,300), legend=:outertop)

p1 = plot(res_greedy.LOLH, markershape=:diamond, label="A1: High energy", c=1; kwargs...)
p1 = plot!(res_derated.LOLH, markershape=:square, label="A2: Derated energy", c=2; kwargs...)
p1 = plot!(res.LOLH, markershape=:circle, label="A3: Economic operation", c=3,
   ylabel="Average LOLH [hours/yr]",
   leftmargin=5Plots.mm; kwargs...)
xlabel!("Planning years")
xticks!(1:length(target_years), string.(target_years))

p2 = plot(res_greedy.CVAR95_LOLH, markershape=:diamond, label="A1: High energy", c=1, ls=:dash; kwargs...)
p2 = plot!(res_derated.CVAR95_LOLH, markershape=:square, label="A2: Derated energy", c=2, ls=:dash;  kwargs...)
p2 = plot!(res.CVAR95_LOLH, markershape=:circle, label="A3: Economic operation", c=3, ls=:dash,
   ylabel="CVaR(95%) of LOLH [hours/yr]"; kwargs...)
xlabel!("Planning years")
xticks!(1:length(target_years), string.(target_years))

plot(p1, p2, layout=(1,2), title="", size=(900,400), link=:all, bottommargin=5Plots.mm, leftmargin=5Plots.mm, 
   rightmargin=5Plots.mm, topmargin=5Plots.mm)
savefig(joinpath(@__DIR__, "figures", "7-LOLH_and_CVaR95_LOLH.png"))


#%% Frequency of events

target_years = [2025, 2030, 2035, 2040]

# First count the number of events per year, reference year, POE, and sample for each case
r.events.normalised_magnitude_mwh = r.events.magnitude_mwh ./ r.events.total_load_mwh
r_derated.events.normalised_magnitude_mwh = r_derated.events.magnitude_mwh ./ r_derated.events.total_load_mwh
r_greedy.events.normalised_magnitude_mwh = r_greedy.events.magnitude_mwh ./ r_greedy.events.total_load_mwh

event_freq_reoptimised = combine(groupby(r.events, [:year, :ref_year, :poe, :sample, :poe_weight]), nrow => :count, :normalised_magnitude_mwh => sum => :total_normalised_magnitude_mwh, :duration_hrs => sum => :total_duration_hrs)
event_freq_derated = combine(groupby(r_derated.events, [:year, :ref_year, :poe, :sample, :poe_weight]), nrow => :count, :normalised_magnitude_mwh => sum => :total_normalised_magnitude_mwh, :duration_hrs => sum => :total_duration_hrs)
event_freq_greedy = combine(groupby(r_greedy.events, [:year, :ref_year, :poe, :sample, :poe_weight]), nrow => :count, :normalised_magnitude_mwh => sum => :total_normalised_magnitude_mwh, :duration_hrs => sum => :total_duration_hrs)


# Greedy case
for group in groupby(event_freq_greedy, :year)
   ty = group.year[1]

   temp = combine(groupby(group, [:poe_weight]), :count => mean, :total_normalised_magnitude_mwh => mean, :total_duration_hrs => mean)
   all_freq_mean[target_years .== ty, 1] .= sum(temp.count_mean .* temp.poe_weight)
   all_lolh_mean[target_years .== ty, 1] .= sum(temp.total_duration_hrs_mean .* temp.poe_weight)
   all_use_mean[target_years .== ty, 1] .= sum(temp.total_normalised_magnitude_mwh_mean .* temp.poe_weight)

   # Frequency
   all_freq_temp = vcat(group.count, zeros(total_number_of_samples - nrow(group))) # Add zeros for the samples with no events
   var = quantile(all_freq_temp, 0.95)
   all_freq_cvar[target_years .== ty, 1] .= mean(all_freq_temp[all_freq_temp .>= var])

   all_lolh_temp = vcat(group.total_duration_hrs, zeros(total_number_of_samples - nrow(group))) # Add zeros for the samples with no events
   all_lolh_mean[target_years .== ty, 1] .= mean(all_lolh_temp)
   var = quantile(all_lolh_temp, 0.95)
   all_lolh_cvar[target_years .== ty, 1] .= mean(all_lolh_temp[all_lolh_temp .>= var])
end



# Create initial objects
all_use_mean = zeros(length(target_years), 3)
all_freq_mean = zeros(length(target_years), 3)
all_lolh_mean = zeros(length(target_years), 3)
all_use_cvar = zeros(length(target_years), 3)
all_freq_cvar = zeros(length(target_years), 3)
all_lolh_cvar = zeros(length(target_years), 3)
total_number_of_samples = 2600 + 1300 # 100 samples for each combination + 100 samples for the (fictional) 90% POE run, for each reference year, for each target year

# Greedy case
for group in groupby(event_freq_greedy, :year)
   ty = group.year[1]

   temp = combine(groupby(group, [:poe_weight]), :count => mean, :total_normalised_magnitude_mwh => mean, :total_duration_hrs => mean)
   all_freq_mean[target_years .== ty, 1] .= sum(temp.count_mean .* temp.poe_weight)
   all_lolh_mean[target_years .== ty, 1] .= sum(temp.total_duration_hrs_mean .* temp.poe_weight)
   all_use_mean[target_years .== ty, 1] .= sum(temp.total_normalised_magnitude_mwh_mean .* temp.poe_weight)

   # Frequency
   all_freq_temp = vcat(group.count, zeros(total_number_of_samples - nrow(group))) # Add zeros for the samples with no events
   var = quantile(all_freq_temp, 0.95)
   all_freq_cvar[target_years .== ty, 1] .= mean(all_freq_temp[all_freq_temp .>= var])

   all_lolh_temp = vcat(group.total_duration_hrs, zeros(total_number_of_samples - nrow(group))) # Add zeros for the samples with no events
   all_lolh_mean[target_years .== ty, 1] .= mean(all_lolh_temp)
   var = quantile(all_lolh_temp, 0.95)
   all_lolh_cvar[target_years .== ty, 1] .= mean(all_lolh_temp[all_lolh_temp .>= var])
end

# Derated case
for group in groupby(event_freq_derated, :year)
   ty = group.year[1]
   all_freq_temp = vcat(group.count, zeros(total_number_of_samples - nrow(group))) # Add zeros for the samples with no events
   all_freq_mean[target_years .== ty, 2] .= mean(all_freq_temp)
   var = quantile(all_freq_temp, 0.95)
   all_freq_cvar[target_years .== ty, 2] .= mean(all_freq_temp[all_freq_temp .>= var])
end

# Reoptimised case
for group in groupby(event_freq_reoptimised, :year)
   ty = group.year[1]
   all_freq_temp = vcat(group.count, zeros(total_number_of_samples - nrow(group))) # Add zeros for the samples with no events
   all_freq_mean[target_years .== ty, 3] .= mean(all_freq_temp)
   var = quantile(all_freq_temp, 0.95)
   all_freq_cvar[target_years .== ty, 3] .= mean(all_freq_temp[all_freq_temp .>= var])
end

kwargs = (lw=2, dpi=300, size=(500,300), legend=:outertop)

p1 = plot(all_freq_mean[:,1], markershape=:diamond, label="A1: High energy", c=1; kwargs...)
p1 = plot!(all_freq_mean[:,2], markershape=:square, label="A2: Derated energy", c=2; kwargs...)
p1 = plot!(all_freq_mean[:,3], markershape=:circle, label="A3: Economic operation", c=3,
   ylabel="Average LOLF [events/yr]",
   leftmargin=5Plots.mm; kwargs...)
xlabel!("Planning years")
xticks!(1:length(target_years), string.(target_years))

p2 = plot(all_freq_cvar[:,1], markershape=:diamond, label="A1: High energy", c=1, ls=:dash; kwargs...)
p2 = plot!(all_freq_cvar[:,2], markershape=:square, label="A2: Derated energy", c=2, ls=:dash;  kwargs...)
p2 = plot!(all_freq_cvar[:,3], markershape=:circle, label="A3: Economic operation", c=3, ls=:dash,
   ylabel="CVaR(95%) of LOLF [events/yr]"; kwargs...)
xlabel!("Planning years")
xticks!(1:length(target_years), string.(target_years))

plot(p1, p2, layout=(1,2), title="", size=(900,400), link=:all, bottommargin=5Plots.mm, leftmargin=5Plots.mm, 
   rightmargin=5Plots.mm, topmargin=5Plots.mm)
savefig(joinpath(@__DIR__, "figures", "7-LOLF_and_CVaR95_LOLF.png"))


#%%

ref_poe_scen_sets[15]
findall(r.ens[3,:,:] .< r_greedy.ens[3,:,:])


path = joinpath("Z:/results/baseVPP/out-ref2018-poe10/ty2035/sf_samples_reoptimised_s2_batch1.csv")

SchedNEM.eensFromSfMatrix(path)
ens_reopt = SchedNEM.ensFromSfMatrix("Z:/results/baseVPP/out-ref2018-poe10/ty2035/sf_samples_reoptimised_s2_batch1.csv")
ens_greedy = SchedNEM.ensFromSfMatrix("Z:/results/baseVPP/out-ref2018-poe10/ty2035/sf_samples_greedy_s2_batch1.csv")

sys = PRAS.SystemModel(joinpath("Z:/pras-files/baseVPP/out-ref2018-poe10", "2035-01-01_to_2035-12-31_s2_all_regions_incl_line_9_10_18_23_25_29_30_34_45_49_50_51.pras"))
PRASNEM.updateStorageOutageDerating!(sys)

sfsamples, = assess(sys, SequentialMonteCarlo(samples=100, seed=1), ShortfallSamples())
sum(sfsamples.shortfall, dims=(1,2))[:]

findall(ens_reopt .< ens_greedy)


ens_greedy

s = SchedNEM.read_schedule(joinpath("Z:/schedules/baseVPP/out-ref2018-poe10/ty2035/2035-ref2018-poe10-s2.h5"))

sf_expectation = SchedNEM.readSfMatrix(joinpath("Z:/results/baseVPP/out-ref2018-poe10/ty2035/sf_samples_expectation_s2_batch1.csv"))

sum(sf_expectation, dims=(1,2))[:]

sfsamples.shortfall

#%%

sys = PRAS.SystemModel(joinpath("Z:/pras-files/baseVPP/out-ref2018-poe10", "2035-01-01_to_2035-12-31_s2_all_regions_incl_line_9_10_18_23_25_29_30_34_45_49_50_51.pras"))
PRASNEM.updateStorageOutageDerating!(sys)

m = SchedNEM.build_operation_model(sys; input_folder="Z:/pisp-datasets/base/out-ref2018-poe10/csv/",
   optimiser=Gurobi.Optimizer(),
   DER_parameters=PRASNEM.get_DER_parameters(; case="baseVPP"))

res = SchedNEM.run_operation_model(m, sys;
   include_reserve_run=true)

sum(res.shortfall)

sys_assessment = deepcopy(sys)
PRASNEM.updateDRExpectationDispatch!(sys_assessment, res)
PRASNEM.updateStorageExpectationDispatch!(sys_assessment, res)
PRASNEM.updateUnitCommitment!(sys_assessment, res)

sf_expectation_new, = assess(sys_assessment, SequentialMonteCarlo(samples=100, seed=1), ShortfallSamples())

findall(sf_expectation_new.shortfall .>= sf_expectation)


plot(sum(res.stor_energy, dims=1)[:])
plot!(sum(s.stor_energy, dims=1)[:])

scatter(sum(res.stor_energy, dims=1)[:], sum(s.stor_energy, dims=1)[:])

scatter(sf_expectation[:], sf_expectation_new.shortfall[:])


#%%

ref_poe_scen_sets_test = [(ref, poe, 2) for ref in [2011,2015] for poe in [10]]
r_test = create_summary("base"; 
   samples=100, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets_test,
   apply_demand_weights=true, 
   )

r_test_greedy = create_summary("base"; 
   samples=100, 
   storage_case="greedy", 
   ref_poe_scen_sets=ref_poe_scen_sets_test,
   apply_demand_weights=true, 
   )

#%%


