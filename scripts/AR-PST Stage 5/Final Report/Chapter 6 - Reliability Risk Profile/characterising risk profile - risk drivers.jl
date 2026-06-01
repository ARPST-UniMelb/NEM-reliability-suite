

using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../../functions/all_functions.jl"));

#%%


ref_poe_scen_sets = [(ref, poe, 2) for ref in collect(2011:2023) for poe in [10, 50]]
samples = 100


r_greedy =  create_summary("baseVPP";
   samples=samples, 
   storage_case="greedy", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

r_derated = create_summary("baseVPP";
   samples=samples, 
   storage_case="derated", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

r_reoptimised = create_summary("baseVPP";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

#%%


# Figure that shows when the events start

bins = 0:1:24

critical_hours_start = zeros(3, length(target_years), 24)

for row in eachrow(r_greedy.events)
   critical_hours_start[1, findfirst(target_years .== row.year), round(Int, row.start_index) % 24 + 1] += 1
end
critical_hours_start[1, :, :] ./= [length(findall(r_greedy.events.year .== year)) for year in target_years]

for row in eachrow(r_derated.events)
   critical_hours_start[2, findfirst(target_years .== row.year), round(Int, row.start_index) % 24 + 1] += 1
end
critical_hours_start[2, :, :] ./= [length(findall(r_derated.events.year .== year)) for year in target_years]

for row in eachrow(r_reoptimised.events)
   critical_hours_start[3, findfirst(target_years .== row.year), round(Int, row.start_index) % 24 + 1] += 1
end
critical_hours_start[3, :, :] ./= [length(findall(r_reoptimised.events.year .== year)) for year in target_years]

critical_hours_start = critical_hours_start .* 100

x = repeat(repeat(0:1:23, inner=2), 1, 3)
y = hcat(zeros(3,1), repeat(critical_hours_start[:,1, :], inner=(1,2))[:,1:end-1])
p1 = plot(x, y', label="", lw=2, c = [1 2 3], fillalpha=0.5, fillcolor=[1 2 3], xlabel="Hour of the day", 
   ylabel="USE events starting in hour [%]", leftmargin=10Plots.mm)

x = repeat(repeat(0:1:23, inner=2), 1, 3)
y = hcat(zeros(3,1), repeat(critical_hours_start[:,2, :], inner=(1,2))[:,1:end-1])
p2 = plot(x, y', label="", lw=2, c = [1 2 3], fillalpha=0.5, fillcolor=[1 2 3], xlabel="Hour of the day", 
   yticks=(0:20:100,""))

x = repeat(repeat(0:1:23, inner=2), 1, 3)
y = hcat(zeros(3,1), repeat(critical_hours_start[:,3, :], inner=(1,2))[:,1:end-1])
p3 = plot(x, y', label="", lw=2, c = [1 2 3], fillalpha=0.5, fillcolor=[1 2 3], 
 yticks=(0:20:100,""))

x = repeat(repeat(0:1:23, inner=2), 1, 3)
y = hcat(zeros(3,1), repeat(critical_hours_start[:,4, :], inner=(1,2))[:,1:end-1])
p4 = plot(x, y', label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], lw=2, c = [1 2 3], fillalpha=0.5, fillcolor=[1 2 3],
   yticks=(0:20:100,""))



plot(p1, p2, p3, p4, layout=(1,4), 
   xticks=(0.5:3:24, string.(0:3:23)), xlabel="Hour of the day", 
   size=(1200,350), link=:all,
   dpi=300,
   bottommargin=10Plots.mm, topmargin=5Plots.mm,
   title=["2025" "2030" "2035" "2040"],
   ylims=(0,100))

savefig(joinpath(@__DIR__,"figures", "critical_hours_start_by_time_of_day.png"))

#%% =========================================================================================

# Need to call ___collect_all_info.jl to get the system details across the different weather years (see utils/___collect_all_info.jl)


target_years = [2025, 2030, 2035, 2040]
all_demand_normalised = all_demand ./ maximum(all_demand, dims=3)
all_operational_demand_normalised = (all_demand .- all_roof_pv_production) ./ maximum((all_demand .- all_roof_pv_production), dims=3)


r = r_reoptimised
r.events.normalised_magnitude = r.events.magnitude_mwh ./ r.events.total_load_mwh
r.events.demand = [all_demand_normalised[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r.events)]
r.events.operational_demand = [all_operational_demand_normalised[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r.events)]

#%%
idxs_2025 = findall(r.events.year .== 2025)
idxs_2030 = findall(r.events.year .== 2030)
idxs_2035 = findall(r.events.year .== 2035)
idxs_2040 = findall(r.events.year .== 2040)


kwargs = (xlabel="Operational demand at start of event [% of annual peak]", 
    size=(600,400), dpi=300,
   xlims=(50,100), xticks=50:10:100, #ylims=(-0.5,35), yticks=0:5:35, legend=:topleft, 
   palette=:Spectral_11)

p1 = plot(r.events.operational_demand[idxs_2025] .* 100, r.events.normalised_magnitude[idxs_2025] .* 1e2, 
   seriestype=:scatter, label="", c=1, alpha=0.5,  
   ylabel="Event USE [%]", leftmargin=7Plots.mm;
   kwargs...)
p2 = plot(r.events.operational_demand[idxs_2030] .* 100, r.events.normalised_magnitude[idxs_2030] .* 1e2, 
   seriestype=:scatter, label="", c=4, alpha=0.5;
   kwargs...)
p3 = plot(r.events.operational_demand[idxs_2035] .* 100, r.events.normalised_magnitude[idxs_2035] .* 1e2,
   seriestype=:scatter, label="", c=9, alpha=0.5;
   kwargs...)
p4 = plot(r.events.operational_demand[idxs_2040] .* 100, r.events.normalised_magnitude[idxs_2040] .* 1e2,
   seriestype=:scatter, label="", c=10, alpha=0.5; kwargs...)

plot(p1, p2, p3, p4, layout=(1,4), link=:all, 
   xlabel="Demand - Distributed PV [% of max]", 
   size=(1400,400), dpi=300,
   bottommargin=10Plots.mm, topmargin=5Plots.mm,
   title=["2025" "2030" "2035" "2040"])
savefig(joinpath(@__DIR__, "figures", "7-event_magnitude_vs_demand.png"))


#%% ==========================================================================================

residual_demand = all_demand .- all_vre_production


hours_before = 4

residual_demand_before = zeros(size(residual_demand))
residual_demand_before .= NaN

average_soc_before = zeros(size(all_socs))
average_soc_derated_before = zeros(size(all_socs_derated))
average_soc_greedy_before = zeros(size(all_socs_greedy))
average_soc_before .= NaN
average_soc_derated_before .= NaN
average_soc_greedy_before .= NaN

for i in hours_before:size(residual_demand, 3)
   residual_demand_before[:, :, i-hours_before+1:i] .= mean(residual_demand[:, :, i-hours_before+1:i], dims=3)
   average_soc_before[:, :, i-hours_before+1:i] .= mean(all_socs[:, :, i-hours_before+1:i], dims=3)
   average_soc_derated_before[:, :, i-hours_before+1:i] .= mean(all_socs_derated[:, :, i-hours_before+1:i], dims=3)
   average_soc_greedy_before[:, :, i-hours_before+1:i] .= mean(all_socs_greedy[:, :, i-hours_before+1:i], dims=3)
end


#%%

# residual_demand_before = residual_demand
# average_soc_before = all_socs
# average_soc_derated_before = all_socs_derated
# average_soc_greedy_before = all_socs_greedy

residual_demand_bins = (00_000:5_000:40_000)
soc_bins = collect(0:0.2:1)
target_years = [2025, 2030, 2035, 2040]

risk_matrix = zeros(length(target_years), length(residual_demand_bins)-1, length(soc_bins)-1)
risk_matrix_derated = zeros(length(target_years), length(residual_demand_bins)-1, length(soc_bins)-1)
risk_matrix_greedy = zeros(length(target_years), length(residual_demand_bins)-1, length(soc_bins)-1)

for (i, target_year) in enumerate(target_years)
   for (j, bin_start) in enumerate(residual_demand_bins[1:end-1])
      for (k, soc_bin_start) in enumerate(soc_bins[1:end-1])
         idxs_in_bin = findall((average_soc_before[i,:,:] .>= soc_bin_start) .&& (average_soc_before[i,:,:] .< soc_bins[k+1]) .&& (residual_demand_before[i,:,:] .>= bin_start) .&& (residual_demand_before[i,:,:] .< residual_demand_bins[j+1]))
         risk_matrix[i, j, k] = mean(all_mean_shortfalls[i, idxs_in_bin])

         idxs_in_bin = findall((average_soc_derated_before[i,:,:] .>= soc_bin_start) .&& (average_soc_derated_before[i,:,:] .< soc_bins[k+1]) .&& (residual_demand_before[i,:,:] .>= bin_start) .&& (residual_demand_before[i,:,:] .< residual_demand_bins[j+1]))
         risk_matrix_derated[i, j, k] = mean(all_mean_shortfalls_derated[i, idxs_in_bin])

         idxs_in_bin = findall((average_soc_greedy_before[i,:,:] .>= soc_bin_start) .&& (average_soc_greedy_before[i,:,:] .< soc_bins[k+1]) .&& (residual_demand_before[i,:,:] .>= bin_start) .&& (residual_demand_before[i,:,:] .< residual_demand_bins[j+1]))
         risk_matrix_greedy[i, j, k] = mean(all_mean_shortfalls_greedy[i, idxs_in_bin])
      end
   end
end

#%%
residual_demand = all_demand .- all_vre_production

hours_before = 8

sizes = (length(residual_demand[:,1,1]), length(residual_demand[1,:,1]),length(residual_demand[1,1,:]))

residual_demand_before = zeros(sizes)
residual_demand_before .= NaN

for i in hours_before:length(residual_demand[1,1,:])
   residual_demand_before[:, :, i-hours_before+1:i] .= mean(residual_demand[:, :, i-hours_before+1:i], dims=3)
end

r_greedy.events.normalised_magnitude = r_greedy.events.magnitude_mwh ./ r_greedy.events.total_load_mwh
r_derated.events.normalised_magnitude = r_derated.events.magnitude_mwh ./ r_derated.events.total_load_mwh

r.events.residual_demand_before = [residual_demand[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r.events)]
r_greedy.events.residual_demand_before = [residual_demand[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r_greedy.events)]
r_derated.events.residual_demand_before = [residual_demand[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r_derated.events)]

idxs_2040 = findall(r.events.year .== 2040)
idxs_2040_greedy = findall(r_greedy.events.year .== 2040)
idxs_2040_derated = findall(r_derated.events.year .== 2040)

xmax = max(maximum((r.events.residual_demand_before[idxs_2040]) / 1e3), maximum((r_greedy.events.residual_demand_before[idxs_2040_greedy]) / 1e3), maximum((r_derated.events.residual_demand_before[idxs_2040_derated]) / 1e3))

xmax = 41

p1 = scatter(r_greedy.events.residual_demand_before[idxs_2040_greedy] ./ 1e3, r_greedy.events.normalised_magnitude[idxs_2040_greedy] .* 1e2, 
   seriestype=:scatter, label="", c=1, alpha=0.5,  
   xlabel="", ylabel="Event USE [%]",
   title="A1: High energy",
   size=(600,400), dpi=300, xlims=(0, xmax))

p2 = scatter(r_derated.events.residual_demand_before[idxs_2040_derated] ./ 1e3, r_derated.events.normalised_magnitude[idxs_2040_derated] .* 1e2, 
   seriestype=:scatter, label="", c=2, alpha=0.5,
   title="A2: Derated energy",  
    ylabel="Event USE [%]", 
   size=(600,400), dpi=300, xlims=(0, xmax))

p3 = scatter(r.events.residual_demand_before[idxs_2040] ./ 1e3, r.events.normalised_magnitude[idxs_2040] .* 1e2,
   seriestype=:scatter, label="", c=3, alpha=0.5,
   title="A3: Economic operation",  
   xlabel="Average residual demand in preceding 8 hours [GW]", ylabel="Event USE [%]", 
   size=(600,400), dpi=300, xlims=(0, xmax))

average_soc_before = zeros(sizes)
average_soc_derated_before = zeros(sizes)
average_soc_greedy_before = zeros(sizes)
average_soc_before .= NaN
average_soc_derated_before .= NaN
average_soc_greedy_before .= NaN

for i in hours_before:length(average_soc_before[1,1,:])
   average_soc_before[:, :, i-hours_before+1:i] .= mean(all_socs[:, :, i-hours_before+1:i], dims=3)
   average_soc_derated_before[:, :, i-hours_before+1:i] .= mean(all_socs_derated[:, :, i-hours_before+1:i], dims=3)
   average_soc_greedy_before[:, :, i-hours_before+1:i] .= mean(all_socs_greedy[:, :, i-hours_before+1:i], dims=3)
end

r.events.average_soc_before = [average_soc_before[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r.events)]
r_greedy.events.average_soc_before = [average_soc_greedy_before[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r_greedy.events)]
r_derated.events.average_soc_before = [average_soc_derated_before[findfirst(target_years .== row.year), findfirst(ref_poe_scen_sets .== [(row.ref_year, row.poe, 2)]), row.start_index] for row in eachrow(r_derated.events)]


p5 = scatter(r_greedy.events.average_soc_before[idxs_2040_greedy] .* 1e2, r_greedy.events.normalised_magnitude[idxs_2040_greedy] .* 1e2, 
   seriestype=:scatter, label="", c=1, alpha=0.5,  
   xlabel="", ylabel="Event USE [%]",
   title="A1: High energy",
   size=(600,400), dpi=300, xlims=(0,100), xticks=0:20:100)

p6 = scatter(r_derated.events.average_soc_before[idxs_2040_derated] .* 1e2, r_derated.events.normalised_magnitude[idxs_2040_derated] .* 1e2,
   seriestype=:scatter, label="", c=2, alpha=0.5,  
   xlabel="", ylabel="Event USE [%]", 
   title="A2: Derated energy",
   size=(600,400), dpi=300, xlims=(0,100), xticks=0:20:100)

p7 = scatter(r.events.average_soc_before[idxs_2040] .* 1e2, r.events.normalised_magnitude[idxs_2040] .* 1e2,
   seriestype=:scatter, label="", c=3, alpha=0.5, 
   title="A3: Economic operation", 
   xlabel="Average SoC of storage in preceding 8 hours [%]", ylabel="Event USE [%]", 
   size=(600,400), dpi=300, xlims=(0,100), xticks=0:20:100)

plot(p5, p6, p7, layout=(3,1), link=:all, size=(600, 800),
   leftmargin=7Plots.mm)


plot(p1, p5, p2, p6, p3, p7, layout=(3,2), link=:all, 
   size=(1200, 1000), leftmargin=7Plots.mm, dpi=500)
savefig(joinpath(@__DIR__, "figures", "7-event_magnitude_vs_demand_and_average_soc_before.png"))
