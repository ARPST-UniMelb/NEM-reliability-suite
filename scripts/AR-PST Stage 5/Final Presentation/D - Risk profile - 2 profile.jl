

#%%
using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));


#%%

ref_poe_scen_sets = [(ref, poe, 2) for ref in collect(2011:2023) for poe in [10, 50]]
samples = 100

r_greedy = create_summary("baseVPP";
   samples=samples, 
   storage_case="greedy", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

r_derated = create_summary("baseVPP";
   samples=samples, 
   storage_case="derated", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

r = create_summary("baseVPP";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   )

#%%


xlims_overall = (0, 26)# (0, maximum(r.events.duration_hrs) + 1)
ylims_overall = (-0.5, 35) #maximum(r.events.magnitude_mwh ./ r.events.total_load_mw) .* 1e3 + 0.001)

yticks_overall = 0:5:ylims_overall[2]
xticks_overall = 0:3:xlims_overall[2]

kwargs_scatter = (label="", markersize=3, xlabel= "Duration [hrs]",
    xlims=xlims_overall, ylims=ylims_overall, ylabel="Event USE [GWh]",
   yticks=yticks_overall, xticks=xticks_overall, legend=false,
   size=(350,300), dpi=500
   )

ev_reoptimised = vcat(r.events[r.events.year .== 2025, :], r.events[r.events.year .== 2030, :])

scatter(ev_reoptimised.duration_hrs, 
   ev_reoptimised.magnitude_mwh ./ 1e3, 
   c=3; kwargs_scatter...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - profile - 1.png"))

ev_reoptimised = vcat(r.events[r.events.year .== 2035, :], r.events[r.events.year .== 2040, :])

scatter(ev_reoptimised.duration_hrs, 
   ev_reoptimised.magnitude_mwh ./ 1e3, ylabel="", 
   c=3; kwargs_scatter...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - profile - 2.png"))


#%%


ev_reoptimised = vcat(r.events[r.events.year .== 2035, :], r.events[r.events.year .== 2040, :])
ev_derated = vcat(r_derated.events[r_derated.events.year .== 2035, :], r_derated.events[r_derated.events.year .== 2040, :])
ev_greedy = vcat(r_greedy.events[r_greedy.events.year .== 2035, :], r_greedy.events[r_greedy.events.year .== 2040, :])

scatter(ev_reoptimised.duration_hrs, 
   ev_reoptimised.magnitude_mwh ./ 1e3, ylabel="", alpha=0.2,
   c=3; kwargs_scatter...)
scatter!(ev_derated.duration_hrs, 
   ev_derated.magnitude_mwh ./ 1e3, ylabel="", alpha=1.0,
   c=2; kwargs_scatter...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - profile - 3.png"))

scatter(ev_reoptimised.duration_hrs, 
   ev_reoptimised.magnitude_mwh ./ 1e3, ylabel="", alpha=0.2,
   c=3; kwargs_scatter...)
scatter!(ev_derated.duration_hrs, 
   ev_derated.magnitude_mwh ./ 1e3, ylabel="", alpha=0.2,
   c=2; kwargs_scatter...)
scatter!(ev_greedy.duration_hrs, 
   ev_greedy.magnitude_mwh ./ 1e3, ylabel="", alpha=1.0,
   c=1; kwargs_scatter...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - profile - 4.png"))


