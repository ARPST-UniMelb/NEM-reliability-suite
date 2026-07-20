

using Plots; gr()
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));

#%%

ref_poe_scen_sets = [(2019, 50, 2), (2011,10,2)] 

r_greedy = create_summary("baseVPP"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=ref_poe_scen_sets)

res_greedy = unstack(r_greedy.metrics, :metric, :value)


r_derated = create_summary("baseVPP"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=ref_poe_scen_sets)

res_derated = unstack(r_derated.metrics, :metric, :value)

r_reoptimised = create_summary("baseVPP"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets)

res_reoptimised = unstack(r_reoptimised.metrics, :metric, :value)

#%%


ylab = "Average annual USE [%]"
labs = ["A1: High energy"  "A2: Derated energy" "A3: Economic operation"]
lab_rel_stand = "Reliability standard"
title = "NEM-wide reliability risk"

ytck_labels = repeat([""], 8)
ytck_labels[1:2:end] .= string.(0.000:0.0005:0.0035)[1:2:end]
ytck_labels[1] = "0.000"

kwargs = (legend=false, c=[1 2 3], dpi=300, fillalpha=1.0, lc=:black,
   fillcolor=[1 2 3], lw=1, size=(600, 300), bottommargin=5Plots.mm, leftmargin=5Plots.mm, title=title,
   ylabel=ylab, xticks = (1:4,string.(res_greedy.year)), 
   yticks=(0:0.0005:0.0035, ytck_labels),
   ylims=(0,35 ./ 1e4), xlims=(0.5, 4.5),
   xlabel="Calendar Year")

res_all = hcat(res_greedy.NEUE, res_derated.NEUE, res_reoptimised.NEUE)

val_all = zeros(size(res_all,1), size(res_all,2))

groupedbar(val_all ./ 1e4; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "B - storage - 0.png"))

val_all[:,1] = res_all[:,1]
groupedbar(val_all ./ 1e4; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "B - storage - 1.png"))

val_all[:,2] = res_all[:,2]
groupedbar(val_all ./ 1e4; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "B - storage - 2.png"))

val_all[1:2,3] = res_all[1:2,3]
groupedbar(val_all ./ 1e4; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "B - storage - 3.png"))

val_all[3,3] = res_all[3,3]
groupedbar(val_all ./ 1e4; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "B - storage - 4.png"))

val_all[4,3] = res_all[4,3]
groupedbar(val_all ./ 1e4; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "B - storage - 5.png"))
