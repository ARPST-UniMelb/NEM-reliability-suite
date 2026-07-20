

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

r_greedy_DSP = create_summary("baseHalfDR"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=ref_poe_scen_sets)

res_greedy_DSP = unstack(r_greedy_DSP.metrics, :metric, :value)


r_derated_DSP = create_summary("baseHalfDR"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=ref_poe_scen_sets)

res_derated_DSP = unstack(r_derated_DSP.metrics, :metric, :value)

r_reoptimised_DSP = create_summary("baseHalfDR"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets)

res_reoptimised_DSP = unstack(r_reoptimised_DSP.metrics, :metric, :value)


#%%

res = hcat(unstack(r_greedy.metrics, :metric, :value).NEUE, 
    unstack(r_derated.metrics, :metric, :value).NEUE, 
    unstack(r_reoptimised.metrics, :metric, :value).NEUE)

resHalfDR = hcat(unstack(r_greedy_DSP.metrics, :metric, :value).NEUE,
      unstack(r_derated_DSP.metrics, :metric, :value).NEUE, 
      unstack(r_reoptimised_DSP.metrics, :metric, :value).NEUE)

res_all = hcat(res, resHalfDR) ./ 1e4

lab_rel_stand = "Reliability standard"

ytck_labels = repeat([""], 8)
ytck_labels[1:2:end] .= string.(0.000:0.0005:0.0035)[1:2:end]
ytck_labels[1] = "0.000"

kwargs = (
     bar_width = 0.8,
     bar_spacing = 0.2,
     color=["#009BFA" "#E36F47" "#3EA44D" :black :black :black],
     #lw=[1 1 1 3 3 3],
     lc = [:black :black :black "#009BFA" "#E36F47" "#3EA44D"],
     palette=:batlow10, ylabel="Average annual USE [%]",
     leftmargin=7Plots.mm, size=(600, 300), dpi=500,
     ylims=(0,35/1e4),
     xticks=(1:4,string.(res_greedy.year)),
     xlabel="Calendar Year",
     yticks=(0:0.0005:0.0035, ytck_labels),
     title="NEM-wide reliability risk")

val_all = zeros(size(res_all,1), size(res_all,2)) .- 1

val_all[:,1:3] = res_all[:,1:3]
groupedbar(val_all, lw=[1 1 1 0 0 0], legend=false; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "C - DER - 0.png"))

val_all[:,4] = res_all[:,4]
groupedbar(val_all, lw=[1 1 1 3 0 0], legend=false; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "C - DER - 1.png"))

val_all[:,5] = res_all[:,5]
groupedbar(val_all, lw=[1 1 1 3 3 0], legend=false; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "C - DER - 2.png"))

val_all[:,6] = res_all[:,6]
groupedbar(val_all, lw=[1 1 1 3 3 3], legend=false; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
savefig(joinpath(@__DIR__, "figures", "C - DER - 3.png"))

#%%

labels = ["" "" "" "                    " "                    " "                    "]
   

groupedbar(val_all, lw=[1 1 1 3 3 3], legend=:outerright, dpi=500,
   label=labels; kwargs...)
savefig(joinpath(@__DIR__, "figures", "C - DER - 4.png"))


