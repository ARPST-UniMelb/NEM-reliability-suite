

using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));


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

#%% ========================================================================================
# Metrics with 
target_years = [2025, 2030, 2035, 2040]
res = unstack(r.metrics, :metric, :value)
res_derated = unstack(r_derated.metrics, :metric, :value)
res_greedy = unstack(r_greedy.metrics, :metric, :value)

kwargs = (lw=3, size=(500,300), markersize=5, xlims=(0.5, 4.5),
   ylims=(0, 90/1e4), xlabel="Planning years", ylabel="Average annual USE [%]",
   xticks=(1:length(target_years), string.(target_years)), dpi=500)

plot(res_greedy.NEUE ./ 1e4 .- 1, markershape=:diamond, label="A1: High energy", c=1, legend=false; kwargs...)
plot!([0.9,4.1], [20,20] ./ 1e4, c=:black, lw=2, ls=:dash, label="")
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 0.png"))

plot(res_greedy.NEUE ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false; kwargs...)
plot!([0.9,4.1], [20,20] ./ 1e4, c=:black, lw=2, ls=:dash, label="")
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 1.png"))

plot(res_greedy.NEUE ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false; kwargs...)
plot!(res_derated.NEUE ./ 1e4, markershape=:square, label="A2: Derated energy", c=2; kwargs...)
plot!([0.9,4.1], [20,20] ./ 1e4, c=:black, lw=2, ls=:dash, label="")
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 2.png"))

plot(res_greedy.NEUE ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false; kwargs...)
plot!(res_derated.NEUE ./ 1e4, markershape=:square, label="A2: Derated energy", c=2; kwargs...)
plot!(res.NEUE ./ 1e4, markershape=:circle, label="A3: Economic operation", c=3,
   ylabel="Average annual USE [%]",
   legend=false; kwargs...)
plot!([0.9,4.1], [20,20] ./ 1e4, c=:black, lw=2, ls=:dash, label="")
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 3.png"))


#%%

kwargs = (lw=3, size=(500,300), markersize=5, xlims=(0.5, 4.5),
   ylims=(0, 90/1e4), xlabel="Planning years", ylabel="CVaR(95%) of annual USE [%]",
   xticks=(1:length(target_years), string.(target_years)), dpi=500)

plot(res_greedy.NCVAR95 ./ 1e4 .- 1, markershape=:diamond, label="A1: High energy", c=1, legend=false, ls=:dash; kwargs...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 0 - cvar.png"))

plot(res_greedy.NCVAR95 ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false, ls=:dash; kwargs...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 1 - cvar.png"))

plot(res_greedy.NCVAR95 ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false, ls=:dash; kwargs...)
plot!(res_derated.NCVAR95 ./ 1e4, markershape=:square, label="A2: Derated energy", c=2, ls=:dash; kwargs...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 2 - cvar.png"))

plot(res_greedy.NCVAR95 ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false, ls=:dash; kwargs...)
plot!(res_derated.NCVAR95 ./ 1e4, markershape=:square, label="A2: Derated energy", c=2, ls=:dash; kwargs...)
plot!(res.NCVAR95 ./ 1e4, markershape=:circle, label="A3: Economic operation", c=3,
   ylabel="CVaR(95%) of annual USE [%]",
   legend=false, ls=:dash; kwargs...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - 3 - cvar.png"))


#%%
plot(res_greedy.NCVAR95 ./ 1e4, markershape=:diamond, label="A1: High energy", c=1, legend=false, ls=:dash; kwargs...)
plot!(res_derated.NCVAR95 ./ 1e4, markershape=:square, label="A2: Derated energy", c=2, ls=:dash; kwargs...)
plot!(res.NCVAR95 ./ 1e4, markershape=:circle, label="A3: Economic operation", c=3,
   ylabel="CVaR(95%) of annual USE [%]",
   legend=:outerright, ls=:dash; kwargs...)
savefig(joinpath(@__DIR__, "figures", "D - Risk Profile - legend.png"))

