




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



xlab = "Average annual USE [%]"
labs = ["A1: High energy"  "A2: Derated energy" "A3: Economic operation"]
lab_rel_stand = "Reliability standard"
title = "NEM-wide reliability risk"
kwargs = (legend=:topleft, c=[1 2 3], dpi=300, fillalpha=1.0, lc=:black,
   fillcolor=[1 2 3], lw=1, size=(600, 400), bottommargin=5Plots.mm, leftmargin=5Plots.mm)

res_all = hcat(res_greedy.NEUE, res_derated.NEUE, res_reoptimised.NEUE)

groupedbar(res_all ./ 1e4, label=labs, 
   legend=:outerright, c=[1 2 3]; kwargs...)
plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
xlabel!("Planning Year")
ylabel!(xlab)
title!(title)
xticks!(1:4,string.(res_greedy.year))
ylims!(0,35 ./ 1e4)
xlims!(0.5, 4.5)
savefig(joinpath(@__DIR__, "figures", "6 - storage - NEUE.png"))

#%%

xlab = "Normalised CVaR(95%) of annual USE [%]"
labs = ["A1: High energy"  "A2: Derated energy" "A3: Economic operation"]
lab_rel_stand = "Reliability standard"
title = "NEM-wide tail risk"
kwargs = (legend=:topleft, c=[1 2 3], dpi=300, fillalpha=1.0, lc=:black,
   fillcolor=[1 2 3], lw=1, size=(600, 400), bottommargin=5Plots.mm, leftmargin=5Plots.mm)

res_all = hcat(res_greedy.NCVAR95, res_derated.NCVAR95, res_reoptimised.NCVAR95)

groupedbar(res_all ./ 1e4, label=labs, 
   legend=:outerright, c=[1 2 3], fillstyle=:x; kwargs...)
#plot!([0.75,4.25], [20,20] ./ 1e4, label=lab_rel_stand, lc=:black, ls=:dash, lw=2)
xlabel!("Planning Year")
ylabel!(xlab)
title!(title)
xticks!(1:4,string.(res_greedy.year))
ylims!(0,140 ./ 1e4)
xlims!(0.5, 4.5)
savefig(joinpath(@__DIR__, "figures", "6 - storage - CVAR95.png"))

#%% ===================================================================================================================
# Plot the risk profile per 

yt = 0:50:250
yt_string = string.(yt ./ 1e2)

kwargs = (dpi=300,
    #lw=1, #size=(800, 230), 
   #yscale=:log10, 
   bottommargin=5Plots.mm, leftmargin=5Plots.mm, xlims=(0,220/1e4), ylims=(0,250),
   #ylabel="Sample Count", 
   yticks=(yt, yt_string))



bins = collect(0.0001:5:220) ./ 1e4 

p1 = stephist(r_greedy.normalised_ens[1,:,:][:] ./ 1e4, label="A1: High energy", 
   fillcolor=1, bins=bins, fillalpha=1.0, linecolor=1, lw=2; kwargs...)
p1 = stephist!(r_derated.normalised_ens[1,:,:][:] ./ 1e4, label="A2: Derated energy", 
   fillcolor=2, bins=bins, fillalpha=1.0, linecolor=2, lw=2; kwargs...)
p1 = stephist!(r_reoptimised.normalised_ens[1,:,:][:] ./ 1e4, label="A3: Economic operation", 
   fillcolor=3, bins=bins, fillalpha=0.7, linecolor=3, lw=2,
   title="2025", legend=false, ylabel="Share of samples [%]"; kwargs...)


p2 = stephist(r_greedy.normalised_ens[2,:,:][:] ./ 1e4, label="A1: High energy", 
   fillcolor=1, bins=bins, fillalpha=1.0, linecolor=1, lw=2; kwargs...)
p2 = stephist!(r_derated.normalised_ens[2,:,:][:] ./ 1e4, label="A2: Derated energy", 
   fillcolor=2, bins=bins, fillalpha=1.0, linecolor=2, lw=2; kwargs...)
p2 = stephist!(r_reoptimised.normalised_ens[2,:,:][:] ./ 1e4, label="A3: Economic operation", 
   fillcolor=3, bins=bins, fillalpha=0.7, linecolor=3, lw=2,
   title="2030", xlabel="",  ylabel=""; kwargs...)


p3 = stephist(r_greedy.normalised_ens[3,:,:][:] ./ 1e4, label="A1: High energy", 
   fillcolor=1, bins=bins, fillalpha=1.0, linecolor=1, lw=2; kwargs...)
p3 = stephist!(r_derated.normalised_ens[3,:,:][:] ./ 1e4, label="A2: Derated energy", 
   fillcolor=2, bins=bins, fillalpha=1.0, linecolor=2, lw=2; kwargs...)
p3 = stephist!(r_reoptimised.normalised_ens[3,:,:][:] ./ 1e4, label="A3: Economic operation", 
   fillcolor=3, bins=bins, fillalpha=0.7, linecolor=3, lw=2,
   title="2035", legend=false, xlabel="Normalised annual USE [%]",
   ylabel="Share of samples [%]"; kwargs...)


p4 = stephist(r_greedy.normalised_ens[4,:,:][:] ./ 1e4, label="A1: High energy", 
   fillcolor=1, bins=bins, fillalpha=1.0, linecolor=1, lw=2; kwargs...)
p4 = stephist!(r_derated.normalised_ens[4,:,:][:] ./ 1e4, label="A2: Derated energy", 
   fillcolor=2, bins=bins, fillalpha=1.0, linecolor=2, lw=2; kwargs...)
p4 = stephist!(r_reoptimised.normalised_ens[4,:,:][:] ./ 1e4, label="A3: Economic operation", 
   fillcolor=3, bins=bins, fillalpha=0.7, linecolor=3, lw=2,
   title="2040", legend=false, xlabel="Normalised annual USE [%]", ylabel=""; kwargs...)


plot(p1, p2, p3, p4, layout=(2,2), size=(900, 700), 
   bottommargin=5Plots.mm, leftmargin=5Plots.mm, link=:all)
savefig(joinpath(@__DIR__, "figures", "6 - storage - Risk_distribution_all_years.png"))

