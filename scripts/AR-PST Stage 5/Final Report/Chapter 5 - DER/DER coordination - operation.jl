



#using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));

#%%

ref_poe_scen_sets = [(2011, 10, 2), (2019, 50, 2)] # (2011,10,2),
target_years = collect(2025:5:2040)
samples = 500


r_supply_greedy = create_summary("base";
   samples=samples, 
   storage_case="greedy",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_supply_derated = create_summary("base";
   samples=samples, 
   storage_case="derated",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_supply_reoptimised = create_summary("base";
   samples=samples, 
   storage_case="reoptimised",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )


rVPP_greedy = create_summary("baseVPP";
   samples=samples, 
   storage_case="greedy",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rVPP_derated = create_summary("baseVPP";
   samples=samples, 
   storage_case="derated",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rVPP_reoptimised = create_summary("baseVPP";
   samples=samples, 
   storage_case="reoptimised",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )


rDR_greedy = create_summary("baseDR";
   samples=samples, 
   storage_case="greedy",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rDR_derated = create_summary("baseDR";
   samples=samples, 
   storage_case="derated",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rDR_reoptimised = create_summary("baseDR";
   samples=samples, 
   storage_case="reoptimised",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rDRnoVPP_greedy = create_summary("baseDRnoVPP";
   samples=samples, 
   storage_case="greedy",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rDRnoVPP_derated = create_summary("baseDRnoVPP";
   samples=samples,
   storage_case="derated",
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rDRnoVPP_reoptimised = create_summary("baseDRnoVPP";
   samples=samples,
   storage_case="reoptimised",
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rEV_greedy = create_summary("baseEV";
   samples=samples, 
   storage_case="greedy",  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rEV_derated = create_summary("baseEV";
   samples=samples,
   storage_case="derated",
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

rEV_reoptimised = create_summary("baseEV";
   samples=samples,
   storage_case="reoptimised",
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )


#%%


res = hcat(unstack(r_supply_greedy.metrics, :metric, :value).NEUE, 
    unstack(r_supply_derated.metrics, :metric, :value).NEUE, 
    unstack(r_supply_reoptimised.metrics, :metric, :value).NEUE)

resVPP = hcat(unstack(rVPP_greedy.metrics, :metric, :value).NEUE, 
    unstack(rVPP_derated.metrics, :metric, :value).NEUE, 
    unstack(rVPP_reoptimised.metrics, :metric, :value).NEUE)

resDR = hcat(unstack(rDR_greedy.metrics, :metric, :value).NEUE,
      unstack(rDR_derated.metrics, :metric, :value).NEUE, 
      unstack(rDR_reoptimised.metrics, :metric, :value).NEUE)

resDRnoVPP = hcat(unstack(rDRnoVPP_greedy.metrics, :metric, :value).NEUE,
      unstack(rDRnoVPP_derated.metrics, :metric, :value).NEUE, 
      unstack(rDRnoVPP_reoptimised.metrics, :metric, :value).NEUE)


groupedbar(-(1.0 .- hcat(resVPP, resDRnoVPP) ./ hcat(res, res)) * 100, 
   labels = ["VPP | A1: High energy" "VPP | A2: Derated energy" "VPP | A3: Economic operation" "DSP | A1: High energy" "DSP | A2: Derated energy" "DSP | A3: Economic operation"],
     fillalpha=0.7,
     bar_width = 0.8,
     bar_spacing = 0.2,
     #fillstyle=[:/ :+ :x :+ :+ :+],
     color=[5 5 5 :black :black :black],
     lw=[3 3 3 3 3 3],
     lc = ["#009BFA" "#E36F47" "#3EA44D" "#009BFA" "#E36F47" "#3EA44D"],
     legend=:outertop,
     palette=:batlow10,
     #legendtitle="",
     ylabel="Change in average annual USE [%]",
     xlabel="",
     leftmargin=7Plots.mm,
     ylims=(-110, 0), size=(600, 500), dpi=500)
xticks!(3:4, string.(target_years[3:4]))
xlims!(2.5, 4.5)
ylims!(-105, 0)
savefig(joinpath(@__DIR__, "figures", "4-VPP_and_DSP_benefits_storage_operation.png"))

#%%



res = hcat(unstack(r_supply_greedy.metrics, :metric, :value).NEUE, 
    unstack(r_supply_derated.metrics, :metric, :value).NEUE, 
    unstack(r_supply_reoptimised.metrics, :metric, :value).NEUE)

resDRnoVPP = hcat(unstack(rDRnoVPP_supply_greedy.metrics, :metric, :value).NEUE,
      unstack(rDRnoVPP_supply_derated.metrics, :metric, :value).NEUE, 
      unstack(rDRnoVPP_supply_reoptimised.metrics, :metric, :value).NEUE)


p1 = groupedbar(hcat(res, resDRnoVPP) ./ 1e4,
   labels = ["Supply case | A1: High energy" "Supply case | A2: Derated energy" "Supply case | A3: Economic operation" "DSP | A1: High energy" "DSP | A2: Derated energy" "DSP | A3: Economic operation"],
   fillalpha=0.8,
   bar_width = 0.8,
   bar_spacing = 0.2,
   color=[:grey :grey :grey  :black :black :black],
   lw=[3 3 3  3 3 3],
   lc = [ "#009BFA" "#E36F47" "#3EA44D" "#009BFA" "#E36F47" "#3EA44D"],
   legend=:outertop,
   palette=:batlow10,
   ylabel="Average annual USE [%]",
   xlabel="Planning year",
   xlims=(2.5, 4.5),
   xticks=(3:4, string.(target_years[3:4])),
   ylims=(0, 0.03),
   size=(700,500),
   leftmargin=7Plots.mm,
   dpi=500,
) 
p1 = plot!([2.55, 4.45], [0.002, 0.002], label="Reliability standard", lc=:black, ls=:dot, lw=2)


res = hcat(unstack(r_supply_greedy.metrics, :metric, :value).NCVAR95, 
    unstack(r_supply_derated.metrics, :metric, :value).NCVAR95, 
    unstack(r_supply_reoptimised.metrics, :metric, :value).NCVAR95)

resDRnoVPP = hcat(unstack(rDRnoVPP_supply_greedy.metrics, :metric, :value).NCVAR95,
      unstack(rDRnoVPP_supply_derated.metrics, :metric, :value).NCVAR95, 
      unstack(rDRnoVPP_supply_reoptimised.metrics, :metric, :value).NCVAR95)



p2 = groupedbar(hcat(res, resDRnoVPP) ./ 1e4,
   labels = ["Supply case | A1: High energy" "Supply case | A2: Derated energy" "Supply case | A3: Economic operation" "DSP | A1: High energy" "DSP | A2: Derated energy" "DSP | A3: Economic operation"],
   fillalpha=0.6,
   bar_width = 0.8,
   bar_spacing = 0.2,
   color=[:grey :grey :grey  :black :black :black],
   lw=[3 3 3  3 3 3],
   lc = [ "#009BFA" "#E36F47" "#3EA44D" "#009BFA" "#E36F47" "#3EA44D"],
   legend=:outertop,
   palette=:batlow10,
   ylabel="CVaR(95%) of annual USE [%]",
   xlabel="Planning year",
   xlims=(2.5, 4.5),
   xticks=(3:4, string.(target_years[3:4])),
   ylims=(0, 0.03),
   size=(700,500),
   leftmargin=7Plots.mm,
   dpi=500,
) 

p2 = groupedbar!(hcat(res, resDRnoVPP) ./ 1e4,
   fillalpha=1.0,
   alpha=0.7,
   fillstyle=:x,
   bar_width = 0.8,
   bar_spacing = 0.2,
   color=[:grey :grey :grey  :black :black :black],
   lw=0,
   lc = ["#009BFA" "#E36F47" "#3EA44D" "#009BFA" "#E36F47" "#3EA44D"],
   label="")

p2 = groupedbar!(hcat(res, resDRnoVPP) ./ 1e4,
   fillalpha=0.0,
   alpha=1.0,
   bar_width = 0.8,
   bar_spacing = 0.2,
   color=[:grey :grey :grey  :black :black :black],
   lw=3,
   lc = ["#009BFA" "#E36F47" "#3EA44D" "#009BFA" "#E36F47" "#3EA44D" "#009BFA" "#E36F47" "#3EA44D"],
   label="")


plot(p1, p2, layout=(1,2), size=(900, 500), bottommargin=5Plots.mm, leftmargin=7Plots.mm, rightmargin=5Plots.mm, topmargin=5Plots.mm)

savefig(joinpath(@__DIR__, "figures", "4-DSP_benefits_hedge_against_storage.png"))

