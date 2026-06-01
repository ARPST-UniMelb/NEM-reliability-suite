

#using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../../functions/all_functions.jl"));


#%%

ref_poe_scen_sets = [(2011, 10, 2), (2019, 50, 2)] # (2011,10,2),
target_years = collect(2025:5:2040)
samples = 500
storage_case = "reoptimised"

r_supply = create_summary("base";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_LowEV = create_summary("baseLowEV";
   samples=samples, 
   storage_case=storage_case, 
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_EV = create_summary("baseEV";
   samples=samples, 
   storage_case=storage_case, 
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_VPPlow = create_summary("baseLowVPP";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_VPP = create_summary("baseVPP";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_DR = create_summary("baseDR";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_LowDRLowVPP = create_summary("baseLowDRLowVPP"; # Note the 
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )


r_lowDRnoVPP = create_summary("baseLowDRnoVPP";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_DRnoVPP = create_summary("baseDRnoVPP";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )


#%%

res_supply = unstack(r_supply.metrics, :metric, :value)
res_lowEV = unstack(r_LowEV.metrics, :metric, :value)
res_EV = unstack(r_EV.metrics, :metric, :value)
res_lowVPP = unstack(r_VPPlow.metrics, :metric, :value)
res_VPP = unstack(r_VPP.metrics, :metric, :value)
res_LowDRnoVPP = unstack(r_lowDRnoVPP.metrics, :metric, :value)
res_DRnoVPP = unstack(r_DRnoVPP.metrics, :metric, :value)
res_LowDRLowVPP = unstack(r_LowDRLowVPP.metrics, :metric, :value)
res_DR = unstack(r_DR.metrics, :metric, :value)
res_all_NEUE = hcat(res_supply.NEUE, res_lowEV.NEUE, res_EV.NEUE, res_lowVPP.NEUE, res_VPP.NEUE, res_LowDRnoVPP.NEUE, res_DRnoVPP.NEUE, res_LowDRLowVPP.NEUE, res_DR.NEUE)
res_all_CVAR = hcat(res_supply.NCVAR95, res_lowEV.NCVAR95, res_EV.NCVAR95, res_lowVPP.NCVAR95, res_VPP.NCVAR95, res_LowDRnoVPP.NCVAR95, res_DRnoVPP.NCVAR95, res_LowDRLowVPP.NCVAR95, res_DR.NCVAR95)


kwargs = (c=[:grey 1 1 5 5 :black :black :orange :green :orange],
   alpha=[1.0 0.7 1.0 0.7 1.0 0.7 1.0 1.0 1.0],
   xlabel="Planning year", xticks=(1:4, string.(target_years)),
   legend=:topleft) 

p1 = groupedbar(res_all_NEUE ./ 1e4, alpha=0.7, 
   palette=:batlow10,
   ylabel="Average annual USE [%]", 
   leftmargin=7Plots.mm,
   label=["Supply case" "25% EV" "50% EV" "50% VPP" "100% VPP" "50% DSP" "100% DSP" "50% VPP + 50% DSP" "100% VPP + 100% DSP"]; kwargs...)
p1 = plot!([0.75, 4.25], [20, 20] ./ 1e4, 
   c=:black, ls=:dash, label="Reliability standard")
p1 = ylims!(0, 290/1e4)


p2 = groupedbar(res_all_CVAR ./ 1e4, 
   label="",fillalpha=0.2, 
   palette=:batlow10, 
   legend=:topleft,
   ylabel="CVaR(95%) of annual USE [%]",  
   leftmargin=7Plots.mm; kwargs...)
p2 = groupedbar!(res_all_CVAR ./ 1e4, 
   fillalpha=1.0, 
   fillstyle=:x,
   palette=:batlow10, 
   legend=:topleft,
   ylabel="CVaR(95%) of annual USE [%]",
   label=["Supply case" "25% EV" "50% EV" "50% VPP" "100% VPP" "50% DSP" "100% DSP" "50% VPP + 50% DSP" "100% VPP + 100% DSP"],  
   leftmargin=7Plots.mm; kwargs...)
p2 = ylims!(0, 290/1e4)

plot(p1, p2, layout=(1,2), size=(970,300), dpi=500, bottommargin=5Plots.mm)
savefig(joinpath(@__DIR__, "figures", "4-reliability_metrics_with_DER-0.png"))

#%%


kwargs = (c=[ 1 1 5 5 :black :black :orange :green :orange],
   lc = :black, #[ 1 1 5 5 :black :black :orange :green :orange],
   xlabel="Planning year", xticks=(1:2, string.(target_years[3:4])),
   ylims=(-105, 0),
   label=["25% EV" "50% EV" "50% VPP" "100% VPP" "50% DSP" "100% DSP" "50% VPP + 50% DSP" "100% VPP + 100% DSP"],) 


p1 = groupedbar( (res_all_NEUE[3:4,2:end] .- res_all_NEUE[3:4,1]) ./ res_all_NEUE[3:4,1] .* 1e2,
   palette=:batlow10,
   ylabel="Change in average annual USE\nwith respect to supply case [%]", 
   leftmargin=7Plots.mm,
   alpha=[0.7 1.0 0.7 1.0 0.7 1.0 0.7 1.0],
   legend=:outertop; kwargs...)

p2 = groupedbar((res_all_CVAR[3:4,2:end] .- res_all_CVAR[3:4,1]) ./ res_all_CVAR[3:4,1] .* 1e2, 
   label="",
   palette=:batlow10, 
   legend=:outertop,
   alpha=[0.7 1.0 0.7 1.0 0.7 1.0 1.0 1.0] .* 0.2,
   ylabel="Change in CVaR(95%) of annual USE\nwith respect to supply case [%]",  
   leftmargin=7Plots.mm; kwargs...)
p2 = groupedbar!((res_all_CVAR[3:4,2:end] .- res_all_CVAR[3:4,1]) ./ res_all_CVAR[3:4,1] .* 1e2, 
   label="",fillalpha=1.0, 
   fillstyle=:x,
   c=[ 1 1 5 5 :black :black :orange :green :orange],
   lc = :black,
   palette=:batlow10, 
   legend=:outertop,
   ylabel="Change in CVaR(95%) of annual USE\nwith respect to supply case [%]",  
   leftmargin=7Plots.mm)

plot(p1, p2, layout=(1,2), size=(1000,500), dpi=500, bottommargin=5Plots.mm)
savefig(joinpath(@__DIR__, "figures", "4-relative_impact_of_DER.png"))

