

#%%

using JuMP
using Plots
using Dates
using Gurobi
using SchedNEM
using PRAS
using PRASNEM
using Statistics
using StatsPlots

include("../../../../functions/read_results.jl")


#%%
ty = [2040]

r_7days_greedy = create_summary("baseVPP"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)

r_7days_derated = create_summary("baseVPP"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)

r_7days_reoptimised = create_summary("baseVPP"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)


r_7days_greedy_DR = create_summary("baseDR"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)

r_7days_derated_DR = create_summary("baseDR"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)

r_7days_reoptimised_DR = create_summary("baseDR"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)

r_7days_reoptimised_EV = create_summary("baseVPPwithEV"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years = ty)



#%%

res_all_7days = hcat(unstack(r_7days_greedy.metrics, :metric, :value).NEUE, unstack(r_7days_derated.metrics, :metric, :value).NEUE, unstack(r_7days_reoptimised.metrics, :metric, :value).NEUE)
res_all_7days_DR = hcat(unstack(r_7days_greedy_DR.metrics, :metric, :value).NEUE, unstack(r_7days_derated_DR.metrics, :metric, :value).NEUE, unstack(r_7days_reoptimised_DR.metrics, :metric, :value).NEUE)

res_all_7days_CVAR = hcat(unstack(r_7days_greedy.metrics, :metric, :value).NCVAR95, unstack(r_7days_derated.metrics, :metric, :value).NCVAR95, unstack(r_7days_reoptimised.metrics, :metric, :value).NCVAR95)
res_all_7days_DR_CVAR = hcat(unstack(r_7days_greedy_DR.metrics, :metric, :value).NCVAR95, unstack(r_7days_derated_DR.metrics, :metric, :value).NCVAR95, unstack(r_7days_reoptimised_DR.metrics, :metric, :value).NCVAR95)




#%%

p1 = groupedbar(vcat(res_all_7days, res_all_7days_DR) ./ 1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    title="", legend=:topleft,
     ylims=(0, 0.3),
     xticks=(1:2, ["Base case (incl. 100% VPP)", "+ 100% DSP"]), dpi=500)

p2 = groupedbar(vcat(res_all_7days_CVAR, res_all_7days_DR_CVAR) ./ 1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    title="", legend=:topleft,
     ylims=(0, 0.3), fillalpha=0.5,
     xticks=(1:2, ["Base case (incl. 100% VPP)", "+ 100% DSP"]), dpi=500)

p2 = groupedbar!(vcat(res_all_7days_CVAR, res_all_7days_DR_CVAR) ./1e4, 
    label="", 
    xlabel="", ylabel="CVaR(95%) of USE [%]", 
    title="", legend=false, fillstyle=:x, dpi=500)

plot(p1, p2, size=(1000, 400), dpi=500, layout=(1,2), 
   link=:y, leftmargin=5Plots.mm)

savefig(joinpath(@__DIR__, "figures", "5-VREdrought_with_DR.png"))
