

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

include("../../../../../functions/read_results.jl")

#%%

r_greedy = create_summary("baseVPP"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day1"],)

r_derated = create_summary("baseVPP";
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day1"],)

r_reoptimised = create_summary("baseVPP";
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day1"],)

r_3days_greedy = create_summary("baseVPP"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day3"])

r_3days_derated = create_summary("baseVPP"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day3"])

r_3days_reoptimised = create_summary("baseVPP"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day3"])

r_7days_greedy = create_summary("baseVPP"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"])

r_7days_derated = create_summary("baseVPP"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"])

r_7days_reoptimised = create_summary("baseVPP"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"])

r_7days_reoptimised_large_bess = create_summary("larger-bess_baseVPP"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years =[2040])

r_7days_greedy_large_bess = create_summary("larger-bess_baseVPP"; 
   samples=500, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years =[2040])

r_7days_derated_large_bess = create_summary("larger-bess_baseVPP"; 
   samples=500, 
   storage_case="derated", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years =[2040])


#%%

res_all = hcat(unstack(r_greedy.metrics, :metric, :value).NEUE, unstack(r_derated.metrics, :metric, :value).NEUE, unstack(r_reoptimised.metrics, :metric, :value).NEUE)
res_all_3days = hcat(unstack(r_3days_greedy.metrics, :metric, :value).NEUE, unstack(r_3days_derated.metrics, :metric, :value).NEUE, unstack(r_3days_reoptimised.metrics, :metric, :value).NEUE)
res_all_7days = hcat(unstack(r_7days_greedy.metrics, :metric, :value).NEUE, unstack(r_7days_derated.metrics, :metric, :value).NEUE, unstack(r_7days_reoptimised.metrics, :metric, :value).NEUE)
res_all_7days_large_bess = hcat(unstack(r_7days_greedy_large_bess.metrics, :metric, :value).NEUE, unstack(r_7days_derated_large_bess.metrics, :metric, :value).NEUE, unstack(r_7days_reoptimised_large_bess.metrics, :metric, :value).NEUE)


res_all_CVAR = hcat(unstack(r_greedy.metrics, :metric, :value).NCVAR95, unstack(r_derated.metrics, :metric, :value).NCVAR95, unstack(r_reoptimised.metrics, :metric, :value).NCVAR95)
res_all_3days_CVAR = hcat(unstack(r_3days_greedy.metrics, :metric, :value).NCVAR95, unstack(r_3days_derated.metrics, :metric, :value).NCVAR95, unstack(r_3days_reoptimised.metrics, :metric, :value).NCVAR95)
res_all_7days_CVAR = hcat(unstack(r_7days_greedy.metrics, :metric, :value).NCVAR95, unstack(r_7days_derated.metrics, :metric, :value).NCVAR95, unstack(r_7days_reoptimised.metrics, :metric, :value).NCVAR95)
res_all_7days_large_bess_CVAR = hcat(unstack(r_7days_greedy_large_bess.metrics, :metric, :value).NCVAR95, unstack(r_7days_derated_large_bess.metrics, :metric, :value).NCVAR95, unstack(r_7days_reoptimised_large_bess.metrics, :metric, :value).NCVAR95)

#%%


p1 = groupedbar(res_all ./ 1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    title="Base case", legend=:topleft,
    xticks=(3:4, [ "",""]), 
    xlims=(2.5, 4.5), ylims=(0, 0.01))

p4 = groupedbar(res_all_CVAR ./1e4, 
    label=[""], 
    xlabel="", ylabel="Average USE [%]", 
    title="Base case", legend=:topleft,
      xticks=(3:4, [ "",""]), fillalpha=0.5,
      ylims=(0, 0.01),     xlims=(2.5, 4.5),)
p4 = groupedbar!(res_all_CVAR ./1e4, 
    label="", 
    xlabel="", ylabel="CVaR(95%) of USE [%]", 
    title="Base case", legend=false, fillstyle=:x,
      xticks=(3:4, ["",""]), dpi=500,
          xlims=(2.5, 4.5),)

p2 = groupedbar(res_all_3days ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    title="3-day VRE drought", legend=false,
    xticks=(3:4, ["",""]), dpi=500,
    xlims=(2.5, 4.5),
    ylims=(0, 0.01))

p5 = groupedbar(res_all_3days_CVAR ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="CVaR(95%) of USE [%]", 
    title="3-day VRE drought", legend=false,
    fillalpha=0.5, xticks=(3:4, ["",""]), 
    ylims=(0, 0.01),
        xlims=(2.5, 4.5))
p5 = groupedbar!(res_all_3days_CVAR ./1e4, 
    xticks=(3:4, ["",""]), dpi=500, fillstyle=:x, label="", legend=false)
p3 = groupedbar(res_all_7days ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"],
      ylabel="Average USE [%]",
      title="7-day VRE drought", 
      xlabel="Planning years",
      xticks=(3:4, string.(collect(2035:5:2040))),
      legend=false, size=(800, 500), dpi=500,
     xlims=(2.5, 4.5), ylims=(0, 0.25))
p6 = groupedbar(res_all_7days_CVAR ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"],
      ylabel="CVaR(95%) of USE [%]",
      title="7-day VRE drought", 
      xlabel="Planning years",
      xticks=(3:4, string.(collect(2035:5:2040))),
      legend=:topleft, size=(800, 500), fillalpha=0.5,
      ylims=(0, 0.25))
p6 = groupedbar!(res_all_7days_CVAR ./1e4, 
    label="",
      ylabel="CVaR(95%) of USE [%]",
      title="7-day VRE drought", 
      xlabel="Planning years",
      xticks=(3:4, string.(collect(2035:5:2040))),
      legend=false, fillstyle=:x, size=(800, 500), dpi=500, 
      xlims=(2.5, 4.5), ylims=(0, 0.25))

#plot(p1, p2, p3, layout=(3,1), size=(700, 700), dpi=500, link=:x,
#   bottommargin=5Plots.mm, topmargin=5Plots.mm)

plot(p1, p4, p2, p5, p3, p6, layout=(3,2), size=(1100, 900), 
   dpi=300, link=:both, leftmargin=5Plots.mm, bottommargin=5Plots.mm)
#xlims!(2.5, 4.5)

savefig(joinpath(@__DIR__, "figures", "5-VREdrought_USE_comparison.png"))





#%%
x = ["A1: High energy","A2: Derated energy","A3: Economic operation"]

p1 = bar(x,res_all[4, :] ./ 1e4, 
    label=["A1: High energy","A2: Derated energy","A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    c=[1,2,3],
    title="Base case", legend=false,
    dpi=500)

p4 = bar(x, res_all_CVAR[4, :] ./1e4, 
    label=[""], 
    xlabel="", ylabel="Average USE [%]", 
    title="Base case", legend=false,  fillalpha=0.5)
p4 = bar!(x, res_all_CVAR[4, :] ./1e4, 
    label="", 
    xlabel="", ylabel="CVaR(95%) of USE [%]", 
    title="Base case", legend=false, fillstyle=:x, dpi=500)

p2 = groupedbar(res_all_3days ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    title="3-day VRE drought", legend=:topleft,
    xticks=(1:4, ["","", "",""]), dpi=500)
p5 = groupedbar(res_all_3days_CVAR ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"], 
    xlabel="", ylabel="Average USE [%]", 
    title="3-day VRE drought", legend=:topleft,
    fillalpha=0.5, xticks=(1:4, ["","", "",""]), dpi=500)
p5 = groupedbar!(res_all_3days_CVAR ./1e4, 
    xticks=(1:4, ["","", "",""]), dpi=500, fillstyle=:x, label="", legend=false)


p3 = groupedbar(res_all_7days ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"],
      ylabel="Average USE [%]",
      title="7-day VRE drought", 
      xlabel="Planning years",
      xticks=(1:4, string.(collect(2025:5:2040))),
      legend=:topleft, size=(800, 500), dpi=500)
p6 = groupedbar(res_all_7days_CVAR ./1e4, 
    label=["A1: High energy" "A2: Derated energy" "A3: Economic operation"],
      ylabel="CVaR(95%) of USE [%]",
      title="7-day VRE drought", 
      xlabel="Planning years",
      xticks=(1:4, string.(collect(2025:5:2040))),
      legend=:topleft, size=(800, 500), dpi=500, fillalpha=0.5)
p6 = groupedbar!(res_all_7days_CVAR ./1e4, 
    label="",
      ylabel="CVaR(95%) of USE [%]",
      title="7-day VRE drought", 
      xlabel="Planning years",
      xticks=(1:4, string.(collect(2025:5:2040))),
      legend=false, fillstyle=:x, size=(800, 500), dpi=500)

plot(p1, p2, p3, layout=(3,1), size=(700, 700), dpi=500, link=:x,
   bottommargin=5Plots.mm, topmargin=5Plots.mm)

plot(p1, p4, p2, p5, p3, p6, layout=(3,2), size=(1300, 900), 
   dpi=500, link=:y)


#%%

ty = 2040

r_7days_reoptimised = create_summary("baseVPP"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"])

r_7days_reoptimised_DR = create_summary("baseDR"; 
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2019-poe50-vre_drought_area12345_day7"],
   target_years =[2040])
