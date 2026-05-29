



#%%

using JuMP
using Plots
using Dates
using Gurobi
using SchedNEM
using PRAS
using PRASNEM

include("../../../../functions/all_functions.jl");


#%%

target_years = [2040]
base_path = "Z://"
samples = 1500

r_res = create_summary("base";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6"],
   target_years=target_years,
   base_path=base_path,
   )

r_res_greedy = create_summary("base";
   samples=samples, 
   storage_case="greedy", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6"],
   target_years=target_years,
   base_path=base_path,
   )

r_res_lines = create_summary("base";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6-lines"],
   target_years=target_years,
   base_path=base_path,
   )

r_res_VRE = create_summary("base";
   samples=samples,
   storage_case="reoptimised",
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6-VRE"],
   target_years=target_years,
   base_path=base_path,
   )

r_res_thermal = create_summary("base";
   samples=samples,
   storage_case="reoptimised",
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6-thermal"],
   target_years=target_years,
   base_path=base_path,
   )

r_without = create_summary("base";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6-comparison"],
   target_years=target_years,
   base_path=base_path,
   )

r_res_VPP = create_summary("baseVPP";
   samples=500,
   storage_case="reoptimised",
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6"],
   target_years=target_years,
   base_path=base_path,
   )

r_withoutVPP = create_summary("baseVPP";
   samples=500, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6-comparison"],
   target_years=target_years,
   base_path=base_path,
   )


#sys = PRAS.SystemModel("Z:/pras-files/base/out-ref2017-poe10-heatwave-ref2017-ty2038-v6-comparison/2040-01-01_to_2040-03-31_s2_all_regions_incl_line_9_10_18_23_25_29_30_34_45_49_50_51.pras")


#%%


r_without.events.normalised_magnitude = r_without.events.magnitude_mwh ./ r_without.events.total_load_mwh
r_res.events.normalised_magnitude = r_res.events.magnitude_mwh ./ r_res.events.total_load_mwh
r_res_lines.events.normalised_magnitude = r_res_lines.events.magnitude_mwh ./ r_res_lines.events.total_load_mwh
r_res_VRE.events.normalised_magnitude = r_res_VRE.events.magnitude_mwh ./ r_res_VRE.events.total_load_mwh
r_res_thermal.events.normalised_magnitude = r_res_thermal.events.magnitude_mwh ./ r_res_thermal.events.total_load_mwh
r_res_VPP.events.normalised_magnitude = r_res_VPP.events.magnitude_mwh ./ r_res_VPP.events.total_load_mwh
r_withoutVPP.events.normalised_magnitude = r_withoutVPP.events.magnitude_mwh ./ r_withoutVPP.events.total_load_mwh

no = combine(groupby(r_without.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
lines = combine(groupby(r_res_lines.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
VRE = combine(groupby(r_res_VRE.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
thermal = combine(groupby(r_res_thermal.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
full = combine(groupby(r_res.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
noVPP = combine(groupby(r_withoutVPP.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
vpp = combine(groupby(r_res_VPP.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)


NEUE = [sum(no.normalised_magnitude_sum), sum(lines.normalised_magnitude_sum), sum(VRE.normalised_magnitude_sum), sum(thermal.normalised_magnitude_sum), sum(full.normalised_magnitude_sum)] ./ 1500
LOLF = [sum(no.count), sum(lines.count), sum(VRE.count), sum(thermal.count), sum(full.count)] ./ 1500
LOLH = [sum(no.duration_hrs_sum), sum(lines.duration_hrs_sum), sum(VRE.duration_hrs_sum), sum(thermal.duration_hrs_sum), sum(full.duration_hrs_sum)] ./ 1500


all_samples = zeros(1500)


NEUE_VAR = [quantile(vcat(no.normalised_magnitude_sum, zeros(1500 - length(no.normalised_magnitude_sum))), 0.95),
   quantile(vcat(lines.normalised_magnitude_sum, zeros(1500 - length(lines.normalised_magnitude_sum))), 0.95), 
   quantile(vcat(VRE.normalised_magnitude_sum, zeros(1500 - length(VRE.normalised_magnitude_sum))), 0.95), 
   quantile(vcat(thermal.normalised_magnitude_sum, zeros(1500 - length(thermal.normalised_magnitude_sum))), 0.95), 
   quantile(vcat(full.normalised_magnitude_sum, zeros(1500 - length(full.normalised_magnitude_sum))), 0.95)]

NEUE_CVAR = [mean(vcat(no.normalised_magnitude_sum[no.normalised_magnitude_sum .>= NEUE_VAR[1]])), 
   mean(vcat(lines.normalised_magnitude_sum[lines.normalised_magnitude_sum .>= NEUE_VAR[2]])), 
   mean(vcat(VRE.normalised_magnitude_sum[VRE.normalised_magnitude_sum .>= NEUE_VAR[3]])), 
   mean(vcat(thermal.normalised_magnitude_sum[thermal.normalised_magnitude_sum .>= NEUE_VAR[4]])), 
   mean(vcat(full.normalised_magnitude_sum[full.normalised_magnitude_sum .>= NEUE_VAR[5]]))]

LOLH_VAR = [quantile(vcat(no.duration_hrs_sum, zeros(1500 - length(no.duration_hrs_sum))), 0.95), 
   quantile(vcat(lines.duration_hrs_sum, zeros(1500 - length(lines.duration_hrs_sum))), 0.95), 
   quantile(vcat(VRE.duration_hrs_sum, zeros(1500 - length(VRE.duration_hrs_sum))), 0.95), 
   quantile(vcat(thermal.duration_hrs_sum, zeros(1500 - length(thermal.duration_hrs_sum))), 0.95), 
   quantile(vcat(full.duration_hrs_sum, zeros(1500 - length(full.duration_hrs_sum))), 0.95)]


LOLH_CVAR = [mean(no.duration_hrs_sum[no.duration_hrs_sum .>= LOLH_VAR[1]]), 
   mean(lines.duration_hrs_sum[lines.duration_hrs_sum .>= LOLH_VAR[2]]), 
   mean(VRE.duration_hrs_sum[VRE.duration_hrs_sum .>= LOLH_VAR[3]]), 
   mean(thermal.duration_hrs_sum[thermal.duration_hrs_sum .>= LOLH_VAR[4]]), 
   mean(full.duration_hrs_sum[full.duration_hrs_sum .>= LOLH_VAR[5]])]

LOLF_VAR = [quantile(vcat(no.count, zeros(1500 - length(no.count))), 0.95), 
   quantile(vcat(lines.count, zeros(1500 - length(lines.count))), 0.95), 
   quantile(vcat(VRE.count, zeros(1500 - length(VRE.count))), 0.95), 
   quantile(vcat(thermal.count, zeros(1500 - length(thermal.count))), 0.95), 
   quantile(vcat(full.count, zeros(1500 - length(full.count))), 0.95)]

LOLF_CVAR = [mean(no.count[no.count .>= LOLF_VAR[1]]), 
   mean(lines.count[lines.count .>= LOLF_VAR[2]]), 
   mean(VRE.count[VRE.count .>= LOLF_VAR[3]]), 
   mean(thermal.count[thermal.count .>= LOLF_VAR[4]]), 
   mean(full.count[full.count .>= LOLF_VAR[5]])]

p1 = bar(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   NEUE .* 1e2, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.8)
p1 = ylabel!("Average USE [%]")
p1 = xlabel!("")
p1 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)

p2 = bar(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   LOLH, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.8)
p2 = ylabel!("Average LOLH [h]")
p2 = xlabel!("")
p2 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)


p3 = bar(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   LOLF .* 1e2 ./ 1500, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.8)
p3 = ylabel!("Average LOLF [%]")
p3 = xlabel!("Component impact considered")
p3 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)



plot(p1, p2, p3, layout=(3,1), size=(700, 800), dpi=500)


savefig(joinpath(@__DIR__, "figures", "5-heatwave_component_impact_all_metrics.png"))


p4 = bar(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   NEUE_CVAR .* 1e2, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.5)
p4 = bar!(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   NEUE_CVAR .* 1e2, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.8,
   fillstyle=:x)
p4 = ylabel!("CVaR USE [%]")
p4 = xlabel!("")
p4 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)

p5 = bar(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   LOLH_CVAR, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.5)
p5 = bar!(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   LOLH_CVAR, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.8,
   fillstyle=:x)
p5 = ylabel!("CVaR LOLH [h]")
p5 = xlabel!("")
p5 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)

p6 = bar(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   LOLF_CVAR .* 1e2 ./ 1500, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.5)
p6 = bar!(["Base", "Lines derated", "VRE derated", "Thermal derated", "All components"], 
   LOLF_CVAR .* 1e2 ./ 1500, label="",
   palette=:Spectral_11,
   c=[:black,10,8,3,1],
   alpha=0.8,
   fillstyle=:x)

p6 = ylabel!("CVaR LOLF [%]")
p6 = xlabel!("Component impact considered")
p6 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)

plot(p1, p4, p2, p5, p3, p6, layout=(3,2), size=(1300, 900), 
   dpi=500, link=:y)

savefig(joinpath(@__DIR__, "figures", "5-heatwave_component_impact_all_metrics_with_CVaR.png"))

#%%

NEUE = [sum(no.normalised_magnitude_sum), sum(full.normalised_magnitude_sum), sum(noVPP.normalised_magnitude_sum), sum(vpp.normalised_magnitude_sum)] ./ 1500

p1 = bar(["Base", "Heatwave", "Base + 100% VPP", "Heatwave + 100% VPP"], 
   NEUE .* 1e2, label="",
   palette=:Spectral_11,
   c=[:black,1, 10, 11],
   alpha=0.8, 
   ylims=(0,0.0005))
p1 = ylabel!("Average USE [%]")
p1 = xlabel!("Increased storage capacity considered")
p1 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)
plot(p1, size=(650, 300), dpi=500)
savefig(joinpath(@__DIR__, "figures", "5-Heatwave_with_VPP_comparison.png"))

#%%


p1 = groupedbar(["Without VPP", "With 100% VPP coordination"], 
   reshape(NEUE' .* 1e2, :, 2)',
   palette=:Spectral_11,
   c=[:black 1; 10 11],
   alpha=0.8, 
   label=["Base" "Heatwave"],
   ylims=(0,0.0005))
p1 = ylabel!("Average USE [%]")
p1 = xlabel!("")
p1 = plot!(size=(650, 300), dpi=500, leftmargin=5Plots.mm, bottommargin=5Plots.mm)
plot(p1, size=(650, 300), dpi=500)
savefig(joinpath(@__DIR__, "figures", "5-Heatwave_with_VPP_comparison.png"))
