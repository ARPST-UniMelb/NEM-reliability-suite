
using Pkg; Pkg.activate(".")

using PRASNEM
using PRAS
using CSV
using DataFrames
using Dates
using Plots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));

#%%


target_year = 2040
ref = 2017
poe = 10
scenario = 2

base_path = "Z://"
resilience_folder = joinpath(base_path, "resilience", "heatwave-ref2017-ty2038-v5")

pisp_input_folder = joinpath(base_path, "pisp-datasets", "base", "out-ref2017-poe10", "csv")
timeseries_folder = "schedule-$target_year"


pras_folder = joinpath(base_path, "pras-files", "baseVPP", "out-ref2017-poe10")

add_lines = PRASNEM.get_added_lines_per_year()
DER_parameters = PRASNEM.get_DER_parameters(; case="baseVPP")

start_dt = DateTime("$target_year-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt = DateTime("$target_year-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
sys = PRASNEM.create_pras_system(start_dt, end_dt, pisp_input_folder, timeseries_folder; DER_parameters=DER_parameters, output_folder=pras_folder,
    line_alias_included=add_lines[target_year])
PRASNEM.updateStorageOutageDerating!(sys)
sys_original = deepcopy(sys)


PRASNEM.applyGenHeatwaveDerating!(sys, resilience_folder)
PRASNEM.applyLineHeatwaveDerating!(sys, resilience_folder)

#%%

t = 889:889+7*24-1
t_ticks = vcat(collect(t), [t[end]+1])
t_extended = t[1]-24*7:t[end]+24*7

gen_before = sys_original.generators.capacity[:, t]
gen_after = sys.generators.capacity[:, t]

# Difference per generation type

idx_wind = findall(x -> x == "Wind", sys.generators.categories)
idx_solar = findall(x -> x in ["LargePV", "RoofPV"], sys.generators.categories)
idx_coal = findall(x -> occursin("coal", lowercase(x)), sys.generators.categories)
idx_ccgt = findall(x -> occursin("ccgt", lowercase(x)), sys.generators.categories)
idx_ocgt = findall(x -> occursin("ocgt", lowercase(x)), sys.generators.categories)
idx_diesel = findall(x -> occursin("diesel", lowercase(x)), sys.generators.categories)
idx_other = setdiff(1:size(sys.generators.names,1), vcat(idx_wind, idx_solar, idx_coal, idx_ccgt, idx_ocgt, idx_diesel))


diff_wind = sum((gen_after .- gen_before)[idx_wind, :], dims=1)[:]
diff_solar = sum((gen_after .- gen_before)[idx_solar, :], dims=1)[:]
diff_coal = sum((gen_after .- gen_before)[idx_coal, :], dims=1)[:]
diff_ccgt = sum((gen_after .- gen_before)[idx_ccgt, :], dims=1)[:]
diff_ocgt = sum((gen_after .- gen_before)[idx_ocgt, :], dims=1)[:]
diff_diesel = sum((gen_after .- gen_before)[idx_diesel, :], dims=1)[:]
diff_other = sum((gen_after .- gen_before)[idx_other, :], dims=1)[:]


kwargs = (lw=0, fillalpha=0.8, size=(400,300), dpi=500, xlabel="Time", ylabel="Derating of capacity [GW]",
    xticks=(t_ticks[1:24:end], Dates.format.(Date.(sys.timestamps[t_ticks[1:24:end]]), "dd/mm")),
    yticks=(0:-3:-12, string.(0:3:12)),
    ylims=(-12, 0),
    palette=:Spectral_11)

areaplot(t, [diff_ccgt diff_ocgt diff_diesel] ./ 1e3, 
    c=[1 2 3], 
   legend=false,
   dpi=500; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 0.png"))

areaplot(t, [diff_ccgt diff_ocgt diff_diesel diff_wind] ./ 1e3, 
    c=[1 2 3 8], 
   legend=false,
   dpi=500; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 1.png"))

areaplot(t, [diff_ccgt diff_ocgt diff_diesel diff_wind diff_solar] ./ 1e3, 
    c=[1 2 3 8 5], 
   legend=false,
   dpi=500; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 2.png"))


areaplot(t, [diff_ccgt diff_ocgt diff_diesel diff_wind diff_solar] ./ 1e3, 
    c=[1 2 3 8 5], fillalpha=0.8,
   label=["CCGT" "OCGT" "Diesel" "Wind" "Solar" ], 
   dpi=500, legend=:outerright; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - legend.png"))

#%%

samples = 1500
target_years = [2040]

r_without = create_summary("base";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6-comparison"],
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

r_res = create_summary("base";
   samples=samples, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=[],
   resilience_events=["out-ref2017-poe10-heatwave-ref2017-ty2038-v6"],
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

#%%


r_without.events.normalised_magnitude = r_without.events.magnitude_mwh ./ r_without.events.total_load_mwh
r_res.events.normalised_magnitude = r_res.events.magnitude_mwh ./ r_res.events.total_load_mwh
r_res_lines.events.normalised_magnitude = r_res_lines.events.magnitude_mwh ./ r_res_lines.events.total_load_mwh
r_res_VRE.events.normalised_magnitude = r_res_VRE.events.magnitude_mwh ./ r_res_VRE.events.total_load_mwh
r_res_thermal.events.normalised_magnitude = r_res_thermal.events.magnitude_mwh ./ r_res_thermal.events.total_load_mwh
r_res_VPP.events.normalised_magnitude = r_res_VPP.events.magnitude_mwh ./ r_res_VPP.events.total_load_mwh

no = combine(groupby(r_without.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
lines = combine(groupby(r_res_lines.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
VRE = combine(groupby(r_res_VRE.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
thermal = combine(groupby(r_res_thermal.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
full = combine(groupby(r_res.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)
vpp = combine(groupby(r_res_VPP.events, :sample), :normalised_magnitude => sum, nrow => :count, :duration_hrs => sum)

NEUE = [sum(no.normalised_magnitude_sum), sum(lines.normalised_magnitude_sum), sum(VRE.normalised_magnitude_sum), sum(thermal.normalised_magnitude_sum), sum(full.normalised_magnitude_sum), sum(vpp.normalised_magnitude_sum)] ./ 1500

#%%

kwargs = (palette=:Spectral_11,
   c=[:black,10,9,3,:grey], alpha=1.0,
   size=(500,350), dpi=500,
   ylims=(0, 0.0005), legend=false,
   ylabel="Average annual USE [%]", xlabel="Resilience scenario")

vals = zeros(5)

vals[1] = NEUE[1]
bar(["Base", "Lines\nderated", "VRE\nderated", "Thermal\nderated", "All\nderated"], 
   vals .* 1e2; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 3 - components - 0.png"))

vals[2] = NEUE[2]
bar(["Base", "Lines\nderated", "VRE\nderated", "Thermal\nderated", "All\nderated"], 
   vals .* 1e2; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 3 - components - 1.png"))

vals[3] = NEUE[3]
bar(["Base", "Lines\nderated", "VRE\nderated", "Thermal\nderated", "All\nderated"], 
   vals .* 1e2; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 3 - components - 2.png"))

vals[4] = NEUE[4]
bar(["Base", "Lines\nderated", "VRE\nderated", "Thermal\nderated", "All\nderated"], 
   vals .* 1e2; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 3 - components - 3.png"))

vals[5] = NEUE[5]
bar(["Base", "Lines\nderated", "VRE\nderated", "Thermal\nderated", "All\nderated"], 
   vals .* 1e2; kwargs...)
savefig(joinpath(@__DIR__, "figures", "E - Risk profile - 3 - components - 4.png"))

