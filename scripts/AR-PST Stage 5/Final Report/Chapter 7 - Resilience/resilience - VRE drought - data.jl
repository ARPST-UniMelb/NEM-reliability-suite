
#%%
using JuMP
using Plots
using Dates
using Gurobi
using SchedNEM
using PRAS
using PRASNEM
using Statistics

include("../../../../../functions/read_results.jl")
include("../../../../functions/assess_adequacy_resilience.jl");


#%%

ref_scen_set = (2019, 50, 2) #[(2019, 50, 2),(2011,10,2)]
case = "base"
target_year = 2040


pras_folder = joinpath("Z:/", "pras-files", case, "out-ref$(ref_scen_set[1])-poe$(ref_scen_set[2])")
pisp_input_folder = joinpath("Z:/", "pisp-datasets", "base", "out-ref$(ref_scen_set[1])-poe$(ref_scen_set[2])", "csv")
timeseries_folder = "schedule-$target_year"

DER_parameters = PRASNEM.get_DER_parameters(; case="base")
add_lines = PRASNEM.get_added_lines_per_year()
start_dt = DateTime("$target_year-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt = DateTime("$target_year-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")

sys = PRASNEM.create_pras_system(start_dt, end_dt, pisp_input_folder, timeseries_folder; DER_parameters=DER_parameters, output_folder=pras_folder,
    line_alias_included=add_lines[target_year])
sys = PRASNEM.updateStorageOutageDerating!(sys)

pras_folder = joinpath("Z:/", "pras-files", case, "out-ref$(ref_scen_set[1])-poe50")
pisp_input_folder = joinpath("Z:/", "pisp-datasets", "base", "out-ref$(ref_scen_set[1])-poe50", "csv")

#%%


moving_average(x, window) = [(i < window ? NaN : mean(x[i-+round(Int,window/2)+1:i+round(Int,window/2)])) for i in 1:length(x)-window]

daily_res_demand = [sum((dem .- vre)[(i-1)*24+1:(i-1)*24+24]) for i in 1:366] ./ 1e3
bar(daily_res_demand,
   xlabel = "Day of year", 
   ylabel="Daily residual demand [GWh]", 
   label="Daily residual demand",
   c=:black, alpha=0.7)
plot!(moving_average(daily_res_demand, 7), label="7-day moving average", c=2, lw=2)
hline!([mean(daily_res_demand)], 
   label="Annual average", c=1, lw=3, linestyle=:dot,
   legend=:topleft, dpi=500)
xlims!(0, 366)
ylims!(-1000, 1000)
yticks!(-1000:250:1000)

first_hour = findall(day.(DateTime.(2024,1,1) .+ (0:365)*Day(1)) .== 1)
xticks!(first_hour, string.(day.(sys.timestamps[first_hour*24])) .* "/" .* string.(month.(sys.timestamps[first_hour*24])))
#savefig(joinpath(@__DIR__, "figures","5-VREdrought_daily_residual_demand.png"))


#%% =================================================================================================================

sys_original = deepcopy(sys)
sys_3days = deepcopy(sys)
PRASNEM.updateVREDroughtLength!(sys_3days; consecutive_days=3)
sys_7days = deepcopy(sys)
PRASNEM.updateVREDroughtLength!(sys_7days; consecutive_days=7)


#%%
idxs = 4033:4056
idxs_extended = idxs[1]-24*2:idxs[end]+24*7
vre_idxs = findall(x -> x in ["Wind", "LargePV", "RoofPV"], sys.generators.categories)

plot(sum(sys.regions.load[:, idxs_extended], dims=1)[:] ./ 1e3, lw=2, c=:black, label="Demand")
areaplot!(2*24:3*24, repeat([70], 25), color=:blue, alpha=0.1,
   label="Critical event window")
areaplot!(3*24:9*24, repeat([70], 6*24+1), color=:grey, alpha=0.1,
   label="Window with extended VRE drought")
areaplot!(sum(sys_original.generators.capacity[vre_idxs, idxs_extended], dims=1)[:] ./ 1e3, 
   label="VRE capacity (original)", lw=2, c=5,
   fillalpha=0.3, alpha=1.0,
   linecolor=:black, linestyle=:dash,
   legend=:outertop)
areaplot!(sum(sys_3days.generators.capacity[vre_idxs, idxs_extended], dims=1)[:] ./ 1e3, 
   label="VRE capacity (3-day drought)", lw=2, c=4,
   fillalpha=0.3, alpha=0.7)
areaplot!(sum(sys_7days.generators.capacity[vre_idxs, idxs_extended], dims=1)[:] ./ 1e3, 
   label="VRE capacity (7-day drought)", lw=2, c=8, 
   fillalpha=0.3, alpha=0.7)
areaplot!(sum(sys_original.generators.capacity[vre_idxs, idxs_extended], dims=1)[:] ./ 1e3, 
   label="", lw=2, c=5,
   fillalpha=0.0, alpha=1.0,
   linecolor=:black, linestyle=:dash)

crtical_hours = idxs[1]:idxs[1]+7*24


xticks!(1:48:length(idxs_extended), string.(Date.(sys.timestamps[idxs_extended[1:48:end]])))
ylabel!("Power [GW]")
plot!(size=(800, 500), dpi=500, leftmargin=5Plots.mm)
#savefig(joinpath(@__DIR__, "figures","5-VREdrought_vre_capacity_during_event.png"))

#%% Reduced VRE availability by state


diff_per_region3days = (sys_3days.generators.capacity .- sys.generators.capacity)[:, idxs_extended]
diff_per_region7days = (sys_7days.generators.capacity .- sys.generators.capacity)[:, idxs_extended]

area2region = PRASNEM.get_region_area_map(; system="ISP24", rev=true)
diff_per_area = [sum(diff_per_region7days[area2region[area], :], dims=1)[:] for area in 1:5]
diff_per_area = hcat(diff_per_area...)'

labs = ["QLD" "NSW" "VIC" "TAS" "SA"]
c = ["#9B2242" "#A1DAE7" "#7C7FAB" "#A8D088" "#E56A54"]
areaplot(diff_per_area' ./ 1e3, fillalpha=0.8, lw=0, c=c, label=labs, palette=:batlow25)
xlabel!("Dates")
ylabel!("Change in generation capacity [GW]")
title!("")


xticks!(49:24:length(idxs_extended), string.(Date.(sys.timestamps[idxs_extended[49:24:end]])))
ylims!(-6.7, 0.8)
xlims!(49, length(idxs_extended))
plot!(size=(800, 500), dpi=500, leftmargin=5Plots.mm)
savefig(joinpath(@__DIR__, "figures", "5-VREdrought_gen_derating_by_state.png"))
