



using Pkg; Pkg.activate(".")

using PRASNEM
using PRAS
using CSV
using DataFrames
using Dates
using Plots

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
t_extended = t[1]-24*7:t[end]+24*7
#t = 384:551

gen_before = sys_original.generators.capacity[:, t]
gen_after = sys.generators.capacity[:, t]

gen_before50 = sys50_original.generators.capacity[:, t]
gen_after50 = sys50.generators.capacity[:, t]


# Look at the demand
plot(sum(sys.regions.load[:, t], dims=1)[:] ./ 1e3, lw=2, c=:black, label="Demand")
#plot!(sum(sys50.regions.load[:, t], dims=1)[:] ./ 1e3, lw=2, c=:black, label="Demand (50% POE)", linestyle=:dash)
plot!(sum(gen_before, dims=1)[:] ./ 1e3, label="Before derating", lw=2, c=:blue)
plot!(sum(gen_after, dims=1)[:] ./ 1e3, label="After derating", lw=2, c=:red)
xlabel!("Time")
xticks!(1:48:length(t), string.(Date.(sys.timestamps[t[1:48:end]])))
ylabel!("Generation Capacity [GW]")
title!("Generation Capacity Before and After Derating")

#%%

lines_fwcap_before = sys_original.lines.forward_capacity[:, t]
lines_rvcap_before = sys_original.lines.backward_capacity[:, t]
#plot(gen_before')


#%%

gen_caps = [sum(sys.generators.capacity[sys.region_gen_idxs[i],t], dims=1) for i in 1:length(sys.regions)]
gen_after = sys.generators.capacity[:,t]


# Generators

plot(t, sum(gen_before, dims=1)[:] ./ 1e3, label="Before derating")
plot!(t, sum(gen_after, dims=1)[:] ./ 1e3, label="After derating")
xlabel!("Time")
ylabel!("Total generation capacity [GW]")
title!("Total generation capacity")
xticks!(t[1:24:end], string.(Date.(sys.timestamps[t[1:24:end]])))


#%%

# Difference per region

diff_per_region = [sum((gen_after .- gen_before)[sys.region_gen_idxs[i], :], dims=1)[:] for i in 1:length(sys.regions)]
diff_per_region = hcat(diff_per_region...)'


labels = ["NQ" "CQ" "GG" "SQ" "NNSW" "CNSW" "SNW" "SNSW" "VIC" "TAS" "CSA" "SESA"]
areaplot(t, diff_per_region' ./ 1e3, fillalpha=0.8, lw=0, c=[1 3 5 7 9 11 13 15 17 19 21 23], label=labels, palette=:batlow25)
xlabel!("Time")
ylabel!("Change in generation capacity [GW]")
title!("Change in gen capacity")
xticks!(t[1:48:end], string.(Date.(sys.timestamps[t[1:48:end]])))

#%%

area2region = PRASNEM.get_region_area_map(; system="ISP24", rev=true)
diff_per_area = [sum(diff_per_region[area2region[area], :], dims=1)[:] for area in 1:5]
diff_per_area = hcat(diff_per_area...)'

labs = ["QLD" "NSW" "VIC" "TAS" "SA"]
c = ["#9B2242" "#A1DAE7" "#7C7FAB" "#A8D088" "#E56A54"]
areaplot(t, diff_per_area' ./ 1e3, fillalpha=0.8, lw=0, c=c, label=labs, palette=:batlow25)
xlabel!("Dates")
ylabel!("Reduction in generation capacity [GW]")
title!("Generation capacity reduction by state")
xticks!(t[1:48:end], string.(Date.(sys.timestamps[t[1:48:end]])))
ylims!(-15, 0)
xlims!(t[1], t[end])
savefig(joinpath(@__DIR__, "figures", "5_gen_derating_by_state.png"))


#%%

lines_fwcap_after = sys.lines.forward_capacity[:, t]
lines_rvcap_after = sys.lines.backward_capacity[:, t]

plot(t, sum(lines_fwcap_before, dims=1)[:] ./ 1e3, label="Forward Capacity", lw=2, c=1, linestyle=:dash)
plot!(t, sum(lines_fwcap_after, dims=1)[:] ./ 1e3, label="Forward Capacity with derating", lw=2, c=1)
plot!(t, sum(lines_rvcap_before, dims=1)[:] ./ 1e3, label="Reverse Capacity", lw=2, c=2, linestyle=:dash)
plot!(t, sum(lines_rvcap_after, dims=1)[:] ./ 1e3, label="Reverse Capacity with derating", lw=2, c=2, legend=:bottomleft)
xlabel!("Dates")
ylabel!("Total line capacity [GW]")
title!("Transfer capacity reduction due to heatwave")
xticks!(t[1:48:end], string.(Date.(sys.timestamps[t[1:48:end]])))
savefig(joinpath(@__DIR__, "figures", "5_line_derating.png"))


#%%

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


areaplot(t, [diff_coal diff_ccgt diff_ocgt diff_diesel diff_wind diff_solar  diff_other] ./ 1e3, 
   fillalpha=0.8, lw=0, c=[:black 1 2 3 8 5 11], 
   label=["Coal" "CCGT" "OCGT" "Diesel" "Wind" "Solar"  "Other"], palette=:Spectral_11,
   dpi=500)
xlabel!("Time")
ylabel!("Change in generation capacity [GW]")
title!("Change in gen capacity by type")
xticks!(t[1:48:end], string.(Date.(sys.timestamps[t[1:48:end]])))
savefig(joinpath(@__DIR__, "figures", "5_gen_derating_by_type.png"))


#%%

plot(t, sum(lines_fwcap_before, dims=1)[:] ./ 1e3, label="FwCap before derating", c=1, linestyle=:dash)
plot!(t, sum(lines_fwcap_after, dims=1)[:] ./ 1e3, label="FwCap after derating", c=1)
plot!(t, sum(lines_rvcap_before, dims=1)[:] ./ 1e3, label="RvCap before derating", c=2, linestyle=:dash)
plot!(t, sum(lines_rvcap_after, dims=1)[:] ./ 1e3, label="RvCap after derating", c=2, legend=:bottomleft)
xlabel!("Time")
ylabel!("Total line capacity [GW]")
title!("Total line capacity")
xticks!(t[1:24:end], string.(Date.(sys.timestamps[t[1:24:end]])))


#%%
plot(t, (lines_fwcap_before ./ 1e3)', label="", linestyle=:dash)
plot!(t, (lines_fwcap_after ./ 1e3)', label="")
xlabel!("Time")
ylabel!("Line capacity [GW]")
xticks!(t[1:24:end], string.(Date.(sys.timestamps[t[1:24:end]])))

#%% ============================================================================================


# What regions provide renewables in this event?

s = SchedNEM.read_schedule(joinpath("Z:/schedules/base/out-ref2017-poe10-heatwave-ref2017-ty2038-v4/ty2040", "2040-ref2017-poe50-s2.h5"))
#%%
vre_idxs = findall(x -> x in ["Wind", "LargePV", "RoofPV"], sys.generators.categories)
vre_per_region = [sum(sys.generators.capacity[intersect(vre_idxs, sys.region_gen_idxs[i]), :], dims=1)[:] for i in 1:length(sys.regions)]
vre_per_region = hcat(vre_per_region...)'
plot(vre_per_region' ./ 1e3,
   label=["NQ" "CQ" "GG" "SQ" "NNSW" "CNSW" "SNW" "SNSW" "VIC" "TAS" "CSA" "SESA"], 
   palette=:batlow25, c=collect(1:2:23)', fillalpha=0.8, lw=2)
ylabel!("Renewable production [GW]")
xlims!(t[1], t[end])
xticks!(t[1:24:end], string.(Date.(sys.timestamps[t[1:24:end]])))


#%%


area2region = PRASNEM.get_region_area_map(; system="ISP24", rev=true)
prod_per_region = [sum(s.p_gen[vcat(sys.region_gen_idxs[area2region[a]]...), t], dims=1)[:] for a in 1:5]
prod_per_region = hcat(prod_per_region...)'

areaplot(t, prod_per_region' ./ 1e3,
   label=["QLD" "NSW" "VIC" "TAS" "SA"], stack=true,
   palette=:batlow10, c=collect(1:2:10)', fillalpha=0.8, lw=2)
ylabel!("Generator production [GW]")
xticks!(t[1:24:end], string.(Date.(sys.timestamps[t[1:24:end]])))


#%%

idx_wind = findall(x -> x == "Wind", sys.generators.categories)

t_jan = findfirst(DateTime.(sys.timestamps) .== DateTime("2040-01-15 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS"))
t_jan = t_jan:t_jan + 24*7 - 1

t_feb = findfirst(DateTime.(sys.timestamps) .== DateTime("2040-02-07 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS"))
t_feb = t_feb:t_feb + 24*7 - 1
plot(sum(sys_original.generators.capacity[idx_wind, t_jan], dims=1)[:], lw=2, label="Jan 15-22", c=:blue)
plot!(sum(sys_original.generators.capacity[idx_wind, t_feb], dims=1)[:], lw=2, label="Feb 7-14", c=:red)
xlabel!("Time")
ylabel!("Wind generation capacity [MW]")
title!("Wind generation capacity in Jan vs Feb")

