
#%% Initialise all dependencies and packages
using Pkg
Pkg.develop(path="../PRASNEM.jl")
Pkg.develop(path="../PISP.jl")
using PISP
using PRASNEM
using PRAS
using Dates
using DataFrames
using Plots
using Statistics

#%% ================ Load and assess the system of 2028 ===============================
nsamples = 100
simspec = SequentialMonteCarlo(samples=nsamples, seed=1);
resultspecs = (Shortfall(),ShortfallSamples());
output_folder = joinpath(pwd(), "data", "pras")
year = 2028

input_folder = joinpath(pwd(), "..","data", "nem12")
timeseries_folder = joinpath(input_folder, "schedule-$(year)")

sys = PRASNEM.create_pras_system(
    DateTime("$(year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS"),
    DateTime("$(year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS"),
    input_folder, timeseries_folder)

sf, sfsamples = assess(sys, simspec, resultspecs...);

#%% ======================= Analysing the results ===============================

df = DataFrame(length=Int[], sum=Int[], maximum=Int[], region=Int[], area=Int[])

region_area_map = PRASNEM.get_region_area_map() # Map region to area

for i in 1:nsamples
    for r in 1:12
        t = PRASNEM.get_event_details(sfsamples.shortfall[r,:,i])
        for event in t
            push!(df, (event.length, event.sum, event.maximum, r, region_area_map[r]))
        end
    end
end


#%%
b_range = range(0, 6, length=25)
histogram(df.sum ./ 1e3, xlabel="Total unserved energy per event [GWh]", ylabel="Probability", normalize=:probability, 
    legend=true,
    label ="",
    bins=b_range,
    #title="Histogram of USE Event Sizes for 2026 System",
    xlims=(0,6), 
    dpi=300, size=(700,400))
vline!([mean(df.sum) ./ 1e3], linestyle=:dot, color=:black, linewidth=2, label="Mean")
var = quantile(df.sum ./ 1e3, 0.90)
cvar = mean(df.sum[df.sum .> var .* 1e3]) ./ 1e3
vline!([var], linestyle=:dash, color=:red, linewidth=2, label="VaR (90%)")
vline!([cvar], linestyle=:dashdot, color=:blue, linewidth=2, label="CVaR (90%)")
savefig("./tutorials/figures/PRASNEM_1_USE_Event_Size_Histogram_2028.png")

#%%

gr()
scatter(df.length, df.sum, dpi=300, size=(800,600),
    markersize=5, markershape=:rect, #z=df.region, 
    yscale=:log10, #palette=:buda, 
    #colorbar=true, 
    #colorbar_title="Size [MWh]", colorbar_ticks=(0:2000:10000, vcat(string.(0:2000:8000), ">10000")),clims=(0,10000),
    label = ["QLD" "NSW" "VIC" "SA"],
    xlims=(0,15), ylims=(10,100000),
    group=df.area,
    #title="USE Events by Region for 2028 System",
    xlabel="Event Length [hours]",
    ylabel="Event Total Energy [MWh]", layout=(2,2), sharex=true)

savefig("./tutorials/figures/PRASNEM_2_USE_Event_Length_vs_Size_2028.png")
#%% Understand when the shortfall is occurring across the year by month and hour of day heatmap
months = month.(sys.timestamps);
hours = hour.(sys.timestamps) .+ 1;

heatmap_matrix = zeros(Float64, 24, 12);
number_of_periods = zeros(Int, 24, 12);
for (val, m, h) in zip(sum(sf.shortfall_mean, dims=1)[:], months, hours)
    heatmap_matrix[h, m] += val;
    number_of_periods[h, m] += 1;
end
heatmap_matrix .= heatmap_matrix ./ number_of_periods; # Normalize to get average

heatmap(
    1:12, 0:23, heatmap_matrix;
    xlabel="Month", ylabel="Hour of Day", #title="Total Unserved Energy",
    xticks=(1:12, ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]),
    colorbar_title="Average unserved energy [MWh]", color=cgrad([:white, :red], scale = :linear),
    cmax = 1.0, cmin=0.0
)
savefig("./tutorials/figures/PRASNEM_2_USE_Heatmap_2028.png")