# Tutorial: Assessing adequacy of the ISP with PRASNEM.jl and PRAS

# Goal: Present how to create pras-files using PRASNEM, run simple adequacy assessments, and visualise results


#%% Initialise all dependencies and packages
using Pkg
Pkg.develop(path="../PRASNEM.jl")
Pkg.develop(path="../PISP.jl")
using PISP
using PRASNEM
using PRAS
using Dates
using Plots
using Measures

#%% ================= Creating a PRAS file with PRASNEM ===============================
#Create PRAS file for 2026
start_dt = DateTime("2026-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt = DateTime("2026-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
input_folder = joinpath(pwd(), "data", "csv")
timeseries_folder = joinpath(input_folder, "schedule-2026")
sys = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder="", regions_selected=collect(1:12))

#%% ================= Evaluating adequacy with PRAS ===============================
simspec = SequentialMonteCarlo(samples=100);
resultspecs = (Shortfall(),ShortfallSamples());

println("Running adequacy assessment...")
sf, sfsamples = assess(sys, simspec, resultspecs...)


#%% ================= Analysing the results ===============================
# Note: NEUE (Normalised expected unserved energy) corresponds to the expected USE metric used by AEMO, 
# however PRAS displays parts per million (ppm) instead of % . The reliability standard of 0.002% corresponds to 20 ppm.

println("Results: ")
println(LOLE(sf))
println(EUE(sf))
println(NEUE(sf))

println("NEUE by subregion [%]: ")
println(round.(sum(sf.shortfall_mean,dims=2) ./ sum(sys.regions.load, dims=2) .* 100, digits=4)[:])
println("NEUE by state:")
neue_by_state = PRASNEM.NEUE_area(sys, sf; bus_file_path=joinpath(pwd(), "data", "csv", "Bus.csv"));