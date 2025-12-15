
#%% Initialise all dependencies and packages
using Pkg
using PISP
using PRASNEM
using PRAS
using Plots
using CSV
using DataFrames
using Dates

#%% ================= Loading the system ===============================

simspec = SequentialMonteCarlo(samples=500, seed=110);
respec = (Shortfall(),);
tyear = 2030

input_folder = joinpath(pwd(), "..", "data", "nem12")
output_folder = joinpath(pwd(), "..", "data", "nem12", "pras")

# Get the original system first
start_dt = DateTime("$(tyear)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt = DateTime("$(tyear)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
timeseries_folder = joinpath(input_folder, "schedule-$(tyear)")
sys = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder)
sys_without_dsp = deepcopy(sys)
sys_without_dsp.demandresponses.borrow_capacity .= 0 # Set all DSP to zero. This effectively removes DSP. 
# This approach is taken here to keep the random seeds consistent (instead of changing the system structure to not have DSP included).
# Alternatively, one could create the system without DSP from the beginning:
# sys_without_dsp = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder, gentech_excluded=["DSP"])

#%% ====================================  Run PRAS with and without DSP ================================================

# Note that the the location of DSP is not defined in the ISP. Therefore, multiple options to distribute DSP across regions can be considered.
# Here, we consider two options:
# 1) Assign the full DSP capacity to the main load subregions(default)
sf_with_dsp_default, = assess(sys, simspec, respec...);
# 2) Distribute DSP capacity across the subregions for each state according to the peak demand in that state in the data
sf_with_dsp_peak, = assess(PRASNEM.redistribute_DR(sys; mode="max demand"), simspec, respec...);
# And compare it to the no DSP case
sf_without_dsp, = assess(sys_without_dsp, simspec, respec...);

println("Year $tyear: Shortfall with DSP = $(NEUE(sf_with_dsp_default)) / $(NEUE(sf_with_dsp_peak)), without DSP = $(NEUE(sf_without_dsp))")
println("Without DSP:")
PRASNEM.NEUE_area(sys_without_dsp, sf_without_dsp; bus_file_path=joinpath(input_folder, "Bus.csv"));
println("With DSP (default):")
PRASNEM.NEUE_area(sys, sf_with_dsp_default; bus_file_path=joinpath(input_folder, "Bus.csv"));
println("With DSP (peak demand distribution):")
PRASNEM.NEUE_area(sys, sf_with_dsp_peak; bus_file_path=joinpath(input_folder, "Bus.csv"));