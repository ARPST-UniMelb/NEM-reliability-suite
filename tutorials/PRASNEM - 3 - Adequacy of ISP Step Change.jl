# Tutorial: Assessing adequacy of the ISP with PRASNEM.jl and PRAS
# Goal: Present how to create pras-files using PRASNEM, run simple adequacy assessments, and visualise results


#%% Initialise all dependencies and packages
using Pkg
Pkg.develop(path="../PRASNEM.jl") # Adjust path as needed
Pkg.develop(path="../PISP.jl") # Adjust path as needed
using PISP
using PRASNEM
using PRAS
using Plots
using Dates
using CSV
using DataFrames

# %% Create the PRAS files (optional if already created)
# for year in 2025:1:2035
#     start_dt = DateTime("$(year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
#     end_dt = DateTime("$(year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
#     input_folder = joinpath(pwd(), "data", "csv")
#     output_folder = joinpath(pwd(), "data", "pras")
#     timeseries_folder = joinpath(input_folder, "schedule-$(year)")
#     # Create the full PRAS system with the 12 subregions from the ISP 2024
#     sys = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder, regions_selected=collect(1:12))
# end


#%% ================= Evaluating adequacy with PRAS ===============================

# First without any transmission expansion

simspec = SequentialMonteCarlo(samples=100, seed=1);
resultspecs = (Shortfall(),);
target_years = 2025:1:2035

sf_states = zeros(Float64, 5, length(target_years)) # Store NEUE results for 5 areas, 11 target years

input_folder = joinpath(pwd(), "..", "data", "nem12")
output_folder = joinpath(pwd(), "..", "data", "nem12", "pras")

for year in target_years
    println("Running adequacy assessment for year $year...")
    start_dt = DateTime("$(year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
    end_dt = DateTime("$(year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
    
    timeseries_folder = joinpath(input_folder, "schedule-$(year)")
    sys = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder)
    sf, = assess(sys, simspec, resultspecs...);
    println("    Year $year: Total average NEUE = ", NEUE(sf))
    sf_states[:, year - target_years[1] + 1] .= PRASNEM.NEUE_area(sys, sf; bus_file_path=joinpath(input_folder, "Bus.csv"))
    println(sf_states)
end

# Aggregate results in DataFrame
res = DataFrame()
for i in eachindex(target_years)
    year = target_years[i]
    push!(res, (targetYear=year, 
        QLD=sf_states[1, i], NSW=sf_states[2, i], VIC=sf_states[3, i], TAS=sf_states[4, i], SA=sf_states[5, i]))
end

CSV.write("./tutorials/PRASNEM_3_ISP_Step_Change_adequacy_results.csv", res)


#%% Then with some transmission expansion added in later years

added_lines_per_year = Dict(
    2025 => [],
    2026 => [],
    2027 => ["NL_86_INV30"], # HumeLink
    2028 => ["NL_86_INV30"], 
    2029 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3"], # Sydney Ring North, Gladstone Grid Reinforcement
    2030 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3", "NL_109_INV35", "VNI North", "VNI South", "NL_98_INV34"], # Marinus Link Stage 1, VNI + VNI West
    2031 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3", "NL_109_INV35", "VNI North", "VNI South", "NL_98_INV34"],
    2032 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3", "NL_109_INV35", "VNI North", "VNI South", "NL_98_INV34", "NL_42_INV8"], # QLD SuperGrid South
    2033 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3", "NL_109_INV35", "VNI North", "VNI South", "NL_98_INV34", "NL_42_INV8", "NL_109_INV36"], # Marinus Link Stage 2
    2034 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3", "NL_109_INV35", "VNI North", "VNI South", "NL_98_INV34", "NL_42_INV8", "NL_109_INV36", "NL_54_INV10"], # QNI Connect
    2035 => ["NL_86_INV30", "NL_67_INV19", "NL_23_INV3", "NL_109_INV35", "VNI North", "VNI South", "NL_98_INV34", "NL_42_INV8", "NL_109_INV36", "NL_54_INV10"]
    )

simspec = SequentialMonteCarlo(samples=100, seed=1);
resultspecs = (Shortfall(),);
target_years = 2025:1:2035

sf_states_expansion = zeros(Float64, 5, length(target_years)) # Store NEUE results for 5 areas, 11 target years

input_folder = joinpath(pwd(), "..", "data", "nem12")
output_folder = joinpath(pwd(), "..", "data", "nem12", "pras")

for year in target_years
    println("Running adequacy assessment for year $year...")
    start_dt = DateTime("$(year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
    end_dt = DateTime("$(year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
    timeseries_folder = joinpath(input_folder, "schedule-$(year)")
    sys = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder=output_folder, line_alias_included=added_lines_per_year[year])
    sf, = assess(sys, simspec, resultspecs...);
    println("    Year $year: Total average NEUE = ", NEUE(sf))
    sf_states_expansion[:, year - target_years[1] + 1] .= PRASNEM.NEUE_area(sys, sf; bus_file_path=joinpath(input_folder, "Bus.csv"))
    println(sf_states_expansion)
end

# Aggregate results in DataFrame
res = DataFrame()
for i in eachindex(target_years)
    year = target_years[i]
    push!(res, (targetYear=year, 
        QLD=sf_states_expansion[1, i], NSW=sf_states_expansion[2, i], VIC=sf_states_expansion[3, i], TAS=sf_states_expansion[4, i], SA=sf_states_expansion[5, i]))
end

CSV.write("./tutorials/PRASNEM_3_ISP_Step_Change_adequacy_results_with_expansion.csv", res)





#%% ================= Visualise the results ===============================

gr()
plot(2025:1:2035, sf_states' ./ 1e4, labels=["QLD" "NSW" "VIC" "TAS" "SA"], 
    xlabel="Calendar Year", ylabel="Expected yearly unserved energy",
    #title="Projected adequacy of the NEM regions\n_ISP Step Change, no transmission expansion_", 
    grid=true, size=(600,400), dpi=300, markershape=:rect, linewidth=2, legend=:topleft,
    titlefontsize=12, yticks=(0:0.005:0.02, ""))
ylims!(0, 0.02)
#hline!([0.002], linestyle=:dash, color=:black, label="Reliability standard\n(0.002% USE)")
savefig("./tutorials/figures/PRASNEM_3_ISP_Step_Change_adequacy.png")


#%%
gr()
plot(2025:1:2035, sf_states_expansion' ./ 1e4, labels=["QLD" "NSW" "VIC" "TAS" "SA"], 
    xlabel="Calendar Year", ylabel="Expected yearly unserved energy",
    #title="Projected adequacy of the NEM regions\n_ISP Step Change, no transmission expansion_", 
    grid=true, size=(600,400), dpi=300, markershape=:rect, linewidth=2, legend=:topleft,
    titlefontsize=12, yticks=(0:0.005:0.02, ""))
ylims!(0, 0.02)
#hline!([0.002], linestyle=:dash, color=:black, label="Reliability standard\n(0.002% USE)")
savefig("./tutorials/figures/PRASNEM_3_ISP_Step_Change_adequacy_expansion.png")