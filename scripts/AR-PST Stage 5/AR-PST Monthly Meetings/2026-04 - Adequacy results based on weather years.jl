

#%%

using Pkg; Pkg.activate("."); #Pkg.instantiate()
using PRAS
using Gurobi # Ensure Gurobi is available and licensed, else use HiGHS
using JuMP
using PRASNEM
using SchedNEM
using StatsPlots
using Dates
using CSV
using Statistics

# Include the function to assess adequacy and read results
include("../../../functions/all_functions.jl")

#%%
# First, run the full adequacy workflow for the different weather years to generate the required results files (if not already done)

target_year = 2030
reference_years = collect(2011:2023)
poes = [10, 50]
samples = 100
scenario = 2 # Step change scenario
base_path = "Z://" # This should also include all the pisp datasets

for ref_trace in reference_years
   for poe in poes
      ens = assess_adequacy(target_year, ref_trace, poe, samples, scenario, base_path;
            solver="Gurobi")
   end
end

#%% Get all the total load levels for the different reference years to compare the adequacy metrics normalised
all_loads = zeros(length(reference_years), length(poes))
for (i, reference_year) in enumerate(reference_years) 
   for (j, poe) in enumerate(poes)
      pras_folder = joinpath("Z:/", "pras-files", "out-ref$(reference_year)-poe$(poe)")
      # 2025: 
      # 2030: 2030-01-01_to_2030-12-31_s2_all_regions_incl_line_9_10_18_34_45_49_50.pras
      sys = SystemModel(joinpath(pras_folder, "$target_year-01-01_to_$target_year-12-31_s2_all_regions_incl_line_9_10_18_34_45_49_50.pras"))
      total_load = sum(sys.regions.load)
      all_loads[i, j] = total_load
   end
   #println("Reference year $reference_year: Total load in $target_year is $(round(total_load, sigdigits=3)) MWh")
end

#%%

target_year = 2025
reference_years = collect(2011:2023)
nsamples = 100
base_folder_results = joinpath("Z:/", "results")
case = "reoptimised" 
poes = [10, 50]

all_neue = zeros(length(reference_years), 4)
all_ncvar_ens = zeros(length(reference_years), 4)
for (i, case) in enumerate(["greedy", "derated",  "expectation", "reoptimised"])
   r = read_results(target_year, reference_years, poes, scenario, nsamples, base_folder_results; case=case, var_confidence_level=0.99)
   neue_case = r.eue[:] ./ all_loads * 1e6
   cvar_ens_case = r.cvar_ens[:] ./ all_loads * 1e6
   replace!(neue_case, NaN => 0.0)
   replace!(cvar_ens_case, NaN => 0.0)
   all_neue[:, i] = neue_case
   all_ncvar_ens[:, i] = cvar_ens_case
end

#%%

case_no = [4] # 1: greedy, 2: derated, 3: expectation, 4: reoptimised
groupedbar(reference_years, all_ncvar_ens[:, case_no], label="CVaR (99%)", fillcolor=:white, linecolor=:black)
groupedbar!(reference_years, all_neue[:, case_no], label="NEUE", fillcolor=1)
hline!([mean(all_ncvar_ens[:, case_no])], label="Average CVaR", linestyle=:dash, color=:blue)
hline!([mean(all_neue[:, case_no])], label="Average NEUE", linestyle=:dash, color=:red)

xticks!(reference_years)
ylims!(0, 11)
yticks!(0:2:10)
xlabel!("Reference year")
ylabel!("NEUE [ppm] / CVaR ENS [ppm]")
title!("Step Change Scenario - $target_year - 100 Samples")
savefig(joinpath("./scripts/AR-PST Stage 5/AR-PST Monthly Meetings/figures", "2026-04_adequacy_metrics_weighted_by_poe.png"))

