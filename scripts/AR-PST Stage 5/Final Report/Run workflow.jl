
#%%
using Pkg; Pkg.activate("."); #Pkg.instantiate()
using PRAS
using Gurobi
using JuMP
using PRASNEM
using SchedNEM
using Dates
using CSV
using Statistics

include("../../../functions/assess_adequacy.jl")

#%%

scenario = 2
samples = 100
for ty in [2030] #collect(2025:2035)
    for ref_trace in collect(2011:2023)
        for poe in [10, 50]
            ens = assess_adequacy(ty, ref_trace, poe, samples, scenario, "Z://";
                solver="Gurobi")
        end
    end
end

#        # Optional parameters that allow for custom configuration
#        genOpDetails::NamedTuple = (uc=true, ramping=true, binary=false),
#        DERparameters::Dict{Symbol, Any} = PRASNEM.get_DER_parameters(),
#        add_lines::Dict{Int, Vector{Int}} = PRASNEM.get_added_lines_per_year(),
#        hydro_parameters::Dict{Symbol, Any} = PRASNEM.get_hydro_parameters(),
#        solver::String = "HiGHS", # "HiGHS" or "Gurobi",
#        sample_number_per_run::Int=100,
#        default_horizon::Int=4, min_time_after_event::Int=4, 
#        optimisation_window::Int=48, move_forward::Int=24
