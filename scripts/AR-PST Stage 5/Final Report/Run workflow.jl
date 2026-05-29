
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

include("../../../functions/all_functions.jl")

#%%

ref_poe_scen_sets = [(ref, poe, 2) for ref in collect(2012:2023) for poe in [10]]
target_years = collect(2025:5:2040)
samples = 500
base_path = "Z://"

case_name = "base"
buildout_case =  "base" 
DER_parameters = PRASNEM.get_DER_parameters(; case="base")
genOpDetails = (uc=true, ramping=true, binary=false)
resilience_events = [""] #"heatwave-ref2017-ty2038-v5-thermal", "heatwave-ref2017-ty2038-v5-lines", "heatwave-ref2017-ty2038-v5-VRE", "heatwave-ref2017-ty2038-v5"] #"heatwave-ref2017-ty2038-v5" #"heatwave-ref2017-ty2038"

for resilience_event in resilience_events
    for ty in target_years
        for (ref, poe, scen) in ref_poe_scen_sets
            ens = assess_adequacy(ty, ref, poe, samples, scen, base_path;
                DER_parameters=DER_parameters,
                case_name=case_name,
                solver="Gurobi",
                resilience_event=resilience_event,
                case_name_buildout=buildout_case,
                genOpDetails=genOpDetails,
                )
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
#%% ================================================================================

# Analyse the results

#r = create_summary("baseVPP"; 
#   samples=500, 
#   storage_case="reoptimised", 
#   ref_poe_scen_sets=[(2011, 10, 2), (2019, 50, 2)],)
