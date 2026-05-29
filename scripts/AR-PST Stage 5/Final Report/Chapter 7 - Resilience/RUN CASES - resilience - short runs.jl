


using JuMP
using Plots
using Dates
using Gurobi
using SchedNEM
using PRAS
using PRASNEM
using Statistics

include("../../../../functions/read_results.jl")
include("../../../../functions/assess_adequacy_resilience_heatwave.jl");
include("../../../../functions/assess_adequacy_resilience_VRE_drought.jl");

#%% Heatwave runs!

# ref_poe_scen_sets = [(2017, 10, 2)] #[(2019, 50, 2),(2011,10,2)]
# target_years = collect(2025:5:2040)
# samples = 500

# case_name = "base"
# DER_parameters = PRASNEM.get_DER_parameters(; case=case_name)
# genOpDetails = (uc=true, ramping=true, binary=false)
# resilience_events = ["heatwave-ref2017-ty2040", "heatwave-ref2017-ty20-comparison", "heatwave-ref2017-ty2040-lines"]

# for resilience_event in resilience_events
#     for ty in target_years
#         for (ref, poe, scen) in ref_poe_scen_sets
#             ens = assess_adequacy_resilience(ty, ref, poe, samples, scen, "Z://";
#                 DER_parameters=DER_parameters,
#                 case_name=case_name,
#                 solver="Gurobi",
#                 resilience_event=resilience_event,
#                 genOpDetails=genOpDetails,
#                 )
#         end
#     end
# end

#%% VRE drought runs

ref_poe_scen_sets = [(2019, 50, 2)] #[(2019, 50, 2),(2011,10,2)]
target_years = collect(2040:5:2040)
samples = 500
        

vre_drought_area_day_sets = [([1,2,3,4,5], 1), ([1,2,3,4,5], 3), ([1,2,3,4,5], 7)]
    #  ([1,2,3,4,5], 1), ([1,2,3,4,5], 3), ([1,2,3,4,5], 7),
    #                             ([1], 3), ([1], 7), 
    #                             ([2], 3), ([2], 7),
    #                             ([3], 3), ([3], 7),
    #                             ([4], 3), ([4], 7),
    #                             ([5], 3), ([5], 7),
                                


area2region = PRASNEM.get_region_area_map(;rev=true)
case_name = "baseVPP"
buildout_case = "base"
DER_parameters = PRASNEM.get_DER_parameters(; case="baseVPP")
genOpDetails = (uc=true, ramping=true, binary=false)

for (vre_drought_area, vre_drought_days) in vre_drought_area_day_sets
    resilience_event = "vre_drought_area$(join(vre_drought_area, ""))_day$(vre_drought_days)"
    vre_drought_regions = vcat([area2region[area] for area in vre_drought_area]...)
    for ty in target_years
        for (ref, poe, scen) in ref_poe_scen_sets
            ens = assess_adequacy_resilience_VRE_drought(ty, ref, poe, samples, scen, "Z://";
                DER_parameters=DER_parameters,
                case_name=case_name,
                solver="Gurobi",
                case_name_buildout=buildout_case,
                resilience_event=resilience_event,
                genOpDetails=genOpDetails,
                regions_vre_drought=vre_drought_regions,
                days_vre_drought=vre_drought_days,
                saving_details=(:shortfall,),
                min_time_after_event=10,
                )
        end
    end
end



