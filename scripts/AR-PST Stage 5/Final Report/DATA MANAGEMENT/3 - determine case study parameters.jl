

#%%

using Pkg; Pkg.activate("."); #Pkg.instantiate()
using PRAS
using Gurobi # Ensure Gurobi is available and licensed, else use HiGHS
using JuMP
using PRASNEM
using SchedNEM
using Plots
using Dates
using CSV
using Statistics

# Include the function to assess adequacy and read results
include("../../functions/all_functions.jl");

#%%

# Calculate all capacity factors
refs = collect(2011:2023)
tys = collect(2025:5:2040)
poes = [10,50]

all_cf = zeros(length(refs), length(tys), length(poes))
all_loads = zeros(length(refs), length(tys), length(poes))


for (i, ref) in enumerate(refs)
    for (j, ty) in enumerate(tys)
        for (k, poe) in enumerate(poes)
            #println("Processing reference year $ref, target year $ty, and PoE $poe...")
            pisp_input_folder = joinpath("Z:/", "pisp-datasets", "out-ref$(ref)-poe$(poe)", "csv")
            pras_folder = joinpath("Z:/", "pras-files", "out-ref$(ref)-poe$(poe)")

            sys = PRASNEM.create_pras_system(DateTime("$(ty)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS"), 
                                            DateTime("$(ty)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS"), 
                                            pisp_input_folder, "schedule-$(ty)"; output_folder=pras_folder)

            vre_idxs = findall(x -> x in ["RoofPV", "LargePV", "Wind"], sys.generators.categories)
            vre_cap = sum(maximum(sys.generators.capacity[vre_idxs, :], dims=2))
            other_gen_cap = sum(maximum(sys.generators.capacity, dims=2)) - vre_cap
            total_load = sum(sys.regions.load)
            all_loads[i, j, k] = total_load

            cf = sum(sys.generators.capacity[vre_idxs,:]) / vre_cap / 8760
            all_cf[i, j, k] = cf
            #println("Reference year $ref and target year $ty: Average capacity factor is $(round(cf*100, sigdigits=3)) %")
        end
    end
end

#%%

lab = ["2025" "2030" "2035" "2040"]
plot(all_cf[:,:,1]  .* 100, marker=:circle, label=lab, color=[1 2 3 4], legend_title="Planning Years", legend=:bottomright)
xticks!(1:length(refs), string.(refs))
xlabel!("Reference Weather Year")
ylabel!("Average VRE Capacity Factor [%]")
savefig(joinpath(@__DIR__, "figures", "vre_capacity_factors.png"))

# => Select weather reference year 2011 as low capacity factor and 2019 as high capacity factor
#%%

plot(all_loads[:,:,1] ./ 1e6, marker=:circle, label=lab, color=[1 2 3 4], legend_title="Planning Years", legend=:bottomright)
plot!(all_loads[:,:,2] ./ 1e6, marker=:square, label=lab, color=[1 2 3 4], legend_title="Planning Years", legend=:bottomright)
xticks!(1:length(refs), string.(refs))
xlabel!("Reference Weather Year")
ylabel!("Total Load [GW]")
#savefig(joinpath(@__DIR__, "figures", "total_loads.png"))

