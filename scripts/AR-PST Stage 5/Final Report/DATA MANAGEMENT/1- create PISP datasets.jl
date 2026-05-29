
using PISP

#%% ====================================================================================
# And now run the workflow to generate results for specific buildout cases

download_path = joinpath("Z://", "pisp-downloads") #joinpath("C:/Users/tkopka/Documents/data", "pisp-downloads")
output_root = joinpath("Z://", "pisp-datasets") #joinpath("C:/Users/tkopka/Documents/data", "pisp-datasets")

# Set parameters (see all parameters below)
reference_traces = vcat([4006], collect(2011:2023))  # Use 4006 for the reference trace of the ODP
poes            = [10, 50]    # Probability of exceedance (POE) for demand
target_years    = collect(2025:5:2040) #[2025,2026,2027,2028,2029,2030,2031,2040,2045,2050]


ref_poe_sets = [(ref, poe) for ref in reference_traces for poe in poes]

# Optional parameters that include buildout scenarios of storage and gas generation
buildout_filepath  = normpath(joinpath(@__DIR__, "base_buildout.xlsx"))
sc_buildouts       = Dict(1 => "buildout_odp_s2", 2 => "buildout_odp_s2", 3 => "buildout_odp_s2")
case_path_output = joinpath(output_root, "more-bess")

for (reference_trace, poe) in ref_poe_sets
    println("Creating datasets for reference trace $reference_trace and POE $poe...")
    PISP.build_ISP24_datasets(
        downloadpath       = download_path,
        download_from_AEMO = false,
        poe                = poe,
        reftrace           = reference_trace,
        years              = target_years,
        output_root        = case_path_output,
        write_csv          = true,
        write_arrow        = false,
        scenarios          = [2], #[1,2,3],
        write_traces       = false,
        buildout_filepath  = buildout_filepath,
        sc_buildouts       = sc_buildouts,)
end

