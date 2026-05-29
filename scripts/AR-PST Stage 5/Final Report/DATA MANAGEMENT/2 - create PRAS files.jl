

#%%
using Pkg; Pkg.activate(".")
using PRASNEM
using Dates
using CSV

#%%


target_years = collect(2025:5:2040) #[2025,2030,2035,2040] #collect(2025:2040)
reference_years = [2011, 2019] #collect(2011:2023)
poes = [10, 50]
scenarios = [1,2,3]

# Optional parameters that include buildout scenarios of storage and gas generation - else keep empty
ref_poe_scen_sets = [(2011, 10, 2), (2019,50, 2)]

base_folder = "Z:/"
buildout_case = "base"
case = "baseVPP"
pisp_base_path = joinpath(base_folder, "pisp-datasets", buildout_case)
pras_base_path = joinpath(base_folder, "pras-files", case)


add_lines = PRASNEM.get_added_lines_per_year()
hydro_parameters = PRASNEM.get_hydro_parameters()
DER_parameters = PRASNEM.get_DER_parameters(; case=case)
regions_selected = collect(1:12)

#%%

all_loads = zeros(length(target_years), length(reference_years), length(poes))

if isempty(ref_poe_scen_sets)
    ref_poe_scen_sets = [(ref, poe, scen) for ref in reference_years for poe in poes for scen in scenarios]
end

for (i, target_year) in enumerate(target_years)
   for (reference_year, poe, scenario) in ref_poe_scen_sets
      println("Processing target year $target_year, reference year $reference_year, poe $poe, scenario $scenario...")
   
      pras_folder = joinpath(pras_base_path, "out-ref$(reference_year)-poe$(poe)")
      start_dt = DateTime("$(target_year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
      end_dt = DateTime("$(target_year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
      pisp_input_folder = joinpath(pisp_base_path, "out-ref$(reference_year)-poe$(poe)", "csv")
      timeseries_folder = "schedule-$(target_year)"

      sys = PRASNEM.create_pras_system(start_dt, end_dt, pisp_input_folder, timeseries_folder; 
                                line_alias_included=add_lines[target_year], output_folder=pras_folder,
                                regions_selected=regions_selected,
                                hydro_parameters=hydro_parameters,
                                DER_parameters=DER_parameters, scenario=scenario)
      
      #total_load = sum(sys.regions.load)
      #all_loads[i, j, k] = total_load
      #println("Reference year $reference_year: Total load in $target_year is $(round(total_load, sigdigits=3)) MWh")
   end
end


