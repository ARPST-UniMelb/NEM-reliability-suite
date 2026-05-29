using Statistics
using DataFrames
using CSV
using SchedNEM
using PRASNEM


function create_summary(case::String; 
   samples=500, 
   base_path::String="Z://", 
   var_confidence_level::Float64=0.95,
   ref_poe_scen_sets = [(2011, 10, 2), (2019,50, 2)],
   target_years = [2025,2030,2035,2040],
   storage_case = "reoptimised",
   add_neue=true,
   resilience_events=[], #["out-ref2017-poe10-heatwave-ref2017-ty2038"]
   write_output=false,
   apply_demand_weights=false
   )
   combination_strings = join(["ref$(ref)-poe$(poe)-s$(scen)" for (ref, poe, scen) in ref_poe_scen_sets], ", ")
   @info "Reading results for case $case\nTarget years: $(target_years)\nRef and POE combinations: $combination_strings\nScenario:$storage_case\nResilience Events:$resilience_events\nWith a total of $samples samples (i.e. 100 samples per batch)..."
   
   poe_weight = Dict(10 => 1.0, 50 => 1.0)
   if apply_demand_weights
      poe_weight = Dict(10 => 0.304, 50 => 0.392)
      @warn "Applying demand weights to the results of $poe_weight"
   end

   base_path_case = joinpath(base_path, "results", case)
   output_path_case = joinpath(base_path, "results-summaries", case)
   pras_path_case = joinpath(base_path, "pras-files", case)
   pras_filenames = Dict(2025 => "2025-01-01_to_2025-12-31_s2_all_regions.pras", 2030 => "2030-01-01_to_2030-12-31_s2_all_regions_incl_line_9_10_18_29_34_45_49.pras", 
      2035 => "2035-01-01_to_2035-12-31_s2_all_regions_incl_line_9_10_18_23_25_29_30_34_45_49_50_51.pras",
      2040 => "2040-01-01_to_2040-12-31_s2_all_regions_incl_line_9_10_18_23_25_29_30_34_45_49_50_51.pras")


   batches = floor(Int, samples / 100)
   samples_per_year = samples * (length(ref_poe_scen_sets) + length(resilience_events))

   # Get all the event details
   all_events = DataFrame(year=[], magnitude_mwh=Float64[], duration_hrs=Float64[], maximum_mw=Float64[], ref_year=Int[], poe=Int[], sample=Int[], start_index=Int[], region=Int[], resilience_event=String[], poe_weight=Float64[])
   all_ens = zeros(length(target_years), length(ref_poe_scen_sets) + length(resilience_events), samples)
   all_ens .= NaN
   for (i, target_year) in enumerate(target_years)
      for (j, (reference_year, poe, scenario)) in enumerate(ref_poe_scen_sets)

         for k in 1:batches
            filename_output = joinpath(base_path_case, "out-ref$(reference_year)-poe$(poe)", "ty$(target_year)", "sf_samples_$(storage_case)_s$(scenario)_batch$(k).csv")
            if !isfile(filename_output)
               @error "Output file $filename_output not found. Skipping any further batches of this case."
               break
            else
               # Get all the event details
               batch_events = PRASNEM.get_all_event_details(SchedNEM.readSfMatrix(filename_output))
               batch_events.year .= target_year
               batch_events.ref_year .= reference_year
               batch_events.poe .= poe
               batch_events.sample .= ((k-1)*100 .+ batch_events.sample)
               batch_events.resilience_event .= ""
               batch_events.poe_weight .= poe_weight[poe]
               append!(all_events, rename(batch_events, :sum => :magnitude_mwh, :length => :duration_hrs, :maximum => :maximum_mw)[!, [:year, :magnitude_mwh, :duration_hrs, :maximum_mw, :ref_year, :poe, :sample, :start_index, :region, :resilience_event, :poe_weight]])
            
               # And all the ens values per sample
               all_ens[i, j, k*100-99:k*100] = SchedNEM.ensFromSfMatrix(filename_output)
            end
         end
      end

      for (j, name) in enumerate(resilience_events)
         for k in 1:batches
            filename_output = joinpath(base_path_case, "$(name)", "ty$(target_year)", "sf_samples_$(storage_case)_s2_batch$(k).csv")
            reference_year = parse(Int, name[findfirst("ref", name)[1]+3:findfirst("-poe", name)[1]-1])
            poe = parse(Int, name[findfirst("-poe", name)[1]+4:findfirst("-poe", name)[1]+5])
            if !isfile(filename_output)
               @error "Output file $filename_output not found. Skipping any further batches of this case."
               break
            else
               # Get all the event details
               batch_events = PRASNEM.get_all_event_details(SchedNEM.readSfMatrix(filename_output))
               batch_events.year .= target_year
               batch_events.ref_year .= reference_year
               batch_events.poe .= poe
               batch_events.sample .= batch_events.sample .+ length(ref_poe_scen_sets) * samples .+ (k - 1) * 100 # To make sure the sample numbers are unique across the reference year/poe combinations and the resilience events
               batch_events.resilience_event .= name
               batch_events.poe_weight .= poe_weight[poe]
               append!(all_events, rename(batch_events, :sum => :magnitude_mwh, :length => :duration_hrs, :maximum => :maximum_mw)[!, [:year, :magnitude_mwh, :duration_hrs, :maximum_mw, :ref_year, :poe, :sample, :start_index, :region, :resilience_event, :poe_weight]])

               # And all the ens values per sample
               all_ens[i, j+length(ref_poe_scen_sets), k*100-99:k*100] = SchedNEM.ensFromSfMatrix(filename_output)
            end
         end
      end

   end

   all_metrics = DataFrame(year=Int[], metric=String[], value=Float64[])
   for (i, year) in enumerate(target_years)
      group = all_events[all_events.year .== year, :]
      eue = sum(group.magnitude_mwh .* group.poe_weight) / samples_per_year
      lolh = sum(group.duration_hrs .* group.poe_weight) / samples_per_year

      # And calculate the tail metrics
      if apply_demand_weights 
         if (length(ref_poe_scen_sets) != 26)
            @warn "Applying demand weights to the results with only a subset of reference year and POE combinations might lead to inaccurate results for the tail metrics. Please check the results for these metrics carefully."
         end
         ens_values = vcat(copy(all_ens[i, :, :][:]), zeros(round(Int, samples * length(ref_poe_scen_sets) / 2))) # Add the ens events from the (fictional) 90% POE run
      else
         ens_values = all_ens[i, :, :][:]
      end
      var_ens = quantile(ens_values, var_confidence_level)
      cvar_ens = mean(ens_values[ens_values .>= var_ens])

      if !isempty(group.duration_hrs)
         all_lolh_values = combine(groupby(group, [:sample, :ref_year, :poe]), :duration_hrs => sum => :total_duration_hrs).total_duration_hrs
         all_lolh_values = vcat(all_lolh_values, zeros(samples_per_year - length(all_lolh_values))) 
         if apply_demand_weights
            all_lolh_values = vcat(all_lolh_values, zeros(round(Int, samples * length(ref_poe_scen_sets) / 2))) # Add the duration values from the (fictional) 90% POE run
         end
         var_lolh = quantile(all_lolh_values, var_confidence_level)
         cvar_lolh = mean(all_lolh_values[all_lolh_values .>= var_lolh])
      else
         var_lolh = 0.0
         cvar_lolh = 0.0
      end

      if (eue != (sum(all_ens[i, :, :][.!isnan.(all_ens[i, :, :])]) / samples_per_year)) && !apply_demand_weights
         @warn "Calculated EUE from events and ENS values do not match for target year $year. Please check the results for this target year."
      end

      push!(all_metrics, (year, "EUE", eue))
      push!(all_metrics, (year, "LOLH", lolh))
      push!(all_metrics, (year, "VAR$(Int(var_confidence_level*100))", var_ens))
      push!(all_metrics, (year, "CVAR$(Int(var_confidence_level*100))", cvar_ens))
      push!(all_metrics, (year, "VAR$(Int(var_confidence_level*100))_LOLH", var_lolh))
      push!(all_metrics, (year, "CVAR$(Int(var_confidence_level*100))_LOLH", cvar_lolh))
   end

   all_events.total_load_mwh .= NaN
   all_events.total_area_load_mwh .= NaN
   all_events.total_duration_hrs .= NaN

   if add_neue
      base_path_pras_file = joinpath(base_path, "pras-files", case)
      normalised_ens = copy(all_ens)

      # And add the NEUE values to the metrics dataframe as well
      for (i, target_year) in enumerate(target_years)
         for (j, (reference_year, poe, scenario)) in enumerate(ref_poe_scen_sets)
            pras_folder = joinpath(base_path_pras_file, "out-ref$(reference_year)-poe$(poe)")
            if !isdir(pras_folder)
               @warn "PRAS folder $pras_folder not found. Skipping NEUE calculation for target year $target_year and reference year $reference_year."
               continue
            end
            # Important that you search for "$(target_year)-12-31" to get the whole year!
            rel_files = filter(f -> occursin("$(target_year)-01-01_to_$(target_year)-12-31", f) && occursin("s$(scenario)", f) && occursin("all_regions", f), readdir(pras_folder))
            if length(rel_files) == 0
               @warn "No PRAS file found for target year $target_year and reference year $reference_year in folder $pras_folder. Skipping NEUE calculation for this combination."
               continue
            else
               pras_file = joinpath(pras_folder, filter(f -> occursin("$(target_year)-01-01_to_$(target_year)-12-31", f) && occursin("s$(scenario)", f) && occursin("all_regions", f), readdir(pras_folder))[1])
               #println("Normalising ENS with: $pras_file")
               sys = PRAS.SystemModel(pras_file)
               normalised_ens[i, j, :] .= all_ens[i, j, :] ./ sum(sys.regions.load) .* 1e6

               # Add the year information to the all_events dataframe for normalisation later on
               all_events[(all_events.year .== target_year) .&& (all_events.ref_year .== reference_year) .&& (all_events.poe .== poe) .&& (all_events.resilience_event .== ""), :total_load_mwh] .= sum(sys.regions.load)
               all_events[(all_events.year .== target_year) .&& (all_events.ref_year .== reference_year) .&& (all_events.poe .== poe) .&& (all_events.resilience_event .== ""), :total_duration_hrs] .= length(sys.timestamps)
            end
         end

         for (j, name) in enumerate(resilience_events)
            reference_year = parse(Int, name[findfirst("ref", name)[1]+3:findfirst("-poe", name)[1]-1])
            poe = parse(Int, name[findfirst("-poe", name)[1]+4:findfirst("-poe", name)[1]+5])
            
            pras_folder = joinpath(base_path_pras_file, "out-ref$(reference_year)-poe$(poe)")

            if !isdir(pras_folder)
               @warn "PRAS folder $pras_folder not found. Skipping NEUE calculation for target year $target_year and resilience event $name."
               continue
            end
            if occursin("heatwave", name)
               # Only choose the files that have the right length
               rel_files = filter(f -> occursin("$(target_year)-01-01_to_$(target_year)-03-31", f) && occursin("s2", f) && occursin("all_regions", f), readdir(pras_folder))
            elseif occursin("vre_drought", name)
               rel_files = filter(f -> occursin("$(target_year)-06-01_to_$(target_year)-08-31", f) && occursin("s2", f) && occursin("all_regions", f), readdir(pras_folder))
            else
               @error "Resilience event name $name does not contain a recognised event type (e.g. heatwave or VRE_drought). Please check the naming of the resilience events and make sure they contain the event type in their name for the NEUE calculation to work."
               return
            end

            if length(rel_files) == 0
               @warn "No PRAS file found for target year $target_year and resilience event $name in folder $pras_folder. Skipping NEUE calculation for this combination."
               continue
            else
               pras_file = joinpath(pras_folder, filter(f -> occursin("$(target_year)-", f) && occursin("s2", f) && occursin("all_regions", f), rel_files)[1])
               sys = PRAS.SystemModel(pras_file)
               normalised_ens[i, j+length(ref_poe_scen_sets), :] .= all_ens[i, j+length(ref_poe_scen_sets), :] ./ sum(sys.regions.load) .* 1e6
               # And add the total load and duration information to the all_events dataframe for normalisation later on
               all_events[(all_events.year .== target_year) .&& (all_events.ref_year .== reference_year) .&& (all_events.poe .== poe) .&& (all_events.resilience_event .== name), :total_load_mwh] .= sum(sys.regions.load)
               all_events[(all_events.year .== target_year) .&& (all_events.ref_year .== reference_year) .&& (all_events.poe .== poe) .&& (all_events.resilience_event .== name), :total_duration_hrs] .= length(sys.timestamps)
            end
         end

         if any(isnan.(normalised_ens[i, :, :]))
            @warn "Not all PRAS files found for target year $target_year after normalisation. Skipping NEUE, NVAR and NCVAR calculation for this target year."
            continue
         end
         selected_ens_values = normalised_ens[i, :, :][.!isnan.(normalised_ens[i, :, :])]
         neue = mean(selected_ens_values)
         nvar_ens = quantile(selected_ens_values, var_confidence_level)
         ncvar_ens = mean(selected_ens_values[selected_ens_values .>= nvar_ens])

         push!(all_metrics, (target_year, "NEUE", neue))
         push!(all_metrics, (target_year, "NVAR$(Int(var_confidence_level*100))", nvar_ens))
         push!(all_metrics, (target_year, "NCVAR$(Int(var_confidence_level*100))", ncvar_ens))

      end
   end

   if !isdir(output_path_case)
      mkpath(output_path_case)
   end

   if write_output
      CSV.write(joinpath(output_path_case, "summary_metrics_$(storage_case).csv"), all_metrics)
      CSV.write(joinpath(output_path_case, "all_events_$(storage_case).csv"), all_events)
      CSV.write(joinpath(output_path_case, "all_ens_$(storage_case).csv"), DataFrame(all_ens=reshape(all_ens, :), target_year=repeat(target_years, inner=length(ref_poe_scen_sets)*samples), ref_year=repeat([ref for (ref, poe, scen) in ref_poe_scen_sets], inner=samples, outer=length(target_years)), poe=repeat([poe for (ref, poe, scen) in ref_poe_scen_sets], inner=samples, outer=length(target_years))))
   end

   return (metrics=all_metrics, events=all_events, ens=all_ens, normalised_ens=add_neue ? normalised_ens : nothing)
end

