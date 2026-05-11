"""

Calculates the adequacy of the system with the full workflow, saving outputs of results under storage operation:
1. Greedy storage operation (i.e. original PRAS)
2. Derated storage operation
3. Expectation storage operation (i.e. with the expected dispatch of DERs and storage)
4. Reoptimised storage operation (i.e. full workflow)


Parameters:
- 'target_year': Target year(s) to assess adequacy for (vector of integers).
- `reference_traces`: Reference trace(s) to use for the adequacy assessment (vector of integers).
- `poes`: Probability of exceedance (POE) for demand (vector of integers) (either [10, 50] or [10] or [50]).
- `base_path`: Path to download the datasets to (string)


"""
function assess_adequacy(target_year::Int=2030,
        reference_trace::Int=4006,
        poe::Int=10,
        samples::Int=100,
        scenario::Int=2,
        base_path::String="Z://";
        # Optional parameters that allow for custom configuration
        genOpDetails::NamedTuple = (uc=true, ramping=true, binary=false),
        case_name::String="base",
        case_name_buildout::String="base",
        regions_selected::Vector{Int} = collect(1:12),
        DER_parameters = PRASNEM.get_DER_parameters(),
        add_lines = PRASNEM.get_added_lines_per_year(),
        hydro_parameters = PRASNEM.get_hydro_parameters(),
        solver::String = "HiGHS", # "HiGHS" or "Gurobi",
        sample_number_per_run::Int=100,
        default_horizon::Int=4, min_time_after_event::Int=4, 
        optimisation_window::Int=48, move_forward::Int=24,
        resilience_event::String="")

    # Run some checks on the input parameters
    if !(poe in [10, 50])
        @error "Invalid POE value: $poe. Please choose either 10 or 50."
        return
    end

    if !(solver in ["HiGHS", "Gurobi"])
        @error "Invalid solver: $solver. Please choose either \"HiGHS\" or \"Gurobi\"."
        return
    end

    if sample_number_per_run != 100
        @warn "The sample_number_per_run parameter is set to $sample_number_per_run. Please ensure that the adequacy assessment workflow has been run with this same value to generate the required results files, otherwise this function will not be able to read the results correctly."
    end

    # Make sure that if any default parameters are changed, the case_name parameter is set to avoid overwriting results files and to ensure that results can be found when reading the results in again
    if (regions_selected != collect(1:12)) || (DER_parameters != PRASNEM.get_DER_parameters()) || (add_lines != PRASNEM.get_added_lines_per_year()) || (hydro_parameters != PRASNEM.get_hydro_parameters()) || (optimisation_window != 48) || (move_forward != 24) || (genOpDetails != (uc=true, ramping=true, binary=false)) || (sample_number_per_run != 100) || (default_horizon != 4) || (min_time_after_event != 4)
        if case_name == "base"
            case_name = "_temp_case_$(round(Int, rand()*1000))"
            @warn "You have changed default parameters without providing a custom case name!\nTEMPORARY NAME: $case_name"
        else
            @info "Running adequacy assessment with custom parameters under case name: $case_name"
        end
    end

    @info "Running adequacy assessment with case $case_name:\nBuildout: $case_name_buildout\nPlanning year $target_year\nWeather reference trace: $reference_trace\nPOE: $poe\nISP scenario: $scenario\n Samples: $samples\nResilience event: $resilience_event"

    # File paths
    base_folder_pisp = joinpath(base_path, "pisp-datasets", case_name_buildout)
    base_folder_pras = joinpath(base_path, "pras-files", case_name)
    base_folder_schedules = joinpath(base_path, "schedules", case_name)
    base_folder_results = joinpath(base_path, "results", case_name)

    pisp_input_folder = joinpath(base_folder_pisp, "out-ref$(reference_trace)-poe$(poe)", "csv")
    timeseries_folder = "schedule-$(target_year)"
    pras_folder = joinpath(base_folder_pras, "out-ref$(reference_trace)-poe$(poe)",)
    if resilience_event != ""
        schedules_folder = joinpath(base_folder_schedules, "out-ref$(reference_trace)-poe$(poe)-$(resilience_event)", "ty$(target_year)")
        results_folder = joinpath(base_folder_results, "out-ref$(reference_trace)-poe$(poe)-$(resilience_event)", "ty$(target_year)")
    else
        schedules_folder = joinpath(base_folder_schedules, "out-ref$(reference_trace)-poe$(poe)", "ty$(target_year)")
        results_folder = joinpath(base_folder_results, "out-ref$(reference_trace)-poe$(poe)", "ty$(target_year)")
    end

    println(Threads.nthreads(), " threads available for parallel processing.")

    if !isdir(joinpath(pisp_input_folder, timeseries_folder))
        @error "PISP timeseries folder $(joinpath(pisp_input_folder, timeseries_folder)) not found. Please run PISP first to generate the required timeseries data."
    end

    # ==========================================================================
    # 1. Create PRAS files for the adequacy assessment
    # ==========================================================================
    start_dt = DateTime("$(target_year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
    end_dt = DateTime("$(target_year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")

    sys = PRASNEM.create_pras_system(start_dt, end_dt, pisp_input_folder, timeseries_folder; 
                                line_alias_included=add_lines[target_year], output_folder=pras_folder,
                                regions_selected=regions_selected,
                                hydro_parameters=hydro_parameters,
                                DER_parameters=DER_parameters, scenario=scenario)

    if resilience_event != ""
        resilience_event_folder = joinpath(base_path, "resilience", resilience_event)
        PRASNEM.applyGenHeatwaveDerating!(sys, resilience_event_folder)
        PRASNEM.applyLineHeatwaveDerating!(sys, resilience_event_folder)
    end

    # Approximating storage/genstorage outages by derating the available capacity by the FOR
    PRASNEM.updateStorageOutageDerating!(sys)

    # ==========================================================================
    # 2. Run the scheduling of the system
    # ==========================================================================
    # Create a copy of the system with reserves added for scheduling
    solver_instance = solver == "Gurobi" ? Gurobi.Optimizer() : HiGHS.Optimizer()

    m = SchedNEM.build_operation_model(sys; optimisation_window=optimisation_window, 
        move_forward=move_forward, input_folder=pisp_input_folder, 
        optimiser=solver_instance,
        genOpDetails=genOpDetails,
        hydro_parameters=hydro_parameters,
        DER_parameters=DER_parameters)
    
    res = SchedNEM.run_operation_model(m, sys;
        output_file=joinpath(schedules_folder, "$(target_year)-ref$(reference_trace)-poe$(poe)-s$(scenario).h5"),
        include_reserve_run=true)

    # ==========================================================================
    # 3. Evaluate the system against outages (PRAS)
    # ==========================================================================
    # Create a copy of the system to update with the expected dispatch and unit commitment for the adequacy assessment
    sys_assessment = deepcopy(sys)
    PRASNEM.updateDERExpectationDispatch!(sys_assessment, res)
    PRASNEM.updateStorageExpectationDispatch!(sys_assessment, res)
    PRASNEM.updateUnitCommitment!(sys_assessment, res)

    sys_derated = deepcopy(sys)
    PRASNEM.updateEnergyDerating!(sys_derated)

    resspecs = (ShortfallSamples(),)
    number_of_runs = ceil(Int, samples / sample_number_per_run)
    eens_expectation = zeros(number_of_runs)
    eens_derated = zeros(number_of_runs)
    eens_greedy = zeros(number_of_runs)

    for i in 1:1:number_of_runs
        if i % 10 == 0 || i == 1
            println("     Running adequacy assessments for sample batch $i / $number_of_runs...")
        end

        # Set the simulation parameters
        simspecs = SequentialMonteCarlo(samples=sample_number_per_run, seed=i)

        # Original PRAS shortfall samples (i.e. greedy)
        filename_greedy = joinpath(results_folder, "sf_samples_greedy_s$(scenario)_batch$(i).csv")
        if isfile(filename_greedy)
            @debug "File $filename_greedy already exists. Skipping greedy shortfall sampling for batch $i."
            eens_greedy[i] = SchedNEM.eensFromSfMatrix(filename_greedy)
        else
            sfsamples_greedy, = assess(sys, simspecs, resspecs...)
            SchedNEM.saveSfMatrix(sfsamples_greedy.shortfall, filename_greedy)
            eens_greedy[i] = sum(sfsamples_greedy.shortfall) ./ sample_number_per_run
        end

        # Derated storage shortfall samples
        filename_derated = joinpath(results_folder, "sf_samples_derated_s$(scenario)_batch$(i).csv")
        if isfile(filename_derated)
            @debug "File $filename_derated already exists. Skipping derated shortfall sampling for batch $i."
            eens_derated[i] = SchedNEM.eensFromSfMatrix(filename_derated)
        else
            sfsamples_derated, = assess(sys_derated, simspecs, resspecs...)
            SchedNEM.saveSfMatrix(sfsamples_derated.shortfall, filename_derated)
            eens_derated[i] = sum(sfsamples_derated.shortfall) ./ sample_number_per_run
        end

        # Expectation shortfall calculation
        filename = joinpath(results_folder, "sf_samples_expectation_s$(scenario)_batch$(i).csv")
        if isfile(filename)
            @debug "File $filename already exists. Skipping outage sampling for batch $i."
            eens_expectation[i] = SchedNEM.eensFromSfMatrix(filename)
            continue
        else
            sfsamples, = assess(sys_assessment, simspecs, resspecs...)
            SchedNEM.saveSfMatrix(sfsamples.shortfall, filename)
            eens_expectation[i] = sum(sfsamples.shortfall) ./ sample_number_per_run
        end       
        
    end
    @info "Finished outage sampling. \nAverage expected shortfall across all samples:\nGreedy: $(sum(eens_greedy) ./ number_of_runs)\nDerated: $(sum(eens_derated) ./ number_of_runs)\nExpectation: $(sum(eens_expectation) ./ number_of_runs)"

    # ==========================================================================
    # 4. Simulate system response in critical periods
    # ==========================================================================
    
    resspecs = (GeneratorAvailability(), LineAvailability())
    eens_reoptimised = zeros(number_of_runs)

    Threads.@threads for i in 1:1:number_of_runs
        println("     Running system response analysis for sample batch $i / $number_of_runs...")
        # Check if the reoptimised shortfall file already exists for this batch
        filename_output = joinpath(results_folder, "sf_samples_reoptimised_s$(scenario)_batch$(i).csv")
        if isfile(filename_output)
            @debug "File $filename_output already exists. Skipping system response analysis for batch $i."
            eens_reoptimised[i] = SchedNEM.eensFromSfMatrix(filename_output)
            continue
        end
        # If not, run the reoptimisation for this batch
        filename_input = joinpath(results_folder, "sf_samples_expectation_s$(scenario)_batch$(i).csv")

        # Calculate the (un)availability of generators and lines
        genAv, lineAv = assess(sys_assessment, SequentialMonteCarlo(samples=sample_number_per_run, seed=i), resspecs...)

        sf_new = SchedNEM.reoptimise(filename_input, sys, res, genAv.available, lineAv.available; 
                default_horizon=default_horizon, min_time_after_event=min_time_after_event, 
                DER_parameters=DER_parameters,
                hydro_parameters=hydro_parameters,
                optimisation_window=optimisation_window, move_forward=move_forward, 
                optimiser_name=solver, input_folder=pisp_input_folder,
                genOpDetails=genOpDetails);

        SchedNEM.saveSfMatrix(sf_new, filename_output)
        eens_reoptimised[i] = sum(sf_new) ./ sample_number_per_run
    end
    @info "Finished system response analysis."

    # ==========================================================================
    # 5. Calculate the final output and return
    # ==========================================================================
    total_final_ens = zeros(samples)
    for i in 1:number_of_runs
        start_idx = (i-1)*sample_number_per_run + 1
        end_idx = min(i*sample_number_per_run, samples)
        filename = joinpath(results_folder, "sf_samples_reoptimised_s$(scenario)_batch$(i).csv")
        total_final_ens[start_idx:end_idx] .= SchedNEM.ensFromSfMatrix(filename)
    end

    @info "Final expected shortfall (EENS) across all samples after reoptimisation: $(mean(total_final_ens)) +- $(std(total_final_ens) ./ sqrt(samples))."

    return total_final_ens
end
