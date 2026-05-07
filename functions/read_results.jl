"""
   read_results(target_year::Int, reference_years::Vector{Int}, poe::Int, samples::Int, base_path::String; 
      sample_number_per_run::Int=100, case::String="reoptimised")


"""
function read_results(target_year::Int,
   reference_years::Vector{Int}, 
   poes::Vector{Int},
   scenario::Int,
   samples::Int,
   base_path::String;
   sample_number_per_run::Int=100, case::String="reoptimised",
   var_confidence_level::Float64=0.95,
   poe_weights::Vector{Float64} = [0.304, 0.392, 0.304] # Weights for POE 10, 50, 90 based ESOO 2025
   )

   #@info("Reading results for target year $target_year, reference years $(join(reference_years, ", ")), poe $poe, case $case, with a total of $samples samples (i.e. $sample_number_per_run samples per batch)...")

   no_runs = floor(Int, samples / sample_number_per_run)
   eens = zeros(no_runs, length(reference_years), length(poes))
   lolh = zeros(no_runs, length(reference_years), length(poes))
   ens = zeros(no_runs * sample_number_per_run, length(reference_years), length(poes))

   poe_order = []
   for (j, reference_year) in enumerate(reference_years)
      for (k, poe) in enumerate(poes)
         if !(poe in poe_order)
            push!(poe_order, poe)
         end

         results_folder = joinpath(base_path, "out-ref$(reference_year)-poe$(poe)", "ty$(target_year)")
         
         Threads.@threads for i in 1:no_runs
            filename_output = joinpath(results_folder, "sf_samples_$(case)_s$(scenario)_batch$(i).csv")
            if !isfile(filename_output)
               @error "Output file $filename_output not found. Please run the adequacy assessment workflow first to generate the required results files."
            end
            eens[i, j, k] = SchedNEM.eensFromSfMatrix(filename_output)
            ens[((i-1)*sample_number_per_run + 1):(i*sample_number_per_run), j, k] = SchedNEM.ensFromSfMatrix(filename_output)
            lolh[i, j, k] = SchedNEM.lolhFromSfMatrix(filename_output)
         end
      end
   end

   VaR = quantile(reshape(ens, :), var_confidence_level)

   @info "Weights applied for POEs in order of $(poe_order): $(poe_weights)"
   poe_weight_factor = reshape([poe_weights[findfirst(==(poe), [10, 50, 90])] for poe in poe_order], 1, 1, :)

   r_total = (eue=mean(eens .* poe_weight_factor), lolh=mean(lolh .* poe_weight_factor), cvar_ens=mean((ens .* poe_weight_factor)[ens .>= VaR]))
   #@info("Results for target year $target_year, reference years $(join(reference_years, ", ")), poe $poe, case \"$case\", samples $samples:\n EUE: $(round(r_total.eue, sigdigits=2)) MWh/year\n LOLH: $(round(r_total.lolh, sigdigits=2)) hours/year\n CVaR: $(round(r_total.cvar_ens, sigdigits=2)) MWh/year")

   return (eue=mean(eens .* poe_weight_factor, dims=[1,3])[:], lolh=mean(lolh .* poe_weight_factor, dims=[1,3])[:], var_ens=VaR, cvar_ens=[mean((ens[:,j,:] .* poe_weight_factor)[:,1,:][findall(ens[:,j,:] .>= VaR)]) for j in 1:length(reference_years)])
end