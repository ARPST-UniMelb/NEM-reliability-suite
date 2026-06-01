


using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));

#%%

ref_poe_scen_sets = [(ref, poe, 2) for ref in collect(2011:2023) for poe in [10, 50]]

r = create_summary("baseVPP"; 
   samples=100, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   apply_demand_weights=true, 
   )

r_DR = create_summary("baseDR"; 
   samples=100, 
   storage_case="reoptimised", 
   ref_poe_scen_sets=ref_poe_scen_sets,
   apply_demand_weights=true, 
   )

#%%

#%% ========================================================================================
samples = 100
target_years = [2025, 2030, 2035, 2040]
duration_bins = collect(0:1:maximum(r.events.duration_hrs) + 1)
magnitude_bins = collect(0:0.002:(maximum(r.events.magnitude_mwh ./ r.events.total_load_mwh) .* 1e3 + 0.001))

total_samples = length(ref_poe_scen_sets) * samples


# 2025

function make_duration_use_plot_DER(ev_reoptimised, ev_DER, target_year)

   duration_bins = collect(0:1:25)
   magnitude_bins = collect(0:0.002:0.125)

   total_samples = length(ref_poe_scen_sets) * samples

   xlims_overall = (0, 26)# (0, maximum(r.events.duration_hrs) + 1)
   ylims_overall = (-0.002, 0.125) #maximum(r.events.magnitude_mwh ./ r.events.total_load_mw) .* 1e3 + 0.001)

   yticks_overall = 0:0.025:ylims_overall[2]
   xticks_overall = 0:3:xlims_overall[2]

   hist_ticks = (0:total_samples/10:1600, string.(0:0.1:1.0))
   hist_lims = (-10, 1600)
   scatter_labs = ["Base (incl. 100% VPP)", "+ 100% DSP"]
   kwargs_scatter = ( markersize=3, alpha=0.5, xlabel= "Duration [hrs]",
      ylabel="Event USE [%]", xlims=xlims_overall, ylims=ylims_overall,
      yticks=yticks_overall, xticks=xticks_overall)
   kwargs_duration_hist = (label="", lw=2, xlims=xlims_overall, xticks=(xticks_overall,""),
      ylabel="Events [#/yr]",
      yticks=hist_ticks, ylims=hist_lims)
   kwargs_use_hist = (label="", lw=2, c=[3 4], xrotation=-90, ylims=ylims_overall,
      xlabel="Events [#/yr]", yticks=(yticks_overall,""), xticks=hist_ticks, xlims=hist_lims)

   ev_reoptimised = copy(ev_reoptimised.events[ev_reoptimised.events.year .== target_year, :])
   ev_DER = copy(ev_DER.events[ev_DER.events.year .== target_year, :])

   # Scatter plot
   p_scatter = scatter(ev_reoptimised.duration_hrs, ev_reoptimised.magnitude_mwh ./ ev_reoptimised.total_load_mwh .* 1e3, c=3, label=scatter_labs[1]; kwargs_scatter...)
   p_scatter = scatter!(ev_DER.duration_hrs, ev_DER.magnitude_mwh ./ ev_DER.total_load_mwh .* 1e3, c=4, label=scatter_labs[2]; kwargs_scatter...)

   # Duration histogram
   p_duration_hist = stephist(ev_reoptimised.duration_hrs, bins=duration_bins, label="", c=3; kwargs_duration_hist...)
   p_duration_hist = stephist!(ev_DER.duration_hrs, bins=duration_bins, c=4; kwargs_duration_hist...)

  # USE histogram
   hist_values = zeros(2,length(magnitude_bins) - 1)
   hist_values[1,:] = [sum((ev_reoptimised.magnitude_mwh ./ ev_reoptimised.total_load_mwh .* 1e3) .>= x) for x in magnitude_bins][1:end-1] .- [sum((ev_reoptimised.magnitude_mwh ./ ev_reoptimised.total_load_mwh .* 1e3) .>= x) for x in magnitude_bins][2:end]
   hist_values[2,:] = [sum((ev_DER.magnitude_mwh ./ ev_DER.total_load_mwh .* 1e3) .>= x) for x in magnitude_bins][1:end-1] .- [sum((ev_DER.magnitude_mwh ./ ev_DER.total_load_mwh .* 1e3) .>= x) for x in magnitude_bins][2:end]

   y = repeat(repeat(magnitude_bins[1:end-1], inner=2), 1, 2)
   x = hcat(zeros(2,1), repeat(hist_values, inner=(1,2))[:,1:end-1])
   p_use_hist = plot(x', y, label=""; kwargs_use_hist...)

   l = @layout [
    a{0.2h} _ ;
    b{0.8h,0.8w} c{0.2w} 
   ]
   return plot(p_duration_hist,p_scatter,p_use_hist, layout=l, 
      link=:both, suptitle="$target_year", size=(600, 470), dpi=500,
      bottommargin=1Plots.mm)
end



for ty in target_years
   p = make_duration_use_plot_DER(r, r_DR, ty)
   savefig(p, joinpath(@__DIR__, "figures", "1-scatter_$(ty)_DER.png"))
end

