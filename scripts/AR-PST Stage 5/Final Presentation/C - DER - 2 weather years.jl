



using Plots
using Dates
using DataFrames
using PRAS
using SchedNEM
using StatsPlots

include(joinpath(@__DIR__, "../../../functions/all_functions.jl"));

#%%


ref_poe_scen_sets = [(2011, 10, 2), (2019, 50, 2)] # (2011,10,2),
target_years = collect(2025:5:2040)
samples = 500
storage_case = "reoptimised"

r_supply = create_summary("base";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_EV = create_summary("baseEV";
   samples=samples, 
   storage_case=storage_case, 
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

r_VPP = create_summary("baseVPP";
   samples=samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )


r_DRnoVPP = create_summary("baseDRnoVPP";
   samples=100,#samples, 
   storage_case=storage_case,  
   ref_poe_scen_sets=ref_poe_scen_sets,
   resilience_events=[],
   )

#%%

yl = [5, 50, 500, 5000]
yl_lab = string.(yl ./ 500)

kwargs = (dpi=500, 
   yscale=:log10,
   yticks =(yl, yl_lab),
   ylims=(0.9, 5000), 
   legend=:topright,
   bins=collect(0:1:75),
   xticks=0:5:75,
   ylabel="Number of events [#/yr]")

e11 = r_supply.events[(r_supply.events.year .== 2040) .&& (r_supply.events.ref_year .== 2011), :]
e_EV11 = r_EV.events[(r_EV.events.year .== 2040) .&& (r_EV.events.ref_year .== 2011), :]
e_VPP11 = r_VPP.events[(r_VPP.events.year .== 2040) .&& (r_VPP.events.ref_year .== 2011), :]
e_DR11 = r_DRnoVPP.events[(r_DRnoVPP.events.year .== 2040) .&& (r_DRnoVPP.events.ref_year .== 2011), :]

e19 = r_supply.events[(r_supply.events.year .== 2040) .&& (r_supply.events.ref_year .== 2019), :]
e_EV19 = r_EV.events[(r_EV.events.year .== 2040) .&& (r_EV.events.ref_year .== 2019), :]
e_VPP19 = r_VPP.events[(r_VPP.events.year .== 2040) .&& (r_VPP.events.ref_year .== 2019), :]
e_DR19 = r_DRnoVPP.events[(r_DRnoVPP.events.year .== 2040) .&& (r_DRnoVPP.events.ref_year .== 2019), :]


p1 = histogram(e11.magnitude_mwh ./ 1e3,  label="", alpha=0.1,
   c=:grey, lw=0, title=""; kwargs...)
p1 = stephist!(e11.magnitude_mwh ./ 1e3, label="Supply case", alpha=1.0, lw=3,
   c=:grey; kwargs...)

p2 = histogram(e19.magnitude_mwh ./ 1e3,  label="", alpha=0.1,
   c=:grey, lw=0, title=""; kwargs...)
p2 = stephist!(e19.magnitude_mwh ./ 1e3, label="Supply case", alpha=1.0, lw=3,
   xlabel="Recorded USE per load shedding event [GWh]",
   c=:grey; kwargs...)

plot(p1, p2, layout=(2,1), size=(800,600), bottommargin=5Plots.mm, leftmargin=5Plots.mm, legend=false)
savefig(joinpath(@__DIR__, "figures", "C - DER - Hist - 0.png"))

p1 = histogram!(p1, e_DR11.magnitude_mwh ./ 1e3, label="", alpha=0.1, c=:black,
    lw=0; kwargs...)
p1 = stephist!(p1, e_DR11.magnitude_mwh ./ 1e3, label="DSP",  c=:black, lw=3; kwargs...)
p2 = histogram!(p2, e_DR19.magnitude_mwh ./ 1e3, label="", alpha=0.1, c=:black,
    lw=0; kwargs...)
p2 = stephist!(p2, e_DR19.magnitude_mwh ./ 1e3, label="DSP",  c=:black, lw=3,
   ; kwargs...)
plot(p1, p2, layout=(2,1), size=(800,600), bottommargin=5Plots.mm, leftmargin=5Plots.mm,
   legend=false)
savefig(joinpath(@__DIR__, "figures", "C - DER - Hist - 1.png"))

p1 = histogram!(p1, e_EV11.magnitude_mwh ./ 1e3, label="", alpha=0.1, c=1, palette=:batlow10,
    lw=0; kwargs...)
p1 = stephist!(p1, e_EV11.magnitude_mwh ./ 1e3, label="EV", c=1, palette=:batlow10, lw=3; kwargs...)

p2 = histogram!(p2, e_EV19.magnitude_mwh ./ 1e3, label="", alpha=0.0, c=1, palette=:batlow10,
    lw=0; kwargs...)
p2 = stephist!(p2, e_EV19.magnitude_mwh ./ 1e3, label="EV", c=1, palette=:batlow10, lw=3; kwargs...)
p2 = histogram!(p2, e_EV19.magnitude_mwh ./ 1e3, label="", alpha=0.1, c=1, palette=:batlow10,
    lw=0; kwargs...)
p2 = stephist!(p2, e_EV19.magnitude_mwh ./ 1e3, label="", c=1, palette=:batlow10, lw=3; kwargs...)

plot(p1, p2, layout=(2,1), size=(800,600), bottommargin=5Plots.mm, leftmargin=5Plots.mm, legend=false)
savefig(joinpath(@__DIR__, "figures", "C - DER - Hist - 2.png"))


p1 = histogram!(p1, e_VPP11.magnitude_mwh ./ 1e3,  label="", alpha=0.1,c=5, palette=:batlow10,
    lw=0; kwargs...)
p1 = stephist!(p1, e_VPP11.magnitude_mwh ./ 1e3,  label="VPP", c=5, palette=:batlow10, lw=3; kwargs...)
p2 = histogram!(p2, e_VPP19.magnitude_mwh ./ 1e3,  label="", alpha=0.1,c=5, palette=:batlow10,
    lw=0; kwargs...)
p2 = stephist!(p2, e_VPP19.magnitude_mwh ./ 1e3,  label="VPP", c=5, palette=:batlow10, lw=3; kwargs...)

plot(p1, p2, layout=(2,1), size=(800,600), bottommargin=5Plots.mm, leftmargin=5Plots.mm, legend=false)
savefig(joinpath(@__DIR__, "figures", "C - DER - Hist - 3.png"))


plot(p1, p2, layout=(2,1), size=(800,600), bottommargin=5Plots.mm, leftmargin=5Plots.mm, legend=:outerright)
savefig(joinpath(@__DIR__, "figures", "C - DER - Hist - legend.png"))



