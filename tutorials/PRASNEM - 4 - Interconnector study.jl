#%% Initialise all dependencies and packages
using Pkg
Pkg.develop(path="../PRASNEM.jl") # Adjust path as needed
Pkg.develop(path="../PISP.jl") # Adjust path as needed
using PISP
using PRASNEM
using PRAS
using Plots
using CSV
using DataFrames
using Dates
using Statistics

#%% ================= Creating a PRAS file with PRASNEM ===============================
year = 2030
start_dt = DateTime("$(year)-01-01 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt = DateTime("$(year)-12-31 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
input_folder = joinpath(pwd(), "..", "data", "nem12")
timeseries_folder = joinpath(input_folder, "schedule-$(year)")
# Add the lines to the simulation
base_lines = ["NL_23_INV3", "NL_67_INV19", "NL_86_INV30"] # CQ-GG, Sydney Ring North, HumeLink 
add_lines = vcat(base_lines, ["NL_109_INV35"]) # Marinus Link Stage 1 and CQ-GG (see alias and names in the Line.csv file)


sys = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder="", regions_selected=collect(1:12), line_alias_included=base_lines)
sys_marinus = PRASNEM.create_pras_system(start_dt, end_dt, input_folder, timeseries_folder; output_folder="", regions_selected=collect(1:12),  line_alias_included=add_lines)

#%% ================= Evaluating adequacy with PRAS ===============================
simspec = SequentialMonteCarlo(samples=200, seed=1);
resultspecs = (Shortfall(),Flow(),GeneratorStorageEnergy());

println("Running adequacy assessments...")
#sys.interfaces.limit_forward[6,:] .= 1e6
sf, flow, gse = assess(sys, simspec, resultspecs...)
sf_marinus, flow_marinus = assess(sys_marinus, simspec, resultspecs...);

println(" == Base case: ",NEUE(sf))
println(" == With Marinus Link: ",NEUE(sf_marinus))

#%% ================= Analysing the results ===============================

# Import the coordinate data for plotting
bus_data = CSV.read(joinpath(pwd(), "data", "csv", "Bus.csv"), DataFrame)
bus_coords= bus_data[:, [:id_bus, :longitude, :latitude]]

# Map the interfaces to the coordinates of the buses
interface_coords = zeros(Float64, length(sys.interfaces.regions_from), 4)
for i in 1:length(sys.interfaces.regions_from)
    bus_id = sys.interfaces.regions_from[i]
    row = findfirst(bus_data.id_bus .== bus_id)
    interface_coords[i, 1] = bus_data.longitude[row]
    interface_coords[i, 2] = bus_data.latitude[row]
    bus_id = sys.interfaces.regions_to[i]
    row = findfirst(bus_data.id_bus .== bus_id)
    interface_coords[i, 3] = bus_data.longitude[row]
    interface_coords[i, 4] = bus_data.latitude[row]
end

#%% Plot the expected unserved energy by bus for both systems
pyplot(dpi=300)

neue_by_bus = sum(sf.shortfall_mean, dims=2) ./ sum(sys.regions.load, dims=2) .* 100

xmin = minimum(bus_coords.longitude) - 1
xmax = maximum(bus_coords.longitude) + 1
ymin = minimum(bus_coords.latitude) - 1
ymax = maximum(bus_coords.latitude) + 1

p1 = plot([interface_coords[1, 1], interface_coords[1, 3]], 
          [interface_coords[1, 2], interface_coords[1, 4]], 
          color=:black, linewidth=4, label="")
for i in 2:size(interface_coords, 1)
    p1 = plot!([interface_coords[i, 1], interface_coords[i, 3]], 
          [interface_coords[i, 2], interface_coords[i, 4]], 
          color=:black, linewidth=4, label="")
end
p1 = scatter!(bus_coords.longitude, bus_coords.latitude,
    zcolor=neue_by_bus,
    clims=(0, 0.005), colorbar_title="NEUE [%]",
    color=cgrad([:white, :red], scale = :linear),
    markersize=13, label="", xlabel="", ylabel="", 
    #title="Expected unserved energy by bus - Base case", 
    grid=false, size=(350,500),
    xlims=(xmin, xmax), ylims=(ymin, ymax),
    colorbar=false,
    showaxis=false,
    background_color = :transparent)

neue_by_bus_marinus = sum(sf_marinus.shortfall_mean, dims=2) ./ sum(sys.regions.load, dims=2) .* 100

# Highlight Marinus Link
p2 = plot([interface_coords[10, 1], interface_coords[10, 3]], 
        [interface_coords[10, 2], interface_coords[10, 4]], 
        color=:blue, linewidth=7, label="")
# Highlight Gladstone Reinforcement
p2 = plot!([interface_coords[2, 1], interface_coords[2, 3]], 
        [interface_coords[2, 2], interface_coords[2, 4]], 
        color=:blue, linewidth=7, label="")
# Highlight Sydney Ring North
p2 = plot!([interface_coords[6, 1], interface_coords[6, 3]], 
        [interface_coords[6, 2], interface_coords[6, 4]], 
        color=:blue, linewidth=7, label="")

# Highlight HumeLink
p2 = plot!([interface_coords[7, 1], interface_coords[7, 3]], 
        [interface_coords[7, 2], interface_coords[7, 4]], 
        color=:blue, linewidth=7, label="")

# Add remaining interfaces
p2 = plot!([interface_coords[1, 1], interface_coords[1, 3]], 
          [interface_coords[1, 2], interface_coords[1, 4]], 
          color=:black, linewidth=4, label="")
for i in 2:size(interface_coords, 1)
    p2 = plot!([interface_coords[i, 1], interface_coords[i, 3]], 
          [interface_coords[i, 2], interface_coords[i, 4]], 
          color=:black, linewidth=4, label="")
end


p2 = scatter!(bus_coords.longitude, bus_coords.latitude,
    zcolor=neue_by_bus_marinus,
    clims=(0, 0.005), colorbar_title="NEUE [%]",
    color=cgrad([:white, :red], scale = :linear),
    markersize=13, label="", xlabel="", ylabel="",  
    grid=false, size=(350,500),
    xlims=(xmin, xmax), ylims=(ymin, ymax),
    #colorbar_ticks=(0:0.005:0.005, vcat(string.(0:0.005:0.005))),
    showaxis=false,
    background_color = :transparent)

plot(p1, p2, layout=(1,2), size=(500,400), )

savefig("./tutorials/figures/PRASNEM_4_maps.png")