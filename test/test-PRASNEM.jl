using PRASNEM
using Dates
PRAS = PRASNEM.PRAS

start_dt          = DateTime("2025-01-07 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
end_dt            = DateTime("2025-01-13 23:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
input_folder      = joinpath(pwd(), "data", "csv")
output_folder     = joinpath(pwd(), "out")
timeseries_folder = joinpath(input_folder, "schedule-1w")

# ================================== #
# Creation of system files
# ================================== #
sys_sc1 = PRASNEM.create_pras_file(start_dt, end_dt, input_folder, output_folder, timeseries_folder; regions_selected=collect(1:12), scenario = 1)
sys_sc2 = PRASNEM.create_pras_file(start_dt, end_dt, input_folder, output_folder, timeseries_folder; regions_selected=collect(1:12), scenario = 2)
sys_sc3 = PRASNEM.create_pras_file(start_dt, end_dt, input_folder, output_folder, timeseries_folder; regions_selected=collect(1:12), scenario = 3)

PRASNEM.run_pras_study(sys_sc1, 1000)
PRASNEM.run_pras_study(sys_sc2, 1000)
PRASNEM.run_pras_study(sys_sc3, 1000)

sf, = PRAS.assess(sys_sc1,PRAS.SequentialMonteCarlo(samples=100),PRAS.Shortfall())