using Pkg
using PISP
using Dates

# ======================================== #
# Data file paths   
# ======================================== #
datapath    = "/Users/papablaza/git/jpisp-dev/PISP-dev.jl/data"; # Adjust this path as needed
ispdata19   = normpath(datapath, "2019InputandAssumptionsworkbookv13Dec19.xlsx");
ispdata24   = normpath(datapath, "2024 ISP Inputs and Assumptions workbook.xlsx");
profiledata = "/Users/papablaza/Library/CloudStorage/OneDrive-TheUniversityofMelbourne/Modelling/ISP24/Traces/";
outlookdata = "/Users/papablaza/Library/CloudStorage/OneDrive-TheUniversityofMelbourne/Modelling/ISP24/2024 ISP generation and storage outlook/Core";
outlookAEMO = "/Users/papablaza/Library/CloudStorage/OneDrive-TheUniversityofMelbourne/Modelling/ISP24/CapacityOutlook/CapacityOutlook_2024_ISP_melted_CDP14.xlsx";
vpp_cap     = "/Users/papablaza/Library/CloudStorage/OneDrive-TheUniversityofMelbourne/Modelling/ISP24/CapacityOutlook/Storage/StorageOutlook_Capacity.xlsx";
vpp_ene     = "/Users/papablaza/Library/CloudStorage/OneDrive-TheUniversityofMelbourne/Modelling/ISP24/CapacityOutlook/Storage/StorageOutlook_Energy.xlsx";
dsp_data    = "/Users/papablaza/Library/CloudStorage/OneDrive-TheUniversityofMelbourne/Modelling/ISP24/CapacityOutlook/2024ISP_DSP.xlsx";

# ================================================ #
#  Define dates and scenarios for data collection  #
# ================================================ #
# --> Example of problem table
# `name`          : Name of the study/case
# `scenario`      : Scenario id as per ID2SCE dictionary in PISPparameters.jl
# `weight`        : Weight of the scenario
# `problem_type`  : UC (unit commitment) - just for reference
# `dstart`        : Start date of the study [to generate only the corresponding traces]
# `dend`          : End date of the study [to generate only the corresponding traces]
# `tstep`         : Time step in minutes
# ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#  Row │ id     name                    scenario  weight   problem_type  dstart               dend                 tstep |
#      │ Int64  String                  Int64     Float64  String        DateTime             DateTime             Int64 |
# ─────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │     1  Progressive_Change_1           1      1.0  UC            2025-01-07T00:00:00  2025-01-13T23:00:00     60 |
#    2 │     2  Step_Change_2                  2      1.0  UC            2025-01-07T00:00:00  2025-01-13T23:00:00     60 |
#    3 │     3  Green_Energy_Exports_3         3      1.0  UC            2025-01-07T00:00:00  2025-01-13T23:00:00     60 |
# ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

function fill_problem_table_year(tc::PISPtimeConfig, year::Int)
    # Generate date blocks from 2025 to 2035, with periods starting 01/01 and 01/07
    date_blocks = PISP.OrderedDict()
    block_id = 1
    
    # First block: January 1 to June 30
    dstart_jan = DateTime(year, 1, 1, 0, 0, 0)
    dend_jan = DateTime(year, 6, 30, 23, 0, 0)
    date_blocks[block_id] = (dstart_jan, dend_jan, year)
    block_id += 1
    
    # Second block: July 1 to December 31
    dstart_jul = DateTime(year, 7, 1, 0, 0, 0)
    dend_jul = DateTime(year, 12, 31, 23, 0, 0)
    date_blocks[block_id] = (dstart_jul, dend_jul, year)
    block_id += 1

    # Create problem entries for each scenario and each date block
    row_id = 1
    for (block_num, (dstart, dend, year)) in date_blocks
        for sc in keys(PISP.ID2SCE)
            pbname = "$(PISP.ID2SCE[sc])_$(year)_$(month(dstart) == 1 ? "H1" : "H2")" # H1 for first half, H2 for second half   
            arr = [row_id, replace(pbname, " " => "_"), sc, 1, "UC", dstart, dend, 60]
            push!(tc.problem, arr)
            row_id += 1
        end
    end
end

for year in 2025:2035
    # Initialise DataFrames
    tc = PISPtimeConfig();
    ts = PISPtimeStatic();
    tv = PISPtimeVarying();
    fill_problem_table_year(tc, year);

    # ============================================ #
    # ======= Fill tables with information  ====== #
    # ============================================ #
    PISP.bus_table(ts); 
    PISP.dem_load(tc, ts, tv, profiledata);

    txdata = PISP.line_table(ts, tv, ispdata24);
    PISP.line_sched_table(tc, tv, txdata);
    PISP.line_invoptions(ts, ispdata24);

    SYNC4, GENERATORS, PS = PISP.generator_table(ts, ispdata19, ispdata24);
    PISP.gen_n_sched_table(tv, SYNC4, GENERATORS);
    PISP.gen_retirements(ts, tv);
    PISP.gen_pmax_distpv(tc, ts, tv, profiledata);
    PISP.gen_pmax_solar(tc, ts, tv, ispdata24, outlookdata, outlookAEMO, profiledata);
    PISP.gen_pmax_wind(tc, ts, tv, ispdata24, outlookdata, outlookAEMO, profiledata);
    SNOWY_GENS = PISP.gen_inflow_sched(ts, tv, tc, ispdata24);


    PISP.ess_tables(ts, tv, PS, ispdata24);
    PISP.ess_vpps(tc, ts, tv, vpp_cap, vpp_ene);
    PISP.ess_inflow_sched(ts, tv, tc, ispdata24, SNOWY_GENS);

    PISP.der_tables(ts);
    PISP.der_pred_sched(ts, tv, dsp_data);
    # ============================================ #
    # Write dataframes in CSV and Arrow formats 
    # ============================================ #
    # CSV format
    PISP.PISPwritedataCSV(tv, normpath(@__DIR__,"data/csv/schedule-$year"))  # Time-varying data (schedules)

    # Arrow format
    PISP.PISPwritedataArrow(tv, normpath(@__DIR__,"data/arrow/schedule-$year"))    # Time-varying data (schedules)
end