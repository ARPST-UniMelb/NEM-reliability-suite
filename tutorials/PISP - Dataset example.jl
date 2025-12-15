using PISP

PISP.build_ISP24_datasets( 
                        downloadpath = normpath(@__DIR__, "../pisp-downloads"),    # Path where all files from AEMO's website will be downloaded and extracted
                        download_from_AEMO = true,                    # Whether to download files from AEMO's website
                        poe          = 10,                            # Probability of exceedance (POE) for demand: 10% or 50%
                        reftrace     = 4006,                          # Reference weather year trace: select among 2011 - 2023 or 4006 (trace for the ODP)
                        years        = [2030,2031,2032,2040,2050],    # Calendar years for which to build the time-varying schedules: select among 2025 - 2050
                        output_name  = "out",                         # Output folder name   
                        output_root  = normpath(@__DIR__, "../pisp-datasets"), # Output directory for datasets
                        write_csv    = true,                          # Whether to write CSV files
                        write_arrow  = false,                         # Whether to write Arrow files
                        scenarios    = [1,2,3]                         # Scenario numbers to include
                    )