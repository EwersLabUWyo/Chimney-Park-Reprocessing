from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mpl_dates
import xarray as xr

from EC_Processing_Engine import *

converted_dirs = ["/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Alex Work/Bad/Chimney/EC Processing/BB-NF/Fast/17m/Converted",
                  "/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Alex Work/Bad/Chimney/EC Processing/BB-NF/Fast/3m/Converted",
                  "/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Alex Work/Bad/Chimney/EC Processing/BB-SF/Fast/4m/Converted",
                 "/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Alex Work/Bad/Chimney/EC Processing/BB-SF/Fast/7m/Converted",
                 "/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Alex Work/Bad/Chimney/EC Processing/BB-UF/Fast/3m/Converted"]
metadata_fn = "/Volumes/TempData/Bretfeld Mario/Chimney/Site Information/Changelog_Alex_fieldnotes.xlsx"
file_length = 30
acq_freq = 10
start_time = "2021-03-12 00:00"
end_time = "2021-03-12 04:00"
site_names = ["NF17", "NF3", "SF4", "SF7", "UF3"]
out_dir = "/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Alex Work/Bad/Chimney/EC Processing/Combined/Fast/Standardized"

engine = fast_processing_engine(converted_dirs, file_length, acq_freq, start_time, end_time, site_names, out_dir)

engine.process_fast_files()

summary = engine.summary
summary.to_netcdf('./summary.nc')

