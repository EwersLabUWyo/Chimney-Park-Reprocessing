from glue import qglue
from scipy import stats
import numpy as np
import pandas as pd
import xarray as xr


summary = xr.load_dataset('./summary_short.nc')
nf17_avg = summary.sel(SITE='NF17', STAT='Avg').to_pandas()

qglue(NF17 = nf17_avg)
