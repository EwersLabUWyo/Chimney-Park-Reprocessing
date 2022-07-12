from glue import qglue
from scipy import stats
import numpy as np
import pandas as pd
import xarray as xr


summary = xr.load_dataset('./summary.nc')
nf17_avg = summary.sel(SITE='NF17', STAT='Avg').to_pandas().reset_index()
nf3_avg = summary.sel(SITE='NF3', STAT='Avg').to_pandas().reset_index()
uf3_avg = summary.sel(SITE='UF3', STAT='Avg').to_pandas().reset_index()
sf4_avg = summary.sel(SITE='SF4', STAT='Avg').to_pandas().reset_index()
sf7_avg = summary.sel(SITE='SF7', STAT='Avg').to_pandas().reset_index()

print(nf17_avg)

qglue(NF17 = nf17_avg, NF3 = nf3_avg, UF3 = uf3_avg, SF4 = sf4_avg, SF7 = sf7_avg)
