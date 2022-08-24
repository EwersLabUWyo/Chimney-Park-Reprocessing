import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

from utilities import *
import corrections
import NF_EC17_Manager as nf17ec
import NF_Burba17_Manager as nf17burba
import NF_EC3_Manager as nf3ec
import NF_Sap_Manager as nfsap
import NF_Snow_First_Manager as nfsnow1
import NF_Snow_Second_Manager as nfsnow2
import NF_Soils_Manager as nfsoils
import initialize

data_dir = Path('/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/Data-by-logger/')
# db_fn = ':memory:'
db_fn = Path('../CP.db')
db_fn.unlink()  


with closing(sq.connect(db_fn)) as con:
    # initialize database
    initialize.initialize(con)

    # NF17 EC logger
    print('Started NFEC17')
    nf17ec.initialize(con)
    fns = list(data_dir.glob('NFEC17m/Flux30Min/Converted/TOA5*.dat'))  # flux summaries
    nf17ec.load_flux30min(con, fns)
    fns = list(data_dir.glob('NFEC17m/Met30Min/Converted/TOA5*.dat'))  # met summaries
    nf17ec.load_met30min(con, fns)
    fns = list(data_dir.glob('NFEC17m/PRI/Converted/TOA5*.dat'))  # pri
    nf17ec.load_pri1min(con, fns)
    print('Searching for 10Hz files...')
    fns = list(data_dir.glob('NFEC17m/Flux10Hz/Converted/TOA5*.dat'))  # fast flux
    nf17ec.load_flux10Hz(con, fns)

    # NF17 burba logger
    print('Started NF17Burba')
    nf17burba.initialize(con)
    fns = list(data_dir.glob('NFBurba17m/Burba/233*.dat'))
    nf17burba.load_burba30min(con, fns)

    # NF3 EC logger
    print('Started NFEC3')
    nf3ec.initialize(con)
    fns = list(data_dir.glob('NFEC3m/Flux30Min/Converted/TOA5*.dat'))  # flux summaries
    nf3ec.load_flux30min(con, fns)
    fns = list(data_dir.glob('NFEC3m/Met30Min/Converted/TOA5*.dat'))  # met summaries
    nf3ec.load_met30min(con, fns)
    print('Searching for 10Hz files...')
    fns = list(data_dir.glob('NFEC3m/Flux10Hz/Converted/TOA5*.dat'))  # flux 10hz
    nf3ec.load_flux10Hz(con, fns)

    # NF3 Sap logger
    print('Started NFSap')
    nfsap.initialize(con)
    fns = list(data_dir.glob('NFSap/Sap/BBNF*.dat'))
    nfsap.load_sap30min(con, fns)
    fns = list(data_dir.glob('NFSap/Pri/PRI*.dat')) 
    nfsap.load_pri1min(con, fns)

    # NF3 Snow logger(s)
    print('Started NFSnow')
    nfsnow1.initialize(con)
    fns = list(data_dir.glob('NFSnow/Snow/3860Snow*.dat'))
    nfsnow1.load_snow30min(con, fns)
    fns = list(data_dir.glob('NFSnow/Batt/3860Table2*.dat'))
    nfsnow1.load_status30min(con, fns)

    # nfsnow2.initialize(con)
    fns = list(data_dir.glob('NFSnow/Snow/CR1000 NF Snow_Snow.dat'))
    fns += list(data_dir.glob('NFSnow/Snow/66463Data*.dat'))
    fns += list(data_dir.glob('NFSnow/Snow/nf_snow_Snow*.dat'))
    fns += list(data_dir.glob('NFSnow/Snow/66463Snowdepth_BBNF*.dat'))
    nfsnow2.load_snow30min(con, fns)
    fns = list(data_dir.glob('NFSnow/Batt/CR1000 NF Snow_Table*.dat'))
    fns += list(data_dir.glob('NFSnow/Batt/66463Table*.dat'))
    fns += list(data_dir.glob('NFSnow/Batt/nf_snow_Table*.dat'))
    nfsnow2.load_status30min(con, fns)

    # NF3 Soils logger(s)
    print('Started NFSoil')
    nfsoils.initialize(con)
    fns = list(data_dir.glob('NFSoil/TDR/Converted/TOA5*30M*.dat'))
    nfsoils.load_cs30min(con, fns)
    fns = list(data_dir.glob('NFSoil/Hydra/Converted/TOA5*30M*.dat'))
    fns += list(data_dir.glob('NFSoil/Stevens/202*/Converted/TOA5*30M*.dat'))
    nfsoils.load_hydraprobes30min(con, fns)
    fns = list(data_dir.glob('NFSoil/GsTs//Converted/TOA5*30M*.dat'))
    nfsoils.load_gsts30min(con, fns)





