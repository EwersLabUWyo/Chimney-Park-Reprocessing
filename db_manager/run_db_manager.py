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
import NF_17_EC_manager as nf17ec
import NF_17_Burba_manager as nf17burba
import NF_3_EC_manager as nf3ec
import initialize

sandbox_dir = Path('/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/')
data_dir = Path('/Volumes/TempData/Bretfeld Mario/Chimney') / 'Data'
wd = sandbox_dir = sandbox_dir/'Alex Work'
db_fn = './cp.sqlite'#Path('/Users/waldinian/Documents/CPTest6.db')

with closing(sq.connect(db_fn)) as con:
    # initialize database
    # initialize.initialize(con)

    # # NF17 EC logger
    # nf17ec.initialize(con)
    # fns = list(data_dir.glob('BB-NF/EC/17m/20*/Converted/TOA5*Flux30Min.dat'))  # flux summaries
    # nf17ec.load_flux30min(con, fns)
    # fns = list(data_dir.glob('BB-NF/EC/17m/20*/Converted/TOA5*Met30Min.dat'))  # met summaries
    # nf17ec.load_met30min(con, fns)
    # fns = list(data_dir.glob('BB-NF/EC/17m/20*/Converted/TOA5*PRI*.dat'))  # pri
    # nf17ec.load_pri1min(con, fns)
    # fns = list(wd.glob('*/Chimney/EC Processing/BB-NF/Fast/17m/Converted/TOA5*10Hz*.dat'))  # fast flux
    # nf17ec.load_flux10Hz(con, fns)

    # # NF17 burba logger
    # nf17burba.initialize(con)
    # fns = list(data_dir.glob('BB-NF/Snow/20*/23311Birba*.dat'))
    # nf17burba.load_burba30min(con, fns)

    # NF3 EC logger
    # nf3ec.initialize(con)
    fns = list(data_dir.glob('BB-NF/EC/3m/20*/Converted/TOA5*Flux30Min.dat'))  # flux summaries
    nf3ec.load_flux30min(con, fns)
    # fns = list(data_dir.glob('BB-NF/EC/3m/20*/Converted/TOA5*Met30Min.dat'))  # met summaries
    # nf3ec.load_met30min(con, fns)
    # fns = list(data_dir.glob('BB-NF/EC/3m/20*/Converted/TOA5*PRI*.dat'))  # pri
    # nf3ec.load_flux10Hz(con, fns)

    # apply corrections
    # corrections.lw(con)







