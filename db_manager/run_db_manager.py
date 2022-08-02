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

sandbox_dir = Path('/Volumes/TempData/Bretfeld Mario/Chimney-Park-Reprocessing-Sandbox/')
data_dir = Path('/Volumes/TempData/Bretfeld Mario/Chimney') / 'Data'
wd = sandbox_dir = sandbox_dir/'Alex Work'
db_fn = './cp.db'#Path('/Users/waldinian/Documents/CPTest6.db')

with closing(sq.connect(db_fn)) as con:
    # initialize database
    initialize.initialize(con)

    # NF17 EC logger
    nf17ec.initialize(con)
    fns = list(data_dir.glob('BB-NF/EC/17m/20*/Converted/TOA5*Flux30Min.dat'))  # flux summaries
    nf17ec.load_flux30min(con, fns)
    fns = list(data_dir.glob('BB-NF/EC/17m/20*/Converted/TOA5*Met30Min.dat'))  # met summaries
    nf17ec.load_met30min(con, fns)
    fns = list(data_dir.glob('BB-NF/EC/17m/20*/Converted/TOA5*PRI*.dat'))  # pri
    nf17ec.load_pri1min(con, fns)
    fns = list(wd.glob('*/Chimney/EC Processing/BB-NF/Fast/17m/Converted/TOA5*10Hz*.dat'))  # fast flux
    nf17ec.load_flux10Hz(con, fns)

    # NF17 burba logger
    nf17burba.initialize(con)
    fns = list(data_dir.glob('BB-NF/Snow/20*/23311Birba*.dat'))
    nf17burba.load_burba30min(con, fns)

    # NF3 EC logger
    nf3ec.initialize(con)
    fns = list(data_dir.glob('BB-NF/EC/3m/20*/Converted/TOA5*Flux30Min.dat'))  # flux summaries
    nf3ec.load_flux30min(con, fns)
    fns = list(data_dir.glob('BB-NF/EC/3m/20*/Converted/TOA5*Met30Min.dat'))  # met summaries
    nf3ec.load_met30min(con, fns)
    fns = list(data_dir.glob('BB-NF/EC/3m/20*/Converted/TOA5*PRI*.dat'))  # pri
    nf3ec.load_flux10Hz(con, fns)

    # NF3 Sap logger
    nfsap.initialize(con)
    fns = list(data_dir.glob('BB-NF/Sap Flow/*/BBNF_SapFlow*.dat'))
    nfsap.load_sap30min(con, fns)
    fns = list(data_dir.glob('BB-NF/Sap Flow/*/PRI*2m*.dat')) 
    nfsap.load_pri1min(con, fns)

    # NF3 Snow logger(s)
    nfsnow1.initialize(con)
    fns = list(data_dir.glob('BB-NF/Snow/*/3860Snow*.dat'))
    nfsnow1.load_snow30min(con, fns)
    fns = list(data_dir.glob('BB-NF/Snow/*/3860Table2*.dat'))
    nfsnow1.load_status30min(con, fns)

    # nfsnow2.initialize(con)
    fns = list(data_dir.glob('BB-NF/Snow/*/CR1000 NF Snow_Snow.dat'))
    fns += list(data_dir.glob('BB-NF/Snow/*/66463Data*.dat'))
    fns += list(data_dir.glob('BB-NF/Snow/*/nf_snow_Snow*.dat'))
    fns += list(data_dir.glob('BB-NF/Snow/*/66463Snowdepth_BBNF*.dat'))
    nfsnow2.load_snow30min(con, fns)
    fns = list(data_dir.glob('BB-NF/Snow/*/CR1000 NF Snow_Table*.dat'))
    fns += list(data_dir.glob('BB-NF/Snow/*/66463Table*.dat'))
    fns += list(data_dir.glob('BB-NF/Snow/*/nf_snow_Table*.dat'))
    nfsnow2.load_status30min(con, fns)

    # NF3 Soils logger(s)
    # an issue here is that we get like a bajillion tables, one for each instrument
    # a solution could be to take all replicate measurements and merge them
    nfsoils.initialize(con)
    fns = list(data_dir.glob('BB-NF/Soils/202*/Converted/TOA5_3864.CS616_30Min.dat'))
    nfsoils.load_cs30min(con, fns)
    fns = list(data_dir.glob('BB-NF/Soils/202*/Converted/TOA5_3864.HydraProbe_30Min.dat'))
    fns += list(data_dir.glob('BB-NF/Soils/202*/Converted/TOA5_3864.NRCS_zsStevens_30Min*.dat'))
    nfsoils.load_hydraprobes30min(con, fns)
    fns = list(data_dir.glob('BB-NF/Soils/202*/Converted/TOA5_3864.GsTs_30Min.dat'))
    nfsoils.load_gsts30min(con, fns)

    # example: make the following table:
    # ts instr_sn u_std u_avg
    # to get an instrument from this table, you can do

    # CREATE TABLE IF NOT EXISTS temp(instr_sn INT);
    # 
    # INSERT INTO 
    #     temp 
    # SELECT 
    #     instr_sn 
    # FROM 
    #     instruments_lu 
    # WHERE("site"="NF" 
    #       AND "height"="17" 
    #       AND "shortname"="sonic");
    #
    # SELECT
    #     ts, 
    #    u_avg)
    # FROM 
    #     u_table, 
    #     temp 
    # WHERE(u_table.instr_sn=temp.instr_sn); 

    

    # apply corrections
    # corrections.lw(con)







