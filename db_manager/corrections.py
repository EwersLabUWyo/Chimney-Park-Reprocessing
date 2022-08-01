import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

def lw(con):
    '''apply known lw correction'''
    tables = ['netrad_NF_17_1', 'netrad_NF_3_1']#, 'netrad_SF_7_1']
    for table in tables:
        lwdat = pd.read_sql(f'SELECT * FROM {table}', con)
        correction = lambda lw: lw + 5.67e-8*((lwdat['tb'] + 273.15)**4 - lwdat['tb']**4)
        lwdat['lwin'] = correction(lwdat['lwin'])
        lwdat['lwout'] = correction(lwdat['lwout'])
        # cannot fix std, must remove
        lwdat.loc[lwdat['stat'] == 'std', ['netrad', 'lwin', 'lwout']] = np.nan
        lwdat.set_index(['timestamp', 'idx', 'stat'], inplace=True)
        lwdat.to_sql(table, con, if_exists='replace')

        return