import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

def lw(con):
    '''apply known lw correction'''
    tables = ['netrad_NF_1700cm_1', 'netrad_NF_300cm_1']#, 'netrad_SF_7_1']
    for table in tables:
        lwdat = pd.read_sql(f'SELECT * FROM {table}', con)
        correction = lambda lw: lw + 5.67e-8*((lwdat['tb'] + 273.15)**4 - lwdat['tb']**4)

        if table=='netrad_NF_1700cm_1':
            suff = '_1_1_1'
        elif table=='netrad_NF_300cm_1':
            suff = '_2_1_1'
        lwdat[f'lwin{suff}'] = correction(lwdat[f'lwin{suff}'])
        lwdat[f'lwout{suff}'] = correction(lwdat[f'lwout{suff}'])
        # cannot fix std, must remove
        lwdat.loc[lwdat['stat'] == 'std', ['netrad', f'lwin{suff}', f'lwout{suff}']] = np.nan
        lwdat.set_index(['timestamp', 'idx', 'stat'], inplace=True)

        lwdat.to_sql(table, con, if_exists='replace')

        return