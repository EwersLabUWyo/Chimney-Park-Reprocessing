# Code to manage and upload NF_17_Burba data to the database.

# Author: Alex Fox
# Initial creation: 20220801

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

logger_sn = 23311
site = 'NF'

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables

    # logger metadata
    add_logger(logger_model='CR1000X', site=site, rep=2, logger_sn=logger_sn, shortname='NF_Burba_17m', comment='', con=con, show=show)

    # logger status
    # logger status
    status_cols = dict(vlogger='REAL', tlogger='REAL')
    status_units = dict(vlogger='V+1', tlogger='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model='CR1000X', rep=1, instr_sn=logger_sn,  comment='NF Burba 17m logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    burba_cols = dict(body='REAL', sparlow='REAL', sparmid='REAL', sparhigh='REAL', tref='REAL')
    burba_units = {k:'C+1' for k in burba_cols}
    add_instrument(shortname='burba', site=site, height=17, instr_model='Type-T TC', rep=1, instr_sn=False,  comment='', columns=burba_cols, units=burba_units, logger_sn=logger_sn,  con=con, show=show)

def load_burba30min(con, fns):
    # read in 30-minute flux summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(burba=re.compile("^Birba.*|^TC.*"),
             logger=re.compile("^PTemp.*"))
    table_names = ['burba_NF_17_1', 'status_NF_0_2']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "PTemp_C_Avg":'tlogger_avg',
                    "PTemp_C_Max":'tlogger_max',
                    "PTemp_C_Min":'tlogger_min',
                    "PTemp_C_Std":'tlogger_std',
                    "Birba_T_C_Avg(1)":'body_avg',
                    "Birba_T_C_Avg(2)":'sparlow_avg',
                    "Birba_T_C_Avg(3)":'sparmid_avg',
                    "Birba_T_C_Avg(4)":'sparhigh_avg',
                    "Birba_T_C_Max(1)":'body_max',
                    "Birba_T_C_Max(2)":'sparlow_max',
                    "Birba_T_C_Max(3)":'sparmid_max',
                    "Birba_T_C_Max(4)":'sparhigh_max',
                    "Birba_T_C_Min(1)":'body_min',
                    "Birba_T_C_Min(2)":'sparlow_min',
                    "Birba_T_C_Min(3)":'sparmid_min',
                    "Birba_T_C_Min(4)":'sparhigh_min',
                    "Birba_T_C_Std(1)":'body_std',
                    "Birba_T_C_Std(2)":'sparlow_std',
                    "Birba_T_C_Std(3)":'sparmid_std',
                    "Birba_T_C_Std(4)":'sparhigh_std',
                    "TC_MUX_Ref_T_C_Avg":'tref_avg',
                    "TC_MUX_Ref_T_C_Max":'tref_max',
                    "TC_MUX_Ref_T_C_Min":'tref_min',
                    "TC_MUX_Ref_T_C_Std":'tref_std'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def main():
    print(__name__, f'{site}_17_Burba_manager.py')

if __name__ == '__main__':
    main()