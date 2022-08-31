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
    status_cols = dict(vlogger_1_1_2='REAL', tlogger_1_1_2='REAL')
    status_units = dict(vlogger_1_1_2='V+1', tlogger_1_1_2='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model='CR1000X', rep=2, instr_sn=logger_sn,  comment='NF Burba 17m logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    burba_cols = dict(body_1_1_1='REAL', sparlow_1_1_1='REAL', sparmid_1_1_1='REAL', sparhigh_1_1_1='REAL', tref_1_1_1='REAL')
    burba_units = {k:'C+1' for k in burba_cols}
    add_instrument(shortname='burba', site=site, height=17, instr_model='Type-T TC', rep=1, instr_sn=False,  comment='', columns=burba_cols, units=burba_units, logger_sn=logger_sn,  con=con, show=show)

def load_burba30min(con, fns):
    # read in 30-minute flux summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(burba=re.compile("^Birba.*|^TC.*"),
             logger=re.compile("^PTemp.*"))
    table_names = ['burba_NF_1700cm_1', 'status_NF_0cm_2']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "PTemp_C_Avg":'tlogger_1_1_2-avg',
                    "PTemp_C_Max":'tlogger_1_1_2-max',
                    "PTemp_C_Min":'tlogger_1_1_2-min',
                    "PTemp_C_Std":'tlogger_1_1_2-std',
                    "Birba_T_C_Avg(1)":'body_1_1_1-avg',
                    "Birba_T_C_Avg(2)":'sparlow_1_1_1-avg',
                    "Birba_T_C_Avg(3)":'sparmid_1_1_1-avg',
                    "Birba_T_C_Avg(4)":'sparhigh_1_1_1-avg',
                    "Birba_T_C_Max(1)":'body_1_1_1-max',
                    "Birba_T_C_Max(2)":'sparlow_1_1_1-max',
                    "Birba_T_C_Max(3)":'sparmid_1_1_1-max',
                    "Birba_T_C_Max(4)":'sparhigh_1_1_1-max',
                    "Birba_T_C_Min(1)":'body_1_1_1-min',
                    "Birba_T_C_Min(2)":'sparlow_1_1_1-min',
                    "Birba_T_C_Min(3)":'sparmid_1_1_1-min',
                    "Birba_T_C_Min(4)":'sparhigh_1_1_1-min',
                    "Birba_T_C_Std(1)":'body_1_1_1-std',
                    "Birba_T_C_Std(2)":'sparlow_1_1_1-std',
                    "Birba_T_C_Std(3)":'sparmid_1_1_1-std',
                    "Birba_T_C_Std(4)":'sparhigh_1_1_1-std',
                    "TC_MUX_Ref_T_C_Avg":'tref_1_1_1-avg',
                    "TC_MUX_Ref_T_C_Max":'tref_1_1_1-max',
                    "TC_MUX_Ref_T_C_Min":'tref_1_1_1-min',
                    "TC_MUX_Ref_T_C_Std":'tref_1_1_1-std'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def main():
    print(__name__, f'{site}_17_Burba_manager.py')

if __name__ == '__main__':
    main()