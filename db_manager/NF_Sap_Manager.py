# Code to manage and upload NF sapflow and 2m PRI data to the database.

# Author: Alex Fox
# Initial creation: 20220802

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

logger_sn = 3863
site = 'NF'
logger_model = 'CR1000'

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables

    # logger metadata
    add_logger(logger_model=logger_model, site=site, rep=4, logger_sn=logger_sn, shortname='NF_Sap_Pri_3', comment='sap flow and pri on the NF3m tower', con=con, show=show)

    # logger status
    status_cols = dict(vlogger='REAL', tlogger='REAL')
    status_units = dict(vlogger='V+1', tlogger='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model=logger_model, rep=4, instr_sn=logger_sn,  comment='NF3 sap logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # sap flow sensors without stem temp
    for rep in range(1, 11):
        sap_cols = dict(sapdvolt='REAL')
        sap_units = dict(sapdvolt='mV+1')
        add_instrument(shortname='picosap', site=site, height=1, instr_model='Sapflow', rep=rep, instr_sn=False,  comment='PICO', columns=sap_cols, units=sap_units, logger_sn=logger_sn,  con=con, show=show)

    # PRI sensors
    # PRI sensor(s?)
    pri_cols = dict(pri='REAL', indup='REAL', inddown='REAL', up532='REAL', down532='REAL', up570='REAL', down570='REAL')
    pri_units = dict(pri='', indup='', inddown='', up532='W+1m-2nm-1', down532='W+1m-2nm-1sr-1', up570='W+1m-2nm-1', down570='W+1m-2nm-1sr-1')
    add_instrument(shortname='pri', site=site, height=2,  instr_model='PRI', rep=1, instr_sn=False, comment='pri sensor(s?)', columns=pri_cols, units=pri_units, logger_sn=logger_sn, con=con, show=show)


def load_sap30min(con, fns):
    # read in 30-minute sap flux summaries into database

    # instructions for subsetting instruments using a regex filter for each instrument
    # sapflow sensors with bole temp
    rx = {f'picosap_{rep:02d}':re.compile(f"^BBNF\.PICO\.Stem_{rep:02d}_Avg") for rep in range(1, 11)}
    # logger status
    rx2 = dict(status=re.compile('^BattV_Min|^PTemp_C_Avg'))
    # mush into on dictionary
    rx.update(rx2)

    table_names = [f'picosap_NF_100cm_{rep}' for rep in range(1, 11)] + ['status_NF_0cm_4']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "BattV_Min": 'vlogger_min',
                    "PTemp_C_Avg": 'tlogger_avg',
                    "BBNF.PICO.Stem_01_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_02_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_03_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_04_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_05_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_06_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_07_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_08_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_09_Avg": 'sapdvolt_avg',
                    "BBNF.PICO.Stem_10_Avg": 'sapdvolt_avg'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_pri1min(con, fns):
    # read in 1-minute pri data and add it to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(pri_2=re.compile('.*_2m_.*'))

    table_names = ['pri_NF_200cm_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            "PRI_2m_Med":'pri_med',
            "Down_532_2m_Med":'down532_med',
            "Down_570_2m_Med":'down570_med',
            "Up_532_2m_Med":'up532_med',
            "Up_570_2m_Med":'up570_med',
            "Ind_down_2m_Med":'inddown_med',
            "Ind_up_2m_Med":'indup_med'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def main():
    print(__name__, f'{site}_Sap_manager.py')

if __name__ == '__main__':
    main()