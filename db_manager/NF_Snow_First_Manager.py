# Code to manage and upload NF 3m snowdepth and burba data to the database.

# Author: Alex Fox
# Initial creation: 20220802

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

logger_sn = 3860
site = 'NF'
logger_model = 'CR1000'

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables

    # logger metadata
    add_logger(logger_model=logger_model, site=site, rep=5, logger_sn=logger_sn, shortname='NF_Snow_1', comment='first snow logger at NF', con=con, show=show)

    # logger status
    status_cols = dict(vlogger_2_1_3='REAL')
    status_units = dict(vlogger_2_1_3='V+1')
    add_instrument(shortname='status', site=site, height=0, instr_model=logger_model, rep=5, instr_sn=logger_sn,  comment='first snow logger at NF', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # snow pack temperature probes
    snow_cols = {f'tsn_1_{i}_1':'REAL' for i in range(1, 16)}
    snow_cols.update({'tref_2_1_1':'REAL'})
    snow_units = {k:'C+1' for k in snow_cols}
    snow_units = {k:'C+1' for k in snow_cols}
    add_instrument(shortname='snowtemp', site=site, height=0, instr_model='Type-T TC', rep=1, instr_sn=False,  comment='Pole A', columns=snow_cols, units=snow_units, logger_sn=logger_sn,  con=con, show=show)
    snow_cols = {f'tsn_2_{i}_1':'REAL' for i in range(1, 16)}
    snow_cols.update({'tref_2_1_1':'REAL'})
    add_instrument(shortname='snowtemp', site=site, height=0, instr_model='Type-T TC', rep=2, instr_sn=False,  comment='Pole B', columns=snow_cols, units=snow_units, logger_sn=logger_sn,  con=con, show=show)

    # snow depth pinger
    depth_cols = dict(distto_1_1_1='REAL', tempcompdistto_1_1_1='REAL', dsnow_1_1_1='REAL', taref_1_1_1='REAL', quality_1_1_1='REAL')
    depth_units = dict(distto_1_1_1='m+1', tempcompdistto_1_1_1='m+1', dsnow_1_1_1='m+1', taref_1_1_1='C+1', quality_1_1_1='')
    add_instrument(shortname='snowdepth', site=site, height=2, instr_model='SR50A', rep=1, instr_sn=False,  comment='Snow depth pinger', columns=depth_cols, units=depth_units, logger_sn=logger_sn,  con=con, show=show)    

    return

def load_snow30min(con, fns):
    # read in 30-minute sap flux summaries into database

    # instructions for subsetting instruments using a regex filter for each instrument
    # sapflow sensors with bole temp
    rx = dict(poleA=re.compile('^Pack_C_Pole_A.*'),
              poleB=re.compile('^Pack_C_Pole_B.*'),
              depth=re.compile('^DT.*|^TCDT.*|^DBT.*|^TA.*|^Q.*'))

    table_names = ['snowtemp_NF_0cm_1', 'snowtemp_NF_0cm_2', 'snowdepth_NF_200cm_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "DT_Avg": 'distto_1_1_1-avg',
                    'Q_Avg': 'quality_1_1_1-avg',
                    "TCDT_Avg": 'tempcompdistto_1_1_1-avg',
                    "DBTCDT_Avg": 'dsnow_1_1_1-avg',
                    "TA_Ref_Avg": 'taref_1_1_1-avg',
                    "DT_Max": 'distto_1_1_1-max',
                    'Q_Max': 'quality_1_1_1-max',
                    "TCDT_Max": 'tempcompdistto_1_1_1-max',
                    "DBTCDT_Max": 'dsnow_1_1_1-max',
                    "TA_Ref_Max": 'taref_1_1_1-max',
                    "DT_Min": 'distto_1_1_1-min',
                    'Q_Min': 'quality_1_1_1-min',
                    "TCDT_Min": 'tempcompdistto_1_1_1-min',
                    "DBTCDT_Min": 'dsnow_1_1_1-min',
                    "TA_Ref_Min": 'taref_1_1_1-min',
                    "DT_Std": 'distto_1_1_1-std',
                    'Q_Std': 'quality_1_1_1-std',
                    "TCDT_Std": 'tempcompdistto_1_1_1-std',
                    "DBTCDT_Std": 'dsnow_1_1_1-std',
                    "TA_Ref_Std": 'taref_1_1_1-std'}
    renaming_dict.update({f'Pack_C_Pole_A_{stat}': f'tsn_1_1_1-{stat.lower()}' for stat in ['Avg', 'Max', 'Min', 'Std']})
    renaming_dict.update({f'Pack_C_Pole_A_Avg({i})': f'tsn_1_{i}_1-avg' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_A_Max({i})': f'tsn_1_{i}_1-max' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_A_Min({i})': f'tsn_1_{i}_1-min' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_A_Std({i})': f'tsn_1_{i}_1-std' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_{stat}': f'tsn_2_1_1-_{stat.lower()}' for stat in ['Avg', 'Max', 'Min', 'Std']})
    renaming_dict.update({f'Pack_C_Pole_B_Avg({i})': f'tsn_2_{i}_1-avg' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Max({i})': f'tsn_2_{i}_1-max' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Min({i})': f'tsn_2_{i}_1-min' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Std({i})': f'tsn_2_{i}_1-std' for i in range(1, 16)})

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_status30min(con, fns):
    # read in 1-minute pri data and add it to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(status=re.compile('BattV_Min'))

    table_names = ['status_NF_0cm_5']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            'BattV_Min':'vlogger_2_1_3-min'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def main():
    print(__name__, f'{site}_Snow1_manager.py')

if __name__ == '__main__':
    main()