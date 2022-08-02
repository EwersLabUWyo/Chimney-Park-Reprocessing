# Code to manage and upload NF 3m snowdepth and burba data to the database.

# Author: Alex Fox
# Initial creation: 20220802

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

logger_sn = 66463
site = 'NF'
logger_model = 'CR1000'

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables

    # logger metadata
    add_logger(logger_model=logger_model, site=site, rep=6, logger_sn=logger_sn, shortname='NF_Snow_2', comment='second snow logger at NF, replaces the first one.', con=con, show=show)

    # logger status
    status_cols = dict(vlogger='REAL')
    status_units = dict(vlogger='V+1')
    add_instrument(shortname='status', site=site, height=0, instr_model=logger_model, rep=6, instr_sn=logger_sn,  comment='second snow logger at NF', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # snow pack temperature probes
    snow_cols = {f'tsn{i}':'REAL' for i in range(1, 16)}
    snow_cols.update({'tref':'REAL'})
    snow_units = {k:'C+1' for k in snow_cols}
    add_instrument(shortname='snowtemp', site=site, height=0, instr_model='Type-T TC', rep=3, instr_sn=False,  comment='Pole A, replacement logger', columns=snow_cols, units=snow_units, logger_sn=logger_sn,  con=con, show=show)
    add_instrument(shortname='snowtemp', site=site, height=0, instr_model='Type-T TC', rep=4, instr_sn=False,  comment='Pole B, replacement logger', columns=snow_cols, units=snow_units, logger_sn=logger_sn,  con=con, show=show)

    # snow depth pinger
    depth_cols = dict(distto='REAL', tempcompdistto='REAL', dsnow='REAL', tref='REAL', quality='REAL')
    depth_units = dict(distto='m+1', tempcompdistto='m+1', dsnow='m+1', tref='C+1', quality='')
    add_instrument(shortname='snowdepth', site=site, height=2, instr_model='SR50A', rep=2, instr_sn=False,  comment='Snow depth pinger', columns=depth_cols, units=depth_units, logger_sn=logger_sn,  con=con, show=show)    

    # burba cols
    burba_cols = dict(body='REAL', sparlow='REAL', sparmid='REAL', sparhigh='REAL', tref='REAL')
    burba_units = {k:'C+1' for k in burba_cols}
    add_instrument(shortname='burba', site=site, height=3, instr_model='Type-T TC', rep=1, instr_sn=False,  comment='', columns=burba_cols, units=burba_units, logger_sn=logger_sn,  con=con, show=show)

    return

def load_snow30min(con, fns):
    # read in 30-minute sap flux summaries into database

    # instructions for subsetting instruments using a regex filter for each instrument
    # sapflow sensors with bole temp
    rx = dict(poleA=re.compile('^PoleA.*|^MUX.*|^Pack_C_Pole_A.*'),
              poleB=re.compile('^PoleB.*|^MUX.*|^Pack_C_Pole_B.*'),
              depth=re.compile('^DistTo.*|^TempComp.*|^Dep.*|^PingerAir.*|^Qual.*|^DT.*|^TCDT.*|^DBT.*|^TA.*|^Q.*'),
              burba=re.compile("^Birba.*|^MUX.*"),)

    table_names = ['snowtemp_NF_0cm_1', 'snowtemp_NF_0cm_2', 'snowdepth_NF_200cm_1', 'burba_NF_300cm_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    # so, this setup is a bit of a mess. Names in the file changed several times, 
    # and the datalogger was swapped out at one point for some reason.
    # we have to account for 3 different naming conventions.
    
    # snow pinger convention 1
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "DistTo_Avg": 'distto_avg',
                    'Qual_Avg': 'quality_avg',
                    "TempCompDistTo_Avg": 'tempcompdistto_avg',
                    "Depth_Avg": 'dsnow_avg',
                    "PingerAirtemp_Avg": 'tref_avg',
                    "DistTo_Max": 'distto_max',
                    'Qual_Max': 'quality_max',
                    "TempCompDistTo_Max": 'tempcompdistto_max',
                    "Depth_Max": 'dsnow_max',
                    "PingerAirtemp_Max": 'tref_max',
                    "DistTo_Min": 'distto_min',
                    'Qual_Min': 'quality_min',
                    "TempCompDistTo_Min": 'tempcompdistto_min',
                    "Depth_Min": 'dsnow_min',
                    "PingerAirtemp_Min": 'tref_min',
                    "DistTo_Std": 'distto_std',
                    'Qual_Std': 'quality_std',
                    "TempCompDistTo_Std": 'tempcompdistto_std',
                    "Depth_Std": 'dsnow_std',
                    "PingerAirtemp_Std": 'tref_std'}
    # snow pinger convention 2
    renaming_dict.update({"DT_Avg": 'distto_avg',
                        'Q_Avg': 'quality_avg',
                        "TCDT_Avg": 'tempcompdistto_avg',
                        "DBTCDT_Avg": 'dsnow_avg',
                        "TA_Ref_Avg": 'tref_avg',
                        "DT_Max": 'distto_max',
                        'Q_Max': 'quality_max',
                        "TCDT_Max": 'tempcompdistto_max',
                        "DBTCDT_Max": 'dsnow_max',
                        "TA_Ref_Max": 'tref_max',
                        "DT_Min": 'distto_min',
                        'Q_Min': 'quality_min',
                        "TCDT_Min": 'tempcompdistto_min',
                        "DBTCDT_Min": 'dsnow_min',
                        "TA_Ref_Min": 'tref_min',
                        "DT_Std": 'distto_std',
                        'Q_Std': 'quality_std',
                        "TCDT_Std": 'tempcompdistto_std',
                        "DBTCDT_Std": 'dsnow_std',
                        "TA_Ref_Std": 'tref_std'})
    # snowpack thermocouples convention 1
    renaming_dict.update({f'PoleA_Avg({i})': f'tsn{i}_avg' for i in range(1, 16)})
    renaming_dict.update({f'PoleA_Max({i})': f'tsn{i}_max' for i in range(1, 16)})
    renaming_dict.update({f'PoleA_Min({i})': f'tsn{i}_min' for i in range(1, 16)})
    renaming_dict.update({f'PoleA_Std({i})': f'tsn{i}_std' for i in range(1, 16)})
    renaming_dict.update({f'PoleB_Avg({i})': f'tsn{i}_avg' for i in range(1, 16)})
    renaming_dict.update({f'PoleB_Max({i})': f'tsn{i}_max' for i in range(1, 16)})
    renaming_dict.update({f'PoleB_Min({i})': f'tsn{i}_min' for i in range(1, 16)})
    renaming_dict.update({f'PoleB_Std({i})': f'tsn{i}_std' for i in range(1, 16)})
    # snowpack thermovouples convention 2
    renaming_dict.update({f'Pack_C_Pole_A_Avg({i})': f'tsn{i}_avg' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_A_Max({i})': f'tsn{i}_max' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_A_Min({i})': f'tsn{i}_min' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_A_Std({i})': f'tsn{i}_std' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Avg({i})': f'tsn{i}_avg' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Max({i})': f'tsn{i}_max' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Min({i})': f'tsn{i}_min' for i in range(1, 16)})
    renaming_dict.update({f'Pack_C_Pole_B_Std({i})': f'tsn{i}_std' for i in range(1, 16)})
    # snowpack thermocouples conventions 1.5 and 2.5
    renaming_dict.update({f'PoleA_{stat}': f'tsn1_{stat.lower()}' for stat in ['Avg', 'Max', 'Min', 'Std']})
    renaming_dict.update({f'PackB_{stat}': f'tsn1_{stat.lower()}' for stat in ['Avg', 'Max', 'Min', 'Std']})
    renaming_dict.update({f'Pack_C_Pole_A_{stat}': f'tsn1_{stat.lower()}' for stat in ['Avg', 'Max', 'Min', 'Std']})
    renaming_dict.update({f'Pack_C_Pole_B_{stat}': f'tsn1_{stat.lower()}' for stat in ['Avg', 'Max', 'Min', 'Std']})
    # thermocouple reference
    renaming_dict.update({'MUXTemp_Avg':'tref_avg', 'MUXTemp_Max':'tref_max', 'MUXTemp_Min':'tref_min', 'MUXTemp_Std':'tref_std'})
    renaming_dict.update({"Birba_Avg(1)":'body_avg',
                        "Birba_Avg(2)":'sparlow_avg',
                        "Birba_Avg(3)":'sparmid_avg',
                        "Birba_Avg(4)":'sparhigh_avg',
                        "Birba_Max(1)":'body_max',
                        "Birba_Max(2)":'sparlow_max',
                        "Birba_Max(3)":'sparmid_max',
                        "Birba_Max(4)":'sparhigh_max',
                        "Birba_Min(1)":'body_min',
                        "Birba_Min(2)":'sparlow_min',
                        "Birba_Min(3)":'sparmid_min',
                        "Birba_Min(4)":'sparhigh_min',
                        "Birba_Std(1)":'body_std',
                        "Birba_Std(2)":'sparlow_std',
                        "Birba_Std(3)":'sparmid_std',
                        "Birba_Std(4)":'sparhigh_std'})

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_status30min(con, fns):
    # read in 1-minute pri data and add it to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(status=re.compile('BattV_Min'))

    table_names = ['status_NF_0cm_6']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            'BattV_Min':'vlogger_min'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return


def main():
    print(__name__, f'{site}_Snow1_manager.py')

if __name__ == '__main__':
    main()