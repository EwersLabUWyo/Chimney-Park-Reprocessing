# Code to manage and upload NF_3_EC data to the database.

# Author: Alex Fox
# Initial creation: 20220801

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables
    logger_sn = 10365
    site = 'NF'

    # logger metadata
    add_logger(site=site, logger_model='CR3000', rep=3, logger_sn=logger_sn, shortname='NF_EC_3m', con=con, show=show, comment='')

    # logger status
    status_cols = dict(vlogger_2_1_1='REAL', tlogger_2_1_1='REAL')
    status_units = dict(vlogger_2_1_1='V+1', tlogger_2_1_1='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model='CR3000', rep=3, instr_sn=logger_sn,  comment='NF EC 3m logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # 3m sonic
    sonic_cols = dict(u_2_1_1='REAL', v_2_1_1='REAL', w_2_1_1='REAL', tsonic_2_1_1='REAL', diagsonic_2_1_1='INT')
    sonic_units = dict(u_2_1_1='m+1s-1', v_2_1_1='m+1s-1', w_2_1_1='m+1s-1', tsonic_2_1_1='C+1', diagsonic_2_1_1='')
    add_instrument(shortname='sonic',   site=site, height=3, instr_model='CSAT3B',   rep=1, instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=logger_sn,  con=con, show=show)
    
    # 3m IRGA
    irga_cols = dict(co2_2_1_1='REAL', h2o_2_1_1='REAL', pa_2_1_1='REAL', diagirga_2_1_1='INT')
    irga_units = dict(co2_2_1_1='mg+1m-3', h2o_2_1_1='g+1m-3', pa_2_1_1='kPa+1', diagirga_2_1_1='')
    add_instrument(shortname='irga',    site=site, height=3, instr_model='LI7500',  rep=1, instr_sn=False, comment='', columns=irga_cols,    units=irga_units,    logger_sn=logger_sn,  con=con, show=show)
    
    # 3m CNR1
    netrad_cols = dict(swin_2_1_1='REAL', swout_2_1_1='REAL', lwin_2_1_1='REAL', lwout_2_1_1='REAL', rn_2_1_1='REAL', tb_2_1_1='REAL', alb_2_1_1='REAL')
    netrad_units = dict(swin_2_1_1='W+1m-2', swout_2_1_1='W+1m-2', lwin_2_1_1='W+1m-2', lwout_2_1_1='W+1m-2', rn_2_1_1='W+1m-2', tb_2_1_1='C+1', alb_2_1_1='')
    add_instrument(shortname='netrad',  site=site, height=3, instr_model='CNR1',  rep=1, instr_sn=80246, comment='', columns=netrad_cols,    units=netrad_units,    logger_sn=logger_sn,  con=con, show=show)

    # LI-190 SB, up and down facing
    parin_cols = dict(ppfd_2_1_1='REAL')
    parin_units = dict(ppfd_2_1_1='umol+1m-2s-1')
    add_instrument(shortname='ppfd',  site=site, height=3, instr_model='LI190SB',  rep=1, instr_sn=27919, comment='upward-facing',   columns=parin_cols,     units=parin_units,     logger_sn=logger_sn,  con=con, show=show)

    # HMP & RTD
    hmp_cols = dict(ta_2_1_1='REAL', rh_2_1_1='REAL')
    hmp_units = dict(ta_2_1_1='C+1', rh_2_1_1='%+1')
    add_instrument(shortname='hmp', site=site, height=3, instr_model='HMP', rep=1, instr_sn=False, comment='ta/rh', columns=hmp_cols, units=hmp_units, logger_sn=logger_sn, con=con, show=show)

    # SB111
    surft_cols = dict(tsurf_2_1_1='REAL', sbt_2_1_1='REAL')
    surft_units = dict(tsurf_2_1_1='C+1', sbt_2_1_1='C+1')
    add_instrument(shortname='tsurf', site=site, height=3,  instr_model='SB-111', rep=1, instr_sn=5063, comment='infrared radiometers', columns=surft_cols, units=surft_units, logger_sn=logger_sn, con=con, show=show)

    # Fast (10Hz) files
    fast_cols = dict(file='TEXT')
    create_table('fast_NF_300cm_1', {'timestamp':'TEXT', 'fn':'TEXT'}, con)


def load_flux30min(con, fns):
    # read in 30-minute flux summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(sonic=re.compile(".*CSAT3B.*"),
             irga=re.compile(".*LI7500.*"),
             logger=re.compile(".*CR3000.*"))
    table_names = ['sonic_NF_300cm_1', 'irga_NF_300cm_1', 'status_NF_0cm_3']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            'TIMESTAMP':'timestamp', 
            'Ux_CSAT3B_Avg': 'u_2_1_1-avg',
            'Uy_CSAT3B_Avg': 'v_2_1_1-avg',
            'Uz_CSAT3B_Avg': 'w_2_1_1-avg',
            'Ts_CSAT3B_Avg': 'tsonic_2_1_1-avg',
            'rho_c_LI7500_Avg': 'co2_2_1_1-avg',
            'rho_v_LI7500_Avg': 'h2o_2_1_1-avg',
            'P_LI7500_Avg': 'pa_2_1_1-avg',
            'DIAG_CSAT3B_Avg': 'diagsonic_2_1_1-avg',
            'DIAG_LI7500_Avg': 'diagirga_2_1_1-avg',
            'T_CR3000_Avg': 'tlogger_2_1_1-avg',
            'V_CR3000_Avg': 'vlogger_2_1_1-avg',
            'Ux_CSAT3B_Std': 'u_2_1_1-std',
            'Uy_CSAT3B_Std': 'v_2_1_1-std',
            'Uz_CSAT3B_Std': 'w_2_1_1-std',
            'Ts_CSAT3B_Std': 'tsonic_2_1_1-std',
            'rho_c_LI7500_Std': 'co2_2_1_1-std',
            'rho_v_LI7500_Std': 'h2o_2_1_1-std',
            'P_LI7500_Std': 'pa_2_1_1-std',
            'DIAG_CSAT3B_Std': 'diagsonic_2_1_1-std',
            'DIAG_LI7500_Std': 'diagirga_2_1_1-std',
            'T_CR3000_Std': 'tlogger_2_1_1-std',
            'V_CR3000_Std': 'vlogger_2_1_1-std'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_met30min(con, fns):
    # read in 30-minute met summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(hmp=re.compile("^AirT_3.*|^RH_3.*"),
              tsurf=re.compile("^SurfT.*|^SBT.*"),
              netrad=re.compile('^SWD.*|^SWU.*|^LWD.*|^LWU.*|^Tb.*|^albedo.*|^Rn.*'),
              ppfd_in=re.compile('^PAR_up.*'))

    table_names = ['hmp_NF_300cm_1', 'tsurf_NF_300cm_1', 'netrad_NF_300cm_1', 'ppfd_NF_300cm_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            "AirT_3m_Avg":"ta_2_1_1-avg",
            "AirT_3m_Min":"ta_2_1_1-min",
            "AirT_3m_Max":"ta_2_1_1-max",
            "AirT_3m_Std":"ta_2_1_1-std",
            "RH_3m":"rh_2_1_1-spl",
            "RH_3m_Min":"rh_2_1_1-min",
            "RH_3m_Max":"rh_2_1_1-max",
            "RH_3m_Std":"rh_2_1_1-std",
            "SurfT_Avg":"tsurf_2_1_1-avg",
            "SurfT_Min":"tsurf_2_1_1-min",
            "SurfT_Max":"tsurf_2_1_1-max",
            "SurfT_Std":"tsurf_2_1_1-std",
            "SBT_C_Avg":"sbt_2_1_1-avg",
            "SBT_C_Min":"sbt_2_1_1-min",
            "SBT_C_Max":"sbt_2_1_1-max",
            "SBT_C_Std":"sbt_2_1_1-std",
            "SWD_Avg":"swin_2_1_1-avg",
            "SWD_Std":"swin_2_1_1-std",
            "SWU_Avg":"swout_2_1_1-avg",
            "SWU_Std":"swout_2_1_1-std",
            "LWD_cor_Avg":"lwin_2_1_1-avg",
            "LWD_cor_Std":"lwin_2_1_1-std",
            "LWU_cor_Avg":"lwout_2_1_1-avg",
            "LWU_cor_Std":"lwout_2_1_1-std",
            "Tb_Avg":"tb_2_1_1-avg",
            "Tb_Std":"tb_2_1_1-std",
            "albedo_Avg":"alb_2_1_1-avg",
            "albedo_Std":"alb_2_1_1-std",
            "Rn_Avg":"rn_2_1_1-avg",
            "Rn_Std":"rn_2_1_1-std",
            "PAR_up_Avg":"ppfd_2_1_1-avg",
            "PAR_up_Std":"ppfd_2_1_1-std",
            "T_CR3000_Avg":"tlogger_2_1_1-avg",
            "T_CR3000_Std":"tlogger_2_1_1-std",
            "V_CR3000_Avg":"vlogger_2_1_1-avg",
            "V_CR3000_Min":"vlogger_2_1_1-std"}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_flux10Hz(con, fns):
    timestamps = [f'{fn.stem[-15:-11]}-{fn.stem[-10:-8]}-{fn.stem[-7:-5]} {fn.stem[-4:-2]}:{fn.stem[-2:]}' for fn in fns]
    values = [(str(timestamp), str(fn)) for timestamp, fn in zip(timestamps, fns)]
    # print(values[:10])
    # input()
    # Fast (10Hz) files
    insert('fast_NF_300cm_1', values, con=con)

def main():
    print(__name__, 'NF_3_EC_manager.py')

if __name__ == '__main__':
    main()

