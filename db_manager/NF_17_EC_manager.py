# Code to manage and upload NF_17_EC data to the database.

# Author: Alex Fox
# Initial creation: 20220728

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables
    logger_sn = 9809
    site = 'NF'

    # logger metadata
    add_logger(site=site, logger_model='CR3000', rep=1, logger_sn=logger_sn, shortname='NF_EC_17m', con=con, show=show, comment='')

    # logger status
    status_cols = dict(vlogger='REAL', tlogger='REAL')
    status_units = dict(vlogger='V+1', tlogger='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model='CR3000', rep=1, instr_sn=logger_sn,  comment='NF EC 17m logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # 17m +7m sonics
    sonic_cols = dict(u='REAL', v='REAL', w='REAL', tsonic='REAL', diagsonic='INT')
    sonic_units = dict(u='m+1s-1', v='m+1s-1', w='m+1s-1', tsonic='C+1', diagsonic='')
    add_instrument(shortname='sonic',   site=site, height=17, instr_model='CSAT3',   rep=1, instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=logger_sn,  con=con, show=show)
    add_instrument(shortname='sonic',   site=site, height=7,  instr_model='CSAT3',   rep=1, instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=logger_sn,  con=con, show=show)
    
    # 17m IRGA
    irga_cols = dict(co2='REAL', h2o='REAL', pairga='REAL', diagirga='INT')
    irga_units = dict(co2='mg+1m-3', h2o='g+1m-3', pairga='kPa+1', diagirga='')
    add_instrument(shortname='irga',    site=site, height=17, instr_model='LI7500',  rep=1, instr_sn=False, comment='', columns=irga_cols,    units=irga_units,    logger_sn=logger_sn,  con=con, show=show)
    
    # 17m CNR4
    netrad_cols = dict(swin='REAL', swout='REAL', lwin='REAL', lwout='REAL', netrad='REAL', tb='REAL', alb='REAL')
    netrad_units = dict(swin='W+1m-2', swout='W+1m-2', lwin='W+1m-2', lwout='W+1m-2', netrad='W+1m-2', tb='C+1', alb='')
    add_instrument(shortname='netrad',  site=site, height=17, instr_model='CNR4',  rep=1, instr_sn=131353, comment='', columns=netrad_cols,    units=netrad_units,    logger_sn=logger_sn,  con=con, show=show)

    # LI-190 SB, up and down facing
    parin_cols = dict(ppfdin='REAL')
    parout_cols = dict(ppfdout='REAL')
    parin_units = dict(ppfdin='umol+1m-2s-1')
    parout_units = dict(ppfdin='umol+1m-2s-1')
    add_instrument(shortname='ppfd',  site=site, height=17, instr_model='LI190SB',  rep=1, instr_sn=34700, comment='upward-facing',   columns=parin_cols,     units=parin_units,     logger_sn=logger_sn,  con=con, show=show)
    add_instrument(shortname='ppfd',  site=site, height=17, instr_model='LI190SB',  rep=2, instr_sn=35376, comment='downward-facing', columns=parout_cols,    units=parout_units,    logger_sn=logger_sn,  con=con, show=show)

    # HMP & RTD
    hmp_cols = dict(ta='REAL', rh='REAL')
    hmp_units = dict(ta='C+1', rh='%+1')
    add_instrument(shortname='hmp', site=site, height=17, instr_model='HMP', rep=1, instr_sn=False, comment='ta/rh', columns=hmp_cols, units=hmp_units, logger_sn=logger_sn, con=con, show=show)
    add_instrument(shortname='hmp', site=site, height=12, instr_model='HMP', rep=1, instr_sn=False, comment='ta/rh', columns=hmp_cols, units=hmp_units, logger_sn=logger_sn, con=con, show=show)
    add_instrument(shortname='hmp', site=site, height=7,  instr_model='HMP', rep=1, instr_sn=False, comment='ta/rh', columns=hmp_cols, units=hmp_units, logger_sn=logger_sn, con=con, show=show)

    # PRI sensor(s?)
    pri_cols = dict(pri='REAL', indup='REAL', inddown='REAL', up532='REAL', down532='REAL', up570='REAL', down570='REAL')
    pri_units = dict(pri='', indup='', inddown='', up532='W+1m-2nm-1', down532='W+1m-2nm-1sr-1', up570='W+1m-2nm-1', down570='W+1m-2nm-1sr-1')
    add_instrument(shortname='pri', site=site, height=7,  instr_model='PRI', rep=1, instr_sn=False, comment='pri sensor(s)?', columns=pri_cols, units=pri_units, logger_sn=logger_sn, con=con, show=show)

    # RM Young
    windcols = dict(ws='REAL', wd='REAL')
    windunits = dict(ws='m+1s-1', wd='deg')
    add_instrument(shortname='wind', site=site, height=12,  instr_model='RM-Young', rep=1, instr_sn=False, comment='propellor wind gauge', columns=windcols, units=windunits, logger_sn=logger_sn, con=con, show=show)

    # SB111
    surft_cols = dict(tcanopy='REAL', sbt='REAL')
    surft_units = dict(tcanopy='C+1', sbt='C+1')
    add_instrument(shortname='tcanopy', site=site, height=17,  instr_model='SB-111', rep=1, instr_sn=False, comment='infrared radiometers', columns=surft_cols, units=surft_units, logger_sn=logger_sn, con=con, show=show)

    # Fast (10Hz) files
    fast_cols = dict(file='TEXT')
    add_instrument(shortname='fast', site=site, height=17, rep=1, instr_sn=False, instr_model='2xCSAT3,1xLI7500', logger_sn=logger_sn, comment='10Hz files from the NF tall tower', columns=fast_cols, units=False, con=con, show=show)

def load_flux30min(con, fns):
    # read in 30-minute flux summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(sonic_17=re.compile(".*CSAT3_17m.*"),
             sonic_7=re.compile(".*CSAT3_7m.*"),
             irga=re.compile(".*LI7500.*"),
             logger=re.compile(".*CR3000.*"))
    table_names = ['sonic_NF_17_1', 'sonic_NF_7_1', 'irga_NF_17_1', 'status_NF_0_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            'TIMESTAMP':'timestamp', 
            'Ux_CSAT3_17m_Avg': 'u_avg',
            'Uy_CSAT3_17m_Avg': 'v_avg',
            'Uz_CSAT3_17m_Avg': 'w_avg',
            'Ts_CSAT3_17m_Avg': 'tsonic_avg',
            'Ux_CSAT3_7m_Avg': 'u_avg',
            'Uy_CSAT3_7m_Avg': 'v_avg',
            'Uz_CSAT3_7m_Avg': 'w_avg',
            'Ts_CSAT3_7m_Avg': 'tsonic_avg',
            'rho_c_LI7500_Avg': 'co2_avg',
            'rho_v_LI7500_Avg': 'h2o_avg',
            'P_LI7500_Avg': 'pairga_avg',
            'DIAG_CSAT3_17m_Avg': 'diagsonic_avg',
            'DIAG_LI7500_Avg': 'diagirga_avg',
            'T_CR3000_Avg': 'tlogger_avg',
            'V_CR3000_Avg': 'vlogger_avg',
            'Ux_CSAT3_17m_Std': 'u_std',
            'Uy_CSAT3_17m_Std': 'v_std',
            'Uz_CSAT3_17m_Std': 'w_std',
            'Ts_CSAT3_17m_Std': 'tsonic_std',
            'Ux_CSAT3_7m_Std': 'u_std',
            'Uy_CSAT3_7m_Std': 'v_std',
            'Uz_CSAT3_7m_Std': 'w_std',
            'Ts_CSAT3_7m_Std': 'tsonic_std',
            'rho_c_LI7500_Std': 'co2_std',
            'rho_v_LI7500_Std': 'h2o_std',
            'P_LI7500_Std': 'pairga_std',
            'DIAG_CSAT3_17m_Std': 'diagsonic_std',
            'DIAG_CSAT3_7m_Std': 'diagsonic_std',
            'DIAG_LI7500_Std': 'diagirga_std',
            'T_CR3000_Std': 'tlogger_std',
            'V_CR3000_Std': 'vlogger_std'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_met30min(con, fns):
    # read in 30-minute met summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(hmp_17=re.compile("^AirT_17.*|^RH_17.*"),
              hmp_12=re.compile("^AirT_12.*|^RH_12.*"),
              hmp_7=re.compile("^AirT_7.*|^RH_7.*"),
              wind=re.compile("^WS_.*|^WD.*"),
              tcanopy=re.compile("^SurfT.*|^SBT.*"),
              netrad=re.compile('^SWD.*|^SWU.*|^LWD.*|^LWU.*|^Tb.*|^albedo.*|^Rn.*'),
              ppfd_in=re.compile('^PAR_up.*'),
              ppfd_out=re.compile('^PAR_dn.*'))

    table_names = ['hmp_NF_17_1', 'hmp_NF_12_1', 'hmp_NF_7_1', 'wind_NF_12_1', 'tcanopy_NF_17_1', 'netrad_NF_17_1', 'ppfd_NF_17_1', 'ppfd_NF_17_2']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            "AirT_17m_Avg":"ta_avg",
            "AirT_12m_Avg":"ta_avg",
            "AirT_7m_Avg":"ta_avg",
            "AirT_17m_Min":"ta_min",
            "AirT_12m_Min":"ta_min",
            "AirT_7m_Min":"ta_min",
            "AirT_17m_Max":"ta_max",
            "AirT_12m_Max":"ta_max",
            "AirT_7m_Max":"ta_max",
            "AirT_17m_Std":"ta_std",
            "AirT_12m_Std":"ta_std",
            "AirT_7m_Std":"ta_std",
            "RH_17m":"rh_spl",
            "RH_12m":"rh_spl",
            "RH_7m":"rh_spl",
            "RH_17m_Min":"rh_min",
            "RH_12m_Min":"rh_min",
            "RH_7m_Min":"rh_min",
            "RH_17m_Max":"rh_max",
            "RH_12m_Max":"rh_max",
            "RH_7m_Max":"rh_max",
            "RH_17m_Std":"rh_std",
            "RH_12m_Std":"rh_std",
            "RH_7m_Std":"rh_std",
            "WS_12m_Avg":"ws_avg",
            "WS_12m_Med":"ws_med",
            "WS_12m_Min":"ws_min",
            "WS_12m_Max":"ws_max",
            "WS_12m_Std":"ws_std",
            "WD_12m":"wd_spl",
            "SurfT_canopy_Avg":"tcanopy_avg",
            "SurfT_canopy_Min":"tcanopy_min",
            "SurfT_canopy_Max":"tcanopy_max",
            "SurfT_canopy_Std":"tcanopy_std",
            "SBT_C_Avg":"sbt_avg",
            "SBT_C_Min":"sbt_min",
            "SBT_C_Max":"sbt_max",
            "SBT_C_Std":"sbt_std",
            "SWD_Avg":"swin_avg",
            "SWD_Std":"swin_std",
            "SWU_Avg":"swout_avg",
            "SWU_Std":"swout_std",
            "LWD_cor_Avg":"lwin_avg",
            "LWD_cor_Std":"lwin_std",
            "LWU_cor_Avg":"lwout_avg",
            "LWU_cor_Std":"lwout_std",
            "Tb_Avg":"tb_avg",
            "Tb_Std":"tb_std",
            "albedo_Avg":"alb_avg",
            "albedo_Std":"alb_std",
            "Rn_Avg":"netrad_avg",
            "Rn_Std":"netrad_std",
            "PAR_up_17m_Avg":"ppfdin_avg",
            "PAR_up_17m_Std":"ppfdin_std",
            "PAR_dn_17m_Avg":"ppfdout_avg",
            "PAR_dn_17m_Std":"ppfdout_std",
            "T_CR3000_Avg":"tlogger_avg",
            "T_CR3000_Std":"tlogger_std",
            "V_CR3000_Avg":"vlogger_avg",
            "V_CR3000_Min":"vlogger_std"}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_pri1min(con, fns):
    # read in 1-minute pri data and add it to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(pri_17=re.compile('.*_17m_.*'),
              pri_7=re.compile('.*_7m_.*'))

    table_names = ['pri_NF_17_1', 'pri_NF_7_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            "PRI_17m_Med":'pri_med',
            "Down_532_17m_Med":'down532_med',
            "Down_570_17m_Med":'down570_med',
            "Up_532_17m_Med":'up532_med',
            "Up_570_17m_Med":'up570_med',
            "Ind_down_17m_Med":'inddown_med',
            "Ind_up_17m_Med":'indup_med',
            "PRI_7m_Med":'pri_med',
            "Down_532_7m_Med":'down532_med',
            "Down_570_7m_Med":'down570_med',
            "Up_532_7m_Med":'up532_med',
            "Up_570_7m_Med":'up570_med',
            "Ind_down_7m_Med":'inddown_med',
            "Ind_up_7m_Med":'indup_med'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_flux10Hz(con, fns):
    for idx, fn in enumerate(fns):
        fn = Path(fn).name
        fn_parts = re.split('_|\.', fn)
        filenum, timestamp = fn_parts[4][4:], f'{fn_parts[5]}-{fn_parts[6]}-{fn_parts[7]} {fn_parts[8][:2]}:{fn_parts[8][2:]}'
        values = [(fn, filenum, timestamp,  'file')]

        insert('fast_NF_17_1', values, con=con)

def main():
    print(__name__, 'NF_17_EC_manager.py')

if __name__ == '__main__':
    main()

