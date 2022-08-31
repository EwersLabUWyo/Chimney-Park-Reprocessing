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
    status_cols = dict(vlogger_1_1_1='REAL', tlogger_1_1_1='REAL')
    status_units = dict(vlogger_1_1_1='V+1', tlogger_1_1_1='C+1')   
    add_instrument(shortname='status', site=site, height=0, instr_model='CR3000', instr_sn=logger_sn,  comment='NF EC 17m logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show, rep=1)
    
    # 17m +7m sonics
    sonic_cols = dict(u_1_1_1='REAL', v_1_1_1='REAL', w_1_1_1='REAL', tsonic_1_1_1='REAL', diagsonic_1_1_1='INT')
    sonic_units = dict(u_1_1_1='m+1s-1', v_1_1_1='m+1s-1', w_1_1_1='m+1s-1', tsonic_1_1_1='C+1', diagsonic_1_1_1='')
    add_instrument(shortname='sonic',   site=site, height=17, instr_model='CSAT3', instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=logger_sn,  con=con, show=show, rep=1)
    sonic_cols = dict(u_1_2_1='REAL', v_1_2_1='REAL', w_1_2_1='REAL', tsonic_1_2_1='REAL', diagsonic_1_2_1='INT')
    sonic_units = dict(u_1_2_1='m+1s-1', v_1_2_1='m+1s-1', w_1_2_1='m+1s-1', tsonic_1_2_1='C+1', diagsonic_1_2_1='')
    add_instrument(shortname='sonic',   site=site, height=7,  instr_model='CSAT3', instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=logger_sn,  con=con, show=show, rep=1)
    
    # 17m IRGA
    irga_cols = dict(co2_1_1_1='REAL', h2o_1_1_1='REAL', pa_1_1_1='REAL', diagirga_1_1_1='INT')
    irga_units = dict(co2_1_1_1='mg+1m-3', h2o_1_1_1='g+1m-3', pairga_1_1_1='kPa+1', diagirga_1_1_1='')
    add_instrument(shortname='irga',    site=site, height=17, instr_model='LI7500',  rep=1, instr_sn=False, comment='', columns=irga_cols,    units=irga_units,    logger_sn=logger_sn,  con=con, show=show)
    
    # 17m CNR4
    netrad_cols = dict(swin_1_1_1='REAL', swout_1_1_1='REAL', lwin_1_1_1='REAL', lwout_1_1_1='REAL', rn_1_1_1='REAL', tb_1_1_1='REAL', alb_1_1_1='REAL')
    netrad_units = dict(swin_1_1_1='W+1m-2', swout_1_1_1='W+1m-2', lwin_1_1_1='W+1m-2', lwout_1_1_1='W+1m-2', rn_1_1_1='W+1m-2', tb_1_1_1='C+1', alb_1_1_1='')
    add_instrument(shortname='netrad',  site=site, height=17, instr_model='CNR4',  rep=1, instr_sn=131353, comment='', columns=netrad_cols,    units=netrad_units,    logger_sn=logger_sn,  con=con, show=show)

    # LI-190 SB, up and down facing
    parin_cols = dict(ppfd_1_1_1='REAL')
    parout_cols = dict(ppfdr_1_1_2='REAL')
    parin_units = dict(ppfd_1_1_1='umol+1m-2s-1')
    parout_units = dict(ppfdr_1_1_2='umol+1m-2s-1')
    add_instrument(shortname='ppfd',  site=site, height=17, instr_model='LI190SB',  rep=1, instr_sn=34700, comment='upward-facing',   columns=parin_cols,     units=parin_units,     logger_sn=logger_sn,  con=con, show=show)
    add_instrument(shortname='ppfd',  site=site, height=17, instr_model='LI190SB',  rep=2, instr_sn=35376, comment='downward-facing', columns=parout_cols,    units=parout_units,    logger_sn=logger_sn,  con=con, show=show)

    # HMP & RTD
    for i, height in zip(range(1, 4), [17, 12, 7]):
        hmp_cols = {f'ta_1_{i}_1':'REAL'}
        hmp_cols.update({f'rh_1_{i}_1':'REAL'})
        hmp_units = {f'ta_1_{i}_1':'C+1'}
        hmp_units.update({f'rh_1_{i}_1':'%+1'})
        add_instrument(shortname='hmp', site=site, height=height, instr_model='HMP', rep=1, instr_sn=False, comment='ta/rh', columns=hmp_cols, units=hmp_units, logger_sn=logger_sn, con=con, show=show)
    
    # PRI sensor(s?)
    pri_cols = dict(pri_1_1_1='REAL', indup_1_1_1='REAL', inddown_1_1_1='REAL', up532_1_1_1='REAL', down532_1_1_1='REAL', up570_1_1_1='REAL', down570_1_1_1='REAL')
    pri_units = dict(pri_1_1_1='', indup_1_1_1='', inddown_1_1_1='', up532_1_1_1='W+1m-2nm-1', down532_1_1_1='W+1m-2nm-1sr-1', up570_1_1_1='W+1m-2nm-1', down570_1_1_1='W+1m-2nm-1sr-1')
    add_instrument(shortname='pri', site=site, height=17,  instr_model='PRI', rep=1, instr_sn=False, comment='pri sensor(s)?', columns=pri_cols, units=pri_units, logger_sn=logger_sn, con=con, show=show)
    pri_cols = dict(pri_1_2_1='REAL', indup_1_2_1='REAL', inddown_1_2_1='REAL', up532_1_2_1='REAL', down532_1_2_1='REAL', up570_1_2_1='REAL', down570_1_2_1='REAL')
    pri_units = dict(pri_1_2_1='', indup_1_2_1='', inddown_1_2_1='', up532_1_2_1='W+1m-2nm-1', down532_1_2_1='W+1m-2nm-1sr-1', up570_1_2_1='W+1m-2nm-1', down570_1_2_1='W+1m-2nm-1sr-1')
    add_instrument(shortname='pri', site=site, height=7,  instr_model='PRI', rep=1, instr_sn=False, comment='pri sensor(s)?', columns=pri_cols, units=pri_units, logger_sn=logger_sn, con=con, show=show)


    # RM Young
    windcols = dict(ws_1_1_1='REAL', wd_1_1_1='REAL')
    windunits = dict(ws_1_1_1='m+1s-1', wd_1_1_1='deg')
    add_instrument(shortname='wind', site=site, height=12,  instr_model='RM-Young', rep=1, instr_sn=False, comment='propellor wind gauge', columns=windcols, units=windunits, logger_sn=logger_sn, con=con, show=show)

    # SB111
    surft_cols = dict(tc_1_1_1='REAL', sbt_1_1_1='REAL')
    surft_units = dict(tc_1_1_1='C+1', sbt_1_1_1='C+1')
    add_instrument(shortname='tcanopy', site=site, height=17,  instr_model='SB-111', rep=1, instr_sn=False, comment='infrared radiometers', columns=surft_cols, units=surft_units, logger_sn=logger_sn, con=con, show=show)

    # Fast (10Hz) files
    fast_cols = dict(file='TEXT')
    fast_units = dict(file='None')
    # create_table('fast_NF_1700cm_1', {'timestamp':'TEXT', 'fn':'TEXT'}, con)
    add_instrument(shortname='fast', site=site, height=17, rep=1, instr_sn=False, instr_model='2xCSAT3,1xLI7500', logger_sn=logger_sn, comment='10Hz files from the NF tall tower', columns=fast_cols, units=fast_units, con=con, show=show)

def load_flux30min(con, fns):
    # read in 30-minute flux summaries and add them to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(sonic_17=re.compile(".*CSAT3_17m.*"),
             sonic_7=re.compile(".*CSAT3_7m.*"),
             irga=re.compile(".*LI7500.*"),
             logger=re.compile(".*CR3000.*"))
    table_names = ['sonic_NF_1700cm_1', 'sonic_NF_700cm_1', 'irga_NF_1700cm_1', 'status_NF_0cm_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            'TIMESTAMP':'timestamp', 
            'Ux_CSAT3_17m_Avg': 'u_1_1_1-avg',
            'Uy_CSAT3_17m_Avg': 'v_1_1_1-avg',
            'Uz_CSAT3_17m_Avg': 'w_1_1_1-avg',
            'Ts_CSAT3_17m_Avg': 'tsonic_1_1_1-avg',
            'Ux_CSAT3_7m_Avg': 'u_1_2_1-avg',
            'Uy_CSAT3_7m_Avg': 'v_1_2_1-avg',
            'Uz_CSAT3_7m_Avg': 'w_1_2_1-avg',
            'Ts_CSAT3_7m_Avg': 'tsonic_1_2_1-avg',
            'rho_c_LI7500_Avg': 'co2_1_1_1-avg',
            'rho_v_LI7500_Avg': 'h2o_1_1_1-avg',
            'P_LI7500_Avg': 'pa_1_1_1-avg',
            'DIAG_CSAT3_17m_Avg': 'diagsonic_1_1_1-avg',
            'DIAG_LI7500_Avg': 'diagirga_1_1_1-avg',
            'T_CR3000_Avg': 'tlogger_1_1_1-avg',
            'V_CR3000_Avg': 'vlogger_1_1_1-avg',
            'Ux_CSAT3_17m_Std': 'u_1_1_1-std',
            'Uy_CSAT3_17m_Std': 'v_1_1_1-std',
            'Uz_CSAT3_17m_Std': 'w_1_1_1-std',
            'Ts_CSAT3_17m_Std': 'tsonic_1_1_1-std',
            'Ux_CSAT3_7m_Std': 'u_1_2_1-std',
            'Uy_CSAT3_7m_Std': 'v_1_2_1-std',
            'Uz_CSAT3_7m_Std': 'w_1_2_1-std',
            'Ts_CSAT3_7m_Std': 'tsonic_1_2_1-std',
            'rho_c_LI7500_Std': 'co2_1_1_1-std',
            'rho_v_LI7500_Std': 'h2o_1_1_1-std',
            'P_LI7500_Std': 'pa_1_1_1-std',
            'DIAG_CSAT3_17m_Std': 'diagsonic_1_1_1-std',
            'DIAG_CSAT3_7m_Std': 'diagsonic_1_2_1-std',
            'DIAG_LI7500_Std': 'diagirga_1_1_1-std',
            'T_CR3000_Std': 'tlogger_1_1_1-std',
            'V_CR3000_Std': 'vlogger_1_1_1-std'}

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

    table_names = ['hmp_NF_1700cm_1', 'hmp_NF_1200cm_1', 'hmp_NF_700cm_1', 'wind_NF_1200cm_1', 'tcanopy_NF_1700cm_1', 'netrad_NF_1700cm_1', 'ppfd_NF_1700cm_1', 'ppfd_NF_1700cm_2']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            "AirT_17m_Avg":"ta_1_1_1-avg",
            "AirT_12m_Avg":"ta_1_2_1-avg",
            "AirT_7m_Avg":"ta_1_3_1-avg",
            "AirT_17m_Min":"ta_1_1_1-min",
            "AirT_12m_Min":"ta_1_2_1-min",
            "AirT_7m_Min":"ta_1_3_1-min",
            "AirT_17m_Max":"ta_1_1_1-max",
            "AirT_12m_Max":"ta_1_2_1-max",
            "AirT_7m_Max":"ta_1_3_1-max",
            "AirT_17m_Std":"ta_1_1_1-std",
            "AirT_12m_Std":"ta_1_2_1-std",
            "AirT_7m_Std":"ta_1_3_1-std",
            "RH_17m":"rh_1_1_1-spl",
            "RH_12m":"rh_1_2_1-spl",
            "RH_7m":"rh_1_3_1-spl",
            "RH_17m_Min":"rh_1_1_1-min",
            "RH_12m_Min":"rh_1_2_1-min",
            "RH_7m_Min":"rh_1_3_1-min",
            "RH_17m_Max":"rh_1_1_1-max",
            "RH_12m_Max":"rh_1_2_1-max",
            "RH_7m_Max":"rh_1_3_1-max",
            "RH_17m_Std":"rh_1_1_1-std",
            "RH_12m_Std":"rh_1_2_1-std",
            "RH_7m_Std":"rh_1_3_1-std",
            "WS_12m_Avg":"ws_1_1_1-avg",
            "WS_12m_Med":"ws_1_1_1-med",
            "WS_12m_Min":"ws_1_1_1-min",
            "WS_12m_Max":"ws_1_1_1-max",
            "WS_12m":"ws_1_1_1-spl",
            "WS_12m_Std":"ws_1_1_1-std",
            "WD_12m":"wd_1_1_1-spl",
            "SurfT_canopy_Avg":"tc_1_1_1-avg",
            "SurfT_canopy_Min":"tc_1_1_1-min",
            "SurfT_canopy_Max":"tc_1_1_1-max",
            "SurfT_canopy_Std":"tc_1_1_1-std",
            "SBT_C_Avg":"sbt_1_1_1-avg",
            "SBT_C_Min":"sbt_1_1_1-min",
            "SBT_C_Max":"sbt_1_1_1-max",
            "SBT_C_Std":"sbt_1_1_1-std",
            "SWD_Avg":"swin_1_1_1-avg",
            "SWD_Std":"swin_1_1_1-std",
            "SWU_Avg":"swout_1_1_1-avg",
            "SWU_Std":"swout_1_1_1-std",
            "LWD_cor_Avg":"lwin_1_1_1-avg",
            "LWD_cor_Std":"lwin_1_1_1-std",
            "LWU_cor_Avg":"lwout_1_1_1-avg",
            "LWU_cor_Std":"lwout_1_1_1-std",
            "Tb_Avg":"tb_1_1_1-avg",
            "Tb_Std":"tb_1_1_1-std",
            "albedo_Avg":"alb_1_1_1-avg",
            "albedo_Std":"alb_1_1_1-std",
            "Rn_Avg":"rn_1_1_1-avg",
            "Rn_Std":"rn_1_1_1-std",
            "PAR_up_17m_Avg":"ppfd_1_1_1-avg",
            "PAR_up_17m_Std":"ppfd_1_1_1-std",
            "PAR_dn_17m_Avg":"ppfdr_1_1_2-avg",
            "PAR_dn_17m_Std":"ppfdr_1_1_2-std",
            "T_CR3000_Avg":"tlogger_1_1_1-avg",
            "T_CR3000_Std":"tlogger_1_1_1-std",
            "V_CR3000_Avg":"vlogger_1_1_1-avg",
            "V_CR3000_Min":"vlogger_1_1_1-std"}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_pri1min(con, fns):
    # read in 1-minute pri data and add it to the database

    # instructions for subsetting instruments using a regex filter for each instrument
    rx = dict(pri_17=re.compile('.*_17m_.*'),
              pri_7=re.compile('.*_7m_.*'))

    table_names = ['pri_NF_1700cm_1', 'pri_NF_700cm_1']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {
            "TIMESTAMP":'timestamp',
            "PRI_17m_Med":'pri_1_1_1-med',
            "Down_532_17m_Med":'down532_1_1_1-med',
            "Down_570_17m_Med":'down570_1_1_1-med',
            "Up_532_17m_Med":'up532_1_1_1-med',
            "Up_570_17m_Med":'up570_1_1_1-med',
            "Ind_down_17m_Med":'inddown_1_1_1-med',
            "Ind_up_17m_Med":'indup_1_1_1-med',
            "PRI_7m_Med":'pri_1_2_1-med',
            "Down_532_7m_Med":'down532_1_2_1-med',
            "Down_570_7m_Med":'down570_1_2_1-med',
            "Up_532_7m_Med":'up532_1_2_1-med',
            "Up_570_7m_Med":'up570_1_2_1-med',
            "Ind_down_7m_Med":'inddown_1_2_1-med',
            "Ind_up_7m_Med":'indup_1_2_1-med'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_flux10Hz(con, fns):
    
    # pbar = tqdm(enumerate(sorted(fns)))
    # for i, fn in pbar:
    #     pbar.set_description(fn.name)
    #     timestamp = f'{fn.stem[-15:-11]}-{fn.stem[-10:-8]}-{fn.stem[-7:-5]} {fn.stem[-4:-2]}:{fn.stem[-2:]}'
    #     epoch = int(pd.to_datetime([timestamp]).astype(int)[0]/10**9)
    #     print(epoch)
    #     df = pd.read_csv(fn, skiprows=[0,2,3])  
    #     df['epoch'] = epoch
    #     df.set_index(['epoch', 'RECORD'])
    #     if i==0:
    #         columns = {'epoch':'INT', 'RECORD':'INT'}
    #         columns.update({k:'REAL' for k in df.columns})
    #         create_table('fastdata_NF_1700cm_1', columns, con)
    #     df.to_sql('fastdata_NF_1700cm_1', con, if_exists='append', index=False)

            




    timestamps = [f'{fn.stem[-15:-11]}-{fn.stem[-10:-8]}-{fn.stem[-7:-5]} {fn.stem[-4:-2]}:{fn.stem[-2:]}' for fn in fns]
    values = [(i, str(timestamp), 'file', str(fn)) for i, timestamp, fn in zip(range(len(timestamps)), timestamps, fns)]
    # print(values[:10])
    # input()
    insert('fast_NF_1700cm_1', values, con=con)

def main():
    print(__name__, 'NF_1700_EC_manager.py')

if __name__ == '__main__':
    main()

