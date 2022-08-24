# Code to manage and upload NF soils data

# Author: Alex Fox
# Initial creation: 20220802

import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import re

from tqdm import tqdm

from utilities import *

logger_sn = 3864
site = 'NF'
logger_model = 'CR1000'

def initialize(con, show=False):
    # Adds datalogger, instruments, and units tables

    # logger metadata
    add_logger(logger_model=logger_model, site=site, rep=6, logger_sn=logger_sn, shortname='NF_Soils', comment='soils logger', con=con, show=show)

    # logger status
    status_cols = dict(vlogger_2_1_4='REAL', tlogger_2_1_4='REAL')
    status_units = dict(vlogger_2_1_4='V+1', tlogger_2_1_4='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model=logger_model, rep=6, instr_sn=logger_sn,  comment='NF soils logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # cs616 TDR probes
    for height, i in zip([-0.05, -0.1, -0.15, -0.3, -0.5], range(1, 6)):
        cs616_cols = {f'swc_3_{i}_1':'REAL', f'period_3_{i}_1':'REAL'}
        cs616_units = {f'swc_3_{i}_1':'', f'period_3_{i}_1':'us+1'}
        add_instrument(shortname='tdr', site=site, height=height, instr_model='CS616', rep=3, instr_sn=False,  comment='Pit C time domain reflectometer', columns=cs616_cols, units=cs616_units, logger_sn=logger_sn,  con=con, show=show)

    # stevens hydraprobes
    hydra_cols = {}
    hydra_units = {}
    for varname, unit in zip(['swc', 'tempcorrcond', 'ts', 'tsfarenheit', 'cond', 'redipe', 'imdipe', 'tcredipe', 'tcimdipe'], 
                             ['', 'Ohm-1m-1', 'C+1', 'F+1', 'Ohm-1m-1', 'Clb+1V-1m-1', 'Clb+1V-1m-1', 'Clb+1V-1m-1', 'Clb+1V-1m-1']):
        for pit, name in zip(range(1, 3), ['A', 'B']):
            for height in range(1, 6):
                hydra_cols.update({f'{varname}_{pit}_{height}_1':'REAL'})
                hydra_units.update({f'{varname}_{pit}_{height}_1':unit})
                add_instrument(shortname='hydra', site=site, height=height, instr_model='Hydraprobe', rep=pit, instr_sn=False,  comment=f'Pit {name} hydraprobe: SWC, TS, electrical properties', columns=hydra_cols, units=hydra_units, logger_sn=logger_sn,  con=con, show=show)


    # Soil heat flux plates
    for h, v, r, sn, height, pit in zip([1, 1, 1, 2, 2, 2, 3], 
                                        [1, 2, 3, 1, 2, 3, 1], 
                                        [1, 1, 1, 1, 1, 1, 1], 
                                        [767, 3680, 3748, 13041, 13042, 13051, 4647], 
                                        [-0.05, -0.1, -0.15, -0.05, -0.1, -0.15, -0.05], 
                                        list('AAABBBC')):
        gs_cols = {f'g_{h}_{v}_{r}':'REAL'}
        gs_units = {k:'W+1m-2' for k in gs_cols}
        add_instrument(shortname='soilhf', site=site, height=height, instr_model='Unknown Heat Flux Plate', rep=h, instr_sn=sn,  comment=f'Pit {pit} soil heat flux, possibly hukseflux?', columns=gs_cols, units=gs_units, logger_sn=logger_sn,  con=con, show=show)        
    
    for v, height in zip(range(1, 6), 
                         [-0.05, -0.1, -0.15, -0.3, -0.5]):
        ts_cols = {f'ts_3_{i}_1':'REAL'}
        ts_units = {k:'C+1' for k in ts_cols}
        add_instrument(shortname='tsoil', site=site, height=height, instr_model='Type-T TC', rep=3, instr_sn=False,  comment='Pit C soil thermocouples', columns=ts_cols, units=ts_units, logger_sn=logger_sn,  con=con, show=show)        



def load_cs30min(con, fns):
    # read in 30-minute cs616 soiltdr summaries into database
    rx = {f'cs616_{height}':re.compile(f'^WC_C{height}cm.*|^tau616_.*({i})') for i, height in enumerate(['5', '10', '15', '30', '50'])}
    rx.update({'status':re.compile('T_CR3000.*|V_CR3000.*')})

    heights = ['neg5', 'neg10', 'neg15', 'neg30', 'neg50']
    table_names = [f'tdr_NF_{height}_3' for height in heights] + ['status_NF_0_6']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "WC_C5cm_Avg": 'swc_3_1_1-avg',
                    "WC_C10cm_Avg": 'swc_3_2_1-avg',
                    "WC_C15cm_Avg": 'swc_3_3_1-avg',
                    "WC_C30cm_Avg": 'swc_3_4_1-avg',
                    "WC_C50cm_Avg": 'swc_3_5_1-avg',
                    "tau616_Avg(1)": 'period_3_1_1-avg',
                    "tau616_Avg(2)": 'period_3_2_1-avg',
                    "tau616_Avg(3)": 'period_3_3_1-avg',
                    "tau616_Avg(4)": 'period_3_4_1-avg',
                    "tau616_Avg(5)": 'period_3_5_1-avg',
                    "T_CR3000_Avg": 'tlogger_2_1_4-avg',
                    "V_CR3000_Avg": 'vlogger_2_1_4-avg',
                    "WC_C5cm_Std": 'swc_3_1_1-std',
                    "WC_C10cm_Std": 'swc_3_2_1-std',
                    "WC_C15cm_Std": 'swc_3_3_1-std',
                    "WC_C30cm_Std": 'swc_3_4_1-std',
                    "WC_C50cm_Std": 'swc_3_5_1-std',
                    "tau616_Std(1)": 'period_3_1_1-std',
                    "tau616_Std(2)": 'period_3_2_1-std',
                    "tau616_Std(3)": 'period_3_3_1-std',
                    "tau616_Std(4)": 'period_3_4_1-std',
                    "tau616_Std(5)": 'period_3_5_1-std',
                    "T_CR3000_Std": 'tlogger_2_1_4-std',
                    "V_CR3000_Std": 'vlogger_2_1_4-std'}

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_hydraprobes30min(con, fns):
    # read in 30-minute hydraprobe summaries
    rx = {f'hydra_{height}A':     re.compile(f'^Soil_Moist_A{height}cm.*|^Temp_Corr_Soil_Cond_A{height}cm.*|^Soil_Temp_C_A{height}cm.*|^Soil_Temp_F_A{height}cm.*|^Soil_Cond_A{height}cm.*|^Real_Diel_Perm_A{height}cm.*|^Imag_Diel_Perm_A{height}cm.*|^Corr_Real_Diel_Perm_A{height}cm.*|^Corr_Imag_Diel_Perm_A{height}cm.*') for i, height in enumerate(['5', '10', '15', '30', '50'])}
    rx.update({f'hydra_{height}B':re.compile(f'^Soil_Moist_B{height}cm.*|^Temp_Corr_Soil_Cond_B{height}cm.*|^Soil_Temp_C_B{height}cm.*|^Soil_Temp_F_B{height}cm.*|^Soil_Cond_B{height}cm.*|^Real_Diel_Perm_B{height}cm.*|^Imag_Diel_Perm_B{height}cm.*|^Corr_Real_Diel_Perm_B{height}cm.*|^Corr_Imag_Diel_Perm_B{height}cm.*') for i, height in enumerate(['5', '10', '15', '30', '50'])})

    
    heights = ['neg5', 'neg10', 'neg15', 'neg30', 'neg50']
    table_names = [f'hydra_NF_{height}_1' for height in heights] + [f'hydra_NF_{height}_2' for height in heights]
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp'}
    for height, v in zip(['5', '10', '15', '30', '50'], [1, 2, 3, 4, 5]):
        for stat in ['Avg', 'Std']:
            for pit, h in zip(['A', 'B'], [1, 2]):
                renaming_dict.update(
                    {f"Soil_Moist_{pit}{height}cm_{stat}":f'swc_{h}_{v}_1-{stat.lower()}',
                    f"Temp_Corr_Soil_Cond_{pit}{height}cm_{stat}":f'tempcorrcond_{h}_{v}_1-{stat.lower()}',
                    f"Soil_Temp_C_{pit}{height}cm_{stat}":f'ts_{h}_{v}_1-{stat.lower()}',
                    f"Soil_Temp_F_{pit}{height}cm_{stat}":f'tsfarenheit_{h}_{v}_1-{stat.lower()}',
                    f"Soil_Cond_{pit}{height}cm_{stat}": f'cond_{h}_{v}_1-{stat.lower()}',
                    f"Real_Diel_Perm_{pit}{height}cm_{stat}": f'redipe_{h}_{v}_1-{stat.lower()}',
                    f"Imag_Diel_Perm_{pit}{height}cm_{stat}": f'imdipe_{h}_{v}_1-{stat.lower()}',
                    f"Corr_Real_Diel_Perm_{pit}{height}cm_{stat}": f'tcredipe_{h}_{v}_1-{stat.lower()}',
                    f"Corr_Imag_Diel_Perm_{pit}{height}cm_{stat}":f'tcimdipe_{h}_{v}_1-{stat.lower()}'}
                )

    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def load_gsts30min(con, fns):
    # read in 30-minute heat flux and soil temp data
    rx = {f'gs_A{height}':re.compile(f'^Gs_A{height}.*') for height in ['5cm', '10m', '15m']}
    rx.update({f'gs_B{height}':re.compile(f'^Gs_A{height}.*') for height in ['5cm', '10m', '15m']})
    rx.update({f'ts_C{height}':re.compile(f'^Ts_C{height}.*') for height in ['5cm', '10cm', '15cm', '30cm', '50cm']})
    
    heights = ['neg5', 'neg10', 'neg15', 'neg30', 'neg50']
    table_names = ([f'soilhf_NF_{height}_1' for height in heights[:4]]
                   + [f'soilhf_NF_{height}_2' for height in heights[:4]]
                   + [f'ts_NF_{height}_3' for height in heights])
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "Gs_A5cm_Avg": 'gs_1_1_1-avg',
                    "Gs_A10m_Avg": 'gs_1_2_1-avg',
                    "Gs_A15m_Avg": 'gs_1_3_1-avg',
                    "Gs_B5cm_Avg": 'gs_2_1_1-avg',
                    "Gs_B10m_Avg": 'gs_2_2_1-avg',
                    "Gs_B15m_Avg": 'gs_2_3_1-avg',
                    "Gs_C5cm_Avg": 'gs_3_1_1-avg',
                    "Ts_C5cm_Avg": 'ts_3_1_1-avg',
                    "Ts_C10cm_Avg": 'ts_3_2_1-avg',
                    "Ts_C15cm_Avg": 'ts_3_3_1-avg',
                    "Ts_C30cm_Avg": 'ts_3_4_1-avg',
                    "Ts_C50cm_Avg": 'ts_3_5_1-avg',
                    "Gs_A5cm_Std": 'gs_1_1_1-std',
                    "Gs_A10m_Std": 'gs_1_2_1-std',
                    "Gs_A15m_Std": 'gs_1_3_1-std',
                    "Gs_B5cm_Std": 'gs_2_1_1-std',
                    "Gs_B10m_Std": 'gs_2_2_1-std',
                    "Gs_B15m_Std": 'gs_2_3_1-std',
                    "Gs_C5cm_Std": 'gs_3_1_1-std',
                    "Ts_C5cm_Std": 'ts_3_1_1-std',
                    "Ts_C10cm_Std": 'ts_3_2_1-std',
                    "Ts_C15cm_Std": 'ts_3_3_1-std',
                    "Ts_C30cm_Std": 'ts_3_4_1-std',
                    "Ts_C50cm_Std": 'ts_3_5_1-std'}


    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def main():
    print(__name__, f'{site}_soil_manager.py')

if __name__ == '__main__':
    main()