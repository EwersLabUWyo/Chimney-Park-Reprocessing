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
    status_cols = dict(vlogger='REAL', tlogger='REAL')
    status_units = dict(vlogger='V+1', tlogger='C+1')
    add_instrument(shortname='status', site=site, height=0, instr_model=logger_model, rep=6, instr_sn=logger_sn,  comment='NF soils logger', columns=status_cols, units=status_units, logger_sn=logger_sn,  con=con, show=show)
    
    # cs616 TDR probes
    cs616_cols = dict(swc='REAL', period='REAL')
    cs616_units = dict(swc='', period='us+1')
    for height in [-0.05, -0.1, -0.15, -0.3, -0.5]:
        add_instrument(shortname='tdr', site=site, height=height, instr_model='CS616', rep=3, instr_sn=False,  comment='Pit C time domain reflectometer', columns=cs616_cols, units=cs616_units, logger_sn=logger_sn,  con=con, show=show)

    # stevens hydraprobes
    hydra_cols = dict(swc='REAL', tempcorrcond='REAL', ts='REAL', tsfarenheit='REAL', cond='REAL', redipe='REAL', imdipe='REAL', tcredipe='REAL', tcimdipe='REAL')
    hydra_units = dict(swc='', tempcorrcond='Ohm-1m-1', ts='C+1', tsfarenheit='F+1', cond='Ohm-1m-1', redipe='Clb+1V-1m-1', imdipe='Clb+1V-1m-1', tcredipe='Clb+1V-1m-1', tcimdipe='Clb+1V-1m-1')
    for height in [-0.5, -0.1, -0.15, -0.3, -0.5]:
        add_instrument(shortname='hydra', site=site, height=height, instr_model='Hydraprobe', rep=1, instr_sn=False,  comment='Pit A hydraprobe: SWC, TS, electrical properties', columns=hydra_cols, units=hydra_units, logger_sn=logger_sn,  con=con, show=show)
    for height in [-0.5, -0.1, -0.15, -0.3, -0.5]:
        add_instrument(shortname='hydra', site=site, height=height, instr_model='Hydraprobe', rep=2, instr_sn=False,  comment='Pit B hydraprobe: SWC, TS, electrical properties', columns=hydra_cols, units=hydra_units, logger_sn=logger_sn,  con=con, show=show)


    # Soil heat flux plates
    gs_cols = dict(gs='REAL')
    gs_units = dict(gs='W+1m-2')
    for height, instr_sn in zip([-0.05, -0.1, -0.15], [767, 3680, 3748]):
        add_instrument(shortname='soilhf', site=site, height=height, instr_model='Unknown Heat Flux Plate', rep=1, instr_sn=instr_sn,  comment='Pit A soil heat flux, possibly hukseflux?', columns=gs_cols, units=gs_units, logger_sn=logger_sn,  con=con, show=show)        
    for height, instr_sn in zip([-0.05, -0.1, -0.15], [13041, 13042, 13051]):
        add_instrument(shortname='soilhf', site=site, height=height, instr_model='Unknown Heat Flux Plate', rep=2, instr_sn=instr_sn,  comment='Pit B soil heat flux, possibly hukseflux?', columns=gs_cols, units=gs_units, logger_sn=logger_sn,  con=con, show=show)        
    for height, instr_sn in zip([-0.05], [4647]):
        add_instrument(shortname='soilhf', site=site, height=height, instr_model='Unknown Heat Flux Plate', rep=3, instr_sn=instr_sn,  comment='Pit B soil heat flux, possibly hukseflux?', columns=gs_cols, units=gs_units, logger_sn=logger_sn,  con=con, show=show)        
    
    # soil temperature
    ts_cols = dict(ts='REAL')
    ts_units = dict(gs='W+1m-2')
    for height in [-0.05, -0.1, -0.15, -0.3, -0.5]:
        add_instrument(shortname='tsoil', site=site, height=height, instr_model='Type-T TC', rep=1, instr_sn=False,  comment='Pit C soil thermocouples', columns=ts_cols, units=ts_units, logger_sn=logger_sn,  con=con, show=show)        



def load_cs30min(con, fns):
    # read in 30-minute cs616 soiltdr summaries into database
    rx = {f'cs616_{height}':re.compile(f'^WC_C{height}cm.*|^tau616_.*({i})') for i, height in enumerate(['5', '10', '15', '30', '50'])}
    rx.update({'status':re.compile('T_CR3000.*|V_CR3000.*')})

    heights = ['neg5', 'neg10', 'neg15', 'neg30', 'neg50']
    table_names = [f'tdr_NF_{height}_3' for height in heights] + ['status_NF_0_6']
    table_names = {instr:name for instr, name in zip(rx, table_names)}

    # renaming/standardization instructions
    renaming_dict = {"TIMESTAMP":'timestamp',
                    "WC_C5cm_Avg": 'swc_avg',
                    "WC_C10cm_Avg": 'swc_avg',
                    "WC_C15cm_Avg": 'swc_avg',
                    "WC_C30cm_Avg": 'swc_avg',
                    "WC_C50cm_Avg": 'swc_avg',
                    "tau616_Avg(1)": 'period_avg',
                    "tau616_Avg(2)": 'period_avg',
                    "tau616_Avg(3)": 'period_avg',
                    "tau616_Avg(4)": 'period_avg',
                    "tau616_Avg(5)": 'period_avg',
                    "T_CR3000_Avg": 'tlogger_avg',
                    "V_CR3000_Avg": 'vlogger_avg',
                    "WC_C5cm_Std": 'swc_std',
                    "WC_C10cm_Std": 'swc_std',
                    "WC_C15cm_Std": 'swc_std',
                    "WC_C30cm_Std": 'swc_std',
                    "WC_C50cm_Std": 'swc_std',
                    "tau616_Std(1)": 'period_std',
                    "tau616_Std(2)": 'period_std',
                    "tau616_Std(3)": 'period_std',
                    "tau616_Std(4)": 'period_std',
                    "tau616_Std(5)": 'period_std',
                    "T_CR3000_Std": 'tlogger_std',
                    "V_CR3000_Std": 'vlogger_std'}

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
    for height in ['5', '10', '15', '30', '50']:
        for stat in ['Avg', 'Std']:
            for pit in ['A', 'B']:
                renaming_dict.update(
                    {f"Soil_Moist_{pit}{height}cm_{stat}":f'swc_{stat.lower()}',
                    f"Temp_Corr_Soil_Cond_{pit}{height}cm_{stat}":f'tempcorrcond_{stat.lower()}',
                    f"Soil_Temp_C_{pit}{height}cm_{stat}":f'ts_{stat.lower()}',
                    f"Soil_Temp_F_{pit}{height}cm_{stat}":f'tsfarenheit_{stat.lower()}',
                    f"Soil_Cond_{pit}{height}cm_{stat}": f'cond_{stat.lower()}',
                    f"Real_Diel_Perm_{pit}{height}cm_{stat}": f'redipe_{stat.lower()}',
                    f"Imag_Diel_Perm_{pit}{height}cm_{stat}": f'imdipe_{stat.lower()}',
                    f"Corr_Real_Diel_Perm_{pit}{height}cm_{stat}": f'tcredipe_{stat.lower()}',
                    f"Corr_Imag_Diel_Perm_{pit}{height}cm_{stat}":f'tcimdipe_{stat.lower()}'}
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
                    "Gs_A5cm_Avg": 'gs_avg',
                    "Gs_A10m_Avg": 'gs_avg',
                    "Gs_A15m_Avg": 'gs_avg',
                    "Gs_B5cm_Avg": 'gs_avg',
                    "Gs_B10m_Avg": 'gs_avg',
                    "Gs_B15m_Avg": 'gs_avg',
                    "Gs_C5cm_Avg": 'gs_avg',
                    "Ts_C5cm_Avg": 'ts_avg',
                    "Ts_C10cm_Avg": 'ts_avg',
                    "Ts_C15cm_Avg": 'ts_avg',
                    "Ts_C30cm_Avg": 'ts_avg',
                    "Ts_C50cm_Avg": 'ts_avg',
                    "Gs_A5cm_Std": 'gs_std',
                    "Gs_A10m_Std": 'gs_std',
                    "Gs_A15m_Std": 'gs_std',
                    "Gs_B5cm_Std": 'gs_std',
                    "Gs_B10m_Std": 'gs_std',
                    "Gs_B15m_Std": 'gs_std',
                    "Gs_C5cm_Std": 'gs_std',
                    "Ts_C5cm_Std": 'ts_std',
                    "Ts_C10cm_Std": 'ts_std',
                    "Ts_C15cm_Std": 'ts_std',
                    "Ts_C30cm_Std": 'ts_std',
                    "Ts_C50cm_Std": 'ts_std'}


    process_instructions(fns, rx, renaming_dict, table_names, con)

    return

def main():
    print(__name__, f'{site}_soil_manager.py')

if __name__ == '__main__':
    main()