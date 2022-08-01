import re
import pandas as pd

fn = '/Volumes/TempData/Bretfeld Mario/Chimney/Data/BB-NF/EC/17m/20191031/Converted/TOA5_9809.Flux30Min.dat'
df = pd.read_csv(fn, skiprows=[0, 2, 3])

default_cols = ['TIMESTAMP']
r = re.compile(".*CSAT3_17m.*")
sonic_17_cols = list(filter(r.match, df.columns))

r = re.compile(".*CSAT3_7m.*")
sonic_7_cols = list(filter(r.match, df.columns))

r = re.compile(".*LI7500m.*")
irga_cols = list(filter(r.match, df.columns))

r = re.compile(".*CR3000.*")
logger_cols = list(filter(r.match, df.columns))

renaming_dict = {
        'TIMESTAMP':'TIMESTAMP_STAT', 
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
        'P_LI7500_Avg': 'pa_irga_avg',
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

sonic_17 = df[default_cols + sonic_17_cols].rename(renaming_dict, axis='columns')

print(sonic_17)

stubnames='u_avg'
d = pd.wide_to_long(d, , i=i[k], j=j[k], sep=sep[k], suffix=suffix[k])