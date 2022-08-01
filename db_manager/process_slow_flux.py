with closing(sq.connect(db_fn)) as con:
        
        # slow data variable names
        sonic_cols = dict(u='REAL', v='REAL', w='REAL', t_sonic='REAL', diag_sonic='INT')
        irga_cols = dict(co2='REAL', h2o='REAL', p_irga='REAL', diag_irga='INT')
        irgason_cols = dict(u='REAL', v='REAL', w='REAL', t_sonic='REAL', diag_sonic='INT', co2='REAL', h2o='REAL', p_irga='REAL', t_irga='REAL', diag_irga='INT', co2_strength='REAL', h2o_strength='REAL')
        status_cols = dict(v_logger='REAL', t_logger='REAL')
        
        # slow data units
        sonic_units = dict(u='m+1s-1', v='m+1s-1', w='m+1s-1', t_sonic='C+1', diag_sonic='')
        irga_units = dict(co2='mg+1m-3', h2o='g+1m-3', p_irga='kPa+1', diag_irga='')
        irgason_units = dict(u='m+1s-1', v='m+1s-1', w='m+1s-1', t_sonic='C+1', diag_sonic='', co2='mg+1m-3', h2o='g+1m-3', p_irga='kPa+1', t_irga='C+1', diag_irga='', co2_strength='', h2o_strength='')
        status_units = dict(v_logger='V+1', t_logger='C+1')
        
        show=False
        sns = [9809, 10365, 2992, 9810, 2991]

        # add EC logger metadata
        add_logger(site='NF', logger_model='CR3000', rep=2, logger_sn=sns[1], shortname='NF EC 3m',  con=con, show=show, comment='')
        add_logger(site='SF', logger_model='CR6',    rep=1, logger_sn=sns[2], shortname='SF EC 4m',  con=con, show=show, comment='')
        add_logger(site='SF', logger_model='CR3000', rep=2, logger_sn=sns[3], shortname='SF EC 7m',  con=con, show=show, comment='')
        add_logger(site='UF', logger_model='CR6',    rep=1, logger_sn=sns[4], shortname='UF EC 3m',  con=con, show=show, comment='')
        
        # add EC logger status data tables
        add_instrument(shortname='status', site='NF', height=0, instr_model='CR3000', rep=2, instr_sn=sns[1],  comment='NF EC 3m logger',  columns=status_cols, units=status_units, logger_sn=sns[1],  con=con, show=show)
        add_instrument(shortname='status', site='SF', height=0, instr_model='CR6',    rep=1, instr_sn=sns[2],  comment='SF EC 4m logger',  columns=status_cols, units=status_units, logger_sn=sns[2],  con=con, show=show)
        add_instrument(shortname='status', site='SF', height=0, instr_model='CR3000', rep=2, instr_sn=sns[3],  comment='SF EC 7m logger',  columns=status_cols, units=status_units, logger_sn=sns[3],  con=con, show=show)
        add_instrument(shortname='status', site='UF', height=0, instr_model='CR6',    rep=1, instr_sn=sns[4],  comment='UF EC 3m logger',  columns=status_cols, units=status_units, logger_sn=sns[4],  con=con, show=show)
        
        
        # add EC instrument data tables
        add_instrument(shortname='sonic',   site='NF', height=3,  instr_model='CSAT3B',  rep=1, instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=10365, con=con, show=show)
        add_instrument(shortname='sonic',   site='SF', height=7,  instr_model='CSAT3B',  rep=1, instr_sn=False, comment='', columns=sonic_cols,   units=sonic_units,   logger_sn=9810,  con=con, show=show)
        add_instrument(shortname='irga',    site='NF', height=3,  instr_model='LI7500',  rep=1, instr_sn=False, comment='', columns=irga_cols,    units=irga_units,    logger_sn=10365, con=con, show=show)
        add_instrument(shortname='irga',    site='SF', height=7,  instr_model='LI7500',  rep=1, instr_sn=False, comment='', columns=irga_cols,    units=irga_units,    logger_sn=9810,  con=con, show=show)
        add_instrument(shortname='irgason', site='SF', height=4,  instr_model='IRGASON', rep=1, instr_sn=False, comment='', columns=irgason_cols, units=irgason_units, logger_sn=2992,  con=con, show=show)
        add_instrument(shortname='irgason', site='UF', height=3,  instr_model='IRGASON', rep=1, instr_sn=False, comment='', columns=irgason_cols, units=irgason_units, logger_sn=2991,  con=con, show=show)

        def upload_slowflux(db_fn):
    with closing(sq.connect(db_fn)) as con:
        
        # slowflux_fns = list(data_dir.glob('BB*/EC/*m/20*/Converted/TOA5*Flux30Min.dat'))
        with open('slowfiles.txt') as f:
            lines = f.readlines()
            slowflux_fns = [Path(line) for line in lines]
    
        pbar = tqdm(slowflux_fns)
        for fn in pbar:
            fn = Path(fn)
            pbar.set_description(f'Processing {fn}')
            
            site = fn.parent.parent.parent.parent.parent.name
            height = fn.parent.parent.parent.name
            site = str(site) + str(height)
            if site=='BB-NF17m':
                # read in file
                
                df = pd.read_csv(fn, skiprows=[0, 2, 3])
                
                # rename to standardized names
                cols_to_drop = ['RECORD', 'n_Tot']
                renaming_dict = {
                    'TIMESTAMP':'timestamp', 
                    'Ux_CSAT3_17m_Avg': 'u.17-avg',
                    'Uy_CSAT3_17m_Avg': 'v.17-avg',
                    'Uz_CSAT3_17m_Avg': 'w.17-avg',
                    'Ts_CSAT3_17m_Avg': 't_sonic.17-avg',
                    'Ux_CSAT3_7m_Avg': 'u.7-avg',
                    'Uy_CSAT3_7m_Avg': 'v.7-avg',
                    'Uz_CSAT3_7m_Avg': 'w.7-avg',
                    'Ts_CSAT3_7m_Avg': 't_sonic.7-avg',
                    'rho_c_LI7500_Avg': 'co2.17-avg',
                    'rho_v_LI7500_Avg': 'h2o.17-avg',
                    'P_LI7500_Avg': 'p_irga.17-avg',
                    'DIAG_CSAT3_17m_Avg': 'diag_sonic.17-avg',
                    'DIAG_LI7500_Avg': 'diag_irga.17-avg',
                    'T_CR3000_Avg': 't_logger.0-avg',
                    'V_CR3000_Avg': 'v_logger.0-avg',
                    'Ux_CSAT3_17m_Std': 'u.17-std',
                    'Uy_CSAT3_17m_Std': 'v.17-std',
                    'Uz_CSAT3_17m_Std': 'w.17-std',
                    'Ts_CSAT3_17m_Std': 't_sonic.17-std',
                    'Ux_CSAT3_7m_Std': 'u.7-std',
                    'Uy_CSAT3_7m_Std': 'v.7-std',
                    'Uz_CSAT3_7m_Std': 'w.7-std',
                    'Ts_CSAT3_7m_Std': 't_sonic.7-std',
                    'rho_c_LI7500_Std': 'co2.17-std',
                    'rho_v_LI7500_Std': 'h2o.17-std',
                    'P_LI7500_Std': 'p_irga.17-std',
                    'DIAG_CSAT3_17m_Std': 'diag_sonic.17-std',
                    'DIAG_CSAT3_7m_Std': 'diag_sonic.7-std',
                    'DIAG_LI7500_Std': 'diag_irga.17-std',
                    'T_CR3000_Std': 't_logger.0-std',
                    'V_CR3000_Std': 'v_logger.0-std'}
                df = (df
                      .drop(columns=cols_to_drop)
                      .rename(renaming_dict, axis='columns'))
                
                # the above dictionary contains all possible columns. Remove the ones that we don't see in this file.
                desired_columns_sonic17 = ['timestamp', 'u.17-avg', 'v.17-avg', 'w.17-avg', 't_sonic.17-avg', 'diag_sonic.17-avg', 'u.17-std', 'v.17-std', 'w.17-std', 't_sonic.17-std']
                desired_columns_sonic7 = ['timestamp', 'u.7-avg', 'v.7-avg', 'w.7-avg', 't_sonic.7-avg', 'diag_sonic.7-avg', 'u.7-std', 'v.7-std', 'w.7-std', 't_sonic.7-std']
                desired_columns_irga17 = ['timestamp', 'co2.17-avg', 'h2o.17-avg', 'p_irga.17-avg', 'diag_irga.17-avg', 'co2.17-std', 'h2o.17-std', 'p_irga.17-std', 'diag_irga.17-std']
                desired_columns_status = ['timestamp', 't_logger.0-avg', 'v_logger.0-avg', 't_logger.0-std', 'v_logger.0-std']
                for i, desired_column in enumerate(desired_columns_sonic17):
                    if desired_column not in df.columns:
                        desired_columns_sonic17.pop(i)
                for i, desired_column in enumerate(desired_columns_sonic7):
                    if desired_column not in df.columns:
                        desired_columns_sonic7.pop(i)
                for i, desired_column in enumerate(desired_columns_irga17):
                    if desired_column not in df.columns:
                        desired_columns_irga17.pop(i)
                for i, desired_column in enumerate(desired_columns_status):
                    if desired_column not in df.columns:
                        desired_columns_status.pop(i)
                        
                # subset the data by instrument
                sonic_17 = df[desired_columns_sonic17]
                sonic_7 = df[desired_columns_sonic7]
                irga_17 = df[desired_columns_irga17]
                status = df[desired_columns_status]

                # pivot longer by expanding on height and stat suffixes
                ds = [sonic_17, sonic_7, irga_17, status]
                for di, d in enumerate(ds):
                    # turn all columns but 'timestamp' into stubs
                    cols_to_stub = d.drop(columns='timestamp').columns
                    # first: pass make stat column, index by timestamp, chop off at '-'.
                    # second pass: make height column, index by timestamp and stat, chop off at '.'
                    i = [['timestamp', 'index'], ['timestamp', 'index', 'stat']]
                    j = ['stat', 'height']
                    sep = ['-', '.']
                    suffix = ['\w+', '\d+']
                    # generate stubnames
                    stubnames = [[*set([colname.split(sep[0])[0] for colname in cols_to_stub])],
                                 [*set([colname.split(sep[1])[0] for colname in cols_to_stub])]]
                    
                    # add column 'index'
                    d = d.reset_index()
                    # pivot wider
                    for k in range(2):
                        d = pd.wide_to_long(d, stubnames[k], i=i[k], j=j[k], sep=sep[k], suffix=suffix[k])
                        # wide_to_long will turn columns i and j into index columns. Undo this so we can pivot again.
                        d=d.reset_index()
                    # remove height columns
                    d = d.drop(columns='height').rename({'index':'idx'}, axis='columns')
                    # overwrite old list entry
                    ds[di] = d
                
                # upload (append) to database
                sonic_17 = ds[0]
                sonic_7 = ds[1]
                irga_17 = ds[2]
                status = ds[3]
                
                sonic_17.to_sql('sonic_NF_17_1', con, if_exists='append', index=False)
                sonic_7.to_sql('sonic_NF_7_1', con, if_exists='append', index=False)
                irga_17.to_sql('irga_NF_17_1', con, if_exists='append', index=False)
                status.to_sql('status_NF_17_1', con, if_exists='append', index=False)
                
#             if site=='BB-NF3m':
#                 # read in file
#                 df = pd.read_csv(fn, skiprows=[0, 2, 3])
                
#                 # rename to standardized names
#                 cols_to_drop = ['RECORD', 'n_Tot']
#                 renaming_dict = {
#                     'TIMESTAMP':'timestamp', 
#                     'Ux_CSAT3B_Avg': 'u.3-avg',
#                     'Uy_CSAT3B_Avg': 'v.3-avg',
#                     'Uz_CSAT3B_Avg': 'w.3-avg',
#                     'Ts_CSAT3B_Avg': 't_sonic.3-avg',
#                     'rho_c_LI7500_Avg': 'co2.3-avg',
#                     'rho_v_LI7500_Avg': 'h2o.3-avg',
#                     'P_LI7500_Avg': 'p_irga.3-avg',
#                     'DIAG_CSAT3B_Avg': 'diag_sonic.3-avg',
#                     'DIAG_LI7500_Avg': 'diag_irga.3-avg',
#                     'T_CR3000_Avg': 't_logger.0-avg',
#                     'V_CR3000_Avg': 'v_logger.0-avg',
#                     'Ux_CSAT3B_Std': 'u.3-std',
#                     'Uy_CSAT3B_Std': 'v.3-std',
#                     'Uz_CSAT3B_Std': 'w.3-std',
#                     'Ts_CSAT3B_Std': 't_sonic.3-std',
#                     'rho_c_LI7500_Std': 'co2.3-std',
#                     'rho_v_LI7500_Std': 'h2o.3-std',
#                     'P_LI7500_Std': 'p_irga.3-std',
#                     'DIAG_CSAT3B_Std': 'diag_sonic.3-std',
#                     'DIAG_LI7500_Std': 'diag_irga.3-std',
#                     'T_CR3000_Std': 't_logger.0-std',
#                     'V_CR3000_Std': 'v_logger.0-std'}
#                 df = df.drop(columns=cols_to_drop).rename(renaming_dict, axis='columns')
                
#                 # subset the data by instrument
#                 sonic_cols = ['timestamp', 'u.3-avg', 'v.3-avg', 'w.3-avg', 't_sonic.3-avg', 'diag_sonic.3-avg', 'u.3-std', 'v.3-std', 'w.3-std', 't_sonic.3-std', 'diag_sonic.3-std']
#                 irga_cols = ['timestamp', 'co2.3-avg', 'h2o.3-avg', 'p_irga.3-avg', 'diag_irga.3-avg', 'co2.3-std', 'h2o.3-std', 'p_irga.3-std', 'diag_irga.3-std']
#                 logger_cols = ['timestamp', 't_logger.0-avg', 'v_logger.0-avg', 't_logger.0-std', 'v_logger.0-std']
                
#                 # pivot longer by expanding on height and stat suffixes
#                 ds = [df[sonic_cols], df[irga_cols], df[logger_cols]]

#                 for di, d in enumerate(ds):
#                     cols_to_stub = d.drop(columns='timestamp')
#                     i = ['timestamp', ['timestamp', 'stat']]
#                     j = ['stat', 'height']
#                     sep = ['-', '.']
#                     suffix = ['\w+', '\d+']
#                     stubnames = [[*set([colname.split(sep[0])[0] for colname in cols_to_stub])],
#                                  [*set([colname.split(sep[1])[0] for colname in cols_to_stub])]]
#                     for k in range(2):
#                         d = pd.wide_to_long(d, stubnames[k], i=i[k], j=j[k], sep=sep[k], suffix=suffix[k]).reset_index()
#                     d['site'] = 'NF3'
#                     d['replicate'] = 1

#                     ds[di] = d
                
#                 # upload (append) to database
#                 sonic = ds[0]
#                 irga = ds[1]
#                 logger = ds[2]

#                 sonic.to_sql('sonic_slow', con, if_exists='append', index=False)
#                 irga.to_sql('irga_slow', con, if_exists='append', index=False)
#                 logger.to_sql('logger_slow', con, if_exists='append', index=False)
    
#             if site=='BB-SF4m':
#                 # read in file
#                 df = pd.read_csv(fn, skiprows=[0, 2, 3])
                
#                 # rename to standardized names
#                 cols_to_drop = ['RECORD', 'DT_Avg', 'DT_Std', 'Q_Avg', 'Q_Std', 'TCDT_Avg', 'TCDT_Std', 'DBTCDT_Avg', 'DBTCDT_Std', 'CO2_sig_strgth_Avg', 'H2O_sig_strgth_Avg', 'CO2_sig_strgth_Std', 'H2O_sig_strgth_Std',]
                
#                 renaming_dict = {
#                     'TIMESTAMP':'timestamp', 
#                     'Ux_Avg': 'u.4-avg',
#                     'Uy_Avg': 'v.4-avg',
#                     'Uz_Avg': 'w.4-avg',
#                     'Ts_Avg': 't_sonic.4-avg',
#                     'CO2_Avg': 'co2.4-avg',
#                     'H2O_Avg': 'h2o.4-avg',
#                     'cell_press_Avg': 'p_irga.4-avg',
#                     'cell_tmpr_Avg': 't_irga.4-avg',
#                     'diag_sonic_Avg': 'diag_sonic.4-avg',
#                     'diag_irga_Avg': 'diag_irga.4-avg',
#                     'PTemp_C_Avg': 't_logger.0-avg',
#                     'BattV_Avg': 'v_logger.0-avg',
#                     'BattV_Min': 'v_logger.0-min',
#                     'Ux_Std': 'u.4-std',
#                     'Uy_Std': 'v.4-std',
#                     'Uz_Std': 'w.4-std',
#                     'Ts_Std': 't_sonic.4-std',
#                     'CO2_Std': 'co2.4-std',
#                     'H2O_Std': 'h2o.4-std',
#                     'cell_press_Std': 'p_irga.4-std',
#                     'cell_tmpr_Std': 't_irga.4-std',
#                     'diag_irga_Std': 'diag_sonic.4-std',
#                     'diag_sonic_Std': 'diag_irga.4-std',
#                     }
#                 df = df.drop(columns=cols_to_drop).rename(renaming_dict, axis='columns')
                
#                 # subset the data by instrument
#                 sonic_cols = ['timestamp', 'u.4-avg', 'v.4-avg', 'w.4-avg', 't_sonic.4-avg', 'diag_sonic.4-avg', 'u.4-std', 'v.4-std', 'w.4-std', 't_sonic.4-std', 'diag_sonic.4-std']
#                 irga_cols = ['timestamp', 'co2.4-avg', 'h2o.4-avg', 'p_irga.4-avg', 't_irga.4-avg', 'diag_irga.4-avg', 'co2.4-std', 'h2o.4-std', 'p_irga.4-std', 'diag_irga.4-std']
#                 logger_cols = ['timestamp', 't_logger.0-avg', 'v_logger.0-avg', 'v_logger.0-min']
                
#                 # pivot longer by expanding on height and stat suffixes
#                 ds = [df[sonic_cols], df[irga_cols], df[logger_cols]]

#                 for di, d in enumerate(ds):
#                     cols_to_stub = d.drop(columns='timestamp')
#                     i = ['timestamp', ['timestamp', 'stat']]
#                     j = ['stat', 'height']
#                     sep = ['-', '.']
#                     suffix = ['\w+', '\d+']
#                     stubnames = [[*set([colname.split(sep[0])[0] for colname in cols_to_stub])],
#                                  [*set([colname.split(sep[1])[0] for colname in cols_to_stub])]]
#                     for k in range(2):
#                         d = pd.wide_to_long(d, stubnames[k], i=i[k], j=j[k], sep=sep[k], suffix=suffix[k]).reset_index()
#                     d['site'] = 'SF4'
#                     d['replicate'] = 1

#                     ds[di] = d
                
#                 # upload (append) to database
#                 sonic = ds[0]
#                 irga = ds[1]
#                 logger = ds[2]

#                 sonic.to_sql('sonic_slow', con, if_exists='append', index=False)
#                 irga.to_sql('irga_slow', con, if_exists='append', index=False)
#                 logger.to_sql('logger_slow', con, if_exists='append', index=False)
                
#             if site=='BB-SF7m':
#                 # read in file
#                 df = pd.read_csv(fn, skiprows=[0, 2, 3])
#                 print(df.columns)
#                 display(df)
                
#                 # rename to standardized names
#                 cols_to_drop = ['RECORD', 'n_Tot']
#                 renaming_dict = {
#                     'TIMESTAMP':'timestamp', 
#                     'Ux_CSAT3B_Avg': 'u.7-avg',
#                     'Uy_CSAT3B_Avg': 'v.7-avg',
#                     'Uz_CSAT3B_Avg': 'w.7-avg',
#                     'Ts_CSAT3B_Avg': 't_sonic.7-avg',
#                     'rho_c_LI7500_Avg': 'co2.7-avg',
#                     'rho_v_LI7500_Avg': 'h2o.7-avg',
#                     'P_LI7500_Avg': 'p_irga.7-avg',
#                     'DIAG_CSAT3B_Avg': 'diag_sonic.7-avg',
#                     'DIAG_LI7500_Avg': 'diag_irga.7-avg',
#                     'T_CR3000_Avg': 't_logger.0-avg',
#                     'V_CR3000_Avg': 'v_logger.0-avg',
#                     'Ux_CSAT3B_Std': 'u.7-std',
#                     'Uy_CSAT3B_Std': 'v.7-std',
#                     'Uz_CSAT3B_Std': 'w.7-std',
#                     'Ts_CSAT3B_Std': 't_sonic.7-std',
#                     'rho_c_LI7500_Std': 'co2.7-std',
#                     'rho_v_LI7500_Std': 'h2o.7-std',
#                     'P_LI7500_Std': 'p_irga.7-std',
#                     'DIAG_CSAT3B_Std': 'diag_sonic.7-std',
#                     'DIAG_LI7500_Std': 'diag_irga.7-std',
#                     'T_CR3000_Std': 't_logger.0-std',
#                     'V_CR3000_Std': 'v_logger.0-std'}
#                 df = df.drop(columns=cols_to_drop).rename(renaming_dict, axis='columns')
                
#                 # subset the data by instrument
#                 sonic_cols = ['timestamp', 'u.7-avg', 'v.7-avg', 'w.7-avg', 't_sonic.7-avg', 'diag_sonic.7-avg', 'u.7-std', 'v.7-std', 'w.7-std', 't_sonic.7-std', 'diag_sonic.7-std']
#                 irga_cols = ['timestamp', 'co2.7-avg', 'h2o.7-avg', 'p_irga.7-avg', 'diag_irga.7-avg', 'co2.7-std', 'h2o.7-std', 'p_irga.7-std', 'diag_irga.7-std']
#                 logger_cols = ['timestamp', 't_logger.0-avg', 'v_logger.0-avg', 't_logger.0-std', 'v_logger.0-std']
                
#                 # pivot longer by expanding on height and stat suffixes
#                 ds = [df[sonic_cols], df[irga_cols], df[logger_cols]]

#                 for di, d in enumerate(ds):
#                     d.reset_index(inplace=True)
#                     cols_to_stub = d.drop(columns='timestamp')
#                     i = [['timestamp', 'index'], ['timestamp', 'index', 'stat']]
#                     j = ['stat', 'height']
#                     sep = ['-', '.']
#                     suffix = ['\w+', '\d+']
                    
                    
#                     stubnames = [[*set([colname.split(sep[0])[0] for colname in cols_to_stub])],
#                                  [*set([colname.split(sep[1])[0] for colname in cols_to_stub])]]
#                     for k in range(2):
                        
#                         print(len(set(d.timestamp)))
#                         print(len(d.timestamp))
                        
#                         d = pd.wide_to_long(d, stubnames[k], i=i[k], j=j[k], sep=sep[k], suffix=suffix[k]).reset_index()
#                     d['site'] = 'SF7'
#                     d['replicate'] = 1

#                     ds[di] = d
                
#                 # upload (append) to database
#                 sonic = ds[0]
#                 irga = ds[1]
#                 logger = ds[2]

#                 sonic.to_sql('sonic_slow', con, if_exists='append', index=False)
#                 irga.to_sql('irga_slow', con, if_exists='append', index=False)
#                 logger.to_sql('logger_slow', con, if_exists='append', index=False)

#             if site=='BB-UF3m':
#                 # read in file
#                 df = pd.read_csv(fn, skiprows=[0, 2, 3])
#                 # rename to standardized names
#                 cols_to_drop = ['RECORD', 'CO2_sig_strgth_Avg', 'H2O_sig_strgth_Avg', 'CO2_sig_strgth_Std', 'H2O_sig_strgth_Std',]

                
#                 renaming_dict = {
#                     'TIMESTAMP':'timestamp', 
#                     'Ux_Avg': 'u.3-avg',
#                     'Uy_Avg': 'v.3-avg',
#                     'Uz_Avg': 'w.3-avg',
#                     'Ts_Avg': 't_sonic.3-avg',
#                     'CO2_Avg': 'co2.3-avg',
#                     'H2O_Avg': 'h2o.3-avg',
#                     'cell_press_Avg': 'p_irga.3-avg',
#                     'cell_tmpr_Avg': 't_irga.3-avg',
#                     'diag_sonic_Avg': 'diag_sonic.3-avg',
#                     'diag_irga_Avg': 'diag_irga.3-avg',
#                     'PTemp_C_Avg': 't_logger.0-avg',
#                     'BattV_Avg': 'v_logger.0-avg',
#                     'BattV_Min': 'v_logger.0-min',
#                     'Ux_Std': 'u.3-std',
#                     'Uy_Std': 'v.3-std',
#                     'Uz_Std': 'w.3-std',
#                     'Ts_Std': 't_sonic.3-std',
#                     'CO2_Std': 'co2.3-std',
#                     'H2O_Std': 'h2o.3-std',
#                     'cell_press_Std': 'p_irga.3-std',
#                     'cell_tmpr_Std': 't_irga.3-std',
#                     'diag_irga_Std': 'diag_sonic.3-std',
#                     'diag_sonic_Std': 'diag_irga.3-std',
#                     }
#                 df = df.drop(columns=cols_to_drop).rename(renaming_dict, axis='columns')
                
#                 # subset the data by instrument
#                 sonic_cols = ['timestamp', 'u.3-avg', 'v.3-avg', 'w.3-avg', 't_sonic.3-avg', 'diag_sonic.3-avg', 'u.3-std', 'v.3-std', 'w.3-std', 't_sonic.3-std', 'diag_sonic.3-std']
#                 irga_cols = ['timestamp', 'co2.3-avg', 'h2o.3-avg', 'p_irga.3-avg', 't_irga.3-avg', 'diag_irga.3-avg', 'co2.3-std', 'h2o.3-std', 'p_irga.3-std', 'diag_irga.3-std']
#                 logger_cols = ['timestamp', 't_logger.0-avg', 'v_logger.0-avg', 'v_logger.0-min']
                
#                 # pivot longer by expanding on height and stat suffixes
#                 ds = [df[sonic_cols], df[irga_cols], df[logger_cols]]

#                 for di, d in enumerate(ds):
#                     cols_to_stub = d.drop(columns='timestamp')
#                     i = ['timestamp', ['timestamp', 'stat']]
#                     j = ['stat', 'height']
#                     sep = ['-', '.']
#                     suffix = ['\w+', '\d+']
#                     stubnames = [[*set([colname.split(sep[0])[0] for colname in cols_to_stub])],
#                                  [*set([colname.split(sep[1])[0] for colname in cols_to_stub])]]
#                     for k in range(2):
#                         d = pd.wide_to_long(d, stubnames[k], i=i[k], j=j[k], sep=sep[k], suffix=suffix[k]).reset_index()
#                     d['site'] = 'UF3'
#                     d['replicate'] = 1

#                     ds[di] = d
                
#                 # upload (append) to database
#                 sonic = ds[0]
#                 irga = ds[1]
#                 logger = ds[2]

#                 sonic.to_sql('sonic_slow', con, if_exists='append', index=False)
#                 irga.to_sql('irga_slow', con, if_exists='append', index=False)
#                 logger.to_sql('logger_slow', con, if_exists='append', index=False)

def main():
    print(__name__, 'NF_17_EC_manager.py')

if __name__ == '__main__':
        main()