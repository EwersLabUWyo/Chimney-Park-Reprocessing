import sqlite3 as sq
from pathlib import Path
from contextlib import closing
import sys

import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

# define some constants and such
utils_key_cols = dict(idx='INT NOT NULL', timestamp='TEXT NOT NULL', stat='TEXT NOT NULL')


def create_table(name, columns, con, primary=None, foreign=None):
    '''
    creates a sqlite3 table
    csr, con: cursor and connection objects
    name: the table name
    columns: a dict of column names as {colname:dtype}
    primary: list of str indicating primary keys
    foreign: list of tuples the form (foreign_key_name, ref_table_name, ref_primary_key_name) indicating foreign key links
    '''
    
    csr = con.cursor()

    columns = [f'{name} {_type}' for name, _type in columns.items()]
    columns = ", ".join(columns)
    query = f'CREATE TABLE IF NOT EXISTS {name}({columns})'
    
    if primary is not None:
        query += f', PRIMARY KEY({", ".join(primary)})'
        
    if foreign is not None:
        for foreign_key, ref_table, ref_key in foreign:
            query += f', FOREIGN KEY ({foreign}) REFERENCES {ref_table} ({ref_primary}) ON DELETE CASCADE ON UPDATE NO ACTION'
    
    # print(query, '\n')
    
    # send the query
    csr.execute(query)
    # save changes
    con.commit()
    
    return

def insert(name, values, columns=None, con=None):
    '''
    inserts several rows of data
    csr, con: cursor and connection objects
    name: table to insert into
    values: list of tuples, shape (nrows, ncols)
    columns: iterable of column names for associated values
    '''
    
    csr = con.cursor()
    # create a string '?, ?, ?, ...' 
    qmarks = ', '.join(['?']*len(values[0]))
    if columns is not None:
        # create a string 'col1, col2, col3, ...'
        columns = ', '.join(columns)
        query = f'INSERT INTO {name}({columns}) VALUES({qmarks})'
    
    else:
        query = f'INSERT INTO {name} VALUES({qmarks})'
    
    # print(query, '\n')

    try:
        csr.executemany(query, values)
        con.commit()
    except sq.InterfaceError as err:
        print(values[:10])
        print(err)
        sys.exit(0)
    
    return

def create_multi_index(name, columns, con):
    '''create a multi-column index on the given columns. columns is a list of str.'''
    
    csr = con.cursor()

    columns = ', '.join(columns)
    query = f'CREATE INDEX IF NOT EXISTS {name}_multidx ON {name}({columns});'
    # print(query, '\n')
    
    csr.execute(query)
    
    con.commit()
    
    
    return


    
def add_instrument(instr_model, site, height, rep, instr_sn, logger_sn, shortname, comment, columns, units, con, show):
    '''
    Add a new instrument. This will create a  new data table for this instrument, record instrument metadata in the associated instrument metadata table, and add units to a unit table.
    instr_model: str, model name
    site: str, site name (without height)
    rep: int, indicate uniqueness of this instrument/type of instrument
    instr_sn: int or bool, if not given, a unique negative integer will be chosen.
    logger_sn: int, serial number of logger attached to instrument
    shortname: str, should be short and convey instrument type. examples: irga, son, irgason, thp, soiltdr
    columns: dict of {variable:datatype} for data columns, not including timestamp or record numbers
    units: dict of {variable:units} or bool for data columns, not including timestamp or record numbers. Use the format <unit>±n<unit>±m, for example m+1s-1, or C+1. Set 'False' to not add units.
    comment: str, any additional info about the instrument.
    con: sqlite3.connect(), a connection to the sqlite3 database
    show: bool (default False), whether to display the created/modified tables.
    '''
    
    csr = con.cursor()
    
    # get serial numbers: assign instr serial number randomely if not given
    if not instr_sn:
        instr_sn = np.random.randint(-1e7, 0)
    
    # create data table for instrument
    columns.update(utils_key_cols)
    sign = ['', '', 'neg']
    # sonic_NF_1700cm_1
    # hydraprobe_NF_neg50cm_1
    instr_table_name = f'{shortname}_{site}_{sign[int(np.sign(height))]}{int(np.abs(height*100))}cm_{rep}'
    create_table(instr_table_name, columns, con)
    create_multi_index(instr_table_name, ['idx', 'timestamp', 'stat'], con)
    
    # create/update units table
    if units:
        insert('units', [(instr_sn, var_name, unit, '') for var_name, unit in units.items()], con=con)
    
    # add metadata to logger table
    insert('instruments_lu', [(shortname, instr_model, site, height, rep, instr_sn, logger_sn, instr_table_name, comment)], con=con)

    if show:
        print(instr_table_name)
        display(pd.read_sql(f'SELECT * FROM {instr_table_name}', con))
        print('instruments')
        display(pd.read_sql(f'SELECT * FROM instruments_lu', con))
        print('units')
        display(pd.read_sql(f'SELECT * FROM units', con))

    
def add_instrument(instr_model, site, height, rep, instr_sn, logger_sn, shortname, comment, columns, units, con, show=False):
    '''
    Add a new instrument. This will create a  new data table for this instrument, record instrument metadata in the associated instrument metadata table, and add units to a unit table.
    instr_model: str, model name
    site: str, site name (without height)
    rep: int, indicate uniqueness of this instrument/type of instrument
    instr_sn: int or bool, if not given, a unique negative integer will be chosen.
    logger_sn: int, serial number of logger attached to instrument
    shortname: str, should be short and convey instrument type. examples: irga, son, irgason, thp, soiltdr
    columns: dict of {variable:datatype} for data columns, not including timestamp or record numbers
    units: dict of {variable:units} or bool for data columns, not including timestamp or record numbers. Use the format <unit>±n<unit>±m, for example m+1s-1, or C+1. Set 'False' to not add units.
    comment: str, any additional info about the instrument.
    con: sqlite3.connect(), a connection to the sqlite3 database
    show: bool (default False), whether to display the created/modified tables.
    '''
    
    # print(len(columns))
    csr = con.cursor()
    
    # get serial numbers: assign instr serial number randomely if not given
    if not instr_sn:
        instr_sn = np.random.randint(-1e7, 0)
    

    columns_old = columns.copy()
    columns = utils_key_cols.copy()
    columns.update({col:datatype for col, datatype in columns_old.items()})

    # create data table for instrument
    sign = ['', '', 'neg']
    # sonic_NF_1700cm_1
    # hydraprobe_NF_neg50cm_1
    instr_table_name = f'{shortname}_{site}_{sign[int(np.sign(height))]}{int(np.abs(height*100))}cm_{rep}'
    create_table(instr_table_name, columns, con)
    create_multi_index(instr_table_name, ['idx', 'timestamp'], con)
    
    # create/update units table
    if units:
        insert('units_lu', [(instr_sn, var_name, unit, '') for var_name, unit in units.items()], con=con)
    
    # add metadata to logger table
    insert('instruments_lu', [(shortname, instr_model, site, height, rep, instr_sn, logger_sn, instr_table_name, comment)], con=con)

    if show:
        print(instr_table_name)
        display(pd.read_sql(f'SELECT * FROM {instr_table_name}', con))
        print('instruments')
        display(pd.read_sql(f'SELECT * FROM instruments_lu', con))
        print('units')
        display(pd.read_sql(f'SELECT * FROM units', con))
        
    return

def add_logger(logger_model, site, rep, logger_sn, shortname, comment, con, show):
    '''
    Add a new instrument. This will create a  new data table for this instrument, record instrument metadata in the associated instrument metadata table, and add units to a unit table.
    logger_model: str, logger model
    site: str, site name (without height)
    rep: int, indicate uniqueness of this instrument/type of instrument
    logger_sn: int, serial number of logger attached to instrument
    shortname: str, should be short and convey instrument type. examples: irga, son, irgason, thp, soiltdr
    comment: str, any additional info about the instrument.
    con: sqlite3.connect(), a connection to the sqlite3 database
    show: bool (default False), whether to display the created/modified tables.
    '''
    
    csr = con.cursor()
    
    # get serial numbers: assign instr serial number randomely if not given
    if not logger_sn:
        logger_sn = np.random.randint(-1e7, 0)
    
    # add metadata to logger table

    insert('loggers_lu', [(site, logger_model, logger_sn, shortname, comment)], con=con)
        
    return

def process_instructions(fns, rx, renaming_dict, table_names, con):
    '''given a list of file names and a set of subsetting/renaming instructions, process the given files and upload them to the database'''
    
    csr = con.cursor()

    default_cols = ['TIMESTAMP']

    pbar = tqdm(fns)
    for fn in pbar:
        fn = Path(fn)
        pbar.set_description(f'Processing {fn.name:50s}')#"/".join(fn.parts[-6:])}')
        
        # record TOA5 header data if a new file type is detected
        with open(fn, 'r') as f:
            header = f.readline().strip('"\n').split('","')
            f.readline(), f.readline(), f.readline()
            timestamp = f.readline().split(',')[0].strip('"')
        header += [timestamp]
        header = [tuple(header)]
        insert('slow_headers', header, con=con)

        # read in file, skipping metadata rows
        df = pd.read_csv(fn, skiprows=[0, 2, 3], low_memory=False)
        # apply regex filter: first find columns, then subset by those columns and rename to standard names
        cols = {instr:default_cols + list(filter(r.match, list(df.columns))) for instr, r in rx.items()}
        dfs = {instr:df[col].rename(renaming_dict, axis='columns') for instr, col in cols.items()}  # dict of dataframes, with instruments as keys
        # add an index column named "idx", since timestamps are not always unique.
        dfs = {instr:df.reset_index().rename({'index':'idx'}, axis='columns') for instr, df in dfs.items()}

        # for k,v in cols.items():
        #     print(k)
        #     print(v)
        #     print(df.columns)

        # loop through instruments and pivot longer by expanding on stat suffixes
        for instr, df in dfs.items():
            sep = '-'
            suffix = '\w+'
            i = ['timestamp', 'idx']
            j = 'stat'

            # stubs: remove the statistical suffix and then remove duplicates from all data (non-default) columns
            # e.g. [timestamp, idx, u_avg, v_avg, u_std, v_std] --> [u, v]
            stubnames = list(set([col.split(sep)[0] for col in df.drop(i, axis='columns')]))
            
            # print()
            # print(instr)
            # print('stubnames')
            # print(stubnames)
            # print('columns')
            # print(list(df.columns))
            # print('tablename')
            # print(table_names[instr])
            # print('df')
            # print(dfs[instr])

            # pivot wider, creating a 'stat' column
            dfs[instr] = pd.wide_to_long(df=df, stubnames=stubnames, i=i, j=j, sep=sep, suffix='\w+')
            dfs[instr].reset_index(inplace=True)
            # save to database
            dfs[instr].to_sql(table_names[instr], con, if_exists='append', index=False)

    return