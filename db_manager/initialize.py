# Initializes tables for a new SQLite3 database for met data. this includes:
# * Logger metadata
# * Instrument metadata
# * Units data
# * File metadata

# Author: Alex Fox
# Initial creation: 20220728

import sqlite3 as sq

def initialize(con):
    csr = con.cursor()
    
    # create logger table
    csr.execute(
        '''
        CREATE TABLE IF NOT EXISTS loggers(
            site TEXT NOT NULL,
            logger_model TEXT NOT NULL,
            logger_sn INT NOT NULL,
            shortname TEXT NOT NULL,
            comment TEXT,
            PRIMARY KEY (logger_sn)
        );
        '''
    )
    csr.execute('''CREATE INDEX IF NOT EXISTS loggers_snidx ON loggers(logger_sn);''')
    
    # create instrument table
    csr.execute(
        '''
        CREATE TABLE IF NOT EXISTS instruments(
            shortname TEXT NOT NULL,
            instr_model TEXT NOT NULL,
            site TEXT NOT NULL,
            height INT NOT NULL,
            rep INT NOT NULL,
            instr_sn INT NOT NULL,
            logger_sn INT NOT NULL,
            instr_table TEXT NOT NULL,
            comment TEXT,
            PRIMARY KEY(instr_sn),
            FOREIGN KEY(logger_sn) REFERENCES loggers(logger_sn) 
                ON DELETE CASCADE 
                ON UPDATE NO ACTION
        );
        '''
    )
    csr.execute('''CREATE INDEX IF NOT EXISTS instruments_snidx ON instruments(instr_sn);''')
    csr.execute('''CREATE INDEX IF NOT EXISTS instruments_nameidx ON instruments(shortname);''')
    csr.execute('''CREATE INDEX IF NOT EXISTS instruments_tableidx ON instruments(instr_table);''')
    
    # units table
    csr.execute(
        '''
        CREATE TABLE IF NOT EXISTS units(
            instr_sn TEXT NOT NULL,
            variable TEXT NOT NULL,
            units TEXT NOT NULL,
            comment TEXT,
            FOREIGN KEY(instr_sn) REFERENCES instruments(instr_sn) 
                ON DELETE CASCADE 
                ON UPDATE NO ACTION
            
            )
        '''
    )
    csr.execute('''CREATE INDEX IF NOT EXISTS units_multidx ON units(instr_sn, variable)''')

    # slow file metadata
    csr.execute(
        '''
        CREATE TABLE IF NOT EXISTS slow_headers(
            format TEXT,
            station_name TEXT,
            logger_sn INT,
            logger_model TEXT,
            logger_os TEXT,
            program TEXT,
            program_signature INT,
            crbasic_table_name TEXT,
            timestamp TEXT,
            FOREIGN KEY(logger_sn) REFERENCES loggers(logger_sn) 
                ON DELETE CASCADE 
                ON UPDATE NO ACTION
            )
        '''
    )
    
    con.commit()

def main():
    print(__name__, 'NF_17_EC_manager.py')

if __name__ == '__main__':
    main()