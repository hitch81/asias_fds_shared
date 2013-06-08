# -*- coding: utf-8 -*-
"""
Database routines -- Oracle 

@author: keithc
"""
import analyser_custom_settings
import cx_Oracle as ora

def get_connection():
    ''' connect to Oracle
    
        connection.txt should contain a single connection string,
        e.g. scott/tiger@servername   (username/password@server)
        
        The server needs to have been previously configured in Oracle client,
          for example using a TNSnames entry or an LDAP connection.
    '''
    with open(analyser_custom_settings.SHARED_CODE_PATH+'connection.txt') as f:
        connection_string = f.read()
        
    cn = ora.connect(connection_string)
    return cn 

    
def insert_from_ordered_dict(my_record, table):
    '''appends a recond to Oracle from an OrderedDict.  It does NOT check integrity or uniqueness!'''
    with get_connection() as cn:
        cur =cn.cursor()
        cols = ','.join(my_record.keys())
        colsyms = ','.join([':'+k for k in my_record.keys()])
        isql = """insert /*append*/ into TABLE(COLS) values (SYMS)""".replace('TABLE',table).replace('COLS',cols).replace('SYMS',colsyms)           
        cur.execute(isql, my_record.values())
        cn.commit()
        cur.close()


def flight_record_filepaths(query):
    '''Pass query like 'select file_path from fds_flight_record where ...'
       Get back a list of files to process in your profile
    '''
    with get_connection() as cn:
        cur = cn.cursor()
        cur.execute(query)       
        files_to_process = [fld[0] for fld in cur.fetchall()]
        cur.close()
    return files_to_process



