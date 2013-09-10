# -*- coding: utf-8 -*-
"""
simple_flight_sets  : small sets of flights for testing profiles
 file repository 'local' assumes you have 7/13 files on your local drive.
 file repository 'central' is on //serrano/foqa_evolution
 
Created on Sat Aug 10 16:19:48 2013
@author: KEITHC
"""
import glob
import os

import pandas as pd

import analyser_custom_settings as settings 
import fds_oracle  # db connection

def _flight_list(files_to_process, aircraft_info, repo):
    flight_list=[]
    for f in files_to_process:
        flt={'filepath':f, 'aircraft_info':aircraft_info, 'repo':repo}
        flight_list.append(flt)
    return flight_list
    
def _flight_set_dataframe(files_to_process, aircraft_info, repo):
    
    flight_set = pd.DataFrame({'filepath': files_to_process })
    flight_set['repo']=repo
    flight_set['aircraft_info']=[{'Frame': 'B737-300_specimen', 'Manufacturer': 'Boeing', 'Precise Positioning': True, 'Series': 'B737-300', 'Family': 'B737', 'Frame Doubled': False} for f in files_to_process]
    return flight_set

def ffd897():
    '''quick test set'''
    input_dir  = settings.FFD_PATH + 'ffd40/'
    print input_dir
    files_to_process = glob.glob(os.path.join(input_dir, '*.ffd'))
    repo='keith'
    aircraft_info =  {'Frame':'FFD', 'Manufacturer':'Bombardier', 'Precise Positioning':True, 'Series': 'CRJ 700',  'Frame Doubled':False}
    return _flight_list(files_to_process, aircraft_info, repo)


def specimen_flight():
    '''FDS Specimen Flight, a partial 737-300 64wps frame'''
    input_dir  = settings.BASE_DATA_PATH + 'specimen_flight/'
    print input_dir
    files_to_process = glob.glob(os.path.join(input_dir, '*.hdf5'))
    print files_to_process
    repo='local'
    aircraft_info = {'Frame': 'B737-300_specimen', 'Manufacturer': 'Boeing', 'Precise Positioning': True, 'Series': 'B737-300', 'Family': 'B737', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)

def tiny_test():
    '''quick test set'''
    input_dir  = settings.BASE_DATA_PATH + 'tiny_test/'
    print input_dir
    files_to_process = glob.glob(os.path.join(input_dir, '*.hdf5'))
    repo='local'
    aircraft_info={'Frame': 'A320_SFIM_ED45_CFM', 'Manufacturer': 'Airbus', 'Precise Positioning': True, 'Series': 'A320-200', 'Family': 'A320', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)


def test10_shared():
    '''quick test set on serrano shared storage'''
    input_dir  = 'Y:/asias_fds/base_data/test10/'
    print input_dir
    files_to_process = glob.glob(os.path.join(input_dir, '*.hdf5'))
    repo='serrano'
    aircraft_info={'Frame': 'A320_SFIM_ED45_CFM', 'Manufacturer': 'Airbus', 'Precise Positioning': True, 'Series': 'A320-200', 'Family': 'A320', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)


def test10():
    '''quick test set'''
    input_dir  = settings.BASE_DATA_PATH + 'test10/'
    print input_dir
    files_to_process = glob.glob(os.path.join(input_dir, '*.hdf5'))
    repo='local'
    aircraft_info={'Frame': 'A320_SFIM_ED45_CFM', 'Manufacturer': 'Airbus', 'Precise Positioning': True, 'Series': 'A320-200', 'Family': 'A320', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)

    

def test_sql_jfk():
    '''sample test set based on query from Oracle fds_flight_record'''
    query = """select distinct file_path from fds_flight_record 
                 where 
                    file_repository='central' 
                    and orig_icao='KJFK' and dest_icao in ('KFLL','KMCO' )
                    --and rownum<15
                    """
    files_to_process = fds_oracle.flight_record_filepaths(query)[:40]
    repo='central'
    aircraft_info={'Frame': 'A320_SFIM_ED45_CFM', 'Manufacturer': 'Airbus', 'Precise Positioning': True, 'Series': 'A320-200', 'Family': 'A320', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)


def test_sql_jfk_local():
    '''sample test set based on query from Oracle fds_flight_record'''
    repo='local'
    query = """select distinct file_path from fds_flight_record 
                 where 
                    file_repository='REPO' 
                    and orig_icao='KJFK' and dest_icao in ('KFLL','KMCO' )
                    --and rownum<15
                    """.replace('REPO',repo)
    files_to_process = fds_oracle.flight_record_filepaths(query)[:40]
    aircraft_info={'Frame': 'A320_SFIM_ED45_CFM', 'Manufacturer': 'Airbus', 'Precise Positioning': True, 'Series': 'A320-200', 'Family': 'A320', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)


def fll_local():
    '''sample test set based on query from Oracle fds_flight_record'''
    repo='local'
    query = """select distinct file_path from fds_flight_record 
                 where 
                    file_repository='REPO' 
                    and dest_icao in ('KFLL')
                    """.replace('REPO',repo)
    files_to_process = fds_oracle.flight_record_filepaths(query) #[:40]
    aircraft_info={'Frame': 'A320_SFIM_ED45_CFM', 'Manufacturer': 'Airbus', 'Precise Positioning': True, 'Series': 'A320-200', 'Family': 'A320', 'Frame Doubled': False}
    return _flight_list(files_to_process, aircraft_info, repo)
