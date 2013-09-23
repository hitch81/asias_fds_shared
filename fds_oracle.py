# -*- coding: utf-8 -*-
"""
Database routines -- Oracle 

@author: keithc
"""
import os
from collections import OrderedDict
import cx_Oracle as ora
import analyser_custom_settings


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

               
def oracle_execute(connection, sql, values=None):
    '''run and commit some sql'''
    cur =connection.cursor()
    if values:
        cur.execute(sql, values)
    else:
        cur.execute(sql)
    connection.commit()
    cur.close()
 
       
def oracle_executemany(connection, sql, values):
    '''run and commit some sql'''
    cur =connection.cursor()
    cur.executemany(sql, values)
    connection.commit()
    cur.close()


def record_to_csv(record, dest_path):
    '''append data from a list as a record to a CSV file.  assumes simple fields.'''
    #header = record.keys()
    row =  [ str(v) for v in record]
    with open(dest_path, 'at') as dest:
         dest.write( ','.join(row) +'\n')
               
       
def dict_to_oracle(cn, mydict, table):
    cols = ','.join(mydict.keys())
    colsyms = ','.join([':'+k for k in mydict.keys()])
    isql = """insert /*append*/ into TABLE (COLS) values (SYMS)""".replace('TABLE',table).replace('COLS',cols).replace('SYMS',colsyms)           
    #pdb.set_trace()
    oracle_execute(cn, isql, mydict.values())



def analyzer_to_oracle(cn, short_profile, res, params, flight, output_dir, output_path_and_file): # file_repository in Flight
        file_repository = flight.file_repository
        flight_file = os.path.basename(flight.filepath)
        kti_to_oracle(cn, short_profile, flight_file, output_path_and_file, res['kti'], file_repository)
        phase_to_oracle(cn, short_profile, flight_file, output_path_and_file, res['phase'], file_repository)
        kpv_to_oracle(cn, short_profile, flight_file, output_path_and_file, params, res['kpv'], file_repository)
        if short_profile=='base':  # for base analyze, store flight record
             flight_record = get_flight_record(flight.filepath, output_path_and_file, flight.aircraft_info, res['attr'], res['approach'], res['kti'], file_repository) # an OrderedDict
             save_flight_record(cn, flight_record, output_dir, output_path_and_file)                     
        #logger.debug('done ora out')
  


def get_flight_record(source_file, output_path_and_file,aircraft_info, flight, approach, kti, file_repository='central'):
    '''build a record-per-flight summary from the base analysis    '''
    flight_file = source_file
    registration = aircraft_info['Tail Number'] 
    base_file = os.path.basename(output_path_and_file)
    flt = OrderedDict([ ('file_repository',file_repository), ('source_file',flight_file), 
                        ('file_path', output_path_and_file), ('base_file_path', base_file), 
                        ('tail_number',registration), ('fleet_series', aircraft_info['Series']), ])    
                        
    attr = dict([(a.name, a.value) for a in flight])
    flt['operator']= 'xxx'
    flt['analyzer_version'] = attr.get('FDR Version','')
    flt['flight_type'] = attr.get('FDR Flight Type','')     
    flt['analysis_time'] = attr.get('FDR Analysis Datetime',None)
    
    lift = [k.index for k in kti if k.name=='Liftoff']
    flt['liftoff_min']        = min(lift) if len(lift)>0 else None
    tclimb = [k.index for k in kti if k.name=='Top of Climb']
    flt['top_of_climb_min']   = min(tclimb) if len(tclimb)>0 else None
    tdescent = [k.index for k in kti if k.name=='Top of Descent']
    flt['top_of_descent_min'] = min(tdescent) if len(tdescent)>0 else None
    tdown =[k.index for k in kti if k.name=='Touchdown']
    flt['touchdown_min']      = min(tdown) if len(tdown)>0 else None   
    flt['duration']       = attr.get('FDR Duration',None)

    if attr.get('FDR Takeoff Airport',None): #key must exist and contain a val other than None
        flt['orig_icao'] = attr['FDR Takeoff Airport']['code'].get('icao',None)
        flt['orig_iata'] = attr['FDR Takeoff Airport']['code'].get('iata',None)
        flt['orig_elevation'] = attr['FDR Takeoff Airport'].get('elevation',None)
    else:
        flt['orig_icao']=''; flt['orig_iata']=''; flt['orig_elevation']=None

    if attr.get('FDR Takeoff Runway',None):
        flt['orig_rwy'] = attr['FDR Takeoff Runway'].get('identifier',None)
        flt['orig_rwy_length'] = attr['FDR Takeoff Runway']['strip'].get('length',None)
    else:
        flt['orig_rwy']=''; flt['orig_rwy_length']=None
        
    if attr.get('FDR Landing Airport',None):
        flt['dest_icao'] = attr['FDR Landing Airport']['code'].get('icao',None)
        flt['dest_iata'] = attr['FDR Landing Airport']['code'].get('iata',None)
        flt['dest_elevation'] = attr['FDR Landing Airport'].get('elevation',None)
    else:
        flt['dest_icao']=''; flt['dest_iata']=''; flt['dest_elevation']=None

    if attr.get('FDR Landing Runway',None):
        flt['dest_rwy'] = attr['FDR Landing Runway'].get('identifier',None)
        flt['dest_rwy_length'] = attr['FDR Landing Runway']['strip'].get('length',None)
        if attr['FDR Landing Runway'].has_key('glideslope'):
            flt['glideslope_angle'] = attr['FDR Landing Runway']['glideslope'].get('angle',None)
        else:
            flt['glideslope_angle']=None
    else:
        flt['dest_rwy']=''; flt['dest_rwy_length']=None; flt['glideslope_angle']=None

    landing_count=0; go_around_count=0; touch_and_go_count=0
    for appr in approach:
        atype = appr.type
        if atype=='LANDING':        landing_count+=1
        elif atype=='GO_AROUND':    go_around_count+=1
        elif atype=='TOUCH_AND_GO': touch_and_go_count+=1
        else: pass
    flt['landing_count']        = landing_count
    flt['go_around_count']      = go_around_count
    flt['touch_and_go_count']   = touch_and_go_count

    flt['other_json'] = ''                  
    #dump_flight_attributes(res['flight'])
    return flt



def save_flight_record(cn, flight_record, OUTPUT_DIR, output_path_and_file):
     record_to_csv(flight_record.values(), OUTPUT_DIR+'flight_record.csv')
     repo = flight_record['file_repository']
     src = flight_record['source_file']
     dsql= """delete from fds_flight_record where file_repository='REPO' and source_file='SRC'""".replace('REPO',repo).replace('SRC', src)
     oracle_execute(cn, dsql)
     #with hdfaccess.file.hdf_file(output_path_and_file) as hfile:
     #    flight_record['recorded_parameters'] = ','.join(hfile.lfl_keys())
     dict_to_oracle(cn, flight_record, 'fds_flight_record')
     #logger.debug(flight_record)


def kti_to_oracle(cn, profile, flight_file, output_path_and_file, kti, file_repository='central'):
    '''node: index name datetime latitude longitude'''
    if profile=='base':
        base_file = os.path.basename(output_path_and_file)
    else:
        base_file = os.path.basename(flight_file)
        
    rows = []    
    for value in kti:
        vals = [profile, flight_file, value.name, float(value.index), base_file, file_repository]
        if value.index and value.index>=0:
            rows.append( vals )    
        else:
            print 'suspect kti index', value.name, value.index
    dsql= """delete from fds_kti where file_repository='REPO' and source_file='SRC' and profile='PROFILE'""".replace('PROFILE',profile).replace('REPO',file_repository).replace('SRC', flight_file)
    oracle_execute(cn, dsql)

    isql = """insert /*append*/ into fds_kti (profile, source_file,  name,  time_index, base_file_path, file_repository) 
                                    values (:profile, :source_file, :name, :time_index, :base_file_path, :file_repository)"""                
    oracle_executemany(cn, isql, rows)


def kpv_to_oracle(cn, profile, flight_file, output_path_and_file, params, kpv, file_repository='central'):
    '''node: index value name slice datetime latitude longitude'''
    if profile=='base':
        base_file = os.path.basename(output_path_and_file)
    else:
        base_file=os.path.basename(flight_file)
    
    rows = []    
    for value in kpv:
        try:
            units = params.get(value.name).units
        except:
            units = None
        vals = [profile, flight_file, value.name, float(value.index), float(value.value), base_file, units, file_repository ] 
        rows.append( vals )
    dsql= """delete from fds_kpv where file_repository='REPO' and source_file='SRC' and profile='PROFILE'""".replace('PROFILE',profile).replace('REPO',file_repository).replace('SRC', flight_file)
    oracle_execute(cn, dsql)
    isql = """insert /*append*/ into fds_kpv (profile, source_file,  name,  time_index,  value,  base_file_path,  units, file_repository) 
                                    values (:profile, :source_file, :name, :time_index, :value, :base_file_path, :units, :file_repository)"""
    oracle_executemany(cn, isql, rows)

    
def phase_to_oracle(cn, profile, flight_file, output_path_and_file, phase_list, file_repository='central'):
    '''node: 'name slice start_edge stop_edge'''
    if profile=='base':
        base_file = os.path.basename(output_path_and_file)
    else:
        base_file=os.path.basename(flight_file)
    rows = []    
    for value in phase_list:
        vals = [profile, flight_file, value.name, float(value.start_edge), float(value.stop_edge), value.stop_edge-value.start_edge, base_file, file_repository ]                
        rows.append( vals )
    dsql= """delete from fds_phase where file_repository='REPO' and source_file='SRC' and profile='PROFILE'""".replace('PROFILE',profile).replace('REPO',file_repository).replace('SRC', flight_file)
    oracle_execute(cn, dsql)
    isql = """insert /*append*/ into fds_phase (profile, source_file,  name,  time_index,  stop_edge, duration, base_file_path, file_repository) 
                                    values (:profile, :source_file, :name,   :time_index, :stop_edge, :duration, :base_file_path, :file_repository)"""
    oracle_executemany(cn, isql, rows)

