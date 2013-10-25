# -*- coding: utf-8 -*-
"""
Airport and Runway AFR set

Goal: build airport and runway achieved flight record entries--
      'AFR Landing Airport', 'AFR Landing Runway', 'AFR Takeoff Airport', 'AFR Takeoff Runway'
      
Created on Thu Oct 10 15:27:25 2013
@author: KEITHC
"""
import pdb
import cPickle as pickle
import fds_oracle       

def jeplat2dd(jeplat):
    ns = jeplat[0]
    sign = 1.0 if ns=='N' else -1.0
    dms=jeplat[1:].split('-')
    dd = sign* int(dms[0]) + float(dms[1])/60. + float(dms[2])/3600. 
    return dd
    
def jeplon2dd(jeplon):
    ns = jeplon[0]
    sign = 1.0 if ns=='E' else -1.0
    dms=jeplon[1:].split('-')
    dd = sign* int(dms[0]) + float(dms[1])/60. + float(dms[2])/3600. 
    return dd
    
def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]


def get_apt_dict(cur):
    '''build apt afrs in a dictionary keyed by ICAO id, with entries like:
    {
     'code': {'icao': 'KEWR', 'iata': ''}, 'icao': 'KEWR', 
     'name': 'NEWARK LIBERTY INTL', 
     'update_month': 1204, 'region': 'USA', 
     'APT_LONGITUDE': 'W074-10-07.18', 'longitude': -73.83055555555555, 
     'APT_LATITUDE':  'N40-41-32.99',  'latitude': 40.69472222222222, 
     'elevation': '18', 
     'magnetic_variation': 'W013.0', 
   }
   '''
    apt_sql='''select  apt_id as "icao",  
                       apt_name as "name", 
                       apt_area as "region",
                       apt_latitude,   
                       apt_longitude,
                       apt_elevation as "elevation",  
                       apt_mag_var as "magnetic_variation",    
                       ap.cycle_num as "update_month"
                from crs_prod.jep_ge_apt@crsprod ap
                where cycle_num=1204'''
                    
    cur.execute(apt_sql)
    apt_dictlist = rows_to_dict_list(cur)
    apt_dict = {}
    for apd in apt_dictlist:
        icao = apd['icao']
        apd['code'] = {'icao':icao, 'iata':''}
        apd['latitude']= jeplat2dd(apd['APT_LATITUDE'])
        apd['longitude']= jeplon2dd(apd['APT_LONGITUDE'])
        if apd['latitude']<-90.0 or apd['latitude']>90.0:
            print "BAD LAT!!!", icao, apd['latitude']
            pdb.set_trace()
        if apd['longitude']<-180.0 or apd['longitude']>180.0:
            print "BAD LON!!!", icao, apd['longitude']
            pdb.set_trace()
            
        apt_dict[icao] = apd
    return apt_dict

def get_runways(cur):
    ''' AFR runways: need to pull together start and end of each. Example:
      URL: 'https://polaris-test.flightdataservices.com/api/airport/2744/runway/nearest.json?ll=40.683101%2C-74.171306&heading=37.265625'
      Response: {
    	'identifier': '04R', 
    	'id': 11729
    	'strip': {'width': 150, 'length': 10000, 'id': 5865, 'surface': 'ASP'}, 
    	'start': {'latitude': 40.677583, 'longitude': -74.174244}, 
        'end': {'latitude': 40.702289, 'longitude': -74.158536}, 
    	'magnetic_heading': 38.7, 
    	'localizer': {'latitude': 40.704428, 'frequency': 108700.0, 'longitude': -74.157178, 'heading': 39, 'beam_width': 4.2}, 
    	'glideslope': {'latitude': 40.682664, 'angle': 3.0, 'longitude': -74.169414, 'threshold_distance': 1060}, 
    	}
    '''
    rwy_sql='''select rwy_apt_id,           -- eg KEWR   
               rwy_Id,                      -- 'identifier' eg 'RW04L'.  Convert to '04L'  
               cycle_num,                   -- eg '1204'
               rwy_latitude,                -- convert dms
               rwy_longitude,               -- convert dms
               rwy_landing_threshold_elev,  --units ok?; use for localizer elevation
               rwy_threshold_distance,
               rwy_mag_bearing,
               rwy_length,            
               null as surface,       -- OK for now. NFDC has it for US     
               w.width 
        from 
        (
        select *
            from crs_prod.jep_ge_rwy@crsprod rwy
            where cycle_num=1204 
          ---rwy_apt_id ='KEWR' and --data_year>=2011 
        ) rw left join
        (
        select jepcode, runwayid, width
            from crs_prod.jep_runway_arc@crsprod rwy
            where data_year>=2011 
               --and jepcode ='KEWR' 
        ) w
        on w.jepcode=rw.rwy_apt_id and trim(w.runwayid)=rw.rwy_id
        '''
        
    #sample return record    
    '''{'RWY_APT_ID': 'KDTW', 'RWY_ID': 'RW03L', 'CYCLE_NUM': 1204, 
        'RWY_LENGTH': '8501',  'RWY_LATITUDE': 'N42-12-28.20', 'RWY_LONGITUDE': 'W083-21-04.38', 
        'RWY_LANDING_THRESHOLD_ELEV': '636', 'RWY_THRESHOLD_DISTANCE': '0', 
        'SURFACE': None, 'RWY_WIDTH': None}'''
        
    cur.execute(rwy_sql)
    rwy_dictlist = rows_to_dict_list(cur)                
    #group runways by apt
    apt_rwy = {}        
    for rw in rwy_dictlist:
        apid  = rw['RWY_APT_ID']
        #if apid=='KGAD':
        #   pdb.set_trace()
        apd = apt_rwy[apid] if apt_rwy.has_key(apid) else {} #apt group of rwy dicts
        
        rwd = {}
        rwid = rw['RWY_ID'][2:].strip()        
        rwd['identifier']=rwid        
        rwd['id']=None
        rwd['cycle_num']=rw['CYCLE_NUM']
        rwd['magnetic_heading'] = rw['RWY_MAG_BEARING']
        
        rwd['RWY_LATITUDE']=rw['RWY_LATITUDE']
        rwd['RWY_LONGITUDE']=rw['RWY_LONGITUDE']
        end_lat=jeplat2dd(rw['RWY_LATITUDE'])
        end_lon=jeplon2dd(rw['RWY_LONGITUDE'])
        rwd['end'] = {'latitude':end_lat, 'longitude':end_lon}
        rwd['strip'] = {'width':rw['WIDTH'], 'length':rw['RWY_LENGTH'], 'id':None, 'surface':None}
        apd[rwid] = rwd #add runway dict to apt grouping
        apt_rwy[apid]=apd
    return apt_rwy
    

def opposite_runway(rwid):
    """given a runway id like '04L' return the opposite direction, eg '22R' """
    rwdir = int(rwid[:2])       
    rwsfx = rwid[-1] if len(rwid)==3 else ''        
    oppdir = rwdir+18 if rwdir<=18 else rwdir-18
    oppcode = {'L':'R', 'R':'L', 'C':'C', 'T':'T', '':''}
    oppsfx = oppcode[rwsfx]
    #print rwsfx, oppsfx
    oppid = '0'+str(oppdir)+oppsfx if oppdir<10 else str(oppdir)+oppsfx
    return oppid        

def add_start(apt_rwy):
    #add start lat/lon using opposite direction : populate in 2nd pass
    for apt, aprwys in apt_rwy.items():
        #aprwys is a group of rwy dicts for a given airport
        for rw in aprwys.values():
            rwid = rw['identifier']
            oppid = opposite_runway(rwid)
            opprw= aprwys.get(oppid) #get rwy dict for opposite direction rwy
            if opprw:
                rw['start'] = {'latitude':opprw['end']['latitude'], 'longitude':opprw['end']['longitude']}
            else:
                rw['start'] = {'latitude':None, 'longitude':None}
            if  rw['start']['latitude']==rw['end']['latitude'] and rw['start']['longitude']==rw['end']['longitude']:
                print 'same start and end loc',apt, rwid, oppid                
                #pdb.set_trace()
    return apt_rwy
    
def add_ils(cur, apt_rwy):
    '''add glideslope and localizer dicts'''
    ils_sql = '''select ge.*, 
                       glideslopelatitude, glideslopelongitude, gs_threshold_distance,
                       LOCALIZERWIDTH
                from
                (
                select ils_apt_id,            -- eg KEWR   
                                   ils_runway_Id,         -- 'identifier' e.g 'RW04L'.  Convert to '04L'
                                   cycle_Num,             --eg 1204
                                   ILS_GLIDE_SLOPE_ANGLE, -- glideslope|angle 
                                   ILS_GLIDE_SLOPE_ELEV,  -- glideslope|elevation.  required? units ok? feet/meters
                                   ILS_LOC_FREQ,          --localizer|frequency. eg '111.50' Convert to 111500.0
                                   ILS_BEARING,           --localizer|heading,   eg '035.0'  Convert to integer.
                                -- localizer | elevation  --use elevation of far runway end?
                                   ILS_LOC_LATITUDE,      --localizer|latitude (convert dms)
                                   ILS_LOC_LONGITUDE     --localizer|longitude (convert dms)   
                            from crs_prod.jep_ge_ils@crsprod
                            where cycle_num=1204 
                            --      and ils_apt_id ='KDTW'
                ) ge
                left join
                (
                select jepcode, runwayid, 
                        glideslopelatitude, glideslopelongitude, glideslopeposition as gs_threshold_distance,
                       LOCALIZERWIDTH
                from crs_prod.JEP_ILSLOCALIZERGLIDESLOPE_ARC@crsprod
                where
                 data_year>=2011
                 ---and jepcode='KEWR'
                 ) jo
                 on jo.jepcode=ge.ils_apt_id and trim(jo.runwayid)=trim(ge.ils_runway_id)
                 '''
 
    cur.execute(ils_sql)
    ils_dictlist = rows_to_dict_list(cur)
    print ils_dictlist[0]
    '''{'ILS_APT_ID': 'KDTW', 'ILS_RUNWAY_ID': 'RW04L', 'CYCLE_NUM': 1204, 
        'ILS_LOC_LATITUDE': 'N42-13-43.23', 'ILS_LOC_LONGITUDE': 'W083-21-52.16', 
        'LOCALIZERWIDTH': 0.0, 'ILS_LOC_FREQ': '111.95', 
        'ILS_BEARING': '035.0' #=localizer|heading
        'GLIDESLOPELONGITUDE': -83.383333, 'GLIDESLOPELATITUDE': 42.205278, 
        'GS_THRESHOLD_DISTANCE': 1065, 'ILS_GLIDE_SLOPE_ELEV': '641', 
        'ILS_GLIDE_SLOPE_ANGLE': '3.00',   } '''
        
     #'localizer': {'latitude': 40.704428, 'longitude': -74.157178, 
     #                'frequency': 108700.0, , 'heading': 39, 'beam_width': 4.2}, 
     #'glideslope': {'latitude': 40.682664, 'longitude': -74.169414, 
     #                 'angle': 3.0, 'threshold_distance': 1060}, 

    for ils in ils_dictlist:
        #aprwys is a group of rwy dicts for a given airport
        apid = ils['ILS_APT_ID']
        rwid = ils['ILS_RUNWAY_ID'][2:]
        #print 'add ils: ', apid, rwid
        loc = { 'latitude':   jeplat2dd(ils['ILS_LOC_LATITUDE']), 
                'longitude':  jeplon2dd(ils['ILS_LOC_LONGITUDE']), 
                'frequency':  float(ils['ILS_LOC_FREQ'])*1000.0, 
                'heading':    int(float(ils['ILS_BEARING'])), 
                'beam_width': ils['LOCALIZERWIDTH'] 
              }   
        try:
            gs_angle = float(ils['ILS_GLIDE_SLOPE_ANGLE']) 
        except: 
            #print 'bad angle', ils['ILS_GLIDE_SLOPE_ANGLE']
            gs_angle = None
        gs =  { 'latitude':  ils['GLIDESLOPELATITUDE'],
                'longitude': ils['GLIDESLOPELONGITUDE'],

                'angle': gs_angle, 
                'threshold_distance': ils['GS_THRESHOLD_DISTANCE']
              }
        if apt_rwy.has_key(apid) and apt_rwy[apid].has_key(rwid):
            apt_rwy[apid][rwid]['localizer']=loc
            apt_rwy[apid][rwid]['glideslope']=gs    
        else:
            print 'not found in apt_rwy', apid, rwid
    
    return apt_rwy

def get_apt_rwy_dict(cur):
    apt_rwy = get_runways(cur)          
    apt_rwy = add_start( apt_rwy)        
    apt_rwy = add_ils(cur, apt_rwy)        
    return apt_rwy

####################################################################
if __name__=='__main__':
    mylat='N40-41-32.99'
    mylon='W074-10-07.18'
    print mylat, jeplat2dd(mylat)
    print mylon, jeplon2dd(mylon)

    print '22R', opposite_runway('22R')
    print '04L', opposite_runway('04L')
    print '36', opposite_runway('36')
    print '01', opposite_runway('01')
    print '18', opposite_runway('18')


    cn = fds_oracle.get_connection()
    print 'connected'
    cur = cn.cursor()
    apt_dict = get_apt_dict(cur)
    print  "apt_dict['KEWR']", apt_dict['KEWR']   
    
    apt_rwy = get_apt_rwy_dict(cur)
    
    cur.close()
    cn.close()   
    print apt_rwy['KEWR']['22R']
    '''
    {'identifier': '22R', 'id': None
     'start': {'latitude': 40.67777777777778, 'longitude': -73.83055555555555}, 
     'end': {'latitude': 40.69472222222222, 'longitude': -73.8475}, 
     'cycle_num': 1204, 
     'localizer': {'latitude': 40.67777777777778, 'beam_width': 3.83, 'frequency': 110750.0, 
                   'heading': 219, 'longitude': -73.83055555555555}, 
     'glideslope': {'latitude': 40.696388999999996, 'angle': 3.0, 'longitude': -74.165, 
                    'threshold_distance': 914}, 
      'magnetic_heading': '218.9', 
      'strip': {'width': 150, 'length': '11000', 'id': None, 'surface': None}, 
      }
    '''
    with open('apt.pkl','wb') as fa:
        pickle.dump(apt_dict, fa)
    with open('apt_rwy.pkl','wb') as fr:        
        pickle.dump(apt_rwy, fr)
    print 'done'