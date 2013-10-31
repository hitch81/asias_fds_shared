# -*- coding: utf-8 -*-
"""
    Helper functions for foqa-test, base and profile generation and reporting
    Created on Tue Apr 16 15:42:57 2013
    @author: KEITHC
"""
import pdb
import sys, traceback, os, glob, shutil, logging, time, copy
import cPickle as pickle
from datetime import datetime

import numpy as np
import simplejson as json

from hdfaccess.file import hdf_file
from analysis_engine import __version__ as analyzer_version # to check pickle files
from analysis_engine import settings
from analysis_engine.library import np_ma_masked_zeros_like
from analysis_engine.dependency_graph import dependency_order
from analysis_engine.node import (ApproachNode, Attribute,
                                  derived_param_from_hdf,
                                  DerivedParameterNode,
                                  FlightAttributeNode,
                                  KeyPointValueNode,
                                  KeyTimeInstanceNode, Node,
                                  NodeManager, P, Section, SectionNode)
import analysis_engine.node as node
from analysis_engine.process_flight import get_derived_nodes, geo_locate, _timestamp
import hdfaccess.file

import fds_oracle
import frame_list # map of tail# to LFLs
from frame_list import get_aircraft_info
import load_apt_rwy
logger = logging.getLogger(__name__) #for process_short)_


class Flight(object):
    '''Container for data describing a single flight, principally from an hdf5 file
        used in conjunction with get_deps_series(), derive_parameters_series(), and derive()
        
       These support profile development.  Also make be useful for processing FFDs.
       TODO: in profile, load and process only time series that are dependencies
    '''
    def __init__(self):
        self.filepath = None
        self.file_repository=None
        self.duration = None
        self.start_datetime= None
        self.start_epoch = None
        self.aircraft_info = {}            
        self.achieved_flight_record={}            
        self.series = {}         #dict of hdfaccess.Parameter        
        self.invalid_series = {} #dict of hdfaccess.Parameter marked invalid
        self.lfl_params = []
        self.parameters = {}     #dict of ParameterNodes
        #maybe add  kpv, kti, phase, attr later
        self.superframe_present = 1
        self.hdfaccess_version = 1 #=version in hdf5 attributes
        self.reliable_frame_counter = 1

    def load_from_hdf5(self, flight_dict, required_series=[]): #all_hdfseries, 
        '''load data from an hdf flight data file
            flight_series is a dictionary with fields:  'filepath', 'aircraft_info', 'repo'        
        '''
        # look up aircraft info by tail number
        self.filepath = flight_dict['filepath']
        self.aircraft_info  = flight_dict['aircraft_info']
        if not os.path.exists(self.filepath):
            logger.warning('cannot find: '+ self.filepath)
            #pdb.set_trace()
                
        with hdfaccess.file.hdf_file(self.filepath) as ff:
            self.duration = ff.duration
            self.start_datetime = ff.start_datetime
            self.start_epoch = ff.hdf.attrs.get('start_timestamp') #keep as epoch
            self.superframe_present = ff.superframe_present 
            self.hdfaccess_version =  ff.hdfaccess_version 
            self.reliable_frame_counter = ff.reliable_frame_counter 
            
            # for profiles we can choose to load only selected series   
            if required_series==[]:
                 series_to_load = ff.keys() #all_hdfseries
            else:
                series_to_load = frozenset(required_series).intersection( frozenset(ff.keys()) )
                            
            for k in series_to_load:
                self.series[k] =  ff.get_param(k, valid_only=False)
                if  self.series[k].lfl==True:  
                    self.lfl_params.append(k)                     
                is_invalid =self.series[k].invalid
                if is_invalid is None or is_invalid==False:
                    param_node = node.derived_param_from_hdf( self.series[k] )
                    param_node.units = self.series[k].units #better
                    param_node.lfl = self.series[k].lfl
                    param_node.data_type =self.series[k].data_type 
                    self.parameters[k] = param_node

    
    def save_to_hdf5(self, hdf5_path):
        '''this will overwrite any existing file with the same name'''
        with hdf_file(hdf5_path, cache_param_list=[], create=True) as hfile:
            # set attributes
            hfile.hdf.attrs['duration'] = self.duration
            hfile.hdf.attrs['start_datetime '] = self.start_epoch
            hfile.hdf.attrs['superframe_present']=self.superframe_present 
            hfile.hdf.attrs['hdfaccess_version']=hdfaccess.file.HDFACCESS_VERSION
            hfile.hdf.attrs['reliable_frame_counter']=self.reliable_frame_counter 
            hfile.hdf.attrs['aircraft_info'] = json.dumps(self.aircraft_info)           
            # save time series
            for k in self.parameters.keys():
                hfile.set_param( self.parameters[k])
        return
    
    def __repr__(self):
        s='class Flight'
        s = s+ '\n  filepath:       ' + str(self.filepath)
        s = s+ '\n  duration:       ' + str(self.duration)
        s = s+ '\n  start_datetime: ' + str(self.start_datetime)
        s = s+ '\n  series:         ' + str(self.series.keys()[:3]) + '...'
        s = s+ '\n  invalid_series: ' + str(self.invalid_series.keys()[:3]) + '...'
        s = s+ '\n  aircraft_info:  ' + str(self.aircraft_info)
        s = s+ '\n  parameter nodes:  ' + str(self.parameters.keys()[:3]) + '...'
        return s

###########################################################

def get_input_files(INPUT_DIR, file_suffix, logger):
    ''' returns a list of absolute paths '''
    files_to_process = glob.glob(os.path.join(INPUT_DIR, file_suffix))
    file_count = len(files_to_process)
    logger.warning('Processing '+str(file_count)+' files.')
    return files_to_process, file_count


def get_short_profile_name(myfile):
    '''peel off last level of folder names in path = profile name'''
    this_path = os.path.realpath(myfile)  #full path to this script
    this_folder =  os.path.split(this_path)[0]
    short_profile = this_folder.replace('\\','/').split('/')[-1]
    return short_profile


def file_move(from_path, to_path):
    '''attempts to move the file even if a file at to_path exists.
       on Windows it will fail if the file at to_path is already open
    '''
    try:
        os.remove(to_path)
    except:
        pass
    os.rename(from_path, to_path)
    return
    

def initialize_logger(LOG_LEVEL, filename='log_messages.txt'):
    '''all stages use this common logger setup'''
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(logging.FileHandler(filename=filename)) #send to file 
    logger.addHandler(logging.StreamHandler())                #tee to screen
    return logger


def make_kml_file(start_datetime, flight_attrs, kti, kpv, flight_file, REPORTS_DIR, output_path_and_file): 
    '''adapted from FDS process_flight.  As of 2013/6/6 we do not geolocate unless KML was requested, to save time.'''
    from analysis_engine.plot_flight    import track_to_kml
    with hdf_file(output_path_and_file) as hdf:
        # geo locate KTIs
        kti = geo_locate(hdf, kti)
        kti = _timestamp(start_datetime, kti)                    
        # geo locate KPVs
        kpv = geo_locate(hdf, kpv)
        kpv = _timestamp(start_datetime, kpv)
    report_path_and_file = REPORTS_DIR + flight_file.replace('.','_')+'.kml'
    track_to_kml(output_path_and_file, kti, kpv, flight_attrs, dest_path=report_path_and_file)
    

def pkl_suffix():
    '''file suffix versioning'''
    return 'ver'+analyzer_version.replace('.','_') +'.pkl'  # eg 0.0.5 => ver0_0_5.pkl

def get_precomputed_parameters(flight):    #flight_path_and_file, flight):    
    ''' if pkl file exists and matches version, read it into params dict'''
    # suffix includes FDS version as a compatibility check
    #source_file = flight_path_and_file.replace('.hdf5', pkl_suffix())
    #precomputed_parameters={}
    precomputed_parameters = flight.parameters.copy()        
    #if flight.filepath.find('.hdf5')<0:
    if True:
        return precomputed_parameters
        
    source_file = flight.filepath.replace('.hdf5', pkl_suffix()) 
    if os.path.isfile(source_file):
        logger.info('get_precomputed_profiles. found: '+ source_file)
        #pdb.set_trace()
        with open(source_file, 'rb') as pkl_file:
            pickled_parameters = pickle.load(pkl_file)
        precomputed_parameters.update(pickled_parameters)
    else:
        logger.info('No compatible precomputed profile found')
    return precomputed_parameters


def dump_pickles(output_path_and_file, params, kti, kpv, phases, approach, flight_attrs, logger):
    #dump params to pickle file -- versioned
    pkl_end = pkl_suffix()
    pickle_file=output_path_and_file.replace('.hdf5',pkl_end )
    with open(pickle_file, 'wb') as output:
        pickle.dump(params, output)
    with open( pickle_file.replace(pkl_end,'_kti_'+pkl_end), 'wb') as output:
        pickle.dump(kti, output)
    with open(pickle_file.replace(pkl_end,'_kpv_'+pkl_end), 'wb') as output:
        pickle.dump(kpv, output)
    with open(pickle_file.replace(pkl_end,'_phases_'+pkl_end), 'wb') as output:
        pickle.dump(phases, output)
    with open(pickle_file.replace(pkl_end,'_approach_'+pkl_end), 'wb') as output:
        pickle.dump(approach, output)
    with open(pickle_file.replace(pkl_end,'_fltattr_'+pkl_end), 'wb') as output:
        pickle.dump(flight_attrs, output)
    logger.info('saved '+ pickle_file)


### run FlightDataAnalyzer for analyze and profile

def prep_nodes(short_profile, module_names):
    ''' go through modules to get derived nodes '''
    if short_profile=='base':
        requested_nodes  = get_derived_nodes(settings.NODE_MODULES + module_names)
        available_nodes   = requested_nodes          
        
        requested_params = available_nodes.keys()
        exclusions = ['Transmit', 
                      'EngGasTempDuringMaximumContinuousPowerForXMinMax',  #still calcs
                      'Eng Gas Temp During Maximum Continuous Power For X Min Max',
                      'EngGasTempDuringEngStartForXSecMax',
                      'Eng Gas Temp During Eng Start For X Sec Max',
                      'Eng Oil Temp For X Min Max',         
                      # 'Configuration',
                      ]
        requested_params = sorted( set(requested_params) - set(exclusions)) #exclude select params from FDS set              
        requested_params = list(set( requested_params + get_derived_nodes(['analysis_engine.flight_attribute']).keys()))            
    else:
        requested_nodes   = get_derived_nodes(module_names)    
        available_nodes  = get_derived_nodes(settings.NODE_MODULES + module_names)
        requested_params = requested_nodes.keys()
    return requested_params, available_nodes
               

def prep_order(flight, frame_dict, start_datetime, derived_nodes, required_params):
    ''' open example HDF to see recorded params and build process order'''
    derived_nodes_copy = derived_nodes   #copy.deepcopy(derived_nodes)  #
    precomputed_parameters = get_precomputed_parameters(flight)
    precomputed_keys=precomputed_parameters.keys()
    for k in flight.series.keys():
        if k not in precomputed_keys:
            precomputed_keys.append(k)
    node_mgr = NodeManager( start_datetime, flight.duration, 
                            precomputed_keys, #flight.series.keys(),       #from HDF.   was hdf.valid_param_names(), #hdf_keys; should be from LFL
                            required_params,   #requested
                            derived_nodes_copy,     #methods that can be computed; equals profile + base nodes   ????
                            flight.aircraft_info, 
                            achieved_flight_record=flight.achieved_flight_record
                            )
    # calculate dependency tree
    process_order, gr_st = dependency_order(node_mgr, draw=False)     
    logger.warning( 'process order: ' + str(process_order[:5]) + '...' ) #, gr_st
    return node_mgr, process_order  # a list of node names
    

def get_output_file(OUTPUT_DIR, flight_path_and_file, profile):
    ''' if no new timeseries, just set output path  input path'''
    if profile=='base':
        #logger.debug('writing new hdf5')
        flight_file          = os.path.basename(flight_path_and_file)
        output_path_and_file = (OUTPUT_DIR+flight_file).replace('.0','_0').replace('.hdf5', '_'+profile+'.hdf5')
    else:
        logger.debug('read only. no new hdf5')
        output_path_and_file = flight_path_and_file            
    return output_path_and_file 




###node post-processing
def align_section(result, duration, section_list):
    aligned_section = result.get_aligned(P(frequency=1, offset=0))
    for index, one_hz in enumerate(aligned_section):
        # SectionNodes allow slice starts and stops being None which
        # signifies the beginning and end of the data. To avoid TypeErrors
        # in subsequent derive methods which perform arithmetic on section
        # slice start and stops, replace with 0 or hdf.duration.
        fallback = lambda x, y: x if x is not None else y
        duration = fallback(duration, 0)

        start = fallback(one_hz.slice.start, 0)
        stop = fallback(one_hz.slice.stop, duration)
        start_edge = fallback(one_hz.start_edge, 0)
        stop_edge = fallback(one_hz.stop_edge, duration)
        slice_ = slice(start, stop)
        one_hz = Section(one_hz.name, slice_, start_edge, stop_edge)
        aligned_section[index] = one_hz
        
        if not (0 <= start <= duration and 0 <= stop <= duration + 1):
            msg = "Section '%s' (%.2f, %.2f) not between 0 and %d"
            raise IndexError(msg % (one_hz.name, start, stop, duration))
        if not 0 <= start_edge <= duration:
            msg = "Section '%s' start_edge (%.2f) not between 0 and %d"
            raise IndexError(msg % (one_hz.name, start_edge, duration))
        if not 0 <= stop_edge <= duration + 1:
            msg = "Section '%s' stop_edge (%.2f) not between 0 and %d"
            raise IndexError(msg % (one_hz.name, stop_edge, duration))
        section_list.append(one_hz)
    return aligned_section, section_list
            
            
def check_approach(result, duration, approach_list):
    aligned_approach = result.get_aligned(P(frequency=1, offset=0))
    for approach in aligned_approach:
        # Does not allow slice start or stops to be None.
        valid_turnoff = (not approach.turnoff or
                         (0 <= approach.turnoff <= duration))
        valid_slice = ((0 <= approach.slice.start <= duration) and
                       (0 <= approach.slice.stop <= duration))
        valid_gs_est = (not approach.gs_est or
                        ((0 <= approach.gs_est.start <= duration) and
                         (0 <= approach.gs_est.stop <= duration)))
        valid_loc_est = (not approach.loc_est or
                         ((0 <= approach.loc_est.start <= duration) and
                          (0 <= approach.loc_est.stop <= duration)))
        if not all([valid_turnoff, valid_slice, valid_gs_est,
                    valid_loc_est]):
            raise ValueError('ApproachItem contains index outside of '
                             'flight data: %s' % approach)
    return aligned_approach, approach_list
            

def check_derived_array(param_name, result, duration ):
    # check that the right number of results were returned
    # Allow a small tolerance. For example if duration in seconds
    # is 2822, then there will be an array length of  1411 at 0.5Hz and 706
    # at 0.25Hz (rounded upwards). If we combine two 0.25Hz
    # parameters then we will have an array length of 1412.
    expected_length = duration * result.frequency
    if result.array is None:
        logger.debug("No array set; creating a fully masked array for %s", param_name)
        array_length = expected_length
        # Where a parameter is wholly masked, we fill the HDF
        # file with masked zeros to maintain structure.
        result.array = \
            np_ma_masked_zeros_like(np.ma.arange(expected_length))
    else:
        array_length = len(result.array)
    length_diff = array_length - expected_length
    if length_diff == 0:
        pass
    elif 0 < length_diff < 5:
        logger.debug("Cutting excess data for parameter '%s'. Expected length was '%s' while resulting "
                       "array length was '%s'.", param_name, expected_length, len(result.array))
        result.array = result.array[:expected_length]
    else:
        #pdb.set_trace()
        raise ValueError("Array length mismatch for parameter "
                         "'%s'. Expected '%s', resulting array length '%s'." % (param_name, expected_length, array_length))
    return result

###########################################
def get_frequency_offset(mynode, deps):
        """
        adapted from node get_derived(self, deps)
        
        get_frequency_offset( ( flight.parameters['Airspeed'], flight.parameters['Altitude AAL'], .. ) )

        :param args: List of available Parameter objects
        :type args: list
        :returns:  frequency, offset 
        """
        frequency=None
        offset=None
        dependencies_to_align = [d for d in deps if d is not None and d.frequency]
        if dependencies_to_align and mynode.align:
            if mynode.align_frequency and mynode.align_offset is not None:
                # align to the class declared frequency and offset
                frequency = mynode.align_frequency
                offset = mynode.align_offset
            elif mynode.align_frequency:
                # align to class frequency, but set offset to first dependency
                # This will cause a problem during alignment if the offset is
                # greater than the frequency allows (e.g. 0.6 offset for a 2Hz
                # parameter). It may be best to always define a suitable
                # align_offset.
                frequency = mynode.align_frequency
                offset = dependencies_to_align[0].offset
            elif mynode.align_offset is not None:
                # align to class offset, but set frequency to first dependency
                frequency = dependencies_to_align[0].frequency
                offset = mynode.align_offset
            else:
                # This is the class' default behaviour:
                # align both frequency and offset to the first parameter
                alignment_param = dependencies_to_align.pop(0)
                frequency = alignment_param.frequency
                offset = alignment_param.offset
        else:
                frequency = mynode.frequency if hasattr(mynode, 'frequency') else 1.0
                offset = mynode.offset if hasattr(mynode, 'attribute') else 0.0
        return frequency, offset, dependencies_to_align


def get_deps_series(node_class, params, node_mgr, pre_aligned):
        # build ordered and aligned dependencies without touching an hdf file
        # 'params' is a dictionary of previously computed nodes: time series, kpv, etc.
        # pre_aligned is a dictionary of pre-aligned params, key=(name, frequency, offset)
        deps = []
        nd = node_class()
        node_deps = node_class.get_dependency_names()
        logger.debug( 'deps of'+ nd.name+ '  deps: '+ str(node_deps) )

        # need to augment params with pre-aligned
        for dep_name in node_deps:            
            if dep_name in params:  # already calculated
                deps.append(params[dep_name])
            elif node_mgr.get_attribute(dep_name) is not None:
                deps.append(node_mgr.get_attribute(dep_name))                
            else:  # dependency not available
                logger.info('dep_name not added in get_deps: '+dep_name)
                deps.append(None)

        #pdb.set_trace()
        if all([d is None for d in deps]):
            #print node_deps, deps
            logger.warning("No dependencies available: "+str(node_deps))
            raise RuntimeError("No dependencies available - Nodes cannot "
                               "operate without ANY dependencies available! "
                               "Node: %s" % node_class.__name__)

        frequency, offset, deps_to_align = get_frequency_offset(node_class, deps) 
        nd.frequency = frequency
        nd.offset = offset
        #pre-align
        aligned_deps = []        
        for d in deps:
            if d in deps_to_align:
                nodestr= '\t  dep_name:'+ d.name + ',  hdf?'+ str(d.name in node_mgr.hdf_keys) + ',  freq:'+str(frequency) + ',  offset:'+str(offset)
                if pre_aligned.has_key((d.name, frequency, offset)):
                    logger.debug( nodestr +  '---using pre-aligned')
                    aligned_deps.append(  pre_aligned[(d.name, frequency, offset)]  )
                else: 
                    logger.debug( nodestr +  '***pre-aligning')
                    pre_aligned[(d.name, frequency, offset)] = d.get_aligned(nd)
                    aligned_deps.append(  pre_aligned[(d.name, frequency, offset)]  )
            else: #no need to worry about alignment
                aligned_deps.append( d )
        return aligned_deps, pre_aligned, nd
        

            

def derive_parameters_series(flight, node_mgr, process_order, precomputed={}):
    '''
    Non HDF5 version. Suitable for FFD and Notebook profile development.
    
    Derives the parameter values and if limits are available, applies
    parameter validation upon each param before storing the resulting masked
    array back into the hdf file.
    
    :param series: Data file accessor used to get and save parameter data and attributes
    :type series: dictionary of ParameterNode objects
    :param node_mgr: Used to determine the type of node in the process_order
    :type node_mgr: NodeManager
    :param process_order: Parameter / Node class names in the required order to be processed
    :type process_order: list of strings
    '''
    duration = flight.duration
    params    = precomputed #{}   # dictionary of derived params that aren't masked arrays
    pre_aligned = {} #key = (name, frequency, offset)
    res     = {'series':{}, 
              'approach': ApproachNode(restrict_names=False),
              'kpv': KeyPointValueNode(restrict_names=False),
              'kti': KeyTimeInstanceNode(restrict_names=False),
              'phase': SectionNode(),
              'attr': []
              } #results by node type
   
    #print 'Process Order'
    #for p in process_order:
    #    print '  ',p
   
    for param_name in process_order:
        #if param_name in node_mgr.hdf_keys:
        #   logger.info('_derive_: hdf '+param_name)            
        #   continue        
        #elif

        if node_mgr.get_attribute(param_name) is not None:
            logger.info('_derive_: get_attribute '+param_name)
            continue
        elif param_name in params.keys():  # already calculated
            logger.info('_derive_: re-using precomputed'+param_name)
            continue
        elif not node_mgr.derived_nodes.has_key(param_name):
            logger.info('_derive_: in process_order but not derived_nodes: '+param_name)
            continue
        elif param_name in res['series'].keys():
            print 'derive_parameters: in series but not params:', param_name
            #logger.info('_derive_: series'+param_name)
            #continue

        ####compute###########################################################    
        logger.info('_derive_: computing '+param_name)        
        node_class = node_mgr.derived_nodes[param_name]  #NB raises KeyError if Node is "unknown"

        try:        
            deps, pre_aligned, node = get_deps_series(node_class, params, node_mgr, pre_aligned )
        except Exception, e:
            logger.exception('ERROR '+param_name+' get_deps')
            raise
        
        try:
            node.derive(*deps) #node.get_derived(deps)
            result = node
        except  Exception, e:
            logger.exception('ERROR '+param_name+' get_derived')
            raise            
        
        ###############################################################
        #def post_process_node(flight, node, param_name, result, res, params)

        #post-process (node, result, params, res)
        if node.node_type is KeyPointValueNode:
            #Q: track node instead of result here??
            params[param_name] = result
            for one_hz in result.get_aligned(P(frequency=1, offset=0)):
                if not (0 <= one_hz.index <= duration):
                    raise IndexError("KPV '%s' index %.2f is not between 0 and %d" % (one_hz.name, one_hz.index, duration))
                res['kpv'].append(one_hz)
        
        elif node.node_type is KeyTimeInstanceNode:
            params[param_name] = result
            for one_hz in result.get_aligned(P(frequency=1, offset=0)):
                if not (0 <= one_hz.index <= duration):
                    raise IndexError("KTI '%s' index %.2f is not between 0 and %d" % (one_hz.name, one_hz.index, duration))
                res['kti'].append(one_hz)
        
        elif node.node_type is FlightAttributeNode:
            params[param_name] = result
            try:
                res['attr'].append(Attribute(result.name, result.value)) # only has one Attribute result
            except:
                logger.warning("Flight Attribute Node '%s' returned empty handed.", param_name)
                    
        elif issubclass(node.node_type, SectionNode):
            aligned_section, res['phase'] = align_section(result, duration, res['phase'])
            params[param_name] = aligned_section  ### 
            
        elif issubclass(node.node_type, DerivedParameterNode):
            logger.info('series: ' + param_name)
            result = check_derived_array(param_name, result, duration)
            #print str(result
            res['series'][param_name] = result  
            res['series'][param_name].name = param_name
            params[param_name] = result
            
        elif issubclass(node.node_type, ApproachNode):
            aligned_approach, res['approach'] = check_approach(result, duration, res['approach'])   
            params[param_name] = aligned_approach
        else:
            raise NotImplementedError("Unknown Type %s" % node.__class__)
        continue
    return res, params

###################################################################################################

def analyze_one(flight, output_path, profile, requested_params, available_nodes): 
        # , test_param_names, test_node_mgr, test_process_order):
        '''analyze one flight'''
        #precomputed_parameters = flight.parameters.copy()        
        precomputed_parameters = get_precomputed_parameters(flight)
        available_nodes_copy        = available_nodes #copy.deepcopy(available_nodes)
        #if flight.parameters.keys() != test_param_names: #rats, have to redo this
        node_mgr = node.NodeManager( 
                flight.start_datetime, flight.duration,
                precomputed_parameters.keys(), #flight.parameters.keys(), 
                requested_params, available_nodes_copy, flight.aircraft_info,
                achieved_flight_record=flight.achieved_flight_record
              )                  
        #pdb.set_trace()
        process_order, gr_st = dependency_order(node_mgr, draw=False) 
        res, params = derive_parameters_series(flight, node_mgr, process_order, precomputed_parameters)
        #post-process: save
        for k in res['series'].keys():
                flight.parameters[k] = res['series'][k]
        if profile=='base': 
            flight.save_to_hdf5(output_path)
            dump_pickles(output_path, params, res['kti'], res['kpv'], res['phase'], res['approach'], res['attr'], logger)
        return flight, res, params

def load_afr(fltrec, apt_dict, apt_rwy_dict):
        afr = {}
        orig = fltrec.get('Takeoff ICAO')
        dest = fltrec.get('Landing ICAO')
        if  apt_dict.has_key(orig):            
            afr['AFR Takeoff Airport'] = apt_dict.get(orig) 
            if  apt_rwy_dict[orig].has_key(fltrec['Takeoff Runway']):
                afr['AFR Takeoff Runway'] = apt_rwy_dict[orig].get( fltrec['Takeoff Runway']  ) 
        if  apt_dict.has_key(dest):
            afr['AFR Landing Airport'] = apt_dict.get(dest) 
            if  apt_rwy_dict[dest].has_key(fltrec['Landing Runway']):
                afr['AFR Landing Runway'] = apt_rwy_dict[dest].get( fltrec['Landing Runway']  )             
        return afr


def load_flight(filepath, frame_dict, frame_hdfseries, file_repository, 
                         series_to_load=[], flightrec_dict=None, apt_dict=None, apt_rwy_dict=None):
    '''load a Flight object'''
    aircraft_info = get_aircraft_info( filepath, frame_dict)                
    
    logger.debug(aircraft_info)
    flight_dict = {'filepath': filepath, 'aircraft_info':aircraft_info}
    flight = Flight()   
    flight.file_repository = file_repository
    flight.aircraft_info = aircraft_info        
    
    if flightrec_dict and apt_dict and apt_rwy_dict and flightrec_dict.has_key(filepath):
        logger.info("have an AFR for: "+ filepath)
        fltrec = flightrec_dict[filepath]
        flight.achieved_flight_record = load_afr(fltrec, apt_dict, apt_rwy_dict)

    if filepath.endswith('.hdf5'):
        flight.load_from_hdf5(flight_dict, required_series=series_to_load)        
    #elif filepath.endswith('.ffd'):
    #   flight.load_from_flight(flight_dict)        
    else:
        print 'Aack!!! Unknown file type'
    return flight, frame_hdfseries
    
def analyzer_fail(flight_file):
        ex_type, ex, tracebck = sys.exc_info()
        logger.warning('ANALYZER ERROR '+flight_file)
        traceback.print_tb(tracebck)        
        return tracebck               

        
def run_analyzer(short_profile,    module_names,
                 LOG_LEVEL,           files_to_process,   
                 input_dir,        output_dir,       reports_dir, 
                 flightrec_dict=None,     #for AFR
                 make_kml=False,   
                 save_oracle=True, comment='',
                 file_repository='linux', 
                 start_datetime = datetime(2012, 4, 1, 0, 0, 0), 
                 mortal=True ):    
    '''
    run FlightDataAnalyzer for analyze and profile. mostly file mgmt and reporting.
    currently runs against a single fleet at a time.
    
    afr_dict is an optional dictionary mapping file paths to airport/runway attributes
    
    TODO: add loop per fleet
    '''
    print 'mortal', mortal
    logger = initialize_logger(LOG_LEVEL)
    stage = 'analyze' if short_profile=='base' else 'profile'    
    if not files_to_process or len(files_to_process)==0:
        logger.warning('run_analyzer: No files to process.')
        return
    timestamp = datetime.now()
    frame_dict = frame_list.build_frame_list(logger)
    frame_hdfseries = {} # for each lfl, keep a list of available hdf series
    
    # get frame info    
    
    cn = fds_oracle.get_connection() 
    #load airport and runway info
    cur = cn.cursor()
    apt_dict = load_apt_rwy.get_apt_dict(cur)
    print  "apt_dict['KEWR']", apt_dict['KEWR']      
    apt_rwy_dict = load_apt_rwy.get_apt_rwy_dict(cur)
    cur.close()

    requested_params, available_nodes = prep_nodes(short_profile, module_names)
    test_file    = files_to_process[0]
    
    test_flight, frame_hdfseries = load_flight( test_file, frame_dict, frame_hdfseries, file_repository, 
                                                                       flightrec_dict=flightrec_dict, 
                                                                       apt_dict=apt_dict, apt_rwy_dict=apt_rwy_dict  )
    logger.warning( 'test_file for prep_order(): '+ test_file)
    #test_param_names = test_flight.parameters.keys()
    test_node_mgr, test_process_order = prep_order(test_flight, frame_dict, start_datetime, 
                                                                                   available_nodes, requested_params)
    if short_profile=='base':
        series_to_load= []
    else:
        tpo=set(test_process_order)    
        series_to_load = tpo.intersection(test_flight.series.keys())
        
    ### loop over files and compute nodes for each
    file_count = len(files_to_process)
    ok_count = 0
    fail_count = 0
    logger.warning( 'Processing '+str(file_count)+' files.' )
    start_time = time.time()
    for flight_path_and_file in files_to_process:
        status=None
        file_start_time = time.time()
        flight_file          = os.path.basename(flight_path_and_file)
        logger.warning('starting '+ flight_file)
        flight, frame_hdfseries = load_flight(flight_path_and_file, frame_dict, frame_hdfseries, file_repository, 
                                                                 series_to_load=series_to_load, 
                                                                 flightrec_dict=flightrec_dict,
                                                                 apt_dict=apt_dict, apt_rwy_dict=apt_rwy_dict  )
        output_path  = get_output_file(output_dir, flight_path_and_file, short_profile)
        logger.info(' *** Processing flight %s', flight_file)
        try: 
            flight, res, params = analyze_one(flight, output_path, short_profile, 
                                                                  requested_params, available_nodes) 
            status='ok'
            ok_count += 1
        except Exception, e: 
            analyzer_fail(flight_file)
            print e
            status='fail'
            fail_count += 1
            if not mortal: 
                continue
            else:
                raise
                    
        proc_time = "{:2.4f}".format(time.time()-file_start_time)
        logger.warning(' *** Processing flight %s finished ' + flight_file + ' time: ' + proc_time + 'status: '+status)
        processing_time = time.time()-file_start_time
        if save_oracle: fds_oracle.report_timing(timestamp, stage, short_profile, flight_path_and_file, processing_time, status, logger, cn)
        if status=='ok' and save_oracle:  fds_oracle.analyzer_to_oracle(cn, short_profile, res, params, flight, output_dir, output_path)
        if status=='ok' and make_kml:    make_kml_file(start_datetime, res['attr'], res['kti'], res['kpv'], flight_file, reports_dir, output_path)

    fds_oracle.report_job(timestamp, stage, short_profile, comment, 
                                      file_repository, input_dir, output_dir, 
                                      len(files_to_process), (time.time()-start_time), logger, db_connection=cn)
    cn.close()
    #for handler in logger.handlers(): handler.close()
    analyzer_status = {'timestamp':timestamp, 'file_count':file_count, 
                                   'ok_count':ok_count, 'fail_count':fail_count 
                                   }
    return analyzer_status 
    

def parallel_directview_generic(PROFILE_NAME, module_names, FILE_REPOSITORY, LOG_LEVEL, 
               FILES_TO_PROCESS, input_dir, output_dir, reports_dir,
               COMMENT, MAKE_KML_FILES ):
    '''sets up worker namespaces for ipython parallel runs'''
    print "Run 'ipcluster start -n 10' from the command line first!"
    #from IPython import parallel
    from IPython.parallel import Client
    c=Client()  #c=Client(debug=True)
    print c.ids
    #engine_count = len(c.ids)
    dview = c[:]  #DirectView list of engines
    dview.clear() #clean up the namespaces on the eng
    dview.block = True           
    
    #build parallel namespace
    dview['module_names']    = module_names 
    dview['PROFILE_NAME'] = PROFILE_NAME
    dview['COMMENT'] = COMMENT
    dview['LOG_LEVEL'] = LOG_LEVEL 
    dview.scatter('files_to_process', FILES_TO_PROCESS)
    dview['file_repository'] = FILE_REPOSITORY    
    dview['MAKE_KML_FILES'] = MAKE_KML_FILES 
    
    reports_dir = settings.PROFILE_REPORTS_PATH                              
    output_dir = settings.PROFILE_DATA_PATH + PROFILE_NAME+'/'   
    if not os.path.exists(output_dir): 
        os.makedirs(output_dir)
    dview['input_dir ']    =  input_dir 
    dview['output_dir ']  =  output_dir 
    dview['reports_dir '] =  reports_dir 
    
    #logger = logging.Logger('ipcluster')
    #logger.setLevel(LOG_LEVEL)
    #dview['logger'] = logger
    return dview


def parallel_directview(PROFILE_NAME, module_names, FILE_REPOSITORY, LOG_LEVEL, 
               FILES_TO_PROCESS, COMMENT, MAKE_KML_FILES ):
    '''sets up PROFILE worker namespaces for ipython parallel runs'''
    input_dir = "N/A"    
    output_dir = settings.PROFILE_DATA_PATH + PROFILE_NAME+'/'   
    reports_dir = settings.PROFILE_REPORTS_PATH                              
    dview= parallel_directview_generic(PROFILE_NAME, module_names, FILE_REPOSITORY, LOG_LEVEL, 
               FILES_TO_PROCESS, input_dir, output_dir, reports_dir,
               COMMENT, MAKE_KML_FILES )
    return dview


def run_profile(profile_name, module_names, 
                          LOG_LEVEL, FILES_TO_PROCESS, COMMENT, MAKE_KML_FILES, 
                          FILE_REPOSITORY='central', save_oracle=True, mortal=True ):

    reports_dir = settings.PROFILE_REPORTS_PATH
    output_dir = settings.PROFILE_DATA_PATH + profile_name+'/' 
    if not os.path.exists(output_dir): 
        os.makedirs(output_dir)
    output_dir  = output_dir if output_dir.endswith('/') or output_dir .endswith('\\') else output_dir +'/'
    reports_dir = reports_dir if reports_dir.endswith('/') or reports_dir.endswith('\\') else reports_dir +'/'

    print 'calling run_analyzer'
    analyzer_status =run_analyzer(profile_name, module_names, 
                                                     LOG_LEVEL, FILES_TO_PROCESS, 
                                                     'NA', output_dir, reports_dir, 
                                                     make_kml=MAKE_KML_FILES, 
                                                     save_oracle=save_oracle,
                                                     comment=COMMENT,
                                                     file_repository=FILE_REPOSITORY,
                                                     mortal=mortal)        
    return analyzer_status 
     

if __name__=='__main__':
    print 'loaded'