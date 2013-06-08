''' 
  Customized configuration settings for FDS and asias_fds libraries
  Note: Oracle settings are managed in fds_oracle.py
'''

# base paths
SHARED_CODE_PATH = 'c:/asias_fds/asias_fds_shared/'
PREP_DATA_PATH = 'c:/asias_fds_prep/data_in_preparation/'  
PREP_REPORTS_PATH = 'c:/asias_fds_prep/preparation_reports/'

BASE_DATA_PATH = 'c:/asias_fds/base_data/'

PROFILE_DATA_PATH = 'c:/asias_fds/data_from_profiles/'
PROFILE_REPORTS_PATH = 'c:/asias_fds/profile_reports/'

# API Handler  -- use of the Web API requires coordination with Flight Data Services
#API_HANDLER = 'analysis_engine.api_handler_analysis_engine.AnalysisEngineAPIHandlerHTTP'
API_HANDLER = 'analysis_engine.api_handler_analysis_engine.AnalysisEngineAPIHandlerLocal'

# Set the base URL for the API handler:
BASE_URL = '' #if API_HANDLER is local