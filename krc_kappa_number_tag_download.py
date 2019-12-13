# =============================================================================
# Installations:
# python 3.6
# pip install pythonnet
# pip install dateparser
# ============================================================================

import datetime
# =============================================================================
# Import libraries
# =============================================================================
import sys
from datetime import timedelta
from timeit import default_timer as timer

import clr
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgresACERASadmin@localhost:5432/opex')
#from apscheduler.schedulers.blocking import BlockingScheduler



sys.path.append(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')    
clr.AddReference('OSIsoft.AFSDK') 

from OSIsoft.AF import *  
from OSIsoft.AF.PI import *  
from OSIsoft.AF.Asset import *  
from OSIsoft.AF.Data import *  
from OSIsoft.AF.Time import *  
from OSIsoft.AF.UnitsOfMeasure import *  


# =============================================================================
# Setup Server
# =============================================================================
piServers = PIServers()    
piServers.DefaultPIServer = piServers['RAPP1'];  
piServer = piServers.DefaultPIServer
print("Server: {}".format(piServer))
        

# =============================================================================
# Extraction functions
# =============================================================================
def pi_interpolated_value(piServer='RAPP1',
                          pi_tag_list=[],
                          start_time='2019-01-01 00:00:00',
                          end_time='2019-01-02 00:00:00',
                          freq="1h",
                          server_timebased = True,
                          convert_invalid_to_nan = False,
                          datetime_index = False,
                          convert_col_to_lower = True):
    '''
    This function extracts interpolated data (Sampled)
    '''
    
    global error_detector
    
    download_process_start_time = timer()
    df_combined = pd.DataFrame()
    
    #if len(pi_tag_list)>10:
    #    print("For load control purposes, only the first 10 tags will be extracted")
        
    if type(pi_tag_list) not in [list,tuple]:
        pi_tag_list = [pi_tag_list]
        
    
    if server_timebased == True:
        #Adjust timezone:
        if str(piServer) == 'RAPP1': #Add one hour, cause SDK will bring it back 1 hour
            start_time = (pd.to_datetime(start_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            if pd.to_datetime(end_time) > (datetime.datetime.now()-timedelta(hours=1)):
                end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            else:
                end_time = (pd.to_datetime(end_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        
        
    for pi_tag in pi_tag_list:
        span = freq
        try:
            pt = PIPoint.FindPIPoint(piServer, pi_tag) 
            timerange = AFTimeRange(start_time, end_time)  
            span = AFTimeSpan.Parse(span)  
            interpolated = pt.InterpolatedValues(timerange, span, "", False)
            data = [(str(event.Timestamp.LocalTime),event.Value) for event in interpolated]
            df = pd.DataFrame(data,columns=['datetime',str(pi_tag)])
            df['datetime'] = pd.to_datetime(df['datetime'])
            if len(df_combined) == 0:
                df_combined = df.copy()
            else:
                df_combined = pd.merge(left=df_combined, right=df, how='left', on='datetime')
                    
        except:
            print("Extraction error: {}".format(pi_tag))
            error_detector = 1
            break
    download_process_end_time = timer()
    process_time = download_process_end_time - download_process_start_time
    print("Process finished in {:.1f} seconds".format(process_time))
    
    if server_timebased == True:
        #Adjust back timezone:
        if len(df_combined)>0:
            if str(piServer) == 'RAPP1': #Change timezone from MY to IND
                df_combined['datetime'] = df_combined['datetime'] - timedelta(hours=1)
            
            
    #covert from AF object to string for non-valid data:
    for col in df_combined.columns:
        if col != 'datetime':
            df_combined[col] = df_combined[col].apply(lambda x:str(x) if not isinstance(x,float) else x)
            
            
    #Convert str to np.nan for invalid data:
    if convert_invalid_to_nan == True:
        for col in df_combined.columns:
            if col != 'datetime':
                df_combined[col] = df_combined[col].apply(lambda x:np.nan if isinstance(x,str) else x)
                df_combined[col].replace([np.inf, -np.inf], np.nan, inplace=True)
    
    #Convert col to lowercase:
    if convert_col_to_lower == True:
        colnames = [col.lower() for col in df_combined.columns]
        df_combined.columns = colnames
            
    
    if datetime_index == True:
        df_combined.set_index(['datetime'],inplace=True)
        
        
    return df_combined.copy()
                
                

##########################################################################
def pi_as_recorded_value(piServer='RAPP1',pi_tag='',
                         start_time='2019-01-01 00:00:00',
                         end_time='2019-01-02 00:00:00',
                         server_timebased = True,
                         convert_invalid_to_nan = False,
                         datetime_index = False,
                         convert_col_to_lower = True):
    '''
    This function extracts as_recorded data (Compressed)
    '''
    download_process_start_time = timer()
    
    if server_timebased == True:
        #Adjust timezone:
        if str(piServer) == 'RAPP1': #Add one hour, cause SDK will bring it back 1 hour
            start_time = (pd.to_datetime(start_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            end_time = (pd.to_datetime(end_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        
    df = pd.DataFrame()
    try:
        if type(pi_tag) in [list,tuple]:
            pi_tag = pi_tag[0]
            
        timerange = AFTimeRange(start_time, end_time)
        pt = PIPoint.FindPIPoint(piServer, pi_tag) 
        recorded = pt.RecordedValues(timerange, AFBoundaryType.Inside, "", False)  
        data = [(str(event.Timestamp.LocalTime),event.Value) for event in recorded]
        df = pd.DataFrame(data,columns=['datetime',str(pi_tag)])
        df['datetime'] = pd.to_datetime(df['datetime'])
    except:
        print("Tag extraction error: {}".format(pi_tag))
        
    download_process_end_time = timer()
    process_time = download_process_end_time - download_process_start_time
    print("Process finished in {:.1f} seconds".format(process_time))
    
    
    if server_timebased == True:
        #Adjust back timezone:
        if len(df)>0:
            if str(piServer) == 'RAPP1': #Change timezone from MY to IND
                df['datetime'] = df['datetime'] - timedelta(hours=1)
            
            
    #covert from AF object to string for non-valid data:
    for col in df.columns:
        if col != 'datetime':
            df[col] = df[col].apply(lambda x:str(x) if not isinstance(x,float) else x)
            
    #Convert str to np.nan for invalid data:
    if convert_invalid_to_nan == True:
        for col in df.columns:
            if col != 'datetime':
                df[col] = df[col].apply(lambda x:np.nan if isinstance(x,str) else x)
                df[col].replace([np.inf, -np.inf], np.nan, inplace=True)
                
    #Convert col to lowercase:
    if convert_col_to_lower == True:
        colnames = [col.lower() for col in df.columns]
        df.columns = colnames
        
    
    if datetime_index == True:
        df.set_index(['datetime'],inplace=True)
        
    return df.copy()





# =============================================================================
# Extraction
# =============================================================================
    

#### Kappa number tag
kappa = ["RPL.422AT2013B:value"] 




def refresh_data():
    
    print("Executing refresh_data now:" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    error_detector = 0
    
    live = datetime.datetime.now()
    live = live - datetime.timedelta(minutes=live.minute % 30) - datetime.timedelta(seconds=live.second)


                
    kappa_table = pi_interpolated_value(piServer,
                                   pi_tag_list = kappa,
                                   start_time = "2019-01-01 00:00:00",
                                   end_time = live,
                                   freq = "30m",
                                   server_timebased = True,
                                   convert_invalid_to_nan = True,
                                   datetime_index = False,
                                   convert_col_to_lower = False)
    if error_detector == 1:
        print('error encocuntered')

    # kappa_table.to_sql('kappa_number_tag_30m', engine, if_exists="replace")
    kappa_table.to_sql('kappa_number_tag_30m',engine, if_exists="replace")
    print("Execution ended," + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    

refresh_data()
#scheduler = BlockingScheduler()
#scheduler.add_job( refresh_data,'interval',minutes=5)
#scheduler.start()
     