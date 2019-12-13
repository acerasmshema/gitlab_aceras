# =============================================================================
# Installations:
# python 3.6
# pip install pythonnet
# pip install dateparser
# ============================================================================

# =============================================================================
# Server names:
# 172.27.83.15 Xinhui
# 172.27.60.24 Rizhao
# RAPP1
# 172.19.8.9
# PIBSC
# =============================================================================

import datetime
# =============================================================================
# Import libraries
# =============================================================================

import sys
from datetime import timedelta
from timeit import default_timer as timer
import logging_funcs as logging
import clr
import numpy as np
import pandas as pd

sys.path.append(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')
clr.AddReference('OSIsoft.AFSDK')

from OSIsoft.AF import *
from OSIsoft.AF.PI import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *
from OSIsoft.AF.UnitsOfMeasure import *

#==============================================================================
# Logging Error Path
pulpsdkpipath = r'log\pulpsdkpipath.txt'
#==============================================================================


# =============================================================================
# Server Connection
# =============================================================================
def connect_to_server(server_name='RAPP1'):
    piServers = PIServers()
    if piServers.Contains(server_name):
        piServers.DefaultPIServer = piServers[server_name];  
        piServer = piServers.DefaultPIServer
    else:
        piServer = piServers.Add(server_name)
        piServers.DefaultPIServer = piServers[server_name];          
        piServer = piServers.DefaultPIServer
        

    logging.log_message(pulpsdkpipath,'Server in use: {}'.format(server_name))
    print('Server in use: {}'.format(server_name))
    return piServer
    

#piServers = PIServers()    
#piServer = piServers.DefaultPIServer
#print("Server: {}".format(piServer))
        

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
    download_process_start_time = timer()
    df_combined = pd.DataFrame()


    #if len(pi_tag_list)>10:
    #    print("For load control purposes, only the first 10 tags will be extracted")
        
    if type(pi_tag_list) not in [list,tuple]:
        pi_tag_list = [pi_tag_list]
        
    
    if server_timebased == True:
        #Adjust timezone:
        if str(piServer) in ['RAPP1','172.19.8.9']: #Add one hour, cause SDK will bring it back 1 hour
            start_time = (pd.to_datetime(start_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            if pd.to_datetime(end_time) > (datetime.datetime.now()-timedelta(hours=1)):
                end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            else:
                end_time = (pd.to_datetime(end_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            print('Start time & End time are adjusted according to Indonesia')
                
        elif str(piServer) in ["172.27.60.24","172.27.83.15"]: #Same time as MY server
            if pd.to_datetime(end_time) > datetime.datetime.now():
                end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            print('Start time & End time are adjusted according to China')
                
    else: #Same time as MY server
        if pd.to_datetime(end_time) > datetime.datetime.now():
            end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        print('Start time & End time are adjusted according to Malaysia')

        
    print("\nLocal Start Time: {}".format(start_time))
    print("Local End Time: {}".format(end_time))
    print("Sampling Frequency: {}\n".format(freq))
    
    for pi_tag in pi_tag_list:
        print(pi_tag) # HEmaCode

        # if (pi_tag == "RPL.111XC0221:afc" ):
        #     print(pi_tag)
        # else:
        #     continue

        print("Downloading '{}'...".format(pi_tag), end="")
        span = freq
        try:
            pt = PIPoint.FindPIPoint(piServer, pi_tag) 
            timerange = AFTimeRange(start_time, end_time)  
            span = AFTimeSpan.Parse(span)  
            interpolated = pt.InterpolatedValues(timerange, span, "", False)
            data = [(str(event.Timestamp.LocalTime),event.Value) for event in interpolated]
            print(data)
            df = pd.DataFrame(data,columns=['datetime',str(pi_tag)])
            df['datetime'] = pd.to_datetime(df['datetime'])
            if len(df_combined) == 0:
                df_combined = df.copy()
            else:
                df_combined = pd.merge(left=df_combined, right=df, how='left', on='datetime')
                    
        except:
            print("Extraction error: {}".format(pi_tag))
            continue
        print("Done, {} tags remaining".format(len(pi_tag_list) - pi_tag_list.index(pi_tag) - 1 ))
        
        
    download_process_end_time = timer()
    process_time = download_process_end_time - download_process_start_time
    print("Process finished in {:.1f} seconds".format(process_time))
    
    if server_timebased == True:
        #Adjust back timezone:
        if len(df_combined)>0:
            if str(piServer) in ['RAPP1','172.19.8.9']: #Change timezone from MY to IND
                df_combined['datetime'] = df_combined['datetime'] - timedelta(hours=1)
            
            
    #covert from AF object to string for non-valid data:
    for col in df_combined.columns:
        if col != 'datetime':
            df_combined[col] = df_combined[col].apply(lambda x:str(x) if not isinstance(x,float) else x)
            print( "df_combined[col] details : ", df_combined[col])
            
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
        if str(piServer) in ['RAPP1','172.19.8.9']: #Add one hour, cause SDK will bring it back 1 hour
            start_time = (pd.to_datetime(start_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            end_time = (pd.to_datetime(end_time) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            print('Start time & End time are adjusted according to Indonesia')
    
    print("\nLocal Start Time: {}".format(start_time))
    print("Local End Time: {}\n".format(end_time))
    
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
            if str(piServer) in ['RAPP1','172.19.8.9']: #Change timezone from MY to IND
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
# Server names:
# 172.27.83.15 Xinhui
# 172.27.60.24 Rizhao
# RAPP1
# 172.19.8.9
# PIBSC
# =============================================================================

#pi_tag_list =  ['RPL.421FC1907:me'] #Kerinci
#pi_tag_list = ["OE.PL11.CHE.CLO2","OE.PL12.CHE.CLO2"] #Rizhao  
#pi_tag_list = ["pr:531-PC1-3712.3:me"] #Xinhui  
#pi_tag_list = ["421LC-077.CO"] #TPL
#pi_tag_list = ["371AI1025.PV"] #Bahia    

#piServer = connect_to_server(server_name='172.27.83.15') #Xinhui
#piServer = connect_to_server(server_name='RAPP1') #Kerinci
#piServer = connect_to_server(server_name='172.19.8.9') #TPL
#piServer = connect_to_server(server_name='172.27.60.24') #Rizhao  
#piServer = connect_to_server(server_name='PIBSC') #Bahia
    
#result = pi_interpolated_value(piServer,
#                               pi_tag_list = pi_tag_list,
#                               start_time = "2019-09-02 00:00:00",
#                               end_time = "2019-09-04 11:00:00",
#                               freq = "1m",
#                               server_timebased = True,
#                               convert_invalid_to_nan = False,
#                               datetime_index = False,
#                               convert_col_to_lower = True)


#
#result = pi_as_recorded_value(piServer,
#                               pi_tag = 'RPL.411LC1550:me',
#                               start_time = "2019-09-02 00:00:00",
#                               end_time = "2019-09-05 23:00:00",
#                               server_timebased = True,
#                               convert_invalid_to_nan = False,
#                               datetime_index = False,
#                               convert_col_to_lower = True)



#['RPE.282FI8355:Value','RPE.282FI8365:Value','RPE.282FC8008:mv','RPE.282FC8028:mv']














