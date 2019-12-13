# =============================================================================
# FL1 -> process_line_1
# FL2 -> process_line_2
# FL3 -> process_line_3
# PCD -> process_line_4
# PD1 -> process_line_5
# PD2 -> process_line_6
# PD3 -> process_line_7
# PD4 -> process_line_8
# =============================================================================

# =============================================================================
# Import libraries
# =============================================================================
import pandas as pd
import numpy as np
from datetime import timedelta
import logging_funcs as logging

#raw_data=downloaded_data.fillna(method='ffill').copy()
#kpi_info_columns=kpi_info_columns
#dataset = kraft_pulp_kpi.copy()



def tag_2_kraft_kpi_info(dataset,pulp_kpi_calc_logfile_path):
    '''
    '''
    #### Production
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0003:afc".lower())
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [1,1]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0002:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.433FQ1260.F:Y_DAY".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.422YI5139-YD:value".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0005:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0006:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_6'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0007:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_7'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0008:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_8'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        #Hema Entries
        kpi_fetch_idx = np.where(dataset['tag_code'] == "RPL.111XC0221:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx], 'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx], 'kpi_value'])
        # print("HEma trying her code attempt")
        # print(dataset.loc)
    except:
        error_message = "Production tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
            
    #### ClO2 Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0017:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,2]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0018:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0019:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "ClO2 consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
    
    
    
    #### ClO2 Total Active Cl2
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0023:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,3]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0024:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0025:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        # RPL.111XC0251:afc

        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0251:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "ClO2 Total Active Cl2 tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Defoamer
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0170:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,4]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0171:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0172:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0173:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        #HemaEntries
        kpi_fetch_idx = np.where(dataset['tag_code'] == "RPL.111XC0202:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx], 'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx], 'kpi_value'])
        
    except:
        error_message = "Defoamer tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
    
    
    
    #### H2O2 Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0020:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,5]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0021:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0022:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        #HemaEntries
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0194:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "H2O2 tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        

    #### NaOH(Own) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0032:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,6]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0033:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0034:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "NaOH(Own) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
    
    #### NaOH(Purchase) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0035:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,7]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0036:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0037:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "NaOH(Purchase) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### NaOH(Total) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0038:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,8]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0039:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0040:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        #HemaEntries
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0197:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])


    except:
        error_message = "NaOH(Total) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Oxygen Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0026:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,9]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0027:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0028:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0198:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "Oxygen tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Sulfuric_Acid (H2SO4) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0029:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,10]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0030:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0031:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0200:afc".lower())
        # dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])


    except:
        error_message = "Sulfuric Acid (H2SO4) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### White Liqour Consumption (m3/ADt)
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0009:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,11]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0010:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0011:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0012:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0201:afc".lower())
        dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

    except:
        error_message = "White Liqour Consumption (m3/ADt) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### White Liqour Consumption (tAA/ADt)
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0013:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,12]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0014:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0015:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0016:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
    except:
        error_message = "White Liqour Consumption (tAA/ADt) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility Power Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0050:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,13]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0051:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0052:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0053:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0054:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0055:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_6'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0056:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_7'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0057:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_8'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "Utility Power Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility LP Steam Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0060:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,14]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0061:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0062:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0063:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0064:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0065:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_6'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0066:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_7'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0067:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_8'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "LP Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility MP Steam Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0068:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,15]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0069:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0070:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0071:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0072:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0073:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_6'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0074:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_7'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0075:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_8'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "MP Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility Total Steam Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0076:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,16]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0077:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0078:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0079:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0080:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0081:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_6'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0082:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_7'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0083:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_8'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "Total Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility water Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0084:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,17]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0085:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0086:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0087:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0088:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0089:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_6'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0090:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_7'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0091:afc".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_8'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    except:
        error_message = "Total Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)

    #### Woodchip Consumption - Woodchip total @2019-11-28
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code'] == "RPL.111XC0220:afc".lower())
    try:
        dataset.loc[dataset.index[kpi_main_idx], ['kpi_category_id', 'kpi_id']] = [4, 18]
        dataset.loc[dataset.index[kpi_main_idx], 'process_line_1'] = dataset['kpi_value']
    except:
        error_message = "Woodchip Consumption - Woodchip total tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)

    # NOT USING THIS Function BElOW @Hema
    #### Woodchip Consumption - Woodchip total
    # kpi_main_idx = np.nan
    # kpi_fetch_idx = np.nan
    # kpi_main_idx = np.where(dataset['tag_code']=="RPL.411WQ7000.F:Y_DAY".lower())
    # try:
    #     dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,18]
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.341WQ5080YD:value".lower())
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.341WI2794-YD:value".lower())  #PCD
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    #     # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0219:afc".lower())  #PCD
    #     # dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    # except:
    #     error_message = "Woodchip Consumption - Woodchip total tag cannot be found"
    #     print(error_message)
    #     logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
    
    #### Woodchip Consumption - Chip meter avg
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0048:afc".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,19]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = dataset['kpi_value'] #FL3
        
    except:
        error_message = "Woodchip Consumption - Chip meter avg tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Woodchip Consumption - Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    # kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0106:afc".lower()) #FL1
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0211:afc".lower()) #FL1
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,20]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0107:afc".lower()) #FL2
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0213:afc".lower()) #FL2
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0108:afc".lower()) #FL3
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0215:afc".lower()) #FL3
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0109:afc".lower()) #PCD
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0217:afc".lower()) #PCD
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0219:afc".lower()) #PL15
        dataset.loc[dataset.index[kpi_main_idx],'process_line_15'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])


        
    except:
        error_message = "Woodchip Consumption - Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
    # NOT USING THIS Function BElOW @Hema
    #### Yield - Bleaching
    # kpi_main_idx = np.nan
    # kpi_fetch_idx = np.nan
    # kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0126:afc".lower()) #FL1
    # try:
    #     dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,21]
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0127:afc".lower()) #FL2
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0128:afc".lower()) #FL3
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    # except:
    #     error_message = "Yield - Bleaching tag cannot be found"
    #     print(error_message)
    #     logging.log_message(pulp_kpi_calc_logfile_path, error_message)
    #
    
    
    #### Yield - Cooking
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    # kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0118:afc".lower()) #FL1
    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0130:afc".lower()) #FL1
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,22]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0119:afc".lower()) #FL2
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0131:afc".lower()) #FL2
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0120:afc".lower()) #FL3
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0132:afc".lower()) #FL3
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        # kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0121:afc".lower()) #PCD
        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0133:afc".lower()) #PCD
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
    except:
        error_message = "Yield - Cooking tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)


    # NOT USING THIS Function BElOW @Hema
    #### Yield - Screening
    # kpi_main_idx = np.nan
    # kpi_fetch_idx = np.nan
    # kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0122:afc".lower()) #FL1
    # try:
    #     dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,23]
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0123:afc".lower()) #FL2
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0124:afc".lower()) #FL3
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    #     kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0125:afc".lower()) #PCD
    #     dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
    #
    # except:
    #     error_message = "Yield - Screening tag cannot be found"
    #     print(error_message)
    #     logging.log_message(pulp_kpi_calc_logfile_path, error_message)


    return dataset.copy()




def kraft_kpi_calcs(raw_data, kpi_info_columns):
    '''
    This function gets the raw PI values and transfer it to KPI table form
    '''
    kraft_pulp_kpi = pd.melt(raw_data,id_vars='datetime', value_vars=raw_data.columns[1:])
    kraft_pulp_kpi['country_id'] = 1#'Indonesia'
    kraft_pulp_kpi['mill_id'] = 1 #'Kerinci'
    kraft_pulp_kpi['bu_id'] = 1 #'Pulp'
    kraft_pulp_kpi['bu_type_id'] = 1 #KP
    kraft_pulp_kpi.rename(columns={'value':'kpi_value','variable':'tag_code'}, inplace=True)
    
    kraft_pulp_kpi['kpi_category_id'] = np.nan
    kraft_pulp_kpi['kpi_id'] = np.nan
    
    kraft_pulp_kpi['process_line_1'] = np.nan #FL1
    kraft_pulp_kpi['process_line_2'] = np.nan #FL2
    kraft_pulp_kpi['process_line_3'] = np.nan #FL3
    kraft_pulp_kpi['process_line_4'] = np.nan #PCD
    kraft_pulp_kpi['process_line_5'] = np.nan #PD1
    kraft_pulp_kpi['process_line_6'] = np.nan #PD2
    kraft_pulp_kpi['process_line_7'] = np.nan #PD3
    kraft_pulp_kpi['process_line_8'] = np.nan #PD4
    kraft_pulp_kpi['process_line_9'] = np.nan
    kraft_pulp_kpi['process_line_10'] = np.nan
    kraft_pulp_kpi['process_line_11'] = np.nan
    kraft_pulp_kpi['process_line_12'] = np.nan
    kraft_pulp_kpi['process_line_13'] = np.nan
    kraft_pulp_kpi['process_line_14'] = np.nan
    kraft_pulp_kpi['process_line_15'] = np.nan
    
    pulp_kpi_calc_logfile_path = r'log\krc_pulp_calc_log.txt'
    kraft_pulp_kpi = tag_2_kraft_kpi_info(kraft_pulp_kpi.copy(),pulp_kpi_calc_logfile_path)
    
    kraft_pulp_kpi = kraft_pulp_kpi[kpi_info_columns] #change the columns order
    
    kraft_pulp_kpi.dropna(subset=['kpi_category_id','kpi_id'], inplace=True) #drop non-related tags to kraft
    kraft_pulp_kpi['kpi_category_id'] = kraft_pulp_kpi['kpi_category_id'].astype(int)
    kraft_pulp_kpi['kpi_id'] = kraft_pulp_kpi['kpi_id'].astype(int)
    
    #Transfer to 1 day before, as KPI is measured the day after:
    kraft_pulp_kpi['datetime'] = kraft_pulp_kpi['datetime'] - timedelta(1)
    return kraft_pulp_kpi.copy()
    


def dp_kpi_calcs(raw_data, kpi_info_columns):
    pass

