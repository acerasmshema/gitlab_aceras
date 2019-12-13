# =============================================================================
# PD1 -> process_line_1
# PD2 -> process_line_2
# PD3 -> process_line_3
# PL11 -> process_line_4
# PL12 -> process_line_5
# =============================================================================

from datetime import timedelta

import numpy as np
# =============================================================================
# Import libraries
# =============================================================================
import pandas as pd

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
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PD1.PULP.PRODUCTION".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [1,1]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD2.PULP.PRODUCTION".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD3.PULP.PRODUCTION".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL11.PULP.PRODUCTION".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.PULP.PRODUCTION".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        
    except:
        error_message = "Production tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
            
    #### ClO2 Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.CLO2".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,2]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.CLO2".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
    except:
        error_message = "ClO2 consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
    
    
    
    #### ClO2 Total Active Cl2
#    kpi_main_idx = np.nan
#    kpi_fetch_idx = np.nan
#    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0023:afc".lower()) 
#    try:
#        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,3]
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0024:afc".lower()) 
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0025:afc".lower()) 
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#    except:
#        error_message = "ClO2 Total Active Cl2 tag cannot be found"
#        print(error_message)
#        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Defoamer
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.Defoamer".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,4]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.Defoamer".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        
    except:
        error_message = "Defoamer tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
    
    
    
    #### H2O2 Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.H202".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,5]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.H202".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        

    except:
        error_message = "H2O2 tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        

    #### NaOH(Own) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.NaOH.OWN".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,6]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = dataset['kpi_value']
        
    except:
        error_message = "NaOH(Own) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
    
    #### NaOH(Purchase) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.NaOH.PUR".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,7]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.NaOH.PUR".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

    except:
        error_message = "NaOH(Purchase) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### NaOH(Total) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.NaOH.TOT".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,8]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.NaOH.TOT".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
    except:
        error_message = "NaOH(Total) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Oxygen Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.O2".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,9]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.O2".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
    except:
        error_message = "Oxygen tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Sulfuric_Acid (H2SO4) Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.H2SO4".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,10]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.H2SO4".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])

    except:
        error_message = "Sulfuric Acid (H2SO4) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### White Liqour Consumption (m3/ADt)
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHE.WL".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,11]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHE.WL".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        
    except:
        error_message = "White Liqour Consumption (m3/ADt) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### White Liqour Consumption (tAA/ADt)
#    kpi_main_idx = np.nan
#    kpi_fetch_idx = np.nan
#    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0013:afc".lower()) 
#    try:
#        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [2,12]
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0014:afc".lower()) 
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0015:afc".lower()) 
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.111XC0016:afc".lower()) 
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#        
#    except:
#        error_message = "White Liqour Consumption (m3/ADt) tag cannot be found"
#        print(error_message)
#        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility Power Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PD1.UT.POWER.TOT".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,13]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD2.UT.POWER.TOT".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD3.UT.POWER.TOT".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL11.UT.PR.POWER".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.UT.PR.POWER".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
    except:
        error_message = "Utility Power Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility LP Steam Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PD1.UT.LP.STEAM".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,14]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD2.UT.LP.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD3.UT.LP.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL11.UT.LP.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.UT.LP.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        

    except:
        error_message = "LP Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility MP Steam Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.UT.MP.STEAM".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,15]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.UT.MP.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
   
    except:
        error_message = "MP Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility Total Steam Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PD1.UT.TOT.STEAM".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,16]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD2.UT.TOT.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD3.UT.TOT.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL11.UT.TOT.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.UT.TOT.STEAM".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        

    except:
        error_message = "Total Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Utility water Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PD1.UT.PR.WATER".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [3,17]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD2.UT.PR.WATER".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PD3.UT.PR.WATER".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL11.UT.PR.WATER".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.UT.PR.WATER".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        

    except:
        error_message = "Total Steam Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Woodchip Consumption - Woodchip total
#    kpi_main_idx = np.nan
#    kpi_fetch_idx = np.nan
#    kpi_main_idx = np.where(dataset['tag_code']=="RPL.411WQ7000.F:Y_DAY".lower()) 
#    try:
#        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,18]
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_1'] = dataset['kpi_value']
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.341WQ5080YD:value".lower()) 
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_2'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#        
#        kpi_fetch_idx = np.where(dataset['tag_code']=="RPL.341WI2794-YD:value".lower())  #PCD
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
#    except:
#        error_message = "Woodchip Consumption - Woodchip total tag cannot be found"
#        print(error_message)
#        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
    
    #### Woodchip Consumption - Chip meter avg
#    kpi_main_idx = np.nan
#    kpi_fetch_idx = np.nan
#    kpi_main_idx = np.where(dataset['tag_code']=="RPL.111XC0048:afc".lower()) 
#    try:
#        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,19]
#        dataset.loc[dataset.index[kpi_main_idx],'process_line_3'] = dataset['kpi_value'] #FL3
#        
#    except:
#        error_message = "Woodchip Consumption - Chip meter avg tag cannot be found"
#        print(error_message)
#        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Woodchip Consumption - Consumption
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHIPS.BDT".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,20]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHIPS.BDT".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        

    except:
        error_message = "Woodchip Consumption - Consumption tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Yield - Bleaching
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHIPS.BLC.YIELD".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,21]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHIPS.BLC.YIELD".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        
    except:
        error_message = "Yield - Bleaching tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
    
    
    #### Yield - Cooking (Digester Yield)
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHIPS.DIG.YIELD".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,22]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHIPS.DIG.YIELD".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        
    except:
        error_message = "Yield - Cooking (Digester Yield) tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    #### Yield - Screening
    kpi_main_idx = np.nan
    kpi_fetch_idx = np.nan
    kpi_main_idx = np.where(dataset['tag_code']=="OE.PL11.CHIPS.SCR.YIELD".lower()) 
    try:
        dataset.loc[dataset.index[kpi_main_idx],['kpi_category_id','kpi_id']] = [4,23]
        dataset.loc[dataset.index[kpi_main_idx],'process_line_4'] = dataset['kpi_value']
        
        kpi_fetch_idx = np.where(dataset['tag_code']=="OE.PL12.CHIPS.SCR.YIELD".lower()) 
        dataset.loc[dataset.index[kpi_main_idx],'process_line_5'] = list(dataset.loc[dataset.index[kpi_fetch_idx],'kpi_value'])
        
        
    except:
        error_message = "Yield - Screening tag cannot be found"
        print(error_message)
        logging.log_message(pulp_kpi_calc_logfile_path, error_message)
        
        
    return dataset.copy()




def kraft_kpi_calcs(raw_data, kpi_info_columns):
    '''
    This function gets the raw PI values and transfer it to KPI table form
    '''
    kraft_pulp_kpi = pd.melt(raw_data,id_vars='datetime', value_vars=raw_data.columns[1:])
    kraft_pulp_kpi['country_id'] = 2 #'China'
    kraft_pulp_kpi['mill_id'] = 3 #'Rizhao'
    kraft_pulp_kpi['bu_id'] = 1 #'Pulp'
    kraft_pulp_kpi['bu_type_id'] = 1 #KP
    kraft_pulp_kpi.rename(columns={'value':'kpi_value','variable':'tag_code'}, inplace=True)
    
    kraft_pulp_kpi['kpi_category_id'] = np.nan
    kraft_pulp_kpi['kpi_id'] = np.nan
    
    kraft_pulp_kpi['process_line_1'] = np.nan #PD1
    kraft_pulp_kpi['process_line_2'] = np.nan #PD2
    kraft_pulp_kpi['process_line_3'] = np.nan #PD3
    kraft_pulp_kpi['process_line_4'] = np.nan #PL11
    kraft_pulp_kpi['process_line_5'] = np.nan #PL12
    kraft_pulp_kpi['process_line_6'] = np.nan 
    kraft_pulp_kpi['process_line_7'] = np.nan 
    kraft_pulp_kpi['process_line_8'] = np.nan 
    kraft_pulp_kpi['process_line_9'] = np.nan
    kraft_pulp_kpi['process_line_10'] = np.nan
    kraft_pulp_kpi['process_line_11'] = np.nan
    kraft_pulp_kpi['process_line_12'] = np.nan
    kraft_pulp_kpi['process_line_13'] = np.nan
    kraft_pulp_kpi['process_line_14'] = np.nan
    kraft_pulp_kpi['process_line_15'] = np.nan
    
    pulp_kpi_calc_logfile_path = r'log\rz_pulp_calc_log.txt'
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

