# =============================================================================
# Import libraries
# =============================================================================
import datetime

import numpy as np
import pandas as pd

import logging_funcs as logging


#downloaded_data = pd.DataFrame()


# =============================================================================
#raw_data=downloaded_data.fillna(method='ffill').fillna(0).copy()
#pd.set_option('display.max_rows', 5000)
#pd.set_option('display.max_columns', 500)

# =============================================================================



def power_kpi_calcs(raw_data, minute_kpi_info_columns, daily_kpi_info_columns,
                    working_shift_hour,working_shift_duration,
                    black_liquor_tags, coal_tags, coal_ydy_tags,biomass_tags,natural_gas_pb_tags,
                    natural_gas_vk_tags,natural_gas_rk_tags,raw_water_tags,marine_fuel_oil_tags,diesel_oil_tags,
                    wbl_feed_tags, rb_liquor_firing_tags, rb_total_steam_tags, pb_total_steam_tags,
                    mpprv_tags, lpprv_tags, condensate_return_tags,
                    evap_steam_economy_tags, power_to_rpe_tags, mp_steam_to_rpe_tags,
                    lp_steam_to_rpe_tags, process_water_to_rpe_tags, total_power_tags,
                    total_mp_steam_tags,total_lp_steam_tags, total_demin_water_tags,
                    total_soft_water_tags,total_wl_tags):
    '''
    This function gets the raw PI values and calculates KPIs
    '''
    power_kpi_calc_logfile_path = r'log\krc_power_calc_log.txt'

    #Dataframe to store yesterday KPI 
    daily_kpi_df = pd.DataFrame(index=pd.to_datetime(pd.Series(raw_data.index).dt.date.unique())+pd.DateOffset(hours=working_shift_hour))
    
    # =============================================================================
    # TABLE 1 - INPUT
    # =============================================================================
    
    #Black Liquor
    kpi_name = 'black_liquor'
    try:
        raw_data[kpi_name] = np.sum(raw_data[black_liquor_tags], axis=1)          
        daily_kpi_df[kpi_name] = pd.DataFrame(raw_data[kpi_name].resample(rule=working_shift_duration,base=working_shift_hour).mean())

        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
    
    #Coal
    kpi_name = 'coal'
    try:
        PB1 = ((raw_data['RPE.282FI4051:value']/2) + raw_data['RPE.282FI4053:value'])*86.4
        PB2 = (np.sum(raw_data[['RPE.283WI1503:VALUE','RPE.283WI1506:VALUE','RPE.283WI1509:VALUE','RPE.283WI1512:VALUE']], axis=1) )*86.4
        raw_data[kpi_name] = PB1+PB2

        
        PB1 = np.sum(raw_data[['RPE.282FQ4051_YDY:value','RPE.282FQ4053_YDY:value']], axis =1)
        PB2 = np.sum(raw_data[['RPE.283WQ1503_2:VALUE','RPE.283WQ1509_2:VALUE','RPE.283WQ1512_2:VALUE','RPE.283WQ1506_2:VALUE']], axis =1)
        PB1_ydy = PB1.resample(rule=working_shift_duration,base=working_shift_hour).last()
        PB2_ydy = PB2.resample(rule=working_shift_duration,base=working_shift_hour).last()
        coal_ydy = PB1_ydy +PB2_ydy
        daily_kpi_df[kpi_name] = coal_ydy

    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
        
    
    #Biomass
    kpi_name = 'biomass'
    try:
        PB1 = (raw_data['RPE.281FI-1109.pv'] - (np.sum(raw_data[['RPE.281FT-3615_BMS:Value','RPE.281FT-3625_BMS:Value','RPE.281FT-3635_BMS:Value','RPE.281FT-3645_BMS:Value','RPE.281FT-3655_BMS:Value','RPE.281FT-3665_BMS:Value']],axis=1)/28.175/3600*1000*0.95*0.3)-(np.sum(raw_data[['RPE.281FI-1509B.pv','RPE.281FI-1509C.pv']], axis=1)/60*0.95*10))/3.032*86.4
        PB2 = raw_data['RPE.282FI4050B:value'] * 86.4
        PB3 = np.sum(raw_data[['RPE.283WI1498:VALUE','RPE.283WI1518:VALUE']], axis =1) * 86.4
        raw_data[kpi_name] = PB1+PB2+PB3
        
        PB1_ydy = PB1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        PB2_ydy = PB2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        PB3_ydy = PB3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = PB1_ydy + PB2_ydy + PB3_ydy
        

    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Natural Gas BP
    kpi_name = 'natural_gas_pb'
    try:
        PB1 = np.sum(raw_data[['RPE.281FT-3615_BMS:Value','RPE.281FT-3625_BMS:Value','RPE.281FT-3635_BMS:Value','RPE.281FT-3645_BMS:Value','RPE.281FT-3655_BMS:Value','RPE.281FT-3665_BMS:Value']], axis =1) * 24/28.176
        PB2 = np.sum(raw_data[['RPE.282FI8355:Value','RPE.282FI8365:Value','RPE.282FC8008:mv','RPE.282FC8028:mv']], axis =1) * 24/28.176

        raw_data[kpi_name] = PB1+PB2
        
        PB1_ydy = PB1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        PB2_ydy = PB2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = PB1_ydy + PB2_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Natural Gas VK
    kpi_name = 'natural_gas_vk'
    try:
        raw_data[kpi_name] = raw_data['RPL.485FQ1234_CALCULATE:Input'] * 0.0381295880248238 * 24 
        natural_gas_vk_ydy = raw_data.loc[raw_data['RPL.485FC1234:outP']>0,kpi_name].resample(rule=working_shift_duration,base=working_shift_hour).mean()

        daily_kpi_df[kpi_name] = natural_gas_vk_ydy
        daily_kpi_df[kpi_name].fillna(0, inplace=True)


    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Natural Gas RK
    kpi_name = 'natural_gas_rk'
    try:
        raw_data[kpi_name] = np.sum(raw_data[natural_gas_rk_tags], axis=1) * 0.0381295880248238 * 24 
        natural_gas_rk_ydy = raw_data.loc[raw_data['RPL.485FC1234:outP']>0,kpi_name].resample(rule=working_shift_duration,base=working_shift_hour).mean()

        daily_kpi_df[kpi_name] = natural_gas_rk_ydy
        daily_kpi_df[kpi_name].fillna(0, inplace=True)


    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
    
    #Raw Water
    kpi_name = 'raw_water'
    try:
        process_water_rc1 = raw_data['RPE.271FT-1075:Value']
        process_water_rc2 = raw_data['RPE.271FI-1076.pv']
        process_water_rc3 = raw_data['RPE.272FI1075:Value']
        process_water_rc4 = raw_data['RPE.273FI1076:Value']
        total_process_water_rc = (process_water_rc1+process_water_rc2+process_water_rc3+process_water_rc4)*86.4
        backwash_SFB = raw_data['RPE.272FT2057:Value']
        apr = raw_data['RPE.1281FT1110:Value']
        woodyard1 = raw_data['RPE.272FT2058:Value']
        woodyard2 = raw_data['RPL.326FT5112:value']
        road_truck = raw_data['RPE.273FT1072:Value']
        total_raw_water = (process_water_rc1+process_water_rc2+process_water_rc3+process_water_rc4) + (apr+woodyard1+woodyard2+road_truck) - backwash_SFB
        raw_data[kpi_name] = total_raw_water
        
        process_water_rc1_ydy = process_water_rc1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_rc2_ydy = process_water_rc2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_rc3_ydy = process_water_rc3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_rc4_ydy = process_water_rc4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_process_water_rc_ydy = (process_water_rc1_ydy + process_water_rc2_ydy + process_water_rc3_ydy + process_water_rc4_ydy)*86.4
        backwash_SFB_ydy = backwash_SFB.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        apr_ydy = apr.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        woodyard1_ydy = woodyard1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        woodyard2_ydy = woodyard2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        road_truck_ydy = road_truck.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_raw_water_ydy = (process_water_rc1_ydy + process_water_rc2_ydy + process_water_rc3_ydy + process_water_rc4_ydy) + (apr_ydy + woodyard1_ydy + woodyard2_ydy + road_truck_ydy) - backwash_SFB_ydy
        daily_kpi_df[kpi_name] = total_raw_water_ydy
        

    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
    
    #Marine Fuel Oil (MFO)
    kpi_name = 'marine_fuel_oil'
    kpi_name_p1 = 'mfo_burner_oil_rb_1235'
    kpi_name_p2 = 'mfo_oil_kiln_123'
    try:
        load_burner_oil_flow_rb_1 = raw_data['RPE.461-FI-1709:Value']
        start_up_burner_oil_flow_rb_1 = raw_data['RPE.461-FI-1714:Value']
        load_burner_oil_flow_rb_2 = raw_data['RPE.462FI1709:value']
        start_up_burner_oil_flow_rb_2 = raw_data['RPE.462FI1714:value']
        load_burner_oil_flow_rb_3 = raw_data['RPE.463FI1709:value']
        start_up_burner_oil_flow_rb_3 = raw_data['RPE.463FI1714:value']
        load_burner_mh_oil_flow_rb_5 = raw_data['RPE.465FI4602:av']
        start_up_burner_mh_oil_flow_rb_5 = raw_data['RPE.465FI3231:av']
        oil_flow_to_kiln_1 = raw_data['RPL.481FC1081:mv']
        oil_flow_to_kiln_2 = raw_data['RPL.482FC1310:mv']
        oil_flow_to_kiln_3 = raw_data['RPL.483FCQ1289:me']
        raw_data[kpi_name_p1] = load_burner_oil_flow_rb_1 + start_up_burner_oil_flow_rb_1 +\
                                load_burner_oil_flow_rb_2 + start_up_burner_oil_flow_rb_2 +\
                                load_burner_oil_flow_rb_3 + start_up_burner_oil_flow_rb_3 +\
                                load_burner_mh_oil_flow_rb_5 + start_up_burner_mh_oil_flow_rb_5
                             
        raw_data[kpi_name_p2] = oil_flow_to_kiln_1 + oil_flow_to_kiln_2 +oil_flow_to_kiln_3
        raw_data[kpi_name] = raw_data[kpi_name_p1] + raw_data[kpi_name_p2]
                
        
        load_burner_oil_flow_rb_1_ydy = (load_burner_oil_flow_rb_1.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        start_up_burner_oil_flow_rb_1_ydy = (start_up_burner_oil_flow_rb_1.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        load_burner_oil_flow_rb_2_ydy = (load_burner_oil_flow_rb_2.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        start_up_burner_oil_flow_rb_2_ydy = (start_up_burner_oil_flow_rb_2.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        load_burner_oil_flow_rb_3_ydy = (load_burner_oil_flow_rb_3.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        start_up_burner_oil_flow_rb_3_ydy = (start_up_burner_oil_flow_rb_3.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        load_burner_mh_oil_flow_rb_5_ydy = (load_burner_mh_oil_flow_rb_5.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        start_up_burner_mh_oil_flow_rb_5_ydy = (start_up_burner_mh_oil_flow_rb_5.resample(rule=working_shift_duration,base=working_shift_hour).mean())/60
        oil_flow_to_kiln_1_ydy = oil_flow_to_kiln_1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        oil_flow_to_kiln_2_ydy = oil_flow_to_kiln_2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        oil_flow_to_kiln_3_ydy = oil_flow_to_kiln_3.resample(rule=working_shift_duration,base=working_shift_hour).mean()

        daily_kpi_df[kpi_name_p1] = (load_burner_oil_flow_rb_1_ydy + start_up_burner_oil_flow_rb_1_ydy +\
                             load_burner_oil_flow_rb_2_ydy + start_up_burner_oil_flow_rb_2_ydy +\
                             load_burner_oil_flow_rb_3_ydy + start_up_burner_oil_flow_rb_3_ydy +\
                             load_burner_mh_oil_flow_rb_5_ydy + start_up_burner_mh_oil_flow_rb_5_ydy)*86.4
                    
        daily_kpi_df[kpi_name_p2] = (oil_flow_to_kiln_1_ydy + oil_flow_to_kiln_2_ydy +oil_flow_to_kiln_3_ydy)*86.4
                    
        daily_kpi_df[kpi_name] = daily_kpi_df[kpi_name_p1] + daily_kpi_df[kpi_name_p2]
                            
        

    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
    
    #Diesel oil
    kpi_name = 'diesel_oil'
    try:
        diesel_oil_to_boiler_rb1 = raw_data['RPE.461-FC-3062:MV']/3600
        diesel_oil_to_boiler_rb2 = raw_data['RPE.462FC3062:mv']/3600
        diesel_oil_to_boiler_rb3 = raw_data['RPE.463FC3062:mv']/3600
        diesel_oil_to_boiler_rb5 = raw_data['RPE.465FC6323:me']/3600
        diesel_oil_rb5 =  raw_data['RPE.465FC6241:me']
        diesel_oil_pb1 = raw_data['RPE.281FQ-1509Cydy.pv']
        diesel_oil_bed_lances = raw_data['RPE.282FQ50512_YDY:value']
        diesel_oil_burner_flow = raw_data['RPE.282FQ50557_YDY:value']
        diesel_oil_startup_burner_flow = raw_data['RPE.282FQ5053_YDY:value']
        raw_data[kpi_name] = diesel_oil_to_boiler_rb1 + diesel_oil_to_boiler_rb2 + diesel_oil_to_boiler_rb3 + diesel_oil_to_boiler_rb5+\
                             diesel_oil_rb5 + diesel_oil_pb1 + diesel_oil_bed_lances + diesel_oil_burner_flow + diesel_oil_startup_burner_flow
                             
        
        diesel_oil_to_boiler_rb1_ydy = raw_data['RPE.461FQ3062:Q_Prev'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_to_boiler_rb2_ydy = raw_data['RPE.462FQ3062_YDY:Value'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_to_boiler_rb3_ydy = raw_data['RPE.463FQ3062_YDY:value'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_to_boiler_rb5_ydy = raw_data['RPE.465FQ6323.F:Y_DAY'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_rb5_ydy = raw_data['RPE.465FQ6241.F:Y_DAY'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        
        diesel_oil_pb1_ydy = diesel_oil_pb1.resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_bed_lances_ydy = diesel_oil_bed_lances.resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_burner_flow_ydy = diesel_oil_burner_flow.resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_startup_burner_flow_ydy = diesel_oil_startup_burner_flow.resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_startup_burner_flow2_ydy = raw_data['RPE.283FQ1631H_2:VALUE'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        diesel_oil_startup_burner_flow3_ydy = raw_data['RPE.283FQ1631D_2:VALUE'].resample(rule=working_shift_duration,base=working_shift_hour).last()
        
        diesel_oil_lime_stone_burner_ydy = raw_data['RPE.283FT1669:VALUE'].resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = diesel_oil_to_boiler_rb1_ydy + diesel_oil_to_boiler_rb2_ydy + diesel_oil_to_boiler_rb3_ydy +\
                                 diesel_oil_to_boiler_rb5_ydy + diesel_oil_rb5_ydy + diesel_oil_pb1_ydy + diesel_oil_bed_lances_ydy +\
                                 diesel_oil_burner_flow_ydy + diesel_oil_startup_burner_flow_ydy + diesel_oil_startup_burner_flow2_ydy +\
                                 diesel_oil_startup_burner_flow3_ydy + diesel_oil_lime_stone_burner_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    # =============================================================================
    # TABLE 2 - PROCESS
    # =============================================================================
    
    #Total WBL Feed
    kpi_name = 'total_wbl_feed'
    kpi_name_p1 = 'wbl_feed_flow'
    kpi_name_p2 = 'wbl_feed_solid'
    kpi_name_p3 = 'tds_wbl_feed'
    try:
        wbl_feed_flow_eva1 = raw_data['RPE.451-FC-1003:MV']
        wbl_feed_flow_eva2 = raw_data['RPE.452FC1001:mv']
        wbl_feed_flow_eva3 = raw_data['RPE.453FC1001:mv']
        wbl_feed_flow_eva4 = raw_data['RPE.454FC1450:me']
        wbl_feed_solid_eva1 = raw_data['RPE.451WBL_TS:MV']
        wbl_feed_solid_eva2 = raw_data['RPE.452WBL_TS:value']
        wbl_feed_solid_eva3 = raw_data['RPE.453WBL_TS:value']
        wbl_feed_solid_eva4 = raw_data['RPE.454WBL_TS:av']
        tds_wbl_feed_eva1 = wbl_feed_flow_eva1 * wbl_feed_solid_eva1/100*(1.05+(wbl_feed_solid_eva1-15)/(80-15)*(1.5-1.05))*86.4
        tds_wbl_feed_eva2 = wbl_feed_flow_eva2 * wbl_feed_solid_eva2/100*(1.05+(wbl_feed_solid_eva2-15)/(80-15)*(1.5-1.05))*86.4
        tds_wbl_feed_eva3 = wbl_feed_flow_eva3 * wbl_feed_solid_eva3/100*(1.05+(wbl_feed_solid_eva3-15)/(80-15)*(1.5-1.05))*86.4
        tds_wbl_feed_eva4 = wbl_feed_flow_eva4 * wbl_feed_solid_eva4/100*(1.05+(wbl_feed_solid_eva4-15)/(80-15)*(1.5-1.05))*86.4
        
        raw_data[kpi_name_p1] = wbl_feed_flow_eva1 + wbl_feed_flow_eva2 + wbl_feed_flow_eva3 + wbl_feed_flow_eva4
        raw_data[kpi_name_p2] = wbl_feed_solid_eva1 + wbl_feed_solid_eva2 + wbl_feed_solid_eva3 + wbl_feed_solid_eva4
        raw_data[kpi_name_p3] = tds_wbl_feed_eva1 + tds_wbl_feed_eva2 + tds_wbl_feed_eva3 + tds_wbl_feed_eva4
        raw_data[kpi_name] = raw_data[kpi_name_p3]/0.15/1.05/86.4
        
        wbl_feed_flow_eva1_ydy = wbl_feed_flow_eva1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_flow_eva2_ydy = wbl_feed_flow_eva2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_flow_eva3_ydy = wbl_feed_flow_eva3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_flow_eva4_ydy = wbl_feed_flow_eva4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_solid_eva1_ydy = wbl_feed_solid_eva1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_solid_eva2_ydy = wbl_feed_solid_eva2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_solid_eva3_ydy = wbl_feed_solid_eva3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wbl_feed_solid_eva4_ydy = wbl_feed_solid_eva4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        tds_wbl_feed_eva1_ydy = tds_wbl_feed_eva1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        tds_wbl_feed_eva2_ydy = tds_wbl_feed_eva2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        tds_wbl_feed_eva3_ydy = tds_wbl_feed_eva3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        tds_wbl_feed_eva4_ydy = tds_wbl_feed_eva4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        

        daily_kpi_df[kpi_name_p1] = wbl_feed_flow_eva1_ydy + wbl_feed_flow_eva2_ydy + wbl_feed_flow_eva3_ydy + wbl_feed_flow_eva4_ydy
        daily_kpi_df[kpi_name_p2] = wbl_feed_solid_eva1_ydy + wbl_feed_solid_eva2_ydy + wbl_feed_solid_eva3_ydy + wbl_feed_solid_eva4_ydy
        daily_kpi_df[kpi_name_p3] = tds_wbl_feed_eva1_ydy + tds_wbl_feed_eva2_ydy + tds_wbl_feed_eva3_ydy + tds_wbl_feed_eva4_ydy
        daily_kpi_df[kpi_name] = daily_kpi_df[kpi_name_p3]/0.15/1.05/86.4
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #RB Liquor Firing
    kpi_name = 'rb_liquor_firing'
    kpi_name_p1 = 'liquor_firing_rb1235'
    kpi_name_p2 = 'solid_rb1235'
    kpi_name_p3 = 'total_dry_solid_rb1235'
    try:
        liquor_firing_rb1 = raw_data['RPE.461-FC-1414:MV']
        liquor_firing_rb2 = raw_data['RPE.462BL_LIQ_TO_FURN:value']
        liquor_firing_rb3 = raw_data['RPE.463BL_LIQ_TO_FURN:value']
        liquor_firing_rb5 = raw_data['RPE.465FI2890:av']
        solid_rb1 = raw_data['RPE.461-DI-1415B:Value']
        solid_rb2 = raw_data['RPE.462DT1415B:Value']
        solid_rb3 = raw_data['RPE.463DT1415B:Value']
        solid_rb5 = raw_data['RPE.465DI2850_BMS:av']
        total_dry_solid_rb1 = pd.Series(np.where(liquor_firing_rb1<15,0,liquor_firing_rb1 * solid_rb1 * (1.05+(solid_rb1-15)/(80-15)*(1.5-1.05)))*86.4/100,
                                        index = liquor_firing_rb1.index)
        total_dry_solid_rb2 = pd.Series(np.where(liquor_firing_rb2<15,0,liquor_firing_rb2 * solid_rb2 * (1.05+(solid_rb2-15)/(80-15)*(1.5-1.05)))*86.4/100,
                                        index = liquor_firing_rb2.index)
        total_dry_solid_rb3 = pd.Series(np.where(liquor_firing_rb3<15,0,liquor_firing_rb3 * solid_rb3 * (1.05+(solid_rb3-15)/(80-15)*(1.5-1.05)))*86.4/100,
                                        index = liquor_firing_rb3.index)
        total_dry_solid_rb5 = pd.Series(np.where(liquor_firing_rb5<15,0,liquor_firing_rb5 * solid_rb5 * (1.05+(solid_rb5-15)/(80-15)*(1.5-1.05)))*86.4/100,
                                        index = liquor_firing_rb5.index)
        
        raw_data[kpi_name_p1] = liquor_firing_rb1 + liquor_firing_rb2 + liquor_firing_rb3 + liquor_firing_rb5
        raw_data[kpi_name_p2] = solid_rb1 + solid_rb2 + solid_rb3 + solid_rb5
        raw_data[kpi_name_p3] = total_dry_solid_rb1 + total_dry_solid_rb2 + total_dry_solid_rb3 + total_dry_solid_rb5
        raw_data[kpi_name] = raw_data[kpi_name_p3]/0.8/1.5/86.4
        
        
        liquor_firing_rb1_ydy = liquor_firing_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        liquor_firing_rb2_ydy = liquor_firing_rb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        liquor_firing_rb3_ydy = liquor_firing_rb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        liquor_firing_rb5_ydy = liquor_firing_rb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        solid_rb1_ydy = solid_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        solid_rb2_ydy = solid_rb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        solid_rb3_ydy = solid_rb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        solid_rb5_ydy = solid_rb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_dry_solid_rb1_ydy = total_dry_solid_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_dry_solid_rb2_ydy = total_dry_solid_rb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_dry_solid_rb3_ydy = total_dry_solid_rb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_dry_solid_rb5_ydy = total_dry_solid_rb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        daily_kpi_df[kpi_name_p1] = liquor_firing_rb1_ydy + liquor_firing_rb2_ydy + liquor_firing_rb3_ydy + liquor_firing_rb5_ydy
        daily_kpi_df[kpi_name_p2] = solid_rb1_ydy + solid_rb2_ydy + solid_rb3_ydy + solid_rb5_ydy
        daily_kpi_df[kpi_name_p3] =  total_dry_solid_rb1_ydy +  total_dry_solid_rb2_ydy +  total_dry_solid_rb3_ydy +  total_dry_solid_rb5_ydy
        daily_kpi_df[kpi_name] = daily_kpi_df[kpi_name_p3]/0.8/1.5/86.4
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    #RB Total Steam
    kpi_name = 'rb_total_steam'
    try:
        total_steam_rb1 =  raw_data['RPE.461-FI-1134:Value']
        total_steam_rb2 =  raw_data['RPE.462FI1134:value']
        total_steam_rb3 =  raw_data['RPE.463FI1134:value']
        total_steam_rb5 =  raw_data['RPE.465FI1878:av']
        raw_data[kpi_name] = total_steam_rb1+total_steam_rb2+total_steam_rb3+total_steam_rb5
        
        total_steam_rb1_ydy = total_steam_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_steam_rb2_ydy = total_steam_rb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_steam_rb3_ydy = total_steam_rb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_steam_rb5_ydy = total_steam_rb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = total_steam_rb1_ydy + total_steam_rb2_ydy + total_steam_rb3_ydy + total_steam_rb5_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
    
    #PB Total Steam
    kpi_name = 'pb_total_steam'
    try:
        total_steam_pb1 =  raw_data['RPE.281FI-1109.pv']
        total_steam_pb2 =  raw_data['RPE.282FC1152:mv']
        total_steam_pb3 =  raw_data['RPE.283FI1200:VALUE']
        raw_data[kpi_name] = total_steam_pb1 + total_steam_pb2 + total_steam_pb3
        
        total_steam_pb1_ydy = total_steam_pb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_steam_pb2_ydy = total_steam_pb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        total_steam_pb3_ydy = total_steam_pb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = total_steam_pb1_ydy + total_steam_pb2_ydy + total_steam_pb3_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    #MP PRV
    kpi_name = 'mp_prv'
    try:
        MPPRV1 =  raw_data['RPE.291FQ-4016YDY:Input']
        MPPRV2 =  raw_data['RPE.292FQ1023_YDY:value']
        MPPRV3 =  raw_data['RPE.293FQ1023_YDY:value']
        raw_data[kpi_name] = (MPPRV1 + MPPRV2 + MPPRV3)/86.4
        
        MPPRV1_ydy = MPPRV1.resample(rule=working_shift_duration,base=working_shift_hour).last()
        MPPRV2_ydy = MPPRV2.resample(rule=working_shift_duration,base=working_shift_hour).last()
        MPPRV3_ydy = MPPRV3.resample(rule=working_shift_duration,base=working_shift_hour).last()
        daily_kpi_df[kpi_name] = (MPPRV1_ydy + MPPRV2_ydy + MPPRV3_ydy)/86.4
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    #LP PRV
    kpi_name = 'lp_prv'
    try:
        LPPRV1 =  raw_data['RPE.291FQ-4017YDY:Input']
        LPPRV2 =  raw_data['RPE.291-FQ-3201-YDY:Input']
        LPPRV3 =  raw_data['RPE.292FQ1015_YDY:value']
        LPPRV4 =  raw_data['RPE.293FQ1015_YDY:value']
        raw_data[kpi_name] = (LPPRV1 + LPPRV2 + LPPRV3 + LPPRV4)/86.4
        
        LPPRV1_ydy = LPPRV1.resample(rule=working_shift_duration,base=working_shift_hour).last()
        LPPRV2_ydy = LPPRV2.resample(rule=working_shift_duration,base=working_shift_hour).last()
        LPPRV3_ydy = LPPRV3.resample(rule=working_shift_duration,base=working_shift_hour).last()
        LPPRV4_ydy = LPPRV4.resample(rule=working_shift_duration,base=working_shift_hour).last()
        daily_kpi_df[kpi_name] = (LPPRV1_ydy + LPPRV2_ydy + LPPRV3_ydy + LPPRV4_ydy)/86.4
        
        
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Condensate Return
    kpi_name = 'condensate_return'
    try:
        MB1 = raw_data['RPE.462FI3336:value']	
        MB2 = raw_data['RPE.462FI3339:value']
        MB3 = raw_data['RPE.281-FI-2036:Value']	
        MB4 = raw_data['RPE.281-FI-2039:Value']	
        MB5 = raw_data['RPE.271-FI-2101:Value']/3.6
        MB6 = raw_data['RPE.271-FI-2201:Value']/3.6
        MB7 = raw_data['RPE.271-FI-2301:Value']/3.6
        MB8 = raw_data['RPE.271-FI-2401:Value']/3.6
        TG5 = raw_data['RPE.292FI5961:value']	
        TG7 = raw_data['RPE.294FI3056:av']	
        LPFW1 = raw_data['RPE.281FI-1027.pv']	
        LPFW1_2 = raw_data['RPE.281FI-1099.pv']	
        LPFW2 = raw_data['RPE.462FI1027:value']	
        LPFWPB2 = raw_data['RPE.282FI1053:value']	
        LPFWPB3 = raw_data['RPE.283FI1003:VALUE']	
        LPFWRB5 = raw_data['RPE.465FI1053:av']
        raw_data[kpi_name] = MB1 + MB2 + MB3 + MB4 + MB5 + MB6 + MB7 + MB8 +\
                             TG5 + TG7 +\
                             LPFW1 + LPFW1_2 + LPFW2 +\
                             LPFWPB2 + LPFWPB3 + LPFWRB5
                             
        MB1_ydy = MB1.resample(rule=working_shift_duration,base=working_shift_hour).mean()	
        MB2_ydy = MB2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        MB3_ydy = MB3.resample(rule=working_shift_duration,base=working_shift_hour).mean()	
        MB4_ydy = MB4.resample(rule=working_shift_duration,base=working_shift_hour).mean()	
        MB5_ydy = MB5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        MB6_ydy = MB6.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        MB7_ydy = MB7.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        MB8_ydy = MB8.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        TG5_ydy = TG5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        TG7_ydy = TG7.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        LPFW1_ydy = LPFW1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        LPFW1_2_ydy = LPFW1_2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        LPFW2_ydy = LPFW2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        LPFWPB2_ydy = LPFWPB2.resample(rule=working_shift_duration,base=working_shift_hour).mean()	
        LPFWPB3_ydy = LPFWPB3.resample(rule=working_shift_duration,base=working_shift_hour).mean()	
        LPFWRB5_ydy = LPFWRB5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = MB1_ydy + MB2_ydy + MB3_ydy + MB4_ydy + MB5_ydy + MB6_ydy + MB7_ydy + MB8_ydy +\
                             TG5_ydy + TG7_ydy +\
                             LPFW1_ydy + LPFW1_2_ydy + LPFW2_ydy +\
                             LPFWPB2_ydy + LPFWPB3_ydy + LPFWRB5_ydy
        
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #RB`s steam ratio
    kpi_name = 'rb_steam_ratio'
    try:
        steam_by_oil_rb1 = (load_burner_oil_flow_rb_1 + start_up_burner_oil_flow_rb_1)/60*10
        steam_by_liquor_rb1 = total_steam_rb1 - steam_by_oil_rb1
        
        steam_by_oil_rb2 = (load_burner_oil_flow_rb_2 + start_up_burner_oil_flow_rb_2)/60*10
        steam_by_liquor_rb2 = total_steam_rb2 - steam_by_oil_rb2
        
        steam_by_oil_rb3 = (load_burner_oil_flow_rb_3 + start_up_burner_oil_flow_rb_3)/60*10
        steam_by_liquor_rb3 = total_steam_rb3 - steam_by_oil_rb3
               
        steam_by_oil_rb5 = (load_burner_mh_oil_flow_rb_5 + start_up_burner_mh_oil_flow_rb_5)/60*10
        steam_by_liquor_rb5 = total_steam_rb5 - steam_by_oil_rb5
        
        steam_by_liquor_rb1235 = steam_by_liquor_rb1 + steam_by_liquor_rb2 + steam_by_liquor_rb3 + steam_by_liquor_rb5
        raw_data[kpi_name] = steam_by_liquor_rb1235/raw_data['total_dry_solid_rb1235']*86.4
        
        
        daily_kpi_df[kpi_name] = raw_data[kpi_name].resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Evap. Steam Economy
    kpi_name = 'evap_steam_economy'
    kpi_name_p1 = 'evap_water_evaporation'
    kpi_name_p2 = 'evap_steam'
    try:
        eva1_feed_rate = raw_data['RPE.451-FC-1003:MV']
        eva1_density = raw_data['RPE.451-DC-1020:MV']
        eva1_feed_solid = raw_data['451XI002LP']
        eva1_product_solid = raw_data['451XI014LP']
        eva1_crystall_flow = raw_data['RPE.451-FC-1010:MV']
        eva1_crystall_solid = raw_data['451XI017LP']
        water_evaporation_eva1 = ((( eva1_feed_rate * eva1_density )*(1- eva1_feed_solid / eva1_product_solid ))+( eva1_crystall_flow *(1050+( eva1_crystall_solid -15))/65*450*(1- eva1_crystall_solid / eva1_product_solid )))/1000*3.6
 
        eva2_feed_rate = raw_data['RPE.452FC1001:mv']
        eva2_density = raw_data['RPE.452DT1002:Value']
        eva2_feed_solid = raw_data['452XI002LP']
        eva2_product_solid = raw_data['452XI016LP']
        eva2_product_flow = raw_data['RPE.454FC1020:mv']
        water_evaporation_eva2 = (( eva2_feed_rate * eva2_density *(1- eva2_feed_solid / eva2_product_solid ))*0.0864)/24
        
        eva3_feed_rate = raw_data['RPE.453FC1001:mv']
        eva3_density = raw_data['RPE.453DC1002:mv']
        eva3_feed_solid = raw_data['453XI002LP']
        eva3_product_solid = raw_data['453XI016LP']
        eva3_product_flow = raw_data['RPE.454FC1023:mv']
        water_evaporation_eva3 = (( eva3_feed_rate * eva3_density *(1- eva3_feed_solid / eva3_product_solid ))*0.0864)/24
        
        eva4_feed_rate = raw_data['RPE.454FC1450:me']
        eva4_density = raw_data['RPE.454DI1443:av']
        eva4_crystall_rate = raw_data['RPE.454FC1190:me']
        eva4_crystall_solid = raw_data['451XI014LP']
        eva4_feed_solid = raw_data['RPL.454XI0002LP']
        eva4_product_solid = raw_data['RPE.454DI1114:av']
        eva4_condensate_from_crp = raw_data['RPE.454FI3004:av']
        water_evaporation_eva4 = ((( eva4_feed_rate * eva4_density + eva4_crystall_rate *(1050+( eva4_crystall_solid -15)/65*450)*(1- eva4_feed_solid /( eva4_product_solid ))+ eva4_condensate_from_crp*1000)*0.0864)/24)

        hd1_feed_rate = raw_data['RPE.454FC1020:mv']
        hd1_feed_solid= raw_data['452XI016LP']
        hd1_density= 1.05+(hd1_feed_solid-15)/(80-15)*(1.5-1.05)
        hd1_product_solid= raw_data['RPL.454XI0023LP']
        water_evaporation_hd1 = hd1_feed_rate * hd1_density *(1- hd1_feed_solid/ hd1_product_solid)*3.6
        
        hd2_feed_rate = raw_data['RPE.454FC1020:mv']
        hd2_feed_solid= raw_data['452XI016LP']
        hd2_density= 1.05+(hd2_feed_solid-15)/(80-15)*(1.5-1.05)
        hd2_product_solid= raw_data['RPL.454XI0023LP']
        water_evaporation_hd2 = hd2_feed_rate * hd2_density *(1- hd2_feed_solid/ hd2_product_solid)*3.6
        
        evap_water_evaporation = water_evaporation_eva1 + water_evaporation_eva2 + water_evaporation_eva3 + water_evaporation_eva4 + water_evaporation_hd1 + water_evaporation_hd2
        
        eva1_steam = np.sum(raw_data[['RPE.451-FC-1001:MV','RPE.451-FC-1011B:MV']], axis =1)*3.6
        eva2_steam = np.sum(raw_data[['RPE.452FC1055:mv','RPE.452FC1047:mv','RPE.452FC1050:mv']], axis =1)*3.6
        eva3_steam = np.sum(raw_data[['RPE.453FC1055:mv','RPE.453FC1047:mv','RPE.453FC1050:mv']], axis =1)*3.6
        eva4_steam = np.sum(raw_data[['RPE.454FC1100:me','RPE.454FC1160:me','RPE.454FI1785:av','RPE.454FC1160:me']], axis =1)*3.6
        hd1_steam = (raw_data['RPE.454FC1029:mv'] + np.subtract(raw_data['RPE.454FC1029:mv'],raw_data['RPE.454FI5101:Value']))*3.6
        hd2_steam = (raw_data['RPE.454FC1039:mv'] + np.subtract(raw_data['RPE.454FC1039:mv'],raw_data['RPE.454FI5201:Value']))*3.6
        evap_water_steam = eva1_steam + eva2_steam + eva3_steam + eva4_steam + hd1_steam + hd2_steam
        
        
        evap_steam_economy_eva1 = water_evaporation_eva1 / eva1_steam
        evap_steam_economy_eva2 = water_evaporation_eva2 / eva2_steam
        evap_steam_economy_eva3 = water_evaporation_eva3 / eva3_steam
        evap_steam_economy_eva4 = water_evaporation_eva4 / eva4_steam
        evap_steam_economy_hd1 = water_evaporation_hd1 / hd1_steam
        evap_steam_economy_hd2 = water_evaporation_hd2 / hd2_steam
        
        raw_data[kpi_name_p1] = evap_water_evaporation
        raw_data[kpi_name_p2] = evap_water_steam
        raw_data[kpi_name] = raw_data[kpi_name_p1] / raw_data[kpi_name_p2]
        
                
        water_evaporation_eva1_ydy = water_evaporation_eva1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        water_evaporation_eva2_ydy = water_evaporation_eva2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        water_evaporation_eva3_ydy = water_evaporation_eva3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        water_evaporation_eva4_ydy = water_evaporation_eva4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        water_evaporation_hd1_ydy = water_evaporation_hd1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        water_evaporation_hd2_ydy = water_evaporation_hd2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_water_evaporation_ydy = water_evaporation_eva1_ydy + water_evaporation_eva2_ydy + water_evaporation_eva3_ydy + water_evaporation_eva4_ydy + water_evaporation_hd1_ydy + water_evaporation_hd2_ydy
        
        eva1_steam_ydy = eva1_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        eva2_steam_ydy = eva2_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        eva3_steam_ydy = eva3_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        eva4_steam_ydy = eva4_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        hd1_steam_ydy = hd1_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        hd2_steam_ydy = hd2_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_water_steam_ydy = eva1_steam_ydy + eva2_steam_ydy + eva3_steam_ydy + eva4_steam_ydy + hd1_steam_ydy + hd2_steam_ydy
        
        evap_steam_economy_eva1_ydy = evap_steam_economy_eva1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_steam_economy_eva2_ydy = evap_steam_economy_eva2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_steam_economy_eva3_ydy = evap_steam_economy_eva3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_steam_economy_eva4_ydy = evap_steam_economy_eva4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_steam_economy_hd1_ydy = evap_steam_economy_hd1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        evap_steam_economy_hd2_ydy = evap_steam_economy_hd2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        
        daily_kpi_df[kpi_name_p1] = evap_water_evaporation_ydy
        daily_kpi_df[kpi_name_p2] = evap_water_steam_ydy
        daily_kpi_df[kpi_name] = daily_kpi_df[kpi_name_p1] / daily_kpi_df[kpi_name_p2]
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Power ro RPE
    kpi_name = 'power_to_rpe'
    try:
        power_water_intake = (np.sum(raw_data[['RPE.291J1-AC1-20A-FLT:Input','RPE.292JT-AC12-06:Value']], axis=1)) - 0.602
        power_water_treatment1 = np.sum(raw_data[['RPE.291JI-5102:Value','RPE.291JI-5113:Value','RPE.1J13.P:av']], axis=1)
        power_water_treatment2 = raw_data['RPE.292JT-AC11-03:Value']
        power_boiler_feed_water1 = raw_data['RPE.291J1-AC1-8A-FLT:Input']
        power_boiler_feed_water2 = raw_data['RPE.292JT-AC11-09:Value']
        power_boilder1 = raw_data['RPE.291J1-AC1-3A-FLT:Input']
        power_boilder2 = np.sum(raw_data[['RPE.1J14.P:av','RPE.1J15.P:av','RPE.1J16.P:av']], axis=1)
        power_boilder3 = np.sum(raw_data[['RPE.291JT-AC03-00:Value','RPE.291JT-AC03-05:Value']], axis=1)
        power_eva1 = raw_data['RPE.291J1-AC1-16A-FLT:Input']
        power_eva2 = np.sum(raw_data[['RPE.292JT-AC11-06:Value','RPE.292JT-AC11-16:Value']], axis=1)
        power_eva3 = np.sum(raw_data[['RPE.292JT-AC11-11:Value','RPE.291JT-AC03-04:Value']], axis=1)
        power_eva4 = np.sum(raw_data[['RPE.1J02.P:av','RPE.1J03.P:av']], axis=1)
        power_recovery_boiler1 = np.sum(raw_data[['RPE.291JI-5106:Value','RPE.291JI-5107A:MV']], axis=1)
        power_recovery_boiler2 = raw_data['RPE.292JT-AC11-12:Value']
        power_recovery_boiler3 = np.sum(raw_data[['RPE.292JT-AC11-13:Value','RPE.292JT-AC11-14:Value']], axis=1)
        power_recovery_boiler5 = raw_data['RPE.294JT-AC21.03:Value']
        power_turbine_13_Compressor = raw_data['RPE.291J1-AC1-9A-FLT:Input']
        power_turbine_45_cooling_tower2 = np.sum(raw_data[['RPE.292JT-AC11-01:Value','RPE.291JT-AC03-09:Value']], axis=1)
        power_turbine7 = np.sum(raw_data[['RPE.292JT-AC11-07:Value','RPE.1J10.P:av','RPE.1J11.P:av','RPE.1J12.P:av']], axis=1)
        power_rpe1 = np.sum(raw_data[['RPE.291JI-5119:Value','RPE.292JT-AC11-02:Value','RPE.292JT-AC11-04:Value']], axis=1)
        power_no_meter = 5.3
        
        total_rpe_power = power_water_intake + power_water_treatment1 + power_water_treatment2 +\
                          power_boiler_feed_water1 + power_boiler_feed_water2 +\
                          power_boilder1 + power_boilder2 + power_boilder3 +\
                          power_eva1 + power_eva2 + power_eva3 + power_eva4 +\
                          power_recovery_boiler1 + power_recovery_boiler2 + power_recovery_boiler3 + power_recovery_boiler5 +\
                          power_turbine_13_Compressor + power_turbine_45_cooling_tower2 + power_turbine7 +\
                          power_rpe1 + power_no_meter
                           
        raw_data[kpi_name] = total_rpe_power
        
        power_water_intake_ydy = power_water_intake.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_water_treatment1_ydy = power_water_treatment1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_water_treatment2_ydy = power_water_treatment2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_boiler_feed_water1_ydy = power_boiler_feed_water1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_boiler_feed_water2_ydy = power_boiler_feed_water2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_boilder1_ydy = power_boilder1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_boilder2_ydy = power_boilder2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_boilder3_ydy = power_boilder3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_eva1_ydy = power_eva1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_eva2_ydy = power_eva2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_eva3_ydy = power_eva3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_eva4_ydy = power_eva4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_recovery_boiler1_ydy = power_recovery_boiler1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_recovery_boiler2_ydy = power_recovery_boiler2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_recovery_boiler3_ydy = power_recovery_boiler3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_recovery_boiler5_ydy = power_recovery_boiler5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_turbine_13_Compressor_ydy = power_turbine_13_Compressor.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_turbine_45_cooling_tower2_ydy = power_turbine_45_cooling_tower2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_turbine7_ydy = power_turbine7.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_rpe1_ydy = power_rpe1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        power_no_meter_ydy = power_no_meter
        
        total_rpe_power_ydy = power_water_intake_ydy + power_water_treatment1_ydy + power_water_treatment2_ydy +\
                              power_boiler_feed_water1_ydy + power_boiler_feed_water2_ydy +\
                              power_boilder1_ydy + power_boilder2_ydy + power_boilder3_ydy +\
                              power_eva1_ydy + power_eva2_ydy + power_eva3_ydy + power_eva4_ydy +\
                              power_recovery_boiler1_ydy + power_recovery_boiler2_ydy + power_recovery_boiler3_ydy + power_recovery_boiler5_ydy +\
                              power_turbine_13_Compressor_ydy + power_turbine_45_cooling_tower2_ydy + power_turbine7_ydy +\
                              power_rpe1_ydy + power_no_meter_ydy
                           
        daily_kpi_df[kpi_name] = total_rpe_power_ydy
        
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    #MP Steam to RPE
    kpi_name = 'mp_steam_to_rpe'
    try:
        mp_steam_evap2 = raw_data['RPE.452FI1097:value']
        mp_steam_evap3 = raw_data['RPE.453FI1097:value']
        mp_steam_evap4 = np.sum(raw_data[['RPE.454FC1160:me','RPE.454FI1785:av']],axis=1)
        mp_steam_hd1 = np.subtract(raw_data['RPE.454FC1029:mv'], raw_data['RPE.454FI5101:Value'])
        mp_steam_hd2 = np.subtract(raw_data['RPE.454FC1039:mv'], raw_data['RPE.454FI5201:Value'])
        mp_steam_rb1 = raw_data['RPE.461-FI-1651:Value']
        mp_steam_rb2 = raw_data['RPE.462FI1651:value']
        mp_steam_rb3 = raw_data['RPE.463FI1651:value']
        mp_steam_rb5 = raw_data['RPE.465FI5201:av']
        
        mp_steam_total = mp_steam_evap2 + mp_steam_evap3 + mp_steam_evap4 + mp_steam_hd1 + mp_steam_hd2 +\
                         mp_steam_rb1 + mp_steam_rb2 + mp_steam_rb3 + mp_steam_rb5
                         
        raw_data[kpi_name] = mp_steam_total
        
        mp_steam_evap2_ydy = mp_steam_evap2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_evap3_ydy = mp_steam_evap3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_evap4_ydy = mp_steam_evap4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_hd1_ydy = mp_steam_hd1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_hd2_ydy = mp_steam_hd2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_rb1_ydy = mp_steam_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_rb2_ydy = mp_steam_rb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_rb3_ydy = mp_steam_rb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_rb5_ydy = mp_steam_rb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        mp_steam_total_ydy = mp_steam_evap2_ydy + mp_steam_evap3_ydy + mp_steam_evap4_ydy + mp_steam_hd1_ydy + mp_steam_hd2_ydy +\
                         mp_steam_rb1_ydy + mp_steam_rb2_ydy + mp_steam_rb3_ydy + mp_steam_rb5_ydy
                         
        daily_kpi_df[kpi_name] = mp_steam_total_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #LP Steam to RPE
    kpi_name = 'lp_steam_to_rpe'
    try:
        lp_steam_evap1 = raw_data['RPE.451-FC-1001:MV']
        lp_steam_evap2 = raw_data['RPE.452TOTALSTM:value']
        lp_steam_evap3 = np.sum(raw_data[['RPE.453FI1047:value','RPE.453FI1050:value','RPE.453FI1053:value','RPE.453FI1055:value']],axis=1)
        lp_steam_evap4 = raw_data['RPE.454FC1100:me']
        lp_steam_hd1 = raw_data['RPE.454FI5101:Value']
        lp_steam_hd2 = raw_data['RPE.454FI5201:Value']
        lp_steam_crp2 = raw_data['RPE.454FI3001A:av']
        lp_steam_rb1 = raw_data['RPE.461-FI-1652:Value']
        lp_steam_rb2 = raw_data['RPE.462FI1652:value']
        lp_steam_rb3 = raw_data['RPE.463FI1652:value']
        lp_steam_rb5 = raw_data['RPE.465FI5211:av']
        lp_steam_bfw1 = np.sum(raw_data[['RPE.281FI-1027.pv','RPE.281FI-1099.pv']],axis=1)
        lp_steam_bfw2 = raw_data['RPE.462FI1027:value']
        lp_steam_bfw_PB2 = raw_data['RPE.282FI1053:value']
        lp_steam_bfw_PB3 = np.sum(raw_data[['RPE.283FI1003:VALUE','RPE.283FT1433:VALUE']],axis=1)
        
        lp_steam_total = lp_steam_evap1 + lp_steam_evap2 + lp_steam_evap3 + lp_steam_evap4 +\
                         lp_steam_hd1 + lp_steam_hd2 + lp_steam_crp2 +\
                         lp_steam_rb1 + lp_steam_rb2 + lp_steam_rb3 + lp_steam_rb5 +\
                         lp_steam_bfw1 + lp_steam_bfw2 + lp_steam_bfw_PB2 + lp_steam_bfw_PB3
                         
        raw_data[kpi_name] = lp_steam_total
        
        
        lp_steam_evap1_ydy = lp_steam_evap1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_evap2_ydy = lp_steam_evap2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_evap3_ydy = lp_steam_evap3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_evap4_ydy = lp_steam_evap4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_hd1_ydy = lp_steam_hd1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_hd2_ydy = lp_steam_hd2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_crp2_ydy = lp_steam_crp2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_rb1_ydy = lp_steam_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_rb2_ydy = lp_steam_rb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_rb3_ydy = lp_steam_rb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_rb5_ydy = lp_steam_rb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_bfw1_ydy = lp_steam_bfw1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_bfw2_ydy  = lp_steam_bfw2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_bfw_PB2_ydy = lp_steam_bfw_PB2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_bfw_PB3_ydy = lp_steam_bfw_PB3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        
        lp_steam_total_ydy = lp_steam_evap1_ydy + lp_steam_evap2_ydy + lp_steam_evap3_ydy + lp_steam_evap4_ydy +\
                         lp_steam_hd1_ydy + lp_steam_hd2_ydy + lp_steam_crp2_ydy +\
                         lp_steam_rb1_ydy + lp_steam_rb2_ydy + lp_steam_rb3_ydy + lp_steam_rb5_ydy +\
                         lp_steam_bfw1_ydy + lp_steam_bfw2_ydy + lp_steam_bfw_PB2_ydy + lp_steam_bfw_PB3_ydy
                         
        daily_kpi_df[kpi_name] = lp_steam_total_ydy
        
        
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Process Water to RPE
    kpi_name = 'process_water_to_rpe'
    try:
        process_water_wtp = pd.Series(1.5, index=raw_data.index)
        process_water_rb1 = raw_data['RPE.461-FI-1900-Totalizer-Yday:Value']
        process_water_rb23 = pd.Series(6.05, index=raw_data.index)
        process_water_rb5 = raw_data['RPE.465FI5101:av']
        process_water_pb1 = raw_data['RPE.281FI-1900.pv']
        process_water_pb2 = raw_data['RPE.282FT9012:Value']
        process_water_pb3 = raw_data['RPE.283FT1232:VALUE']
        process_water_bfw_1 = pd.Series(4, index=raw_data.index)
        process_water_bfw_2 = pd.Series(4, index=raw_data.index)
        process_water_bfw_5 = pd.Series(0, index=raw_data.index)
        process_water_bfw1 = raw_data['RPE.281FI-1901.pv']
        process_water_bfw2 = pd.Series(6, index=raw_data.index)        
        process_water_tg_compressor = (process_water_pb1)/25*100*20/100        
        process_water_eva1 = pd.Series(0.2, index=raw_data.index)
        process_water_eva2 = pd.Series(0.2, index=raw_data.index)
        process_water_eva3 = pd.Series(0.2, index=raw_data.index) 
        process_water_eva4 = pd.Series(0.2, index=raw_data.index) 
        process_water_ct1 = raw_data['RPE.271FI1229:Value']
        process_water_ct2 = raw_data['RPE.272FI3140:Value']
        process_water_ct3 = raw_data['RPE.452FT2301:Value']
        process_water_ct4 = raw_data['RPE.294FI4077:av']
        process_water_acf1 = raw_data['RPE.271FC-1001:MV']
        process_water_acf2 = raw_data['RPE.271FC-1002:MV']
        process_water_acf3 = raw_data['RPE.271FC-1003:MV']
        process_water_acf4 = raw_data['RPE.271FC-1004:MV']
        process_water_acf5 = raw_data['RPE.272FC1001:mv']
        process_water_acf6 = raw_data['RPE.272FC1002:mv']
        process_water_acf7 = raw_data['RPE.272FC1003:mv']
        process_water_acf8 = raw_data['RPE.272FC1004:mv']
        process_water_acf9 = raw_data['RPE.272FC3001:mv']
        process_water_acf10 = raw_data['RPE.272FC3071:MV']
        process_water_inlet_softwater = raw_data['RPE.274FT-1006:Input']
        process_water_outlet_softwater = raw_data['RPE.274FT-1007:Input']
        process_water_water_to_bark_effluent1 = raw_data['RPL.132FT1160:value']
        process_water_water_to_bark_effluent2 = raw_data['RPL.131FT1331:value']
        process_water_recaust_kiln1 = raw_data['RPL.471FT1900:value']
        process_water_recaust_kiln3 = raw_data['RPL.483FI1703:value']        
        process_water_recovery_water_pb1_rb1 = raw_data['RPE.281FI-2100:MV']
        process_water_ptsi = raw_data['RPL.132FT1281:value']


        process_water_total_rb = process_water_rb1 + process_water_rb23 + process_water_rb5
        process_water_total_pb = process_water_pb1 + process_water_pb2 + process_water_pb3
        process_water_total_bfw = process_water_bfw_1 + process_water_bfw_2 + process_water_bfw_5 + process_water_bfw1 + process_water_bfw2
        process_water_total_eva = process_water_eva1 + process_water_eva2 + process_water_eva3 + process_water_eva4
        process_water_total_ct = process_water_ct1 + process_water_ct2 + process_water_ct3 + process_water_ct4
        process_water_demin_plant = process_water_acf1 + process_water_acf2 + process_water_acf3 + process_water_acf4 + process_water_acf5 +\
                                    process_water_acf6 + process_water_acf7 + process_water_acf8 + process_water_acf9 + process_water_acf10
        process_water_softwater = process_water_inlet_softwater - process_water_outlet_softwater
        process_water_effluent = process_water_water_to_bark_effluent1 + process_water_water_to_bark_effluent2
        process_water_recaust = process_water_recaust_kiln1 + process_water_recaust_kiln3
        
        
        process_water_total = process_water_wtp + process_water_total_rb + process_water_total_pb + process_water_total_bfw +\
                              process_water_tg_compressor + process_water_total_eva +\
        		              process_water_total_ct + process_water_demin_plant +\
                              process_water_softwater + process_water_effluent +\
        		              process_water_recaust - process_water_recovery_water_pb1_rb1 - process_water_ptsi
                              
        raw_data[kpi_name] = process_water_total
        
        
        process_water_wtp_ydy = process_water_wtp.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_total_rb_ydy = process_water_total_rb.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_total_pb_ydy = process_water_total_pb.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_total_bfw_ydy = process_water_total_bfw.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_tg_compressor_ydy = process_water_tg_compressor.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_total_eva_ydy = process_water_total_eva.resample(rule=working_shift_duration,base=working_shift_hour).mean()             
        process_water_total_ct_ydy = process_water_total_ct.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_demin_plant_ydy = process_water_demin_plant.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_softwater_ydy = process_water_softwater.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_effluent_ydy = process_water_effluent.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_recaust_ydy = process_water_recaust.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_recovery_water_pb1_rb1_ydy = process_water_recovery_water_pb1_rb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        process_water_ptsi_ydy = process_water_ptsi.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        process_water_total_ydy = process_water_wtp_ydy + process_water_total_rb_ydy + process_water_total_pb_ydy + process_water_total_bfw_ydy +\
                                  process_water_tg_compressor_ydy + process_water_total_eva_ydy +\
                                  process_water_total_ct_ydy + process_water_demin_plant_ydy +\
                                  process_water_softwater_ydy + process_water_effluent_ydy +\
                                  process_water_recaust_ydy - process_water_recovery_water_pb1_rb1_ydy - process_water_ptsi_ydy
                              
        daily_kpi_df[kpi_name] = process_water_total_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
    
    
    # =============================================================================
    # TABLE 3 - OUTPUT
    # =============================================================================
    
    #Total Power
    kpi_name = 'total_power'
    try:
        total_power = raw_data['RPE.TOTAL_POWER:value']        
        raw_data[kpi_name] = total_power
        
        total_power_ydy = total_power.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = total_power_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
        
    #Total HP Steam
    kpi_name = 'total_hp_steam'
    try:
        total_hp_steam = total_steam_rb1 + total_steam_rb2 + total_steam_rb3 + total_steam_rb5 +\
                         steam_by_oil_rb1 + steam_by_oil_rb2 + steam_by_oil_rb3 + steam_by_oil_rb5 +\
                         steam_by_liquor_rb1 + steam_by_liquor_rb2 + steam_by_liquor_rb3 + steam_by_liquor_rb5 +\
                         total_steam_pb1 + total_steam_pb2 + total_steam_pb3
        
        raw_data[kpi_name] = total_hp_steam
        
        total_hp_steam_ydy = total_hp_steam.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        daily_kpi_df[kpi_name] = total_hp_steam_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Total MP Steam
    kpi_name = 'total_mp_steam'
    try:
        mp_steam_production_tg2 = raw_data['RPE.291FI-2002:MV']
        mp_steam_production_tg3 = raw_data['RPE.291FI-3002:MV']
        mp_steam_production_tg4 = raw_data['RPE.292FI4960:value']
        mp_steam_production_tg6 = raw_data['RPE.293FI6771:value']
        mp_steam_production_tg5 = raw_data['RPE.292FT5003:Value']
        mp_steam_production_tg7 = raw_data['RPE.294FI3021:av']
        mp_steam_production_mpprv1 = raw_data['RPE.291FI-4024:MV']
        mp_steam_production_mpprv2 = raw_data['RPE.292FI1023:value']
        mp_steam_production_mpprv3 = raw_data['RPE.293FI1023:value']
        
        total_mp_steam_production_tg = (mp_steam_production_tg2 + mp_steam_production_tg3 + mp_steam_production_tg4 +\
                                        mp_steam_production_tg6 + mp_steam_production_tg5 + mp_steam_production_tg7)*86.4
                                        
        total_mp_steam_production_prv = (mp_steam_production_mpprv1 + mp_steam_production_mpprv2 + mp_steam_production_mpprv3)*86.4
        total_mp_steam = total_mp_steam_production_tg + total_mp_steam_production_prv
        
        raw_data[kpi_name] = total_mp_steam/86.4  
        
        
        mp_steam_production_tg2_ydy = mp_steam_production_tg2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_tg3_ydy = mp_steam_production_tg3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_tg4_ydy = mp_steam_production_tg4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_tg6_ydy = mp_steam_production_tg6.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_tg5_ydy = mp_steam_production_tg5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_tg7_ydy = mp_steam_production_tg7.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_mpprv1_ydy = mp_steam_production_mpprv1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_mpprv2_ydy = mp_steam_production_mpprv2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        mp_steam_production_mpprv3_ydy = mp_steam_production_mpprv3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        total_mp_steam_production_tg_ydy = (mp_steam_production_tg2_ydy + mp_steam_production_tg3_ydy + mp_steam_production_tg4_ydy +\
                                        mp_steam_production_tg6_ydy + mp_steam_production_tg5_ydy + mp_steam_production_tg7_ydy)*86.4
                                        
        total_mp_steam_production_prv_ydy = (mp_steam_production_mpprv1_ydy + mp_steam_production_mpprv2_ydy + mp_steam_production_mpprv3_ydy)*86.4
        total_mp_steam_ydy = total_mp_steam_production_tg_ydy + total_mp_steam_production_prv_ydy
        daily_kpi_df[kpi_name] = total_mp_steam_ydy/86.4
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
        
    #Total LP Steam
    kpi_name = 'total_lp_steam'
    try:
        lp_steam_production_tg2 = raw_data['RPE.291-FI-2003:MV']
        lp_steam_production_tg3 = raw_data['RPE.291-FI-3003:MV']
        hp_steam_turbine_tg4 = raw_data['RPE.292FI4100:value']
        lp_steam_production_tg4 = hp_steam_turbine_tg4 - mp_steam_production_tg4
        hp_steam_turbine_tg5 = raw_data['RPE.293FI6702:value']
        lp_steam_cond_flow = raw_data['RPE.292FI5961:value']
        lp_steam_hp_heater1 = raw_data['RPE.282FI9010:value']
        lp_steam_hp_heater2 = raw_data['RPE.282FI9011:value']
        lp_steam_lp_heater = raw_data['RPE.292FI6000:value']
        lp_steam_production_tg5 = hp_steam_turbine_tg5 - mp_steam_production_tg5 - lp_steam_cond_flow -\
                                  lp_steam_hp_heater1 - lp_steam_hp_heater2 - lp_steam_lp_heater
        hp_steam_turbine_tg6 = raw_data['RPE.292FI5100:value']
        lp_steam_production_tg6 = hp_steam_turbine_tg6 - mp_steam_production_tg6
        lp_steam_production_tg7 = raw_data['RPE.294FI3060:av']
        lp_steam_production_prv1 = raw_data['RPE.291FI-4017:MV']
        lp_steam_production_prv2 = raw_data['RPE.291FI-3201:MV']
        lp_steam_production_prv3 = raw_data['RPE.292FI1015:value']
        lp_steam_production_prv4 = raw_data['RPE.293FI1015:value']
        
        total_lp_steam_production_tg = (lp_steam_production_tg2 + lp_steam_production_tg3 + lp_steam_production_tg4 +\
                                        lp_steam_production_tg6 + lp_steam_production_tg5 + lp_steam_production_tg7)*86.4
                                        
        total_lp_steam_production_prv = (lp_steam_production_prv1 + lp_steam_production_prv2 + lp_steam_production_prv3 + lp_steam_production_prv4)*86.4
        total_lp_steam = total_lp_steam_production_tg + total_lp_steam_production_prv
        
        raw_data[kpi_name] = total_lp_steam/86.4  
        
        lp_steam_production_tg2_ydy = lp_steam_production_tg2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_tg3_ydy = lp_steam_production_tg3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        hp_steam_turbine_tg4_ydy = hp_steam_turbine_tg4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_tg4_ydy = hp_steam_turbine_tg4_ydy - mp_steam_production_tg4_ydy
        hp_steam_turbine_tg5_ydy = hp_steam_turbine_tg5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_cond_flow_ydy = lp_steam_cond_flow.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_hp_heater1_ydy = lp_steam_hp_heater1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_hp_heater2_ydy = lp_steam_hp_heater2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_lp_heater_ydy = lp_steam_lp_heater.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_tg5_ydy = hp_steam_turbine_tg5_ydy - mp_steam_production_tg5_ydy - lp_steam_cond_flow_ydy -\
                                      lp_steam_hp_heater1_ydy - lp_steam_hp_heater2_ydy - lp_steam_lp_heater_ydy
        hp_steam_turbine_tg6_ydy = hp_steam_turbine_tg6.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_tg6_ydy = hp_steam_turbine_tg6_ydy - mp_steam_production_tg6_ydy
        lp_steam_production_tg7_ydy = lp_steam_production_tg7.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_prv1_ydy = lp_steam_production_prv1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_prv2_ydy = lp_steam_production_prv2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_prv3_ydy = lp_steam_production_prv3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        lp_steam_production_prv4_ydy = lp_steam_production_prv4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        total_lp_steam_production_tg_ydy = (lp_steam_production_tg2_ydy + lp_steam_production_tg3_ydy + lp_steam_production_tg4_ydy +\
                                            lp_steam_production_tg6_ydy + lp_steam_production_tg5_ydy + lp_steam_production_tg7_ydy)*86.4
                                        
        total_lp_steam_production_prv_ydy = (lp_steam_production_prv1_ydy + lp_steam_production_prv2_ydy + lp_steam_production_prv3_ydy + lp_steam_production_prv4_ydy)*86.4
        total_lp_steam_ydy = (total_lp_steam_production_tg_ydy + total_lp_steam_production_prv_ydy)/86.4
        
        daily_kpi_df[kpi_name] = total_lp_steam_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    #Total Process Water
    kpi_name = 'total_process_water'
    try:
        raw_data[kpi_name] = total_process_water_rc/86.4       
        daily_kpi_df[kpi_name] = total_process_water_rc_ydy/86.4
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Total Demin Water
    kpi_name = 'total_demin_water'
    try:
        demin_production_mb1 = raw_data['RPE.271FI-1031flt:Value']
        demin_production_mb2 = raw_data['RPE.271FI-1032flt:Value']
        demin_production_mb3 = raw_data['RPE.271FI-1033flt:Value']
        demin_production_mb4 = raw_data['RPE.272FC1031:mv']
        demin_production_mb5 = raw_data['RPE.272FC1032:mv']
        demin_production_mb6 = raw_data['RPE.272FC1033:mv']
        demin_production_mb7 = raw_data['RPE.272FQ3040_YDY:value']/86.4
        demin_production_mb8 = raw_data['RPE.272FT3109:Value']
        
        total_demin_production = demin_production_mb1 + demin_production_mb2 + demin_production_mb3 +\
                                 demin_production_mb4 + demin_production_mb5 + demin_production_mb6 +\
                                 demin_production_mb7 + demin_production_mb8
        raw_data[kpi_name] = total_demin_production
        
        demin_production_mb1_ydy = demin_production_mb1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb2_ydy = demin_production_mb2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb3_ydy = demin_production_mb3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb4_ydy = demin_production_mb4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb5_ydy = demin_production_mb5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb6_ydy = demin_production_mb6.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb7_ydy = demin_production_mb7.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        demin_production_mb8_ydy = demin_production_mb8.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        total_demin_production_ydy = demin_production_mb1_ydy + demin_production_mb2_ydy + demin_production_mb3_ydy +\
                                 demin_production_mb4_ydy + demin_production_mb5_ydy + demin_production_mb6_ydy +\
                                 demin_production_mb7_ydy + demin_production_mb8_ydy
        daily_kpi_df[kpi_name] = total_demin_production_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Total Soft Water
    kpi_name = 'total_soft_water'
    try:
        soft_water_apr = raw_data['RPE.1271FI1510:Value']
        soft_water_fl = raw_data['RPE.274FT-1007:Input']
        total_soft_water = soft_water_apr + soft_water_fl
        raw_data[kpi_name] = total_soft_water

        soft_water_apr_ydy = soft_water_apr.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        soft_water_fl_ydy = soft_water_fl.resample(rule=working_shift_duration,base=working_shift_hour).mean()/3.6
        total_soft_water_ydy = soft_water_apr_ydy + soft_water_fl_ydy       
        daily_kpi_df[kpi_name] = total_soft_water_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
        
    #Total WL
    kpi_name = 'total_wl'
    try:
        wl_consumption_fl1_p1 = raw_data['RPL.411FC2014:me']
        wl_consumption_fl1_p2 = raw_data['RPL.411FI2072:av']
        wl_consumption_fl1_p3 = raw_data['RPL.411FI2219:av']      
        wl_consumption_fl1_p4 = (raw_data['RPL.411LC1550:me'].resample(rule=working_shift_duration,base=working_shift_hour).transform('last') -\
                                 raw_data['RPL.411LC1550:me'].resample(rule=working_shift_duration,base=working_shift_hour).transform('first'))*2/86.4        
        wl_consumption_fl1 = wl_consumption_fl1_p1 + wl_consumption_fl1_p2 + wl_consumption_fl1_p3 + wl_consumption_fl1_p4
        
        wl_consumption_fl2_p1 = raw_data['RPL.412FC1627:mv']
        wl_consumption_fl2_p2 = raw_data['RPL.422FC2307:mv']
        wl_consumption_fl2 = (wl_consumption_fl2_p1 + wl_consumption_fl2_p2)+3100/86.4
        
        
        wl_consumption_fl3_p1 = raw_data['RPL.413FC1006:me']
        wl_consumption_fl3_p2 = raw_data['RPL.413FC1007:me']
        wl_consumption_fl3_p3 = raw_data['RPL.413FC1009:me'] 
        wl_consumption_fl3_p4 = raw_data['RPL.413FC1012:me'] 
        wl_consumption_fl3_p5 = raw_data['RPL.423FI1801:av']/3.6
        wl_consumption_fl3 = wl_consumption_fl3_p1 + wl_consumption_fl3_p2 + wl_consumption_fl3_p3 + wl_consumption_fl3_p4 + wl_consumption_fl3_p5
        
        
        wl_consumption_pcd_p1 = raw_data['RPL.412FC5015:mv']
        wl_consumption_pcd_p2 = raw_data['RPL.412FC5051:mv']
        wl_consumption_pcd = wl_consumption_pcd_p1 + wl_consumption_pcd_p2
        
        wl_consumption_cp = raw_data['RPL.623FC1186:me']
        
        wl_stock_diff_wltf1 = (raw_data['RPL.411LC1550:me'].resample(rule=working_shift_duration,base=working_shift_hour).transform('last') -\
                               raw_data['RPL.411LC1550:me'].resample(rule=working_shift_duration,base=working_shift_hour).transform('first'))*2/86.4 
        wl_stock_diff_wltf2 = (raw_data['RPL.412LT1608:value'].resample(rule=working_shift_duration,base=working_shift_hour).transform('last') -\
                               raw_data['RPL.412LT1608:value'].resample(rule=working_shift_duration,base=working_shift_hour).transform('first'))*40/86.4 
        wl_stock_diff_wls1 = (raw_data['RPL.472LT4208:value'].resample(rule=working_shift_duration,base=working_shift_hour).transform('last') -\
                               raw_data['RPL.472LT4208:value'].resample(rule=working_shift_duration,base=working_shift_hour).transform('first'))*49.884/86.4 
        wl_stock_diff_wls2 = (raw_data['RPL.472LT4211:value'].resample(rule=working_shift_duration,base=working_shift_hour).transform('last') -\
                               raw_data['RPL.472LT4211:value'].resample(rule=working_shift_duration,base=working_shift_hour).transform('first'))*53.435/86.4 
        
        
        total_wl_consumption = wl_consumption_fl1 + wl_consumption_fl2 + wl_consumption_fl3 + wl_consumption_pcd + wl_consumption_cp
        total_wl_stock_diff = wl_stock_diff_wltf1 + wl_stock_diff_wltf2 + wl_stock_diff_wls1 + wl_stock_diff_wls2
        raw_data[kpi_name] = total_wl_consumption - total_wl_stock_diff
        
        
        
        wl_consumption_fl1_p1_ydy = wl_consumption_fl1_p1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl1_p2_ydy = wl_consumption_fl1_p2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl1_p3_ydy = wl_consumption_fl1_p3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl1_p4_ydy = wl_consumption_fl1_p4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl1_ydy = wl_consumption_fl1_p1_ydy + wl_consumption_fl1_p2_ydy + wl_consumption_fl1_p3_ydy + wl_consumption_fl1_p4_ydy
        
        wl_consumption_fl2_p1_ydy = wl_consumption_fl2_p1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl2_p2_ydy = wl_consumption_fl2_p2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl2_ydy = (wl_consumption_fl2_p1_ydy + wl_consumption_fl2_p2_ydy)+3100/86.4
        
        wl_consumption_fl3_p1_ydy = wl_consumption_fl3_p1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl3_p2_ydy = wl_consumption_fl3_p2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl3_p3_ydy = wl_consumption_fl3_p3.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl3_p4_ydy = wl_consumption_fl3_p4.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl3_p5_ydy = wl_consumption_fl3_p5.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_fl3_ydy = wl_consumption_fl3_p1_ydy + wl_consumption_fl3_p2_ydy + wl_consumption_fl3_p3_ydy + wl_consumption_fl3_p4_ydy + wl_consumption_fl3_p5_ydy
        
        wl_consumption_pcd_p1_ydy = wl_consumption_pcd_p1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_pcd_p2_ydy = wl_consumption_pcd_p2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_consumption_pcd_ydy = wl_consumption_pcd_p1_ydy + wl_consumption_pcd_p2_ydy
        
        wl_consumption_cp_ydy = wl_consumption_cp.resample(rule=working_shift_duration,base=working_shift_hour).mean()/60
        
        wl_stock_diff_wltf1_ydy = wl_stock_diff_wltf1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_stock_diff_wltf2_ydy = wl_stock_diff_wltf2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_stock_diff_wls1_ydy = wl_stock_diff_wls1.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        wl_stock_diff_wls2_ydy = wl_stock_diff_wls2.resample(rule=working_shift_duration,base=working_shift_hour).mean()
        
        
        total_wl_consumption_ydy = wl_consumption_fl1_ydy + wl_consumption_fl2_ydy +wl_consumption_fl3_ydy + wl_consumption_pcd_ydy + wl_consumption_cp_ydy
        total_wl_stock_diff_ydy = wl_stock_diff_wltf1_ydy + wl_stock_diff_wltf2_ydy + wl_stock_diff_wls1_ydy + wl_stock_diff_wls2_ydy
        daily_kpi_df[kpi_name] = total_wl_consumption_ydy - total_wl_stock_diff_ydy
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
        
    #
    kpi_name = ''
    try:
        pass
        
    except:
        error_message = "{} tags cannot be found".format(kpi_name)
        print(error_message)
        logging.log_message(power_kpi_calc_logfile_path, error_message)
        
    
    ###################################
    if raw_data.index[-1].time() > datetime.time(working_shift_hour,0,0):
        daily_kpi_df = daily_kpi_df.iloc[daily_kpi_df.index.date < raw_data.index[-1].date(),:]
        
    
    daily_kpi_df.reset_index(inplace=True)
    daily_kpi_df.rename(columns={'index':'datetime'}, inplace=True)
    
    minute_kpi_df = raw_data.reset_index()[minute_kpi_info_columns]
    
    raw_pi_df = raw_data.reset_index().drop(minute_kpi_info_columns, axis=1)
    
    
    return raw_pi_df.copy(), minute_kpi_df.copy(), daily_kpi_df.copy()
    



# =============================================================================
# ############## TEST CODES
# =============================================================================
#test = raw_data.loc[raw_data.index=='2019-09-04 10:20:00',['RPE.453FC1055:mv','RPE.453FC1047:mv','RPE.453FC1050:mv']]
#raw_data.loc[raw_data.index=='2019-06-18 23:50:00',coal_ydy_tags]
    
#test = raw_data.loc['2019-06-12 08:30:00':'2019-06-19 09:30:00',\
#                    ['RPE.281FQ-1509Cydy.pv','RPE.282FQ50512_YDY:value','RPE.282FQ50557_YDY:value','RPE.282FQ5053_YDY:value','RPE.283FQ1631H_2:VALUE','RPE.283FQ1631D_2:VALUE','RPE.283FT1669:VALUE']]

#daily_kpi_df.iloc[:-2][[kpi_name,kpi_name_p1,kpi_name_p2]].mean()







