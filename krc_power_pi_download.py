# =============================================================================
# Import libraries
# =============================================================================
import datetime
import os
import re
import sys

import pandas as pd

import db_connection
import krc_power_kpi_calc as kpi_calc
import logging_funcs as logging
import pi_sdk_connect

# =============================================================================
# =============================================================================
# # Configs
# =============================================================================
# =============================================================================
krc_power_pi_download_logfile_path = r'log\krc_power_download_log.txt'

db_pi_table = 'krc_power_pi'
db_kpi_minute_table = 'krc_minute_kpi_power'
db_kpi_daily_table = 'krc_daily_kpi_power'

# =============================================================================
# Tags Setup
# =============================================================================
#Table 1 - INPUT:
black_liquor_tags = ['RPL.411FC2206:me','RPL.411FC2010:me','RPL.411FC1558:me','RPL.411FC1551:me',
                     'RPL.412FC1570:mv','RPL.412FT3206:value','RPL.413FI1172:av',
                     'RPL.422FC5073:mv','RPL.422FC5147:mv']

coal_tags = ['RPE.282FI4051:value','RPE.282FI4053:value','RPE.283WI1503:VALUE','RPE.283WI1506:VALUE',
             'RPE.283WI1509:VALUE','RPE.283WI1512:VALUE']

coal_ydy_tags = ['RPE.282FQ4051_YDY:value','RPE.282FQ4053_YDY:value','RPE.283WQ1503_2:VALUE',
                 'RPE.283WQ1509_2:VALUE','RPE.283WQ1512_2:VALUE','RPE.283WQ1506_2:VALUE']

biomass_tags = ['RPE.281FI-1109.pv','RPE.281FT-3615_BMS:Value','RPE.281FT-3625_BMS:Value',
                'RPE.281FT-3635_BMS:Value','RPE.281FT-3645_BMS:Value','RPE.281FT-3655_BMS:Value',
                'RPE.281FT-3665_BMS:Value','RPE.281FI-1509B.pv','RPE.281FI-1509C.pv',
                'RPE.282FI4050B:value','RPE.283WI1498:VALUE','RPE.283WI1518:VALUE']

natural_gas_pb_tags = ['RPE.281FT-3615_BMS:Value','RPE.281FT-3625_BMS:Value','RPE.281FT-3635_BMS:Value',
                       'RPE.281FT-3645_BMS:Value','RPE.281FT-3655_BMS:Value',
                       'RPE.281FT-3665_BMS:Value','RPE.282FI8355:Value','RPE.282FI8365:Value',
                       'RPE.282FC8008:mv','RPE.282FC8028:mv']

natural_gas_vk_tags = ['RPL.485FQ1234_CALCULATE:Input','RPL.485FC1234:outP']

natural_gas_rk_tags = ['RPL.481FT1144:value','RPL.482FT1510:value','RPL.483FC1301:me']

raw_water_tags = ['RPE.271FT-1075:Value','RPE.271FI-1076.pv','RPE.272FI1075:Value','RPE.273FI1076:Value',
                  'RPE.272FT2057:Value','RPE.1281FT1110:Value','RPE.272FT2058:Value','RPL.326FT5112:value',
                  'RPE.273FT1072:Value']

marine_fuel_oil_tags = ['RPE.461-FI-1709:Value','RPE.461-FI-1714:Value','RPE.462FI1709:value','RPE.462FI1714:value',
                        'RPE.463FI1709:value','RPE.463FI1714:value','RPE.465FI4602:av','RPE.465FI3231:av',
                        'RPL.481FC1081:mv','RPL.482FC1310:mv','RPL.483FCQ1289:me']

diesel_oil_tags = ['RPE.461-FC-3062:MV','RPE.462FC3062:mv','RPE.463FC3062:mv','RPE.465FC6323:me',
                   'RPE.465FC6241:me','RPE.281FQ-1509Cydy.pv',
                   'RPE.282FQ50512_YDY:value','RPE.282FQ50557_YDY:value','RPE.282FQ5053_YDY:value',
                   'RPE.283FQ1631H_2:VALUE','RPE.283FQ1631D_2:VALUE','RPE.283FT1669:VALUE',
                   'RPE.461FQ3062:Q_Prev','RPE.462FQ3062_YDY:Value','RPE.463FQ3062_YDY:value',
                   'RPE.465FQ6323.F:Y_DAY','RPE.465FQ6241.F:Y_DAY']


#Table 2 - PROCESS:
wbl_feed_tags = ['RPE.451-FC-1003:MV','RPE.452FC1001:mv','RPE.453FC1001:mv','RPE.454FC1450:me',
                 'RPE.451WBL_TS:MV','RPE.452WBL_TS:value','RPE.453WBL_TS:value','RPE.454WBL_TS:av']

rb_liquor_firing_tags = ['RPE.461-FC-1414:MV','RPE.462BL_LIQ_TO_FURN:value',
                         'RPE.463BL_LIQ_TO_FURN:value','RPE.465FI2890:av',
                         'RPE.461-DI-1415B:Value','RPE.462DT1415B:Value',
                         'RPE.463DT1415B:Value','RPE.465DI2850_BMS:av']

rb_total_steam_tags = ['RPE.461-FI-1134:Value','RPE.462FI1134:value','RPE.463FI1134:value','RPE.465FI1878:av']

pb_total_steam_tags = ['RPE.281FI-1109.pv','RPE.282FC1152:mv','RPE.283FI1200:VALUE']

mpprv_tags = ['RPE.291FQ-4016YDY:Input','RPE.292FQ1023_YDY:value','RPE.293FQ1023_YDY:value']

lpprv_tags = ['RPE.291FQ-4017YDY:Input','RPE.291-FQ-3201-YDY:Input','RPE.292FQ1015_YDY:value','RPE.293FQ1015_YDY:value']

condensate_return_tags = ['RPE.462FI3336:value','RPE.462FI3339:value','RPE.281-FI-2036:Value','RPE.281-FI-2039:Value',
                          'RPE.271-FI-2101:Value','RPE.271-FI-2201:Value','RPE.271-FI-2301:Value','RPE.271-FI-2401:Value',
                          'RPE.292FI5961:value','RPE.294FI3056:av',
                          'RPE.281FI-1027.pv','RPE.281FI-1099.pv','RPE.462FI1027:value','RPE.282FI1053:value',
                          'RPE.283FI1003:VALUE','RPE.465FI1053:av']


evap_steam_economy_tags = ['RPE.451-FC-1003:MV', 'RPE.451-DC-1020:MV', '451XI002LP', '451XI014LP',
                           'RPE.451-FC-1010:MV', '451XI017LP', '451XI017LP', '451XI014LP',
                           'RPE.452FC1001:mv', 'RPE.452DT1002:Value', '452XI002LP', '452XI016LP', 'RPE.454FC1020:mv',
                           'RPE.453FC1001:mv', 'RPE.453DC1002:mv', '453XI002LP','453XI016LP','RPE.454FC1023:mv',
                           'RPE.454FC1450:me','RPE.454DI1443:av','RPE.454FC1190:me','451XI014LP','RPL.454XI0002LP',
                           'RPE.454DI1114:av','RPE.454FI3004:av','RPE.454FC1020:mv','452XI016LP','RPL.454XI0023LP',
                           'RPE.454FC1023:mv','453XI016LP','RPL.454XI0024LP',
                           'RPE.451-FC-1001:MV','RPE.451-FC-1011B:MV','RPE.452FC1055:mv','RPE.452FC1047:mv','RPE.452FC1050:mv',
                           'RPE.453FC1055:mv','RPE.453FC1047:mv','RPE.453FC1050:mv','RPE.454FC1100:me','RPE.454FC1160:me',
                           'RPE.454FI1785:av','RPE.454FC1160:me','RPE.454FC1029:mv','RPE.454FC1029:mv','RPE.454FI5101:Value',
                           'RPE.454FC1039:mv','RPE.454FC1039:mv','RPE.454FI5201:Value']


power_to_rpe_tags = ['RPE.291J1-AC1-20A-FLT:Input','RPE.292JT-AC12-06:Value',
                     'RPE.291JI-5102:Value','RPE.291JI-5113:Value','RPE.1J13.P:av',
                     'RPE.292JT-AC11-03:Value','RPE.291J1-AC1-8A-FLT:Input','RPE.292JT-AC11-09:Value',
                     'RPE.291J1-AC1-3A-FLT:Input','RPE.1J14.P:av','RPE.1J15.P:av','RPE.1J16.P:av',
                     'RPE.291JT-AC03-00:Value','RPE.291JT-AC03-05:Value','RPE.291J1-AC1-16A-FLT:Input',
                     'RPE.292JT-AC11-06:Value','RPE.292JT-AC11-16:Value',
                     'RPE.292JT-AC11-11:Value','RPE.291JT-AC03-04:Value','RPE.1J02.P:av','RPE.1J03.P:av',
                     'RPE.291JI-5106:Value','RPE.291JI-5107A:MV','RPE.292JT-AC11-12:Value',
                     'RPE.292JT-AC11-13:Value','RPE.292JT-AC11-14:Value','RPE.294JT-AC21.03:Value',
                     'RPE.291J1-AC1-9A-FLT:Input','RPE.292JT-AC11-01:Value','RPE.291JT-AC03-09:Value',
                     'RPE.292JT-AC11-07:Value','RPE.1J10.P:av','RPE.1J11.P:av','RPE.1J12.P:av',
                     'RPE.291JI-5119:Value','RPE.292JT-AC11-02:Value','RPE.292JT-AC11-04:Value']

mp_steam_to_rpe_tags = ['RPE.452FI1097:value','RPE.453FI1097:value','RPE.454FC1160:me','RPE.454FI1785:av',
                        'RPE.454FC1029:mv','RPE.454FI5101:Value','RPE.454FC1039:mv','RPE.454FI5201:Value',
                        'RPE.461-FI-1651:Value','RPE.462FI1651:value','RPE.463FI1651:value','RPE.465FI5201:av']

lp_steam_to_rpe_tags = ['RPE.451-FC-1001:MV', 'RPE.452TOTALSTM:value','RPE.453FI1047:value','RPE.453FI1050:value',
                        'RPE.453FI1053:value','RPE.453FI1055:value','RPE.454FC1100:me','RPE.454FI5101:Value',
                        'RPE.454FI5201:Value','RPE.454FI3001A:av','RPE.461-FI-1652:Value','RPE.462FI1652:value',
                        'RPE.463FI1652:value','RPE.465FI5211:av','RPE.281FI-1027.pv','RPE.281FI-1099.pv',
                        'RPE.462FI1027:value', 'RPE.282FI1053:value','RPE.283FI1003:VALUE','RPE.283FT1433:VALUE']

process_water_to_rpe_tags = ['RPE.461-FI-1900-Totalizer-Yday:Value', 'RPE.465FI5101:av',
                             'RPE.281FI-1900.pv', 'RPE.282FT9012:Value', 'RPE.283FT1232:VALUE',
                             'RPE.281FI-1901.pv','RPE.271FI1229:Value', 'RPE.272FI3140:Value',
                             'RPE.452FT2301:Value', 'RPE.294FI4077:av', 'RPE.271FC-1001:MV',
                             'RPE.271FC-1002:MV','RPE.271FC-1003:MV','RPE.271FC-1004:MV',
                             'RPE.272FC1001:mv','RPE.272FC1002:mv','RPE.272FC1003:mv',
                             'RPE.272FC1004:mv','RPE.272FC3001:mv','RPE.272FC3071:MV',
                             'RPE.274FT-1006:Input','RPE.274FT-1007:Input','RPL.132FT1160:value',
                             'RPL.131FT1331:value','RPL.471FT1900:value','RPL.483FI1703:value',
                             'RPE.281FI-2100:MV','RPL.132FT1281:value']

#Table 3 - OUTPUT:
total_power_tags = ['RPE.TOTAL_POWER:value']
total_mp_steam_tags = ['RPE.291FI-2002:MV','RPE.291FI-3002:MV','RPE.292FI4960:value',
                       'RPE.293FI6771:value','RPE.292FT5003:Value','RPE.294FI3021:av',
                       'RPE.291FI-4024:MV','RPE.292FI1023:value','RPE.293FI1023:value']

total_lp_steam_tags = ['RPE.291-FI-2003:MV','RPE.291-FI-3003:MV','RPE.292FI4100:value',
                       'RPE.293FI6702:value','RPE.292FI5961:value','RPE.282FI9010:value',
                       'RPE.282FI9011:value','RPE.292FI6000:value','RPE.292FI5100:value','RPE.294FI3060:av',
                       'RPE.291FI-4017:MV','RPE.291FI-3201:MV','RPE.292FI1015:value','RPE.293FI1015:value']

total_demin_water_tags = ['RPE.271FI-1031flt:Value','RPE.271FI-1032flt:Value','RPE.271FI-1033flt:Value',
                          'RPE.272FC1031:mv','RPE.272FC1032:mv','RPE.272FC1033:mv','RPE.272FQ3040_YDY:value',
                          'RPE.272FT3109:Value']

total_soft_water_tags = ['RPE.1271FI1510:Value','RPE.274FT-1007:Input']

total_wl_tags = ['RPL.411FC2014:me', 'RPL.411FI2072:av', 'RPL.411FI2219:av', 'RPL.411LC1550:me',
                 'RPL.412FC1627:mv','RPL.422FC2307:mv',
                 'RPL.413FC1006:me','RPL.413FC1007:me','RPL.413FC1009:me','RPL.413FC1012:me','RPL.423FI1801:av',
                 'RPL.412FC5015:mv','RPL.412FC5051:mv','RPL.623FC1186:me',
                 'RPL.411LC1550:me','RPL.412LT1608:value','RPL.472LT4208:value','RPL.472LT4211:value']


#Concatenate all tags
power_pi_tags = []
power_pi_tags += black_liquor_tags
power_pi_tags += coal_tags
power_pi_tags += coal_ydy_tags
power_pi_tags += biomass_tags 
power_pi_tags += natural_gas_pb_tags 
power_pi_tags += natural_gas_vk_tags
power_pi_tags += natural_gas_rk_tags
power_pi_tags += raw_water_tags
power_pi_tags += marine_fuel_oil_tags
power_pi_tags += diesel_oil_tags

power_pi_tags += wbl_feed_tags
power_pi_tags += rb_liquor_firing_tags
power_pi_tags += rb_total_steam_tags
power_pi_tags += pb_total_steam_tags
power_pi_tags += mpprv_tags
power_pi_tags += lpprv_tags
power_pi_tags += condensate_return_tags
power_pi_tags += evap_steam_economy_tags
power_pi_tags += power_to_rpe_tags
power_pi_tags += mp_steam_to_rpe_tags
power_pi_tags += lp_steam_to_rpe_tags
power_pi_tags += process_water_to_rpe_tags
power_pi_tags += total_power_tags
power_pi_tags += total_mp_steam_tags
power_pi_tags += total_lp_steam_tags
power_pi_tags += total_demin_water_tags
power_pi_tags += total_soft_water_tags
power_pi_tags += total_wl_tags

tag_list = list(set(power_pi_tags))
print('{} tags to download!'.format(len(tag_list)))
sampling_freq = "10m"
working_shift_hour=7
working_shift_duration = '24h'
# =============================================================================
# =============================================================================
# # Functions
# =============================================================================
# =============================================================================

# =============================================================================
# lowercase
# =============================================================================
#wbl_received_tags = [tag_name.lower() for tag_name in wbl_received_tags]

#### To delete all rows after a certain date:
#db_con.delete_entries(table_name=db_pi_table, date='2019-04-05',single_deletion=False)


# =============================================================================
# =============================================================================
# # # Database connection
# # #db_kpi_table is hardcoded in "upsert" function in db_connection script too
# =============================================================================
# =============================================================================
try:
    db_con = db_connection.DB_connection()
except:
    error_message = "DB Connection error received from db_connection module"
    print(error_message)
    logging.log_message(krc_power_pi_download_logfile_path, error_message)
    sys.exit(1)
        


# =============================================================================
# =============================================================================
# # Download Routine
# =============================================================================
# =============================================================================
try:
    try:
        # Create Tables if not exists
        #db_con.create_krc_power_pi_table(table_name = db_pi_table)
        db_con.create_krc_kpi_power_minute_table(table_name = db_kpi_minute_table)
        db_con.create_krc_kpi_power_daily_table(table_name = db_kpi_daily_table)
    except:
        error_message = "Table creation/check existance error"
        print(error_message)
        logging.log_message(krc_power_pi_download_logfile_path, error_message)
        sys.exit(1)
      

    last_entry = db_con.read_last_entry(table_name = db_kpi_daily_table,with_valid_data = False, table_data_type='raw', location='krc')
    if last_entry is None: #empty table
        #Trigger first batch download
        print("Downloading first batch (first day of current year onwards)")
        start_time = datetime.datetime.now().strftime('%Y-01-01 07:00:00') #%m
        #start_time = (datetime.datetime.now()-timedelta(30)).strftime('%Y-%m-%d 00:00:00')
        #start_time = "2019-01-01 00:00:00"
        datetime_duplicate_check=False
        
    else:
        last_datetime = last_entry[0] #Last date with any column not None
        print('Last valid entry at: {}'.format(last_datetime))
        print('Downloading {} day(s) of data'.format((datetime.datetime.now() - last_datetime).days))
        start_time = last_datetime.strftime('%Y-%m-%d %H:%M:%S')
        datetime_duplicate_check=True
    

    try:
        piServer = pi_sdk_connect.connect_to_server(server_name='RAPP1')
        downloaded_data = pi_sdk_connect.pi_interpolated_value(piServer,
                                                           pi_tag_list = tag_list,
                                                           start_time = start_time, #'2019-09-02 07:00:00',
                                                           end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), #'2019-09-04 11:00:00',
                                                           freq = sampling_freq,
                                                           server_timebased = True,
                                                           convert_invalid_to_nan = True,
                                                           datetime_index = True,
                                                           convert_col_to_lower = False)
    
    except:
        error_message = "PI download error received from pi_sdk_connect module"
        print(error_message)
        logging.log_message(krc_power_pi_download_logfile_path, error_message)
        sys.exit(1)
    
    minute_kpi_info_columns = ['datetime','black_liquor','coal','biomass',
                               'natural_gas_pb','natural_gas_vk','natural_gas_rk',
                               'raw_water','marine_fuel_oil','mfo_burner_oil_rb_1235','mfo_oil_kiln_123', 'diesel_oil',
                               'wbl_feed_flow','wbl_feed_solid','tds_wbl_feed','total_wbl_feed',
                               'liquor_firing_rb1235', 'solid_rb1235', 'total_dry_solid_rb1235','rb_liquor_firing',
                               'rb_total_steam','pb_total_steam','mp_prv','lp_prv','condensate_return',
                               'rb_steam_ratio','evap_steam_economy', 'evap_water_evaporation','evap_steam',
                               'power_to_rpe','mp_steam_to_rpe', 'lp_steam_to_rpe', 'process_water_to_rpe',
                               'total_power','total_hp_steam','total_mp_steam','total_lp_steam', 
                               'total_process_water','total_demin_water', 'total_soft_water','total_wl']
    
    daily_kpi_info_columns = minute_kpi_info_columns
    
    #columns here should be matched with columns defined in create_krc_kpi_power_table in db_connection script     
    minute_kpi_df = pd.DataFrame(columns=minute_kpi_info_columns)
    daily_kpi_df = pd.DataFrame(columns=daily_kpi_info_columns)

    #KPI Calculations:
    _,minute_kpi_df, daily_kpi_df = kpi_calc.power_kpi_calcs(raw_data=downloaded_data.fillna(method='ffill').fillna(0).copy(),
                                                           minute_kpi_info_columns=minute_kpi_info_columns,daily_kpi_info_columns=minute_kpi_info_columns,
                                                           working_shift_hour=working_shift_hour,working_shift_duration = working_shift_duration,
                                                           black_liquor_tags = black_liquor_tags,coal_tags = coal_tags, coal_ydy_tags = coal_ydy_tags,
                                                           biomass_tags = biomass_tags,natural_gas_pb_tags = natural_gas_pb_tags,
                                                           natural_gas_vk_tags = natural_gas_vk_tags,natural_gas_rk_tags = natural_gas_rk_tags,
                                                           raw_water_tags = raw_water_tags,marine_fuel_oil_tags = marine_fuel_oil_tags,diesel_oil_tags = diesel_oil_tags,
                                                           wbl_feed_tags = wbl_feed_tags, rb_liquor_firing_tags=rb_liquor_firing_tags,
                                                           rb_total_steam_tags = rb_total_steam_tags, pb_total_steam_tags = pb_total_steam_tags,
                                                           mpprv_tags = mpprv_tags, lpprv_tags = lpprv_tags, condensate_return_tags = condensate_return_tags,
                                                           evap_steam_economy_tags = evap_steam_economy_tags, 
                                                           power_to_rpe_tags = power_to_rpe_tags, mp_steam_to_rpe_tags = mp_steam_to_rpe_tags,
                                                           lp_steam_to_rpe_tags = lp_steam_to_rpe_tags, process_water_to_rpe_tags = process_water_to_rpe_tags,
                                                           total_power_tags = total_power_tags, total_mp_steam_tags = total_mp_steam_tags,
                                                           total_lp_steam_tags = total_lp_steam_tags, total_demin_water_tags=total_demin_water_tags,
                                                           total_soft_water_tags = total_soft_water_tags, total_wl_tags = total_wl_tags)
    

    contains_nan = downloaded_data.isnull().values.any()


    #Modify the column names to make it compatible with SQL standards:
    for tag_name in tag_list:        
        downloaded_data.rename(columns={tag_name.lower():re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format(tag_name.lower()))},inplace=True)
    


    # Bulk insert into database
    try:
        #db_con.insert_df_to_db(dataframe=downloaded_data, table_name=db_pi_table,datetime_primary=datetime_duplicate_check, convert_nan_to_null=contains_nan)
        db_con.insert_df_to_db(dataframe=minute_kpi_df, table_name=db_kpi_minute_table,datetime_primary=True, convert_nan_to_null=contains_nan)
        db_con.insert_df_to_db(dataframe=daily_kpi_df.fillna('ffill'), table_name=db_kpi_daily_table,datetime_primary=True, convert_nan_to_null=contains_nan)

    except:
        error_message = "Database insertion error received from db_connection module"
        print(error_message)
        logging.log_message(krc_power_pi_download_logfile_path, error_message)
        sys.exit(1)
    
        
    
        
except:
    error_message = "{} execution error".format(os.path.basename(__file__))
    print(error_message)
    logging.log_message(krc_power_pi_download_logfile_path, error_message)
    sys.exit(1)
        


        