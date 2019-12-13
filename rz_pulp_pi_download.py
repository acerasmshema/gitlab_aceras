# =============================================================================
# Things to do for adding a new PI tag:
# 1 - Add the PI tags code to tag_list
# 2 - (Dropped) Map the PI tag code to its proper name in the interpret_column_names function in this script
# 3 - Define the column in the create_<table> function in db_connection script
# 4 - Proceed with one of these two:
#     A) Drop the table from database and let the script create it again upon next run
#     B) Alter the table in the database through sql command: 
#         ALTER TABLE table_name ADD column_name column type
#   Option-A will redownload all the data
# 5 - (Not in use anymore) In insert_df_to_db function in db_connection script, add the column in order to convert nan to null
#
# Side note (haven`t implemented) : Check the column names and add if not exists:
# IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS
# WHERE TABLE_NAME = 'table_name' AND COLUMN_NAME = 'column_name')
# BEGIN
#    ALTER TABLE TEST ADD TEST_DATE DATETIME
# END
# =============================================================================


import datetime
import os
import re
import sys
from datetime import timedelta

import pandas as pd

import db_connection
import logging_funcs as logging
# =============================================================================
# Import libraries
# =============================================================================
import pi_sdk_connect
import rz_pulp_kpi_calc as kpi_calc

# =============================================================================
# =============================================================================
# # Configs
# =============================================================================
# =============================================================================
rz_pulp_pi_download_logfile_path = r'log\rz_pulp_download_log.txt'

db_pi_table = 'rz_pulp_pi'
db_daily_kpi_table = 'daily_kpi_pulp'#hardcoded in "upsert" function in db_connection script

# =============================================================================
# Set tags to download
# tags are hardcoded in "create_rz_pulp_pi_table" function in db_connection script too
# =============================================================================
production_tags = ["OE.PD1.PULP.PRODUCTION","OE.PD2.PULP.PRODUCTION","OE.PD3.PULP.PRODUCTION", #PD1,2,3
                   "OE.PL11.PULP.PRODUCTION","OE.PL12.PULP.PRODUCTION"] #PL11, PL12

clo2_consumption_tags = ["OE.PL11.CHE.CLO2","OE.PL12.CHE.CLO2"] #PL11, PL12

defoamer_tags = ['OE.PL11.Defoamer','OE.PL12.Defoamer'] #PL11, PL12

h2o2_consumption_tags = ["OE.PL11.CHE.H202","OE.PL12.CHE.H202"] #PL11, PL12

naoh_own_tags = ["OE.PL12.CHE.NaOH.OWN"] #PL12
naoh_purchase_tags = ["OE.PL11.CHE.NaOH.PUR","OE.PL12.CHE.NaOH.PUR"] 
naoh_total_tags = ["OE.PL11.CHE.NaOH.TOT","OE.PL12.CHE.NaOH.TOT"]

oxygen_tags = ["OE.PL11.CHE.O2","OE.PL12.CHE.O2"] #PL11, PL12

sulfuric_acid_h2so4_tags = ["OE.PL11.CHE.H2SO4","OE.PL12.CHE.H2SO4"] #PL11, PL12

white_liqour_consumption_m3_adt_tags = ['OE.PL11.CHE.WL','OE.PL12.CHE.WL'] #PL11, PL12

utility_power_consumption = ['OE.PD1.UT.POWER.TOT','OE.PD2.UT.POWER.TOT','OE.PD3.UT.POWER.TOT', #PD1,2,3
                             'OE.PL11.UT.PR.POWER','OE.PL12.UT.PR.POWER'] #PL11, PL12
        
lp_steam_consumption = ['OE.PD1.UT.LP.STEAM', 'OE.PD2.UT.LP.STEAM', 'OE.PD3.UT.LP.STEAM', #PD1,2,3
                        'OE.PL11.UT.LP.STEAM', 'OE.PL12.UT.LP.STEAM'] #PL11,12

mp_steam_consumption = ['OE.PL11.UT.MP.STEAM', 'OE.PL12.UT.MP.STEAM']

total_steam_consumption = ['OE.PD1.UT.TOT.STEAM', 'OE.PD2.UT.TOT.STEAM', 'OE.PD3.UT.TOT.STEAM',
                           'OE.PL11.UT.TOT.STEAM', 'OE.PL12.UT.TOT.STEAM']

utility_water_consumption = ['OE.PD1.UT.PR.WATER', 'OE.PD2.UT.PR.WATER', 'OE.PD3.UT.PR.WATER', #PD1,2,3
                             'OE.PL11.UT.PR.WATER', 'OE.PL12.UT.PR.WATER'] #PL11, PL12

wc_consumption = ["OE.PL11.CHIPS.BDT","OE.PL12.CHIPS.BDT"] #PL11, PL12


yield_bleaching = ["OE.PL11.CHIPS.BLC.YIELD","OE.PL12.CHIPS.BLC.YIELD"] #PL11, PL12
yield_cooking = ["OE.PL11.CHIPS.DIG.YIELD","OE.PL12.CHIPS.DIG.YIELD"] #PL11, PL12 digester yield
yield_screening = ["OE.PL11.CHIPS.SCR.YIELD","OE.PL12.CHIPS.SCR.YIELD"] #PL11, PL12



pulp_pi_tags = []
pulp_pi_tags += production_tags

pulp_pi_tags += clo2_consumption_tags
pulp_pi_tags += defoamer_tags
pulp_pi_tags += h2o2_consumption_tags
pulp_pi_tags += naoh_own_tags
pulp_pi_tags += naoh_purchase_tags
pulp_pi_tags += naoh_total_tags
pulp_pi_tags += oxygen_tags
pulp_pi_tags += sulfuric_acid_h2so4_tags
pulp_pi_tags += white_liqour_consumption_m3_adt_tags


pulp_pi_tags += utility_power_consumption
pulp_pi_tags += lp_steam_consumption
pulp_pi_tags += mp_steam_consumption
pulp_pi_tags += total_steam_consumption
pulp_pi_tags += utility_water_consumption

pulp_pi_tags += wc_consumption

pulp_pi_tags += yield_bleaching
pulp_pi_tags += yield_cooking
pulp_pi_tags += yield_screening

tag_list = list(set(pulp_pi_tags))
print('{} tags to download!'.format(len(tag_list)))

sampling_freq = "24h"


# =============================================================================
# =============================================================================
# # Functions
# =============================================================================
# =============================================================================

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
    logging.log_message(rz_pulp_pi_download_logfile_path, error_message)
    sys.exit(1)
        




# =============================================================================
# =============================================================================
# # Download Routine
# =============================================================================
# =============================================================================
try:
    try:
        # Create Tables if not exists
        db_con.create_rz_pulp_pi_table(table_name = db_pi_table)
        db_con.create_daily_kpi_pulp_table(table_name = db_daily_kpi_table)
    except:
        error_message = "Table creation/check existance error"
        print(error_message)
        logging.log_message(rz_pulp_pi_download_logfile_path, error_message)
        sys.exit(1)
        
    
    last_entry = db_con.read_last_entry(table_name = db_daily_kpi_table,with_valid_data = False, table_data_type='kpi', location='rz')
    if last_entry is None: #empty table
        #Trigger first batch download
        print("Downloading first batch (2019 onwards)")
        start_time = "2019-01-02 09:30:00"
        #start_time = "2019-05-14 09:30:00"
        datetime_duplicate_check=False
    else:
        last_datetime = last_entry[0] #Last date with any column not None
        print('Last valid entry at: {}'.format(last_datetime))
        last_datetime = last_datetime - timedelta(13) #re-download the last 2 weeks of data, disable this line after PI system upgrade
        print('Downloading {} day(s) of data'.format((datetime.datetime.now() - last_datetime).days))
        start_time = last_datetime.strftime('%Y-%m-%d %H:%M:%S')
        datetime_duplicate_check=True
    
    try:
        piServer = pi_sdk_connect.connect_to_server(server_name='172.27.60.24')
        downloaded_data = pi_sdk_connect.pi_interpolated_value(piServer,
                                                               pi_tag_list = tag_list,
                                                               start_time = start_time,
                                                               end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                               freq = sampling_freq,
                                                               server_timebased = True,
                                                               convert_invalid_to_nan = True,
                                                               datetime_index = False,
                                                               convert_col_to_lower = True)
        
    except:
        error_message = "PI download error received from pi_sdk_connect module"
        print(error_message)
        logging.log_message(rz_pulp_pi_download_logfile_path, error_message)
        sys.exit(1)
        
    
    #kpi_info_columns = ['datetime','country','mill','bu','process_line','kpi_category','kpi_name','kpi_unit','process_type','kpi_type','kpi_sub_type','kpi_sub_sub_type','kpi_value','tag_code']
    kpi_info_columns = ['datetime','country_id','mill_id','bu_id','bu_type_id','kpi_category_id','kpi_id',
                        'process_line_1','process_line_2','process_line_3','process_line_4','process_line_5',
                        'process_line_6','process_line_7','process_line_8','process_line_9','process_line_10',
                        'process_line_11','process_line_12','process_line_13','process_line_14','process_line_15']
    
    #columns here should be matched with columns defined in create_daily_kpi_pulp_table & upsert functions 
    #in db_connection script 
    
    kpi_df = pd.DataFrame(columns=kpi_info_columns)
    
    #KPI Calculations:
    kraft_kpi_df = kpi_calc.kraft_kpi_calcs(raw_data=downloaded_data.fillna(method='ffill').copy(), kpi_info_columns=kpi_info_columns)
    dp_kpi_df = kpi_calc.dp_kpi_calcs(raw_data=downloaded_data.fillna(method='ffill').copy(), kpi_info_columns=kpi_info_columns)

    
    #Concatenate the results:
    kpi_df = kpi_df.append(kraft_kpi_df)
    kpi_df = kpi_df.append(dp_kpi_df)
   

    contains_nan = downloaded_data.isnull().values.any()
    
    
    #Modify the column names to make it compatible with SQL standards:
    for tag_name in tag_list:        
        downloaded_data.rename(columns={tag_name.lower():re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format(tag_name.lower()))},inplace=True)
    

        
    # Bulk insert into database
    try:
        #db_con.insert_df_to_db(dataframe=downloaded_data, table_name=db_pi_table,datetime_primary=datetime_duplicate_check, convert_nan_to_null=contains_nan)
        db_con.insert_df_to_db(dataframe=kpi_df, table_name=db_daily_kpi_table,datetime_primary=True, convert_nan_to_null=contains_nan)
    except:
        error_message = "Database insertion error received from db_connection module"
        print(error_message)
        logging.log_message(rz_pulp_pi_download_logfile_path, error_message)
        sys.exit(1)
    
        
except:
    error_message = "{} execution error".format(os.path.basename(__file__))
    print(error_message)
    logging.log_message(rz_pulp_pi_download_logfile_path, error_message)
    sys.exit(1)
    


# =============================================================================
# Serialize
# =============================================================================
#with open('downloaded_data','wb') as fp:
#    pickle.dump(downloaded_data,fp)
#
##Load it back
#with open('downloaded_data', 'rb') as fp:
#    downloaded_data = pickle.load(fp)



# =============================================================================
# =============================================================================
# # Test code
# =============================================================================
# =============================================================================
#db_con.create_pi_data_table(table_name = db_pi_table)
#
#downloaded_data = pi_sdk_connect.pi_interpolated_value(pi_sdk_connect.piServer,
#                                                               pi_tag_list = tag_list,
#                                                               start_time = '2019-03-11 00:00:00',
#                                                               end_time = '2019-03-11 10:30:00',
#                                                               freq = "5m",
#                                                               server_timebased = True,
#                                                               convert_invalid_to_nan = True,
#                                                               datetime_index = False,
#                                                               convert_col_to_lower = True)
#        
        
#downloaded_data = interpret_column_names(dataset = downloaded_data,tag_list = tag_list)

#for col in downloaded_data.columns:
#    print(sum(downloaded_data[col].isnull()))
#    
#    
#downloaded_data.isnull().values.any()

## Bulk insert into database
#contains_nan = downloaded_data.isnull().values.any()
#db_con.insert_df_to_db(dataframe=downloaded_data, table_name=db_pi_table,datetime_primary=False, convert_nan_to_null=contains_nan)
#        
#        
#
#
#
#def upsert(row,**kwargs):
#
#    keys = ["%s" % k for k in row.index]
#    values = ["'%s'" % v for v in tuple(row)]
#    print([values])
#    sql = list()
#    sql.append('INSERT INTO "%s" (' % kwargs['table_name'])
#    sql.append(", ".join(keys))
#    sql.append(") VALUES (")
#    sql.append(", ".join(values))
#    sql.append(") ON CONFLICT (datetime) DO UPDATE SET ")
#    sql.append(", ".join("%s = '%s'" % (k, v) for k, v in row.iteritems()))
#    sql.append(";")
#    sql= "".join(sql)
#    print(sql)
#
#    
#downloaded_data.apply(upsert, table_name=db_pi_table, axis =1)
#
#db_con.insert_df_to_db(dataframe=downloaded_data, table_name=db_pi_table,datetime_primary=True)
#



        