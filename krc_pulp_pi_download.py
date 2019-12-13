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
import krc_pulp_kpi_calc as kpi_calc
import logging_funcs as logging
# =============================================================================
# Import libraries
# =============================================================================
import pi_sdk_connect

# =============================================================================
# =============================================================================
# # Configs
# =============================================================================
# =============================================================================
krc_pulp_pi_download_logfile_path = r'log\krc_pulp_download_log.txt'

db_pi_table = 'krc_pulp_pi'
db_daily_kpi_table = 'daily_kpi_pulp'#hardcoded in "upsert" function in db_connection script

# =============================================================================
# Set tags to download
# tags are hardcoded in "create_krc_pulp_pi_table" function in db_connection script too
# =============================================================================
production_tags = ["RPL.111XC0003:afc","RPL.111XC0002:afc","RPL.433FQ1260.F:Y_DAY", #FL1,2,3
                   "RPL.422YI5139-YD:value","RPL.111XC0221:afc" , #@HemaEntries
                   "RPL.111XC0005:afc","RPL.111XC0006:afc","RPL.111XC0007:afc","RPL.111XC0008:afc"]
# "RPL.111XC0221:afc" , #@HemaEntries
clo2_consumption_tags = ["RPL.111XC0017:afc","RPL.111XC0023:afc", #FL1
                         "RPL.111XC0018:afc","RPL.111XC0024:afc", #FL2
                         "RPL.111XC0019:afc","RPL.111XC0025:afc",
                         "RPL.111XC0251:afc"] #PL15

defoamer_tags = ['RPL.111XC0170:afc','RPL.111XC0171:afc','RPL.111XC0172:afc','RPL.111XC0173:afc','RPL.111XC0202:afc']

h2o2_consumption_tags = ["RPL.111XC0020:afc","RPL.111XC0021:afc", "RPL.111XC0022:afc","RPL.111XC0194:afc"] #FL1,2,3

naoh_own_tags = ["RPL.111XC0032:afc","RPL.111XC0033:afc", "RPL.111XC0034:afc"] #FL1,2,3
naoh_purchase_tags = ["RPL.111XC0035:afc","RPL.111XC0036:afc", "RPL.111XC0037:afc"] #FL1,2,3
naoh_total_tags = ["RPL.111XC0038:afc","RPL.111XC0039:afc", "RPL.111XC0040:afc","RPL.111XC0197:afc"] #FL1,2,3

oxygen_tags = ["RPL.111XC0026:afc","RPL.111XC0027:afc", "RPL.111XC0028:afc","RPL.111XC0198:afc"] #FL1,2,3

sulfuric_acid_h2so4_tags = ["RPL.111XC0029:afc","RPL.111XC0030:afc", "RPL.111XC0031:afc"] #FL1,2,3 , ,"RPL.111XC0200:afc"

white_liqour_consumption_m3_adt_tags = ['RPL.111XC0009:afc','RPL.111XC0010:afc','RPL.111XC0011:afc','RPL.111XC0012:afc','RPL.111XC0201:afc'] #FL1,2,3, PCD
white_liqour_consumption_taa_adt_tags = ['RPL.111XC0013:afc','RPL.111XC0014:afc','RPL.111XC0015:afc','RPL.111XC0016:afc'] #FL1,2,3, PCD


utility_power_consumption = ['RPL.111XC0050:afc', 'RPL.111XC0051:afc', 'RPL.111XC0052:afc', 'RPL.111XC0053:afc',
                             'RPL.111XC0054:afc', 'RPL.111XC0055:afc', 'RPL.111XC0056:afc', 'RPL.111XC0057:afc']

lp_steam_consumption = ['RPL.111XC0060:afc', 'RPL.111XC0061:afc', 'RPL.111XC0062:afc', 'RPL.111XC0063:afc',
                        'RPL.111XC0064:afc', 'RPL.111XC0065:afc', 'RPL.111XC0066:afc', 'RPL.111XC0067:afc']

mp_steam_consumption = ['RPL.111XC0068:afc', 'RPL.111XC0069:afc', 'RPL.111XC0070:afc', 'RPL.111XC0071:afc',
                        'RPL.111XC0072:afc', 'RPL.111XC0073:afc', 'RPL.111XC0074:afc', 'RPL.111XC0075:afc']

total_steam_consumption = ['RPL.111XC0076:afc', 'RPL.111XC0077:afc', 'RPL.111XC0078:afc', 'RPL.111XC0079:afc',
                           'RPL.111XC0080:afc', 'RPL.111XC0081:afc', 'RPL.111XC0082:afc', 'RPL.111XC0083:afc']

utility_water_consumption = ['RPL.111XC0084:afc', 'RPL.111XC0085:afc', 'RPL.111XC0086:afc', 'RPL.111XC0087:afc',
                             'RPL.111XC0088:afc', 'RPL.111XC0089:afc', 'RPL.111XC0090:afc', 'RPL.111XC0091:afc']


# old_wc_woodchip_total = ["RPL.411WQ7000.F:Y_DAY","RPL.341WQ5080YD:value", "RPL.341WI2794-YD:value"] #FL1,2, PCD
wc_woodchip_total =['RPL.111XC0220:afc']
wc_chip_meter_avg = ["RPL.111XC0048:afc"] #FL3
# old_wc_consumption = ["RPL.111XC0106:afc","RPL.111XC0107:afc", "RPL.111XC0108:afc","RPL.111XC0109:afc","RPL.111XC0219:afc"] #FL1,2,3, PCD
wc_consumption=['RPL.111XC0211:afc','RPL.111XC0213:afc','RPL.111XC0215:afc','RPL.111XC0217:afc']

# old_yield_bleaching = ["RPL.111XC0126:afc","RPL.111XC0127:afc", "RPL.111XC0128:afc"] #FL1,2,3
# old_yield_cooking = ["RPL.111XC0118:afc","RPL.111XC0119:afc", "RPL.111XC0120:afc", "RPL.111XC0121:afc"] #FL1,2,3, PCD
yield_cooking=['RPL.111XC0130:afc','RPL.111XC0131:afc','RPL.111XC0132:afc','RPL.111XC0133:afc']
# old_yield_screening = ["RPL.111XC0122:afc","RPL.111XC0123:afc", "RPL.111XC0124:afc", "RPL.111XC0125:afc"] #FL1,2,3, PCD



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
pulp_pi_tags += white_liqour_consumption_taa_adt_tags

pulp_pi_tags += utility_power_consumption
pulp_pi_tags += lp_steam_consumption
pulp_pi_tags += mp_steam_consumption
pulp_pi_tags += total_steam_consumption
pulp_pi_tags += utility_water_consumption

pulp_pi_tags += wc_woodchip_total
pulp_pi_tags += wc_chip_meter_avg
pulp_pi_tags += wc_consumption

# pulp_pi_tags += yield_bleaching
pulp_pi_tags += yield_cooking
# pulp_pi_tags += yield_screening

tag_list = list(set(pulp_pi_tags))
logging.log_message(krc_pulp_pi_download_logfile_path,tag_list)
total_Tag_list='{} tags to download!'.format(len(tag_list))

#HemaEntries
print (total_Tag_list)
# logging.log_message(krc_pulp_pi_download_logfile_path,total_Tag_list )
#print('{} tags to download!'.format(len(tag_list)))

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
    logging.log_message(krc_pulp_pi_download_logfile_path, error_message)
    sys.exit(1)
        




# =============================================================================
# =============================================================================
# # Download Routine
# =============================================================================
# =============================================================================
try:
    try:
        # Create Tables if not exists
        db_con.create_krc_pulp_pi_table(table_name = db_pi_table)
        db_con.create_daily_kpi_pulp_table(table_name = db_daily_kpi_table)
        print("DB Connection has been create successfully")
    except:
        error_message = "Table creation/check existance error"
        print(error_message)
        logging.log_message(krc_pulp_pi_download_logfile_path, error_message)
        sys.exit(1)
        
    
    last_entry = db_con.read_last_entry(table_name = db_daily_kpi_table,with_valid_data = False, table_data_type='kpi', location='krc')
    if last_entry is None: #empty table
        #Trigger first batch download
        print("Downloading first batch (2019 onwards)")
        start_time = "2019-01-02 07:30:00"
        #start_time = "2019-05-14 07:30:00"
        datetime_duplicate_check=False
    else:
        last_datetime = last_entry[0] #Last date with any column not None
        print('Last valid entry at: {}'.format(last_datetime))
        last_datetime = last_datetime - timedelta(13) #re-download the last 2 weeks of data, disable this line after PI system upgrade
        print('Downloading  {} day(s) of data'.format((datetime.datetime.now() - last_datetime).days))
        start_time = last_datetime.strftime('%Y-%m-%d %H:%M:%S')
        datetime_duplicate_check=True
    
    try:
        piServer = pi_sdk_connect.connect_to_server(server_name='RAPP1')
        downloaded_data = pi_sdk_connect.pi_interpolated_value(piServer,
                                                               pi_tag_list = tag_list,
                                                               start_time = start_time,
                                                               end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                               freq = sampling_freq,
                                                               server_timebased = True,
                                                               convert_invalid_to_nan = True,
                                                               datetime_index = False,
                                                               convert_col_to_lower = True)
        print("PI Download able to connect")
    except:
        error_message = "PI download error received from pi_sdk_connect module"
        print(error_message)
        logging.log_message(krc_pulp_pi_download_logfile_path, error_message)
        sys.exit(1)
        
    
    #kpi_info_columns = ['datetime','country','mill','bu','process_line','kpi_category','kpi_name','kpi_unit','process_type','kpi_type','kpi_sub_type','kpi_sub_sub_type','kpi_value','tag_code']
    kpi_info_columns = ['datetime','country_id','mill_id','bu_id','bu_type_id','kpi_category_id','kpi_id',
                        'process_line_1','process_line_2','process_line_3','process_line_4','process_line_5',
                        'process_line_6','process_line_7','process_line_8','process_line_9','process_line_10',
                        'process_line_11','process_line_12','process_line_13','process_line_14','process_line_15']
    
    #columns here should be matched with columns defined in create_daily_kpi_pulp_table & upsert functions 
    #in db_connection script 
    
    kpi_df = pd.DataFrame(columns=kpi_info_columns)
    print(kpi_df)
    
    #KPI Calculations:
    kraft_kpi_df = kpi_calc.kraft_kpi_calcs(raw_data=downloaded_data.fillna(method='ffill').copy(), kpi_info_columns=kpi_info_columns)
    dp_kpi_df = kpi_calc.dp_kpi_calcs(raw_data=downloaded_data.fillna(method='ffill').copy(), kpi_info_columns=kpi_info_columns)
    #dp_kpi_df is empty, asits in use as of today(15/7)
    print(kraft_kpi_df)
    print(dp_kpi_df)

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
        logging.log_message(krc_pulp_pi_download_logfile_path, error_message)
        sys.exit(1)
    
        
except:
    error_message = "{} execution error".format(os.path.basename(__file__))
    print(error_message)
    logging.log_message(krc_pulp_pi_download_logfile_path, error_message)
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



        