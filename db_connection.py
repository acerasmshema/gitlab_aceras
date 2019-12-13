# =============================================================================
# Import libraries
# =============================================================================
import random
import re

import numpy as np
import psycopg2
from sqlalchemy import Column, Integer, Float, DateTime, BigInteger
from sqlalchemy import create_engine, MetaData, Table, UniqueConstraint
from sqlalchemy import desc, or_, and_
from sqlalchemy import insert, select, delete


# =============================================================================
# Create connection engine
# =============================================================================
class DB_connection(object):
    def __init__(self,u='opex_devs', p='opex_devs_ACERAS_pass',host='localhost',port='5432',db='opex'):
        try:
            self.engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(u,p,host,port,db))
            #To write np.nan as null in db, otherwise NaN would be treated as string
            def nan_to_null(f,
                    _NULL=psycopg2.extensions.AsIs('NULL'),
                    _NaN=np.NaN,
                    _Float=psycopg2.extensions.Float):
                if not np.isnan(f):
                    return _Float(f)
                return _NULL
            
            psycopg2.extensions.register_adapter(float, nan_to_null)
 
        except:
            print('Unable to create the engine')
            exit
        
    



    def upsert(self,row,**kwargs):
        '''
        A helper function for the insert_df_to_db func to do update if entry is already available
        Requires postgres 10+
        '''
        self.keys = ["%s" % k for k in row.index]
        self.values = ["'%s'" % v for v in tuple(row)]
        self.sql = list()
        self.sql.append('INSERT INTO "%s" (' % kwargs['table_name'])
        self.sql.append(", ".join(self.keys))
        self.sql.append(") VALUES (")
        self.sql.append(", ".join(self.values))
        
        if kwargs['table_name'] == 'daily_kpi_pulp':
            self.sql.append(") ON CONFLICT (datetime,country_id,mill_id,bu_id,bu_type_id,kpi_category_id,kpi_id) DO UPDATE SET ")
        else:
            self.sql.append(") ON CONFLICT (datetime) DO UPDATE SET ")
        
        
        self.sql.append(", ".join("%s = '%s'" % (k, v) for k, v in row.iteritems()))
        self.sql.append(";")
        self.sql= "".join(self.sql)
        print("UPSERT SQL: {}".format(self.sql))
        self.result_proxy = self.connection.execute(self.sql)
        print('{} row has been inserted/updated in {}'.format(self.result_proxy.rowcount, kwargs['table_name']))
        return 1
        
        

    def insert_df_to_db(self,dataframe, table_name, datetime_primary=False, convert_nan_to_null = False):
        '''
        Insert a dataframe to db
        '''
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.table = Table(table_name, self.metadata, autoload=True,autoload_with=self.engine)
        
        if datetime_primary == True: #Upsert
            dataframe.apply(self.upsert, table_name=table_name, axis =1)
            #if convert_nan_to_null == True: #Not in use anymore
            #    self.stmt = update(self.table)
            #    self.stmt = self.stmt.where(self.table.columns["pm2_energy_power"]=='nan')
            #    self.stmt = self.stmt.values(pm2_energy_power=np.nan)
            #    print("Convert nan to null sql: {}".format(self.stmt))
            #    self.result_proxy = self.connection.execute(self.stmt)
            
            #### instead of using upsert which is available on postgres10+ only, we can do delete,insert:
            #self.result = dataframe.apply(lambda x: self.delete_entries(table_name=table_name, date = x['datetime'], single_deletion=True), axis=1)
            #print('{} rows have been deleted'.format(self.result.sum()))
            #self.stmt = insert(self.table)
            #self.dataframe_list_of_dicts = dataframe.to_dict(orient='records')
            #self.result_proxy = self.connection.execute(self.stmt,self.dataframe_list_of_dicts)
            #print("{} rows have been inserted successfully".format(self.result_proxy.rowcount))
            #return (self.result_proxy.rowcount)
        
        else:
            self.stmt = insert(self.table)
            self.dataframe_list_of_dicts = dataframe.to_dict(orient='records')
            self.result_proxy = self.connection.execute(self.stmt,self.dataframe_list_of_dicts)
            print("{} rows have been inserted successfully".format(self.result_proxy.rowcount))
            return (self.result_proxy.rowcount)
        
        
    
    def delete_entries(self, table_name, date, single_deletion=False):
        '''
        Delete entries from DB:
        If single_deletion == True it will delete the entry on the particular date, otherwise
        it will delete all the entries from date onwards
        '''
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.table = Table(table_name, self.metadata, autoload=True,autoload_with=self.engine)
        if single_deletion == True:
            self.stmt = delete(self.table).where(self.table.columns.datetime == date)
        else:
            self.stmt = delete(self.table).where(self.table.columns.datetime >= date)
        #print('Delete stmt: {}'.format(self.stmt))
        self.result_proxy = (self.connection.execute(self.stmt))
        print("{} rows have been deleted successfully".format(self.result_proxy.rowcount))
        return (self.result_proxy.rowcount)
        
    
    def create_pi_data_table(self, table_name):
        self.metadata = MetaData()
        self.pi_data = Table(table_name, self.metadata,
                              Column('datetime',DateTime(),unique=True,nullable=False),
                              Column('tag_fl1_pre_o2_kappa',Float()),
                              Column('tag_fl2_pre_o2_kappa',Float()),
                              Column('tag_fl3_pre_o2_kappa',Float()),
                              Column('tag_fl1_post_o2_kappa',Float()),
                              Column('tag_fl2_post_o2_kappa',Float()),
                              Column('tag_fl3_post_o2_kappa',Float()))
        self.metadata.create_all(self.engine)
        
        
       
    def create_krc_paper_energy(self, table_name):
        '''
        Create table for paper energy KPIs
        '''
        self.metadata = MetaData()
        self.kpi_data = Table(table_name, self.metadata,
                              Column('datetime',DateTime(),unique=True,nullable=False),
                              Column('pm1_energy_water',Float()),
                              Column('pm1_energy_power',Float()),
                              Column('pm1_energy_lp_steam',Float()),
                              Column('pm1_energy_mp_steam',Float()),
                              
                              Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("502FQ2722_DAY:Q_PREV".lower())),Float()),
                              Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("502FQ2716_DAY:Q_PREV".lower())),Float()),
                              Column('pm2_energy_water',Float()),
                              Column('pm2_energy_power',Float()),
                              Column('pm2_energy_lp_steam',Float()))
        
        self.metadata.create_all(self.engine)
        
        
        
    def read_last_entry(self, table_name, with_valid_data=False,table_data_type='kpi', location='krc'):
        '''
        (if with_valid_data=True) Find the last valid entry in table by choosing 3 random column (except datetime)
        And find the latest timestamp that any of them has non-null value(if with_valid_data == True).
        
        location is mapped to the mill_id to find the latest entry of each particular mill
        '''
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.table = Table(table_name, self.metadata,autoload=True,autoload_with=self.engine)
        
        if with_valid_data == False:
            if table_data_type=='kpi' and location=='krc':
                self.stmt = select([self.table.columns.datetime]).where(self.table.columns['mill_id'] == 1)\
                                    .order_by(desc(self.table.columns.datetime))
            elif table_data_type=='kpi' and location=='rz':
                self.stmt = select([self.table.columns.datetime]).where(self.table.columns['mill_id'] == 3)\
                                    .order_by(desc(self.table.columns.datetime))
            else:
                self.stmt = select([self.table.columns.datetime]).order_by(desc(self.table.columns.datetime))
                
        else:
            self.random_selected_cols = random.sample(self.table.columns.keys()[1:], k = 3)
            self.stmt = select([self.table.columns.datetime]).where(or_(\
                              and_(self.table.columns[ self.random_selected_cols[0] ] != None, self.table.columns[ self.random_selected_cols[0] ] != 'nan'),
                              and_(self.table.columns[ self.random_selected_cols[1] ] != None, self.table.columns[ self.random_selected_cols[1] ] != 'nan'),
                              and_(self.table.columns[ self.random_selected_cols[2] ] != None, self.table.columns[ self.random_selected_cols[2] ] != 'nan'))).\
                              order_by(desc(self.table.columns.datetime))
        #print(self.stmt)

            
        self.results = self.connection.execute(self.stmt).first()
        return self.results
    
    
    def create_krc_pulp_pi_table(self,table_name):
        '''
        create table for pulp raw pi tags (Kerinci)
        '''
        self.metadata = MetaData()
        self.pi_data = Table(table_name, self.metadata,
                             Column('krc_pulp_pi_id',BigInteger(),primary_key=True),
                             Column('datetime',DateTime(),unique=True,nullable=False),
                             #Production:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0003:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0002:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.433FQ1260.F:Y_DAY".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.422YI5139-YD:value".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0005:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0006:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0007:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0008:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0221:afc".lower())),Float()), # Hema entries for processline15



                             #ClO2 Consumption & Total Active:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0017:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0023:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0018:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0024:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0019:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0025:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0251:afc".lower())),Float()),# Hema entries for processline15

                             #Defoamer:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0170:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0171:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0172:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0173:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0202:afc".lower())),Float()),

                             #H2O2
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0020:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0021:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0022:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0194:afc".lower())),Float()),

                             #NaOH(Own)
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0032:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0033:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0034:afc".lower())),Float()),
                             
                             #NaOH(Purchase)
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0035:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0036:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0037:afc".lower())),Float()),
                             
                             #NaOH(Total)
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0038:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0039:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0040:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0197:afc".lower())),Float()),

                             #Oxygen
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0026:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0027:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0028:afc".lower())),Float()),
                             
                             #Sulfuric Acid (H2SO4)
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0029:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0030:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0031:afc".lower())),Float()),
                             
                             #White liqour consumption (m3/ADt):
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0009:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0010:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0011:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0012:afc".lower())),Float()),
                             
                             #White liqour consumption (tAA/ADt):
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0013:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0014:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0015:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0016:afc".lower())),Float()),
                             
                             #Utility power consumption:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0050:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0051:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0052:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0053:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0054:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0055:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0056:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0057:afc".lower())),Float()),
                             
                             #Utility LP steam consumption:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0060:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0061:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0062:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0063:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0064:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0065:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0066:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0067:afc".lower())),Float()),
                             
                             #Utility MP steam consumption:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0068:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0069:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0070:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0071:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0072:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0073:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0074:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0075:afc".lower())),Float()),
                             
                             #Utility Total steam consumption:
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0076:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0077:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0078:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0079:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0080:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0081:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0082:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0083:afc".lower())),Float()),
                             
                             #utility_water_consumption
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0084:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0085:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0086:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0087:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0088:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0089:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0090:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0091:afc".lower())),Float()),
                             
                             #Woodchip consumption - Woodchip Total
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0220:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.411WQ7000.F:Y_DAY".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.341WQ5080YD:value".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.341WI2794-YD:value".lower())),Float()),
                             
                             #Woodchip consumption - Chip meter average
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0048:afc".lower())),Float()),
                             
                             #Woodchip consumption - Consumption
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0106:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0107:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0108:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0109:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0211:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0213:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0215:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0217:afc".lower())),Float()),

                             #Yield - Bleaching
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0126:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0127:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0128:afc".lower())),Float()),
                             
                             #Yield - Cooking
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0118:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0119:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0120:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0121:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0130:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0131:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0132:afc".lower())),Float()),
                             Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0133:afc".lower())),Float()),

                             #Yield - Screening
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0122:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0123:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0124:afc".lower())),Float()),
                             # Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("RPL.111XC0125:afc".lower())),Float())
                             
                             )
        
        
        self.metadata.create_all(self.engine)
    


    
     
    def create_daily_kpi_pulp_table(self, table_name):
        '''
        create table for daily kpis
        '''
        self.metadata = MetaData()
        self.pi_data = Table(table_name, self.metadata,
                             Column('daily_kpi_pulp_id',BigInteger(),primary_key=True),
                             Column('datetime',DateTime(),nullable=False),
                             Column('country_id',Integer(),nullable=False),
                             Column('mill_id',Integer(),nullable=False),
                             Column('bu_id',Integer(),nullable=False),
                             Column('bu_type_id',Integer(),nullable=False),
                             Column('kpi_category_id',Integer(),nullable=False),
                             Column('kpi_id',Integer(),nullable=False),
                             
                             
                             Column('process_line_1',Float()),
                             Column('process_line_2',Float()),
                             Column('process_line_3',Float()),
                             Column('process_line_4',Float()),
                             Column('process_line_5',Float()),
                             Column('process_line_6',Float()),
                             Column('process_line_7',Float()),
                             Column('process_line_8',Float()),
                             Column('process_line_9',Float()),
                             Column('process_line_10',Float()),
                             Column('process_line_11',Float()),
                             Column('process_line_12',Float()),
                             Column('process_line_13',Float()),
                             Column('process_line_14',Float()),
                             Column('process_line_15',Float()),
                             
                             UniqueConstraint('datetime','country_id','mill_id','bu_id','bu_type_id','kpi_category_id','kpi_id',name='uniq_pulp_kpi_entry')
                             )
        
        self.metadata.create_all(self.engine)
    

    def create_rz_pulp_pi_table(self,table_name):
            '''
            create table for pulp raw pi tags (Rizhao)
            '''
            self.metadata = MetaData()
            self.pi_data = Table(table_name, self.metadata,
                                 Column('rz_pulp_pi_id',BigInteger(),primary_key=True),
                                 Column('datetime',DateTime(),unique=True,nullable=False),
                                 #Production:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD1.PULP.PRODUCTION".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD2.PULP.PRODUCTION".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD3.PULP.PRODUCTION".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.PULP.PRODUCTION".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.PULP.PRODUCTION".lower())),Float()),

                                 #ClO2:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.CLO2".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.CLO2".lower())),Float()),
                                 
                                 #Defoamer:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.Defoamer".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.Defoamer".lower())),Float()),
                                 
                                 #H2O2:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.H202".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.H202".lower())),Float()),
                                 
                                 #NAOH Own:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.NaOH.OWN".lower())),Float()),
                                 
                                 #NAOH Puchase:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.NaOH.PUR".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.NaOH.PUR".lower())),Float()),
                                 
                                 #NAOH Total:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.NaOH.TOT".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.NaOH.TOT".lower())),Float()),
                                 
                                 #O2:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.O2".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.O2".lower())),Float()),
                                 
                                 #H2SO4:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.H2SO4".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.H2SO4".lower())),Float()),
                                 
                                 #White liquor consumption (m3):
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHE.WL".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHE.WL".lower())),Float()),
                                 
                                 #Utility Power Consumption:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD1.UT.POWER.TOT".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD2.UT.POWER.TOT".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD3.UT.POWER.TOT".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.UT.PR.POWER".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.UT.PR.POWER".lower())),Float()),
                                 
                                 #LP Steam:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD1.UT.LP.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD2.UT.LP.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD3.UT.LP.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.UT.LP.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.UT.LP.STEAM".lower())),Float()),
                                 
                                 #MP Steam:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.UT.MP.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.UT.MP.STEAM".lower())),Float()),
                                 
                                 #Total Steam:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD1.UT.TOT.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD2.UT.TOT.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD3.UT.TOT.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.UT.TOT.STEAM".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.UT.TOT.STEAM".lower())),Float()),
                                 
                                 #Utility Water Consumption:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD1.UT.PR.WATER".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD2.UT.PR.WATER".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PD3.UT.PR.WATER".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.UT.PR.WATER".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.UT.PR.WATER".lower())),Float()),
                                 
                                 #Woodchip Consumption:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHIPS.GMT".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHIPS.GMT".lower())),Float()),
                                 
                                 #Yield Bleaching:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHIPS.BLC.YIELD".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHIPS.BLC.YIELD".lower())),Float()),
                                 
                                 #Yield Cooking (disgester yield):
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHIPS.DIG.YIELD".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHIPS.DIG.YIELD".lower())),Float()),
                                 
                                 #Yield Screening:
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL11.CHIPS.SCR.YIELD".lower())),Float()),
                                 Column(re.sub('[^0-9a-zA-Z]+', '_', "tag_{}".format("OE.PL12.CHIPS.SCR.YIELD".lower())),Float())
    
                                 )
            
            
            self.metadata.create_all(self.engine)
            
    
    def create_krc_kpi_power_minute_table(self,table_name):
        '''
        create table for power KPI (Kerinci) - minutes
        '''
        self.metadata = MetaData()
        self.pi_data = Table(table_name, self.metadata,
                             Column('krc_power_minute_table_id',BigInteger(),primary_key=True),
                             Column('datetime',DateTime(),unique=True,nullable=False),
                             
                             Column('black_liquor',Float()),
                             Column('coal',Float()),
                             Column('biomass',Float()),
                             Column('natural_gas_pb',Float()),
                             Column('natural_gas_vk',Float()),
                             Column('natural_gas_rk',Float()),
                             Column('raw_water',Float()),
                             Column('marine_fuel_oil',Float()),
                             Column('mfo_burner_oil_rb_1235',Float()),
                             Column('mfo_oil_kiln_123',Float()),
                             Column('diesel_oil',Float()),
                             Column('wbl_feed_flow',Float()),
                             Column('wbl_feed_solid',Float()),
                             Column('tds_wbl_feed',Float()),
                             Column('total_wbl_feed',Float()),
                             Column('liquor_firing_rb1235',Float()),
                             Column('solid_rb1235',Float()),
                             Column('total_dry_solid_rb1235',Float()),
                             Column('rb_liquor_firing',Float()),
                             Column('rb_total_steam',Float()),
                             Column('pb_total_steam',Float()),
                             Column('mp_prv',Float()),
                             Column('lp_prv',Float()),
                             Column('condensate_return',Float()),
                             Column('rb_steam_ratio',Float()),
                             Column('evap_steam_economy',Float()),
                             Column('evap_water_evaporation',Float()),
                             Column('evap_steam',Float()),
                             Column('power_to_rpe',Float()),
                             Column('mp_steam_to_rpe',Float()),
                             Column('lp_steam_to_rpe',Float()),
                             Column('process_water_to_rpe',Float()),
                             Column('total_power',Float()),
                             Column('total_hp_steam',Float()),
                             Column('total_mp_steam',Float()),
                             Column('total_lp_steam',Float()),
                             Column('total_process_water',Float()),
                             Column('total_demin_water',Float()),
                             Column('total_soft_water',Float()),
                             Column('total_wl',Float())
                             )
        self.metadata.create_all(self.engine)
        
        
    def create_krc_kpi_power_daily_table(self,table_name):
        '''
        create table for power KPI (Kerinci) - daily
        '''
        self.metadata = MetaData()
        self.pi_data = Table(table_name, self.metadata,
                             Column('krc_power_daily_table_id',BigInteger(),primary_key=True),
                             Column('datetime',DateTime(),unique=True,nullable=False),
                             
                             Column('black_liquor',Float()),
                             Column('coal',Float()),
                             Column('biomass',Float()),
                             Column('natural_gas_pb',Float()),
                             Column('natural_gas_vk',Float()),
                             Column('natural_gas_rk',Float()),
                             Column('raw_water',Float()),
                             Column('marine_fuel_oil',Float()),
                             Column('mfo_burner_oil_rb_1235',Float()),
                             Column('mfo_oil_kiln_123',Float()),
                             Column('diesel_oil',Float()),
                             Column('wbl_feed_flow',Float()),
                             Column('wbl_feed_solid',Float()),
                             Column('tds_wbl_feed',Float()),
                             Column('total_wbl_feed',Float()),
                             Column('liquor_firing_rb1235',Float()),
                             Column('solid_rb1235',Float()),
                             Column('total_dry_solid_rb1235',Float()),
                             Column('rb_liquor_firing',Float()),
                             Column('rb_total_steam',Float()),
                             Column('pb_total_steam',Float()),
                             Column('mp_prv',Float()),
                             Column('lp_prv',Float()),
                             Column('condensate_return',Float()),
                             Column('rb_steam_ratio',Float()),
                             Column('evap_steam_economy',Float()),
                             Column('evap_water_evaporation',Float()),
                             Column('evap_steam',Float()),
                             Column('power_to_rpe',Float()),
                             Column('mp_steam_to_rpe',Float()),
                             Column('lp_steam_to_rpe',Float()),
                             Column('process_water_to_rpe',Float()),
                             Column('total_power',Float()),
                             Column('total_hp_steam',Float()),
                             Column('total_mp_steam',Float()),
                             Column('total_lp_steam',Float()),
                             Column('total_process_water',Float()),
                             Column('total_demin_water',Float()),
                             Column('total_soft_water',Float()),
                             Column('total_wl',Float())
                             )
        self.metadata.create_all(self.engine)





