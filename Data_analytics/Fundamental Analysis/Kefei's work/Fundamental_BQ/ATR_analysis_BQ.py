# -*- coding: utf-8 -*-
from __future__ import print_function
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import json
import sys
import ast

import os
from sqlalchemy import *
import argparse
from pymongo import MongoClient, errors
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from google.cloud import datastore
from google.cloud import bigquery


#python ATR_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset_test.ATR'

##Record the current ATR, the average ATR of this year, the average ATR in the last 5 years
def ATR_main(args):
    # Feature PECTEN-9
    backup_table_name = backup_table(args.service_key_path, args.table_storage.split('.')[0],
                                     args.table_storage.split('.')[1])

    ATR_table = pd.DataFrame()
    project_name, constituent_list,table_store,table_historical = get_parameters(args)
    table_historical = '{}.historical'.format(args.table_storage.split('.')[0])
    from_date, to_date = get_timerange(args)
    table_store = args.table_storage
    from_date = datetime.strftime(from_date,'%Y-%m-%d %H:%M:%S') #Convert to the standard time format
    to_date = datetime.strftime(to_date,'%Y-%m-%d %H:%M:%S') 
    
    for constituent in constituent_list:
        
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'

        date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S')
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]  
        
        his = get_historical_price(project_name,table_historical,constituent,to_date)
        ATR_array = ATR_calculate(his)
        ATR_table = ATR_table.append(pd.DataFrame({'Constituent': constituent,'Constituent_name':constituent_name, 'Constituent_id':constituent_id, 'Current_14_day_ATR': round(ATR_array[-1],2), 'Average_ATR_in_the_last_12_months': round(ATR_array[-252:].mean(),2), 'Average_ATR_in_the_last_3_years':round(ATR_array[-756:].mean(),2),
                                                   'Table':'ATR analysis','Date_of_analysis':date,
                                                   'From_date':from_date,'To_date':to_date,
                                                   'Status':'active'}, index=[0]), ignore_index=True)

    print("table done")
    update_result(table_store,args)
    print("update done")

    #Feature PECTEN-9
    try:
        before_insert(args.service_key_path,args.table_storage.split('.')[0],table_store.split('.')[1],
                      from_date,to_date,Storage(args.service_key_path))
    except AssertionError as e:
        drop_backup_table(args.service_key_path, args.table_storage.split('.')[0], backup_table_name)
        e.args += ("Data already exists",)
        raise

    store_result(args,project_name, table_store,ATR_table)

    #Feature PECTEN-9
    try:
        after_insert(args.service_key_path, args.table_storage.split('.')[0], table_store.split('.')[1],
                     from_date, to_date)
    except AssertionError as e:
        e.args += ("No data was inserted.",)
        rollback_object(args.service_key_path,'table',args.table_storage.split('.')[0],None,
                        table_store.split('.')[1],backup_table_name)
        raise

    drop_backup_table(args.service_key_path, args.table_storage.split('.')[0], backup_table_name)

    print("all done")
    
    return ATR_table


def ATR_calculate(his):
    TR = his['daily_high'].iloc[0:14]-his['daily_low'].iloc[0:14]
    ATR0 = TR.mean()
    n = his.shape[0]
    ATR_array = np.zeros(n)
    ATR_array[13]=ATR0
    for i in np.arange(14,n):
        ATR = (his['daily_high'].iloc[i] - his['daily_low'].iloc[i] + ATR0 * 13)/14.0
        ATR_array[i] = ATR
        ATR0 = ATR
    return ATR_array


def get_timerange(args):
    query = 'SELECT * FROM PARAM_READ_DATE WHERE STATUS = "active";'
    timetable = pd.read_sql(query, con=args.sql_connection_string)
    from_date = timetable['FROM_DATE'].loc[timetable['ENVIRONMENT']=='test']
    to_date = timetable['TO_DATE'].loc[timetable['ENVIRONMENT']=='test']
    return from_date[0], to_date[0]

def get_parameters(args):
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print(query)
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']=='ATR_analysis'].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']=='ATR_analysis']
    #print a
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_historical = parameter_table["TABLE_COLLECT_HISTORICAL_BQ"].loc[parameter_table['SCRIPT_NAME']=='ATR_analysis'].values[0]
    print(table_historical)
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']=='ATR_analysis'].values[0]
    return project_name, constituent_list,table_store,table_historical


#this makes all the out-dated data in the collection 'inactive'
##alter the status of collection
def update_result(table_store,args):
    storage = Storage(google_key_path=args.service_key_path)
    storage = Storage(google_key_path=args.service_key_path)
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 

def store_result(args,project_name,table_store,result_df):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    #Store result to bigquery
    result_df.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=False, reauth=False, if_exists='append',private_key=None)

#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_historical_price(project_name,table_historical,constituent,to_date):
    #Obtain project name, table for historical data in MySQL
    #QUERY ='SELECT closing_price, date FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2008-01-01 00:00:00 UTC') and TIMESTAMP ('2017-12-11 00:00:00 UTC') ;"
    QUERY ='SELECT * FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2009-01-01 00:00:00 UTC') and TIMESTAMP ('" + to_date + " UTC') ;"
   
    his=pd.read_gbq(QUERY, project_id=project_name,verbose=False)
    his['date'] = pd.to_datetime(his['date'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    his = his.sort_values('date',ascending=1).reset_index(drop=True) #sort by date (from oldest to newest) and reset the index
    return his

def get_constituent_id_name(old_constituent_name):
    mapping = {}
    mapping["BMW"] = ("BMWDE8170003036" , "BAYERISCHE MOTOREN WERKE AG")
    mapping["Allianz"] = ("ALVDEFEI1007380" , "ALLIANZ SE")
    mapping["Commerzbank"] = ("CBKDEFEB13190" , "COMMERZBANK AKTIENGESELLSCHAFT")
    mapping["adidas"] = ("ADSDE8190216927", "ADIDAS AG")
    mapping["Deutsche Bank"] = ("DBKDEFEB13216" , "DEUTSCHE BANK AG")
    mapping["EON"] = ("EOANDE5050056484" , "E.ON SE")
    mapping["Lufthansa"] = ("LHADE5190000974" ,"DEUTSCHE LUFTHANSA AG")
    mapping["Continental"] = ("CONDE2190001578" , "CONTINENTAL AG")
    mapping["Daimler"] = ("DAIDE7330530056" , "DAIMLER AG")
    mapping["Siemens"] = ("SIEDE2010000581" , "SIEMENS AG")
    mapping["BASF"] = ("BASDE7150000030" , "BASF SE")
    mapping["Bayer"] = ("BAYNDE5330000056" , "BAYER AG")
    mapping["Beiersdorf"] = ("BEIDE2150000164" , "BEIERSDORF AG")
    mapping["Deutsche Börse"] = ("DB1DEFEB54555" , "DEUTSCHE BOERSE AG")
    mapping["Deutsche Post"] = ("DPWDE5030147191" , "DEUTSCHE POST AG")
    mapping["Deutsche Telekom"] = ("DTEDE5030147137" , "DEUTSCHE TELEKOM AG")
    mapping["Fresenius"] = ("FREDE6290014544" , "FRESENIUS SE & CO.KGAA")
    mapping["HeidelbergCement"] = ("HEIDE7050000100" , "HEIDELBERGCEMENT AG")
    mapping["Henkel vz"] = ("HEN3DE5050001329" , "HENKEL AG & CO. KGAA")
    mapping["Infineon"] = ("IFXDE8330359160" , "INFINEON TECHNOLOGIES AG")
    mapping["Linde"] = ("LINDE8170014684" , "LINDE AG")
    mapping["Merck"] = ("MRKDE6050108507" , "MERCK KGAA")
    mapping["ProSiebenSat1 Media"] = ("PSMDE8330261794" , "PROSIEBENSAT.1 MEDIA SE")
    mapping["RWE"] = ("RWEDE5110206610" , "RWE AG")
    mapping["SAP"] = ("SAPDE7050001788" , "SAP SE")
    mapping["thyssenkrupp"] = ("TKADE5110216866" , "THYSSENKRUPP AG")
    mapping["Vonovia"] = ("VNADE5050438829" , "VONOVIA SE")
    mapping["DAX"] = ("DAX", "DAX")
    mapping["Fresenius Medical Care"] = ("FMEDE8110066557" , "FRESENIUS MEDICAL CARE AG & CO.KGAA")
    mapping["Volkswagen (VW) vz"] = ("VOW3DE2070000543" , "VOLKSWAGEN AG")
    mapping["Münchener Rückversicherungs-Gesellschaft"] = ("MUV2DEFEI1007130" , "MUNCHENER RUCKVERSICHERUNGS - GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN")

    if old_constituent_name in mapping:
        return mapping[old_constituent_name]
    else:
        return old_constituent_name

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('sql_connection_string', help='The connection string to mysql for parameter table') 
    parser.add_argument('parameter_table',help="The name of the parameter table in MySQL")
    parser.add_argument('service_key_path',help='google service key path')
    parser.add_argument('table_storage',help='BigQuery table where the new data is stored')
    args = parser.parse_args()
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object # Feature PECTEN-9
    from utils.Storage import Storage #Feature PECTEN-9
    ATR_main(args)