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

#python EBITDA_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset_test.EBITDA_t'


def EBITDA_main(args):
    #No data avaliable for'Volkswagen (VW) vz', Commerzbank, Deutsche Bank
    project_name, constituent_list,table_store,table_master = get_parameters(args)
    EBITDA_table=pd.DataFrame()
    
    for constituent in constituent_list:
        master = get_master_data(project_name,table_master,constituent)
        
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'

        date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]  
        
        current_EBITDA,last_year_EBITDA,four_years_ago_EBITDA,pct_last_year,pct_four_years,score=EBITDA_collection(master)
        
        EBITDA_table = EBITDA_table.append(pd.DataFrame({'Constituent': constituent,'Constituent_name':constituent_name, 'Constituent_id':constituent_id, 'Current_EBITDA_in_Mio':current_EBITDA,'EBITDA_last_year_in_Mio':last_year_EBITDA, 'percentage_change_in_EBITDA_from_last_year': round(pct_last_year,2),'EBITDA_score': score,'EBITDA_4_years_ago_in_Mio': four_years_ago_EBITDA,'percentage_change_in_EBITDA_from_4_years_ago':round(pct_four_years,2),'Table':'EBITDA analysis','Date':date,'Status':"active" }, index=[0]), ignore_index=True)
    
    #store the analysis
    print ("table done")
    update_result(args)
    print ("update done")

    store_result(args,project_name,EBITDA_table)

    print ("all done")
      
   
   
def EBITDA_collection(master):
    EBITDA = master[['EBITDA_in_Mio','year']].dropna(thresh=2)
    #EBITDA["EBITDA_in_Mio"] = EBITDA["EBITDA_in_Mio"].str.replace(",","").astype(float)
    EBITDA = EBITDA.reset_index(drop=True)
    
    if len(EBITDA)>0:
        current_EBITDA = float(EBITDA['EBITDA_in_Mio'].iloc[-1])
        last_year_EBITDA = float(EBITDA['EBITDA_in_Mio'].iloc[-2])
        four_years_ago_EBITDA = float(EBITDA['EBITDA_in_Mio'].iloc[-4])
        pct_last_year = (current_EBITDA-last_year_EBITDA)*100.0/last_year_EBITDA
        pct_four_years = (current_EBITDA-four_years_ago_EBITDA)*100.0/four_years_ago_EBITDA
        if (pct_last_year>0) & (pct_four_years>0):
            score = 2
        elif pct_last_year>0:
            score =1
        else: 
            score = 0
    else:
        current_EBITDA = 0
        last_year_EBITDA = 0
        four_years_ago_EBITDA = 0
        pct_last_year = 0
        pct_four_years = 0
        score = 0
        
    return current_EBITDA,last_year_EBITDA,four_years_ago_EBITDA,pct_last_year,pct_four_years,score



def get_parameters(args):
    script = 'EBITDA_analysis'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print (query)
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']==script]
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_master = parameter_table["TABLE_COLLECT_FUNDAMENTAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name, constituent_list,table_store,table_master



def update_result(args):
    table_store = args.table_storage
    #import os
    #os.system("Storage.py")
    storage = Storage(google_key_path='/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
    storage = Storage(google_key_path='igenie-project-key.json' )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


#this obtains the master data as a pandas dataframe from source for one constituent. 
def get_master_data(project_name,table_master,constituent):
    table_master = 'pecten_dataset.historical_key_data'
    constituent_name = get_constituent_id_name(constituent)[1]
    constituent_id = get_constituent_id_name(constituent)[0]
    QUERY ='SELECT EBITDA_in_Mio,year,constituent_id,constituent_name FROM '+ table_master + ' WHERE constituent_id= "'+constituent_id+'";'
    print (QUERY)
    master=pd.read_gbq(QUERY, project_id=project_name)
    master['year'] = pd.to_datetime(master['year'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    master = master.sort_values('year',ascending=1).reset_index(drop=True) 
    return master


def store_result(args,project_name,result_df):
    table_store = args.table_storage
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    #Store result to bigquery
    result_df.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=True, reauth=False, if_exists='append',private_key=None)


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
    
    EBITDA_main(args)