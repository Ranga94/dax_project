# -*- coding: utf-8 -*-
##This script ranks the risk of stocks based on result from VaR Analysis. 
from __future__ import print_function
import pandas as pd
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
from datetime import datetime
import matplotlib.pyplot as plt
import pylab
import scipy
from scipy import stats
from decimal import Decimal
import operator
from bs4 import BeautifulSoup
import urllib
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


#python Risk_ranking_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_SCORING' 'igenie-project-key.json' 'pecten_dataset_test.Risk_ranking'

def risk_main(args):
    # Feature PECTEN-9
    backup_table_name = backup_table(args.service_key_path, args.table_storage.split('.')[0],
                                     args.table_storage.split('.')[1])

    project_name,table_store,table_collect=get_parameters(args)
    from_date, to_date = get_timerange(args)
    table_store = args.table_storage
    VaR_table = VaR_collect(project_name)
    VaR_stats_table=VaR_stats(VaR_table)
    constituent_list = VaR_table['Constituent'].unique()
    VaR_risk_board = VaR_ranking(args,VaR_stats_table,VaR_table,constituent_list)
    VaR_risk_board['From_date'] = datetime.strftime(from_date,'%Y-%m-%d %H:%M:%S') 
    VaR_risk_board['To_date'] = datetime.strftime(to_date,'%Y-%m-%d %H:%M:%S') 
    print ('board done')
    
    update_result(table_store)
    print ("update done")

    #Feature PECTEN-9
    try:
        before_insert(args.service_key_path,args.table_storage.split('.')[0],table_store.split('.')[1],
                      from_date,to_date,Storage(args.service_key_path))
    except AssertionError as e:
        drop_backup_table(args.service_key_path, args.table_storage.split('.')[0], backup_table_name)
        e.args += ("Data already exists",)
        raise

    store_result(args,project_name, table_store,VaR_risk_board)

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

    print ("all done")


def get_timerange(args):
    query = 'SELECT * FROM PARAM_READ_DATE WHERE STATUS = "active";'
    timetable = pd.read_sql(query, con=args.sql_connection_string)
    from_date = timetable['FROM_DATE'].loc[timetable['ENVIRONMENT']=='test']
    to_date = timetable['TO_DATE'].loc[timetable['ENVIRONMENT']=='test']
    return from_date[0], to_date[0]


def VaR_collect(project_name):
    VaR_table=pd.read_gbq('SELECT * FROM pecten_dataset_test.VaR_t WHERE Status = "active";', project_id=project_name) #Collect the analysis within set dataframe
    return VaR_table

def VaR_stats(VaR_table):
    VaR_stats_table = pd.DataFrame()
    VaR_list = ['Average_return','Value_at_Risk','Standard_deviation']
    for i in range(len(VaR_list)): 
        table = VaR_list[i]
        mean = VaR_table[VaR_list[i]].mean()
        std_dev = VaR_table[VaR_list[i]].std()
        max_val = VaR_table[VaR_list[i]].max()
        min_val = VaR_table[VaR_list[i]].min()
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        high_lower = min(max_val,mean+std_dev)
        medium_lower=max(min(max_val,mean-std_dev*0.5),max(min_val,mean-std_dev*0.5))
        
        VaR_stats_table=VaR_stats_table.append(pd.DataFrame({'VaR_quantity':VaR_list[i],'Mean':mean,'Standard_deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'High_lower_bound':high_lower, 'Medium_lower_bound':medium_lower},index=[0]),ignore_index=True)
    return VaR_stats_table


def get_parameters(args):
    script = 'Risk_ranking'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print (query)
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the table storing historical price
    table_collect = parameter_table["TABLE_COLLECT_ANALYSIS_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_SCORES_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name,table_store,table_collect



##Allocate the risk score (out of 6) according to Value at Risk, higher the score, risker the stock. 
def VaR_ranking(args,VaR_stats_table,VaR_table,constituent_list):
    VaR_list = ['Value_at_Risk','Standard_deviation']
    m=len(VaR_list)
    n=len(constituent_list)
    var_score_board = pd.DataFrame()
    var_score_array = np.zeros((n,m))
    constituent_name_list = []
    constituent_id_list = []
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    
    for j in range(m): ##loop through fundamental quantities
        high_lower = float(VaR_stats_table['High_lower_bound'].loc[VaR_stats_table['VaR_quantity']==VaR_list[j]])
        medium_lower = float(VaR_stats_table['Medium_lower_bound'].loc[VaR_stats_table['VaR_quantity']==VaR_list[j]])
        
        for i in range(n): ##loop through constituents
            constituent = constituent_list[i]
            
            if constituent.encode('utf-8') =='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
                constituent = 'Münchener Rückversicherungs-Gesellschaft'
            elif constituent.encode('utf-8') =='Deutsche B\xc3\xb6rse':
                constituent = 'Deutsche Börse'

            constituent_name_list.append(get_constituent_id_name(constituent)[1])
            constituent_id_list.append(get_constituent_id_name(constituent)[0])
            
            
            
            if VaR_table[VaR_list[j]].loc[VaR_table['Constituent']==constituent].empty==False: 
                value = VaR_table[VaR_list[j]].loc[VaR_table['Constituent']==constituent].values[0]
                
                if value > high_lower:
                    score = 3 #high risk
                elif value > medium_lower:
                    score = 2 #medium risk
                else: 
                    score = 1 #low risk
                var_score_array[i,j]=score
            else: 
                print (VaR_list[j]+'=N/A for '+constituent)
                score=0
                var_score_array[i,j]=score
        
    for i in range(n): ## loop constituents
        temp = {'Constituent':constituent_list[i],'Constituent_name':constituent_name_list[i],'Constituent_id':constituent_id_list[i],'Risk_score':sum(var_score_array[i,:])}
        score_dict = {str(VaR_list[j]):int(var_score_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        var_score_board = var_score_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
    
    var_score_board = var_score_board.sort_values('Risk_score',axis=0, ascending=True).reset_index(drop=True)
    var_score_board['Status']='active'
    var_score_board['Date_of_analysis'] = date
    ## Append the consistency scores into the calculation of total price growth score
    return var_score_board





def update_result(table_store):
    storage = Storage(google_key_path=args.service_key_path )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def store_result(args,project_name,table_store,result_df):
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
    
    risk_main(args)
        