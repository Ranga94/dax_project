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
import operator
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

##This dividend table stores the results of linear regression, and the list of years when dividends are offered
##'Commerzbank','Deutsche Bank','Lufthansa','RWE','thyssenkrupp','Vonovia','Volkswagen (VW) vz'may not receive dividend yields

#python dividend_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset.dividend_analysis_t' 'test'


def dividend_main(args):
    dividend_table = pd.DataFrame()
    project_name, constituent_list,table_store,table_div,table_historical = get_parameters(args)
    from_date, to_date = get_timerange(args)
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    #table_store = args.table_storage
    #constituent_list = ['ProSiebenSat1 Media']

    # Feature PECTEN-9
    backup_table_name = backup_table(args.service_key_path, args.table_storage.split('.')[0],
                                     args.table_storage.split('.')[1])

    for constituent in constituent_list:
        
        #master = get_master_data(project_name,table_master,constituent)
        
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'
            
       
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]  
        
        div =  get_dividend_data(project_name,table_div,constituent_id)
        his = get_historical_price(project_name,table_historical,constituent,to_date)
        
        current_price = float(his['closing_price'].iloc[0])
        a,b,mse,current_div,future_div,score=dividend_analysis(div) #a represents the average growth of dividend per year in €
        if constituent == 'BMW' or 'Volkswagen (VW) vz' or 'RWE':
            a = a*2.0 #Calculate the annual dividend growth for stocks that release dividend every 6 months
        #if constituent != 'ProSiebenSat1 Media':
        print (current_div)
        print (future_div)
        if future_div == 'n/a':
            estimated_return = 0
        else:
            estimated_return = Gordon_Growth_estimated_return(a,current_price,current_div,future_div)
        div_yield = 1.0*current_div/current_price
        
        dividend_table = dividend_table.append(pd.DataFrame({'Constituent': constituent, 'Constituent_name':constituent_name,'Constituent_id':constituent_id,'Current_dividend': current_div,'Current_dividend_yield':div_yield, 'Average_rate_of_dividend_growth_per_year':round(a,2),'Mean_square_error_of_fitting': round(mse,2),'Estimated_dividend_next_year':future_div,'Current_share_price':current_price,'Gordon_growth_estimated_return':estimated_return,'Dividend_consistency_score':score,'Table':'dividend analysis','Status':"active",'Date_of_analysis':date,'From_date':from_date,"To_date":to_date}, index=[0]), ignore_index=True)
    
    update_result(table_store)

    #Feature PECTEN-9
    try:
        before_insert(args.service_key_path,args.table_storage.split('.')[0],table_store.split('.')[1],
                      from_date,to_date,Storage(args.service_key_path))
    except AssertionError as e:
        drop_backup_table(args.service_key_path, args.table_storage.split('.')[0], backup_table_name)
        e.args += ("Data already exists",)
        raise

    store_result(args,project_name,table_store,dividend_table)

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
    

#Computes the linear regression model for dividend, and produce list of years where dividend is offered. 
def dividend_analysis(div):
    div = div[['Value','Last_Dividend_Payment']].dropna(thresh=1)
    n=len(div)
    current_div = div['Value'].iloc[-1] 
    
    #Estimate growth of dividend using linear fitting
    z = np.polyfit(range(n),np.asarray(div['Value']),1)
    estimation = [x*z[0]+z[1] for x in range(n)]
    res = map(operator.sub, np.asarray(div['Value']), estimation)
    mse = sum([x**2 for x in res])/n*1.0
    
    ##Find out the years where dividend is offered
    #Volkswagen (VW) vz, BMW, RWE: half year
    
    score =0 #This scores the consistency of dividend issued each year. 
    if mse<0.5:
        score +=2
        future_div = current_div + z[0]
    else: 
        future_div ='n/a'
        score = score+0
    
    return z[0],z[1],mse,current_div,future_div,score


def Gordon_Growth_estimated_return(div_growth,current_price,current_div,future_div):
    estimated_return = future_div*1.0/current_price + div_growth/current_div
    return estimated_return


def get_timerange(args):
    query = 'SELECT * FROM PARAM_READ_DATE WHERE STATUS = "active";'
    timetable = pd.read_sql(query, con=args.sql_connection_string)
    from_date = timetable['FROM_DATE'].loc[timetable['ENVIRONMENT']=='test']
    to_date = timetable['TO_DATE'].loc[timetable['ENVIRONMENT']=='test']
    return from_date[0], to_date[0]
    

#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_historical_price(project_name,table_historical,constituent,to_date):
    #Obtain project name, table for historical data in MySQL
    QUERY ='SELECT closing_price,date FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2017-01-01 00:00:00 UTC') and TIMESTAMP ('" + str(to_date) + " UTC') ;"
    print (QUERY)
    his=pd.read_gbq(QUERY, project_id=project_name)
    his['date'] = pd.to_datetime(his['date'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    his = his.sort_values('date',ascending=1).reset_index(drop=True) #sort by date (from oldest to newest) and reset the index
    return his
    


def get_parameters(args):
    script = 'dividend_analysis'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ' WHERE ENVIRONMENT ="' +args.environment+ '";'
    print (query)
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']==script]
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_div = parameter_table["TABLE_COLLECT_FUNDAMENTAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_div = 'pecten_dataset.dividend' #Overwrite temporarily
    table_historical = parameter_table["TABLE_COLLECT_HISTORICAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0] 
    return project_name, constituent_list,table_store,table_div,table_historical



def update_result(table_store):
    storage = Storage(google_key_path=args.service_key_path )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


#this obtains the master data as a pandas dataframe from source for one constituent. 
def get_dividend_data(project_name,table_div,constituent_id):
    QUERY ='SELECT * FROM '+ table_div + ' WHERE constituent_id= "'+constituent_id+'";'
    print (QUERY)
    div=pd.read_gbq(QUERY, project_id=project_name)
    div = div[div['Dividend_Cycle']=='Annually']
    div['Last_Dividend_Payment'] = pd.to_datetime(div['Last_Dividend_Payment'],format="%Y-%m-%d") #read the date format
    div = div.sort_values('Last_Dividend_Payment',ascending=1).reset_index(drop=True)  #Sorting from the earliest to the latest
    return div


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
    parser.add_argument('environment',help = 'test or production')
    args = parser.parse_args()
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object  # Feature PECTEN-9
    from utils.Storage import Storage  # Feature PECTEN-9
    dividend_table = dividend_main(args)
