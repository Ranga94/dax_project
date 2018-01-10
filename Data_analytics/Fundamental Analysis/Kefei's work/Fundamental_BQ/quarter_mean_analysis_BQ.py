# -*- coding: utf-8 -*-
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


#python quarter_mean_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset_test.quarter_mean_growth' 

def quarter_mean_main(args): 
    project_name, constituent_list,table_store,table_historical = get_parameters(args)
    table_historical = 'pecten_dataset_test.historical'
    from_date, to_date = get_timerange(args)
    table_store = args.table_storage
    quarter_mean_table = pd.DataFrame()
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
        a,b,a_3yrs,b_3yrs,a_1yr,b_1yr,quarter_mean,score = quarter_mean_analysis(his)
        
        quarter_mean_table = quarter_mean_table.append(pd.DataFrame({'Constituent': constituent, 'Constituent_name':constituent_name, 'Constituent_id':constituent_id,'Current_Quarter_mean_price':round(quarter_mean[-1],2),'Rate_of_change_in_price_from_2010_per_quarter': round(a,2), 'Rate_of_change_in_price_in_the_last_3_years_per_quarter':round(a_3yrs,2),'Rate_of_change_in_price_in_the_last_365_days_per_quarter': round(a_1yr,2),'Quarterly_growth_consistency_score':score,'Table':'quarterly_growth_analysis','Date_of_analysis':date,'From_date':from_date,'To_date':to_date,'Status':'active'}, index=[0]), ignore_index=True)
    
    print "table done"
    update_result(table_store)
    print "update done"
    store_result(args,project_name,table_store,quarter_mean_table)
    print "all done"
    
    
    #update the source before storing new data
    

#Computes the mean rate of price growth per quarter for one particular constituent
def quarter_mean_analysis(his):
 
    #Analyse the cumulative return of the stock price after the recession in 2009. Quarterly. 
    his_2010 = his[['closing_price','date']].loc[his['date']>=datetime(2010,01,01)]
    ##Calulate the mean stock price for every quarter
    n=his_2010.shape[0]
    num_quarters = int(n/63.0)
    if n>63:
        quarter_mean = np.zeros(num_quarters)
    
        for i in range(num_quarters): 
            if i<=num_quarters-1:
                quarter_mean[i]=float(his_2010['closing_price'].iloc[63*i:63*(i+1)].mean())
            else: 
                quarter_mean[i]=float(his_2010['closing_price'].iloc[63*i:].mean())
    
    
        #Use linear to estimate the growth of stock price: y=ax+b
        
        z = np.polyfit(range(num_quarters),quarter_mean,1) #fitting for all data
            
        if n>252*3:
            z_1yr = np.polyfit(range(4), quarter_mean[-4:],1) #fitting for the last 1 year data
            z_3yrs =np.polyfit(range(12), quarter_mean[-12:],1) #fitting for the last 3 years data
        elif n>252: 
            z_1yr = np.polyfit(range(4), quarter_mean[-4:],1) #fitting for the last 1 year data
            z_3yrs = z_1yr
        else:
            z_1yr = z
            z_3yrs = z
            
        
        if (z_1yr[0]>0)&(z_3yrs[0]>0)&(z[0]>0):
            score =4
        elif (z_1yr[0]>0)&(z[0]>0):
            score = 3
        elif ((z_1yr[0]>0)&(z_3yrs[0]>0)) or ((z[0]>0)&(z_3yrs[0]>0)):
            score = 2
        elif (z_1yr[0]>0) or (z[0]>0):
            score = 1
        else:
            score = 0
    else:
        z=[0,0]
        z_3yrs = z
        z_1yr = z
        quarter_mean = [0]
        score = 0
    
    return z[0],z[1],z_3yrs[0],z_3yrs[1],z_1yr[0],z_1yr[1],quarter_mean,score

def get_timerange(args):
    query = 'SELECT * FROM PARAM_READ_DATE WHERE STATUS = "active";'
    timetable = pd.read_sql(query, con=args.sql_connection_string)
    from_date = timetable['FROM_DATE'].loc[timetable['ENVIRONMENT']=='test']
    to_date = timetable['TO_DATE'].loc[timetable['ENVIRONMENT']=='test']
    return from_date[0], to_date[0]


def get_parameters(args):
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']=='quarter_mean_analysis'].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']=='quarter_mean_analysis']
    #print a
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_historical = parameter_table["TABLE_COLLECT_HISTORICAL_BQ"].loc[parameter_table['SCRIPT_NAME']=='quarter_mean_analysis'].values[0]
    print table_historical
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']=='quarter_mean_analysis'].values[0]
    return project_name, constituent_list,table_store,table_historical


#this makes all the out-dated data in the collection 'inactive'
##alter the status of collection

def update_result(table_store):
    storage = Storage(google_key_path='igenie-project-key.json')
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

def get_historical_price(project_name,table_historical,constituent,to_date):
    #Obtain project name, table for historical data in MySQL
    #QUERY ='SELECT closing_price, date FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2008-01-01 00:00:00 UTC') and TIMESTAMP ('2017-12-11 00:00:00 UTC') ;"
    QUERY ='SELECT closing_price, date FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2009-01-01 00:00:00 UTC') and TIMESTAMP ('" + to_date + " UTC') ;"
   
    print QUERY
    his=pd.read_gbq(QUERY, project_id=project_name)
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


class Storage:
    def __init__(self, google_key_path=None, mongo_connection_string=None):
        if google_key_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
            self.bigquery_client = bigquery.Client()
        else:
            self.bigquery_client = None

        if mongo_connection_string:
            self.mongo_client = MongoClient(mongo_connection_string)
        else:
            self.mongo_client = None
            
    def get_bigquery_data(self, query, timeout=None, iterator_flag=True): 
        if self.bigquery_client:
            client = self.bigquery_client
        else:
            client = bigquery.Client()

        print("Running query...")
        query_job = client.query(query)
        iterator = query_job.result(timeout=timeout)

        if iterator_flag:
            return iterator
        else:
            return list(iterator)



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('sql_connection_string', help='The connection string to mysql for parameter table') 
    parser.add_argument('parameter_table',help="The name of the parameter table in MySQL")
    parser.add_argument('service_key_path',help='google service key path')
    parser.add_argument('table_storage',help='BigQuery table where the new data is stored')
    
    args = parser.parse_args()
    quarter_mean_main(args)