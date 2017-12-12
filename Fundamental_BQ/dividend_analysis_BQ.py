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

#python dividend_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset.dividend_t'


def dividend_main(args):
    dividend_table = pd.DataFrame()
    table_store = args.table_storage
    project_name, constituent_list,table_store,table_master,table_historical = get_parameters(args)
    table_store = args.table_storage
    
    for constituent in constituents_list:
        master = get_master_data(project_name,table_master,constituent)
        
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'
            
        div = get_dividend_data(args,constituent)
        his = get_historical_price(project_name,table_historical,constituent)
        current_price = float(his['closing_price'].iloc[0])
        a,b,mse,year_list,current_div,future_div,score=dividend_analysis(div) #a represents the average growth of dividend per year in €
        if constituent == 'BMW' or 'Volkswagen (VW) vz' or 'RWE':
            a = a*2.0 #Calculate the annual dividend growth for stocks that release dividend every 6 months
        if constituent != 'ProSiebenSat1 Media':
            estimated_return = Gordon_Growth_estimated_return(a,current_price,current_div,future_div)
        else:
            estimated_return = 0
        
        dividend_table = dividend_table.append(pd.DataFrame({'Constituent': constituent, 'Current dividend': current_div, 'Average rate of dividend growth /year':round(a,2),'Mean square error of fitting': round(mse,2),'Years of dividend offer':'%s'%year_list,'Estimated dividend next year':future_div,'Current share price':current_price,'Gordon growth estimated return':estimated_return,'Dividend consistency score':score,'Table':'dividend analysis','Status':"active",'Date':str(datetime.date.today())}, index=[0]), ignore_index=True)
    
    status_update(args)
    store_result(args, dividend_table)

#Computes the linear regression model for dividend, and produce list of years where dividend is offered. 
def dividend_analysis(div):
    div = div[['Dividend','year']].dropna(thresh=1)
    div = pd.DataFrame(div)
    value = [unicode(x) for x in div["Value"]]
    value = [x.replace(u'\u20ac',"") for x in value]
    value = [float(x) for x in value]
    n=len(value)
    current_div = value[0] 
    
    #Estimate growth of dividend using linear fitting
    z = np.polyfit(range(n),value[::-1],1)
    estimation = [x*z[0]+z[1] for x in range(n)]
    res = map(operator.sub, value[::-1], estimation)
    mse = sum([x**2 for x in res])/n*1.0
    
    ##Find out the years where dividend is offered
    #Volkswagen (VW) vz, BMW, RWE: half year
    date_list = pd.DatetimeIndex(div['year'])
    year_list = date_list.year
    
    score =0 #This scores the consistency of dividend issued each year. 
    if mse<0.5:
        score +=2
        future_div = current_div + z[0]
    else: 
        future_div ='n/a'
        score = score+0
    
    return z[0],z[1],mse,year_list,current_div,future_div,score


def Gordon_Growth_estimated_return(div_growth,current_price,current_div,future_div):
    estimated_return = future_div*1.0/current_price + div_growth/current_div
    return estimated_return



#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_historical_price(project_name,table_historical,constituent):
    #Obtain project name, table for historical data in MySQL
    QUERY ='SELECT closing_price,date FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2008-01-01 00:00:00 UTC') and TIMESTAMP ('2017-12-11 00:00:00 UTC') ;"
    print QUERY
    his=pd.read_gbq(QUERY, project_id=project_name)
    his['date'] = pd.to_datetime(his['date'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    his = his.sort_values('date',ascending=1).reset_index(drop=True) #sort by date (from oldest to newest) and reset the index
    return his
    



def get_parameters(args):
    script = 'dividend_analysis'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']==script]
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_master = parameter_table["TABLE_COLLECT_FUNDAMENTAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_master = 'pecten_dataset.historical_key_data'
    table_historical = parameter_table["TABLE_COLLECT_HISTORICAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name, constituent_list,table_store,table_master,table_historical



def update_result(table_store):
    storage = Storage(google_key_path='igenie-project-key.json' )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


#this obtains the master data as a pandas dataframe from source for one constituent. 
def get_dividend_data(project_name,table_master,constituent):
    QUERY ='SELECT * FROM '+ table_master + ' WHERE Constituent= "'+constituent+'";'
    print QUERY
    master=pd.read_gbq(QUERY, project_id=project_name)
    master['year'] = pd.to_datetime(master['year'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    div_data = master.sort_values('year',ascending=1).reset_index(drop=True) 
    
    return div_data


def store_result(args,project_name,table_store,result_df):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    
    #Store result to bigquery
    result_df.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=True, reauth=False, if_exists='append',private_key=None)
    
    
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
    mapping["Volkswagen"] = ("VOW3DE2070000543" , "VOLKSWAGEN AG")
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
    
    
    dividend_table = dividend_main(args)
