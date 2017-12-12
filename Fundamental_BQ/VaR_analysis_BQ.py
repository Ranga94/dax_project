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
from scipy.stats import norm,t
from scipy.stats.distributions import binom
from scipy.stats import skew, kurtosis, kurtosistest
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

#python VaR_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset_test.VaR_t'

def VaR_main(args):
    #Set parameters for calculating VaR on monthly-return at a confidence level 99%
    n=21
    alpha=0.01
    
    VaR_table = pd.DataFrame()
    project_name, constituent_list,table_store,table_historical = get_parameters(args)
    for constituent in constituent_list:
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'

        date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]  
        his = get_historical_price(project_name,table_historical,constituent) 
        mean_return,mean_standard_dev, VaR_t= student_VAR_calculate(his,n,alpha)
        VaR_table = VaR_table.append(pd.DataFrame({'Constituent': constituent,'Constituent_name':constituent_name, 'Constituent_id':constituent_id, 'Investment_period': n ,'Average_return': mean_return, 'Standard_deviation':mean_standard_dev,'Confidence_level': alpha,'Value_at_Risk': VaR_t,'Table':'VaR analysis','Date':date,'Status':'active'}, index=[0]), ignore_index=True)
    
    print "table done"
    update_result(args)
    #print "update done"
    store_result(args,project_name, VaR_table)
    print "all done"


def student_VAR_calculate(his,n,alpha):
    ##Calculate the daily returns of stock
    ret = (his['closing_price']-his['closing_price'].shift(n)*1.0)/his['closing_price'].shift(n)
    ##Normal distribution best fit, mu_norm=mean, sig_norm=standard deviation
    mu_norm, sig_norm = norm.fit(ret[n:].values)

    # Student t distribution best fit (finding: nu, which is degrees of freedom)
    parm = t.fit(ret[n:].values)
    nu, mu_t, sig_t = parm
    nu = np.round(nu)

    #Set parameters for calculating value at risk from the distribution
    h = 1 
    lev = 100*(1-alpha)
    xanu = t.ppf(alpha, nu)
 
    VaR_t = np.sqrt((nu-2)/nu) * t.ppf(1-alpha, nu)*sig_norm  - h*mu_norm
    #CVaR_t = -1/alpha * (1-nu)**(-1) * (nu-2+xanu**2) * t.pdf(xanu, nu)*sig_norm  - h*mu_norm

    mean_return = mu_norm*n
    mean_standard_dev = sig_norm * np.sqrt(n)
    
    return mean_return,mean_standard_dev, VaR_t



def get_parameters(args):
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']=='VaR_analysis'].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']=='VaR_analysis']
    #print a
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_historical = parameter_table["TABLE_COLLECT_HISTORICAL_BQ"].loc[parameter_table['SCRIPT_NAME']=='VaR_analysis'].values[0]
    print table_historical
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']=='VaR_analysis'].values[0]
    return project_name, constituent_list,table_store,table_historical


#this makes all the out-dated data in the collection 'inactive'
##alter the status of collection
def update_result(args):
    table_store = args.table_storage
    storage = Storage(google_key_path='igenie-project-key.json')
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def store_result(args,project_name,result_df):
    table_store = args.table_storage
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    #Store result to bigquery
    result_df.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=True, reauth=False, if_exists='append',private_key=None)
    
 
#This obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_historical_price(project_name,table_historical,constituent):
    #Obtain project name, table for historical data in MySQL
    QUERY ='SELECT * FROM '+ table_historical + ' WHERE Constituent= "'+constituent+'"'+ " AND date between TIMESTAMP ('2008-01-01 00:00:00 UTC') and TIMESTAMP ('2017-12-11 00:00:00 UTC') ;"
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
    
    
    VaR_main(args)