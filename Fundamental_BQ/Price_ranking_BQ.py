# -*- coding: utf-8 -*-

##This script does ranking for technical/price analysis
import pandas as pd
from re import sub
from decimal import Decimal
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pylab
import scipy
from scipy import stats
from decimal import Decimal
import operator
import sys
import ast
from datetime import datetime
import os
from sqlalchemy import *
import argparse
from pymongo import MongoClient, errors
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from google.cloud import datastore
from google.cloud import bigquery


#python Price_ranking_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_SCORING' 'igenie-project-key.json' 'pecten_dataset_test.Price_ranking_t'


def price_ranking_main(args):
    project_name,table_store,table_collect=get_parameters(args)
    table_store = args.table_storage
    cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,RSI_table=price_analysis_collection(project_name)
    print "collection done"
    
    #Obtain this table from MySQL
    table_list = [cumulative_returns_table,cumulative_returns_table,quarter_mean_table,quarter_mean_table,RSI_table]
    
    #Obtain the following columns from MySQL
    value_list = ['one_year_return','three_years_return','Rate_of_change_in_price_in_the_last_365_days_per_quarter','Rate_of_change_in_price_in_the_last_3_years_per_quarter','Current_RSI']
   
    #Obtain the constituent_list from one of the tables
    constituent_list = cumulative_returns_table['Constituent'].unique()
    
    ##Make a ranking for price growth
    ##Collect the statistics across all DAX-30 constituents first
    price_stats_table=price_growth_stats(args,table_list,value_list)
    "stats table done"
    ##Make a scoreboard. 
    price_growth_board =price_growth_scoring(args,price_stats_table,table_list,value_list,constituent_list)
    
    ##Update the collection
    
    print "table done"
    update_result(table_store)
    print "update done"
    store_result(args,project_name, table_store,price_growth_board)
    print "all done"


def price_analysis_collection(project_name):  #Want to collect data collected between a certain period? 
    cumulative_returns_table=pd.read_gbq('SELECT * FROM pecten_dataset.cumulative_returns_t WHERE Status = "active";', project_id=project_name)
    quarter_mean_table = pd.read_gbq('SELECT * FROM pecten_dataset.quarter_mean_growth_t WHERE Status = "active";', project_id=project_name)
    standard_dev_table = pd.read_gbq('SELECT * FROM pecten_dataset.standard_deviation_t WHERE Status = "active";', project_id=project_name)
    ATR_table = pd.read_gbq('SELECT * FROM pecten_dataset.ATR_t WHERE Status = "active";', project_id=project_name)
    RSI_table = pd.read_gbq('SELECT * FROM pecten_dataset.RSI_t WHERE Status = "active";', project_id=project_name)
    
    #dividend_table = pd.DataFrame(list(collection.find({'Table':'dividend analysis','Status':'active'})))
    return cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,RSI_table


## Calculate the mean and standard deviation on all the fundamental elements based on overall data of DAX-30 constituents. 
def price_growth_stats(args,table_list,value_list):
    price_stats_table = pd.DataFrame()
    n=len(table_list)
    stats_array = np.zeros((n,2))
    for i in range(n):
        table = table_list[i]
        mean = table[value_list[i]].mean()
        std_dev = table[value_list[i]].std()
        max_val = table[value_list[i]].max()
        min_val = table[value_list[i]].min()
        stats_array[i,0]=mean
        stats_array[i,1]=std_dev
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        top_lower = max(mean+std_dev,0)
        good_lower=max(mean+std_dev*0.5,0)
        fair_lower =max(mean-0.5*std_dev,0)
        below_ave_lower =mean-1.0*std_dev
        
        price_stats_table=price_stats_table.append(pd.DataFrame({'Price_growth_quantity':value_list[i],'Mean':mean,'Standard_deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'Top_lower_bound':top_lower, 'Good_lower_bound':good_lower,'Fair_lower_bound':fair_lower},index=[0]),ignore_index=True)
    return price_stats_table



## Scores the growth of price out of 30. 
def price_growth_scoring(args,stats_table,table_list,value_list,constituent_list):
    m=len(value_list)
    #constituents_list = [str(item) for item in args.constituents_list.split(',')]
    n=len(constituent_list)
    price_growth_board = pd.DataFrame()
    price_growth_array=np.zeros((n,m))
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    constituent_name_list = []
    constituent_id_list = []
    
    for j in range(m): ##loop through fundamental quantities
        table = value_list[j]
        print str(table)
        top_lower = float(stats_table['Top_lower_bound'].loc[stats_table['Price_growth_quantity']==value_list[j]])
        good_lower = float(stats_table['Good_lower_bound'].loc[stats_table['Price_growth_quantity']==value_list[j]])
        fair_lower = float(stats_table['Fair_lower_bound'].loc[stats_table['Price_growth_quantity']==value_list[j]])
        
        #print str(table)
    
        for i in range(n): ##loop through constituents
            constituent = constituent_list[i]

            ##Taking care of the special German characters
            print constituent
            
            
            if constituent.encode('utf-8') =='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
                constituent = 'Münchener Rückversicherungs-Gesellschaft'
            elif constituent.encode('utf-8') =='Deutsche B\xc3\xb6rse':
                constituent = 'Deutsche Börse'

            constituent_name_list.append(get_constituent_id_name(constituent)[1])
            constituent_id_list.append(get_constituent_id_name(constituent)[0])
            
            ##'Deutsche Börse','Münchener Rückversicherungs-Gesellschaft' 
            #if constituent =='Deutsche Borse':
                #constituent = u'Deutsche B\xf6rse'
            #elif constituent == 'Munchener Ruckversicherungs-Gesellschaft':
                #constituent = u'M\xfcnchener R\xfcckversicherungs-Gesellschaft'
            
            print "conversion of constituent name done"
            
            table = table_list[j]
            if table[value_list[j]].loc[table['Constituent']==constituent].empty==False: 
                value =  table[value_list[j]].loc[table['Constituent']==constituent].values[0]
                
                if value > top_lower:
                    score = 4 #top-performing
                elif value > good_lower:
                    score = 3 #well-performing
                elif value> fair_lower:
                    score = 2 #fair-performing
                else: 
                    score = 1 #poorly-performing
                price_growth_array[i,j]=score
            else: 
                print value_list[j]+'=N/A for '+constituent
                score=0
                price_growth_array[i,j]=score
        
    for i in range(n): ## loop constituents
        temp = {'Constituent':constituent_list[i], 'Constituent_name':constituent_name_list[i],'Constituent_id':constituent_id_list[i],'Price_growth_score':sum(price_growth_array[i,:])}
        score_dict = {str(value_list[j]):int(price_growth_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        price_growth_board=price_growth_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
    
    ## Append the consistency scores into the calculation of total price growth score
    CR_table = table_list[0]
    QM_table = table_list[2]
    price_growth_board = price_growth_board.merge(CR_table[['Constituent','Cumulative_return_consistency_score']], on='Constituent',how='inner')
    price_growth_board = price_growth_board.merge(QM_table[['Constituent','Quarterly_growth_consistency_score']], on='Constituent',how='inner')
    #price_growth_board = price_growth_board.merge(market_signal_table[['Constituent','Bull score (crossing)']],on='Constituent',how='inner')
    price_growth_board['Total_price_growth_score']=price_growth_board['Cumulative_return_consistency_score']+price_growth_board['Quarterly_growth_consistency_score']+price_growth_board['Price_growth_score']
    columnsTitles = ['Constituent','Constituent_name','Constituent_id','Total_price_growth_score','Price_growth_score','Cumulative_return_consistency_score','Quarterly_growth_consistency_score','one_year_return','three_years_return','Rate_of_change_in_price_in_the_last_365_days_per_quarter','Rate_of_change_in_price_in_the_last_3_years_per_quarter','Current_RSI']
    price_growth_board=price_growth_board.reindex(columns=columnsTitles)
    price_growth_board['Status']='active'
    price_growth_board['Date']=date
    price_growth_board['Table']='Price growth ranking'
    return price_growth_board



def get_parameters(args):
    script = 'Price_ranking'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the table storing historical price
    table_collect = parameter_table["TABLE_COLLECT_ANALYSIS_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_SCORES_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name,table_store,table_collect



def update_result(table_store):
    storage = Storage(google_key_path='igenie-project-key.json' )
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
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    price_ranking_main(args)