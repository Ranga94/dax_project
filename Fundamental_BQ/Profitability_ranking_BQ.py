# -*- coding: utf-8 -*-
##This script combines the scores of fundamental analysis and techinical analysis in order to rank the constituents on their profitability
import pandas as pd
from re import sub
from decimal import Decimal
import numpy as np
import datetime
from datetime import datetime
import pylab
import scipy
from scipy import stats
import operator
import urllib
import json
import sys
import ast
import os
from sqlalchemy import *
import argparse
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from google.cloud import datastore
from google.cloud import bigquery

#python Profitability_ranking_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_PROFITABILITY_RISK_ASSESSMENT' 'igenie-project-key.json' 

def combined_profitability_main(args):
    project_name,table_store,table_collect_price,table_collect_fundamental=get_parameters(args)
    current_fundamental_score_board,fundamental_growth_score_board,price_score_board = scoring_collection(project_name)
    board_list = [current_fundamental_score_board,fundamental_growth_score_board,price_score_board]
    score_list = ['Current_fundamental_total_score','Fundamental_growth_score','Total_price_growth_score']
    constituent_list = current_fundamental_score_board['Constituent'].unique()
    combined_profitability_score_board=combined_profitability_scoring(board_list, score_list) #
    combined_profitability_tag_table = combined_profitability_tag(combined_profitability_score_board,constituent_list) 
    print 'combine completed'
    
    print "table done"
    update_result(table_store,choice=1) #Update tag
    update_result(table_store,choice=0) #Update score
    print "update done"
    store_result(args,project_name, table_store,combined_profitability_score_board,choice=0) #upload score
    store_result(args,project_name, table_store,combined_profitability_tag_table,choice=1) #upload tag
    
    print 'all done'
    
def scoring_collection(project_name):
    current_fundamental_score_board = pd.read_gbq('SELECT * FROM pecten_dataset_test.Fundamental_current_ranking_t WHERE Status = "active";', project_id=project_name)
    fundamental_growth_score_board = pd.read_gbq('SELECT * FROM pecten_dataset_test.Fundamental_growth_ranking_t WHERE Status = "active";', project_id=project_name)
    price_score_board = pd.read_gbq('SELECT * FROM pecten_dataset_test.Price_ranking_t WHERE Status = "active";', project_id=project_name)
    return current_fundamental_score_board,fundamental_growth_score_board,price_score_board 


## Combined profitability scores the price growth and fundamental potential out of 60. 
def combined_profitability_scoring(board_list, score_list): 
    n=len(board_list)
    combined_profitability_board = pd.DataFrame()
    temp = board_list[0]
    for i in range(n-1):
        temp = temp.merge(board_list[i+1],how='left',left_on = ['Constituent','Constituent_name','Constituent_id'], right_on = ['Constituent','Constituent_name','Constituent_id'])
    print temp.columns
    temp['Total_profitability_score']=temp['Total_price_growth_score']+temp['Current_fundamental_total_score']+temp['Fundamental_growth_score']
    combined_profitability_board = pd.DataFrame(temp[['Constituent','Constituent_name','Constituent_id','Total_profitability_score','Total_price_growth_score','Current_fundamental_total_score','Fundamental_growth_score','Status']])
    combined_profitability_board['Date'] = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    
    #combined_profitability_board = combined_profitability_board.sort_values('Total profitability score',axis=0, ascending=False).reset_index(drop=True)
    return combined_profitability_board



def combined_profitability_tag(combined_profitability_board,constituent_list):
    constituent_list = combined_profitability_board['Constituent'].unique()
    profitability_ranking_table=pd.DataFrame()
    combined_profitability_board = combined_profitability_board.sort_values('Total_profitability_score',axis=0, ascending=False).reset_index(drop=True)
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    
    for constituent in constituent_list:
        print constituent
            
        if constituent.encode('utf-8') =='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent.encode('utf-8') =='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'
            
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]
        
        print constituent
       
        index = int(combined_profitability_board[combined_profitability_board['Constituent_id']==constituent_id].index[0])
        price_growth_score= int(max(combined_profitability_board['Total_price_growth_score'].loc[combined_profitability_board['Constituent_id']==constituent_id]))
        print "price score done"
        fundamental_growth_score=  int(max(combined_profitability_board['Fundamental_growth_score'].loc[combined_profitability_board['Constituent_id']==constituent_id]))
        print "fundamental score done"
        
        if price_growth_score >=24 :
            growth_price_status = 'High'
        elif price_growth_score >=16 :
            growth_price_status = 'Medium'
        else :
            growth_price_status = 'Low'
            
        if fundamental_growth_score >=8 :
            growth_fundamental_status = 'High'
        elif fundamental_growth_score >=4 :
            growth_fundamental_status = 'Medium'
        else :
            growth_fundamental_status = 'Low'
            
        profitability_ranking_table=profitability_ranking_table.append(pd.DataFrame({'Constituent':constituent,'Constituent_name':constituent_name,'Constituent_id':constituent_id, 'Profitability_rank':index, 'Price_growth':growth_price_status,'Fundamental_growth':growth_fundamental_status,'Date':date,'Status':'active'},index=[0]),ignore_index=True)
    #profitability_ranking_table=profitability_ranking_table.sort_values('Profitability rank',axis=0, ascending=True).reset_index(drop=True)
    return profitability_ranking_table




def get_parameters(args):
    script = 'Profitability_ranking'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the table storing the score boards for price analysis and fundamental analysis
    table_collect_price = parameter_table["TABLE_COLLECT_PRICE_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_collect_fundamental = parameter_table["TABLE_COLLECT_FUNDAMENTAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    table_store = parameter_table['TABLE_STORE_SCORES_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name,table_store,table_collect_price,table_collect_fundamental



def update_result(table_store,choice):
    if choice == 1:
        table_store = 'pecten_dataset_test.Profitability_tag_ranking_t'
    else:
        table_store = 'pecten_dataset_test.Profitability_score_ranking_t'
    
    storage = Storage(google_key_path='/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
    storage = Storage(google_key_path='igenie-project-key.json' )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def store_result(args,project_name,table_store,result_df,choice):
    if choice == 1:
        table_store = 'pecten_dataset_test.Profitability_tag_ranking_t'
    else:
        table_store = 'pecten_dataset_test.Profitability_score_ranking_t'
        
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

    args = parser.parse_args()
    
    combined_profitability_main(args)