# -*- coding: utf-8 -*-
##This script combines the scores of fundamental analysis and techinical analysis in order to rank the constituents on their profitability
from __future__ import print_function
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
    # Feature PECTEN-9
    backup_table_name_score = backup_table(args.service_key_path, args.table_score.split('.')[0],
                                     args.table_score.split('.')[1])
    backup_table_name_tag = backup_table(args.service_key_path, args.table_tag.split('.')[0],
                                           args.table_tag.split('.')[1])


    project_name,table_store,table_collect_price,table_collect_fundamental=get_parameters(args)

    # Feature PECTEN-9
    backup_table_name = backup_table(args.service_key_path, args.table_storage.split('.')[0],
                                     args.table_storage.split('.')[1])

    from_date, to_date = get_timerange(args)
    from_date = datetime.strftime(from_date,'%Y-%m-%d %H:%M:%S') #Convert to the standard time format
    to_date = datetime.strftime(to_date,'%Y-%m-%d %H:%M:%S') 
    
    current_fundamental_score_board,fundamental_growth_score_board,price_score_board = scoring_collection(project_name)
    board_list = [current_fundamental_score_board,fundamental_growth_score_board,price_score_board]
    score_list = ['Current_fundamental_total_score','Fundamental_growth_score','Total_price_growth_score']
    constituent_list = current_fundamental_score_board['Constituent'].unique()
    
    combined_profitability_score_board=combined_profitability_scoring(board_list, score_list) 
    combined_profitability_score_board['From_date']=from_date
    combined_profitability_score_board['To_date']=to_date
    
    combined_profitability_tag_table = combined_profitability_tag(combined_profitability_score_board,constituent_list) 
    combined_profitability_tag_table['From_date']=from_date
    combined_profitability_tag_table['To_date']=to_date
    
    print ('combine completed')
    
    print ("table done")
    update_result(args,choice=1) #Update tag
    update_result(args,choice=0) #Update score
    print ("update done")

    #Feature PECTEN-9
    try:
        before_insert(args.service_key_path,args.table_score.split('.')[0],args.table_score.split('.')[1],
                      from_date,to_date,Storage(args.service_key_path))
    except AssertionError as e:
        drop_backup_table(args.service_key_path, args.table_score.split('.')[0], backup_table_name_score)
        e.args += ("Data already exists",)
        raise

    store_result(args,project_name, table_store,combined_profitability_score_board,choice=0) #upload score

    #Feature PECTEN-9
    try:
        after_insert(args.service_key_path, args.table_score.split('.')[0], args.table_score.split('.')[1],
                     from_date, to_date)
    except AssertionError as e:
        e.args += ("No data was inserted.",)
        rollback_object(args.service_key_path,'table',args.table_score.split('.')[0],None,
                        args.table_score.split('.')[1],backup_table_name_score)
        raise

    #Feature PECTEN-9
    try:
        before_insert(args.service_key_path,args.table_tag.split('.')[0],args.table_tag.split('.')[1],
                      from_date,to_date,Storage(args.service_key_path))
    except AssertionError as e:
        drop_backup_table(args.service_key_path, args.table_tag.split('.')[0], backup_table_name_tag)
        e.args += ("Data already exists",)
        raise

    store_result(args,project_name, table_store,combined_profitability_tag_table,choice=1) #upload tag

    #Feature PECTEN-9
    try:
        after_insert(args.service_key_path, args.table_tag.split('.')[0], args.table_tag.split('.')[1],
                     from_date, to_date)
    except AssertionError as e:
        e.args += ("No data was inserted.",)
        rollback_object(args.service_key_path,'table',args.table_tag.split('.')[0],None,
                        args.table_tag.split('.')[1],backup_table_name_tag)
        raise

    drop_backup_table(args.service_key_path, args.table_score.split('.')[0], backup_table_name_score)
    drop_backup_table(args.service_key_path, args.table_tag.split('.')[0], backup_table_name_tag)
    
    print ('all done')
    
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
    print (temp.columns)
    temp['Total_profitability_score']=temp['Total_price_growth_score']+temp['Current_fundamental_total_score']+temp['Fundamental_growth_score']
    combined_profitability_board = pd.DataFrame(temp[['Constituent','Constituent_name','Constituent_id','Total_profitability_score','Total_price_growth_score','Current_fundamental_total_score','Fundamental_growth_score','Status']])
    combined_profitability_board['Date_of_analysis'] = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    
    #combined_profitability_board = combined_profitability_board.sort_values('Total profitability score',axis=0, ascending=False).reset_index(drop=True)
    return combined_profitability_board



def combined_profitability_tag(combined_profitability_board,constituent_list):
    constituent_list = combined_profitability_board['Constituent'].unique()
    profitability_ranking_table=pd.DataFrame()
    combined_profitability_board = combined_profitability_board.sort_values('Total_profitability_score',axis=0, ascending=False).reset_index(drop=True)
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    
    
    for constituent in constituent_list:
        print (constituent)
            
        if constituent.encode('utf-8') =='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent.encode('utf-8') =='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'
            
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]
        
        print (constituent)
       
        index = int(combined_profitability_board[combined_profitability_board['Constituent_id']==constituent_id].index[0])
        price_growth_score= int(max(combined_profitability_board['Total_price_growth_score'].loc[combined_profitability_board['Constituent_id']==constituent_id]))
        print ("price score done")
        fundamental_growth_score=  int(max(combined_profitability_board['Fundamental_growth_score'].loc[combined_profitability_board['Constituent_id']==constituent_id]))
        print ("fundamental score done")
        
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
            
        profitability_ranking_table=profitability_ranking_table.append(pd.DataFrame({'Constituent':constituent,'Constituent_name':constituent_name,'Constituent_id':constituent_id, 'Profitability_rank':index, 'Price_growth':growth_price_status,'Fundamental_growth':growth_fundamental_status,'Status':'active'},index=[0]),ignore_index=True)
    #profitability_ranking_table=profitability_ranking_table.sort_values('Profitability rank',axis=0, ascending=True).reset_index(drop=True)
    return profitability_ranking_table

def get_timerange(args):
    query = 'SELECT * FROM PARAM_READ_DATE WHERE STATUS = "active";'
    timetable = pd.read_sql(query, con=args.sql_connection_string)
    from_date = timetable['FROM_DATE'].loc[timetable['ENVIRONMENT']=='test']
    to_date = timetable['TO_DATE'].loc[timetable['ENVIRONMENT']=='test']
    return from_date[0], to_date[0]



def get_parameters(args):
    script = 'Profitability_ranking'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print (query)
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the table storing the score boards for price analysis and fundamental analysis
    table_collect_price = parameter_table["TABLE_COLLECT_PRICE_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_collect_fundamental = parameter_table["TABLE_COLLECT_FUNDAMENTAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    table_store = parameter_table['TABLE_STORE_SCORES_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name,table_store,table_collect_price,table_collect_fundamental



def update_result(args,choice):
    if choice == 1:
        table_store = args.table_tag
    else:
        table_store = args.table_score
    
    storage = Storage(google_key_path=args.service_key_path)
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def store_result(args,project_name,table_store,result_df,choice):
    if choice == 1:
        table_store = args.table_tag
    else:
        table_store = args.table_score
        
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
    parser.add_argument('tag_table', help='google service key path')
    parser.add_argument('score_table', help='google service key path')

    args = parser.parse_args()
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object  # Feature PECTEN-9
    from utils.Storage import Storage  # Feature PECTEN-9
    
    combined_profitability_main(args)