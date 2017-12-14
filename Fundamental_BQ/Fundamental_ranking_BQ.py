# -*- coding: utf-8 -*-

##This script does ranking for Fundamental Analysis
import pandas as pd
from re import sub
from decimal import Decimal
import numpy as np
import datetime
from datetime import datetime
import pylab
import scipy
from scipy import stats
from decimal import Decimal
import operator
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

#python Fundamental_ranking_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_SCORING' 'igenie-project-key.json' 

def fundamental_ranking_main(args):
    ##Collect all the parameters
    project_name,table_store,table_collect=get_parameters(args)
    
    ##Collect all the fundamental analysis results
    sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table=fundamental_analysis_collection(project_name)
    
    #Obtain constituent_list
    constituent_list=profit_margin_table['Constituent'].unique()
    
    #table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    table_list = [sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    #current_values_list = ['Current_ROCE','Current_sales_in_Mio','Current_profit_margin','Current_PER','Current_EPS','Current_EBITDA_in_Mio']
    growth_scores_list = ['Sales_score','Profit_margin_score','PER_score','EPS_score','EBITDA_score']
    current_values_list = ['Current_sales_in_Mio','Current_profit_margin','Current_PER','Current_EPS','Current_EBITDA_in_Mio']
    print "collection done"
    
    #Make a score board for fundamental growth. 
    
    fundamental_growth_score_board=fundamental_growth_scoring(args,table_list,growth_scores_list,constituent_list)
    print "fundamental_growth done"
    
    #Make a score board for current fundamental value
    #Calculate statistics of the current fundamental data across all DAX constituents first. 
    current_fundamental_stats_table=current_fundamental_stats(args,table_list,current_values_list)
    current_fundamental_score_board=current_fundamental_scoring(current_fundamental_stats_table,current_values_list,table_list,constituent_list)

    print "current fundamental done"
    
    print "table done"
    update_result(table_store,choice=1) #Update current fundamental
    update_result(table_store,choice=0) #Update growth fundamental
    print "update done"
    store_result(args,project_name, table_store,fundamental_growth_score_board,choice=0)
    store_result(args,project_name, table_store,current_fundamental_score_board,choice=1)
    print "all done"




def fundamental_analysis_collection(project_name):
    #ROCE_table=pd.read_gbq('SELECT * FROM pecten_dataset.ROCE_t WHERE Status = "active";', project_id=project_name)
    sales_table = pd.read_gbq('SELECT * FROM pecten_dataset_test.sales_t WHERE Status = "active";', project_id=project_name)
    profit_margin_table = pd.read_gbq('SELECT * FROM pecten_dataset_test.profit_margin_t WHERE Status = "active";', project_id=project_name)
    PER_table = pd.read_gbq('SELECT * FROM pecten_dataset_test.PER_t WHERE Status = "active";', project_id=project_name)
    EPS_table = pd.read_gbq('SELECT * FROM pecten_dataset_test.EPS_t WHERE Status = "active";', project_id=project_name)
    EBITDA_table = pd.read_gbq('SELECT * FROM pecten_dataset.EBITDA_t WHERE Status = "active";', project_id=project_name)
    #dividend_table = pd.DataFrame(list(collection.find({'Table':'dividend analysis','Status':'active'})))
    return sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table



##FUNDAMENTAL RANKING
## Scores the growth of fundamental values out of 12
def fundamental_growth_scoring(args,table_list,value_list,constituent_list):
    fundamental_score_board = pd.DataFrame()
    ## Note that pandas could not locate special German alphabets, hence use unicode notations
   
    n=len(table_list)
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
 
    for constituent in constituent_list:
        
        fundamental_score_array = np.zeros(n)
            
        if constituent.encode('utf-8') =='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent.encode('utf-8') =='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'

        constituent_name= get_constituent_id_name(constituent)[1]
        constituent_id=get_constituent_id_name(constituent)[0]
        
        
        for i in range(n):
            table = table_list[i]
            if table[value_list[i]].loc[table['Constituent']==constituent].empty == False:
                score = int(table[value_list[i]].loc[table['Constituent']==constituent].values[0])
                fundamental_score_array[i]=score 
            else:
                score = 0
                fundamental_score_array[i]=0
                print str(value_list[i]) + ' for '+ str(constituent) + ' is not avaliable'
            total_score = sum(fundamental_score_array)
            
        fundamental_score_board = fundamental_score_board.append(pd.DataFrame({'Constituent':constituent, 'Constituent_name':constituent_name,'Constituent_id':constituent_id,'Fundamental_growth_score':total_score,'Sales_score':fundamental_score_array[0],'Profit_margin_score':fundamental_score_array[1],'PER_score':fundamental_score_array[2],'EPS_score':fundamental_score_array[3],'EBITDA_score':fundamental_score_array[4],'Table':'Fundamental growth ranking','Status':'active','Date_of_analysis':date},index=[0]),ignore_index=True)
        #fundamental_score_board= fundamental_score_board.sort_values('Fundamental_growth_score',axis=0, ascending=False).reset_index(drop=True)
    return fundamental_score_board
    
    

## Calculate the mean and standard deviation on all the fundamental elements based on overall data of DAX-30 constituents. 
def current_fundamental_stats(args,table_list,value_list):
    fundamental_stats_table = pd.DataFrame()
    #table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    n=len(table_list)
    stats_array = np.zeros((n,2))
    for i in range(n):
        table = table_list[i]
        mean = round(table[value_list[i]].mean(),2)
        std_dev = round(table[value_list[i]].std(),2)
        max_val = table[value_list[i]].max()
        min_val = table[value_list[i]].min()
        stats_array[i,0]=mean
        stats_array[i,1]=std_dev
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        top_lower = mean+std_dev
        good_lower=mean+std_dev*0.5
        fair_lower =mean-0.5*std_dev
        
        fundamental_stats_table=fundamental_stats_table.append(pd.DataFrame({'Fundamental_quantity':value_list[i],'Mean':mean,'Standard_deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'Top_lower_bound':top_lower, 'Good_lower_bound':good_lower,'Fair_lower_bound':fair_lower},index=[0]),ignore_index=True)
    return fundamental_stats_table




## Scores the current fundamenal values out of 24. 
def current_fundamental_scoring(stats_table,current_values_list,table_list,constituent_list):
    #table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    
    m=len(table_list)
    n=len(constituent_list)
    current_fundamental_board = pd.DataFrame()
    current_fundamental_array=np.zeros((n,m))
    #date = "{:%Y-%m-%dT%H:%M:%S}".format(datetime.datetime.now().date())
    date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
    constituent_name_list = []
    constituent_id_list = []
    
    for j in range(m): ##loop through fundamental quantities
        #array = np.zeros(n) #this array stores scores of one particular fundamental quantity for each constituent. 
        table = table_list[j]
        top_lower = float(stats_table['Top_lower_bound'].loc[stats_table['Fundamental_quantity']==current_values_list[j]])
        good_lower = float(stats_table['Good_lower_bound'].loc[stats_table['Fundamental_quantity']==current_values_list[j]])
        fair_lower = float(stats_table['Fair_lower_bound'].loc[stats_table['Fundamental_quantity']==current_values_list[j]])
        
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
            
            
            #print constituent
            if table[current_values_list[j]].loc[table['Constituent']==constituent].empty==False: 
                #print table[current_values_list[j]].loc[table['Constituent']==constituent]
                value = table[current_values_list[j]].loc[table['Constituent']==constituent]
                value = value.iloc[0]
                value = float(value)

                #print top_lower
                if value > top_lower:
                    score = 4 #top-performing
                elif value > good_lower:
                    score = 2 #well-performing
                elif value> fair_lower:
                    score = 1 #fair-performing
                else: 
                    score = 0 #poorly-performing
                current_fundamental_array[i,j]=score
            else: 
                print current_values_list[j]+'=N/A for '+constituent
                score=0
                current_fundamental_array[i,j]=score

            
        
        #current_fundamental_array stores all the info needed to calculate scores
    for i in range(n): ## loop constituents
        temp = {'Constituent':constituent_list[i],'Constituent_name':constituent_name_list[i],'Constituent_id':constituent_id_list[i],'Table':'Current_fundamental_ranking', 'Current_fundamental_total_score':sum(current_fundamental_array[i,:]),'Status':'active','Date_of_analysis':date}
        score_dict = {str(current_values_list[j]):int(current_fundamental_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        current_fundamental_board=current_fundamental_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
        
    return current_fundamental_board


def get_parameters(args):
    script = 'Fundamental_ranking'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the table storing historical price
    table_collect = parameter_table["TABLE_COLLECT_ANALYSIS_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_SCORES_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name,table_store,table_collect



def update_result(table_store,choice):
    if choice == 1:
        table_store = 'pecten_dataset_test.Fundamental_current_ranking'
    else:
        table_store = 'pecten_dataset_test.Fundamental_growth_ranking'
    
    storage = Storage(google_key_path='/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
    storage = Storage(google_key_path='igenie-project-key.json' )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def store_result(args,project_name,table_store,result_df,choice):
    if choice == 1:
        table_store = 'pecten_dataset_test.Fundamental_current_ranking'
    else:
        table_store = 'pecten_dataset_test.Fundamental_growth_ranking'
        
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
    
    fundamental_ranking_main(args)