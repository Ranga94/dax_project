# -*- coding: utf-8 -*-

##This script does ranking for Fundamental Analysis
import pandas as pd
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
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

#python Fundamental_ranking.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'fundamental analysis'  -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'Fundamental ranking'

def fundamental_ranking_main(args):
    
    ##Collect all the fundamental analysis results
    industry_category_table,ROCE_table,sales_table,dividend_table,profit_margin_table,PER_table,EPS_table,EBITDA_table=fundamental_analysis_collection(args)
    table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    current_values_list = ['Current ROCE','Current sales in Mio','Current profit margin','Current PER','Current EPS','Current EBITDA in Mio']
    print "collection done"
    
    #Make a score board for fundamental growth. 
    fundamental_growth_score_board=fundamental_growth_scoring(args,table_list)
    print "fundamental_growth done"
    
    #Make a score board for current fundamental value
    #Calculate statistics of the current fundamental data across all DAX constituents first. 
    current_fundamental_stats_table=current_fundamental_stats(args,table_list,current_values_list)
    current_fundamental_score_board=current_fundamental_scoring(current_fundamental_stats_table,current_values_list,table_list)
    print "current fundamental done"
    
   
    status_update(args)
    store_result(args,fundamental_growth_score_board)
    store_result(args,current_fundamental_score_board)
    
    print "insert done"

def fundamental_analysis_collection(args):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_get_fundamental_analysis]
    industry_category_table= pd.DataFrame(list(collection.find({'Table':'category analysis'})))
    ROCE_table = pd.DataFrame(list(collection.find({'Table':'ROCE analysis','Status':'active'})))
    sales_table = pd.DataFrame(list(collection.find({'Table':'Sales analysis','Status':'active'})))
    dividend_table = pd.DataFrame(list(collection.find({'Table':'dividend analysis','Status':'active'})))
    profit_margin_table = pd.DataFrame(list(collection.find({'Table':'profit margin analysis','Status':'active'})))
    PER_table = pd.DataFrame(list(collection.find({'Table':'PER analysis','Status':'active'})))
    EPS_table = pd.DataFrame(list(collection.find({'Table':'EPS analysis','Status':'active'})))
    EBITDA_table = pd.DataFrame(list(collection.find({'Table':'EBITDA analysis','Status':'active'})))
    return industry_category_table,ROCE_table,sales_table,dividend_table,profit_margin_table,PER_table,EPS_table,EBITDA_table



##FUNDAMENTAL RANKING
## Scores the growth of fundamental values out of 12
def fundamental_growth_scoring(args,table_list):
    fundamental_score_board = pd.DataFrame()
    ## Note that pandas could not locate special German alphabets, hence use unicode notations
    ## Münchener Rückversicherungs-Gesellschaft:u'M\xfcnchener R\xfcckversicherungs-Gesellschaft'
    ## Deutsche Börse: u'Deutsche B\xf6rse'
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    score_list = ['ROCE score','Sales score','Profit margin score','PER score','EPS score','EBITDA score']
    n=len(table_list)
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        fundamental_score_array = np.zeros(n)
        for i in range(n):
            table = table_list[i]
            if table[score_list[i]].loc[table['Constituent']==constituent].empty == False:
                score = int(table[score_list[i]].loc[table['Constituent']==constituent].values[0])
                fundamental_score_array[i]=score 
            else:
                score = 'N/A'
                fundamental_score_array[i]=0
                print str(score_list[i]) + ' for '+ str(constituent) + ' is not avaliable'
            total_score = sum(fundamental_score_array)
        fundamental_score_board = fundamental_score_board.append(pd.DataFrame({'Constituent':constituent, 'Fundamental growth score':total_score,'ROCE score':fundamental_score_array[0],'Sales score':fundamental_score_array[1],'Profit margin score':fundamental_score_array[2],'PER score':fundamental_score_array[3],'EPS score':fundamental_score_array[4],'EBITDA score':fundamental_score_array[5],'Table':'Fundamental growth ranking','Status':'active','Date':str(datetime.date.today())},index=[0]),ignore_index=True)
        fundamental_score_board= fundamental_score_board.sort_values('Fundamental growth score',axis=0, ascending=False).reset_index(drop=True)
    return fundamental_score_board
    
    

## Calculate the mean and standard deviation on all the fundamental elements based on overall data of DAX-30 constituents. 
def current_fundamental_stats(args,table_list,current_values_list):
    fundamental_stats_table = pd.DataFrame()
    #table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    n=len(table_list)
    stats_array = np.zeros((n,2))
    for i in range(n):
        table = table_list[i]
        mean = round(table[current_values_list[i]].mean(),2)
        std_dev = round(table[current_values_list[i]].std(),2)
        max_val = table[current_values_list[i]].max()
        min_val = table[current_values_list[i]].min()
        stats_array[i,0]=mean
        stats_array[i,1]=std_dev
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        top_lower = mean+std_dev
        good_lower=mean+std_dev*0.5
        fair_lower =mean-0.5*std_dev
        below_ave_lower =mean-1.0*std_dev
        
        fundamental_stats_table=fundamental_stats_table.append(pd.DataFrame({'Fundamental quantity':current_values_list[i],'Mean':mean,'Standard deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'Top lower-bound':top_lower, 'Good lower-bound':good_lower,'Fair lower-bound':fair_lower},index=[0]),ignore_index=True)
    return fundamental_stats_table




## Scores the current fundamenal values out of 24. 
def current_fundamental_scoring(stats_table,current_values_list,table_list):
    #table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    #current_values_list = ['Current ROCE','Current sales in Mio','Current profit margin','Current PER','Current EPS','Current EBITDA in Mio']
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents = ['Allianz']
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    m=len(table_list)
    n=len(constituents_list)
    current_fundamental_board = pd.DataFrame()
    current_fundamental_array=np.zeros((n,m))
    for j in range(m): ##loop through fundamental quantities
        array = np.zeros(n) #this array stores scores of one particular fundamental quantity for each constituent. 
        table = table_list[j]
        top_lower = float(stats_table['Top lower-bound'].loc[stats_table['Fundamental quantity']==current_values_list[j]])
        good_lower = float(stats_table['Good lower-bound'].loc[stats_table['Fundamental quantity']==current_values_list[j]])
        fair_lower = float(stats_table['Fair lower-bound'].loc[stats_table['Fundamental quantity']==current_values_list[j]])
       
        
        for i in range(n): ##loop through constituents
            constituent = constituents_list[i]
            #print constituent
            if table[current_values_list[j]].loc[table['Constituent']==constituent].empty==False: 
                print table[current_values_list[j]].loc[table['Constituent']==constituent]
                value = float(table[current_values_list[j]].loc[table['Constituent']==constituent])
                #print value
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
            #print score
            
        #current_fundamental_sum = sum(current_fundamental_array) ##sum the values on the same row
        
        #current_fundamental_array stores all the info needed to calculate scores
    for i in range(n): ## loop constituents
        temp = {'Constituent':constituents_list[i],'Table':'Current fundamental ranking', 'Current fundamental total score':sum(current_fundamental_array[i,:]),'Status':'active','Date':str(datetime.date.today())}
        score_dict = {str(current_values_list[j]):int(current_fundamental_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        current_fundamental_board=current_fundamental_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
        #columnsTitles =  ['Constituent','Current fundamental total score','Current ROCE','Current sales in Mio','Current profit margin','Current PER','Current EPS','Current EBITDA in Mio']
        current_fundamental_board= current_fundamental_board.sort_values('Current fundamental total score',axis=0, ascending=False).reset_index(drop=True)
    return current_fundamental_board


def status_update(args):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_store_scores]
    collection.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)


def store_result(args,result_df):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_store_scores]
    json_file = json.loads(result_df.to_json(orient='records'))
    collection.insert_many(json_file)



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_fundamental_analysis', help='The collection from which fundamental analysis is extracted')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('collection_store_scores', help='The collection where the fundamental scoring result will be stored')
    
    args = parser.parse_args()
    
    fundamental_ranking_main(args)