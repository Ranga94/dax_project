# -*- coding: utf-8 -*-

##This script does ranking for technical/price analysis
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

#python Price_ranking.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'price analysis'  -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'Price ranking'

def price_ranking_main(args):
    cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,market_signal_table,dividend_table,VaR_table,RSI_table=price_analysis_collection(args)
    
    table_list = [cumulative_returns_table,cumulative_returns_table,quarter_mean_table,quarter_mean_table,RSI_table]
    value_list = ['1 year return','3 years return','Rate of change in price in the last 365 days/quarter','Rate of change in price in the last 3 years/quarter','Current RSI']
   
    ##Make a ranking for price growth
    ##Collect the statistics across all DAX-30 constituents first
    price_stats_table=price_growth_stats(args,table_list,value_list)
    ##Make a scoreboard. 
    price_growth_board =price_growth_scoring(args,price_stats_table,table_list,value_list)
    
    ##Update the collection
    status_update(args)
    store_result(args,price_growth_board)
    
    print "insert done"

def price_analysis_collection(args): 
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_get_price_analysis]
    cumulative_returns_table = pd.DataFrame(list(collection.find({'Table':'cumulative return analysis','Status':'active'})))
    quarter_mean_table = pd.DataFrame(list(collection.find({'Table':'quarterly growth analysis','Status':'active'})))
    standard_dev_table = pd.DataFrame(list(collection.find({'Table':'standard deviation analysis','Status':'active'})))
    ATR_table = pd.DataFrame(list(collection.find({'Table':'ATR analysis','Status':'active'})))
    RSI_table = pd.DataFrame(list(collection.find({'Table':'RSI analysis','Status':'active'})))
    market_signal_table = pd.DataFrame(list(collection.find({'Table':'Market signal','Status':'active'})))
    dividend_table = pd.DataFrame(list(collection.find({'Table':'dividend analysis','Status':'active'})))
    VaR_table = pd.DataFrame(list(collection.find({'Table':'VAR analysis','Status':'active'})))
    return cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,market_signal_table,dividend_table,VaR_table, RSI_table


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
        
        price_stats_table=price_stats_table.append(pd.DataFrame({'Price growth quantity':value_list[i],'Mean':mean,'Standard deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'Top lower-bound':top_lower, 'Good lower-bound':good_lower,'Fair lower-bound':fair_lower},index=[0]),ignore_index=True)
    return price_stats_table



## Scores the growth of price out of 30. 
def price_growth_scoring(args,stats_table,table_list,value_list):
    #price_table_list = [cumulative_returns_table,cumulative_returns_table,quarter_mean_table,quarter_mean_table,market_signal_table]
    price_growth_list = ['1 year return','3 years return','Rate of change in price in the last 365 days/quarter','Rate of change in price in the last 3 years/quarter','Current RSI']
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    m=len(value_list)
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    n=len(constituents_list)
    price_growth_board = pd.DataFrame()
    price_growth_array=np.zeros((n,m))
    
    for j in range(m): ##loop through fundamental quantities
        #array = np.zeros(n) #this array stores scores of one particular fundamental quantity for each constituent. 
        table = value_list[j]
        #print str(table)
        top_lower = float(stats_table['Top lower-bound'].loc[stats_table['Price growth quantity']==value_list[j]])
        good_lower = float(stats_table['Good lower-bound'].loc[stats_table['Price growth quantity']==value_list[j]])
        fair_lower = float(stats_table['Fair lower-bound'].loc[stats_table['Price growth quantity']==value_list[j]])
        
        print str(table)
    
        for i in range(n): ##loop through constituents
            constituent = constituents_list[i]
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
        temp = {'Constituent':constituents_list[i], 'Price growth score':sum(price_growth_array[i,:])}
        score_dict = {str(price_growth_list[j]):int(price_growth_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        price_growth_board=price_growth_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
    
    ## Append the consistency scores into the calculation of total price growth score
    CR_table = table_list[0]
    QM_table = table_list[2]
    price_growth_board = price_growth_board.merge(CR_table[['Constituent','Cumulative return consistency score']], on='Constituent',how='inner')
    price_growth_board = price_growth_board.merge(QM_table[['Constituent','Quarterly growth consistency score']], on='Constituent',how='inner')
    #price_growth_board = price_growth_board.merge(market_signal_table[['Constituent','Bull score (crossing)']],on='Constituent',how='inner')
    price_growth_board['Total price growth score']=price_growth_board['Cumulative return consistency score']+price_growth_board['Quarterly growth consistency score']+price_growth_board['Price growth score']
    columnsTitles = ['Constituent','Total price growth score','Price growth score','Cumulative return consistency score','Quarterly growth consistency score','1 year return','3 years return','Rate of change in price in the last 365 days/quarter','Rate of change in price in the last 3 years/quarter','Current RSI','Bull score (crossing)']
    price_growth_board=price_growth_board.reindex(columns=columnsTitles)
    price_growth_board['Status']='active'
    price_growth_board['Date']=str(datetime.date.today())
    price_growth_board['Table']='Price growth ranking'
    return price_growth_board



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
    parser.add_argument('collection_get_price_analysis', help='The collection from which fundamental analysis is extracted')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('collection_store_scores', help='The collection where the price scoring result will be stored')
   
    args = parser.parse_args()
    
    price_ranking_main(args)