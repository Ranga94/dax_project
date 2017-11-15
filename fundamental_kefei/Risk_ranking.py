# -*- coding: utf-8 -*-
##This script ranks the risk of stocks based on result from VaR Analysis. 
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

#!python Igenie/dax_project/fundamental_kefei/Risk_ranking.py '/Users/kefei/DIgenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','ProSiebenSat1 Media','Volkswagen (VW) vz' 'Risk scores'

#u'Deutsche B\xf6rse'
#u'M\xfcnchener R\xfcckversicherungs-Gesellschaft'
#Münchener Rückversicherungs-Gesellschaft'

def risk_main(args):
    VaR_table = VaR_collect(args)
    VaR_stats_table=VaR_stats(VaR_table)
    VaR_score_board = VaR_ranking(args,VaR_stats_table,VaR_table)
    print 'board done'
    
    ##Store all the score board in mongodb
    client = MongoClient(args.connection_string)
    db = client[args.database]
    
    ##Update the collection
    collection = db[args.collection_store_risk_scores]
    collection.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
    collection.update_many({'Status':'NaN'}, {'$set': {'Status': 'inactive'}},True,True)
    
    ##Insert result into database
    VaR_scores_json = json.loads(VaR_score_board.to_json(orient='records')) #upload the tagging
    collection.insert_many(VaR_scores_json)
    print 'all done'

def VaR_collect(args):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_get_var_analysis]
    VaR_table = pd.DataFrame(list(collection.find({'Status':'active','Table':'VAR analysis'})))
    return VaR_table

def VaR_stats(VaR_table):
    VaR_stats_table = pd.DataFrame()
    VaR_list = ['Average return','Value at Risk','Standard deviation']
    for i in range(len(VaR_list)): 
        table = VaR_list[i]
        mean = VaR_table[VaR_list[i]].mean()
        std_dev = VaR_table[VaR_list[i]].std()
        max_val = VaR_table[VaR_list[i]].max()
        min_val = VaR_table[VaR_list[i]].min()
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        high_lower = min(max_val,mean+std_dev)
        medium_lower=max(min(max_val,mean-std_dev*0.5),max(min_val,mean-std_dev*0.5))
        
        VaR_stats_table=VaR_stats_table.append(pd.DataFrame({'VaR quantity':VaR_list[i],'Mean':mean,'Standard deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'High lower-bound':high_lower, 'Medium lower-bound':medium_lower},index=[0]),ignore_index=True)
    return VaR_stats_table


##Allocate the risk score (out of 6) according to Value at Risk, higher the score, risker the stock. 
def VaR_ranking(args,VaR_stats_table,VaR_table):
    VaR_list = ['Value at Risk','Standard deviation']
    m=len(VaR_list)
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    n=len(constituents_list)
    var_score_board = pd.DataFrame()
    var_score_array = np.zeros((n,m))
    
    for j in range(m): ##loop through fundamental quantities
        high_lower = float(VaR_stats_table['High lower-bound'].loc[VaR_stats_table['VaR quantity']==VaR_list[j]])
        medium_lower = float(VaR_stats_table['Medium lower-bound'].loc[VaR_stats_table['VaR quantity']==VaR_list[j]])
        
        for i in range(n): ##loop through constituents
            constituent = constituents_list[i]
            if VaR_table[VaR_list[j]].loc[VaR_table['Constituent']==constituent].empty==False: 
                value = VaR_table[VaR_list[j]].loc[VaR_table['Constituent']==constituent].values[0]
                
                if value > high_lower:
                    score = 3 #high risk
                elif value > medium_lower:
                    score = 2 #medium risk
                else: 
                    score = 1 #low risk
                var_score_array[i,j]=score
            else: 
                print VaR_list[j]+'=N/A for '+constituent
                score=0
                var_score_array[i,j]=score
        
    for i in range(n): ## loop constituents
        temp = {'Constituent':constituents_list[i], 'Risk score':sum(var_score_array[i,:])}
        score_dict = {str(VaR_list[j]):int(var_score_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        var_score_board = var_score_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
    
    var_score_board = var_score_board.sort_values('Risk score',axis=0, ascending=True).reset_index(drop=True)
    var_score_board['Status']='active'
    var_score_board['Date'] = str(datetime.date.today())
    ## Append the consistency scores into the calculation of total price growth score
    return var_score_board



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The directory connection string') 
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_var_analysis', help='The collection from which VaR analysis is extracted')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('collection_store_risk_scores', help='The collection where the ranking result will be stored')

    args = parser.parse_args()
    
    sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    risk_main(args)
        