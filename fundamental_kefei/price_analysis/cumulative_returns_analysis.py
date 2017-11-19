# -*- coding: utf-8 -*-
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import json
import sys

#!python Igenie/dax_project/fundamental_kefei/cumulative_returns_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'cumulative return analysis'

def cumulative_returns_main(args):
    cumulative_returns_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        his = get_historical_price(args,constituent)
        return_6months, return_1year,return_3years,score = cumulative_returns_analysis(his)
    #append the result of the cumulative analysis result into a table
        cumulative_returns_table = cumulative_returns_table.append(pd.DataFrame({'Constituent': constituent, '6 months return': return_6months, '1 year return':return_1year,'3 years return': return_3years,'Cumulative return consistency score':score,'Table':'cumulative return analysis','Date':str(datetime.date.today()),'Status':'active'}, index=[0]), ignore_index=True)
    
    #cumulative_returns_json = json.loads(cumulative_returns_table.to_json(orient='records'))
    
    #update the source before storing new data
    status_update(args)
    store_result(args,cumulative_returns_table)
    #print cumulative_returns_json
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=cumulative_returns_json)


#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_historical_price(args,constituent):
    print constituent
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_get_price]
    his = collection.find({"constituent":constituent})
    his = pd.DataFrame(list(his))
    his = his.iloc[::-1] #order the data by date in asending order. 
    return his



def cumulative_returns_analysis(his):     
    ##Compute the 21-days moving average of the closing price. 
    #his_rm21=his['closing_price'].rolling(window=21,center=False).mean()
    his_rm21=pd.rolling_mean(his['closing_price'],window=21,center=False)
    his_6months = his_rm21.iloc[-126:].reset_index(drop=True)
    #print float(his_6months.iloc[-1])
    his_1year = his_rm21.iloc[-252:].reset_index(drop=True)
    #print float(his_1year.iloc[-1])
    his_3years = his_rm21.iloc[-756:].reset_index(drop=True)
        
    ##Calculate the cumulative returns
    return_6months =  (float(his_6months.iloc[-1])/float(his_6months.iloc[0]))-1.0
    return_1year =  (float(his_1year.iloc[-1])/float(his_1year.iloc[0]))-1.0
    return_3years =  (float(his_3years.iloc[-1])/float(his_3years.iloc[0]))-1.0
        
##Develop scoring system for cumulative returns
    if (return_6months >0)&(return_1year>0)&(return_3years>0):
        score = 4
    elif ((return_6months >0)&(return_3years>0)) or (return_6months >0)&(return_1year>0):
        score = 3
    elif (return_3years>0)&(return_1year >0):
        score = 2
    elif (return_3years>0) or (return_1year >0) or (return_6months >0):
        score = 1
    else: 
        score = 0
    return return_6months, return_1year,return_3years,score



#this makes all the out-dated data in the collection 'inactive'
##alter the status of collection
def status_update(args):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_store_analysis]
    collection.update_many({'Table':args.table_store_analysis,'status':'active'}, {'$set': {'status': 'inactive'}},True,True)


def store_result(args,result_df):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_store_analysis]
    json_file = json.loads(result_df.to_json(orient='records'))
    collection.insert_many(json_file)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The directory connection string') 
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_price', help='The collection from which historical price is exracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    cumulative_returns_main(args)