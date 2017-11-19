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

#!python Igenie/dax_project/fundamental_kefei/standard_deviation_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'standard deviation analysis'


def standard_dev_main(args):
    standard_dev_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        his = get_historical_price(args,constituent)
        
        #print constituent
        above,below,standard_dev=Bollinger(his)
        std_3yrs = standard_dev[-756:].mean()
        std_1yr = standard_dev[-252:].mean()
        
        ##Set a parameter to measure the stability of the stocks for the last 18 months
        standard_dev_table = standard_dev_table.append(pd.DataFrame({'Constituent': constituent,'Last 12 months':round(std_1yr,2),'Last 3 years':round(std_3yrs,2),'Table': 'standard deviation analysis','Date':str(datetime.date.today()),'Status':"active"},index=[0]),ignore_index=True)
    status_update(args)
    status_update(args)
    store_result(args, standard_dev_table)
    print 'done'


def Bollinger(his):
    #standard_dev = his['closing_price'].rolling(window=21,center=False).std()
    standard_dev = pd.rolling_std(his['closing_price'],window=21,center=False)
    upper = pd.rolling_mean(his['closing_price'],window=21,center=False) + standard_dev*2.0
    lower = pd.rolling_mean(his['closing_price'],window=21,center=False) - standard_dev*2.0
    
    #upper = his['closing_price'].rolling(window=21,center=False).mean() + standard_dev*2.0
    #lower = his['closing_price'].rolling(window=21,center=False).mean() - standard_dev*2.0
    
    ##Sport extreme values,record the number of times they happen.
    above = (his['closing_price']>=upper)
    below = (his['closing_price']<=lower)
    above_dates = his.loc[above, 'date']
    below_dates = his.loc[below,'date']
    n_above = above_dates.shape[0]
    n_below = below_dates.shape[0]
    return n_above,n_below,standard_dev


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
    
    standard_dev_main(args)