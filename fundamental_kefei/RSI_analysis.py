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

#!python Igenie/dax_project/fundamental_kefei/RSI_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'RSI analysis'

def RSI_main(args):
    RSI_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        his = get_historical_price(args,constituent)
        RSI_current,overbought_pct,oversold_pct,RSI_score = RSI_calculate(his,14)
        RSI_table = RSI_table.append(pd.DataFrame({'Current RSI':round(RSI_current,2),'% days overbought':round(overbought_pct*100,2),'% days oversold':round(oversold_pct*100,2),'RSI bull score':RSI_score,'Table':'RSI analysis','Date':str(datetime.date.today()),'Status':'active'},index=[0]),ignore_index=True)
    print 'done'
    #print cumulative_returns_json
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=cumulative_returns_json)


def RSI_calculate(his,n):
    delta = his['closing_price'].diff()
    dUp, dDown = delta.copy(), delta.copy()
    dUp[dUp < 0] = 0
    dDown[dDown > 0] = 0   
    RolUp = pd.rolling_mean(dUp,window=21,center=False) 
    RolDown = pd.rolling_mean(dDown,window=21,center=False).abs()
    #RolUp = dUp.rolling(window=n).mean()
    #RolDown = dDown.rolling(window=n).mean().abs()
    RS = RolUp/RolDown+0.0
    a=RS.shape[0]
    RSI = np.zeros(a)
    for i in np.arange(n,a):
        RSI[i] = 100-100/(1.0+RS[i])
    #If > 70: overbought signal, <30: oversold signal
    RSI_last_year = RSI[-252:]
    #print RSI[-90:]
    overbought = (RSI_last_year>=70)
    oversold = (RSI_last_year<=30)
    overbought_count = RSI_last_year[overbought].shape[0]
    oversold_count = RSI_last_year[oversold].shape[0]
    
    #Indicating the bullish signal
    if RSI[-1] > 70:
        RSI_score = 1
    elif RSI[-1] < 30:
        RSI_score = -1
    else: 
        RSI_score =0
        
    if overbought_count>oversold_count:
        RSI_score = RSI_score+1
    else: 
        RSI_score=RSI_score
        
    return RSI[-1],overbought_count/252.0,oversold_count/252.0,RSI_score


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
    
    RSI_main(args)