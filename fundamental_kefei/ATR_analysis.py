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

#!python Igenie/dax_project/fundamental_kefei/ATR_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'ATR analysis'

##Record the current ATR, the average ATR of this year, the average ATR in the last 5 years
def ATR_main(args):
    ATR_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        his = get_historical_price(args,constituent)
        ATR_array = ATR_calculate(his)
        ATR_table = ATR_table.append(pd.DataFrame({'Constituent': constituent,'Current 14-day ATR': round(ATR_array[-1],2), 'Average ATR in the last 12 months': round(ATR_array[-252:].mean(),2), 'Average ATR in the last 3 years':round(ATR_array[-756:].mean(),2),'Table':'ATR analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    
    return ATR_table


def ATR_calculate(his):
    TR = his['daily_high'].iloc[0:14]-his['daily_low'].iloc[0:14]
    ATR0 = TR.mean()
    n = his.shape[0]
    ATR_array = np.zeros(n)
    ATR_array[13]=ATR0
    for i in np.arange(14,n):
        ATR = (his['daily_high'].iloc[i] - his['daily_low'].iloc[i] + ATR0 * 13)/14.0
        ATR_array[i] = ATR
        ATR0 = ATR
    return ATR_array



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
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    ATR_main(args)