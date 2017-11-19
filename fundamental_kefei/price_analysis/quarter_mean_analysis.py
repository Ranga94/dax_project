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


#!python Igenie/dax_project/fundamental_kefei/quarter_mean_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'quarterly growth analysis'


def quarter_mean_main(args): 
    quarter_mean_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        his = get_historical_price(args,constituent)
        a,b,a_3yrs,b_3yrs,a_1yr,b_1yr,quarter_mean,score = quarter_mean_analysis(his)
        quarter_mean_table = quarter_mean_table.append(pd.DataFrame({'Constituent': constituent, 'Current Quarter mean price':round(quarter_mean[-1],2),'Rate of change in price from 2010/quarter': round(a,2), 'Rate of change in price in the last 3 years/quarter':round(a_3yrs,2),'Rate of change in price in the last 365 days/quarter': round(a_1yr,2),'Quarterly growth consistency score':score,'Table':'quarterly growth analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    
    #quarter_mean_json  = json.loads(quarter_mean_table.to_json(orient='records'))
    
    #update the source before storing new data
    status_update(args)
    store_result(args,quarter_mean_table)
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=quarter_mean_json)



#Computes the mean rate of price growth per quarter for one particular constituent
def quarter_mean_analysis(his):
    #Analyse the cumulative return of the stock price after the recession in 2009. Quarterly. 
    his_2010 = his[['closing_price','date']].loc[his['date']>=datetime.datetime(2010,01,01)]
    ##Calulate the mean stock price for every quarter
    n=his_2010.shape[0]
    num_quarters = int(n/63.0)
    quarter_mean = np.zeros(num_quarters)
    
    for i in range(num_quarters): 
        if i<=num_quarters-1:
            quarter_mean[i]=float(his_2010['closing_price'].iloc[63*i:63*(i+1)].mean())
        else: 
            quarter_mean[i]=float(his_2010['closing_price'].iloc[63*i:].mean())
    
    #Use linear to estimate the growth of stock price: y=ax+b
    z = np.polyfit(range(num_quarters),quarter_mean,1) #fitting for all data
    z_3yrs =np.polyfit(range(12), quarter_mean[-12:],1) #fitting for the last 3 years data
    z_1yr = np.polyfit(range(4), quarter_mean[-4:],1) #fitting for the last 1 year data
    
    
    if (z_1yr[0]>0)&(z_3yrs[0]>0)&(z[0]>0):
        score =4
    elif (z_1yr[0]>0)&(z[0]>0):
        score = 3
    elif ((z_1yr[0]>0)&(z_3yrs[0]>0)) or ((z[0]>0)&(z_3yrs[0]>0)):
        score = 2
    elif (z_1yr[0]>0) or (z[0]>0):
        score = 1
    else:
        score = 0
        
    return z[0],z[1],z_3yrs[0],z_3yrs[1],z_1yr[0],z_1yr[1],quarter_mean,score


#this obtains the historical price data from source for one constituent. 
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
    parser.add_argument('table_store_analysis', help='Name of table for storing the analysis')

    args = parser.parse_args()
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    quarter_mean_main(args)