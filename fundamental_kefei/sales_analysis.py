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


#!python Igenie/dax_project/fundamental_kefei/sales_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://admin:admin@ds019654.mlab.com:19654/dax' 'dax' 'company_data' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' 'Sales analysis'

def sales_main(args):
    sales_coll_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
    #'Commerzbank', all debt NaN,Deutsche Bank',no data avaliable for 'Volkswagen (VW) vz'ranked last
        master = get_master_data(args,constituent)
        pct_sales_last_year, pct_sales_four_years, sales_table,score = sales_calculate(master)
        sales_coll_table = sales_coll_table.append(pd.DataFrame({'Constituent': constituent,'Current sales in Mio':round(sales_table.iloc[-1],2), '%change in Sales from previous year':round(pct_sales_last_year,2),'%change in Sales from 4 years ago':round(pct_sales_four_years,2),'Sales score':score,'Table':'Sales analysis','Date':str(datetime.date.today()),'Status':"active"},index=[0]),ignore_index=True)
        
    status_update(args)
    #store the analysis
    #storage = Storage()
    sales_json = json.loads(sales_coll_table.to_json(orient='records'))
    Storage.storage.save_to_mongodb(connection_string=args.connection_string_upload, database=args.database_upload,collection=args.collection_store_analysis, data=sales_json)

def sales_calculate(master):
    table= master[['Sales in Mio','year']].dropna(thresh=2)
    #print table
    table['Sales in Mio']=table['Sales in Mio'].str.replace(",","").astype(float)
    #print float(table['Sales in Mio'].iloc[-1])
    pct_sales_last_year = 100*(float(table['Sales in Mio'].iloc[-1])-float(table['Sales in Mio'].iloc[-2]))/float(table['Sales in Mio'].iloc[-2])
    pct_sales_four_years = 100*(float(table['Sales in Mio'].iloc[-1])-float(table['Sales in Mio'].iloc[-4]))/float(table['Sales in Mio'].iloc[-4])
    
    if (pct_sales_last_year>0) & (pct_sales_four_years>0):
        score = 2
    elif pct_sales_last_year>0:
        score =1
    else: 
        score = 0
    return float(pct_sales_last_year), float(pct_sales_four_years), table['Sales in Mio'], score


#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_master_data(args,constituent):
    print constituent
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_get_master]
    master = collection.find({"constituent":constituent})
    master = pd.DataFrame(list(master))
    return master


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
    parser.add_argument('connection_string', help='The mongodb connection string for collection')
    parser.add_argument('database',help='Name of the database for collection')
    parser.add_argument('collection_get_master', help='The collection from which company data is extracted')
    parser.add_argument('connection_string_upload', help='The mongodb connection string for uploading result')
    parser.add_argument('database_upload',help='Name of the database for collection for uploadding result')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for storing the analysis')

    args = parser.parse_args()
    
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    
    sales_main(args)