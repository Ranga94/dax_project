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


#!python sales_analysis.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'company_data_bkp' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' 'Sales analysis'


def sales_main(args):
    sales_coll_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
    #'Commerzbank', all debt NaN,Deutsche Bank',no data avaliable for 'Volkswagen (VW) vz'ranked last
        master = get_master_data(args,constituent)
        pct_sales_last_year, pct_sales_four_years, sales_table,score = sales_calculate(master)
        sales_coll_table = sales_coll_table.append(pd.DataFrame({'Constituent': constituent,'Current sales in Mio':round(sales_table.iloc[-1],2), '%change in Sales from previous year':round(pct_sales_last_year,2),'%change in Sales from 4 years ago':round(pct_sales_four_years,2),'Sales score':score,'Table':'Sales analysis','Date':str(datetime.date.today()),'Status':"active"},index=[0]),ignore_index=True)
        
    #store the analysis
    #storage = Storage()
    status_update(args)
    store_result(args, sales_coll_table)
    
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
    collection.update_many({'Table':args.table_store_analysis,'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)

def store_result(args,result_df):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_store_analysis]
    json_file = json.loads(result_df.to_json(orient='records'))
    collection.insert_many(json_file)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The mongodb connection string for collection and storage')
    parser.add_argument('database',help='Name of the database for collection')
    parser.add_argument('collection_get_master', help='The collection where the financial data is obtained')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for storing the analysis')

    args = parser.parse_args()
    sales_main(args)