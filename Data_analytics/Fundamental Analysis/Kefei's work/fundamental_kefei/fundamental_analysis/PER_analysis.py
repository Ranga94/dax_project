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


#!python PER_analysis.py  'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'company_data_bkp' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' 'PER analysis'

#'Volkswagen (VW) vz' not found
def PER_main(args):
    PER_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        master = get_master_data(args,constituent)
        current_PER, last_year_PER,four_years_ago_PER,pct_last_year,pct_four_years,score = PER_calculation(master)
        PER_table = PER_table.append(pd.DataFrame({'Constituent': constituent, 'Current PER':current_PER,'PER last year':last_year_PER,'% change in PER from last year':pct_last_year,'PER 4 years ago': four_years_ago_PER,'% change in PER from 4 years ago':pct_last_year,'PER score':score,'Table':'PER analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    
    status_update(args)
    store_result(args, PER_table)
    
    #status_update(args)
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=quarter_mean_json)


def PER_calculation(master):
    PER_df = master[['PER','year']].dropna(thresh=2)
    PER_df['PER'] = PER_df['PER'].str.replace(",","").astype(float)
    current_PER = float(PER_df['PER'].iloc[-1])
    last_year_PER = float(PER_df['PER'].iloc[-2])
    four_years_ago_PER = float(PER_df['PER'].iloc[-4])
    pct_last_year = (current_PER-last_year_PER)*100.0/last_year_PER
    pct_four_years = (current_PER-four_years_ago_PER)*100.0/four_years_ago_PER
        
    if (pct_last_year>0) & (pct_four_years>0):
        score = 2
    elif pct_last_year>0:
        score =1
    else: 
        score = 0
    return current_PER, last_year_PER,four_years_ago_PER,pct_last_year,pct_four_years,score

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


def store_result(args,result_df):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_store_analysis]
    json_file = json.loads(result_df.to_json(orient='records'))
    collection.insert_many(json_file)




if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_master', help='The collection from which company data is extracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    PER_main(args)