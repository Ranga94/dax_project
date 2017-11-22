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


#python EBITDA_analysis.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'company_data_bkp' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' 'EBITDA analysis'

def EBITDA_main(args):
    #No data avaliable for'Volkswagen (VW) vz', Commerzbank, Deutsche Bank
    EBITDA_table=pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        master = get_master_data(args,constituent)
        current_EBITDA,last_year_EBITDA,four_years_ago_EBITDA,pct_last_year,pct_four_years,score=EBITDA_collection(master)
        EBITDA_table = EBITDA_table.append(pd.DataFrame({'Constituent': constituent, 'Current EBITDA in Mio':current_EBITDA,'EBITDA last year in Mio':last_year_EBITDA, '% change in EBITDA from last year': round(pct_last_year,2),'EBITDA score': score,'EBITDA 4 years ago in Mio': four_years_ago_EBITDA,'% change in EBITDA from 4 years ago':round(pct_four_years,2),'Table':'EBITDA analysis','Date':str(datetime.date.today()),'Status':"active" }, index=[0]), ignore_index=True)
    status_update(args)
    store_result(args,EBITDA_table)
    #status_update(args)
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=quarter_mean_json)


def EBITDA_collection(master):
    EBITDA = master[['EBITDA in Mio','year']].dropna(thresh=2)
    EBITDA["EBITDA in Mio"] = EBITDA["EBITDA in Mio"].str.replace(",","").astype(float)
    current_EBITDA = float(EBITDA['EBITDA in Mio'].iloc[-1])
    last_year_EBITDA = float(EBITDA['EBITDA in Mio'].iloc[-2])
    four_years_ago_EBITDA = float(EBITDA['EBITDA in Mio'].iloc[-4])
    pct_last_year = (current_EBITDA-last_year_EBITDA)*100.0/last_year_EBITDA
    pct_four_years = (current_EBITDA-four_years_ago_EBITDA)*100.0/four_years_ago_EBITDA
    if (pct_last_year>0) & (pct_four_years>0):
        score = 2
    elif pct_last_year>0:
        score =1
    else: 
        score = 0
    return current_EBITDA,last_year_EBITDA,four_years_ago_EBITDA,pct_last_year,pct_four_years,score


#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_master_data(args,constituent):
    print constituent
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection = db[args.collection_get_master]
    master = collection.find({"constituent":constituent})
    master = pd.DataFrame(list(master))
    return master

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
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_master', help='The collection from which company data is extracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    EBITDA_main(args)