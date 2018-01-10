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

#python ROCE_analysis.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' ‘dax_gcp’ 'company_data_bkp' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' "ROCE analysis"

def ROCE_main(args):
    ROCE_coll_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
    #'Commerzbank', all debt NaN,Deutsche Bank',no data avaliable for 'Volkswagen (VW) vz'ranked last
        master = get_master_data(args,constituent)
        pct_ROCE_last_year, pct_ROCE_four_years, ROCE_table,score = ROCE_calculate(master)
        ROCE_coll_table = ROCE_coll_table.append(pd.DataFrame({'Constituent': constituent, 'Current ROCE': round(ROCE_table['ROCE'].iloc[-1],2), '% change in ROCE from previous year':round(pct_ROCE_last_year,2),'% change in ROCE from 4 years ago': round(pct_ROCE_four_years,2),'ROCE score':score,'Table':'ROCE analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    status_update(args)
    store_result(args, ROCE_coll_table)


def ROCE_calculate(master):
    master = master[['EBITDA in Mio','Net debt in Mio','Total assetts in Mio','year']].dropna(thresh=2)
    net_profit = master[['EBITDA in Mio','year']].dropna(0,'any')
    net_debt = master[['Net debt in Mio','year']].dropna(0,'any')
    total_assets=master[['Total assetts in Mio','year']].dropna(0,'any')
    joined = pd.merge(pd.merge(net_profit,net_debt,on='year'),total_assets,on='year')
    joined["EBITDA in Mio"] = joined["EBITDA in Mio"].str.replace(",","").astype(float)
    joined['Net debt in Mio'] = joined['Net debt in Mio'].str.replace(",","").astype(float)
    joined['Total assetts in Mio'] = joined['Total assetts in Mio'].str.replace(",","").astype(float)
    joined['ROCE']=joined["EBITDA in Mio"]*100/(joined['Total assetts in Mio']-joined['Net debt in Mio'])
    #print joined
    pct_ROCE_last_year = 100*(float(joined['ROCE'].loc[joined['year']==2016])-float(joined['ROCE'].loc[joined['year']==2015]))/float(joined['ROCE'].loc[joined['year']== 2015])
    pct_ROCE_four_years = 100*(float(joined['ROCE'].loc[joined['year']==2016])-float(joined['ROCE'].loc[joined['year']==2013]))/float(joined['ROCE'].loc[joined['year']== 2013])
    
    if (pct_ROCE_last_year>0) & (pct_ROCE_four_years>0):
        score = 2
    elif pct_ROCE_last_year>0:
        score =1
    else: 
        score = 0 
    return float(pct_ROCE_last_year), float(pct_ROCE_four_years), joined[['ROCE','year']],score


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
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_master', help='The collection from which master is exracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    
    ROCE_main(args)