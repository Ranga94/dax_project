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


#!python Igenie/dax_project/fundamental_kefei/profit_margin_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://admin:admin@ds019654.mlab.com:19654/dax' 'dax' 'company_data' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' 'profit margin analysis'


def profit_margin_calculator(master):
    sales = master[['Sales in Mio','year']].dropna(thresh=2)
    net_profit=master[['Net profit','year']].dropna(thresh=2)
    sales['Sales in Mio']=sales['Sales in Mio'].str.replace(",","").astype(float)
    net_profit['Net profit']=net_profit['Net profit'].str.replace(",","").astype(float)
    #profit_margin_table = net_profit.merge(sales,on='year',how='inner')
    profit_margin_calculation = [float(net_profit['Net profit'].iloc[i])*100.0/float(sales['Sales in Mio'].iloc[i]) for i in range (sales.shape[0])]
    current_pm = round(profit_margin_calculation[-1],2)
    pm_last_year=round(profit_margin_calculation[-2],2)
    pm_four_years_ago=round(profit_margin_calculation[-4],2)
    pct_last_year=(current_pm -pm_last_year)*100.0/pm_last_year
    pct_four_years_ago=(current_pm -pm_four_years_ago)*100.0/pm_four_years_ago
          
    if (pct_last_year>0) & (pct_four_years_ago>0):
        score = 2
    elif pct_last_year>0:
        score =1
    else: 
        score = 0
    
    return profit_margin_calculation,current_pm,pm_last_year,pm_four_years_ago,pct_last_year,pct_four_years_ago,score


def profit_margin_main(args):
    profit_margin_table = pd.DataFrame()
    #'Volkswagen (VW) vz' does not receive any data on profit margin
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        master=get_master_data(args,constituent)
        profit_margin_calculation,current_pm,pm_last_year,pm_four_years_ago,pct_last_year,pct_four_years_ago,score = profit_margin_calculator(master)
        profit_margin_table = profit_margin_table.append(pd.DataFrame({'Constituent': constituent, 'Current profit margin':current_pm,'Profit margin last year':pm_last_year,'% change in profit margin last year':pct_last_year,'Profit margin 4 years ago': pm_four_years_ago,'% change in profit margin 4 years ago':pct_four_years_ago,'Table':'profit margin analysis','Profit margin score':score,'Date':str(datetime.date.today()),'Status':"active" }, index=[0]), ignore_index=True)
    #return profit_margin_table
    #status_update(args)
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=quarter_mean_json)
   


#this obtains the master data as a pandas dataframe from source for one constituent. 
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
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_master', help='The collection from which company data is extracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    profit_margin_main(args)