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
import operator

#!python Igenie/dax_project/fundamental_kefei/dividend_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'mongodb://admin:admin@ds019654.mlab.com:19654/dax' 'dax_gcp' 'dax' 'historical' 'company_data' 'fundamental analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Continental','Daimler','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media' 'dividend analysis'

##This dividend table stores the results of linear regression, and the list of years when dividends are offered
##'Commerzbank','Deutsche Bank','Lufthansa','RWE','thyssenkrupp','Vonovia','Volkswagen (VW) vz'may not receive dividend yields
def dividend_main(args):
    dividend_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        div = get_dividend_data(args,constituent)
        his = get_historical_price(args,constituent)
        current_price = float(his['closing_price'].iloc[0])
        a,b,mse,year_list,current_div,future_div,score=dividend_analysis(div) #a represents the average growth of dividend per year in €
        if constituent == 'BMW' or 'Volkswagen (VW) vz' or 'RWE':
            a = a*2.0 #Calculate the annual dividend growth for stocks that release dividend every 6 months
        if constituent != 'ProSiebenSat1 Media':
            estimated_return = Gordon_Growth_estimated_return(a,current_price,current_div,future_div)
        else:
            estimated_return = 'NaN'
        
        dividend_table = dividend_table.append(pd.DataFrame({'Constituent': constituent, 'Current dividend': current_div, 'Average rate of dividend growth /year':round(a,2),'Mean square error of fitting': round(mse,2),'Years of dividend offer':'%s'%year_list,'Estimated dividend next year':future_div,'Current share price':current_price,'Gordon growth estimated return':estimated_return,'Dividend consistency score':score,'Table':'dividend analysis','Status':"active",'Date':str(datetime.date.today())}, index=[0]), ignore_index=True)
    
    status_update(args)
    store_result(args, dividend_table)
    #print dividend_table
    #return dividend_table
    #status_update(args)
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=quarter_mean_json)
   

#Computes the linear regression model for dividend, and produce list of years where dividend is offered. 
def dividend_analysis(div):
    div = div[['Value','Last Dividend Payment']].dropna(thresh=1)
    div = pd.DataFrame(div)
    value = [unicode(x) for x in div["Value"]]
    value = [x.replace(u'\u20ac',"") for x in value]
    value = [float(x) for x in value]
    n=len(value)
    current_div = value[0] 
    
    #Estimate growth of dividend using linear fitting
    z = np.polyfit(range(n),value[::-1],1)
    estimation = [x*z[0]+z[1] for x in range(n)]
    res = map(operator.sub, value[::-1], estimation)
    mse = sum([x**2 for x in res])/n*1.0
    
    ##Find out the years where dividend is offered
    #Volkswagen (VW) vz, BMW, RWE: half year
    date_list = pd.DatetimeIndex(div['Last Dividend Payment'])
    year_list = date_list.year
    
    score =0 #This scores the consistency of dividend issued each year. 
    if mse<0.5:
        score +=2
        future_div = current_div + z[0]
    else: 
        future_div ='n/a'
        score = score+0
    
    return z[0],z[1],mse,year_list,current_div,future_div,score


def Gordon_Growth_estimated_return(div_growth,current_price,current_div,future_div):
    estimated_return = future_div*1.0/current_price + div_growth/current_div
    return estimated_return

#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_dividend_data(args,constituent):
    print constituent
    client = MongoClient(args.connection_string_master)
    db = client[args.database_master]
    collection = db[args.collection_get_master]
    div = collection.find({"constituent":constituent,'table':'Dividend'})
    div = pd.DataFrame(list(div))
    return div


#this obtains the historical price data as a pandas dataframe from source for one constituent. 
def get_historical_price(args,constituent):
    client = MongoClient(args.connection_string_price)
    db = client[args.database_price]
    collection = db[args.collection_get_price]
    his = collection.find({"constituent":constituent})
    his = pd.DataFrame(list(his))
    his = his.iloc[::-1] #order the data by date in asending order.
    #print his.head() 
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
    parser.add_argument('connection_string_price', help='The mongodb connection string for price data')
    parser.add_argument('connection_string_master', help='The mongodb connection string for master data (dividend)')
    parser.add_argument('database_price',help='Name of the database where historical price is stored')
    parser.add_argument('database_master',help='Name of the database where master data (dividend) is stored')
    parser.add_argument('collection_get_price', help='The collection from which company data (dividend) is exracted')
    parser.add_argument('collection_get_master', help='The collection from which historical price is exracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    dividend_table = dividend_main(args)
