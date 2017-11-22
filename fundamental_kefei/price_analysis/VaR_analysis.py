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
from scipy.stats import norm,t
from scipy.stats.distributions import binom
from scipy.stats import skew, kurtosis, kurtosistest


#python VaR_analysis.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'VaR analysis'


def VaR_main(args):
    #Set parameters for calculating VaR on monthly-return at a confidence level 99%
    n=21
    alpha=0.01
    
    VaR_table = pd.DataFrame()
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        his = get_historical_price(args,constituent)
        mean_return,mean_standard_dev, VaR_t= student_VAR_calculate(his,n,alpha)
        VaR_table = VaR_table.append(pd.DataFrame({'Constituent': constituent,'Investment period': n ,'Average return': mean_return, 'Standard deviation':mean_standard_dev,'Confidence level': alpha,'Value at Risk': VaR_t,'Table':'VaR analysis','Date':str(datetime.date.today()),'Status':'active'}, index=[0]), ignore_index=True)
    status_update(args)
    store_result(args,VaR_table)
    return VaR_table


def student_VAR_calculate(his,n,alpha):
    ##Calculate the daily returns of stock
    ret = (his['closing_price']-his['closing_price'].shift(n)*1.0)/his['closing_price'].shift(n)
    ##Normal distribution best fit, mu_norm=mean, sig_norm=standard deviation
    mu_norm, sig_norm = norm.fit(ret[n:].values)

    # Student t distribution best fit (finding: nu, which is degrees of freedom)
    parm = t.fit(ret[n:].values)
    nu, mu_t, sig_t = parm
    nu = np.round(nu)

    #Set parameters for calculating value at risk from the distribution
    h = 1 
    lev = 100*(1-alpha)
    xanu = t.ppf(alpha, nu)
 
    VaR_t = np.sqrt((nu-2)/nu) * t.ppf(1-alpha, nu)*sig_norm  - h*mu_norm
    #CVaR_t = -1/alpha * (1-nu)**(-1) * (nu-2+xanu**2) * t.pdf(xanu, nu)*sig_norm  - h*mu_norm

    mean_return = mu_norm*n
    mean_standard_dev = sig_norm * np.sqrt(n)
    
    return mean_return,mean_standard_dev, VaR_t



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
    parser.add_argument('collection_get_price', help='The collection from which historical price is exracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    
    VaR_main(args)