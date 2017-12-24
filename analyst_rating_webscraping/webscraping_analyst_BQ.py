# -*- coding: utf-8 -*-
import pandas as pd
import pymongo
import re
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
from datetime import datetime
import matplotlib.pyplot as plt
import pylab
import scipy
from bs4 import BeautifulSoup
import urllib
import os
from sqlalchemy import *
import json
import sys
import argparse
from pymongo import MongoClient, errors
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from google.cloud import datastore
from google.cloud import bigquery

#python webscraping_analyst_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'igenie-project-key.json' 0 'pecten_dataset_test.analyst_opinions_t'

#Write a function that reads from the parameter table, and extract analyst data for all stocks

def main(args):
    #sql_string = 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project'
    parameter = get_parameter_table(args)
    bi_analyst_table = analyst_businessinsider(parameter)
    
    #Insert this data into BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    update_result(args)
    bi_analyst_table.to_gbq(args.table_storage, project_id = 'igenie-project', chunksize=10000, verbose=True, reauth=False, if_exists='append',private_key=None)
    


def analyst_businessinsider(parameter): 
    analyst_opinion_table = pd.DataFrame()
        
    for i in range(len(parameter)):
        url = parameter['ANALYST_SOURCE_URL'].iloc[i]
        
        if parameter['CONSTITUENT_NAME'].iloc[i]=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif parameter['CONSTITUENT_NAME'].iloc[i]=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'
        else:
            constituent = parameter['CONSTITUENT_NAME'].iloc[i]
        
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]
        #url = 'http://markets.businessinsider.com/analyst/'+constituents_dict[constituent]
        
        print constituent
        r = urllib.urlopen(url).read()
        soup = BeautifulSoup(r,'lxml')
        rating_extract = soup.find_all("div",class_="rating")
        rating = float(rating_extract[0].text)
        
        #Obtain the data inside the table as a resultset (list) and extract the text. 
        opinions = soup.find_all("td",class_=["bar buy",'bar overweight',"bar hold","bar underweight","bar sell"])
        opinions_data = [int(x.text) for x in opinions]
        buy_count=opinions_data[0]+opinions_data[1]
        hold_count=opinions_data[2]
        sell_count=opinions_data[3]+opinions_data[4]
        total = buy_count+hold_count+ sell_count
    
        #Find analyst target for stock prices
        letters = soup.find_all("table",class_='table table-small no-margin-bottom')
        letters2 = letters[0].find_all("td")
        target_list = [str(x.text.strip()) for x in letters2]

        #Extract the prices. 
        median_target = round(float(target_list[5].replace("EUR","")),2)
        highest_target = round(float(target_list[7].replace("EUR","")),2)
        lowest_target = round(float(target_list[9].replace("EUR","")),2)

                             
        #Allocate a status according to the rating
        if rating <= 2:
            rating_result='Strong buy'
        elif rating <= 2.8:
            rating_result = 'Moderate buy'
        elif rating <= 3.2:
            rating_result = 'Hold'
        elif rating <=4:
            rating_result = 'Moderate sell'
        else: 
            rating_result = 'Strong sell'
        
        #If any data extracted is empty, label it NaN
        date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
        analyst_opinion_table = analyst_opinion_table.append(pd.DataFrame({'Constituent':constituent,'Constituent_name':constituent_name, 'Constituent_id':constituent_id,'Analyst_rating': rating, 'Analyst_recommendation': rating_result,'Buy':buy_count,'Hold':hold_count,'Sell':sell_count,'Buy_percentage':round(buy_count*1.0/total,3),'Hold_percentage':round(hold_count*1.0/total,3),'Sell_percentage':round(sell_count*1.0/total,3),'Median_target_price':median_target, 'Highest_target_price':highest_target,'Lowest_target_price':lowest_target,'Date':date,'Table':'Analyst opinions','Status':'active'},index=[0],),ignore_index=True)
    columnsTitles = ['Constituent','Constituent_name','Constituent_id','Analyst_rating','Analyst_recommendation', 'Buy','Hold','Sell','Buy_percentage','Hold_percentage','Sell_percentage','Median_target_price','Highest_target_price','Lowest_target_price','Table','Status','Date']
    analyst_opinion_table =analyst_opinion_table.reindex(columns=columnsTitles)
    return analyst_opinion_table


def display_parameter(args):
    if args.display_parameter ==0: #Then the new data is not displayed on dashboard
        status = 'inactive'
    else: #Want the new data to be displayed on dashboard, hence set status 'active' and make sure older data is inactive
        status = 'active'
    return status
    

def get_parameter_table(args):
    #parameter =  get_sql_data_text_query(sql_string, 'SELECT * FROM PARAM_ANALYST_COLLECTION WHERE SOURCE_NAME="Business Insider";')
    parameter = pd.read_sql('SELECT * FROM PARAM_ANALYST_COLLECTION WHERE SOURCE_NAME="Business Insider"', con=args.sql_connection_string)
    return parameter 


def update_result(args):
    table_store = args.table_storage
    
    #storage = Storage(google_key_path='/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
    storage = Storage(google_key_path=args.service_key_path )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def get_constituent_id_name(old_constituent_name):
    mapping = {}
    mapping["BMW"] = ("BMWDE8170003036" , "BAYERISCHE MOTOREN WERKE AG")
    mapping["Allianz"] = ("ALVDEFEI1007380" , "ALLIANZ SE")
    mapping["Commerzbank"] = ("CBKDEFEB13190" , "COMMERZBANK AKTIENGESELLSCHAFT")
    mapping["adidas"] = ("ADSDE8190216927", "ADIDAS AG")
    mapping["Deutsche Bank"] = ("DBKDEFEB13216" , "DEUTSCHE BANK AG")
    mapping["EON"] = ("EOANDE5050056484" , "E.ON SE")
    mapping["Lufthansa"] = ("LHADE5190000974" ,"DEUTSCHE LUFTHANSA AG")
    mapping["Continental"] = ("CONDE2190001578" , "CONTINENTAL AG")
    mapping["Daimler"] = ("DAIDE7330530056" , "DAIMLER AG")
    mapping["Siemens"] = ("SIEDE2010000581" , "SIEMENS AG")
    mapping["BASF"] = ("BASDE7150000030" , "BASF SE")
    mapping["Bayer"] = ("BAYNDE5330000056" , "BAYER AG")
    mapping["Beiersdorf"] = ("BEIDE2150000164" , "BEIERSDORF AG")
    mapping["Deutsche Börse"] = ("DB1DEFEB54555" , "DEUTSCHE BOERSE AG")
    mapping["Deutsche Post"] = ("DPWDE5030147191" , "DEUTSCHE POST AG")
    mapping["Deutsche Telekom"] = ("DTEDE5030147137" , "DEUTSCHE TELEKOM AG")
    mapping["Fresenius"] = ("FREDE6290014544" , "FRESENIUS SE & CO.KGAA")
    mapping["HeidelbergCement"] = ("HEIDE7050000100" , "HEIDELBERGCEMENT AG")
    mapping["Henkel vz"] = ("HEN3DE5050001329" , "HENKEL AG & CO. KGAA")
    mapping["Infineon"] = ("IFXDE8330359160" , "INFINEON TECHNOLOGIES AG")
    mapping["Linde"] = ("LINDE8170014684" , "LINDE AG")
    mapping["Merck"] = ("MRKDE6050108507" , "MERCK KGAA")
    mapping["ProSiebenSat1 Media"] = ("PSMDE8330261794" , "PROSIEBENSAT.1 MEDIA SE")
    mapping["RWE"] = ("RWEDE5110206610" , "RWE AG")
    mapping["SAP"] = ("SAPDE7050001788" , "SAP SE")
    mapping["thyssenkrupp"] = ("TKADE5110216866" , "THYSSENKRUPP AG")
    mapping["Vonovia"] = ("VNADE5050438829" , "VONOVIA SE")
    mapping["DAX"] = ("DAX", "DAX")
    mapping["Fresenius Medical Care"] = ("FMEDE8110066557" , "FRESENIUS MEDICAL CARE AG & CO.KGAA")
    mapping["Volkswagen (VW) vz"] = ("VOW3DE2070000543" , "VOLKSWAGEN AG")
    mapping["Münchener Rückversicherungs-Gesellschaft"] = ("MUV2DEFEI1007130" , "MUNCHENER RUCKVERSICHERUNGS - GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN")

    if old_constituent_name in mapping:
        return mapping[old_constituent_name]
    else:
        return old_constituent_name
  
  
class Storage:
    def __init__(self, google_key_path=None, mongo_connection_string=None):
        if google_key_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
            self.bigquery_client = bigquery.Client()
        else:
            self.bigquery_client = None

        if mongo_connection_string:
            self.mongo_client = MongoClient(mongo_connection_string)
        else:
            self.mongo_client = None
            
    def get_bigquery_data(self, query, timeout=None, iterator_flag=True): 
        if self.bigquery_client:
            client = self.bigquery_client
        else:
            client = bigquery.Client()

        print("Running query...")
        query_job = client.query(query)
        iterator = query_job.result(timeout=timeout)

        if iterator_flag:
            return iterator
        else:
            return list(iterator)

    

if __name__ == "__main__":
    #Hard-codings to be removed: constituents, mongdbconnection, table to store the results for. 
    parser = argparse.ArgumentParser()
    #parser.add_argument('python_path', help='The connection string') #directory of script
    parser.add_argument('sql_connection_string', help='The connection string to mysql') #mongodb connection string
    parser.add_argument('service_key_path',help='google service key path')
    parser.add_argument('display_parameter',help='The parameter that decides if the new analyst data will be displayed on dashboard, 1 for yes, 0 for no')
    parser.add_argument('table_storage',help='BigQuery table where the new data is stored')
    args = parser.parse_args()
    main(args)
