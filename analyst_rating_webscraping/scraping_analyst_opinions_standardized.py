# -*- coding: utf-8 -*-
import pandas as pd
import pymongo
import re
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pylab
import scipy
from bs4 import BeautifulSoup
import urllib


#Write a function that extract analyst data for all stocks
def analyst_businessinsider(constituents_dict): 
    analyst_opinion_table = pd.DataFrame()
        
    for constituent in constituents_dict:
        print constituent   
        #print constituent
        url = 'http://markets.businessinsider.com/analyst/'+constituents_dict[constituent]
        #url = 'http://markets.businessinsider.com/stock/'+constituents_dict[constituent]+'/analysts-opinions'
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
        
        analyst_opinion_table = analyst_opinion_table.append(pd.DataFrame({'Constituent':constituent,'Analyst rating': rating, 'Analyst recommendation': rating_result,'Buy':buy_count,'Hold':hold_count,'Sell':sell_count,'% Buy':round(buy_count*1.0/total,3),'% Hold':round(hold_count*1.0/total,3),'% Sell':round(sell_count*1.0/total,3),'Median target price':median_target, 'Highest target price':highest_target,'Lowest target price':lowest_target,'Date':str(datetime.date.today()),'Table':'Analyst opinions','Status':'active'},index=[0],),ignore_index=True)
    columnsTitles = ['Constituent','Analyst rating','Analyst recommendation', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Median target price','Highest target price','Lowest target price','Table','Status','Date']
    analyst_opinion_table =analyst_opinion_table.reindex(columns=columnsTitles)
    return analyst_opinion_table
    
def analyst_wallstreet():
    analyst_opinions_table = pd.DataFrame()
    constituent_dict = {'Adidas':'ADS','Commerzbank':'CBK','Deutsche Bank':'DBK', 'EON':'EOAN', 'BMW':'BMW'}
    for constituent in constituent_dict:
        url = 'http://quotes.wsj.com/DE/XFRA/'+constituent_dict[constituent]+'/research-ratings'
        r = urllib.urlopen(url).read()
        soup = BeautifulSoup(r,'lxml')
    
        ## Find analyst opinions
        letters = soup.find_all('div' ,class_='cr_analystRatings cr_data module')
        numbers = letters[0].find_all('span',class_='data_data')
        num_array = [float(x.text) for x in numbers]
        opinions_data = [num_array[i*3+2] for i in range(5)]
        buy_count=opinions_data[0]+opinions_data[1]
        hold_count=opinions_data[2]
        sell_count=opinions_data[3]+opinions_data[4]
        total = buy_count+hold_count+ sell_count

        ## Find target prices
        letters2 = soup.find_all('div' ,class_='cr_data rr_stockprice module')
        data = letters2[0].find_all('span',class_='data_data')
        target_prices = [float(x.text[1:]) for x in data]
        highest_target = target_prices[0]
        lowest_target = target_prices[2]
        median_target = target_prices[1]
        analyst_opinions_table = analyst_opinions_table.append(pd.DataFrame({'Constituent':constituent,'Buy':buy_count,'Hold':hold_count,'Sell':sell_count,'% Buy':round(buy_count*1.0/total,3),'% Hold':round(hold_count*1.0/total,3),'% Sell':round(sell_count*1.0/total,3),'Median target price':median_target, 'Highest target price':highest_target,'Lowest target price':lowest_target,'Date':str(datetime.date.today()),'Table':'Analyst opinions','Status':'active'},index=[0],),ignore_index=True)
    columnsTitles = ['Constituent', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Median target price','Highest target price','Lowest target price','Table','Status','Date']
    analyst_opinions_table =analyst_opinions_table.reindex(columns=columnsTitles)
    return analyst_opinions_table
    
def combined_analyst(ws_analyst_table, bi_analyst_table):
    constituent_list = ['Adidas','Commerzbank','Deutsche Bank', 'EON', 'BMW']
    combined_analyst_table = pd.DataFrame()
    for constituent in constituent_list:
        ##Add the number of buy, sell and hold
        #print ws_analyst_table['Buy'].loc[ws_analyst_table['Constituent']==constituent]
        #print bi_analyst_table['Buy'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        rating = bi_analyst_table['Analyst rating'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        recommendation = bi_analyst_table['Analyst recommendation'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        buy= ws_analyst_table['Buy'].loc[ws_analyst_table['Constituent']==constituent].values[0]+bi_analyst_table['Buy'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        sell=ws_analyst_table['Sell'].loc[ws_analyst_table['Constituent']==constituent].values[0]+bi_analyst_table['Sell'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        hold=ws_analyst_table['Hold'].loc[ws_analyst_table['Constituent']==constituent].values[0]+bi_analyst_table['Hold'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        total = buy+sell+hold
        
        #Find the average of highest, median and lowest target prices
        mean_highest_target = (ws_analyst_table['Highest target price'].loc[ws_analyst_table['Constituent']==constituent].values[0]+bi_analyst_table['Highest target price'].loc[bi_analyst_table['Constituent']==constituent].values[0])/2.0
        mean_median_target = (ws_analyst_table['Median target price'].loc[ws_analyst_table['Constituent']==constituent].values[0]+bi_analyst_table['Median target price'].loc[bi_analyst_table['Constituent']==constituent].values[0])/2.0
        mean_lowest_target = (ws_analyst_table['Lowest target price'].loc[ws_analyst_table['Constituent']==constituent].values[0]+bi_analyst_table['Lowest target price'].loc[bi_analyst_table['Constituent']==constituent].values[0])/2.0
        
        
        rating = bi_analyst_table['Analyst rating'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        recommendation = bi_analyst_table['Analyst recommendation'].loc[bi_analyst_table['Constituent']==constituent].values[0]
        combined_analyst_table = combined_analyst_table.append(pd.DataFrame({'Constituent':constituent, 'Analyst rating': rating,'Analyst recommendation': recommendation,'Buy':buy,'Hold':hold,'Sell':sell,'% Buy':round(buy*1.0/total,3),'% Hold':round(hold/total,3),'% Sell':round(sell/total,3),'Median target price':round(mean_median_target,2), 'Highest target price':round(mean_highest_target,2),'Lowest target price':round(mean_lowest_target,2),'Date': str(datetime.date.today()),'Table':'Analyst opinions','Status':'active'},index=[0]),ignore_index=True)
    columnsTitles = ['Constituent', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Analyst rating','Analyst recommendation','Median target price','Highest target price','Lowest target price','Table','Status','Date']
    combined_analyst_table = combined_analyst_table.reindex(columns=columnsTitles)
    return combined_analyst_table
    

def main(arguments):
    #connection string + database + table for storage
    all_constituents_dict_bi = {'Allianz':'Allianz', 'Adidas':'Adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche_Bank', 'Deutsche Börse':'Deutsche_Boerse', 'Deutsche Post':'Deutsche_Post',
                    'Deutsche Telekom':'Deutsche_Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius_Medical_Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    'Linde':'Linde_6','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': 'Munich_Re',
                    'ProSiebenSat1 Media':'ProSiebenSat1_Media', 'RWE':'RWE', 'Siemens':'Siemens', 'Thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen_vz','Vonovia':'Vonovia'}
    #,'Vonovia':'Vonovia'

    bi_analyst_table = analyst_businessinsider(all_constituents_dict_bi)
    ws_analyst_table = analyst_wallstreet()
    combined_analyst_table = combined_analyst(ws_analyst_table, bi_analyst_table)
    
    import json
    combined_analyst_json = json.loads(combined_analyst_table.to_json(orient='records'))
    bi_analyst_json = json.loads(bi_analyst_table.to_json(orient='records'))
    
    from utils.Storage import Storage
    storage = Storage()
    
    #save the result: combined - analyst_opinions, bi - analyst_opinions_all
    storage.save_to_mongodb(connection_string=args.param_connection_string, database=args.database,collection=args.collection_selected, data=combined_analyst_json)
    storage.save_to_mongodb(connection_string=args.param_connection_string, database=args.database,collection=args.collection_all, data=bi_analyst_json)
    
    
if __name__ == "__main__":
    #Hard-codings to be removed: constituents, mongdbconnection, table to store the results for. 
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    #parser.add_argument('python_path', help='The connection string') #directory of script
    parser.add_argument('param_connection_string', help='The connection string') #mongodb connection string
    #parser.add_argument('constituent_list', help='constituents on DAX') #constituents
    parser.add_argument('database', help='database for storage') #collection name
    parser.add_argument('collection_selected', help='collection for storing selected constituents') #collection name for storage
    parser.add_argument('collection_all', help='collection for storing all constituents') #collection name for storage
    args = parser.parse_args()
    
    #sys.path.insert(0, args.python_path)#for inserting new function? 
    
    main(args)
   