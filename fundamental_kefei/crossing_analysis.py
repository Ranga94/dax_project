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
import json

#!python Igenie/dax_project/fundamental_kefei/crossing_analysis.py '/Users/kefei/Igenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'historical' 'price analysis' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz' 'market signal'



#Note: Henkel_vs(Henkel vs) does not have data
def crossing_main():
    table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        his = get_historical_price(args,constituent)
        cross_interval,ave_golden_duration,ave_golden_growth,recent_cross,diverge,max_price,min_price,days_after_extreme,extreme_status,pct_diff_from_extreme,crossing_score = crossing_analysis(his)
        table = table.append(pd.DataFrame({'Constituent': constituent,'Price on the latest date': float(his.iloc[-1].closing_price),'Average crossing interval (days)': cross_interval,'Crossing frequency per year':round(365.0/cross_interval,2), 'Duration of Golden Cross(days)':int(ave_golden_duration), 'Average return per Golden Cross period':round(ave_golden_growth,2),'Recent cross':recent_cross, 'Status of SMA 50':diverge,'Maximum price after cross':max_price,'Minimum price after cross':min_price,'Days elaspsed from the last max/min':days_after_extreme,'Max/min observation':extreme_status,'% change in price after max/min':pct_diff_from_extreme,'Bull score (crossing)': crossing_score,'Table':'Crossing_score','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    crossing_json = json.loads(table.to_json(orient='records'))
    #store the analysis
    #storage = Storage()
    #storage.save_to_mongodb(connection_string=args.connection_string, database=args.database,collection=args.collection_store_analysis, data=crossing_json)


##This (very long) function returns the following quantities: 
#Crossing frequency - On average the number of days it takes for a Golden Cross/Death Cross to happen
#Average duration of Golden Cross period - time elapsed from the occurrence of Golden Cross till the occurrence of Death Cross
#Average golden growth - the average % growth in share price during a Golden Cross period. 
##The function takes a dataframe of the historical company data as an input. 

def crossing_analysis(his):
    his['SMA-50'] = his['closing_price'].rolling(window=50,center=False).mean()
    his['SMA-200']= his['closing_price'].rolling(window=200,center=False).mean()
    previous_50 = his['SMA-50'].shift(1)
    previous_200 = his['SMA-200'].shift(1)

#Identify all the crosses, calculate the number of days on average a cross occurs. 
    crosses = (((his['SMA-50'] <= his['SMA-200']) & (previous_50 >= previous_200))|((his['SMA-50']>= his['SMA-200']) & (previous_50 <= previous_200)))
    crossing_dates = his.loc[crosses, 'date']
    crossing_index = crosses[crosses==True].index.tolist()
    freq = crossing_dates.diff().mean().days

##Idenfity death and golden crosses 
    death_crosses = ((his['SMA-50'] <= his['SMA-200']) & (previous_50 >= previous_200))
    golden_crosses = ((his['SMA-50']>= his['SMA-200']) & (previous_50 <= previous_200))

##calculating the average duration of golden cross
    golden_crossing_dates = his.loc[golden_crosses, 'date']
    death_crossing_dates = his.loc[death_crosses,'date']

    golden_crossing_dates = pd.DataFrame(golden_crossing_dates)
    death_crossing_dates = pd.DataFrame(death_crossing_dates)

    golden_crossing_index = golden_crosses[golden_crosses==True].index.tolist()
    death_crossing_index = death_crosses[death_crosses==True].index.tolist() 

##Find the average golden cross duration
    durations =0 #initialize
    growth_sum = 0
    num_crossings = min(golden_crossing_dates.shape[0],death_crossing_dates.shape[0])
    
#Different calculations depending on which of golden and death crosses occur the earliest
    if golden_crossing_dates['date'].iloc[0]<death_crossing_dates['date'].iloc[0]:
        for i in range(num_crossings):
            tf = death_crossing_dates['date'].iloc[i]-golden_crossing_dates['date'].iloc[i]
            durations = durations+int(tf.days)
        #Take either SMA-50 or SMA-200. At the point of crossing, the SMAs are identical and are already very close to the actual price in that instant
            golden_price = his['SMA-50'].loc[his['date'] == golden_crossing_dates['date'].iloc[i]]
            death_price = his['SMA-50'].loc[his['date'] == death_crossing_dates['date'].iloc[i]]
        #calculate rowth as a proportion to the price when golden cross occurs. 
            growth = (float(death_price)-float(golden_price))/(0.0+float(golden_price))
            growth_sum = growth_sum+growth
        
    else:
        for i in np.arange(1,num_crossings):
            tf = death_crossing_dates['date'].iloc[i]-golden_crossing_dates['date'].iloc[i-1]
            durations = durations+int(tf.days)
            golden_price = his['SMA-50'].loc[his['date'] == golden_crossing_dates['date'].iloc[i-1]]
            death_price = his['SMA-50'].loc[his['date'] == death_crossing_dates['date'].iloc[i]]
            growth = (float(death_price)-float(golden_price))/(0.0+float(golden_price))
            growth_sum = growth_sum+growth

            
    #Obtain the average golden cross duration in terms of days.        
    ave_golden_duration = durations*1.0/golden_crossing_dates.shape[0]
    ave_death_duration = (his.shape[0] - durations*1.0)/death_crossing_dates.shape[0]

    ##Obtain the average growth in proportion of share price during golden crossing
    ave_golden_growth = growth_sum/(1.0*golden_crossing_dates.shape[0])
    
    ####Obtain the current crossing state of stock
    if golden_crossing_dates['date'].iloc[-1]> death_crossing_dates['date'].iloc[-1]:
        recent_cross = 'Golden Cross'
        #Measures how much is the short-term moving average is above the long-term moving average after a Golden Cross
        difference = his['SMA-50'].iloc[golden_crossing_index[-1]:]- his['SMA-200'].iloc[golden_crossing_index[-1]:] 
        #This records how the difference between SMA-50 and SMA-200 varies over time. (Are they still diverging from each other?)
        n = difference.shape[0]
        difference_diff = difference.diff()
    
        ##Only focus on the most recent 1/3 of the price after Golden Cross. 
        ##Proportional to the average gradient of the difference between SMA-50 AND SMA-200, assuming that the time interval of data collection is consistent. 
        difference_coefficient = difference_diff[int(n*2/3):n].mean()
        ## DO SOMETHING WITH THIS !
    
        if difference_coefficient > 0: 
            diverge = "continues diverging (Bull)"
        else:
            diverge = "starts converging to SMA 200"
        
         ##Find the peak after the gold crossing 
        his_after_gold_cross =his.iloc[golden_crossing_index[-1]:] #truncate the historical price data
        his_max_price = his.ix[his_after_gold_cross['closing_price'].idxmax()] #extract the row in historical price data when price is maximum after crossing
        max_price = round(his_max_price['closing_price'],2) 
        date_max_price = his_max_price['date'] 
        
        ##Find the minimum price recorded after the gold crossing 
        his_min_price = his.ix[his_after_gold_cross['closing_price'].idxmin()] #extract the row in historical price data when price is maximum after crossing
        min_price = round(his_min_price['closing_price'],2)
        date_min_price = his_min_price['date'] 
        
        #Find how many days elapsed from the day when the peak price till the day of latest price collection
        days_after_extreme = int((his['date'].iloc[-1] - date_max_price).days)
        pct_diff_from_extreme = round((his['closing_price'].iloc[-1]-his_max_price['closing_price'])*100.0/his_max_price['closing_price'],2)
        if days_after_extreme == 0: 
            extreme_status = 'Still increasing/reaching a maximum'
        else:
            extreme_status = '%s days'%days_after_extreme +' off the maximum at %s'%max_price
       
        
    else:
        recent_cross= 'Death Cross'
        difference = his['SMA-200'].iloc[golden_crossing_index[-1]:] -his['SMA-50'].iloc[golden_crossing_index[-1]:]
    #This records how the difference between SMA-50 and SMA-200 varies over time. (Are they still diverging from each other?)
        n = difference.shape[0]
        difference_diff = difference.diff()
    
    ##Only focus on the most recent 1/3 of the price after Death Cross. 
    ##Proportional to the average gradient of the difference between SMA-50 AND SMA-200, assuming that the time interval of data collection is consistent. 
        difference_coefficient = difference_diff[int(n*2/3):n].mean()
    
    ## INSTEAD OF 2/3, USE PEAK VALUE POSITIONS!!
    
        if difference_coefficient > 0: 
            diverge = "continues diverging (Bear)"
        else:
            diverge = "starts converging to SMA 200"
        
         ##Find the trough after the death crossing 
        his_after_death_cross =his.iloc[death_crossing_index[-1]:] #truncate the historical price data
        his_min_price = his.ix[his_after_death_cross['closing_price'].idxmin()] #extract the row in historical price data when price is maximum after crossing
        min_price = round(his_min_price['closing_price'],2)
        date_min_price = his_min_price['date'] 
        
        ##Find the maximum price after the death crossing 
        his_max_price = his.ix[his_after_death_cross['closing_price'].idxmax()] #extract the row in historical price data when price is maximum after crossing
        max_price = round(his_max_price['closing_price'],2)
        date_max_price = his_max_price['date'] 
        
        
        #Find how many days elapsed from the day when the peak price till the day of latest price collection
        days_after_extreme = int((his['date'].iloc[-1] - date_min_price).days)
        pct_diff_from_extreme = round((his['closing_price'].iloc[-1]-his_min_price['closing_price'])*100.0/his_max_price['closing_price'],2)
        if days_after_extreme: 
            extreme_status = 'Still decreasing/reaching a minimum'
        else:
            extreme_status = '%s days'%days_after_extreme +' off the minimum at %s'%min_price
    
    ########Sort this out
        #Cross_score scores the bullish trend of SMA-50 and SMA-20. 
        if (recent_cross == 'Golden Cross') & (diverge =="continues diverging (Bull)"):
            cross_score = 2
        elif (recent_cross == 'Death Cross') & (diverge =="continues diverging (Bear)"):
            cross_score = -2
        else:
            cross_score = 0
        
        #This is a stronger measurement of bull/bear trends
        if extreme_status == 'Still increasing/reaching a maximum':
            cross_score +=3
        elif extreme_status == 'Still decreasing/reaching a minimum':
             cross_score -=3
        else:
            cross_score = cross_score
        
        #If Golden-cross period generally last longer than death, add one. 
        if ave_golden_duration>freq:
            cross_score +=1
        else: 
            cross_score=cross_score
            
    return freq,ave_golden_duration,ave_golden_growth,recent_cross,diverge,max_price,min_price,days_after_extreme,extreme_status,pct_diff_from_extreme,cross_score


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
    collection.update_many({'Table':args.table_store_analysis,'status':'active'}, {'$set': {'status': 'inactive'}},True,True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The directory connection string') 
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_price', help='The collection from which historical price is exracted')
    parser.add_argument('collection_store_analysis', help='The collection where the analysis will be stored')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('table_store_analysis', help='Name of table for stroing the analysis')

    args = parser.parse_args()
    
    sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    crossing_main(args)