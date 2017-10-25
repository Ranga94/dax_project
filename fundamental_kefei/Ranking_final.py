
# coding: utf-8

# # Collecting data For Ranking Analysis

# In[1]:

import pandas as pd
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pylab
import scipy
from scipy import stats
from statsmodels.tsa.stattools import adfuller
from odo import odo
from decimal import Decimal
import operator
from bs4 import BeautifulSoup
import urllib


# In[6]:

#client = MongoClient('mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp')
db = client.dax_gcp
collection1 = db['fundamental analysis']
collection2 = db['price analysis']


# In[7]:

## Check today's date and the date when results are collected
today_date = str(datetime.date.today())


# ## Collecting fundamental analysis

# In[14]:

def fundamental_analysis_collection():
    industry_category_table= pd.DataFrame(list(collection1.find({'Table':'category analysis'})))
    ROCE_table = pd.DataFrame(list(collection1.find({'Table':'ROCE analysis'})))
    sales_table = pd.DataFrame(list(collection1.find({'Table':'Sales analysis'})))
    dividend_table = pd.DataFrame(list(collection1.find({'Table':'dividend analysis'})))
    profit_margin_table = pd.DataFrame(list(collection1.find({'Table':'profit margin analysis'})))
    PER_table = pd.DataFrame(list(collection1.find({'Table':'PER analysis'})))
    EPS_table = pd.DataFrame(list(collection1.find({'Table':'EPS analysis'})))
    EBITDA_table = pd.DataFrame(list(collection1.find({'Table':'EBITDA analysis'})))
    return industry_category_table,ROCE_table,sales_table,dividend_table,profit_margin_table,PER_table,EPS_table,EBITDA_table


# In[15]:

industry_category_table,ROCE_table,sales_table,dividend_table,profit_margin_table,PER_table,EPS_table,EBITDA_table=fundamental_analysis_collection()


# ## Collecting price analysis

# In[12]:

def price_analysis_collection(): 
    cumulative_returns_table = pd.DataFrame(list(collection2.find({'Table':'cumulative return analysis'})))
    quarter_mean_table = pd.DataFrame(list(collection2.find({'Table':'quarterly growth analysis'})))
    standard_dev_table = pd.DataFrame(list(collection2.find({'Table':'standard deviation analysis'})))
    ATR_table = pd.DataFrame(list(collection2.find({'Table':'ATR analysis'})))
    market_signal_table = pd.DataFrame(list(collection2.find({'Table':'Market signal'})))
    dividend_table = pd.DataFrame(list(collection2.find({'Table':'dividend analysis'})))
    return cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,market_signal_table,dividend_table


# In[13]:

cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,market_signal_table,dividend_table=price_analysis_collection()


# In[33]:

cumulative_returns_table = cumulative_returns_table[cumulative_returns_table['Status']=='active']
quarter_mean_table = quarter_mean_table[quarter_mean_table['Status']=='active']
standard_dev_table = standard_dev_table[standard_dev_table['Status']=='active']
ATR_table = ATR_table[ATR_table['Status']=='active']
market_signal_table = market_signal_table[market_signal_table['Status']=='active']
dividend_table = dividend_table[dividend_table['Status']=='active']


# # Fundamental Growth Ranking 

# In[17]:

## Scores the growth of fundamental values out of 12
def fundamental_growth_scoring():
    fundamental_score_board = pd.DataFrame()
    ## Note that pandas could not locate special German alphabets, hence use unicode notations
    ## Münchener Rückversicherungs-Gesellschaft:u'M\xfcnchener R\xfcckversicherungs-Gesellschaft'
    ## Deutsche Börse: u'Deutsche B\xf6rse'
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    score_list = ['ROCE score','Sales score','Profit margin score','PER score','EPS score','EBITDA score']
    n=len(table_list)
    for constituent in all_constituents: 
        fundamental_score_array = np.zeros(n)
        for i in range(n):
            table = table_list[i]
            if table[score_list[i]].loc[table['Constituent']==constituent].empty == False:
                score = int(table[score_list[i]].loc[table['Constituent']==constituent].values[0])
                fundamental_score_array[i]=score 
            else:
                score = 'N/A'
                fundamental_score_array[i]=0
                print str(score_list[i]) + ' for '+ str(constituent) + ' is not avaliable'
            total_score = sum(fundamental_score_array)
        fundamental_score_board = fundamental_score_board.append(pd.DataFrame({'Constituent':constituent, 'Fundamental growth score':total_score,'ROCE score':fundamental_score_array[0],'Sales score':fundamental_score_array[1],'Profit margin score':fundamental_score_array[2],'PER score':fundamental_score_array[3],'EPS score':fundamental_score_array[4],'EBITDA score':fundamental_score_array[5]},index=[0]),ignore_index=True)
        columnsTitles =  ['Constituent','Fundamental growth score','ROCE score','Sales score','Profit margin score','PER score','EPS score','EBITDA score']
        fundamental_score_board=fundamental_score_board.reindex(columns=columnsTitles)
        fundamental_score_board= fundamental_score_board.sort_values('Fundamental growth score',axis=0, ascending=False).reset_index(drop=True)
    return fundamental_score_board


# In[18]:

fundamental_score_board=fundamental_growth_scoring()


# In[19]:

fundamental_score_board


# # Current Fundamental Ranking

# ### Calculate the statistics for current fundamental values on all the DAX-30 constituents

# In[34]:

## Calculate the mean and standard deviation on all the fundamental elements based on overall data of DAX-30 constituents. 
def current_fundamental_stats():
    fundamental_stats_table = pd.DataFrame()
    table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    current_values_list = ['Current ROCE','Current sales in Mio','Current profit margin','Current PER','Current EPS','Current EBITDA in Mio']
    n=len(table_list)
    stats_array = np.zeros((n,2))
    for i in range(n):
        table = table_list[i]
        mean = round(table[current_values_list[i]].mean(),2)
        std_dev = round(table[current_values_list[i]].std(),2)
        max_val = table[current_values_list[i]].max()
        min_val = table[current_values_list[i]].min()
        stats_array[i,0]=mean
        stats_array[i,1]=std_dev
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        top_lower = mean+std_dev
        good_lower=mean+std_dev*0.5
        fair_lower =mean-0.5*std_dev
        below_ave_lower =mean-1.0*std_dev
        
        fundamental_stats_table=fundamental_stats_table.append(pd.DataFrame({'Fundamental quantity':current_values_list[i],'Mean':mean,'Standard deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'Top lower-bound':top_lower, 'Good lower-bound':good_lower,'Fair lower-bound':fair_lower},index=[0]),ignore_index=True)
    return fundamental_stats_table


# In[35]:

current_fundamental_stats_table=current_fundamental_stats()


# In[36]:

current_fundamental_stats_table


# ### Creating a scoring system based on the statistics

# In[37]:

table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]


# In[38]:

## Scores the current fundamenal values out of 24. 
def current_fundamental_scoring(stats_table,table_list):
    table_list = [ROCE_table,sales_table,profit_margin_table,PER_table,EPS_table,EBITDA_table]
    current_values_list = ['Current ROCE','Current sales in Mio','Current profit margin','Current PER','Current EPS','Current EBITDA in Mio']
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents = ['Allianz']
    m=len(table_list)
    n=len(all_constituents)
    current_fundamental_board = pd.DataFrame()
    current_fundamental_array=np.zeros((n,m))
    for j in range(m): ##loop through fundamental quantities
        array = np.zeros(n) #this array stores scores of one particular fundamental quantity for each constituent. 
        table = table_list[j]
        top_lower = float(stats_table['Top lower-bound'].loc[stats_table['Fundamental quantity']==current_values_list[j]])
        good_lower = float(stats_table['Good lower-bound'].loc[stats_table['Fundamental quantity']==current_values_list[j]])
        fair_lower = float(stats_table['Fair lower-bound'].loc[stats_table['Fundamental quantity']==current_values_list[j]])
       
        
        for i in range(n): ##loop through constituents
            constituent = all_constituents[i]
            #print constituent
            if table[current_values_list[j]].loc[table['Constituent']==constituent].empty==False: 
                value = float(table[current_values_list[j]].loc[table['Constituent']==constituent])
                #print value
                #print top_lower
                if value > top_lower:
                    score = 4 #top-performing
                elif value > good_lower:
                    score = 2 #well-performing
                elif value> fair_lower:
                    score = 1 #fair-performing
                else: 
                    score = 0 #poorly-performing
                current_fundamental_array[i,j]=score
            else: 
                print current_values_list[j]+'=N/A for '+constituent
                score=0
                current_fundamental_array[i,j]=score
            #print score
            
        current_fundamental_sum = sum(current_fundamental_array) ##sum the values on the same row
        
        #current_fundamental_array stores all the info needed to calculate scores
    for i in range(n): ## loop constituents
        temp = {'Constituent':all_constituents[i], 'Current fundamental total score':sum(current_fundamental_array[i,:])}
        score_dict = {str(current_values_list[j]):int(current_fundamental_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        current_fundamental_board=current_fundamental_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
        columnsTitles =  ['Constituent','Current fundamental total score','Current ROCE','Current sales in Mio','Current profit margin','Current PER','Current EPS','Current EBITDA in Mio']
        current_fundamental_board=current_fundamental_board.reindex(columns=columnsTitles)
        current_fundamental_board= current_fundamental_board.sort_values('Current fundamental total score',axis=0, ascending=False).reset_index(drop=True)
    return current_fundamental_board


# In[39]:

current_fundamental_board=current_fundamental_scoring(current_fundamental_stats_table,table_list)


# # Stock Price Growth Ranking

# ### Scoring system based on statistics

# In[41]:

## Calculate the mean and standard deviation on all the fundamental elements based on overall data of DAX-30 constituents. 
def price_growth_stats():
    price_stats_table = pd.DataFrame()
    price_table_list = table_list = [cumulative_returns_table,cumulative_returns_table,quarter_mean_table,quarter_mean_table,market_signal_table]
    price_growth_list = ['1 year return','3 years return','Rate of change in price in the last 365 days/quarter','Rate of change in price in the last 3 years/quarter','Current RSI']
    n=len(table_list)
    stats_array = np.zeros((n,2))
    for i in range(n):
        table = table_list[i]
        mean = table[price_growth_list[i]].mean()
        std_dev = table[price_growth_list[i]].std()
        max_val = table[price_growth_list[i]].max()
        min_val = table[price_growth_list[i]].min()
        stats_array[i,0]=mean
        stats_array[i,1]=std_dev
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        top_lower = max(mean+std_dev,0)
        good_lower=max(mean+std_dev*0.5,0)
        fair_lower =max(mean-0.5*std_dev,0)
        below_ave_lower =mean-1.0*std_dev
        
        price_stats_table=price_stats_table.append(pd.DataFrame({'Price growth quantity':price_growth_list[i],'Mean':mean,'Standard deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'Top lower-bound':top_lower, 'Good lower-bound':good_lower,'Fair lower-bound':fair_lower},index=[0]),ignore_index=True)
    return price_stats_table


# In[42]:

price_stats_table=price_growth_stats()


# In[43]:

price_stats_table


# In[44]:

## Scores the growth of price out of 30. 
def price_growth_scoring(stats_table,price_table_list):
    #price_table_list = [cumulative_returns_table,cumulative_returns_table,quarter_mean_table,quarter_mean_table,market_signal_table]
    price_growth_list = ['1 year return','3 years return','Rate of change in price in the last 365 days/quarter','Rate of change in price in the last 3 years/quarter','Current RSI']
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents = ['Allianz']
    m=len(price_table_list)
    n=len(all_constituents)
    price_growth_board = pd.DataFrame()
    price_growth_array=np.zeros((n,m))
    
    for j in range(m): ##loop through fundamental quantities
        array = np.zeros(n) #this array stores scores of one particular fundamental quantity for each constituent. 
        table = price_table_list[j]
        #print str(table)
        top_lower = float(stats_table['Top lower-bound'].loc[stats_table['Price growth quantity']==price_growth_list[j]])
        good_lower = float(stats_table['Good lower-bound'].loc[stats_table['Price growth quantity']==price_growth_list[j]])
        fair_lower = float(stats_table['Fair lower-bound'].loc[stats_table['Price growth quantity']==price_growth_list[j]])
        
        for i in range(n): ##loop through constituents
            constituent = all_constituents[i]
            if table[price_growth_list[j]].loc[table['Constituent']==constituent].empty==False: 
                value =  table[price_growth_list[j]].loc[table['Constituent']==constituent].values[0]
                
                if value > top_lower:
                    score = 4 #top-performing
                elif value > good_lower:
                    score = 3 #well-performing
                elif value> fair_lower:
                    score = 2 #fair-performing
                else: 
                    score = 1 #poorly-performing
                price_growth_array[i,j]=score
            else: 
                print price_growth_list[j]+'=N/A for '+constituent
                score=0
                price_growth_array[i,j]=score
        
    for i in range(n): ## loop constituents
        temp = {'Constituent':all_constituents[i], 'Price growth score':sum(price_growth_array[i,:])}
        score_dict = {str(price_growth_list[j]):int(price_growth_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        price_growth_board=price_growth_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
    
    ## Append the consistency scores into the calculation of total price growth score
    CR_table = price_table_list[0]
    QM_table = price_table_list[2]
    market_signal_table = price_table_list[-1]
    price_growth_board = price_growth_board.merge(CR_table[['Constituent','Cumulative return consistency score']], on='Constituent',how='inner')
    price_growth_board = price_growth_board.merge(QM_table[['Constituent','Quarterly growth consistency score']], on='Constituent',how='inner')
    price_growth_board = price_growth_board.merge(market_signal_table[['Constituent','Bull score (crossing)']],on='Constituent',how='inner')
    price_growth_board['Total price growth score']=price_growth_board['Cumulative return consistency score']+price_growth_board['Quarterly growth consistency score']+price_growth_board['Bull score (crossing)']+price_growth_board['Price growth score']
    columnsTitles = ['Constituent','Total price growth score','Price growth score','Cumulative return consistency score','Quarterly growth consistency score','1 year return','3 years return','Rate of change in price in the last 365 days/quarter','Rate of change in price in the last 3 years/quarter','Current RSI','Bull score (crossing)']
    price_growth_board=price_growth_board.reindex(columns=columnsTitles)
    price_growth_board= price_growth_board.sort_values('Total price growth score',axis=0, ascending=False).reset_index(drop=True)
    return price_growth_board


# In[45]:

price_table_list = [cumulative_returns_table,cumulative_returns_table,quarter_mean_table,quarter_mean_table,market_signal_table]
price_growth_board =price_growth_scoring(price_stats_table,price_table_list)


# In[85]:

price_growth_board['Status']='active'


# # Golden cross and momentum measurement 

# In[ ]:

## Note that Bull scores derived from RSI and crossing are medium-term forcast
## RSI,bull score


# ## Measuring profitability from Cross Analysis results

# In[ ]:

#Average return from golden cross

#No.Days duration from golden cross to reach the maximum

#Golden cross period vs. crossing frequency



# ## Measuring risk from Cross Analysis results

# In[ ]:

#Crossing Frequency
#%days overbought/oversell


# # Combined Profitability Ranking

# ### Combine the score for Price growth, Current Fundamental values, and Fundamental growth

# In[86]:

## Combined profitability scores the price growth and fundamental potential out of 60. 
def combined_profitability_scoring(): 
    board_list = [price_growth_board,current_fundamental_board,fundamental_score_board]
    score_list = ['Total price growth score','Current fundamental total score','Fundamental growth score']
    n=len(board_list)
    combined_profitability_board = pd.DataFrame()
    temp = board_list[0]
    for i in range(n-1):
        temp = temp.merge(board_list[i+1],on='Constituent',how='left')
    
    temp['Total profitability score']=temp['Total price growth score']+temp['Current fundamental total score']+temp['Fundamental growth score']
    combined_profitability_board = pd.DataFrame(temp[['Constituent','Total profitability score','Total price growth score','Current fundamental total score','Fundamental growth score','Status']])
    #combined_profitability_board = combined_profitability_board.sort_values('Total profitability score',axis=0, ascending=False).reset_index(drop=True)
    return combined_profitability_board


# In[87]:

combined_profitability_board=combined_profitability_scoring()


# In[88]:

combined_profitability_board


# In[89]:

def combined_profitability_ranking(combined_profitability_board):
    all_constituents_dict = {'Allianz':'Allianz', 'Adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche Bank', 'Deutsche Börse':u'Deutsche B\xf6rse', 'Deutsche Post':'Deutsche Post',
                    'Deutsche Telekom':'Deutsche Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius Medical Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': u'M\xfcnchener R\xfcckversicherungs-Gesellschaft',
                    'ProSiebenSat1 Media':'ProSiebenSat1 Media', 'RWE':'RWE', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen (VW) vz','Vonovia':'Vonovia'}
    #all_constituents_dict = {'Allianz':'Allianz', 'Adidas':'adidas'}
    profitability_ranking_table=pd.DataFrame()
    combined_profitability_board = combined_profitability_board.sort_values('Total profitability score',axis=0, ascending=False).reset_index(drop=True)
    for constituent in all_constituents_dict:
        #print all_constituents_dict[constituent]
        index = int(combined_profitability_board[combined_profitability_board['Constituent']==all_constituents_dict[constituent]].index[0])
        price_growth_score= int(combined_profitability_board['Total price growth score'].loc[combined_profitability_board['Constituent']==all_constituents_dict[constituent]])
        fundamental_growth_score=  int(combined_profitability_board['Fundamental growth score'].loc[combined_profitability_board['Constituent']==all_constituents_dict[constituent]])
        if price_growth_score >=24 :
            growth_price_status = 'High'
        elif price_growth_score >=16 :
            growth_price_status = 'Medium'
        else :
            growth_price_status = 'Low'
            
        if fundamental_growth_score >=10 :
            growth_fundamental_status = 'High'
        elif fundamental_growth_score >=6 :
            growth_fundamental_status = 'Medium'
        else :
            growth_fundamental_status = 'Low'
            
        profitability_ranking_table=profitability_ranking_table.append(pd.DataFrame({'Constituent':constituent, 'Profitability rank':index, 'Price growth':growth_price_status,'Fundamental growth':growth_fundamental_status,'Date':str(datetime.date.today()),'Status':'active'},index=[0]),ignore_index=True)
    profitability_ranking_table=profitability_ranking_table.sort_values('Profitability rank',axis=0, ascending=True).reset_index(drop=True)
    return profitability_ranking_table


# In[90]:

profitability_ranking_table = combined_profitability_ranking(combined_profitability_board)
profitability_ranking_table


# ## Insert Profitability Ranking and Scoring result into Mongodb

# In[91]:

#Insert the profitability score into Mongodb
import json

db = client.dax_gcp
combined_profitability_json = json.loads(combined_profitability_board.to_json(orient='records'))
#db['profitability_ranking'].drop()
#db['profitability_score'].drop()
collection = db['profitability_score']
#collection.update_many({}, {'$set': {'Status': 'active'}},True,True)
collection.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
collection.update_many({'Status':'NaN'}, {'$set': {'Status': 'inactive'}},True,True)
db['profitability_score'].insert_many(combined_profitability_json)


# In[92]:

#Insert the profitability ranking into Mongodb
profitability_rank_json = json.loads(profitability_ranking_table.to_json(orient='records'))
collection2 = db['profitability_ranking']
collection2.update_many({}, {'$set': {'Status': 'active'}},True,True)
collection2.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
db['profitability_ranking'].insert_many(profitability_rank_json)


# # Volatility Ranking

# In[57]:

## Market Risk due to Volatility: Price volatility and crossing frequency


# In[93]:

## gather the statistics
def volatility_stats():
    volatility_stats_table = pd.DataFrame()
    volatility_table_list = [standard_dev_table,standard_dev_table,ATR_table,ATR_table,market_signal_table]
    volatility_list = ['Last 12 months','Last 3 years','Average ATR in the last 12 months','Average ATR in the last 3 years','Crossing frequency per year']
    n=len(volatility_table_list)
    for i in range(n):
        table = volatility_table_list[i]
        mean = table[volatility_list[i]].mean()
        std_dev = table[volatility_list[i]].std()
        max_val = table[volatility_list[i]].max()
        min_val = table[volatility_list[i]].min()
        
        ## categorise boundaries for scoring system, using mean and standard deviation
        high_lower = min(max_val,mean+std_dev)
        medium_lower=max(min(max_val,mean-std_dev*0.5),max(min_val,mean-std_dev*0.5))
        
        volatility_stats_table=volatility_stats_table.append(pd.DataFrame({'Volatility quantity':volatility_list[i],'Mean':mean,'Standard deviation':std_dev,'Maximum':max_val, 'Minimum':min_val,'High volatility lower-bound':high_lower, 'Medium volatility lower-bound':medium_lower},index=[0]),ignore_index=True)
    return volatility_stats_table


# In[94]:

volatility_stats_table=volatility_stats()


# In[95]:

volatility_stats_table


# In[96]:

#Scores out of 15. 
def volatility_scoring(stats_table,volatility_table_list):
    volatility_table_list = [standard_dev_table,standard_dev_table,ATR_table,ATR_table,market_signal_table]
    volatility_list = ['Last 12 months','Last 3 years','Average ATR in the last 12 months','Average ATR in the last 3 years','Crossing frequency per year']
    m=len(volatility_table_list)
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents = ['Allianz']
    n=len(all_constituents)
    volatility_score_board = pd.DataFrame()
    volatility_score_array=np.zeros((n,m))
    
    for j in range(m): ##loop through fundamental quantities
        table = volatility_table_list[j]
        high_lower = float(stats_table['High volatility lower-bound'].loc[stats_table['Volatility quantity']==volatility_list[j]])
        medium_lower = float(stats_table['Medium volatility lower-bound'].loc[stats_table['Volatility quantity']==volatility_list[j]])
        
        for i in range(n): ##loop through constituents
            constituent = all_constituents[i]
            if table[volatility_list[j]].loc[table['Constituent']==constituent].empty==False: 
                value =  table[volatility_list[j]].loc[table['Constituent']==constituent].values[0]
                
                if value > high_lower:
                    score = 3 #high volatility
                elif value > medium_lower:
                    score = 2 #medium volatility
                else: 
                    score = 1 #low volatilility
                volatility_score_array[i,j]=score
            else: 
                print volatility_list[j]+'=N/A for '+constituent
                score=0
                volatility_score_array[i,j]=score
        
    for i in range(n): ## loop constituents
        temp = {'Constituent':all_constituents[i], 'Volatility score':sum(volatility_score_array[i,:])}
        score_dict = {str(volatility_list[j]):int(volatility_score_array[i,j]) for j in range(m)}
        score_dict.update(temp.copy())
        volatility_score_board =volatility_score_board.append(pd.DataFrame(score_dict,index=[0]),ignore_index=True)
    
    columnsTitles = ['Constituent','Volatility score','Last 12 months','Last 3 years','Average ATR in the last 12 months','Average ATR in the last 3 years','Crossing frequency per year']
    volatility_score_board =volatility_score_board.reindex(columns=columnsTitles)
    volatility_score_board = volatility_score_board.sort_values('Volatility score',axis=0, ascending=True).reset_index(drop=True)
    ## Append the consistency scores into the calculation of total price growth score
    return volatility_score_board 


# In[63]:

volatility_table_list = [standard_dev_table,standard_dev_table,ATR_table,ATR_table,market_signal_table]
volatility_score_board =volatility_scoring(volatility_stats_table,volatility_table_list)


# # Color-allocation

# In[69]:

def color_coding(combined_profitability_board,volatility_score_board):
    color_coding_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    all_constituents_dict = {'Allianz':'Allianz', 'Adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche Bank', 'Deutsche Börse':u'Deutsche B\xf6rse', 'Deutsche Post':'Deutsche Post',
                    'Deutsche Telekom':'Deutsche Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius Medical Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': u'M\xfcnchener R\xfcckversicherungs-Gesellschaft',
                    'ProSiebenSat1 Media':'ProSiebenSat1 Media', 'RWE':'RWE', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen (VW) vz','Vonovia':'Vonovia'}
    for constituent in all_constituents_dict:
        ##Combined profitability: out of 60, Volatility out of 15
        ##print constituent
        profitability_score = combined_profitability_board['Total profitability score'].loc[combined_profitability_board['Constituent']==all_constituents_dict[constituent]].values[0]
        volatility_score = volatility_score_board['Volatility score'].loc[volatility_score_board['Constituent']==all_constituents_dict[constituent]].values[0]
        
        if profitability_score>35:
            profitability_color = 1
        elif profitability_score>19:
            profitability_color = 0
        else:
            profitability_color = -1
        
        if volatility_score<6:
            risk_color = 1
        elif volatility_score<12:
            risk_color = 0
        else:
            risk_color = -1
            
        color_coding_table=color_coding_table.append(pd.DataFrame({'Constituent':constituent, 'Profitability':profitability_color, 'Risk':risk_color},index=[0]),ignore_index=True)
    color_coding_table=color_coding_table.reindex(columns=['Constituent','Profitability','Risk'])
    
    return color_coding_table


# In[70]:

color_coding_table=color_coding(combined_profitability_board,volatility_score_board)


# In[71]:

color_coding_table.loc[color_coding_table['Constituent'].isin(['Adidas','Commerzbank','Deutsche Bank', 'EON', 'BMW'])]


# # Updating the colors on Mongodb

# In[91]:

def update_colors_mongodb(color_coding_table): 
    constituents_selected=['Adidas', 'Commerzbank', 'BMW', 'Deutsche Bank', 'EON']
    for constituent in constituents_selected:
        prof_color = color_coding_table['Profitability'].loc[color_coding_table['Constituent']==constituent].values[0]
        risk_color = color_coding_table['Risk'].loc[color_coding_table['Constituent']==constituent].values[0]
        db.summary_box.update_one({"constituent":constituent}, {"$set":{"profitability":prof_color, "risk":risk_color}})


# In[92]:

update_colors_mongodb(color_coding_table)


# In[99]:

#checking if update has been saved in the result table. 
#data=db.profitability_score.find({'Constituent':"adidas"})
#data=pd.DataFrame(list(data))
#data

