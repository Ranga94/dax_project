
# coding: utf-8

# # Fundamental and Stock Price Analysis

# ### This code analyse the historical data and near-real time data to find general trends in equities. The types of analysis shown here are: 
# 
# ### -Cumulative returns for long, medium and short term
# ### -Estimating the average quarterly growth of closing price
# ### -Volatility analysis (Average True Range, standard deviation of closing price)
# ### -Financial performance analysis (Return on Capital + Sales Revenue)
# ### -Categorising equities by industry
# 

# # Extracting historial share price data from MongoDB

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

client_old = MongoClient('mongodb://admin:admin@ds019654.mlab.com:19654/dax')
client = MongoClient('mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp')
#get dax database
db_old = client_old.dax
db = client.dax_gcp
#collection = db['historical']


# In[2]:

collection = db['historical']
his = collection.find({"constituent":'Allianz'})
his = pd.DataFrame(list(his))
#his_rm21 = his['closing_price'].rolling(window=21,center=False).mean()


# In[3]:

all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']


# # Deducing Profitability 

# ## Cumulative Return analysis

# ### This section calculates the cumulative return of each equity, as if they were invested 6 months, 1 year and 5 years ago. It also returns rankings of top 5 with the best cumulative return and the worst cumulative return for these periods of investments

# In[4]:

## Cumulative Return (6 months, 1 year, 5 years)
##Table 2 stores the annual mean price and the annual mean growth
def cumulative_returns_collection():
    collection = db['historical']
    n=0
    cumulative_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer']
    for constituent in all_constituents:
        
        n=n+1
        his = collection.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        his = his.iloc[::-1] #So the most recent data is on the last row, for the convinience of analysis.
        
        
        ##Compute the 21-days moving average of the closing price. 
        his_rm21=his['closing_price'].rolling(window=21,center=False).mean()
        his_6months = his_rm21.iloc[-126:].reset_index(drop=True)
        #print float(his_6months.iloc[-1])
        his_1year = his_rm21.iloc[-252:].reset_index(drop=True)
        #print float(his_1year.iloc[-1])
        his_3years = his_rm21.iloc[-756:].reset_index(drop=True)
        
    ##Calculate the cumulative returns
        return_6months =  (float(his_6months.iloc[-1])/float(his_6months.iloc[0]))-1.0
        return_1year =  (float(his_1year.iloc[-1])/float(his_1year.iloc[0]))-1.0
        return_3years =  (float(his_3years.iloc[-1])/float(his_3years.iloc[0]))-1.0
        
    ##Develop scoring system for cumulative returns
        if (return_6months >0)&(return_1year>0)&(return_3years>0):
            score = 4
        elif ((return_6months >0)&(return_3years>0)) or (return_6months >0)&(return_1year>0):
            score = 3
        elif (return_3years>0)&(return_1year >0):
            score = 2
        elif (return_3years>0) or (return_1year >0) or (return_6months >0):
            score = 1
        else: 
            score = 0
        

    #append the values 
        cumulative_table = cumulative_table.append(pd.DataFrame({'Constituent': constituent, '6 months return': return_6months, '1 year return':return_1year,'3 years return': return_3years,'Cumulative return consistency score':score,'Table':'cumulative return analysis','Date':str(datetime.date.today()),'Status':'active'}, index=[0]), ignore_index=True)
    
    columnsTitles=['Constituent','6 months return','1 year return','3 years return','Cumulative return consistency score','Table','Status','Date']
    cumulative_table =cumulative_table .reindex(columns=columnsTitles)
    #cumulative_table.to_csv('cumulative_table %s.csv'%datetime.date.today(), encoding = 'utf-8', index = False)

    return cumulative_table 


# In[5]:

# Calculating the means and standard deviations for 1-year and 3-year returns
# Used to identify exceptionally performing stocks in terms of cumulative returns
def CR_stats(cumulative_table): 
    mean_cr_1year = cumulative_table['1 year return'].mean()
    mean_cr_3years = cumulative_table['3 years return'].mean()
    std_cr_1year = cumulative_table['1 year return'].std()
    std_cr_3years = cumulative_table['3 years return'].std()
    return mean_cr_1year,mean_cr_3years,std_cr_1year,std_cr_3years


# # Investigating the trend in stock price (quarterly)

# ### The average stock price for each quarter is calculated from 2010-01-01 (filtering the effect of recession in 2009). For each stock, a linear regression is fitted for the mean quarter prices for different three time durations.
# 1. from 2010-01-01
# 2. the last three years
# 3. the last 12 months

# ### The gradient of the linear regression model estimates the rate of change in average price per quarter (€ /quarter or 3 months). By analysing the gradients derived from the three time durations above, we can see how the rate of change vary over time. If the gradient increases from period 1 to period 3, then a trend of accelerated growth in stock price is indicated. 

# In[6]:

def quarter_mean_analysis(his):
    #Analyse the cumulative return of the stock price after the recession in 2009. Quarterly. 
    his_2010 = his[['closing_price','date']].loc[his['date']>=datetime.datetime(2010,01,01)]
    ##Calulate the mean stock price for every quarter
    n=his_2010.shape[0]
    num_quarters = int(n/63.0)
    quarter_mean = np.zeros(num_quarters)
    
    for i in range(num_quarters): 
        if i<=num_quarters-1:
            quarter_mean[i]=float(his_2010['closing_price'].iloc[63*i:63*(i+1)].mean())
        else: 
            quarter_mean[i]=float(his_2010['closing_price'].iloc[63*i:].mean())
            
    z = np.polyfit(range(num_quarters),quarter_mean,1)
    z_3yrs =np.polyfit(range(12), quarter_mean[-12:],1)
    z_1yr = np.polyfit(range(4), quarter_mean[-4:],1)
    return z[0],z[1],z_3yrs[0],z_3yrs[1],z_1yr[0],z_1yr[1],quarter_mean


# In[7]:

def quarter_mean_collection(): 
    n=0
    collection1 = db['historical']
    quarter_mean_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents = ['Allianz']
    for constituent in all_constituents:
        his=collection1.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        his = his.iloc[::-1]
        
        #assume linear model: y=ax+b
        a,b,a_3yrs,b_3yrs,a_1yr,b_1yr,quarter_mean=quarter_mean_analysis(his)
        
        if (a_1yr>0)&(a>0)&(a_3yrs>0):
            score =4
        elif (a_1yr>0)&(a>0):
            score = 3
        elif ((a_1yr>0)&(a_3yrs>0)) or ((a>0)&(a_3yrs>0)):
            score = 2
        elif (a_1yr>0) or (a>0):
            score = 1
        else:
            score = 0
        
        quarter_mean_table = quarter_mean_table.append(pd.DataFrame({'Constituent': constituent, 'Current Quarter mean price':round(quarter_mean[-1],2),'Rate of change in price from 2010/quarter': round(a,2), 'Rate of change in price in the last 3 years/quarter':round(a_3yrs,2),'Rate of change in price in the last 365 days/quarter': round(a_1yr,2),'Quarterly growth consistency score':score,'Table':'quarterly growth analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    columnsTitles = ['Constituent','Current Quarter mean price','Rate of change in price from 2010/quarter', 'Rate of change in price in the last 3 years/quarter','Rate of change in price in the last 365 days/quarter','Quarterly growth consistency score','Table','Status','Date']
    quarter_mean_table =quarter_mean_table.reindex(columns=columnsTitles)
    #quarter_mean_table.to_csv('quarter_mean_table.csv', encoding = 'utf-8', index = False)
    return quarter_mean_table


# In[8]:

# Calculating the means and standard deviations for 1-year and 3-year returns
# Used to identify exceptionally performing stocks in terms of quarter mean rate of growth.
def QM_stats(quarter_mean_table): 
    mean_qm_1year = quarter_mean_table['Rate of change in price in the last 365 days/quarter'].mean()
    mean_qm_3years = quarter_mean_table['Rate of change in price in the last 3 years/quarter'].mean()
    std_qm_1year = quarter_mean_table['Rate of change in price in the last 365 days/quarter'].std()
    std_qm_3years = quarter_mean_table['Rate of change in price in the last 3 years/quarter'].std()
    return mean_qm_1year,mean_qm_3years,std_qm_1year,std_qm_3years


# # Deducing Volatility
# 
# ### -Standard Deviation
# ### -ATR

# ## Standard Deviation of closing price

# In[9]:

##Calculate standard deviation and Bollinger Bands, then plot. 
def Bollinger(his):
    standard_dev = his['closing_price'].rolling(window=21,center=False).std()
    upper = his['closing_price'].rolling(window=21,center=False).mean() + standard_dev*2.0
    lower = his['closing_price'].rolling(window=21,center=False).mean() - standard_dev*2.0
    ##Sport extreme values,record the number of times they happen.
    above = (his['closing_price']>=upper)
    below = (his['closing_price']<=lower)
    above_dates = his.loc[above, 'date']
    below_dates = his.loc[below,'date']
    n_above = above_dates.shape[0]
    n_below = below_dates.shape[0]
    return n_above,n_below,standard_dev


# In[10]:

## Calculate the mean Standard Deviation quarterly in the last 18 months
def standard_dev_collection():
    n=0
    collection = db['historical']
    standard_dev_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        n=n+1
        his = collection.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        his = his.iloc[::-1]
        
        #print constituent
        above,below,standard_dev=Bollinger(his)
        std_3 = standard_dev[-63:].mean()
        std_3yrs = standard_dev[-756:].mean()
        std_1yr = standard_dev[-252:].mean()
        ##Set a parameter to measure the stability of the stocks for the last 18 months
        #std_mean = (std_3+std_3_to_6+std_6_to_9+std_9_to_12+std_12_to_15+std_15_to_18)/6.0
        #standard_dev_table = standard_dev_table.append(pd.DataFrame({'Constituent': constituent, 'Last 3 months': round(std_3,3), 'Last 3-6 months':round(std_3_to_6,3),'Last 6-9 months': round(std_6_to_9,3),'Last 9-12 months':round(std_9_to_12,3), 'Last 12-15 months':round(std_12_to_15,3),'Last 15-18 months':round(std_15_to_18,3),'Mean std dev(quarterly)':round(std_mean,3)}, index=[0]), ignore_index=True)
        standard_dev_table = standard_dev_table.append(pd.DataFrame({'Constituent': constituent,'Last 12 months':round(std_1yr,2),'Last 3 years':round(std_3yrs,2),'Table': 'standard deviation analysis','Date':str(datetime.date.today()),'Status':"active"},index=[0]),ignore_index=True)
    columnsTitles=['Constituent','Last 12 months','Last 3 years','Table','Status','Date']
    #standard_dev_table = standard_dev_table.sort_values('Mean std dev(quarterly)',axis=0, ascending=True).reset_index(drop=True)
    standard_dev_table =standard_dev_table.reindex(columns=columnsTitles)
    #standard_dev_table.to_csv('standard_dev_table.csv', encoding = 'utf-8', index = False)
    return standard_dev_table


# ## Average True Range

# In[11]:

##Calculate the 14-day Average True Range
##For the first 14 days, TR = High-Low
##For the days after: ATR(current) = (ATR(previous) x 13 + TR)/14
def ATR_calculate(his):
    TR = his['daily_high'].iloc[0:14]-his['daily_low'].iloc[0:14]
    ATR0 = TR.mean()
    n = his.shape[0]
    ATR_array = np.zeros(n)
    ATR_array[13]=ATR0
    for i in np.arange(14,n):
        ATR = (his['daily_high'].iloc[i] - his['daily_low'].iloc[i] + ATR0 * 13)/14.0
        ATR_array[i] = ATR
        ATR0 = ATR
    return ATR_array


# In[12]:

##Record the current ATR, the average ATR of this year, the average ATR in the last 5 years
def ATR_collection():
    collection = db['historical']
    ATR_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']

    for constituent in all_constituents:
        his = collection.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        his = his.iloc[::-1]
        
        ATR_array = ATR_calculate(his)
        ATR_table = ATR_table.append(pd.DataFrame({'Constituent': constituent,'Current 14-day ATR': round(ATR_array[-1],2), 'Average ATR in the last 12 months': round(ATR_array[-252:].mean(),2), 'Average ATR in the last 3 years':round(ATR_array[-756:].mean(),2),'Table':'ATR analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    
    columnsTitles=['Constituent','Current 14-day ATR','Average ATR in the last 12 months', 'Average ATR in the last 3 years','Table','Status','Date']
    ATR_table=ATR_table.reindex(columns=columnsTitles)
    ATR_table
    return ATR_table


# # Financial Performance of the Company

# ### Return on Capital Employed (ROCE) is a ratio that indicates the profitability and efficiency of a company, i.e. its profit vs. the total amount of capital used (see formula below).  
# 
# ### Return on Capital Employed = annual net profit/total assets – total liabilities
# 
# ### Sale is one of the biggest sources of profits for most of the equities, hence also taken into account to assess the financial ability of a company. 
# 
# ### Dividend
# 
# ### Profit margin = income to the company per euro of revenue/sale. 
# 
# ### PER - investor expectation
# 
# ### EPS - company's profitability from investment
# 
# ### EBITDA - company's earning before tax, amortization and depreciation, reflects profitability
# 

# ## Return on Capital Employed

# In[13]:

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
    return float(pct_ROCE_last_year), float(pct_ROCE_four_years), joined[['ROCE','year']]


# ## Sales Revenue

# In[14]:

def sales_calculate(master):
    table= master[['Sales in Mio','year']].dropna(thresh=2)
    #print table
    table['Sales in Mio']=table['Sales in Mio'].str.replace(",","").astype(float)
    #print float(table['Sales in Mio'].iloc[-1])
    pct_sales_last_year = 100*(float(table['Sales in Mio'].iloc[-1])-float(table['Sales in Mio'].iloc[-2]))/float(table['Sales in Mio'].iloc[-2])
    pct_sales_four_years = 100*(float(table['Sales in Mio'].iloc[-1])-float(table['Sales in Mio'].iloc[-4]))/float(table['Sales in Mio'].iloc[-4])
    return float(pct_sales_last_year), float(pct_sales_four_years), table['Sales in Mio']


# ## ROCE and Sales analysis

# In[15]:

##Table for company performance, ROCE and Sales Revenue
def ROCE_and_sales_collection():
    collection = db_old['company_data']
    ROCE_coll_table = pd.DataFrame()
    sales_coll_table = pd.DataFrame()
    #'Commerzbank' after 'BMW', all debt NaN,Deutsche Bank' after'Daimler',no data avaliable for 'Volkswagen (VW) vz'ranked last
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Continental', 'Daimler', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media']
    for constituent in all_constituents:
        master = collection.find({"constituent":constituent,'table':'Historical Key Data'})
        master = pd.DataFrame(list(master))
        pct_ROCE_last_year, pct_ROCE_four_years, ROCE_table = ROCE_calculate(master)
        pct_sales_last_year, pct_sales_four_years, sales_table = sales_calculate(master)
        
        if (pct_ROCE_last_year>0) & (pct_ROCE_four_years>0):
            ROCE_score = 2
        elif pct_ROCE_last_year>0:
            ROCE_score =1
        else: 
            ROCE_score = 0
        
        if (pct_sales_last_year>0) & (pct_sales_four_years>0):
            sales_score = 2
        elif pct_sales_last_year>0:
            sales_score =1
        else: 
            sales_score = 0
        
        ROCE_coll_table = ROCE_coll_table.append(pd.DataFrame({'Constituent': constituent, 'Current ROCE': round(ROCE_table['ROCE'].iloc[-1],2), '% change in ROCE from previous year':round(pct_ROCE_last_year,2),'% change in ROCE from 4 years ago': round(pct_ROCE_four_years,2),'ROCE score':ROCE_score,'Table':'ROCE analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
        sales_coll_table = sales_coll_table.append(pd.DataFrame({'Constituent': constituent,'Current sales in Mio':round(sales_table.iloc[-1],2), '%change in Sales from previous year':round(pct_sales_last_year,2),'%change in Sales from 4 years ago':round(pct_sales_four_years,2),'Sales score':sales_score,'Table':'Sales analysis','Date':str(datetime.date.today()),'Status':"active"},index=[0]),ignore_index=True)
        
    columnsTitles_ROCE = ['Constituent', 'Current ROCE','% change in ROCE from previous year','% change in ROCE from 4 years ago','ROCE score','Table','Status','Date']
    columnsTitles_sales=['Constituent','Current sales in Mio', '%change in Sales from previous year','%change in Sales from 4 years ago','Sales score','Table','Status','Date']
    
    ROCE_coll_table =ROCE_coll_table.reindex(columns=columnsTitles_ROCE)
    sales_coll_table =sales_coll_table.reindex(columns=columnsTitles_sales)
    return ROCE_coll_table, sales_coll_table


# ## Dividend and Dividend Yield Analysis

# In[16]:

#Computes the linear regression model for dividend, and produce list of years where dividend is offered. 
def dividend_analysis(div):
    div = div[['Value','Last Dividend Payment']].dropna(thresh=1)
    div = pd.DataFrame(div)
    value = [unicode(x) for x in div["Value"]]
    value = [x.replace(u'\u20ac',"") for x in value]
    value = [float(x) for x in value]
    n=len(value)
    current_div = value[0] 
    z = np.polyfit(range(n),value[::-1],1)
    estimation = [x*z[0]+z[1] for x in range(n)]
    res = map(operator.sub, value[::-1], estimation)
    mse = sum([x**2 for x in res])/n*1.0
    ##Find out the years where dividend is offered
    #Volkswagen (VW) vz, BMW, RWE: half year
    date_list = pd.DatetimeIndex(div['Last Dividend Payment'])
    year_list = date_list.year
    return z[0],z[1],mse,year_list,current_div


# In[17]:

##Dividend yield is not avaliable
def dividend_yield_analysis(master):
    dividend_yield_table = master[['Dividend yield %','year']].dropna(thresh=2)
    #if constituent !='Commerzbank':
    dividend_yield_table['Dividend yield %']=dividend_yield_table['Dividend yield %'].str.replace("%","")
    ##drop the empty cells and convert to float
    filter = dividend_yield_table['Dividend yield %'] != ''
    dividend_yield_table['Dividend yield %']=dividend_yield_table[filter].astype(float)


# In[18]:

##This dividend table stores the results of linear regression, and the list of years when dividends are offered
##'Commerzbank','Deutsche Bank','Lufthansa','RWE','thyssenkrupp','Vonovia','Volkswagen (VW) vz'
def dividend_collection():
    collection = db_old['company_data']
    collection2 = db['historical']
    dividend_table = pd.DataFrame()
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank','Continental', 'Daimler', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde', 'Merck', 'SAP', 'Siemens','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media']
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        div = collection.find({"constituent":constituent,'table':'Dividend'})
        div = pd.DataFrame(list(div))
        his = collection2.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        current_price = float(his['closing_price'].iloc[0])
        
        a,b,mse,year_list,current_div=dividend_analysis(div) #a represents the average growth of dividend per year in €
        if constituent == 'BMW' or 'Volkswagen (VW) vz' or 'RWE':
            a = a*2.0
        
        score =0
        if mse<0.5:
            score +=2
            future_dividend = current_div + a
            intrinsic_val = future_dividend*1.0/current_price + a/current_div
        else: 
            future_dividend ='n/a'
            score = score+0
            intrinsic_val = 'n/a'
        
        dividend_table = dividend_table.append(pd.DataFrame({'Constituent': constituent, 'Current dividend': current_div, 'Average rate of dividend growth /year':round(a,2),'Mean square error of fitting': round(mse,2),'Years of dividend offer':'%s'%year_list,'Estimated dividend next year':future_dividend,'Current share price':current_price,'Gordon growth estimated return':intrinsic_val,'Dividend consistency score':score,'Table':'dividend analysis','Status':"active",'Date':str(datetime.date.today())}, index=[0]), ignore_index=True)
    
    columnsTitles = ['Constituent', 'Current dividend','Average rate of dividend growth /year','Mean square error of fitting','Years of dividend offer','Current share price','Gordon growth estimated return','Dividend consistency score','Table','Status','Date']
    dividend_table =dividend_table.reindex(columns=columnsTitles)
    dividend_table = dividend_table.sort_values('Current dividend',axis=0, ascending=False).reset_index(drop=True)
    #dividend_table.to_csv('dividend_table.csv', encoding = 'utf-8', index = False)
    return dividend_table


# ## Dividend yield estimation

# In[19]:

#The dividend yield is incomplete
collection1 = db_old['company_data']
master=collection1.find({"constituent":'Commerzbank','table':'Historical Key Data'})
master = pd.DataFrame(list(master))
dividend_yield_table = master[['Dividend yield %','year']].dropna(thresh=2)
dividend_yield_table['Dividend yield %']=dividend_yield_table['Dividend yield %'].str.replace("%","")
dividend_yield_table['Dividend yield %']=dividend_yield_table['Dividend yield %'].replace('', np.nan)


# ## Profit Margin Analysis

# In[20]:

def profit_margin_calculator(master):
    sales = master[['Sales in Mio','year']].dropna(thresh=2)
    net_profit=master[['Net profit','year']].dropna(thresh=2)
    sales['Sales in Mio']=sales['Sales in Mio'].str.replace(",","").astype(float)
    net_profit['Net profit']=net_profit['Net profit'].str.replace(",","").astype(float)
    profit_margin_table = net_profit.merge(sales,on='year',how='inner')
    profit_margin_calculation = [float(net_profit['Net profit'].iloc[i])*100.0/float(sales['Sales in Mio'].iloc[i]) for i in range (sales.shape[0])]
    return profit_margin_calculation


# In[21]:

def profit_margin_collection():
    collection = db_old['company_data']
    profit_margin_table = pd.DataFrame()
    #'Volkswagen (VW) vz' does not receive any data
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media']
    for constituent in all_constituents:
        master = collection.find({"constituent":constituent,'table':'Historical Key Data'})
        master = pd.DataFrame(list(master))
        profit_margin_calculation = profit_margin_calculator(master)
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
        
        profit_margin_table = profit_margin_table.append(pd.DataFrame({'Constituent': constituent, 'Current profit margin':current_pm,'Profit margin last year':pm_last_year,'% change in profit margin last year':pct_last_year,'Profit margin 4 years ago': pm_four_years_ago,'% change in profit margin 4 years ago':pct_four_years_ago,'Table':'profit margin analysis','Profit margin score':score,'Date':str(datetime.date.today()),'Status':"active" }, index=[0]), ignore_index=True)
    columnsTitles = ['Constituent', 'Current profit margin','Profit margin last year','Profit margin 4 years ago','Profit margin score','Table','Status','Date']
    profit_margin_table =profit_margin_table.reindex(columns=columnsTitles)
    profit_margin_table = profit_margin_table.sort_values('Current profit margin',axis=0, ascending=False).reset_index(drop=True)
    #dividend_table.to_csv('dividend_table.csv', encoding = 'utf-8', index = False)
    return profit_margin_table


# ## PER Analysis

# In[22]:

#'Volkswagen (VW) vz' not found
def PER_collection():
    n=0
    collection = db_old['company_data']
    PER_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media']
    #all_constituents = ['Allianz']
    for constituent in all_constituents:
        master = collection.find({"constituent":constituent,'table':'Historical Key Data'})
        master = pd.DataFrame(list(master))
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
        
        PER_table = PER_table.append(pd.DataFrame({'Constituent': constituent, 'Current PER':current_PER,'PER last year':last_year_PER,'% change in PER from last year':pct_last_year,'PER 4 years ago': four_years_ago_PER,'% change in PER from 4 years ago':pct_last_year,'PER score':score,'Table':'PER analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    columnsTitles = ['Constituent', 'Current PER','PER last year','PER 4 years ago','% change in PER from last year','PER score','Table','Status','Date']
    PER_table =PER_table.reindex(columns=columnsTitles)
    PER_table = PER_table.sort_values('Current PER',axis=0, ascending=False).reset_index(drop=True)
    #dividend_table.to_csv('dividend_table.csv', encoding = 'utf-8', index = False)
    return PER_table


# ## EPS Analysis

# In[23]:

#No data avaliable for'Volkswagen (VW) vz'
def EPS_collection():
    collection = db_old['company_data']
    EPS_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media']
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        #print constituent
        master = collection.find({"constituent":constituent,'table':'Historical Key Data'})
        master = pd.DataFrame(list(master))
        EPS = master[['Earning per share','year']].dropna(thresh=2)
        current_EPS = float(EPS['Earning per share'].iloc[-1])
        last_year_EPS = float(EPS['Earning per share'].iloc[-2])
        four_years_ago_EPS = float(EPS['Earning per share'].iloc[-4])
        pct_last_year = (current_EPS-last_year_EPS)*100.0/last_year_EPS
        pct_four_years = (current_EPS-four_years_ago_EPS)*100.0/four_years_ago_EPS
        if (pct_last_year>0) & (pct_four_years>0):
            score = 2
        elif pct_last_year>0:
            score =1
        else: 
            score = 0
        EPS_table = EPS_table.append(pd.DataFrame({'Constituent': constituent, 'Current EPS':current_EPS,'EPS last year':last_year_EPS, '% change in EPS from last year': round(pct_last_year,2),'EPS score': score,'EPS 4 years ago': four_years_ago_EPS,'% change in EPS from 4 years ago':round(pct_four_years,2),'Table':'EPS analysis','Date':str(datetime.date.today()),'Status':"active" }, index=[0]), ignore_index=True)
    columnsTitles = ['Constituent', 'Current EPS','EPS last year','% change in EPS from last year','EPS 4 years ago','% change in EPS from 4 years ago','EPS score','Table','Status','Date']
    EPS_table =EPS_table.reindex(columns=columnsTitles)
    EPS_table = EPS_table.sort_values('Current EPS',axis=0, ascending=False).reset_index(drop=True)
    return EPS_table


# ## EBITDA Analysis

# In[24]:

def EBITDA_collection():
    collection = db_old['company_data']
    EBITDA_table = pd.DataFrame()
    #No data avaliable for'Volkswagen (VW) vz', Commerzbank, Deutsche Bank
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Continental', 'Daimler', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media']
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        master = collection.find({"constituent":constituent,'table':'Historical Key Data'})
        master = pd.DataFrame(list(master))
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
        EBITDA_table = EBITDA_table.append(pd.DataFrame({'Constituent': constituent, 'Current EBITDA in Mio':current_EBITDA,'EBITDA last year in Mio':last_year_EBITDA, '% change in EBITDA from last year': round(pct_last_year,2),'EBITDA score': score,'EBITDA 4 years ago in Mio': four_years_ago_EBITDA,'% change in EBITDA from 4 years ago':round(pct_four_years,2),'Table':'EBITDA analysis','Date':str(datetime.date.today()),'Status':"active" }, index=[0]), ignore_index=True)
    columnsTitles = ['Constituent', 'Current EBITDA in Mio','EBITDA last year in Mio','% change in EBITDA from last year','EBITDA 4 years ago in Mio','% change in EBITDA from 4 years ago','EBITDA score','Table','Status','Date']
    EBITDA_table =EBITDA_table.reindex(columns=columnsTitles)
    EBITDA_table = EBITDA_table.sort_values('Current EBITDA in Mio',axis=0, ascending=False).reset_index(drop=True)
    return EBITDA_table


# # Market Signal 

# ## Momentum - RSI Analysis

# In[25]:

def RSI_calculate(his,n):
    delta = his['closing_price'].diff()
    dUp, dDown = delta.copy(), delta.copy()
    dUp[dUp < 0] = 0
    dDown[dDown > 0] = 0    
    RolUp = dUp.rolling(window=n).mean()
    RolDown = dDown.rolling(window=n).mean().abs()
    RS = RolUp/RolDown+0.0
    a=RS.shape[0]
    RSI = np.zeros(a)
    for i in np.arange(n,a):
        RSI[i] = 100-100/(1.0+RS[i])
    #If > 70: overbought signal, <30: oversold signal
    RSI_last_year = RSI[-252:]
    #print RSI[-90:]
    overbought = (RSI_last_year>=70)
    oversold = (RSI_last_year<=30)
    overbought_count = RSI_last_year[overbought].shape[0]
    oversold_count = RSI_last_year[oversold].shape[0]
    return RSI[-1],overbought_count/252.0,oversold_count/252.0


# ## Golden Cross Analysis

# In[26]:

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
    
    
    return freq,ave_golden_duration,ave_golden_growth,recent_cross,diverge,max_price,min_price,days_after_extreme,extreme_status,pct_diff_from_extreme


# In[27]:

#Note: Henkel_vs(Henkel vs) does not have data
def crossing_and_RSI_collection():
    collection = db['historical']
    table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        his = collection.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        his=his.iloc[::-1].reset_index(drop=True)
        
        
        cross_interval,ave_golden_duration,ave_golden_growth,recent_cross,diverge,max_price,min_price,days_after_extreme,extreme_status,pct_diff_from_extreme = crossing_analysis(his)
        RSI_current,overbought,oversold = RSI_calculate(his,14)
        
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
        if ave_golden_duration>cross_interval:
            cross_score +=1
        else: 
            cross_score=cross_score
            
    
        
        if RSI_current > 70:
            RSI_score = 1
        elif RSI_current < 30:
            RSI_score = -1
        else: 
            RSI_score =0
        
        if overbought>oversold:
            RSI_score = RSI_score+1
        else: 
            RSI_score=RSI_score
        
        
        table = table.append(pd.DataFrame({'Constituent': constituent,'Price on the latest date': float(his.iloc[-1].closing_price),'Average crossing interval (days)': cross_interval,'Crossing frequency per year':round(365.0/cross_interval,2), 'Duration of Golden Cross(days)':int(ave_golden_duration), 'Average return per Golden Cross period':round(ave_golden_growth,2),'Recent cross':recent_cross, 'Status of SMA 50':diverge,'Maximum price after cross':max_price,'Minimum price after cross':min_price,'Days elaspsed from the last max/min':days_after_extreme,'Max/min observation':extreme_status,'% change in price after max/min':pct_diff_from_extreme,'Current RSI':round(RSI_current,2),'% days overbought':round(overbought*100,2),'% days oversold':round(oversold*100,2),'Bull score (crossing)': cross_score,'Bull score (RSI)':RSI_score,'Table':'Market signal','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
        ##Find golden cross duration times, and expected growth of stock per unit time. 
    columnsTitles=["Constituent","Price on the latest date",'Average crossing interval (days)','Crossing frequency per year','Duration of Golden Cross(days)','Average return per Golden Cross period','Recent cross','Status of SMA 50','Maximum price after cross','Minimum price after cross','Days elaspsed from the last max/min','Max/min observation','% change in price after max/min','Current RSI','% days overbought','% days oversold', 'Bull score (crossing)','Bull score (RSI)','Table','Status','Date']
    table=table.reindex(columns=columnsTitles)
    #table.to_csv('general_info%s.csv'%datetime.date.today(), encoding = 'utf-8', index = False)
    return table


# ## Value at Risk at 99 Confidence

# In[ ]:

def student_VAR(n,alpha):
    collection = db['historical']
    var_table = pd.DataFrame()
    all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', 'Deutsche Börse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    for constituent in all_constituents:
        print constituent
        his = collection.find({"constituent":constituent})
        his = pd.DataFrame(list(his))
        his = his.iloc[::-1] 
        
        ##Calculate the daily returns of stock
        ret = (his['closing_price']-his['closing_price'].shift(n)*1.0)/his['closing_price'].shift(n)
        ##Normal distribution best fit, mu_norm=mean, sig_norm=standard deviation
        mu_norm, sig_norm = norm.fit(ret[n:].values)
        
        #Set parameters for displaying the distribution
        dx = 0.0001  # resolution
        x = np.arange(-0.4, 0.4, dx)
        pdf = norm.pdf(x, mu_norm, sig_norm)

        # Student t distribution best fit (finding: nu, which is degrees of freedom)
        parm = t.fit(ret[n:].values)
        nu, mu_t, sig_t = parm
        nu = np.round(nu)
        pdf2 = t.pdf(x, nu, mu_t, sig_t)

        #Set parameters for calculating value at risk from the distribution
        h = 1 #period of investment
        alpha = 0.01  # significance level
        lev = 100*(1-alpha)
        xanu = t.ppf(alpha, nu)
 
        #CVaR_n = alpha**-1 * norm.pdf(norm.ppf(alpha))*sig_norm - mu_norm
        #VaR_n = norm.ppf(1-alpha)*sig_norm - mu_norm

        VaR_t = np.sqrt((nu-2)/nu) * t.ppf(1-alpha, nu)*sig_norm  - h*mu_norm
        CVaR_t = -1/alpha * (1-nu)**(-1) * (nu-2+xanu**2) * t.pdf(xanu, nu)*sig_norm  - h*mu_norm
        
        var_table = var_table.append(pd.DataFrame({'Constituent': constituent,'Investment period': n ,'Average return': mu_norm, 'Standard deviation':sig_norm,'Confidence level': alpha,'Value at Risk': VaR_t,'Expected Shortfall': CVaR_t,'Table':'VAR analysis','Date':str(datetime.date.today()),'Status':'active'}, index=[0]), ignore_index=True)
    
    columnsTitles=['Constituent','Investment period','Average return','Standard deviation','Confidence level','Value at Risk','Expected Shortfall','Date','Status']
    var_table =var_table.reindex(columns=columnsTitles)
    
    return var_table


# # Analyst Opinions

# In[28]:

constituents_selected=['Adidas', 'Commerzbank', 'BMW', 'Deutsche_Bank', 'EON']
all_constituents_dict = {'Allianz':'Allianz', 'adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche_Bank', 'Deutsche Börse':'Deutsche_Boerse', 'Deutsche Post':'Deutsche_Post',
                    'Deutsche Telekom':'Deutsche_Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius_Medical_Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': 'Munich_Re',
                    'ProSiebenSat1 Media':'ProSiebenSat1_Media', 'RWE':'RWE', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen_vz','Vonovia':'Vonovia'}
#Data for SAP is missing. 


# In[31]:

#Write a function that extract analyst data for all stocks
def analyst_businessinsider(constituents_dict): 
    analyst_opinion_table = pd.DataFrame()
    for constituent in constituents_dict:
        
        if constituent == 'SAP':
            url = 'http://www.reuters.com/finance/stocks/analyst/SAP'
            r = urllib.urlopen(url).read()
            soup = BeautifulSoup(r,'lxml')
            tables = soup.find_all('div', class_='moduleBody')
            analyst_recommendation = tables[2].find_all('td',class_='data')
            rating = float(analyst_recommendation[-4].text)
            
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
        
        analyst_opinion_table = analyst_opinion_table.append(pd.DataFrame({'Constituent':constituent,'Analyst rating': rating, 'Analyst recommendation': rating_result,'Buy':buy_count,'Hold':hold_count,'Sell':sell_count,'% Buy':round(buy_count*1.0/total,3),'% Hold':round(hold_count*1.0/total,3),'% Sell':round(sell_count*1.0/total,3),'Median target price':median_target, 'Highest target price':highest_target,'Lowest target price':lowest_target,'Date':datetime.date.today(),'Table':'Analyst opinions','Status':"active"},index=[0],),ignore_index=True)
    columnsTitles = ['Constituent','Analyst rating','Analyst recommendation', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Median target price','Highest target price','Lowest target price','Table','Status','Date']
    analyst_opinion_table =analyst_opinion_table.reindex(columns=columnsTitles)
    return analyst_opinion_table


# # Categorising companies by industry

# In[29]:

def industry_categorisation():
    category_table = pd.DataFrame()
    array = ['adidas','Clothing','Allianz','Insurance','BASF','Chemicals','Bayer','Pharmaceuticals','Beiersdorf','Chemicals',
 'BMW','Manufacturing','Commerzbank','Banking','Continental','Manufacturing','Daimler','Manufacturing','Deutsche Bank','Banking',
 'Deutsche Börse','Securities','Lufthansa','Transport Aviation','Deutsche Post','Logistics','Deutsche Telekom','Communications',
 'EON','Energy','Fresenius','Medical','Fresenius Medical Care','Medical','HeidelbergCement','Building',
 'Infineon','Semiconductors','Linde','Industrial gases','Merck','Pharmaceuticals','Münchener Rückversicherungs-Gesellschaft','Insurance',
 'ProSiebenSat1 Media','Media','RWE','Energy','SAP','Software','Siemens','Industrial','thyssenkrupp','Manufacturing',
'Volkswagen (VW) vz','Manufacturing','Vonovia','Real estate']
    n=len(array)
    for i in range(int(n/2)):
        category_table = category_table.append(pd.DataFrame({'Constituent': array[i*2], 'Industry':array[2*i+1],'Table': 'category analysis' },index=[0]), ignore_index=True)
    category_table = pd.DataFrame(category_table)
    #category_table.to_csv('industry_category_table.csv', encoding = 'utf-8', index = False)
    return category_table


# # Running all the analysis

# In[30]:

def all_analysis(all_constituents_dict):
    category_table=industry_categorisation()
#Profitability(baesd on historical stock price) 
    cumulative_returns_table = cumulative_returns_collection() #6mn, 1yr,3yr
    quarter_mean_table = quarter_mean_collection()#1yr,3yr,7yr
    market_signal_table = crossing_and_RSI_collection()

#Volatility measure (based on historical stock price)
    standard_dev_table = standard_dev_collection()#1yr,3yr
    ATR_table = ATR_collection()#1yr,3yr
    
#Balance sheet analysis, may be qualitative (data only released once per year, limitations of data)
    ROCE_table, sales_table = ROCE_and_sales_collection()
    dividend_table = dividend_collection()
    profit_margin_table = profit_margin_collection()
    PER_table=PER_collection()
    EPS_table=EPS_collection()
    EBITDA_table = EBITDA_collection()
    
##Append missing values into the ROCE table
    ROCE_table = ROCE_table.append(pd.DataFrame({'Constituent':'Commerzbank','ROCE in 2016':0.05,'Status':"active"},index=[0]),ignore_index=True)
    ROCE_table = ROCE_table.append(pd.DataFrame({'Constituent':'Deutsche Bank','ROCE in 2016':-0.01,'Status':"active"},index=[0]),ignore_index=True)
    ROCE_table = ROCE_table.append(pd.DataFrame({'Constituent':'Volkswagen (VW) vz','ROCE in 2016':0.03,'Status':"active"},index=[0]),ignore_index=True)

##Append missing values for Volkswagen for PER and Profit Margin table
    PER_table=PER_table.append(pd.DataFrame({'Constituent':'Volkswagen (VW) vz','Current PER':13.0,'Status':"active"},index=[0]),ignore_index=True)
    profit_margin_table = profit_margin_table.append(pd.DataFrame({'Constituent':'Volkswagen (VW) vz','Current profit margin':0.0373,'Profit margin last year':'NaN','Profit margin 4 years ago':'NaN','Status':"active"},index=[0]),ignore_index=True)

##Tables like sales can be used for industry comparison. 
    return category_table,cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,ROCE_table, sales_table ,dividend_table,profit_margin_table,PER_table,EPS_table,EBITDA_table, market_signal_table 


# # Obtaining all the result tables and uploading on MongoDB

# In[31]:

#Obtaining results table for most of fundamental analysis (uploaded on MongoDB individually)
#Cumulative returns, quarter mean growth, ATR, dividend, Return on Capital Employed, Profit margin, PER and Sales
industry_category_table, cumulative_returns_table,quarter_mean_table,standard_dev_table,ATR_table,ROCE_table, sales_table ,dividend_table,profit_margin_table,PER_table,EPS_table,EBITDA_table, market_signal_table =all_analysis(all_constituents_dict)




# ## Converting results to JSON files

# In[37]:

import json
## Converting all the tables into JSON for inserting into MongoDB
##Stock price analysis, to be updated every week
cumulative_returns_json = json.loads(cumulative_returns_table.to_json(orient='records'))
quarter_mean_json = json.loads(quarter_mean_table.to_json(orient='records'))
standard_dev_json = json.loads(standard_dev_table.to_json(orient='records'))
ATR_json = json.loads(ATR_table.to_json(orient='records'))

##Market json analysis
market_signal_json = json.loads(market_signal_table.to_json(orient='records'))
industry_category_json = json.loads(industry_category_table.to_json(orient='records'))

##Balance sheet analysis, to be updated every 6 months
ROCE_json = json.loads(ROCE_table.to_json(orient='records'))
sales_json = json.loads(sales_table.to_json(orient='records'))
dividend_json = json.loads(dividend_table.to_json(orient='records'))
profit_margine_json = json.loads(profit_margin_table.to_json(orient='records'))
PER_json = json.loads(PER_table.to_json(orient='records'))
EPS_json = json.loads(EPS_table.to_json(orient='records'))
EBITDA_json = json.loads(EBITDA_table.to_json(orient='records'))


# ## Inserting JSON results onto MongoDB

# In[51]:


##Empty the result database before updating it with new results. 
db = client.dax_gcp
collection1 = db['fundamental analysis']
collection2 = db['price analysis']

#Append status on the earlier collections (will not need to repeat again)
#collection1.update_many({}, {'$set': {'Status': 'active'}},True,True)
#collection2.update_many({}, {'$set': {'Status': 'active'}},True,True)

##alter the status of collection
#collection1.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
collection2.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
db['fundamental analysis'].drop()


# In[53]:

## Insert new results into databases. 
## insert into db['fundamental analysis']
collection1.insert_many(industry_category_json)
collection1.insert_many(ROCE_json) #table: ROCE analysis
collection1.insert_many(sales_json) #table: Sales analysis
collection1.insert_many(dividend_json) #table:dividend analysis
collection1.insert_many(profit_margine_json) #table:profit margin analysis
collection1.insert_many(PER_json) #table:PER analysis
collection1.insert_many(EPS_json) #table:EPS analysis
collection1.insert_many(EBITDA_json) #table:EBIDTA analysis


## insert into db['price analysis']
collection2.insert_many(cumulative_returns_json) #table: cumulative return analysis
collection2.insert_many(quarter_mean_json) #table: quarterly growth analysis
collection2.insert_many(standard_dev_json) #table: standard deviation analysis
collection2.insert_many(ATR_json) #table: ATR analysis
collection2.insert_many(market_signal_json) #table: Market signal
collection2.insert_many(dividend_json) #table:dividend analysis

