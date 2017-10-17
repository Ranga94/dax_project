
# coding: utf-8

# # Scraping Analyst Opinions

# In[50]:

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


# In[51]:

all_constituents = ['Allianz', 'Adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche_Bank', 'Deutsche_Boerse', 'Deutsche_Post','Deutsche_Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'Siemens', 'Thyssenkrupp','Vonovia','Fresenius_Medical_Care','Munich_RE','ProSiebenSat1_Media','Volkswagen_vz']
constituents_list=['Adidas', 'Commerzbank', 'BMW', 'Deutsche_Bank', 'EON']
all_constituents_dict_bi = {'Allianz':'Allianz', 'Adidas':'Adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche_Bank', 'Deutsche Börse':'Deutsche_Boerse', 'Deutsche Post':'Deutsche_Post',
                    'Deutsche Telekom':'Deutsche_Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius_Medical_Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': 'Munich_Re',
                    'ProSiebenSat1 Media':'ProSiebenSat1_Media', 'RWE':'RWE', 'Siemens':'Siemens', 'Thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen_vz','Vonovia':'Vonovia'}
#,'Vonovia':'Vonovia'


# ## Business Insider Analyst Data - Major resource

# In[52]:

#Write a function that extract analyst data for all stocks
def analyst_businessinsider(constituents_dict): 
    analyst_opinion_table = pd.DataFrame()
        
    for constituent in constituents_dict:
        
        if constituent == 'SAP':
            url = 'http://www.reuters.com/finance/stocks/analyst/SAP'
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
        
        analyst_opinion_table = analyst_opinion_table.append(pd.DataFrame({'Constituent':constituent,'Analyst rating': rating, 'Analyst recommendation': rating_result,'Buy':buy_count,'Hold':hold_count,'Sell':sell_count,'% Buy':round(buy_count*1.0/total,3),'% Hold':round(hold_count*1.0/total,3),'% Sell':round(sell_count*1.0/total,3),'Median target price':median_target, 'Highest target price':highest_target,'Lowest target price':lowest_target,'Date':datetime.date.today(),'Table':'Analyst opinions'},index=[0],),ignore_index=True)
    columnsTitles = ['Constituent','Analyst rating','Analyst recommendation', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Median target price','Highest target price','Lowest target price','Table','Date']
    analyst_opinion_table =analyst_opinion_table.reindex(columns=columnsTitles)
    return analyst_opinion_table


# ## Scraping from FT and Reuters

# In[ ]:

#Collect rating for SAP in case Business Insider doesn't work. Use FT and Reuters   
#url = 'https://markets.ft.com/data/equities/tearsheet/forecasts?s=DBKX.N:GER'
#r = urllib.urlopen(url).read()
#soup = BeautifulSoup(r,'lxml')
#letters = soup.find_all('table' ,class_='mod-ui-table mod-ui-table--colored')
#numbers = re.findall((r"\d+", letters[0].text))
#opinions_data= [int(x) for x in numbers]
                             
#Find the highest, median and lowest targets
#letters = soup.find_all('table',class_='mod-ui-table mod-ui-table--colored mod-tearsheet-forecast__table--visible')
#rows = letters[0].findAll('tr')
#data = [[td.findChildren(text=True) for td in tr.findAll("td")] for tr in rows]
#highest_target = float(data[0][2][0])
#median_target = float(data[1][2][0])
#lowest_target = float(data[2][2][0])
        
#Collect the analyst rating separately
#r = urllib.urlopen('http://www.reuters.com/finance/stocks/analyst/DBKGn.DE').read()
#soup = BeautifulSoup(r,'lxml')
#tables = soup.find_all('div', class_='moduleBody')
#analyst_recommendation = tables[2].find_all('td',class_='data')
#rating = float(analyst_recommendation[-4].text)     


# ## Wall Street Journal - Five selected constituents

# In[53]:

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
        analyst_opinions_table = analyst_opinions_table.append(pd.DataFrame({'Constituent':constituent,'Buy':buy_count,'Hold':hold_count,'Sell':sell_count,'% Buy':round(buy_count*1.0/total,3),'% Hold':round(hold_count*1.0/total,3),'% Sell':round(sell_count*1.0/total,3),'Median target price':median_target, 'Highest target price':highest_target,'Lowest target price':lowest_target,'Date':datetime.date.today(),'Table':'Analyst opinions'},index=[0],),ignore_index=True)
    columnsTitles = ['Constituent', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Median target price','Highest target price','Lowest target price','Table','Date']
    analyst_opinions_table =analyst_opinions_table.reindex(columns=columnsTitles)
    return analyst_opinions_table


# ## Combining Wall Street and Business Insider for 5 constituents

# In[54]:

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
        combined_analyst_table = combined_analyst_table.append(pd.DataFrame({'Constituent':constituent, 'Analyst rating': rating,'Analyst recommendation': recommendation,'Buy':buy,'Hold':hold,'Sell':sell,'% Buy':round(buy*1.0/total,3),'% Hold':round(hold/total,3),'% Sell':round(sell/total,3),'Median target price':round(mean_median_target,2), 'Highest target price':round(mean_highest_target,2),'Lowest target price':round(mean_lowest_target,2),'Date': str(datetime.date.today()),'Table':'Analyst opinions'},index=[0]),ignore_index=True)
    columnsTitles = ['Constituent', 'Buy','Hold','Sell','% Buy','% Hold','% Sell','Analyst rating','Analyst recommendation','Median target price','Highest target price','Lowest target price','Table','Date']
    combined_analyst_table = combined_analyst_table.reindex(columns=columnsTitles)
    return combined_analyst_table


# # Obtaining the analyst opinions 

# In[10]:

#analyst_opinions_selected.to_csv('analyst_opinions_selected.csv', encoding = 'utf-8', index = False)


# In[60]:

#Running the scraping for all the websites and combine to produce combined analyst table for five selected constituents
bi_analyst_table = analyst_businessinsider(all_constituents_dict_bi)
ws_analyst_table = analyst_wallstreet()
combined_analyst_table = combined_analyst(ws_analyst_table, bi_analyst_table)


# In[62]:

import json
combined_analyst_json = json.loads(combined_analyst_table.to_json(orient='records'))


# # Uploading results onto MongoDB

# In[63]:

client_new = MongoClient('mongodb://igenie_readwrite:igenie@35.189.101.142:27017/dax_gcp')
db = client_new.dax_gcp
#db['analyst_opinions'].drop()
#db['analyst_opinions'].insert_many(combined_analyst_json)


# In[81]:

##Or update the existing database with new values. 
def update_analyst(combined_analyst_json):
    constituent_list = ['Adidas','Commerzbank','Deutsche Bank', 'EON', 'BMW']
    i=0
    for constituent in constituent_list:
        
        db['analyst_opinions'].update_one({"constituent":constituent}, {"$set":combined_analyst_json[i]})
        i=i+1


# In[82]:

update_analyst(combined_analyst_json)


# # Check if data has been udpated

# In[84]:


# In[85]:

#analyst_retrieved 


# # Visualising the Analyst Data

# ## Horizontal stacked bar for analyst opinions

# In[326]:

#Plot horizontal barchart using data in the table, presenting only the selected constituents in constituent_list
def barh_opinions(constituent_list,analyst_opinions_table,ht): 
    # the width of the bars,usually 0.35 for five selected constituents displayed
    constituents_list=['Adidas', 'Commerzbank', 'BMW', 'Deutsche_Bank', 'EON']
    analyst_opinions_table = analyst_opinions_table[analyst_opinions_table['Constituent'].isin(constituents_list)]
    N = len(analyst_opinions_table['Constituent'])
    buy_pct = analyst_opinions_table['% Buy']*100
    hold_pct = analyst_opinions_table['% Hold']*100
    sell_pct = analyst_opinions_table['% Sell']*100
    fig = plt.clf()
    fig,ax= plt.subplots(figsize=(8,N/1.5))
    ind = np.arange(N)    # the x locations for the groups
    p1 = ax.barh(ind, sell_pct,height=ht,color='r')
    p2 = ax.barh(ind, hold_pct,height=ht, left=sell_pct,color='#ffc000')
    p3 = ax.barh(ind, buy_pct,height=ht, left=hold_pct+sell_pct,color='g')
    
    i=0
    for p in p1.patches:
            if sell_pct.iloc[i]==0: 
                ax.annotate('', (p.get_width()/3,p.xy[1]+0.1))
            else:
                ax.annotate(str(sell_pct.iloc[i])+'%', (p.get_width()/6,p.xy[1]+0.1))
            i=i+1
    i=0
    for p in p2.patches:
        ax.annotate(str(hold_pct.iloc[i])+'%', (p.xy[0]+p.get_width()/3,p.xy[1]+0.1))
        i=i+1
    i=0
    for p in p3.patches:
        ax.annotate(str(buy_pct.iloc[i])+'%', (p.xy[0]+p.get_width()/3,p.xy[1]+0.1))
        i=i+1
    
    
    #plt.ylabel('Constituents')
    plt.xlabel('Percentage of Analyst')
    plt.title('Analyst Opinions')
    plt.yticks(ind,analyst_opinions_table['Constituent'])
    plt.xticks(np.arange(0,110,10))
    # Put a legend below current axis
    #ax.legend(loc='center left', bbox_to_anchor=(0.5, -0.05),fancybox=True, shadow=True, nrow=3)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width, box.height])
    ax.legend((p1[0], p2[0], p3[0]), ('% Sell', '% Hold','% Buy'),loc='center left', bbox_to_anchor=(1, 0.5)) 
    
    plt.show()


# In[75]:

#barh_opinions(constituents_list,analyst_opinions_table,ht=0.50) 


# ## Analyst ratings and recommendation in vertical scatter plot

# In[334]:

#Presenting the target prices and analyst prediction in a scatter plot, only for selected constituents in the constituents_list
def opinion_scatter(constituents_list,analyst_opinions_table):
    colors = []
    for constituent in constituents_list: # keys are the names of the boys
        status = analyst_opinions_table['Analyst recommendation'].loc[analyst_opinions_table['Constituent']==constituent]
        if status.values[0] == 'Strong buy':
            colors.append('g')
        elif status.values[0] == 'Moderate buy':
            colors.append('#A9CE1D') #lime?
        elif status.values[0] == 'Hold':
            colors.append('#ffc000')
        elif status.values[0] == 'Moderate sell':
            colors.append('#FF8633')
        else:
            colors.append('r')
            
        plt.clf()
        
    constituents_list=['Adidas', 'Commerzbank', 'BMW', 'Deutsche_Bank', 'EON']
    analyst_opinions_table = analyst_opinions_table[analyst_opinions_table['Constituent'].isin(constituents_list)]
    X = analyst_opinions_table['Analyst rating']
    fig, ax = plt.subplots(figsize=(1,8))
    ax.scatter([1]*len(X),X, c=colors,
           marker='s', s=100)

    ax.yaxis.set_visible(True)
    ax.xaxis.set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.yaxis.set_ticks_position('left')

    #ax.get_yaxis().set_ticklabels(['what'])
    plt.ylim(min(X)-0.4,(max(X+0.4)))
    plt.ylabel('Rating')
    #plt.title('Analyst recommendation')
    plt.figtext(.5,.9,'Analyst Rating & Recommendation ', fontsize=12, ha='left')

    for constituent in constituents_list: 
        recommendation = analyst_opinions_table['Analyst recommendation'].loc[analyst_opinions_table['Constituent']==constituent]
        ax.annotate('%s, %s'%(constituent,recommendation.values[0]), xy=(1.1,analyst_opinions_table['Analyst rating'].loc[analyst_opinions_table['Constituent']==constituent]), textcoords='data')
        #ax.annotate('%s' , xy=(1.1,analyst_opinion_table['Analyst rating'].loc[analyst_opinion_table['Constituent']==constituent]-0.1), textcoords='data')
    #for xy in zip([1]*len(X),X):
        #print xy[1]
        #ax.annotate('%s' %xy[1], xy=xy, textcoords='data')
    
    plt.show()


# In[74]:

#opinion_scatter(constituents_list,analyst_opinions_table)


# In[ ]:



