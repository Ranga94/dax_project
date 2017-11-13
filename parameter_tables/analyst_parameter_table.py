# -*- coding: utf-8 -*-
from pymongo import MongoClient, errors
#from google.cloud import storage
#from google.cloud.exceptions import GoogleCloudError, NotFound
import os
#import jsonpickle
from sqlalchemy import *
import json
import pandas as pd
from bs4 import BeautifulSoup
import urllib


def get_sql_data(sql_connection_string=None, sql_table_name=None,sql_column_list=None, sql_where=None):
    engine = create_engine(sql_connection_string)
    metadata = MetaData(engine)

    source_table = Table(sql_table_name, metadata, autoload=True)
    projection_columns = [source_table.columns[name] for name in sql_column_list]

    if sql_where:
        statement = select(projection_columns).where(sql_where(source_table.columns))
    else:
        statement = select(projection_columns)
    result = statement.execute()
    rows = result.fetchall()
    result.close()
    return rows

def insert_to_sql(sql_connection_string, sql_table_name, data):
    engine = create_engine(sql_connection_string)
    metadata = MetaData(engine)

    source_table = Table(sql_table_name, metadata, autoload=True)
    statement = source_table.insert().values(data)
    result = statement.execute()


##Update the Analyst parameter table - Business Insider
def data_analyst_parameter():
    url_df_bi = pd.DataFrame()
    all_constituents_dict_bi = {'Allianz':'Allianz', 'Adidas':'Adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche_Bank', 'Deutsche Börse':'Deutsche_Boerse', 'Deutsche Post':'Deutsche_Post',
                    'Deutsche Telekom':'Deutsche_Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius_Medical_Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    'Linde':'Linde_6','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': 'Munich_Re',
                    'ProSiebenSat1 Media':'ProSiebenSat1_Media', 'RWE':'RWE', 'Siemens':'Siemens', 'Thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen_vz','Vonovia':'Vonovia'}

    for constituent in all_constituents_dict_bi:
        #print constituent
        constituent_name = constituent
        url = 'http://markets.businessinsider.com/analyst/'+all_constituents_dict_bi[constituent]
        source_name = 'Business Insider'
        mongo_db_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
        database_for_storage = 'dax_gcp'
        collection_for_storage = 'analyst_opinions_all'
        day_of_update = 'Wednesday'
    
        ##See if the link is still active
        r = urllib.urlopen(url).read()
        soup = BeautifulSoup(r,'lxml')
        rating_extract = soup.find_all("div",class_="rating")
        if len(rating_extract[0].text)>0:
            status = 'active'#Need to implement a small detection
        else:
            status = 'inactive'
            print "The url for ", constituent, 'is not avaliable.'
        
        url_df_bi=url_df_bi.append(pd.DataFrame({'CONSTITUENT_NAME':constituent,'ANALYST_SOURCE_URL':url,'SOURCE_NAME':source_name,'MONGODB_CONNECTION_STRING':mongo_db_string,'DATABASE_FOR_STORAGE':database_for_storage,'COLLECTION_FOR_STORAGE':collection_for_storage,'SOURCE_STATUS':status,'DAY_OF_UPDATE':day_of_update},index=[0]),ignore_index=True)  
    url_df_bi



    ##Update the parameter table with information from Wall Street Journal.
    url_df_ws = pd.DataFrame()
    constituent_dict = {'Adidas':'ADS','Commerzbank':'CBK','Deutsche Bank':'DBK', 'EON':'EOAN', 'BMW':'BMW'}

    for constituent in constituent_dict:
    #print constituent
        constituent_name = constituent
        url = 'http://quotes.wsj.com/DE/XFRA/'+constituent_dict[constituent]+'/research-ratings'
        source_name = 'Wall Street Journal'
        mongo_db_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
        database_for_storage = 'dax_gcp'
        collection_for_storage = 'analyst_opinions'
        day_of_update = 'Wednesday'
    
        ##See if the link is still active
        r = urllib.urlopen(url).read()
        soup = BeautifulSoup(r,'lxml')
        letters = soup.find_all('div' ,class_='cr_analystRatings cr_data module')
        numbers = letters[0].find_all('span',class_='data_data')
    
        if len(numbers)>0:
            status = 'active'#Need to implement a small detection
        else:
            status = 'inactive'
            print "The url for ", constituent, 'is not avaliable.'
        
        url_df_ws=url_df_ws.append(pd.DataFrame({'CONSTITUENT_NAME':constituent,'ANALYST_SOURCE_URL':url,'SOURCE_NAME':source_name,'MONGODB_CONNECTION_STRING':mongo_db_string,'DATABASE_FOR_STORAGE':database_for_storage,'COLLECTION_FOR_STORAGE':collection_for_storage,'SOURCE_STATUS':status,'DAY_OF_UPDATE':day_of_update},index=[0]),ignore_index=True)  
    url_df_ws.head()
    return url_df_ws,url_df_bi



if __name__ == "__main__":
    url_df_ws,url_df_bi = data_analyst_parameter()
    dict_ws = url_df_ws.set_index('CONSTITUENT_NAME').T.to_dict('list')
    dict_bi = url_df_bi.set_index('CONSTITUENT_NAME').T.to_dict('list')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_ANALYST_COLLECTION', data=dict_bi)
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_ANALYST_COLLECTION', data= dict_ws )