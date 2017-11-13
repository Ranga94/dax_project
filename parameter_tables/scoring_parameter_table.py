# -*- coding: utf-8 -*-

from pymongo import MongoClient, errors
#from google.cloud import storage
#from google.cloud.exceptions import GoogleCloudError, NotFound
import os
#import jsonpickle
from sqlalchemy import *
import json
import pandas as pd


def insert_to_sql(sql_connection_string, sql_table_name, data):
    engine = create_engine(sql_connection_string)
    metadata = MetaData(engine)

    source_table = Table(sql_table_name, metadata, autoload=True)
    statement = source_table.insert().values(data)
    result = statement.execute()
    

def scoring_parameter():
    # Fundamental Scoring from fundamental analysis
    param_scoring_df=pd.DataFrame()
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    mongodb_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database = 'dax_gcp'
    collection_get_analysis = 'fundamental analysis'
    collection_store_scores =  'Fundamental ranking'
    param_scoring_df = param_scoring_df.append(pd.DataFrame({'MONGODB_CONNECTION_STRING':mongodb_string,'CONSTITUENT_LIST':constituent_list,'DATABASE':database,'COLLECTION_GET_ANALYSIS':collection_get_analysis,'COLLECTION_STORE_SCORES':collection_store_scores},index=[0]),ignore_index=True)
    
    # Price Scoring from price analysis
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    mongodb_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database = 'dax_gcp'
    collection_get_analysis  = 'price analysis'
    collection_store_scores =  'Price ranking'
    param_scoring_df = param_scoring_df.append(pd.DataFrame({'MONGODB_CONNECTION_STRING':mongodb_string,'CONSTITUENT_LIST':constituent_list,'DATABASE':database,'COLLECTION_GET_ANALYSIS':collection_get_analysis,'COLLECTION_STORE_SCORES':collection_store_scores},index=[0]),ignore_index=True)
    return param_scoring_df


if __name__ == "__main__":
    sql_string = 'mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project'
    param_scoring_df = scoring_parameter()
    dict_scoring_param =  param_scoring_df.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_SCORING', data=dict_scoring_param)