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

def assessment_parameter():
    assessment_param_table = pd.DataFrame()
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    mongodb_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database = 'dax_gcp'
    collection_get_fundamental_scores = 'Fundamental ranking'
    collection_get_price_scores = 'Price ranking'
    collection_store_profitability_scores = 'Profitability scores'
    collection_store_profitability_ranking = 'Profitability ranking'
    collection_store_risk_scores = 'Risk scores'
    assessment_param_table  = assessment_param_table.append(pd.DataFrame({'MONGDB_CONNECTION_STRING':mongodb_string,'CONSTITUENT_LIST':constituent_list,'DATABASE':database,'COLLECTION_GET_FUNDAMENTAL':collection_get_fundamental_scores,'COLLECTION_GET_PRICE':collection_get_price_scores,'COLLECTION_STORE_PROFITABILITY_SCORES':collection_store_profitability_scores,'COLLECTION_STORE_PROFITABILITY_RANKING':collection_store_profitability_ranking,'COLLECTION_STORE_RISK_SCORES':collection_store_risk_scores},index=[0]),ignore_index=True)
    return assessment_param_table

if __name__ == "__main__":
    sql_string = 'mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project'
    assessment_param_table = assessment_parameter()
    dict_assessment_param =  assessment_param_table.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_PROFITABILITY_RISK_ASSESSMENT', data=dict_assessment_param)
