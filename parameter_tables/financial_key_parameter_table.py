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
    

def financial_parameter():
    fundamental_script_names = ['dividend_analysis','profit_margin_analysis','EBITDA_analysis','EPS_analysis','ROCE_analysis','sales_analysis','PER_analysis']
    ##Update the first row: Fundamental analysis
    param_financial_df =pd.DataFrame()
    mongodb_string_query = 'mongodb://admin:admin@ds019654.mlab.com:19654/dax' 
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'"
    database_query = 'dax'
    collection_query = 'company_data'
    mongodb_string_storage = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database_storage = 'dax_gcp'
    collection_storage = 'fundamental analysis'
    ##new data
    project_name_bq = 'igenie-project'
    table_collect_historical_bq = 'pecten_dataset.historical'
    table_collect_fundamental_bq  = 'pecten_dataset.company_data'
    table_store_analysis_bq = 'pecten_dataset.fundamental_analysis'
    
    for script in fundamental_script_names:
        param_financial_df = param_financial_df.append(pd.DataFrame({'MONGODB_CONNECTION_STRING_QUERY':mongodb_string_query,'CONSTITUENT_LIST':constituent_list,'DATABASE_FOR_QUERY':database_query,'COLLECTION_FOR_QUERY':collection_query,'MONGODB_CONNECTION_STRING_STORAGE':mongodb_string_storage,'DATABASE_FOR_STORAGE':database_storage,'COLLECTION_FOR_STORAGE':collection_storage,
                                                                     'PROJECT_NAME_BQ':project_name_bq, 'TABLE_COLLECT_HISTORICAL_BQ':table_collect_historical_bq, 'TABLE_COLLECT_FUNDAMENTAL_BQ':table_collect_fundamental_bq,'TABLE_STORE_ANALYSIS_BQ':table_store_analysis_bq,'SCRIPT_NAME':script},index=[0]),ignore_index=True)
    
    ##Update the second row: Price analysis
    price_script_names = ['cumulative_return_analysis','quarter_mean_analysis','ATR_analysis','standard_deviation_analysis','VaR_analysis','RSI_analysis']
    mongodb_string_query = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    database_query = 'dax_gcp'
    collection_query = 'historical'
    mongodb_string_storage = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database_storage = 'dax_gcp'
    collection_storage = 'price analysis'
    project_name_bq = 'igenie-project'
    table_collect_historical_bq = 'pecten_dataset.historical'
    table_collect_fundamental_bq  = 'pecten_dataset.company_data'
    table_store_analysis_bq = 'pecten_dataset.price_analysis'
    
    for script in price_script_names:
        param_financial_df = param_financial_df.append(pd.DataFrame({'MONGODB_CONNECTION_STRING_QUERY':mongodb_string_query,'CONSTITUENT_LIST':constituent_list,'DATABASE_FOR_QUERY':database_query,'COLLECTION_FOR_QUERY':collection_query,'MONGODB_CONNECTION_STRING_STORAGE':mongodb_string_storage,'DATABASE_FOR_STORAGE':database_storage,'COLLECTION_FOR_STORAGE':collection_storage,
                                                                    'PROJECT_NAME_BQ':project_name_bq, 'TABLE_COLLECT_HISTORICAL_BQ':table_collect_historical_bq, 'TABLE_COLLECT_FUNDAMENTAL_BQ':table_collect_fundamental_bq,'TABLE_STORE_ANALYSIS_BQ':table_store_analysis_bq,'SCRIPT_NAME':script},index=[0]),ignore_index=True)
    return param_financial_df



if __name__ == "__main__":
    sql_string = 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project'
    param_financial_df = financial_parameter()
    dict_financial_param = param_financial_df.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_FINANCIAL_KEY_COLLECTION', data=dict_financial_param)
