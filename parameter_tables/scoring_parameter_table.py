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
    scoring_scripts = ['Price_ranking','Fundamental_ranking','Risk_ranking']
    param_scoring_df=pd.DataFrame()
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    mongodb_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database = 'dax_gcp'
    project_name_bq = 'igenie-project'
    
    for script in scoring_scripts:
        if script == 'Fundamental_ranking':
            collection_get_analysis = 'fundamental_analysis'
            collection_store_scores =  'Fundamental_ranking'
            table_collect_analysis_bq = 'pecten_dataset.fundamental_analysis'
            table_store_scores_bq = 'pecten_dataset.Fundamental_ranking'#Not sure need to check
            
        if script == 'Price_ranking':
            collection_get_analysis  = 'price_analysis'
            collection_store_scores =  'Price_ranking'
            table_collect_analysis_bq = 'pecten_dataset.price_analysis'
            table_store_scores_bq = 'pecten_dataset.Price_ranking'
            
        if script == 'Risk_ranking':
            collection_get_analysis  = 'price_analysis'
            collection_store_scores =  'Risk_scores'
            table_collect_analysis_bq = 'pecten_dataset.price_analysis'
            table_store_scores_bq = 'pecten_dataset.Risk_scores'
            
        param_scoring_df = param_scoring_df.append(pd.DataFrame({'MONGODB_CONNECTION_STRING':mongodb_string,'CONSTITUENT_LIST':constituent_list,'DATABASE_QUERY':database,'COLLECTION_GET_ANALYSIS':collection_get_analysis,'COLLECTION_STORE_SCORES':collection_store_scores,
                                                                'PROJECT_NAME_BQ':project_name_bq,'TABLE_COLLECT_ANALYSIS_BQ':table_collect_analysis_bq, 'TABLE_STORE_SCORES_BQ':table_store_scores_bq,'SCRIPT_NAME':script},index=[0]),ignore_index=True)
                                                              
    return param_scoring_df


if __name__ == "__main__":
    sql_string = ''mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project'
    param_scoring_df = scoring_parameter()
    dict_scoring_param =  param_scoring_df.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_SCORING', data=dict_scoring_param)
