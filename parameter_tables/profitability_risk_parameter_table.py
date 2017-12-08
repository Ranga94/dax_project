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
    #Double check your shit before uploading
    assessment_param_table = pd.DataFrame()
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    mongodb_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database = 'dax_gcp'
    collection_get_fundamental_scores = 'Fundamental_ranking'
    collection_get_price_scores = 'Price_ranking'
    collection_store_profitability_scores = 'Profitability_scores'
    collection_store_profitability_ranking = 'Profitability_ranking'
    
    collection_store_risk_scores = 'Risk_scores'
    script_names= ['Profitability_ranking','Risk_ranking']
    
    table_collect_price_bq = 'pecten_dataset.Price_ranking'
    table_collect_fundamental_bq = 'pecten_dataset.Fundamental_ranking'
    project_name = 'igenie-project'
    
    for script in script_names:
        if script == 'Profitability_ranking':
            table_store_scores_bq = 'pecten_dataset.Profitability_scores'
            table_store_ranking_bq = 'pecten_dataset.Profitability_ranking'
        else: 
            table_collect_price_bq = 'pecten_dataset.price_analysis'
            table_store_scores_bq = 'pecten_dataset.Risk_scores'
            table_store_ranking_bq = 'n/a'
        assessment_param_table  = assessment_param_table.append(pd.DataFrame({'MONGODB_CONNECTION_STRING':mongodb_string,'CONSTITUENT_LIST':constituent_list,'DATABASE':database,'COLLECTION_GET_FUNDAMENTAL':collection_get_fundamental_scores,'COLLECTION_GET_PRICE':collection_get_price_scores,'COLLECTION_STORE_PROFITABILITY_SCORES':collection_store_profitability_scores,'COLLECTION_STORE_PROFITABILITY_RANKING':collection_store_profitability_ranking,'COLLECTION_STORE_RISK_SCORES':collection_store_risk_scores,
                                                                             'PROJECT_NAME_BQ':project_name,'TABLE_COLLECT_PRICE_BQ':table_collect_price_bq,'TABLE_COLLECT_FUNDAMENTAL_BQ':table_collect_fundamental_bq, 'TABLE_STORE_SCORES_BQ':table_store_scores_bq,'TABLE_STORE_RANKING_BQ':table_store_ranking_bq, 'SCRIPT_NAME':script},index=[0]),ignore_index=True)
    return assessment_param_table

if __name__ == "__main__":
    sql_string = ''mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project'
    assessment_param_table = assessment_parameter()
    dict_assessment_param =  assessment_param_table.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_PROFITABILITY_RISK_ASSESSMENT', data=dict_assessment_param)
