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


def insert_to_sql(sql_connection_string, sql_table_name, data):
    engine = create_engine(sql_connection_string)
    metadata = MetaData(engine)

    source_table = Table(sql_table_name, metadata, autoload=True)
    statement = source_table.insert().values(data)
    result = statement.execute()

def summary_parameter():
    summary_param_table = pd.DataFrame()
    constituent_list = "'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Börse','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','Münchener Rückversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz'" 
    mongodb_string = 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp'
    database = 'dax_gcp'
    collection_get_profitability = 'Profitability scores'
    collection_get_risk = 'Risk scores'
    collection_store_summary = 'summary_box'
    project_name = 'igenie-project'
    table_collect_profitability_scores_bq = 'pecten_dataset.Profitability_scores'
    table_collect_risk_scores_bq = 'pecten_dataset.Risk_scores'
    script_name = 'Color_ranking'
    table_store_summary_bq = 'pecten_dataset.summary_box'
    summary_param_table=summary_param_table.append(pd.DataFrame({'MONGODB_CONNECTION_STRING':mongodb_string,'CONSTITUENT_LIST':constituent_list,'DATABASE':database,'COLLECTION_GET_PROFITABILITY_SCORE':collection_get_profitability,'COLLECTION_GET_RISK_SCORE':collection_get_risk,'COLLECTION_STORE_SUMMARY':collection_store_summary,
                                                                'PROJECT_NAME_BQ':project_name,'TABLE_COLLECT_RISK_SCORES_BQ':table_collect_risk_scores_bq,'TABLE_COLLECT_PROFITABILITY_SCORES_BQ':table_collect_profitability_scores_bq,'TABLE_STORE_SUMMARY_BQ':table_store_summary_bq,'SCRIPT_NAME':script_name},index=[0]),ignore_index=True)
    return summary_param_table

if __name__ == "__main__":
    sql_string = 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project'
    summary_param_table = summary_parameter()
    dict_summary_param =  summary_param_table.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_SUMMARY_TABLE', data=dict_summary_param)
