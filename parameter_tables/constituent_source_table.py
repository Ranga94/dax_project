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






if __name__ == "__main__":
    sql_string = 'mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project'
    assessment_param_table = assessment_parameter()
    dict_assessment_param =  assessment_param_table.to_dict(orient='records')
    insert_to_sql(sql_connection_string=sql_string, sql_table_name='PARAM_PROFITABILITY_RISK_ASSESSMENT', data=dict_assessment_param)
