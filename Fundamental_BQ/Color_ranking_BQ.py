# -*- coding: utf-8 -*-
##This script allocates Profitability and Risk colors to each DAX constituent, based on the profitability scores and risk scores from previous analysis. 
import pandas as pd
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
from datetime import datetime
import pylab
import scipy
from scipy import stats
from decimal import Decimal
import operator
import urllib
import json
import sys
import ast
import os
from sqlalchemy import *
import argparse
from pymongo import MongoClient, errors
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from google.cloud import datastore
from google.cloud import bigquery

#python Color_ranking_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_SUMMARY_TABLE' 'igenie-project-key.json' 'pecten_dataset_test.summary_box'
 
def colors_main(args):
    project_name = 'igenie-project'
    table_store =args.table_storage
    from_date, to_date = get_timerange(args)
    
    twitter_colors = get_twitter_color(from_date, to_date)
    news_colors = get_news_color(from_date, to_date)
    profitability_colors = get_profitability_color()
    risk_colors= get_risk_color()
   
    print "colors_computed"
    summary_box = summary_table(profitability_colors,risk_colors,news_colors,twitter_colors)
    store_summary(project_name,table_store, summary_box)
    
    print 'all done'
    
def get_twitter_color(from_date,to_date):
    query = "SELECT constituent_name, DATE(date) as date, count(date) as count, constituent_id, constituent, sentiment_score FROM pecten_dataset_test.tweets WHERE date between TIMESTAMP ('"+ str(from_date)+ " UTC') and TIMESTAMP ('"+ str(to_date)+ " UTC') GROUP BY constituent_name, constituent, constituent_id, date, sentiment_score ORDER BY date"
    twitter_colors = pd.read_gbq(query, project_id='igenie-project',private_key=None)
    
    
    #set up a df for all the sentiment scores
    sentiment_scores = twitter_colors[['sentiment_score', 'constituent_id']]
    
    #get the list of constituent_name
    constituent_ids = sentiment_scores['constituent_id'].unique()
    from_date = datetime.strftime(from_date,'%Y-%m-%d %H:%M:%S') 
    to_date = datetime.strftime(to_date,'%Y-%m-%d %H:%M:%S') 
    twitter_color = pd.DataFrame()
    date =datetime(2017,12,11)
    for constituent_id in constituent_ids:
        avg_sent = float(sentiment_scores[sentiment_scores['constituent_id']==constituent_id].mean())
    
        if avg_sent>0.25:
            color = 1
        elif avg_sent<-0.25:
            color = -1
        else: 
            color = 0
        
        twitter_color = twitter_color.append(pd.DataFrame({"Constituent_id":constituent_id, 'Twitter_sent_color':color,'Date_of_analysis':date, 'From_date':from_date, 'To_date':to_date},index=[0]),ignore_index=True)
    
    return twitter_color



def get_news_color(from_date,to_date):
    query = "SELECT constituent_name, DATE(news_date) as date, count(news_date) as count, constituent_id, constituent,score FROM pecten_dataset_test.all_news "+"WHERE news_date between TIMESTAMP ('" + str(from_date)+" UTC') and TIMESTAMP ('"+ str(to_date)+ " UTC') GROUP BY constituent_name, constituent, constituent_id, date, score ORDER BY date"
    sentiment_scores=pd.read_gbq(query, project_id='igenie-project', private_key=None)
    news_color = pd.DataFrame()
    date =datetime(2017,12,11)
    constituent_ids = sentiment_scores['constituent_id'].unique()
    for constituent_id in constituent_ids:
        avg_sent = float(sentiment_scores['score'].loc[sentiment_scores['constituent_id']==constituent_id].mean())
        if avg_sent>0.25:
            color = 1
        elif avg_sent<-0.25:
            color = -1
        else: 
            color = 0
            
        news_color = news_color.append(pd.DataFrame({'Constituent_id':constituent_id,'News_sent_color':color,'Date_of_analysis':date},index=[0]),ignore_index=True)
    return news_color


def get_profitability_color():
    QUERY="""SELECT Constituent, Constituent_id, Constituent_name,Total_profitability_score,
    (CASE 
        WHEN Total_profitability_score > 32 THEN '1'
        WHEN Total_profitability_score < 20  THEN '-1'
        ELSE '0'
    END) AS Profitability_color
    FROM pecten_dataset_test.Profitability_score_ranking_t 
    WHERE Status="active"
    GROUP BY Constituent_name, Constituent, Constituent_id, Total_profitability_score, Profitability_color
    ORDER BY Constituent"""
    profitability_colors=pd.read_gbq(QUERY, project_id='igenie-project', private_key=None)
    return profitability_colors


def get_risk_color():
    QUERY="""SELECT Constituent, Constituent_id, Constituent_name,Risk_score,
    (CASE 
        WHEN Risk_score > 4 THEN '-1'
        WHEN Risk_score < 2  THEN '1'
        ELSE '0'
    END) AS Risk_color
    FROM pecten_dataset_test.Risk_ranking_t 
    WHERE Status="active"
    GROUP BY Constituent_name, Constituent, Constituent_id, Risk_score, Risk_color
    ORDER BY Constituent"""
    risk_colors=pd.read_gbq(QUERY, project_id='igenie-project', private_key=None)
    return risk_colors


def summary_table(profitability_colors,risk_colors,news_colors,twitter_colors):
    temp = news_colors[['Constituent_id','Date_of_analysis','News_sent_color']]
    temp = temp.merge(twitter_colors[['Constituent_id','Twitter_sent_color','From_date','To_date']], on = 'Constituent_id',how='left')
    temp = temp.merge(profitability_colors[['Constituent_id','Constituent','Constituent_name','Profitability_color']], on= 'Constituent_id',how='left')
    temp = temp.merge(risk_colors[['Constituent_id','Risk_color']], on = 'Constituent_id',how='outer')

    return temp
  

def get_timerange(args):
    query = 'SELECT * FROM PARAM_READ_DATE WHERE STATUS = "active";'
    timetable = pd.read_sql(query, con=args.sql_connection_string)
    from_date = timetable['FROM_DATE'].loc[timetable['ENVIRONMENT']=='test']
    to_date = timetable['TO_DATE'].loc[timetable['ENVIRONMENT']=='test']
    return from_date[0], to_date[0]



def update_result(table_store,choice):
    storage = Storage(google_key_path=args.service_key_path )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


def store_summary(project_name,table_store,summary_box):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    #Store result to bigquery
    summary_box.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=True, reauth=False, if_exists='replace',private_key=None)
    
    
class Storage:
    def __init__(self, google_key_path=None, mongo_connection_string=None):
        if google_key_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
            self.bigquery_client = bigquery.Client()
        else:
            self.bigquery_client = None

        if mongo_connection_string:
            self.mongo_client = MongoClient(mongo_connection_string)
        else:
            self.mongo_client = None
            
    def get_bigquery_data(self, query, timeout=None, iterator_flag=True): 
        if self.bigquery_client:
            client = self.bigquery_client
        else:
            client = bigquery.Client()

        print("Running query...")
        query_job = client.query(query)
        iterator = query_job.result(timeout=timeout)

        if iterator_flag:
            return iterator
        else:
            return list(iterator)





if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('sql_connection_string', help='The connection string to mysql for parameter table') 
    parser.add_argument('parameter_table',help="The name of the parameter table in MySQL")
    parser.add_argument('service_key_path',help='google service key path')
    parser.add_argument('table_storage',help='BigQuery table where the new data is stored')
    args = parser.parse_args()
    
    
    colors_main(args)