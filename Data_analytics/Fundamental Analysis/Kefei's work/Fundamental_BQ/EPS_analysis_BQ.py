# -*- coding: utf-8 -*-
from __future__ import print_function
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
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


#python EPS_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset_test.EPS_t'

#'Volkswagen (VW) vz' not found
def EPS_main(args):
    EPS_table = pd.DataFrame()
    project_name, constituent_list,table_store,table_master = get_parameters(args)
    table_store = args.table_storage
    for constituent in constituent_list:
        print (constituent)
        master = get_master_data(project_name,table_master,constituent)
        
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'

        date = datetime.strftime(datetime.now().date(),'%Y-%m-%d %H:%M:%S') 
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0] 
       
        current_EPS,last_year_EPS,four_years_ago_EPS,pct_last_year,pct_four_years,score = EPS_calculate(master)
        EPS_table = EPS_table.append(pd.DataFrame({'Constituent': constituent, 'Constituent_name':constituent_name, 'Constituent_id':constituent_id, 'Current_EPS':current_EPS,'EPS_last_year':last_year_EPS, 'percentage_change_in_EPS_from_last_year': round(pct_last_year,2),'EPS_score': score,'EPS_4_years_ago': four_years_ago_EPS,'percentage_change_in_EPS_from_4_years_ago':round(pct_four_years,2),'Table':'EPS analysis','Date':date,'Status':"active" }, index=[0]), ignore_index=True)
    
    #store the analysis
    print ("table done")
    update_result(table_store)
    print ("update done")
    store_result(args,project_name, table_store,EPS_table)
    print ("all done")
    
def EPS_calculate(master):
    EPS = master[['EPS_reported','year']].dropna(thresh=2)
    EPS = EPS.reset_index(drop=True)
    if len(EPS)>0:
        current_EPS = float(EPS['EPS_reported'].iloc[-1])
        last_year_EPS = float(EPS['EPS_reported'].iloc[-2])
        four_years_ago_EPS = float(EPS['EPS_reported'].iloc[-4])
        pct_last_year = (current_EPS-last_year_EPS)*100.0/last_year_EPS
        pct_four_years = (current_EPS-four_years_ago_EPS)*100.0/four_years_ago_EPS
        if (pct_last_year>0) & (pct_four_years>0):
            score = 2
        elif pct_last_year>0:
            score =1
        else: 
            score = 0
    else:
        current_EPS = 0
        last_year_EPS=0
        four_years_ago_EPS=0
        pct_last_year=0
        pct_four_years=0
        score = 0
    return current_EPS,last_year_EPS,four_years_ago_EPS,pct_last_year,pct_four_years,score


def get_parameters(args):
    script = 'EPS_analysis'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print (query)
    parameter_table = pd.read_sql(query, con=args.sql_connection_string)
    project_name = parameter_table["PROJECT_NAME_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    
    #Obtain the constituent_list
    a = parameter_table['CONSTITUENT_LIST'].loc[parameter_table['SCRIPT_NAME']==script]
    constituent_list=np.asarray(ast.literal_eval((a.values[0])))
    
    #Obtain the table storing historical price
    table_master = parameter_table["TABLE_COLLECT_FUNDAMENTAL_BQ"].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    table_store = parameter_table['TABLE_STORE_ANALYSIS_BQ'].loc[parameter_table['SCRIPT_NAME']==script].values[0]
    return project_name, constituent_list,table_store,table_master



def update_result(table_store):
    #storage = Storage(google_key_path='/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
    storage = Storage(google_key_path=args.service_key_path)
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


#this obtains the master data as a pandas dataframe from source for one constituent. 
def get_master_data(project_name,table_master,constituent):
    table_master = 'pecten_dataset.historical_key_data'
    constituent_id = get_constituent_id_name(constituent)[0]
    print (constituent_id)
    QUERY ='SELECT EPS_reported,year,constituent_id,constituent_name FROM '+ table_master + ' WHERE constituent_id= "'+constituent_id+'";'
    print (QUERY)
    master = pd.read_gbq(QUERY, project_id=project_name)
    master['year'] = pd.to_datetime(master['year'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    master = master.sort_values('year',ascending=1).reset_index(drop=True) 
    return master


def store_result(args,project_name,table_store,result_df):
   #private_key = '\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxPennNGNvtil5\n91eBHX+AnII71j1FDLVpRpG0/qSOYS29LAZCJdAb3wIOo6n8KHXMNUHOEIsPaCWx\nm0zkqFEQRHs61rIGf06hlQUcrCb9mt2SFsgc0onkzuDOXjH+ttVDqgUZdqLa5kfW\nKyU+7IAi9WSFQKes/qV+CJVOQpsbiehXzpr3t2biawuHL/5Gq4FRbe2tB8aG8Itk\nmAhkFMqxZ+gEWvYOtnITNH5U6ALuNTkaAzQSmxgAUEay8FneNBODSmpmSu/skDxg\nGYAFpqB5PifdIYmLLJQ2igv1dwHO5eU8QUl8CZvpI3yTTkPYmSd4EQKmCKOAe+4A\nxoSvWQNhAgMBAAECggEAKb5GN3DMNoQ1kochccRLwjOaLJORjJCorSm0qWcLxIi4\nqAQVWUDkmZvVNTNwlnXFAJpObETTK4bA0eqcmoHVzboDN+FWvlb/Yujg8lbNPA9c\nPcrlyHwBhAUBRzlCFxZidfQ2DUA2rM1tf9Bbjk1PBGy9BvEqTIEQ4vUMjQj81Ogp\nWIwTc0F8Ql6gTs15qOaanNTOrnfqvgKyBj+vg5bbx0ZXKsgUHJLtDV3I5WtvADU7\ncuiZZAv4+YQky5L4aJacrF+wnrmmh4Yi+seMN6bCzGeI496ESlhGB1mQ+0kpZ/20\no98t05DvxcxlSQdAsML6FHPwuGqrHGe95fIA5oKAXQKBgQD5rvDa/dzZG4Hre42/\n8TTV5pch/DcCv6GAoohGmLwadv4io1pF+pa0NmO6ALmVI7+Lh3LvGXanWSyJRq9g\n47WaIzT4GARhk8v69MQES/WLNmy15ISt/U7p2aksjJo21kK/H3+pWGpog6emZjS8\nUs7FCwH/54N1uxRWQp3GqerVAwKBgQC1uc9Nxy9/RFVeYkftQb0iNp7Pe/atFy2a\nzj2BFsnc7KQUNWlUIiNljjf5qj0u4ybimDft2sBYWMUyRg+UqLWu+wEtzM2kZx5o\nLnBrIuliTHtpDtSusTNbhU3vRJqSQyNpfYlr8r9wDf3RNcPedusxjrXtGA9UKm40\nlQt82PleywKBgQDcNe/VpTrH/NvMHa96TzmDQhmfbrvx36OIOVEpuoGEcdhYImx2\n9bk+/g1culDzeZDxdafnuzSMCjkeZco+EPdVF6IbAcuzZ8/q6T+MbpYEjx64GfDv\nJ08XLtJnKSbGIJjDyfslF4bZ4OvW9aOjSQF2hevgkIbAKhyYq65NzgUAkwKBgC05\nvuPGbIWpxe0lXG0pbR50pXVRjoRpPkpGhXv45ef7ZKI64Km1jUWa7UdPHMbsdSLz\nawfg5vmVrg3i7cG7cuvHP/XcAFmn6CNZW1TubVKvYg81R6zqILPoKwse8bDw0IFS\nYr7gvM/wS3ijfsmAouvEbnZBBJ+Xp7GiXieGABmdAoGACv4Jx4Ly9zA100nzFJK+\nluqBe635HoeX+LbMbv3iULM/mrpDkPhdsWAejqEnkWh9GJpkCarN4YT/ojj8ox5j\nOuTKyIVsiNPHUgyG+Kb2xeZR7mDviKkBcRNgSHAzB04b0q+aEigiBOBODQsv/vYM\nhsLeRNIFax947XqiL3VvRqg=\n'
    #table_store = 'pecten_dataset.EPS_t'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    #Store result to bigquery
    result_df.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=True, reauth=False, if_exists='append',private_key=None)


def get_constituent_id_name(old_constituent_name):
    mapping = {}
    mapping["BMW"] = ("BMWDE8170003036" , "BAYERISCHE MOTOREN WERKE AG")
    mapping["Allianz"] = ("ALVDEFEI1007380" , "ALLIANZ SE")
    mapping["Commerzbank"] = ("CBKDEFEB13190" , "COMMERZBANK AKTIENGESELLSCHAFT")
    mapping["adidas"] = ("ADSDE8190216927", "ADIDAS AG")
    mapping["Deutsche Bank"] = ("DBKDEFEB13216" , "DEUTSCHE BANK AG")
    mapping["EON"] = ("EOANDE5050056484" , "E.ON SE")
    mapping["Lufthansa"] = ("LHADE5190000974" ,"DEUTSCHE LUFTHANSA AG")
    mapping["Continental"] = ("CONDE2190001578" , "CONTINENTAL AG")
    mapping["Daimler"] = ("DAIDE7330530056" , "DAIMLER AG")
    mapping["Siemens"] = ("SIEDE2010000581" , "SIEMENS AG")
    mapping["BASF"] = ("BASDE7150000030" , "BASF SE")
    mapping["Bayer"] = ("BAYNDE5330000056" , "BAYER AG")
    mapping["Beiersdorf"] = ("BEIDE2150000164" , "BEIERSDORF AG")
    mapping["Deutsche Börse"] = ("DB1DEFEB54555" , "DEUTSCHE BOERSE AG")
    mapping["Deutsche Post"] = ("DPWDE5030147191" , "DEUTSCHE POST AG")
    mapping["Deutsche Telekom"] = ("DTEDE5030147137" , "DEUTSCHE TELEKOM AG")
    mapping["Fresenius"] = ("FREDE6290014544" , "FRESENIUS SE & CO.KGAA")
    mapping["HeidelbergCement"] = ("HEIDE7050000100" , "HEIDELBERGCEMENT AG")
    mapping["Henkel vz"] = ("HEN3DE5050001329" , "HENKEL AG & CO. KGAA")
    mapping["Infineon"] = ("IFXDE8330359160" , "INFINEON TECHNOLOGIES AG")
    mapping["Linde"] = ("LINDE8170014684" , "LINDE AG")
    mapping["Merck"] = ("MRKDE6050108507" , "MERCK KGAA")
    mapping["ProSiebenSat1 Media"] = ("PSMDE8330261794" , "PROSIEBENSAT.1 MEDIA SE")
    mapping["RWE"] = ("RWEDE5110206610" , "RWE AG")
    mapping["SAP"] = ("SAPDE7050001788" , "SAP SE")
    mapping["thyssenkrupp"] = ("TKADE5110216866" , "THYSSENKRUPP AG")
    mapping["Vonovia"] = ("VNADE5050438829" , "VONOVIA SE")
    mapping["DAX"] = ("DAX", "DAX")
    mapping["Fresenius Medical Care"] = ("FMEDE8110066557" , "FRESENIUS MEDICAL CARE AG & CO.KGAA")
    mapping["Volkswagen"] = ("VOW3DE2070000543" , "VOLKSWAGEN AG")
    mapping["Münchener Rückversicherungs-Gesellschaft"] = ("MUV2DEFEI1007130" , "MUNCHENER RUCKVERSICHERUNGS - GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN")

    if old_constituent_name in mapping:
        return mapping[old_constituent_name]
    else:
        return old_constituent_name


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('sql_connection_string', help='The connection string to mysql for parameter table') 
    parser.add_argument('parameter_table',help="The name of the parameter table in MySQL")
    parser.add_argument('service_key_path',help='google service key path')
    parser.add_argument('table_storage',help='BigQuery table where the new data is stored')
    
    args = parser.parse_args()
    from utils.Storage import Storage  # Feature PECTEN-9
    
    EPS_main(args)