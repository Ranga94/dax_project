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

#python profit_margin_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 'pecten_dataset_test.profit_margin_t' 


def profit_margin_main(args):
    project_name, constituent_list,table_store,table_master = get_parameters(args)
    profit_margin_table = pd.DataFrame()
    #'Volkswagen (VW) vz' does not receive any data on profit margin
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
        
        profit_margin_calculation,current_pm,pm_last_year,pm_four_years_ago,pct_last_year,pct_four_years_ago,score = profit_margin_calculator(master)
        profit_margin_table = profit_margin_table.append(pd.DataFrame({'Constituent': constituent, 'Constituent_name':constituent_name, 'Constituent_id':constituent_id, 'Current_profit_margin':current_pm,'Profit_margin_last_year':pm_last_year,'percentage_change_in_profit_margin_last_year':pct_last_year,'Profit_margin_4_years_ago': pm_four_years_ago,'percentage_change_in_profit_margin_4_years_ago':pct_four_years_ago,'Table':'profit margin analysis','Profit_margin_score':score,'Date':date,'Status':"active" }, index=[0]), ignore_index=True)
    
    print ("table done")
    update_result(args)
    print ("update done")
    store_result(args,project_name,profit_margin_table)
    print ("all done")


def profit_margin_calculator(master):
    sales = master[['Sales_in_Mio','year']].dropna(thresh=2)
    net_profit=master[['Net_profit','year']].dropna(thresh=2)
    #sales['Sales_in_Mio']=sales['Sales_in_Mio'].str.replace(",","").astype(float)
    #net_profit['Net_profit']=net_profit['Net_profit'].str.replace(",","").astype(float)
    net_profit=net_profit.reset_index(drop=True)
    
    if len(net_profit)>0:
        profit_margin_calculation = [float(net_profit['Net_profit'].iloc[i])*100.0/float(sales['Sales_in_Mio'].iloc[i]) for i in range (sales.shape[0])]
        current_pm = round(profit_margin_calculation[-1],2)
        pm_last_year=round(profit_margin_calculation[-2],2)
        pm_four_years_ago=round(profit_margin_calculation[-4],2)
        pct_last_year=(current_pm -pm_last_year)*100.0/pm_last_year
        pct_four_years_ago=(current_pm -pm_four_years_ago)*100.0/pm_four_years_ago
            
        if (pct_last_year>0) & (pct_four_years_ago>0):
            score = 2
        elif pct_last_year>0:
            score =1
        else: 
            score = 0
    else:
        profit_margin_calculation=[0]
        current_pm = 0
        pm_last_year = 0
        pm_four_years_ago = 0
        pct_last_year = 0
        pct_four_years_ago = 0
        score = 0
        
    return profit_margin_calculation,current_pm,pm_last_year,pm_four_years_ago,pct_last_year,pct_four_years_ago,score
    

def get_parameters(args):
    script = 'profit_margin_analysis'
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



def update_result(args):
    table_store = args.table_storage
    storage = Storage(google_key_path='igenie-project-key.json' )
    query = 'UPDATE ' + table_store +' SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


#this obtains the master data as a pandas dataframe from source for one constituent. 
def get_master_data(project_name,table_master,constituent):
    table_master = 'pecten_dataset.historical_key_data'
    constituent_id = get_constituent_id_name(constituent)[0]
    print (constituent_id)
    QUERY ='SELECT Sales_in_Mio, Net_profit,year,constituent_id,constituent_name FROM '+ table_master + ' WHERE constituent_id= "'+constituent_id+'";'
    print (QUERY)
    master = pd.read_gbq(QUERY, project_id=project_name)
    master['year'] = pd.to_datetime(master['year'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    master = master.sort_values('year',ascending=1).reset_index(drop=True) 
    return master


def store_result(args,project_name,result_df):
   #private_key = '\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxPennNGNvtil5\n91eBHX+AnII71j1FDLVpRpG0/qSOYS29LAZCJdAb3wIOo6n8KHXMNUHOEIsPaCWx\nm0zkqFEQRHs61rIGf06hlQUcrCb9mt2SFsgc0onkzuDOXjH+ttVDqgUZdqLa5kfW\nKyU+7IAi9WSFQKes/qV+CJVOQpsbiehXzpr3t2biawuHL/5Gq4FRbe2tB8aG8Itk\nmAhkFMqxZ+gEWvYOtnITNH5U6ALuNTkaAzQSmxgAUEay8FneNBODSmpmSu/skDxg\nGYAFpqB5PifdIYmLLJQ2igv1dwHO5eU8QUl8CZvpI3yTTkPYmSd4EQKmCKOAe+4A\nxoSvWQNhAgMBAAECggEAKb5GN3DMNoQ1kochccRLwjOaLJORjJCorSm0qWcLxIi4\nqAQVWUDkmZvVNTNwlnXFAJpObETTK4bA0eqcmoHVzboDN+FWvlb/Yujg8lbNPA9c\nPcrlyHwBhAUBRzlCFxZidfQ2DUA2rM1tf9Bbjk1PBGy9BvEqTIEQ4vUMjQj81Ogp\nWIwTc0F8Ql6gTs15qOaanNTOrnfqvgKyBj+vg5bbx0ZXKsgUHJLtDV3I5WtvADU7\ncuiZZAv4+YQky5L4aJacrF+wnrmmh4Yi+seMN6bCzGeI496ESlhGB1mQ+0kpZ/20\no98t05DvxcxlSQdAsML6FHPwuGqrHGe95fIA5oKAXQKBgQD5rvDa/dzZG4Hre42/\n8TTV5pch/DcCv6GAoohGmLwadv4io1pF+pa0NmO6ALmVI7+Lh3LvGXanWSyJRq9g\n47WaIzT4GARhk8v69MQES/WLNmy15ISt/U7p2aksjJo21kK/H3+pWGpog6emZjS8\nUs7FCwH/54N1uxRWQp3GqerVAwKBgQC1uc9Nxy9/RFVeYkftQb0iNp7Pe/atFy2a\nzj2BFsnc7KQUNWlUIiNljjf5qj0u4ybimDft2sBYWMUyRg+UqLWu+wEtzM2kZx5o\nLnBrIuliTHtpDtSusTNbhU3vRJqSQyNpfYlr8r9wDf3RNcPedusxjrXtGA9UKm40\nlQt82PleywKBgQDcNe/VpTrH/NvMHa96TzmDQhmfbrvx36OIOVEpuoGEcdhYImx2\n9bk+/g1culDzeZDxdafnuzSMCjkeZco+EPdVF6IbAcuzZ8/q6T+MbpYEjx64GfDv\nJ08XLtJnKSbGIJjDyfslF4bZ4OvW9aOjSQF2hevgkIbAKhyYq65NzgUAkwKBgC05\nvuPGbIWpxe0lXG0pbR50pXVRjoRpPkpGhXv45ef7ZKI64Km1jUWa7UdPHMbsdSLz\nawfg5vmVrg3i7cG7cuvHP/XcAFmn6CNZW1TubVKvYg81R6zqILPoKwse8bDw0IFS\nYr7gvM/wS3ijfsmAouvEbnZBBJ+Xp7GiXieGABmdAoGACv4Jx4Ly9zA100nzFJK+\nluqBe635HoeX+LbMbv3iULM/mrpDkPhdsWAejqEnkWh9GJpkCarN4YT/ojj8ox5j\nOuTKyIVsiNPHUgyG+Kb2xeZR7mDviKkBcRNgSHAzB04b0q+aEigiBOBODQsv/vYM\nhsLeRNIFax947XqiL3VvRqg=\n'
    table_store = args.table_storage 
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
    mapping["Volkswagen (VW) vz"] = ("VOW3DE2070000543" , "VOLKSWAGEN AG")
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
    
    #sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object # Feature PECTEN-9
    from utils.Storage import Storage #Feature PECTEN-9
    
    profit_margin_main(args)