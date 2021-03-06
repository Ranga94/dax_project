# -*- coding: utf-8 -*-
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
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

#python ROCE_analysis_BQ.py 'mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project' 'PARAM_FINANCIAL_KEY_COLLECTION' 'igenie-project-key.json' 

def ROCE_main(args):
    ROCE_coll_table = pd.DataFrame()
    project_name, constituent_list,table_store,table_master = get_parameters(args)
    for constituent in constituent_list:
    #'Commerzbank', all debt NaN,Deutsche Bank',no data avaliable for 'Volkswagen (VW) vz'ranked last
        print constituent
        master = get_master_data(project_name,table_master,constituent)
        
        if constituent=='M\xc3\xbcnchener R\xc3\xbcckversicherungs-Gesellschaft':
            constituent = 'Münchener Rückversicherungs-Gesellschaft'
        elif constituent=='Deutsche B\xc3\xb6rse':
            constituent = 'Deutsche Börse'

        date = str("{:%Y-%m-%dT%H:%M:%S}".format(datetime.datetime.now().date()))
        constituent_name = get_constituent_id_name(constituent)[1]
        constituent_id = get_constituent_id_name(constituent)[0]  
        
        pct_ROCE_last_year, pct_ROCE_four_years, ROCE_table,score = ROCE_calculate(master)
        
        ROCE_coll_table = ROCE_coll_table.append(pd.DataFrame({'Constituent': constituent, 'Constituent_name':constituent_name, 'Constituent_id':constituent_id, 'Current_ROCE': round(ROCE_table['ROCE'].iloc[-1],2), 'percentage_change_in_ROCE_from_previous_year':round(pct_ROCE_last_year,2),'percentage_change_in_ROCE_from_4_years_ago': round(pct_ROCE_four_years,2),'ROCE_score':score,'Table':'ROCE analysis','Date':str(datetime.date.today()),'Status':"active"}, index=[0]), ignore_index=True)
    
    #store the analysis
    print "table done"
    update_result(table_store)
    print "update done"
    store_result(args,project_name, table_store,ROCE_coll_table)
    print "all done"


def ROCE_calculate(master):
    master = master[['EBITDA_in_Mio','Net_debt_in_Mio','Total_assets_in_Mio','year']].dropna(thresh=2)
    net_profit = master[['EBITDA_in_Mio','year']].dropna(0,'any')
    net_debt = master[['Net_debt_in_Mio','year']].dropna(0,'any')
    total_assets=master[['Total_assets_in_Mio','year']].dropna(0,'any')
    joined = pd.merge(pd.merge(net_profit,net_debt,on='year'),total_assets,on='year')
    joined["EBITDA_in_Mio"] = joined["EBITDA_in_Mio"].str.replace(",","").astype(float)
    joined['Net_debt_in_Mio'] = joined['Net_debt_in_Mio'].str.replace(",","").astype(float)
    joined['Total_assets_in_Mio'] = joined['Total_assets_in_Mio'].str.replace(",","").astype(float)
    joined['ROCE']=joined["EBITDA_in_Mio"]*100/(joined['Total_assets_in_Mio']-joined['Net_debt_in_Mio'])
    joined = joined.reset_index(drop=True)
    #print joined
    pct_ROCE_last_year = 100*(float(joined['ROCE'].loc[joined['year']==2016])-float(joined['ROCE'].loc[joined['year']==2015]))/float(joined['ROCE'].loc[joined['year']== 2015])
    pct_ROCE_four_years = 100*(float(joined['ROCE'].loc[joined['year']==2016])-float(joined['ROCE'].loc[joined['year']==2013]))/float(joined['ROCE'].loc[joined['year']== 2013])
    
    if (pct_ROCE_last_year>0) & (pct_ROCE_four_years>0):
        score = 2
    elif pct_ROCE_last_year>0:
        score =1
    else: 
        score = 0 
    return float(pct_ROCE_last_year), float(pct_ROCE_four_years), joined[['ROCE','year']],score


#
def get_parameters(args):
    script = 'ROCE_analysis'
    query = 'SELECT * FROM'+' '+ args.parameter_table + ';'
    print query
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
    table_store = 'pecten_dataset.ROCE_t'
    #import os
    #os.system("Storage.py")
    storage = Storage(google_key_path='/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
    storage = Storage(google_key_path='igenie-project-key.json' )
    query = 'UPDATE `' + table_store +'` SET Status = "inactive" WHERE Status = "active"'

    try:
        result = storage.get_bigquery_data(query)
    except Exception as e:
        print(e) 


#this obtains the master data as a pandas dataframe from source for one constituent. 
def get_master_data(project_name,table_master,constituent):
    QUERY ='SELECT * FROM '+ table_master + ' WHERE Constituent= "'+constituent+'";'
    print QUERY
    master=pd.read_gbq(QUERY, project_id=project_name)
    master['year'] = pd.to_datetime(master['year'],format="%Y-%m-%dT%H:%M:%S") #read the date format
    master = master.sort_values('year',ascending=1).reset_index(drop=True) 
    return master


def store_result(args,project_name,table_store,result_df):
   #private_key = '\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxPennNGNvtil5\n91eBHX+AnII71j1FDLVpRpG0/qSOYS29LAZCJdAb3wIOo6n8KHXMNUHOEIsPaCWx\nm0zkqFEQRHs61rIGf06hlQUcrCb9mt2SFsgc0onkzuDOXjH+ttVDqgUZdqLa5kfW\nKyU+7IAi9WSFQKes/qV+CJVOQpsbiehXzpr3t2biawuHL/5Gq4FRbe2tB8aG8Itk\nmAhkFMqxZ+gEWvYOtnITNH5U6ALuNTkaAzQSmxgAUEay8FneNBODSmpmSu/skDxg\nGYAFpqB5PifdIYmLLJQ2igv1dwHO5eU8QUl8CZvpI3yTTkPYmSd4EQKmCKOAe+4A\nxoSvWQNhAgMBAAECggEAKb5GN3DMNoQ1kochccRLwjOaLJORjJCorSm0qWcLxIi4\nqAQVWUDkmZvVNTNwlnXFAJpObETTK4bA0eqcmoHVzboDN+FWvlb/Yujg8lbNPA9c\nPcrlyHwBhAUBRzlCFxZidfQ2DUA2rM1tf9Bbjk1PBGy9BvEqTIEQ4vUMjQj81Ogp\nWIwTc0F8Ql6gTs15qOaanNTOrnfqvgKyBj+vg5bbx0ZXKsgUHJLtDV3I5WtvADU7\ncuiZZAv4+YQky5L4aJacrF+wnrmmh4Yi+seMN6bCzGeI496ESlhGB1mQ+0kpZ/20\no98t05DvxcxlSQdAsML6FHPwuGqrHGe95fIA5oKAXQKBgQD5rvDa/dzZG4Hre42/\n8TTV5pch/DcCv6GAoohGmLwadv4io1pF+pa0NmO6ALmVI7+Lh3LvGXanWSyJRq9g\n47WaIzT4GARhk8v69MQES/WLNmy15ISt/U7p2aksjJo21kK/H3+pWGpog6emZjS8\nUs7FCwH/54N1uxRWQp3GqerVAwKBgQC1uc9Nxy9/RFVeYkftQb0iNp7Pe/atFy2a\nzj2BFsnc7KQUNWlUIiNljjf5qj0u4ybimDft2sBYWMUyRg+UqLWu+wEtzM2kZx5o\nLnBrIuliTHtpDtSusTNbhU3vRJqSQyNpfYlr8r9wDf3RNcPedusxjrXtGA9UKm40\nlQt82PleywKBgQDcNe/VpTrH/NvMHa96TzmDQhmfbrvx36OIOVEpuoGEcdhYImx2\n9bk+/g1culDzeZDxdafnuzSMCjkeZco+EPdVF6IbAcuzZ8/q6T+MbpYEjx64GfDv\nJ08XLtJnKSbGIJjDyfslF4bZ4OvW9aOjSQF2hevgkIbAKhyYq65NzgUAkwKBgC05\nvuPGbIWpxe0lXG0pbR50pXVRjoRpPkpGhXv45ef7ZKI64Km1jUWa7UdPHMbsdSLz\nawfg5vmVrg3i7cG7cuvHP/XcAFmn6CNZW1TubVKvYg81R6zqILPoKwse8bDw0IFS\nYr7gvM/wS3ijfsmAouvEbnZBBJ+Xp7GiXieGABmdAoGACv4Jx4Ly9zA100nzFJK+\nluqBe635HoeX+LbMbv3iULM/mrpDkPhdsWAejqEnkWh9GJpkCarN4YT/ojj8ox5j\nOuTKyIVsiNPHUgyG+Kb2xeZR7mDviKkBcRNgSHAzB04b0q+aEigiBOBODQsv/vYM\nhsLeRNIFax947XqiL3VvRqg=\n'
    table_store = 'pecten_dataset.ROCE_t'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_key_path
    client = bigquery.Client()
    #Store result to bigquery
    result_df.to_gbq(table_store, project_id = project_name, chunksize=10000, verbose=True, reauth=False, if_exists='append',private_key=None)
    
    
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


    args = parser.parse_args()
    
    ROCE_main(args)