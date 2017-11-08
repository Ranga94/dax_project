# -*- coding: utf-8 -*-
##This script combines the scores of fundamental analysis and techinical analysis in order to rank the constituents on their profitability
import pandas as pd
import pymongo
from re import sub
from decimal import Decimal
from pymongo import MongoClient
import numpy as np
import datetime
import matplotlib.pyplot as plt
import pylab
import scipy
from scipy import stats
from decimal import Decimal
import operator
from bs4 import BeautifulSoup
import urllib
import json
import sys


#!python Igenie/dax_project/fundamental_kefei/Profitability_ranking.py '/Users/kefei/DIgenie/dax_project/fundamental_kefei' 'mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp' 'dax_gcp' 'Price ranking' 'Fundamental ranking'  -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Linde','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','ProSiebenSat1 Media','Volkswagen (VW) vz' 'Profitability scores' 'Profitability ranking'

#u'Deutsche B\xf6rse'
#u'M\xfcnchener R\xfcckversicherungs-Gesellschaft'
#Münchener Rückversicherungs-Gesellschaft'

def combined_profitability_main(args):
    current_fundamental_score_board,fundamental_growth_score_board,price_score_board = scoring_collection(args)
    board_list = [current_fundamental_score_board,fundamental_growth_score_board,price_score_board]
    score_list = ['Current fundamental total score','Fundamental growth score','Total price growth score']
    combined_profitability_board=combined_profitability_scoring(board_list, score_list) #
    combined_profitability_tag_table = combined_profitability_tag(combined_profitability_board) #indicates the
    print 'combine completed'
    
    combined_profitability_json = json.loads(combined_profitability_board.to_json(orient='records')) #upload the score
    combined_tag_json = json.loads(combined_profitability_tag_table.to_json(orient='records')) #upload the tagging
    
    
    client = MongoClient(args.connection_string)
    db = client[args.database]
    
    db[args.collection_store_profitablity_scores].drop()
    collection = db[args.collection_store_profitablity_scores]
    collection.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
    collection.update_many({'Status':'NaN'}, {'$set': {'Status': 'inactive'}},True,True)
    
    db[args.collection_store_profitablity_ranking].drop()
    collection = db[args.collection_store_profitablity_ranking]
    collection.update_many({'Status':'active'}, {'$set': {'Status': 'inactive'}},True,True)
    collection.update_many({'Status':'NaN'}, {'$set': {'Status': 'inactive'}},True,True)
    
    db[args.collection_store_profitablity_scores].insert_many(combined_profitability_json)
    db[args.collection_store_profitablity_ranking].insert_many(combined_tag_json)
    
    print 'all done'
    
def scoring_collection(args):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection_price = db[args. collection_get_price_scores]
    collection_fundamental = db[args. collection_get_fundamental_scores]
    current_fundamental_score_board = pd.DataFrame(list(collection_fundamental.find({'Table':'Current fundamental ranking','Status':'active'})))
    fundamental_growth_score_board = pd.DataFrame(list(collection_fundamental.find({'Table':'Fundamental growth ranking','Status':'active'})))
    price_score_board = pd.DataFrame(list(collection_price.find({'Table':'Price growth ranking','Status':'active'})))
    return current_fundamental_score_board,fundamental_growth_score_board,price_score_board 


## Combined profitability scores the price growth and fundamental potential out of 60. 
def combined_profitability_scoring(board_list, score_list): 
    n=len(board_list)
    combined_profitability_board = pd.DataFrame()
    temp = board_list[0]
    for i in range(n-1):
        temp = temp.merge(board_list[i+1],on='Constituent',how='left')
    
    temp['Total profitability score']=temp['Total price growth score']+temp['Current fundamental total score']+temp['Fundamental growth score']
    combined_profitability_board = pd.DataFrame(temp[['Constituent','Total profitability score','Total price growth score','Current fundamental total score','Fundamental growth score','Status']])
    #combined_profitability_board = combined_profitability_board.sort_values('Total profitability score',axis=0, ascending=False).reset_index(drop=True)
    return combined_profitability_board



def combined_profitability_tag(combined_profitability_board):
    #all_constituents_dict = {'Allianz':'Allianz', 'Adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    #'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    #'Deutsche Bank':'Deutsche Bank', 'Deutsche Börse':u'Deutsche B\xf6rse', 'Deutsche Post':'Deutsche Post',
                    #'Deutsche Telekom':'Deutsche Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius Medical Care',
                    #'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                    #'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': u'M\xfcnchener R\xfcckversicherungs-Gesellschaft',
                    #'ProSiebenSat1 Media':'ProSiebenSat1 Media', 'RWE':'RWE', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                    #'Volkswagen (VW) vz':'Volkswagen (VW) vz','Vonovia':'Vonovia'}
    #all_constituents_dict = {'Allianz':'Allianz', 'Adidas':'adidas'}
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    profitability_ranking_table=pd.DataFrame()
    combined_profitability_board = combined_profitability_board.sort_values('Total profitability score',axis=0, ascending=False).reset_index(drop=True)
    for constituent in constituents_list:
        print constituent
        index = int(combined_profitability_board[combined_profitability_board['Constituent']==constituent].index[0])
        price_growth_score= int(combined_profitability_board['Total price growth score'].loc[combined_profitability_board['Constituent']==constituent])
        fundamental_growth_score=  int(combined_profitability_board['Fundamental growth score'].loc[combined_profitability_board['Constituent']==constituent])
        if price_growth_score >=24 :
            growth_price_status = 'High'
        elif price_growth_score >=16 :
            growth_price_status = 'Medium'
        else :
            growth_price_status = 'Low'
            
        if fundamental_growth_score >=10 :
            growth_fundamental_status = 'High'
        elif fundamental_growth_score >=6 :
            growth_fundamental_status = 'Medium'
        else :
            growth_fundamental_status = 'Low'
            
        profitability_ranking_table=profitability_ranking_table.append(pd.DataFrame({'Constituent':constituent, 'Profitability rank':index, 'Price growth':growth_price_status,'Fundamental growth':growth_fundamental_status,'Date':str(datetime.date.today()),'Status':'active'},index=[0]),ignore_index=True)
    profitability_ranking_table=profitability_ranking_table.sort_values('Profitability rank',axis=0, ascending=True).reset_index(drop=True)
    return profitability_ranking_table



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The directory connection string') 
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_price_scores', help='The collection from which price scores is extracted')
    parser.add_argument('collection_get_fundamental_scores', help='The collection from which fundamental scores is extracted')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('collection_store_profitablity_scores', help='The collection where the profitability scores will be stored')
    parser.add_argument('collection_store_profitablity_ranking', help='The collection where the ranking result will be stored')
    #parser.add_argument('table_store_analysis', help='Name of table for storing the analysis')

    args = parser.parse_args()
    
    sys.path.insert(0, args.python_path)
    #from utils.Storage import Storage
    
    combined_profitability_main(args)