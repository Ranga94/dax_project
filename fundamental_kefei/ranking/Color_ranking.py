# -*- coding: utf-8 -*-
##This script allocates Profitability and Risk colors to each DAX constituent, based on the profitability scores and risk scores from previous analysis. 
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

#python Color_ranking.py 'mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp' 'dax_gcp' 'Profitability scores' 'Risk scores' -l 'Allianz','adidas','BASF','Bayer','Beiersdorf','BMW','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Post','Deutsche Telekom','EON','Fresenius','HeidelbergCement','Infineon','Lufthansa','Merck','RWE','SAP','Siemens','thyssenkrupp','Vonovia','Fresenius Medical Care','ProSiebenSat1 Media','Volkswagen (VW) vz' 'summary_box'

#u'Deutsche B\xf6rse'
#u'M\xfcnchener R\xfcckversicherungs-Gesellschaft'
#Münchener Rückversicherungs-Gesellschaft'

def colors_main(args):
    combined_profitability_board, VaR_score_board= score_board_collection(args)
    print 'collection done'
    color_coding_table = color_coding(combined_profitability_board,VaR_score_board)
    update_colors_mongodb(args,color_coding_table)
    print 'all done'
    
    
def score_board_collection(args):
    client = MongoClient(args.connection_string)
    db = client[args.database]
    collection_profitability = db[args.collection_get_profitability_ranking]
    collection_risk = db[args.collection_get_risk_ranking]
    combined_profitability_board=pd.DataFrame(list(collection_profitability.find({'Status':'active'})))
    VaR_score_board = pd.DataFrame(list(collection_risk.find({'Status':'active'})))
    return combined_profitability_board, VaR_score_board
    
    
def update_colors_mongodb(args,color_coding_table): 
    constituents_selected=['Commerzbank', 'adidas', 'BMW', 'Deutsche Bank', 'EON']
    client = MongoClient(args.connection_string)
    db = client[args.database]
    for constituent in constituents_selected:
        
        print constituent
        prof_color = color_coding_table['Profitability'].loc[color_coding_table['Constituent']==constituent].values[0]
        risk_color = color_coding_table['Risk'].loc[color_coding_table['Constituent']==constituent].values[0]
        if constituent =='adidas':
            constituent='Adidas'
        db[args.collection_store_colors].update_one({"constituent":constituent}, {"$set":{"date":str(datetime.date.today()),"profitability":prof_color, "risk":risk_color}})


def color_coding(combined_profitability_board,VaR_score_board):
    color_coding_table = pd.DataFrame()
    #all_constituents = ['Allianz', 'adidas', 'BASF', 'Bayer', 'Beiersdorf','BMW', 'Commerzbank', 'Continental', 'Daimler','Deutsche Bank', u'Deutsche B\xf6rse', 'Deutsche Post','Deutsche Telekom', 'EON', 'Fresenius', 'HeidelbergCement', 'Infineon','Linde','Lufthansa', 'Merck', 'RWE', 'SAP', 'Siemens', 'thyssenkrupp','Vonovia','Fresenius Medical Care',u'M\xfcnchener R\xfcckversicherungs-Gesellschaft','ProSiebenSat1 Media','Volkswagen (VW) vz']
    #all_constituents_dict = {'Allianz':'Allianz', 'Adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                   # 'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                   # 'Deutsche Bank':'Deutsche Bank', 'Deutsche Börse':u'Deutsche B\xf6rse', 'Deutsche Post':'Deutsche Post',
                   #'Deutsche Telekom':'Deutsche Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius Medical Care',
                   #'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Infineon':'Infineon',
                   #'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': u'M\xfcnchener R\xfcckversicherungs-Gesellschaft',
                   #'ProSiebenSat1 Media':'ProSiebenSat1 Media', 'RWE':'RWE', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                   #'Volkswagen (VW) vz':'Volkswagen (VW) vz','Vonovia':'Vonovia'}
    constituents_list = [str(item) for item in args.constituents_list.split(',')]
    for constituent in constituents_list:
        print constituent
        ##Combined profitability: out of 60, Risk out of 6
        profitability_score = int(max(combined_profitability_board['Total profitability score'].loc[combined_profitability_board['Constituent']==constituent]))
        risk_score = VaR_score_board['Risk score'].loc[VaR_score_board['Constituent']==constituent].values[0]
        
        if profitability_score>32:
            profitability_color = 1
        elif profitability_score>19:
            profitability_color = 0
        else:
            profitability_color = -1
        
        if risk_score<=2:
            risk_color = 1
        elif risk_score<=4:
            risk_color = 0
        else:
            risk_color = -1
            
        color_coding_table=color_coding_table.append(pd.DataFrame({'Constituent':constituent, 'Profitability':profitability_color, 'Risk':risk_color},index=[0]),ignore_index=True)
    color_coding_table=color_coding_table.reindex(columns=['Constituent','Profitability','Risk'])
    
    return color_coding_table


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The mongodb connection string')
    parser.add_argument('database',help='Name of the database')
    parser.add_argument('collection_get_profitability_ranking', help='The collection from which Profitability Ranking is extracted')
    parser.add_argument('collection_get_risk_ranking', help='The collection from which Risk Ranking is extracted')
    parser.add_argument('-l', '--constituents_list',help='List of all DAX 30 constituents avaliable',type=str)
    parser.add_argument('collection_store_colors', help='The collection where the coloring result will be stored')
    

    args = parser.parse_args()
    
    
    colors_main(args)