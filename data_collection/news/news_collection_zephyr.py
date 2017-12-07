import requests
import xml.etree.ElementTree as ET
import os
from datetime import datetime
from io import StringIO
import pandas as pd
import sys
import json
from pprint import pprint, pformat
import smtplib
from timeit import default_timer as timer

#Needs some work
def get_zephyr_ma_deals(user,pwd):
    query = ""
    soap = SOAPUtils()

    strategies = ["adidas_strategy",
                  "Allianz_strategy",
                  "BASF_strategy",
                  "Bayer_strategy",
                  "Beiersdorf_strategy",
                  "BMW_strategy",
                  "Commerzbank_strategy",
                  "Continental_strategy",
                  "Daimler_strategy",
                  "Deutsche_Boerse_strategy",
                  "Deutsche_Post_strategy",
                  "Deutsche_strategy",
                  "Deutsche_Telekom_strategy",
                  "EON_strategy",
                  "Fresenius_medical_strategy",
                  "Fresenius_strategy",
                  "HeidelbergCement_strategy",
                  "Henkel_strategy",
                  "Infineon_strategy",
                  "Linde_strategy",
                  "Lufthansa_strategy",
                  "Merck_strategy",
                  "Munchener_strategy",
                  "Prosiebel_strategy",
                  "RWE_strategy",
                  "SAP_strategy",
                  "Siemens_strategy",
                  "thyssenkrupp_strategy",
                  "Volkswagen_strategy",
                  "Vonovia_strategy"]

    data = "all_deals"

    for strategy in strategies:
        token = soap.get_token(user, pwd, "zephyr")
        if not token:
            return None
        try:
            selection_token, selection_count = soap.find_with_strategy(token, strategy, "zephyr")
            get_data_result = soap.get_data(token, selection_token, selection_count, long_query, data, strategy, "zephyr")
        except Exception as e:
            print(str(e))
        finally:
            soap.close_connection(token, "zephyr")