import requests
import xml.etree.ElementTree as ET
from collections import defaultdict
import sys
import os
import time
from clint.textui import progress

def get_token(user,pwd):
    #get access token
    url = 'https://webservices.bvdep.com/orbis/remoteaccess.asmx'
    headers = {'Content-Type': 'text/xml'}
    data = """<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Body>
        <Open xmlns="http://bvdep.com/webservices/">
          <username>{}</username>
          <password>{}</password>
        </Open>
      </soap12:Body>
    </soap12:Envelope>""".format(user,pwd)

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != requests.codes.ok:
        print(response.text)
        return None

    OpenResponse = ET.fromstring(response.text)
    token = OpenResponse[0][0][0].text
    print(token)
    return token

def close_connection(token):
    #get access token
    url = 'https://webservices.bvdep.com/orbis/remoteaccess.asmx'
    headers = {'Content-Type': 'text/xml'}
    data = """<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <Close xmlns="http://bvdep.com/webservices/">
      <sessionHandle>{}</sessionHandle>
    </Close>
  </soap12:Body>
</soap12:Envelope>""".format(token)

    response = requests.post(url, headers=headers, data=data)

def find_by_bvd_id(token, bvdid):
    url = 'https://webservices.bvdep.com/orbis/remoteaccess.asmx'
    headers = {'Content-Type': 'text/xml'}
    #FindByBvdId
    data = """<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Body>
        <FindByBVDId xmlns="http://bvdep.com/webservices/">
          <sessionHandle>{}</sessionHandle>
          <id>{}</id>
        </FindByBVDId>
      </soap12:Body>
    </soap12:Envelope>""".format(token,bvdid)

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != requests.codes.ok:
        print(response.text)
        return None

    FindByBvdId = ET.fromstring(response.text)
    SelectionResult_token = FindByBvdId[0][0][0][0].text
    print(SelectionResult_token)
    return SelectionResult_token

def get_data(token, SelectionResult_token, query, field, constituent):
    url = 'https://webservices.bvdep.com/orbis/remoteaccess.asmx'
    headers = {'Content-Type': 'text/xml'}
    data = """<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <GetData xmlns="http://bvdep.com/webservices/">
      <sessionHandle>{0}</sessionHandle>
      <selection>
        <Token>{1}</Token>
        <SelectionCount>1</SelectionCount>
      </selection>
      <query>{2}</query>
      <fromRecord>0</fromRecord>
      <nrRecords>5</nrRecords>
      <resultFormat>CSV</resultFormat>
    </GetData>
  </soap12:Body>
</soap12:Envelope>""".format(token, SelectionResult_token, query)

    response = requests.post(url, headers=headers, data=data, stream=True)

    if response.status_code != requests.codes.ok:
        return None

    file_name = "{}_{}.xml".format(constituent, field)
    directory = os.path.join(".", "data", file_name)
    with open(str(directory), 'wb') as f:
        total_length = int(response.headers.get('content-length'))
        for chunk in progress.bar(response.iter_content(chunk_size=1024), expected_size=(total_length / 1024) + 1):
            if chunk:
                f.write(chunk)
                f.flush()

    return True

def get_custom_data(user, pwd, query, bvdid, data, constituent):
    token = get_token(user,pwd)
    selection_token = find_by_bvd_id(token, bvdid)

    try:
        get_data_result = get_data(token, selection_token, query, data, constituent)
    except:
        pass
    finally:
        close_connection(token)

def main(user, pwd):
    constituents = [('adidas','DE8190216927'),
                    ('Commerzbank', 'DEFEB13190'),
                    ('EON', 'DE5050056484'),
                    ("BMW", "DE8170003036")
                    ]

    constituents = [('Allianz','DEFEI1007380'),
                    ('BASF','DE7150000030'), ('Bayer','DE5330000056'),
                    ('Beiersdorf','DE2150000164'),
                    ('Continental','DE2190001578'),
                    ('Daimler','DE7330530056'),("Deutsche Bank","DEFEB13216"),
                    ('Deutsche Börse','DEFEB54555'),('Deutsche Post','DE5030147191'),
                    ('Deutsche Telekom','DE5030147137'),
                    ('Fresenius Medical Care','DE8110066557'),
                    ('Fresenius','DE6290014544'), ('HeidelbergCement','DE7050000100'),
                    ('Henkel vz','DE5050001329'), ('Infineon','DE8330359160'),
                    ('Linde','DE8170014684'),('Lufthansa','DE5190000974'),
                    ('Merck','DE6050108507'),
                    ('Münchener Rückversicherungs-Gesellschaft','DEFEI1007130'),
                    ('ProSiebenSat1 Media','DE8330261794'),('RWE','DE5110206610'),
                    ('SAP','DE7050001788'), ('Siemens','DE2010000581'),
                    ('thyssenkrupp','DE5110216866'), ('Volkswagen (VW) vz','DE2070000543'),
                    ('Vonovia','DE5050438829')]

    q = """" SELECT NEWS_DATE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_TITLE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ARTICLE_TXT USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COMPANIES USING [Parameters.RepeatingDimension=NewsDim],
NEWS_TOPICS USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COUNTRY USING [Parameters.RepeatingDimension=NewsDim],
NEWS_REGION USING [Parameters.RepeatingDimension=NewsDim],
NEWS_SOURCE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_PUBLICATION USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ID USING [Parameters.RepeatingDimension=NewsDim] FROM RemoteAccess.A"""

    for name, id in constituents:
        get_custom_data(user, pwd, q, id, "all_news", name)

def main_rest(api_key):
    constituents = [("BMW", "DE8170003036"), ("Commerzbank", "DEFEB13190"),
                    ("Deutsche Bank", "DEFEB13216"),
                    ("EON", "DE5050056484")]

    url = 'https://webservices.bvdep.com/rest/orbis/getdata'
    headers = {'apitoken': api_key, "Accept": "application/json, text/javascript, */*; q=0.01"}

    const = [("BMW", "DE8170003036")]

    for name, id in const:
        payload = {'bvdids': id}

        data = {"BvDIds":[id],
                "QueryString":"SELECT NEWS_DATE USING [Parameters.RepeatingDimension=NewsDim], NEWS_TITLE USING [Parameters.RepeatingDimension=NewsDim],NEWS_ARTICLE_TXT USING [Parameters.RepeatingDimension=NewsDim],NEWS_COMPANIES USING [Parameters.RepeatingDimension=NewsDim],NEWS_TOPICS USING [Parameters.RepeatingDimension=NewsDim],NEWS_COUNTRY USING [Parameters.RepeatingDimension=NewsDim],NEWS_REGION USING [Parameters.RepeatingDimension=NewsDim],NEWS_LANGUAGE USING [Parameters.RepeatingDimension=NewsDim],NEWS_SOURCE USING [Parameters.RepeatingDimension=NewsDim],NEWS_PUBLICATION USING [Parameters.RepeatingDimension=NewsDim],NEWS_ID USING [Parameters.RepeatingDimension=NewsDim] FROM RemoteAccess.A"}

        response = requests.post(url, headers=headers, data=data, params=payload)

        if response.status_code != requests.codes.ok:
            print(response.text)
            return None

        result = response.text
        file_name = "{}_{}.xml".format(name, "all_news")
        directory = os.path.join(".", "data", file_name)
        with open(str(directory), 'w') as f:
            f.write(result)
        return True

if __name__ == "__main__":
    print(sys.argv)
    main(*sys.argv[1:])
    #api_key = "157B84e63f7c059ae7118a1a2c44fd99a5a0"
    #main_rest(api_key)