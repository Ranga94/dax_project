import requests
import xml.etree.ElementTree as ET
from collections import defaultdict
import sys
import os
import time

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

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != requests.codes.ok:
        '''
        print(response.text)
        with open("./results/failed.txt", 'a') as f:
            f.write("{} \n".format(field))
        '''
        return None

    ''''
    with open("./results/success.txt", 'a') as f:
        f.write("{} \n".format(field))
    '''

    result = response.text
    file_name = "{}_{}.xml".format(constituent, field)
    directory = os.path.join(".", "data", file_name)
    with open(str(directory), 'w') as f:
        f.write(result)
    return True

def get_custom_data(user, pwd, query, bvdid, data, constituent):
    token = get_token(user,pwd)
    selection_token = find_by_bvd_id(token, bvdid)

    get_data_result = get_data(token, selection_token, query, data, constituent)
    close_connection(token)

def main(user, pwd):
    constituents = [("BMW", "DE8170003036"),("Commerzbank","DEFEB13190"),
                    ("Deutsche Bank","DEFEB13216"),
                    ("EON","DE5050056484")]

    q = """" SELECT NEWS_DATE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_TITLE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ARTICLE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ARTICLE_TXT USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COMPANIES USING [Parameters.RepeatingDimension=NewsDim],
NEWS_TOPICS USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COUNTRY USING [Parameters.RepeatingDimension=NewsDim],
NEWS_REGION USING [Parameters.RepeatingDimension=NewsDim],
NEWS_LANGUAGE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_SOURCE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_PROV_ID USING [Parameters.RepeatingDimension=NewsDim],
NEWS_PUBLICATION USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COPYRIGHT USING [Parameters.RepeatingDimension=NewsDim],
NEWS_PRODUCTNAME USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ID USING [Parameters.RepeatingDimension=NewsDim],
NEWS_PROVNEWSID USING [Parameters.RepeatingDimension=NewsDim],
NEWS_FLAGS USING [Parameters.RepeatingDimension=NewsDim] FROM RemoteAccess.A"""

    for name, id in constituents:
        get_custom_data(user, pwd, q, id, "all_news", name)

if __name__ == "__main__":
    print(sys.argv)
    main(*sys.argv[1:])