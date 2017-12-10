import requests
import xml.etree.ElementTree as ET

class SOAPUtils:
    def __init__(self):
        pass

    def get_token(self, user, pwd, database):
        # get access token
        url = 'https://webservices.bvdep.com/{}/remoteaccess.asmx'.format(database)
        headers = {'Content-Type': 'text/xml'}
        data = """<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
          <soap12:Body>
            <Open xmlns="http://bvdep.com/webservices/">
              <username>{}</username>
              <password>{}</password>
            </Open>
          </soap12:Body>
        </soap12:Envelope>""".format(user, pwd)

        response = requests.post(url, headers=headers, data=data)

        if response.status_code != requests.codes.ok:
            print(response.text)
            return None

        OpenResponse = ET.fromstring(response.text)
        token = OpenResponse[0][0][0].text
        print(token)
        return token

    def close_connection(self, token, database):
        print("Closing connection")
        # get access token
        url = 'https://webservices.bvdep.com/{}/remoteaccess.asmx'.format(database)
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

    def find_by_bvd_id(self, token, bvdid, database):
        url = 'https://webservices.bvdep.com/{}/remoteaccess.asmx'.format(database)
        headers = {'Content-Type': 'text/xml'}
        # FindByBvdId
        data = """<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
          <soap12:Body>
            <FindByBVDId xmlns="http://bvdep.com/webservices/">
              <sessionHandle>{}</sessionHandle>
              <id>{}</id>
            </FindByBVDId>
          </soap12:Body>
        </soap12:Envelope>""".format(token, bvdid)

        response = requests.post(url, headers=headers, data=data)

        if response.status_code != requests.codes.ok:
            print(response.text)
            return None

        FindByBvdId = ET.fromstring(response.text)
        SelectionResult_token = FindByBvdId[0][0][0][0].text
        SelectionResult_count = FindByBvdId[0][0][0][1].text
        return SelectionResult_token, SelectionResult_count

    def find_with_strategy(self, token, strategy, database):
        url = 'https://webservices.bvdep.com/{}/remoteaccess.asmx'.format(database)
        headers = {'Content-Type': 'text/xml'}

        data = """<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Body>
        <FindWithStrategy xmlns="http://bvdep.com/webservices/">
          <sessionHandle>{}</sessionHandle>
          <strategy>{}</strategy>
        </FindWithStrategy>
      </soap12:Body>
    </soap12:Envelope>""".format(token, strategy)

        response = requests.post(url, headers=headers, data=data)

        if response.status_code != requests.codes.ok:
            print(response.text)
            return None

        FindWithStrategy = ET.fromstring(response.text)
        SelectionResult_token = FindWithStrategy[0][0][0][0].text
        SelectionResult_count = FindWithStrategy[0][0][0][1].text
        print(SelectionResult_token)
        print(SelectionResult_count)
        return SelectionResult_token, SelectionResult_count

    def get_data(self, token, SelectionResult_token, SelectionResult_count, query, database, timeout, number_of_records=5):
        url = 'https://webservices.bvdep.com/{}/remoteaccess.asmx'.format(database)
        headers = {'Content-Type': 'text/xml'}
        data = """<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
          <soap12:Body>
            <GetData xmlns="http://bvdep.com/webservices/">
              <sessionHandle>{0}</sessionHandle>
              <selection>
                <Token>{1}</Token>
                <SelectionCount>{2}</SelectionCount>
              </selection>
              <query>{3}</query>
              <fromRecord>0</fromRecord>
              <nrRecords>{4}</nrRecords>
              <resultFormat>CSV</resultFormat>
            </GetData>
          </soap12:Body>
        </soap12:Envelope>""".format(token, SelectionResult_token, SelectionResult_count, query, number_of_records)

        response = requests.post(url, headers=headers, data=data, timeout=timeout)
        return response.text

        result = ET.fromstring(response.text)
        csv_result = result[0][0][0].text

        if response.status_code != requests.codes.ok:
            return None

        file_name = "{}_{}.csv".format(constituent, field)
        directory = os.path.join(".", "data", file_name)
        with open(str(directory), 'w') as f:
            f.write(csv_result)

        return True

    def get_report_section(self, token, SelectionResult_token, SelectionResult_count, database, section_id):
        url = 'https://webservices.bvdep.com/{}/remoteaccess.asmx'.format(database)
        headers = {'Content-Type': 'text/xml'}
        data = """<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Body>
        <GetReportSection xmlns="http://bvdep.com/webservices/">
          <sessionHandle>{0}</sessionHandle>
          <selection>
            <Token>{1}</Token>
            <SelectionCount>{2}</SelectionCount>
          </selection>
          <fromRecord>0</fromRecord>
          <nrRecords>2</nrRecords>
          <sectionId>{3}</sectionId>
          <format>CSV</format>
        </GetReportSection>
      </soap12:Body>
    </soap12:Envelope>""".format(token, SelectionResult_token, SelectionResult_count, section_id)

        response = requests.post(url, headers=headers, data=data, timeout=60)
        if response.status_code != requests.codes.ok:
            return None
        else:
            return response.text


if __name__=="__main__":
    pass