import sys
#sys.path.insert(0, str(Path('..', '..', 'utils')))
from datetime import datetime
import requests
import time
import os

all_constituents = {}

all_constituents[0] = {'DAX': 'HISTORY+998032;830;814+TICKS+', 'Allianz':'HISTORY+322646;13;814+TICKS+', 'adidas':'HISTORY+11730015;44;814+TICKS+',
                    'BASF':'HISTORY+11450563;44;814+TICKS+', 'Bayer': 'HISTORY+10367293;44;814+TICKS+', 'Beiersdorf':'HISTORY+324660;44;814+TICKS+',
                    'BMW':'HISTORY+324410;44;814+TICKS+', 'Commerzbank':'HISTORY+21170377;44;814+TICKS+','Continental': 'HISTORY+327800;44;814+TICKS+',
                    'Daimler':'HISTORY+945657;44;814+TICKS+'}

all_constituents[1] = {'Deutsche Bank':'HISTORY+829257;13;814+TICKS+', 'Deutsche Borse':'HISTORY+1177233;44;814+TICKS+',
                    'Deutsche Post': 'HISTORY+1124244;44;814+TICKS+', 'Deutsche Telekom':'HISTORY+1124244;44;814+TICKS+', 'EON':'HISTORY+4334819;44;814+TICKS+',
                    'Fresenius Medical Care':'HISTORY+520878;44;814+TICKS+', 'Fresenius':'HISTORY+332902;44;814+TICKS+', 'HeidelbergCement':'HISTORY+335740;44;814+TICKS+',
                    'Henkel vz':'HISTORY+335910;13;814+TICKS+', 'Infineon':'HISTORY+1038049;44;814+TICKS+'}


all_constituents[2] = {'Linde':'HISTORY+340045;44;814+TICKS+',
                    'Lufthansa':'HISTORY+667979;44;814+TICKS+', 'Merck':'HISTORY+412799;44;814+TICKS+',
                    'Munchener Ruckversicherungs-Gesellschaft':'HISTORY+341960;44;814+TICKS+', 'ProSiebenSat1 Media':'HISTORY+21967295;13;814+TICKS+',
                    'RWE':'HISTORY+1158883;13;814+TICKS+', 'SAP':'HISTORY+345952;44;814+TICKS+', 'Siemens': 'HISTORY+827766;44;814+TICKS+',
                    'thyssenkrupp':'HISTORY+412006;44;814+TICKS+', 'Volkswagen (VW) vz':'HISTORY+352781;44;814+TICKS+',
                    'Vonovia':'HISTORY+21644750;13;814+TICKS+'}

number_of_ticks = ['1', '6000']

'''
args:
connection_string:string
database:string
batch:boolean
part:int

'''

def real_time_wrapper(argv):
    base_url = "http://charts.finanzen.net/ChartData.ashx?request="
    database = DB(argv['connection_string'], argv['database'])

    current_part = all_constituents[argv['part']]
    for constiuent in current_part.keys():
        url = base_url + current_part[constiuent]
        print("Extracting tick data from {}".format(constiuent))
        result = extract_real_time_values_batch(url, database, constiuent)
        print(result)
        time.sleep(30)

    return "Success saving real-time data."

def extract_real_time_values(url, database, constituent):
    url = url + number_of_ticks[0]
    try:
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            data = response.text.splitlines()[1].split(';')
            document = {'constituent': constituent,
                        'date': data[0][0:10],
                        'time':data[0][11:19],
                        'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                        'price': float(data[1].replace(',', ''))}

            result = database.insert_one('dax_real_time', document)

            return result
        else:
            return "Error making request, code {}".format(response.status_code)
    
    except Exception as ex:
        return str(ex)

def extract_real_time_values_batch(url, database, constituent):
    url = url + number_of_ticks[1]
    list_of_ticks = []
    try:
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            data = response.text.splitlines()

            for item in data[1:-2]:
                fields = item.split(';')

                document = {'constituent': constituent,
                        'date': fields[0][0:10],
                        'time':fields[0][11:19],
                        'datetime': datetime.strptime(fields[0], "%Y-%m-%d-%H-%M-%S-%f"),
                        'price': float(fields[1].replace(',', ''))}

                list_of_ticks.append(document)

            result = database.bulk_insert('dax_real_time', list_of_ticks)

            return result
        else:
            print("Error making request, code {}. Retrying...".format(response.status_code))
            time.sleep(20)
            response = requests.get(url)
            if response.status_code == requests.codes.ok:
                data = response.text.splitlines()

                for item in data[1:-2]:
                    fields = item.split(';')

                    document = {'constituent': constituent,
                                'date': fields[0][0:10],
                                'time': fields[0][11:19],
                                'datetime': datetime.strptime(fields[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                'price': float(fields[1].replace(',', ''))}

                    list_of_ticks.append(document)

                result = database.bulk_insert('dax_real_time', list_of_ticks)

                return result
            else:
                return "Error making request, code {}. Skipping.".format(response.status_code)
    
    except Exception as ex:
        return str(ex)

def main(args):
    script_directory = os.path.dirname(os.path.realpath(__file__))
    utils_directory = os.path.join(script_directory, "..", "..", "utils")
    sys.path.insert(0, str(utils_directory))
    from DB import DB

    response = real_time_wrapper(
        {'connection_string': args.connection_string,
         'database': args.database,
         'part': int(args.part)})

    print(response)
    #return response

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The MongoDB connection string')
    parser.add_argument('database', help='The MongoDB database')
    parser.add_argument('part', help='The group of constituents')
    args = parser.parse_args()
    main(args)






