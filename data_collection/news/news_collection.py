import requests
import xml.etree.ElementTree as ET
import os
from datetime import datetime
from io import StringIO
import pandas as pd
import sys
import json

#Deprecated
def get_orbis_news(user, pwd):
    soap = SOAPUtils()

    query = """" SELECT NEWS_DATE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_TITLE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ARTICLE_TXT USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COMPANIES USING [Parameters.RepeatingDimension=NewsDim],
NEWS_TOPICS USING [Parameters.RepeatingDimension=NewsDim],
NEWS_COUNTRY USING [Parameters.RepeatingDimension=NewsDim],
NEWS_REGION USING [Parameters.RepeatingDimension=NewsDim],
NEWS_SOURCE USING [Parameters.RepeatingDimension=NewsDim],
NEWS_PUBLICATION USING [Parameters.RepeatingDimension=NewsDim],
NEWS_ID USING [Parameters.RepeatingDimension=NewsDim] FROM RemoteAccess.A"""

    constituents = [('Allianz', 'DEFEI1007380'), ('adidas', 'DE8190216927'),
                    ('BASF', 'DE7150000030'), ('Bayer', 'DE5330000056'),
                    ('Beiersdorf', 'DE2150000164'), ("BMW", "DE8170003036"),
                    ('Continental', 'DE2190001578'), ('Commerzbank', 'DEFEB13190'),
                    ('Daimler', 'DE7330530056'), ("Deutsche Bank", "DEFEB13216"),
                    ('Deutsche Börse', 'DEFEB54555'), ('Deutsche Post', 'DE5030147191'),
                    ('Deutsche Telekom', 'DE5030147137'), ('EON', 'DE5050056484'),
                    ('Fresenius Medical Care', 'DE8110066557'),
                    ('Fresenius', 'DE6290014544'), ('HeidelbergCement', 'DE7050000100'),
                    ('Henkel vz', 'DE5050001329'), ('Infineon', 'DE8330359160'),
                    ('Linde', 'DE8170014684'), ('Lufthansa', 'DE5190000974'),
                    ('Merck', 'DE6050108507'),
                    ('Münchener Rückversicherungs-Gesellschaft', 'DEFEI1007130'),
                    ('ProSiebenSat1 Media', 'DE8330261794'), ('RWE', 'DE5110206610'),
                    ('SAP', 'DE7050001788'), ('Siemens', 'DE2010000581'),
                    ('thyssenkrupp', 'DE5110216866'), ('Volkswagen (VW) vz', 'DE2070000543'),
                    ('Vonovia', 'DE5050438829')]

    data = "all_news"

    for name, bvdid in constituents:
        token = soap.get_token(user, pwd, "orbis")
        if not token:
            return None
        try:
            selection_token = soap.find_by_bvd_id(token, bvdid, "orbis")
            get_data_result = soap.get_data(token, selection_token, "1", query, data, name, "orbis")
        except Exception as e:
            print(str(e))
        finally:
            soap.close_connection(token, "zephyr")

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

#Deprecated
def get_historical_orbis_news_old(user, pwd, database, google_key_path, param_connection_string):
    soap = SOAPUtils()
    storage = Storage()

    fields = ["NEWS_DATE", "NEWS_TITLE", "NEWS_ARTICLE_TXT",
              "NEWS_COMPANIES", "NEWS_TOPICS", "NEWS_COUNTRY", "NEWS_REGION",
              "NEWS_LANGUAGE", "NEWS_SOURCE", "NEWS_PUBLICATION", "NEWS_ID"]

    filter = ["NEWS_DATE_NewsDim", "NEWS_TITLE_NewsDim", "NEWS_ARTICLE_TXT_NewsDim",
              "NEWS_COMPANIES_NewsDim", "NEWS_TOPICS_NewsDim", "NEWS_COUNTRY_NewsDim", "NEWS_REGION_NewsDim",
              "NEWS_LANGUAGE_NewsDim", "NEWS_SOURCE_NewsDim", "NEWS_PUBLICATION_NewsDim", "NEWS_ID_NewsDim"]

    columns = ["NAME", "ISIN"]
    table = "CONSTITUENTS_MASTER"

    constituents = storage.get_sql_data(sql_connection_string=param_connection_string,
                          sql_table_name=table,
                          sql_column_list=columns)

    constituents = [('Allianz', 'DEFEI1007380')]

    for name, bvdid in constituents:

        file_name = "{}_historical_news.json".format(name)

        all_df = []

        i = 0
        while i < len(fields):
            print("i:{}".format(i))
            token = soap.get_token(user, pwd, database)
            query = "SELECT {} USING [Parameters.RepeatingDimension=NewsDim] FROM RemoteAccess.A".format(fields[i])
            selection_token, selection_count = soap.find_by_bvd_id(token, bvdid, database)
            print("Getting {} data".format(fields[i]))
            try:
                get_data_result = soap.get_data(token, selection_token, selection_count, query, fields[i], name, database)
            except Exception as e:
                print(str(e))
                continue
            finally:
                soap.close_connection(token, database)

            result = ET.fromstring(get_data_result)
            csv_result = result[0][0][0].text

            TESTDATA = StringIO(csv_result)

            if fields[i] == "NEWS_DATE":
                all_df.append(pd.read_csv(TESTDATA, sep=",", parse_dates=["NEWS_DATE_NewsDim"]))
            else:
                all_df.append(pd.read_csv(TESTDATA, sep=","))

            i += 1

        df = pd.concat(all_df, axis=1)
        df = df[filter]
        df.columns = fields
        df.to_json(file_name, orient="records", date_format="iso")

        # Save to MongoDB

        # Save to cloud
        if os.path.isfile(file_name):
            cloud_destination = "2017/{}".format(file_name)
            if storage.upload_to_cloud_storage(google_key_path,"igenie-news", file_name,cloud_destination):
                os.remove(file_name)
            else:
                print("File not uploaded to Cloud storage.")
        else:
            print("File does not exists in the local filesystem.")

def get_sentiment_word(score):
    if score > 0.25:
        return "positive"
    elif score < -0.25:
        return "negative"
    else:
        return "neutral"

def get_historical_orbis_news(user, pwd, database, google_key_path, param_connection_string):
    #get parameters
    connection_string = "mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp"
    soap = SOAPUtils()
    storage = Storage(google_key_path, connection_string)
    tagger = TU()

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "BVDID"]
    table = "MASTER_CONSTITUENTS"

    constituents = storage.get_sql_data(sql_connection_string=param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)[28:]

    for constituent_id, constituent_name, bvdid in constituents:
        records = 0
        start = 0
        end = 20
        filename = "bq_news_{}.json".format(constituent_id)
        print("Constituent: {},{}".format(constituent_name,bvdid))

        with open(filename, "a") as f:
            while True:
                try:
                    token = soap.get_token(user, pwd, database)
                    query = "SELECT LINE BVDNEWS.NEWS_DATE USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_date, " \
                            "LINE BVDNEWS.NEWS_TITLE USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_title," \
                            "LINE BVDNEWS.NEWS_ARTICLE_TXT USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_article_txt, " \
                            "LINE BVDNEWS.NEWS_COMPANIES USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_companies, " \
                            "LINE BVDNEWS.NEWS_TOPICS USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_topics," \
                            "LINE BVDNEWS.NEWS_COUNTRY USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_country," \
                            "LINE BVDNEWS.NEWS_REGION USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_region," \
                            "LINE BVDNEWS.NEWS_LANGUAGE USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_language," \
                            "LINE BVDNEWS.NEWS_SOURCE USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_source," \
                            "LINE BVDNEWS.NEWS_PUBLICATION USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_publication," \
                            "LINE BVDNEWS.NEWS_ID USING [Parameters.RepeatingDimension=NewsDim;Parameters.RepeatingOffset={0};Parameters.RepeatingMaxCount={1}] AS news_id FROM RemoteAccess.A".format(
                        start, end)

                    selection_token, selection_count = soap.find_by_bvd_id(token, bvdid, database)

                    get_data_result = soap.get_data(token, selection_token, selection_count, query, database)
                    # print(get_data_result)
                except Exception as e:
                    print(str(e))
                finally:
                    if token:
                        soap.close_connection(token, database)

                result = ET.fromstring(get_data_result)
                csv_result = result[0][0][0].text

                TESTDATA = StringIO(csv_result)
                df = pd.read_csv(TESTDATA, sep=",", parse_dates=["news_date"])

                if pd.isnull(df.iloc[0, 2]) or records == 1000:
                    break

                # Remove duplicate columns
                df.drop_duplicates(["news_title"], inplace=True)

                # Get sentiment score
                df["score"] = df.apply(lambda row: get_nltk_sentiment(str(row["news_article_txt"])), axis=1)

                # get sentiment word
                df["sentiment"] = df.apply(lambda row: get_sentiment_word(row["score"]), axis=1)

                # add constituent name, id and old name
                df["constituent_id"] = constituent_id
                df["constituent_name"] = constituent_name
                old_constituent_name = get_old_constituent_name(constituent_id)
                df["constituent"] = old_constituent_name

                # add URL
                df["url"] = None

                # add show
                df["show"] = True

                # get entity tags
                entity_tags = []
                for text in df["news_title"]:
                    tags = get_spacey_tags(tagger.get_spacy_entities(text))
                    entity_tags.append(tags)

                fields = ["news_date", "news_title", "news_article_txt", "news_companies", "news_source",
                          "news_publication","news_topics", "news_country", "news_region",
                          "news_language", "news_id","score", "sentiment","constituent_id",
                          "constituent_name", "constituent", "url", "show"]

                '''
                # Save to MongoDB
                filter = ["NEWS_DATE_NewsDim", "NEWS_TITLE_NewsDim", "NEWS_ARTICLE_TXT_NewsDim",
                          "NEWS_SOURCE_NewsDim", "NEWS_PUBLICATION_NewsDim", "categorised_tag", "score", "sentiment",
                          "constituent_id", "constituent_name", "constituent","url", "show"]

                df_mongo = df[fields]
                df_mongo.columns = filter

                mongo_data = json.loads(df_mongo.to_json(orient="records", date_format="iso"))

                # set entity_tag field
                i = 0
                for i in range(0, len(mongo_data)):
                    mongo_data[i]["entity_tags"] = entity_tags[i]
                    date = mongo_data[i]["NEWS_DATE_NewsDim"]
                    #ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    mongo_data[i]["NEWS_DATE_NewsDim"] = ts

                #storage.save_to_mongodb(mongo_data, "dax_gcp", "all_news")
                '''

                # Save to BigQuery
                df_bigquery = df[fields]
                bigquery_data = json.loads(df_bigquery.to_json(orient="records", date_format="iso"))

                # set entity_tag field
                i = 0
                for i in range(0, len(bigquery_data)):
                    bigquery_data[i]["entity_tags"] = entity_tags[i]
                    date = bigquery_data[i]["news_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    bigquery_data[i]["news_date"] = ts

                    bigquery_data[i]["news_title"] = str(bigquery_data[i]["news_title"])
                    bigquery_data[i]["news_article_txt"] = str(bigquery_data[i]["news_article_txt"])
                    bigquery_data[i]["news_source"] = str(bigquery_data[i]["news_source"])
                    bigquery_data[i]["news_publication"] = str(bigquery_data[i]["news_publication"])
                    bigquery_data[i]["news_topics"] = str(bigquery_data[i]["news_topics"]) #list
                    bigquery_data[i]["sentiment"] = str(bigquery_data[i]["sentiment"])
                    bigquery_data[i]["constituent_id"] = str(bigquery_data[i]["constituent_id"])
                    bigquery_data[i]["constituent_name"] = str(bigquery_data[i]["constituent_name"])
                    bigquery_data[i]["constituent"] = str(bigquery_data[i]["constituent"])

                    bigquery_data[i]["news_companies"] = str(bigquery_data[i]["news_companies"]) #list
                    bigquery_data[i]["news_country"] = str(bigquery_data[i]["news_country"]) #list
                    bigquery_data[i]["news_region"] = str(bigquery_data[i]["news_region"]) #list
                    bigquery_data[i]["news_language"] = str(bigquery_data[i]["news_language"])
                    bigquery_data[i]["news_id"] = str(bigquery_data[i]["news_id"])


                    f.write(json.dumps(bigquery_data[i], cls=MongoEncoder) + '\n')

                # storage.insert_bigquery_data("pecten_dataset", "news", bigquery_data)

                start = end + 1
                end = start + 20
                records += 20
                print("Records saved: {}".format(records))



def get_daily_orbis_news(user, pwd, database, google_key_path, param_connection_string):
    soap = SOAPUtils()
    storage = Storage()

    fields = ["NEWS_DATE", "NEWS_TITLE", "NEWS_ARTICLE_TXT",
              "NEWS_COMPANIES", "NEWS_TOPICS", "NEWS_COUNTRY", "NEWS_REGION",
              "NEWS_LANGUAGE", "NEWS_SOURCE", "NEWS_PUBLICATION", "NEWS_ID"]

    filter = ["NEWS_DATE_NewsDim", "NEWS_TITLE_NewsDim", "NEWS_ARTICLE_TXT_NewsDim",
              "NEWS_COMPANIES_NewsDim", "NEWS_TOPICS_NewsDim", "NEWS_COUNTRY_NewsDim", "NEWS_REGION_NewsDim",
              "NEWS_LANGUAGE_NewsDim", "NEWS_SOURCE_NewsDim", "NEWS_PUBLICATION_NewsDim", "NEWS_ID_NewsDim"]

    columns = ["NAME", "ISIN"]
    table = "CONSTITUENTS_MASTER"

    '''
    constituents = storage.get_sql_data(sql_connection_string=param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)
    '''
    constituents = [('Allianz', 'DEFEI1007380')]

    for name, bvdid in constituents:
        file_name = "{}_{}".format(name, str(datetime.datetime.today().date()))
        token = soap.get_token(user, pwd, database)
        selection_token, selection_count = soap.find_by_bvd_id(token, bvdid, database)

        try:
            get_report_section_result = soap.get_report_section(token,selection_token,selection_count,
                                                           database,"BVDNEWS")
        except Exception as e:
            print(str(e))
        finally:
            soap.close_connection(token, database)

        #print(get_report_section_result)
        result = ET.fromstring(get_report_section_result)
        csv_result = result[0][0][0].text

        TESTDATA = StringIO(csv_result)
        df = pd.read_csv(TESTDATA, sep=",", parse_dates=["NEWS_DATE_NewsDim"])
        df = df[filter]
        df.columns = fields

        #Filter today's news only
        d = datetime.datetime.today() - datetime.timedelta(days=1)
        df = df.loc[df["NEWS_DATE"] > d.date()]

        df.to_json(file_name, orient="records", date_format="iso")

        # Save to MongoDB

        # Save to cloud
        if os.path.isfile(file_name):
            cloud_destination = "2017/{}".format(file_name)
            if storage.upload_to_cloud_storage(google_key_path,"igenie-news", file_name,cloud_destination):
                os.remove(file_name)
                print("File uploaded to cloud storage")
            else:
                print("File not uploaded to Cloud storage.")
        else:
            print("File does not exists in the local filesystem.")

#Development halted for now
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

def main(args):
    #get_zephyr_data(args.user,args.pwd)
    #get_orbis_news(args.user,args.pwd)
    get_historical_orbis_news(args.user,args.pwd, "orbis", args.google_key_path, args.param_connection_string)
    #get_daily_orbis_news(args.user,args.pwd,"orbis",args.google_key_path,args.param_connection_string)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('user', help='SOAP user')
    parser.add_argument('pwd', help='SOAP pwd')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils.SOAPUtils import SOAPUtils
    from utils.twitter_analytics_helpers import *
    from utils.TaggingUtils import TaggingUtils as TU
    main(args)


