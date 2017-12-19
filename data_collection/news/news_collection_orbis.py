import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
import pandas as pd
import sys
import json

def get_historical_orbis_news(user, pwd, database, google_key_path, param_connection_string):
    #get parameters
    soap = SOAPUtils()
    storage = Storage(google_key_path)
    tagger = TU()

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "BVDID"]
    table = "MASTER_CONSTITUENTS"

    constituents = storage.get_sql_data(sql_connection_string=param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)[2:]

    to_skip = ["BAYERISCHE MOTOREN WERKE AG","DEUTSCHE BANK AG",
               "SIEMENS AG", "SAP SE", "VOLKSWAGEN AG"]

    for constituent_id, constituent_name, bvdid in constituents:
        limit = get_number_of_news_items(constituent_name)
        if constituent_name in to_skip:
            print("Skipping")
            continue

        records = 0
        start = 0
        max_count = 20
        filename = "bq_news_{}.json".format(constituent_id)
        print("Constituent: {},{}".format(constituent_name,bvdid))
        failed = 0

        if constituent_name == "BASF SE":
            start = 4280

        try:
            token = soap.get_token(user, pwd, database)
            selection_token, selection_count = soap.find_by_bvd_id(token, bvdid, database)
        except Exception as e:
            print(e)
            return

        with open(filename, "a") as f:
            while limit > records:
                try:
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
                        start, max_count)

                    get_data_result = soap.get_data(token, selection_token, selection_count, query, database, timeout=None)
                except Exception as e:
                    print(str(e))
                    continue
                finally:
                    pass
                    #if token:
                    #    soap.close_connection(token, database)

                result = ET.fromstring(get_data_result)
                csv_result = result[0][0][0].text

                TESTDATA = StringIO(csv_result)
                try:
                    df = pd.read_csv(TESTDATA, sep=",", parse_dates=["news_date"])
                except Exception as e:
                    print(e)
                    continue

                if df.shape[0] == 0:
                    print("No records in df")
                    continue

                #Make news_title column a string
                df.astype({"news_title":str}, copy=False, errors='ignore')

                # Remove duplicate columns
                df.drop_duplicates(["news_title"], inplace=True)

                # Get sentiment score
                df["score"] = df.apply(lambda row: tah.get_nltk_sentiment(str(row["news_article_txt"])), axis=1)
                #df["score"] = df.apply(lambda row: sia.polarity_scores(str(row["news_article_txt"]))['compound'] , axis=1)

                # get sentiment word
                df["sentiment"] = df.apply(lambda row: tah.get_sentiment_word(row["score"]), axis=1)

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
                    tags = get_spacey_tags(tagger.get_spacy_entities(str(text)))
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

                #Get needed fields
                df_bigquery = df[fields]
                bigquery_data = json.loads(df_bigquery.to_json(orient="records", date_format="iso"))

                # set entity_tag field
                i = 0
                for i in range(0, len(bigquery_data)):
                    bigquery_data[i]["entity_tags"] = entity_tags[i]

                    #set news_date
                    if "news_date" in bigquery_data[i] and bigquery_data[i]["news_date"]:
                        date = bigquery_data[i]["news_date"]
                        ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                        ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                        bigquery_data[i]["news_date"] = ts
                    else:
                        bigquery_data[i]["news_date"] = None

                    if bigquery_data[i]["news_title"]:
                        bigquery_data[i]["news_title"] = str(bigquery_data[i]["news_title"])

                    if bigquery_data[i]["news_article_txt"]:
                        bigquery_data[i]["news_article_txt"] = str(bigquery_data[i]["news_article_txt"])

                    if bigquery_data[i]["news_companies"]:
                        try:
                            bigquery_data[i]["news_companies"] = [i.strip() for i in bigquery_data[i]["news_companies"].split(";")]
                        except Exception as e:
                            bigquery_data[i]["news_companies"] = []
                    else:
                        bigquery_data[i]["news_companies"] = []

                    if bigquery_data[i]["news_topics"]:
                        try:
                            if bigquery_data[i]["news_topics"].isdigit():
                                bigquery_data[i]["news_topics"] = []
                            elif bigquery_data[i]["news_topics"] == 'None':
                                bigquery_data[i]["news_topics"] = []
                            else:
                                bigquery_data[i]["news_topics"] = [i.strip() for i in bigquery_data[i]["news_topics"].split(";")]
                        except Exception as e:
                            bigquery_data[i]["news_topics"] = []
                    else:
                        bigquery_data[i]["news_topics"] = []

                    if bigquery_data[i]["news_country"]:
                        try:
                            bigquery_data[i]["news_country"] = [i.strip() for i in bigquery_data[i]["news_country"].split(";")]
                        except Exception as e:
                            bigquery_data[i]["news_country"] = []
                    else:
                        bigquery_data[i]["news_country"] = []

                    if bigquery_data[i]["news_region"]:
                        try:
                            bigquery_data[i]["news_region"] = [i.strip() for i in bigquery_data[i]["news_region"].split(";")]
                        except Exception as e:
                            bigquery_data[i]["news_region"] = []
                    else:
                        bigquery_data[i]["news_region"] = []

                    if bigquery_data[i]["news_language"]:
                        bigquery_data[i]["news_language"] = str(bigquery_data[i]["news_language"])

                    if bigquery_data[i]["news_source"]:
                        bigquery_data[i]["news_source"] = str(bigquery_data[i]["news_source"])

                    if bigquery_data[i]["news_publication"]:
                        bigquery_data[i]["news_publication"] = str(bigquery_data[i]["news_publication"])

                    if bigquery_data[i]["news_id"]:
                        try:
                            bigquery_data[i]["news_id"] = int(bigquery_data[i]["news_id"])
                        except Exception as e:
                            bigquery_data[i]["news_id"] = str(bigquery_data[i]["news_id"])

                    if bigquery_data[i]["sentiment"]:
                        bigquery_data[i]["sentiment"] = str(bigquery_data[i]["sentiment"])

                    if bigquery_data[i]["constituent_id"]:
                        bigquery_data[i]["constituent_id"] = str(bigquery_data[i]["constituent_id"])

                    if bigquery_data[i]["constituent_name"]:
                        bigquery_data[i]["constituent_name"] = str(bigquery_data[i]["constituent_name"])

                    if bigquery_data[i]["constituent"]:
                        bigquery_data[i]["constituent"] = str(bigquery_data[i]["constituent"])


                    f.write(json.dumps(bigquery_data[i], cls=MongoEncoder) + '\n')

                # storage.insert_bigquery_data("pecten_dataset", "news", bigquery_data)

                start = start + 20
                #end = start + 10
                records += 20
                print("Records saved: {}".format(records))

        if token:
            soap.close_connection(token, database)

def get_number_of_news_items(constituent_name):
    mapping = {}
    mapping["ADIDAS AG"] = 1153
    mapping["ALLIANZ SE"] = 3063
    mapping["BASF SE"] = 14244
    mapping["BAYER AG"] = 3260
    mapping["BEIERSDORF AG"] = 505
    mapping["BAYERISCHE MOTOREN WERKE AG"] = 17676
    mapping["COMMERZBANK AKTIENGESELLSCHAFT"] = 4845
    mapping["CONTINENTAL AG"] = 2455
    mapping["DAIMLER AG"] = 3570
    mapping["DEUTSCHE BOERSE AG"] = 706
    mapping["DEUTSCHE BANK AG"] = 18647
    mapping["DEUTSCHE POST AG"] = 725
    mapping["DEUTSCHE TELEKOM AG"] = 3475
    mapping["E.ON SE"] = 1159
    mapping["FRESENIUS MEDICAL CARE AG & CO. KGAA"] = 925
    mapping["FRESENIUS SE & CO. KGAA"] = 458
    mapping["HEIDELBERGCEMENT AG"] = 610
    mapping["HENKEL AG & CO. KGAA"] = 3443
    mapping["INFINEON TECHNOLOGIES AG"] = 3761
    mapping["DEUTSCHE LUFTHANSA AG"] = 3353
    mapping["LINDE AG"] = 1454
    mapping["MERCK KGAA"] = 2171
    mapping["MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN"] = 1220
    mapping["PROSIEBENSAT.1 MEDIA SE"] = 413
    mapping["RWE AG"] = 821
    mapping["SAP SE"] = 4558
    mapping["SIEMENS AG"] = 14558
    mapping["THYSSENKRUPP AG"] = 2732
    mapping["VONOVIA SE"] = 220
    mapping["VOLKSWAGEN AG"] = 5944

    return mapping[constituent_name]

def get_daily_orbis_news(args):
    if __name__ != "__main__":
        from utils.Storage import Storage
        from utils.Storage import MongoEncoder
        from utils.SOAPUtils import SOAPUtils
        from utils import twitter_analytics_helpers as tah
        from utils.TaggingUtils import TaggingUtils as TU
        from utils.Storage import Storage
        from utils.Storage import MongoEncoder
        from utils.SOAPUtils import SOAPUtils
        from utils import logging_utils as logging_utils

    # get constituents
    soap = SOAPUtils()
    storage = Storage(args.google_key_path)
    tagger = TU()

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "BVDID"]
    table = "MASTER_CONSTITUENTS"

    constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    # Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ["LOGGING", "BVD_USERNAME","BVD_PASSWORD"]
    where = lambda x: x["SOURCE"] == 'Orbis'

    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list, where)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    for constituent_id, constituent_name, bvdid in constituents:
        #get last news date for the constituent
        query = """
            SELECT max(news_id) as max_id FROM `{}.all_news`
            WHERE constituent_id = '{}' and news_origin = "Orbis"
            """.format(common_parameters["BQ_DATASET"],constituent_id)

        try:
            result = storage.get_bigquery_data(query=query, iterator_flag=False)
            max_id = result[0]["max_id"]
        except Exception as e:
            max_id = None
            continue

        start = 0
        max_count = 20
        print("Constituent: {},{}".format(constituent_name, bvdid))

        stop = False

        try:
            token = soap.get_token(parameters["BVD_USERNAME"], parameters["BVD_PASSWORD"], 'orbis')
            selection_token, selection_count = soap.find_by_bvd_id(token, bvdid, 'orbis')
        except Exception as e:
            print(e)
            return

        while not stop:
            try:
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
                    start, max_count)

                get_data_result = soap.get_data(token, selection_token, selection_count, query, 'orbis',
                                                timeout=None)
            except Exception as e:
                print(str(e))
                if token:
                    soap.close_connection(token, 'orbis')
                    token = None
                break

            result = ET.fromstring(get_data_result)
            csv_result = result[0][0][0].text

            TESTDATA = StringIO(csv_result)
            try:
                df = pd.read_csv(TESTDATA, sep=",", parse_dates=["news_date"])
            except Exception as e:
                print(e)
                break

            if df.shape[0] == 0:
                print("No records in df")
                break

            initial_shape = df.shape[0]

            df.astype({"news_id": int}, copy=False, errors='ignore')
            df = df.loc[df["news_id"] > max_id]

            if df.shape[0] == 0:
                print("No new data")
                break

            if df.shape[0] != initial_shape:
                stop = True

            # Make news_title column a string
            df.astype({"news_title": str}, copy=False, errors='ignore')

            # Remove duplicate columns
            df.drop_duplicates(["news_title"], inplace=True)

            # Get sentiment score
            df["score"] = df.apply(lambda row: tah.get_nltk_sentiment(str(row["news_article_txt"])), axis=1)

            # get sentiment word
            df["sentiment"] = df.apply(lambda row: tah.get_sentiment_word(row["score"]), axis=1)

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
                tags = get_spacey_tags(tagger.get_spacy_entities(str(text)))
                entity_tags.append(tags)

            fields = ["news_date", "news_title", "news_article_txt", "news_companies", "news_source",
                      "news_publication", "news_topics", "news_country", "news_region",
                      "news_language", "news_id", "score", "sentiment", "constituent_id",
                      "constituent_name", "constituent", "url", "show"]

            # Get needed fields
            df_bigquery = df[fields]
            bigquery_data = json.loads(df_bigquery.to_json(orient="records", date_format="iso"))

            # set entity_tag field
            i = 0
            for i in range(0, len(bigquery_data)):
                bigquery_data[i]["entity_tags"] = entity_tags[i]

                # set news_date
                if "news_date" in bigquery_data[i] and bigquery_data[i]["news_date"]:
                    date = bigquery_data[i]["news_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    bigquery_data[i]["news_date"] = ts
                else:
                    bigquery_data[i]["news_date"] = None

                if bigquery_data[i]["news_title"]:
                    bigquery_data[i]["news_title"] = str(bigquery_data[i]["news_title"])

                if bigquery_data[i]["news_article_txt"]:
                    bigquery_data[i]["news_article_txt"] = str(bigquery_data[i]["news_article_txt"])

                if bigquery_data[i]["news_companies"]:
                    try:
                        bigquery_data[i]["news_companies"] = [i.strip() for i in
                                                              bigquery_data[i]["news_companies"].split(";")]
                    except Exception as e:
                        bigquery_data[i]["news_companies"] = []
                else:
                    bigquery_data[i]["news_companies"] = []

                if bigquery_data[i]["news_topics"]:
                    try:
                        if bigquery_data[i]["news_topics"].isdigit():
                            bigquery_data[i]["news_topics"] = []
                        elif bigquery_data[i]["news_topics"] == 'None':
                            bigquery_data[i]["news_topics"] = []
                        else:
                            bigquery_data[i]["news_topics"] = [i.strip() for i in
                                                               bigquery_data[i]["news_topics"].split(";")]
                    except Exception as e:
                        bigquery_data[i]["news_topics"] = []
                else:
                    bigquery_data[i]["news_topics"] = []

                if bigquery_data[i]["news_country"]:
                    try:
                        bigquery_data[i]["news_country"] = [i.strip() for i in
                                                            bigquery_data[i]["news_country"].split(";")]
                    except Exception as e:
                        bigquery_data[i]["news_country"] = []
                else:
                    bigquery_data[i]["news_country"] = []

                if bigquery_data[i]["news_region"]:
                    try:
                        bigquery_data[i]["news_region"] = [i.strip() for i in
                                                           bigquery_data[i]["news_region"].split(";")]
                    except Exception as e:
                        bigquery_data[i]["news_region"] = []
                else:
                    bigquery_data[i]["news_region"] = []

                if bigquery_data[i]["news_language"]:
                    bigquery_data[i]["news_language"] = str(bigquery_data[i]["news_language"])

                if bigquery_data[i]["news_source"]:
                    bigquery_data[i]["news_source"] = str(bigquery_data[i]["news_source"])

                if bigquery_data[i]["news_publication"]:
                    bigquery_data[i]["news_publication"] = str(bigquery_data[i]["news_publication"])

                if bigquery_data[i]["news_id"]:
                    try:
                        bigquery_data[i]["news_id"] = int(bigquery_data[i]["news_id"])
                    except Exception as e:
                        bigquery_data[i]["news_id"] = str(bigquery_data[i]["news_id"])

                if bigquery_data[i]["sentiment"]:
                    bigquery_data[i]["sentiment"] = str(bigquery_data[i]["sentiment"])

                if bigquery_data[i]["constituent_id"]:
                    bigquery_data[i]["constituent_id"] = str(bigquery_data[i]["constituent_id"])

                if bigquery_data[i]["constituent_name"]:
                    bigquery_data[i]["constituent_name"] = str(bigquery_data[i]["constituent_name"])

                if bigquery_data[i]["constituent"]:
                    bigquery_data[i]["constituent"] = str(bigquery_data[i]["constituent"])

            try:
                storage.insert_bigquery_data(common_parameters["BQ_DATASET"], "all_news", bigquery_data)
            except Exception as e:
                print(e)

            start = start + 20

            print("Records saved: {}".format(df.shape[0]))

            log = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                    "constituent_name": constituent_name,
                    "constituent_id": constituent_id,
                    "downloaded_news": df.shape[0],
                    "source": "Orbis"}]

            if parameters["LOGGING"] and bigquery_data:
                logging_utils.logging(log, common_parameters["BQ_DATASET"], "news_logs", storage)

        if token:
            soap.close_connection(token, 'orbis')

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
    if __name__ != "__main__":
        sys.path.insert(0, args.python_path)
        from utils.Storage import Storage
        from utils.Storage import MongoEncoder
        from utils.SOAPUtils import SOAPUtils
        from utils import twitter_analytics_helpers as tah
        from utils.TaggingUtils import TaggingUtils as TU
        from utils import email_tools as email_tools
    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    #get_historical_orbis_news(args.user,args.pwd, "orbis", args.google_key_path, args.param_connection_string)
    get_daily_orbis_news(args)

    q1 = """
                    SELECT a.constituent_name, a.downloaded_news, a.date, a.source
                    FROM
                    (SELECT constituent_name, SUM(downloaded_news) as downloaded_news, DATE(date) as date, source
                    FROM `{0}.news_logs`
                    WHERE source = 'Orbis'
                    GROUP BY constituent_name, date, source
                    ) a,
                    (SELECT constituent_name, MAX(DATE(date)) as date
                    FROM `{0}.news_logs`
                    WHERE source = 'Orbis'
                    GROUP BY constituent_name
                    ) b
                    WHERE a.constituent_name = b.constituent_name AND a.date = b.date
                    GROUP BY a.constituent_name, a.downloaded_news, a.date, a.source;
            """.format(common_parameters["BQ_DATASET"])

    q2 = """
                        SELECT constituent_name,count(*)
                        FROM `{}.all_news`
                        WHERE news_origin = "Orbis"
                        GROUP BY constituent_name
    """.format(common_parameters["BQ_DATASET"])

    email_tools.send_mail(args.param_connection_string, args.google_key_path, "Orbis",
              "PARAM_NEWS_COLLECTION", lambda x: x["SOURCE"] == "Orbis", q1, q2)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.Storage import MongoEncoder
    from utils.SOAPUtils import SOAPUtils
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)




