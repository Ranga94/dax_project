from pymongo import MongoClient, errors
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from sqlalchemy import *
import json
from google.cloud import datastore
from google.cloud import bigquery
from datetime import datetime
import time

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

    def save_to_mongodb(self, data, database=None, collection=None, connection_string=None):
        if database and collection:
            if self.mongo_client:
                client = self.mongo_client
            else:
                self.mongo_client = MongoClient(connection_string)
                client = self.mongo_client

            db = client[database]
            collection = db[collection]

            result = None

        if isinstance(data,list):
            try:
                result = collection.insert_many(data, ordered=False)
            except errors.BulkWriteError as e:
                pass
                #print(str(e.details['writeErrors']))
            except Exception as e:
                pass
                #print(str(e))
            finally:
                client.close()
                return result
        else:
            try:
                result = collection.insert_one(data)
            except Exception as e:
                print(str(e))
            finally:
                client.close()
                return result

    def upload_to_cloud_storage(self, google_key_path, bucket_name, source, destination):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path

        try:
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(destination)
            blob.upload_from_filename(source)

            return True
        except GoogleCloudError as e:
            print(str(e))
            return None
        except NotFound as e:
            print(str(e))
            return None

    def save_to_local_file(self, data, destination, mode="w"):
        if isinstance(data, str):
            with open(destination, mode) as f:
                f.write(data)
        elif isinstance(data, dict):
            with open(destination, mode) as f:
                f.write(json.dumps(data, cls=MongoEncoder) + '\n')
        elif isinstance(data,list):
            if data and isinstance(data[0], dict):
                with open(destination, mode) as f:
                    for item in data:
                        #f.write(jsonpickle.encode(item, unpicklable=False) + '\n')
                        f.write(json.dumps(item, cls=MongoEncoder) + '\n')

    def mongo_aggregation_query(self, connection_string, database_name, collection_name, pipeline):
        client = MongoClient(connection_string)
        db = client[database_name]
        collection = db[collection_name]

        return list(collection.aggregate(pipeline))

    def get_sql_data(self, sql_connection_string=None, sql_table_name=None,
                     sql_column_list=None, sql_where=None):
        engine = create_engine(sql_connection_string)
        metadata = MetaData(engine)

        source_table = Table(sql_table_name, metadata, autoload=True)
        projection_columns = [source_table.columns[name] for name in sql_column_list]

        if sql_where:
            statement = select(projection_columns).where(sql_where(source_table.columns))
        else:
            statement = select(projection_columns)
        result = statement.execute()
        rows = result.fetchall()
        result.close()
        return rows

    def get_sql_data_text_query(self, sql_connection_string, query):
        s = text(query)
        engine = create_engine(sql_connection_string)
        conn = engine.connect()
        rows = conn.execute(s).fetchall()

        return rows


    def insert_to_sql(self, sql_connection_string=None, sql_table_name=None, data=None):
        engine = create_engine(sql_connection_string)
        metadata = MetaData(engine)

        source_table = Table(sql_table_name, metadata, autoload=True)
        statement = source_table.insert().values(data)
        result = statement.execute()

    def insert_to_datastore(self, project_id,google_key_path, data, kind, key_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
        client = datastore.Client(project_id)

        if isinstance(data, list):
            for item in data:
                with client.batch() as batch:
                    # batch isnert
                    # The name/ID for the new entity
                    name = item[key_name]
                    item.pop(key_name)
                    # The Cloud Datastore key for the new entity
                    key = client.key(kind, name)

                    # Prepares the new entity
                    entity = datastore.Entity(key=key)
                    entity.update(item)
                    batch.put(entity)

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

    def get_bigquery_data_legacy(self, query, timeout=None, iterator_flag=True):
        if self.bigquery_client:
            client = self.bigquery_client
        else:
            client = bigquery.Client()

        config = bigquery.job.QueryJobConfig()
        config.use_legacy_sql = True

        print("Running query...")
        query_job = client.query(query, job_config=config)
        iterator = query_job.result(timeout=timeout)

        if iterator_flag:
            return iterator
        else:
            return list(iterator)

    def insert_bigquery_data(self, dataset_name, table_name, data):
        if self.bigquery_client:
            client = self.bigquery_client
        else:
            self.bigquery_client = bigquery.Client()
            client = self.bigquery_client

        try:
            dataset_ref = client.dataset(dataset_name)
            dataset = bigquery.Dataset(dataset_ref)
            table_ref = dataset.table(table_name)
            table = client.get_table(table_ref)

            errors = client.create_rows(table, data)  # API request
            if not errors:
                return True
            else:
                print(errors[0])
                return None
        except Exception as e:
            print(e)
            return None

class MongoEncoder(json.JSONEncoder):
    def default(self, v):
        types = {
            'ObjectId': lambda v: str(v),
            'datetime': lambda v: MongoEncoder.convert_timestamp(v)
        }
        vtype = type(v).__name__
        if vtype in types:
            return types[type(v).__name__](v)
        else:
            return json.JSONEncoder.default(self, v)

    @classmethod
    def convert_timestamp(cls, date_object):
        ts = time.strftime('%Y-%m-%d %H:%M:%S', date_object.timetuple())
        return ts

if __name__ == "__main__":
    storage = Storage(google_key_path="C:/Users/Uly/Desktop/Desktop/DAX/dax_project/keys/igenie-project-key.json")
    query = """
    SELECT
  text, constituent, sentiment_score, constituent_name, constituent_id, date as tweet_date,
  GROUP_CONCAT(SPLIT(REGEXP_REPLACE(text, r'[^\d]+', '.'))) AS price,
  CASE
        WHEN date > '2017-11-01 00:00:00' THEN '2017-12-09 00:00:00 UTC'
    END AS date,
CASE
        WHEN date > '2017-11-01 00:00:00' THEN '2017-12-01 00:00:00 UTC'
    END AS from_date,
CASE
        WHEN date > '2017-11-01 00:00:00' THEN '2017-12-11 00:00:00 UTC'
    END AS to_date
FROM [igenie-project:pecten_dataset.twitter_analytics_latest_price_tweets]
WHERE date between TIMESTAMP ('2017-11-01 00:00:00') and TIMESTAMP ('2017-12-01 00:00:00');
    """
    from pprint import pprint
    try:
        result = storage.get_bigquery_data_legacy(query,iterator_flag=False)
        pprint(result)
    except Exception as e:
        print(e)















