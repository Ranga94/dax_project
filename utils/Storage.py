from pymongo import MongoClient, errors
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
import jsonpickle
from sqlalchemy import *
import json
from google.cloud import datastore
from google.cloud import bigquery

class Storage:
    def __init__(self):
        pass

    def save_to_mongodb(self, connection_string=None, database=None,collection=None, data=None):
        if connection_string and database and collection:
            client = MongoClient(connection_string)
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

    def get_bigquery_data(self, google_key_path, query, timeout=None, iterator_flag=True):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
        client = bigquery.Client()

        query_job = client.query(query)
        print(query_job.state)

        if query_job.state == 'RUNNING':
            print("Running query...")
            iterator = query_job.result(timeout=timeout)

            if iterator_flag:
                return iterator
            else:
                return list(iterator)



class MongoEncoder(json.JSONEncoder):
    def default(self, v):
        types = {
            'ObjectId': lambda v: str(v),
            'datetime': lambda v: v.isoformat()
        }
        vtype = type(v).__name__
        if vtype in types:
            return types[type(v).__name__](v)
        else:
            return json.JSONEncoder.default(self, v)

if __name__ == "__main__":
    s = Storage()
    q = "SELECT id_str from `pecten_dataset.tweets_raw`"
    it = s.get_bigquery_data("C:\\Users\\Uly\\Desktop\\Desktop\\DAX\\dax_project\\keys\\igenie-project-key.json",
                             q)

    i = 0
    j = 0

    for item in it:
        i += 1
        j += 1
        if i == 2000:
            print(j)
            i = 0














