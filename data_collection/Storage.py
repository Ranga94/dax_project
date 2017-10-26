from Pymongo import MongoClient, errors
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
import jsonpickle

class Storage:
    def __init__(self):
        pass

    def save_to_mongodb(self, connection_string=None, database=None,collection=None, data=None):
        if connection_string and database and collection:
            client = MongoClient(connection_string)
            db = client[database]
            collection = [collection]

            result = None

        if isinstance(data,list):
            try:
                result = collection.insert_many(data, ordered=False)
            except errors.BulkWriteError as e:
                print(str(e.details['writeErrors']))
            except Exception as e:
                print(str(e))
            finally:
                return result
        else:
            try:
                result = collection.insert_one(data)
            except Exception as e:
                print(str(e))
            finally:
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
        elif isinstance(data,list):
            if data and isinstance(data[0], dict):
                with open(destination, mode) as f:
                    for item in data:
                        f.write(jsonpickle.encode(item, unpicklable=False) + '\n')












