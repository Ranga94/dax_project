from pymongo import MongoClient, errors
import pprint

class DB:
    def __init__(self, connection_string, database):
        self._client = MongoClient(connection_string)
        self._db = self._client[database]


    def insert_one(self, collection_name, document):
        collection = self._db[collection_name]

        try:
            result = collection.insert_one(document)
            return "Inserted document {}".format(result.inserted_id)
        except errors.DuplicateKeyError as e:
            return str(e.details)
        except Exception as e:
            return str(e)

    def bulk_insert(self, collection_name, documents:list):
        collection = self._db[collection_name]
        bulk = collection.initialize_unordered_bulk_op()

        for doc in documents:
            bulk.insert(doc)

        try:
            result = bulk.execute()
            return "Inserted {} records".format(result['nInserted'])
        except errors.BulkWriteError as e:
            return str(e.details)
        except Exception as e:
            return str(e)

    def insert_many(self, collection_name, documents:list):
        collection = self._db[collection_name]
        result = None
        try:
            result = collection.insert_many(documents, ordered=False)
            return result

        except errors.BulkWriteError as e:
            print(str(e.details['writeErrors']))
        except Exception as e:
            print(str(e))
        finally:
            return result


    def find(self, collection_name, **kwargs):
        collection = self._db[collection_name]
        return collection.find(**kwargs)

    def get_collection(self, collection_name):
        return self._db[collection_name]

