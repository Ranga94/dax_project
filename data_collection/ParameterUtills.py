from sqlalchemy import *

class ParameterUtils:
    def __init__(self):
        pass

    def get_parameters(sql_connection_string=None, sql_table=None ,mongo_connection_string=None):
        if sql_connection_string:
            engine = create_engine(sql_connection_string)
            metadata = MetaData(engine)

            param_twitter_collection = Table('PARAM_TWITTER_COLLECTION', metadata, autoload=True)





        statement = select([param_twitter_collection.c.LANGUAGE,
                            param_twitter_collection.c.TWEETS_PER_QUERY,
                            param_twitter_collection.c.MAX_TWEETS,
                            param_twitter_collection.c.CONNECTION_STRING,
                            param_twitter_collection.c.DATABASE_NAME,
                            param_twitter_collection.c.COLLECTION_NAME,
                            param_twitter_collection.c.LOGGING_FLAG])
        result = statement.execute()
        row = result.fetchone()
        result.close()
        return row

if __name__ == "__main__":
    from sqlalchemy.sql import column
    p = ParameterUtils()
    engine = create_engine("mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project")
    metadata = MetaData(engine)

    param_twitter_collection = Table('PARAM_TWITTER_COLLECTION', metadata, autoload=True)
    statement = select([param_twitter_collection.c.LANGUAGE,
                        param_twitter_collection.c.TWEETS_PER_QUERY,
                        param_twitter_collection.c.MAX_TWEETS,
                        param_twitter_collection.c.CONNECTION_STRING,
                        param_twitter_collection.c.DATABASE_NAME,
                        param_twitter_collection.c.COLLECTION_NAME,
                        param_twitter_collection.c.LOGGING_FLAG])
    print(type(statement))
    result = statement.execute()
    row = result.fetchone()
    result.close()