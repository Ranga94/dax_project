from sqlalchemy import *

class ParameterUtils:
    def __init__(self):
        pass

    def get_parameters(self,sql_connection_string=None, sql_table_name=None,
        mongo_connection_string=None, sql_column_list=None):
        if sql_connection_string:
            engine = create_engine(sql_connection_string)
            metadata = MetaData(engine)

            source_table = Table(sql_table_name, metadata, autoload=True)

            projection_columns = [c for c in source_table.columns if str(c) in sql_column_list]

            statement = select(projection_columns)
            result = statement.execute()
            row = result.fetchone()
            result.close()
            return row
        

        

if __name__ == "__main__":
    from sqlalchemy.sql import column
    p = ParameterUtils()
    x = ["PARAM_TWITTER_COLLECTION.LANGUAGE","PARAM_TWITTER_COLLECTION.DATABASE_NAME"]

    p.get_parameters(sql_connection_string="mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project",
        sql_table_name="PARAM_TWITTER_COLLECTION", 
        sql_column_list=x)
    

    