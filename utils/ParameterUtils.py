from sqlalchemy import *

class ParameterUtils:
    def __init__(self):
        pass

    def get_parameters(self, sql_connection_string=None, sql_table_name=None,
        mongo_connection_string=None, sql_column_list=None):
        if sql_connection_string:
            engine = create_engine(sql_connection_string)
            metadata = MetaData(engine)

            source_table = Table(sql_table_name, metadata, autoload=True)

            full_column_names = [sql_table_name + p for p in sql_column_list]

            projection_columns = [c for c in source_table.columns if str(c) in full_column_names]

            statement = select(projection_columns)
            result = statement.execute()
            row = result.fetchone()
            result.close()

            parameters = {}

            for i in range(0,len(sql_column_list)):
                parameters[sql_column_list[i]] = row[i]

            return parameters

if __name__ == "__main__":
    from sqlalchemy.sql import column
    p = ParameterUtils()
    x = ["PARAM_TWITTER_COLLECTION.LANGUAGE","PARAM_TWITTER_COLLECTION.DATABASE_NAME"]

    r = p.get_parameters(sql_connection_string="mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project",
        sql_table_name="PARAM_TWITTER_COLLECTION", 
        sql_column_list=x)
    print(r)
    

    