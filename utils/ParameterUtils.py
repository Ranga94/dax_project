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
            print(type(source_table.columns))

            full_column_names = [sql_table_name + "." + p for p in sql_column_list]

            projection_columns = [c for c in source_table.columns if str(c) in full_column_names]

            statement = select(projection_columns)
            result = statement.execute()
            row = result.fetchone()
            result.close()

            parameters = {}

            for i in range(0,len(sql_column_list)):
                parameters[sql_column_list[i]] = row[i]

            return parameters

    def get_param_data(self, sql_connection_string=None, sql_table_name=None,
        mongo_connection_string=None, sql_column_list=None, sql_where=None):

        if sql_connection_string:
            engine = create_engine(sql_connection_string)
            metadata = MetaData(engine)

            source_table = Table(sql_table_name, metadata, autoload=True)
            full_column_names = [sql_table_name + "." + p for p in sql_column_list]
            projection_columns = [c for c in source_table.columns if str(c) in full_column_names]

            if sql_where:
                statement = select(projection_columns).where(sql_where(source_table.columns))
            else:
                statement = select(projection_columns)
            result = statement.execute()
            rows = result.fetchall()
            result.close()
            return rows




if __name__ == "__main__":
    from sqlalchemy.sql import column
    p = ParameterUtils()
    x = ["CONSTITUENT_ID","NAME"]

    where = lambda x: x["ACTIVE_STATE"] == 1

    r = p.get_param_data(sql_connection_string="mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project",
        sql_table_name="CONSTITUENTS_MASTER",
        sql_column_list=x,
                         sql_where=where)
    print(r)
    

    