#Feature PECTEN-9

from google.cloud import bigquery
import os
import numpy as np

def get_schema(google_key_path, dataset_name, table_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
    client = bigquery.Client(project='igenie-project')
    dataset_ref = client.dataset(dataset_name)
    dataset = client.get_dataset(dataset_ref)
    table_ref = dataset.table(table_name)
    table = client.get_table(table_ref)
    schema = table.schema

    list_of_fields = {}

    for field in schema:
        list_of_fields[field.name] = field

    return list_of_fields

def validate_data(google_key_path, data, dataset_name, table_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
    client = bigquery.Client(project='igenie-project')

    list_of_fields = get_schema(google_key_path,dataset_name,table_name)

    for row in data:
        for column in row.keys():
            assert column in list_of_fields
            assert row[column] is not None
            assert field_mappings(list_of_fields[column].field_type, type(row[column]))

def validate_data_pd(google_key_path, df, dataset_name, table_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
    client = bigquery.Client(project='igenie-project')

    list_of_fields = get_schema(google_key_path, dataset_name, table_name)

    for c in df.columns:
        assert field_mapping_pd(df[c].dtype, list_of_fields[c].field_type)



def field_mappings(type_string, type):
    if type_string == 'INTEGER':
        if type == int:
            return True
        else:
            return False
    elif type_string == 'STRING':
        if type == str:
            return True
        else:
            return False
    elif type_string == 'FLOAT':
        if type == float:
            return True
        else:
            return False
    elif type_string == 'BOOLEAN':
        if type == bool:
            return True
        else:
            return False
    elif type_string == 'TIMESTAMP':
        if type == str:
            return True
        else:
            return False
    elif type_string == 'RECORD':
        if type == dict:
            return True
        else:
            return False

def field_mapping_pd(type_pd, type_bq):
    if type_pd == np.int64:
        if type_bq == 'INTEGER':
            return True
        else:
            return False
    elif type_pd == np.float64:
        if type_bq == 'FLOAT':
            return True
        else:
            return False
    elif type_pd == object:
        if type_bq == 'STRING':
            return True
        else:
            return False

def before_insert(google_key_path, dataset_name, table_name, from_date, to_date, storage_client):
    list_of_fields = get_schema(google_key_path,dataset_name,table_name)

    date_field_type = "TIMESTAMP"

    for key in list(list_of_fields.keys()):
        list_of_fields[key.lower()] = list_of_fields[key]

    date_field_type = list_of_fields["from_date"].field_type

    if date_field_type == 'TIMESTAMP':
        data_validation_query = """
            SELECT count(*) as record_num FROM `{}.{}` WHERE from_date = TIMESTAMP('{}') AND to_date
            = TIMESTAMP('{}')
            """.format(dataset_name, table_name, from_date, to_date)
    else:
        data_validation_query = """
            SELECT count(*) as record_num FROM `{}.{}` WHERE from_date = '{}' AND to_date
            = '{}'
            """.format(dataset_name, table_name, from_date, to_date)

    record_num = storage_client.get_bigquery_data(data_validation_query, iterator_flag=False)[0]['record_num']
    assert record_num == 0

def after_insert(google_key_path, dataset_name, table_name, from_date, to_date, storage_client):
    list_of_fields = get_schema(google_key_path, dataset_name, table_name)

    date_field_type = "TIMESTAMP"

    for key in list(list_of_fields.keys()):
        list_of_fields[key.lower()] = list_of_fields[key]

    date_field_type = list_of_fields["from_date"].field_type

    if date_field_type == 'TIMESTAMP':
        data_validation_query = """
            SELECT count(*) as record_num FROM `{}.{}` WHERE from_date = TIMESTAMP('{}') AND to_date
            = TIMESTAMP('{}')
            """.format(dataset_name, table_name, from_date, to_date)
    else:
        data_validation_query = """
            SELECT count(*) as record_num FROM `{}.{}` WHERE from_date = '{}' AND to_date
            = '{}'
            """.format(dataset_name, table_name, from_date, to_date)

    record_num = storage_client.get_bigquery_data(data_validation_query, iterator_flag=False)[0]['record_num']
    assert record_num > 0