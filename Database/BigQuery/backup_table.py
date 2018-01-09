#!/usr/bin/python3

from google.cloud import bigquery
import os
import random

def backup_table(google_key_path, dataset_name, table_name):
    print("Backing up table {}".format('country_data'))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_name)
    dataset = client.get_dataset(dataset_ref)
    table_ref = dataset.table(table_name)
    table = client.get_table(table_ref)
    schema = table.schema

    number = "_" + str(random.randint(1, 21) * 5)
    temp_table_name = table_name + number
    print("Creating backup table {}".format(temp_table_name))
    temp_table_ref = dataset.table(temp_table_name)
    temp_table = bigquery.Table(temp_table_ref, schema=schema)
    temp_table = client.create_table(temp_table)
    print("Backup table {} created".format(temp_table_name))
    return temp_table_name

def drop_backup_table(google_key_path, dataset_name, table_name):
    print("Dropping backup table {}".format(table_name))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_name)
    dataset = client.get_dataset(dataset_ref)
    table_ref = dataset.table(table_name)
    table = client.get_table(table_ref)
    client.delete_table(table)
