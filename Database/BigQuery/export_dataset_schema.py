#!/usr/bin/python3
from google.cloud import bigquery
import sys
import os

#Args:
#1: google_key_path

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = sys.argv[1]
client = bigquery.Client(project='igenie-project')
dataset_ref = client.dataset('pecten_dataset')
dataset = client.get_dataset(dataset_ref)
tables = list(client.list_tables(dataset))
print(tables)