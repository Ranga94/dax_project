import requests as req
from google.cloud import bigquery

client = bigquery.Client(project='igenie-project')
dataset_ref = client.dataset("pecten_dataset_dev")
dataset = bigquery.Dataset(dataset_ref)

tables = list(client.list_tables(dataset))