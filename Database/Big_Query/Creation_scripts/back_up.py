import requests as req
from google.cloud import bigquery

client = bigquery.Client(project='igenie-project')
tables = list(client.list_tables('pecten_dataset_dev'))