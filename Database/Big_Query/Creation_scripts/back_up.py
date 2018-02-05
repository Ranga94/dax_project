import requests as req
from google.cloud import bigquery


bigquery_client = bigquery.Client()

resp = req.get("https://www.googleapis.com/bigquery/v2/projects/igenie-project/datasets/pecten_dataset_dev/tables")

print(resp.text)