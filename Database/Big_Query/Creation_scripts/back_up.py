from google.cloud import bigquery
import requests as req

resp = req.get("https://www.googleapis.com/bigquery/v2/projects/igenie-project/pecten_dataset_dev")
print(resp.text)