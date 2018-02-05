import requests as req

resp = req.get("GET https://www.googleapis.com/bigquery/v2/projects/igenie-project/datasets/pecten_dataset_dev/tables")

print(resp.text)