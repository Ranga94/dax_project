from google.cloud import bigquery

client = bigquery.Client()
DATASET_ID = "pecten_dataset_test"
dataset = bigquery.Dataset(client.dataset(DATASET_ID))
tables = list(client.list_dataset_tables(dataset))
print(tables.table_id)

