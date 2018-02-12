from google.cloud import bigquery

client = bigquery.Client()
DATASET_ID = "pecten_dataset_test"
dataset = bigquery.Dataset(client.dataset(DATASET_ID))
tables = list(client.list_dataset_tables(dataset))
print(tables)

for table in tables:
	table_ref = dataset.table(table.table_id)
	#print(table.table_id)
	print(table_ref)

