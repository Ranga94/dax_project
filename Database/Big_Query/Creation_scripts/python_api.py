from google.cloud import bigquery

DATASET_ID = ["pecten_dataset_test","pecten_dataset","pecten_dataset_dev"]
client = bigquery.Client()
#DATASET_ID = "pecten_dataset_test"
for dataset_id in DATASET_ID:
	dataset = bigquery.Dataset(client.dataset(dataset_id))
	tables = list(client.list_dataset_tables(dataset))
	#print(tables)

	for table in tables:
		table_ref = dataset.table(table.table_id)
		#print(table.table_id)
		destination = "gs://"+dataset_id+"/"+table.table_id
		job = client.extract_table(table_ref, destination)
		job.result()
		#print('Exported {} to {}'.format(table.table_id, destination)