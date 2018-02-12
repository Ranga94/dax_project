from google.cloud import bigquery

DATASET_ID = ["pecten_dataset_test"]
client = bigquery.Client()
#DATASET_ID = "pecten_dataset_test"
for dataset_id in DATASET_ID:
	dataset = bigquery.Dataset(client.dataset(dataset_id))
	tables = list(client.list_dataset_tables(dataset))
	#print(tables)

	for table in tables:
		table_ref = dataset.table(table.table_id)
		#print(table.table_id)
		destination = "gs://"+dataset_id+"/"+table.table_id+".json"
		job_config = bigquery.ExtractJobConfig()
		job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
		job = client.extract_table(table_ref, destination,job_config= job_config)
		job.result()
		#print('Exported {} to {}'.format(table.table_id, destination)