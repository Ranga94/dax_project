from google.cloud import bigquery

client = bigquery.Client()
job_config = bigquery.ExtractJobConfig()
def export_data():
	DATASET_ID = ["pecten_dataset_test","pecten_dataset_dev","pecten_dataset"]

	for dataset_id in DATASET_ID:
		dataset = bigquery.Dataset(client.dataset(dataset_id))
		tables = list(client.list_dataset_tables(dataset))
		#print(tables)

		for table in tables:
			table_ref = dataset.table(table.table_id)
			print(table_ref)
			print(table.table_id)
			try:
				destination = "gs://"+dataset_id+"/"+table.table_id+".json"
				print(destination)
				job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
				job = client.extract_table(table_ref, destination,job_config= job_config)
				job.result()
			except Exception as e:
				print(e)
			#print('Exported {} to {}'.format(table.table_id, destination)
			
##For tables containing data of more than 1 GB			
def export_big_table():
	DATASET_ID = ["pecten_dataset_test","pecten_dataset_dev","pecten_dataset"]
	for dataset in DATASET_ID:
		dataset = (client.dataset(dataset))
		big_table = ["all_news","tweets","tweets_unmodified"]
		for table in big_table:
			table_ref = dataset.table(table)
			print(table_ref)
			destination = "gs://"+str(dataset)+"/"+table+"/"+table+"-*.json"
			job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
			job = client.extract_table(table_ref, destination,job_config= job_config)
			job.result()
			
if __name__ == '__main__':
	#export_data()
	export_big_table()
