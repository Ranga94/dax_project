from google.cloud import bigquery

def copy_table(from_dataset_id, to_dataset_id, table_id, new_table_id, project):
	bigquery_client = bigquery.Client(project=project)
	from_dataset_ref = bigquery_client.dataset(from_dataset_id)
	to_dataset_ref = bigquery_client.dataset(to_dataset_id)
	table_ref = from_dataset_ref.table(table_id)
	destination_table_ref = to_dataset_ref.table(new_table_id)
	job_config = bigquery.CopyJobConfig()
	job_config.create_disposition = (bigquery.job.CreateDisposition.CREATE_IF_NEEDED)
	copy_job = bigquery_client.copy_table(table_ref, destination_table_ref, job_config=job_config)
	print('Waiting for job to finish...')
	copy_job.result()
	print('Table {} from {} is copied to {} on {}.'.format(table_id, from_dataset_id, new_table_id, to_dataset_id))
	
def drop_table(dataset_name,table_name):
	print("Dropping table {} on {}".format(table_name, dataset_name))
	client = bigquery.Client()
	dataset_ref = client.dataset(dataset_name)
	dataset = client.get_dataset(dataset_ref)
	table_ref = dataset.table(table_name)
	table = client.get_table(table_ref)
	client.delete_table(table)
	
if __name__ == '__main__':
	import sys
	#data_collection_tables = ["all_news","analyst_opinions","business_ratio","dividend","frankfurt_trading_parameters","historical",
	#							"historical_key_data","instrument_information","liquidity","ma_deals","master_data","news_logs","recent_report",
	#							"technical_figures","ticker_data","ticker_logs","trading_parameters","tweet_logs","tweets"]
	data_collection_tables = ["dividend","analyst_opinions"]
	for table in data_collection_tables:
		copy_table("pecten_dataset_test",sys.argv[1],table,table+"_94","igenie-project")
		drop_table(sys.argv[1],table)
		copy_table(sys.argv[1],sys.argv[1],table+"_94",table,"igenie-project")
		drop_table(sys.argv[1],table+"_94")
		print("--------------------------------------")
	