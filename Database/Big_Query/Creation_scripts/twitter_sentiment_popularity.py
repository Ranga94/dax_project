from google.cloud import bigquery

def create_table(dataset_id, table_id, project):
    """Creates a simple table in the given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
	bigquery.SchemaField('count','INTEGER','REQUIRED'),
	bigquery.SchemaField('constituent','STRING','REQUIRED'),
	bigquery.SchemaField('avg_sentiment_all','FLOAT','NULLABLE'),
	bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
	bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('from_date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('to_date','TIMESTAMP','REQUIRED')
		
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	#bigquery.SchemaField('','',''),
	
def load_data_from_gcs(dataset_id, table_id, source):
	bigquery_client = bigquery.Client()
	dataset_ref = bigquery_client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.source_format = 'NEWLINE_DELIMITED_JSON'
	job_config.max_bad_records=100
	job = bigquery_client.load_table_from_uri(source, table_ref,job_config=job_config)
	
	job.result()  # Waits for job to complete
		
	print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))
	
def drop_table(dataset_name,table_name):
	print("Dropping backup table {}".format(table_name))
	client = bigquery.Client()
	dataset_ref = client.dataset(dataset_name)
	dataset = client.get_dataset(dataset_ref)
	table_ref = dataset.table(table_name)
	table = client.get_table(table_ref)
	client.delete_table(table)
		 
def copy_table(dataset_id, table_id, new_table_id, project):
	bigquery_client = bigquery.Client(project=project)
	dataset_ref = bigquery_client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	destination_table_ref = dataset_ref.table(new_table_id)
	job_config = bigquery.CopyJobConfig()
	job_config.create_disposition = (bigquery.job.CreateDisposition.CREATE_IF_NEEDED)
	copy_job = bigquery_client.copy_table(table_ref, destination_table_ref, job_config=job_config)
	print('Waiting for job to finish...')
	copy_job.result()
	print('Table {} copied to {}.'.format(table_id, new_table_id))
		

		 


if __name__ == '__main__':
	create_table("pecten_dataset_test","twitter_sentiment_popularity_copy","igenie-project")
	load_data_from_gcs("pecten_dataset_test","twitter_sentiment_popularity_copy","gs://pecten_dataset_test/twitter_sentiment_popularity.json")
	drop_table("pecten_dataset_test","twitter_sentiment_popularity")
	copy_table("pecten_dataset_test","twitter_sentiment_popularity_copy","twitter_sentiment_popularity","igenie-project")
	drop_table("pecten_dataset_test","twitter_sentiment_popularity_copy")