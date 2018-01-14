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
	bigquery.SchemaField('Constituent','STRING','REQUIRED'),
	bigquery.SchemaField('Constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('Constituent_name','STRING','REQUIRED'),
	bigquery.SchemaField('Fundamental_growth','STRING','REQUIRED'),
	bigquery.SchemaField('Price_growth','STRING','REQUIRED'),
	bigquery.SchemaField('Profitabilty_rank','INTEGER','REQUIRED'),
	bigquery.SchemaField('Status','STRING','NULLABLE'),
	bigquery.SchemaField('From_date','STRING','REQUIRED'),
	bigquery.SchemaField('To_date','STRING','REQUIRED'),
    )

	table = bigquery_client.create_table(table)

	print('Created table {} in dataset {}.'.format(table_id, dataset_id))

def load_data_from_gcs(dataset_id, table_id, source):
	bigquery_client = bigquery.Client()
	dataset_ref = bigquery_client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.source_format = 'NEWLINE_DELIMITED_JSON'
	job = bigquery_client.load_table_from_uri(source, table_ref,job_config=job_config)
	#job_config.load.maxBadRecords=some number
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
	create_table("pecten_dataset_dev","Profitability_tag_ranking_copy","igenie-project")
	load_data_from_gcs("pecten_dataset_dev","Profitability_tag_ranking_copy","gs://pecten_dataset_dev/Profitability_tag_ranking.json")
	drop_table("pecten_dataset_dev","Profitability_tag_ranking")
	copy_table("pecten_dataset_dev","Profitability_tag_ranking_copy","Profitability_tag_ranking","igenie-project")
	drop_table("pecten_dataset_dev","Profitability_tag_ranking_copy")