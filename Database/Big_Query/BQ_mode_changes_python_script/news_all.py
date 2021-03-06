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
	bigquery.SchemaField('NEWS_DATE_NewsDim','STRING','REQUIRED'),
	bigquery.SchemaField('score','FLOAT','NULLABLE'),
	bigquery.SchemaField('NEWS_PUBLICATION_NewsDim','STRING','NULLABLE'),
	bigquery.SchemaField('categorised_tag','STRING','REPEATED'),
	bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('NEWS_ARTICLE_TXT_NewsDim','STRING','REQUIRED'),
	bigquery.SchemaField('sentiment','STRING','REQUIRED'),
	bigquery.SchemaField('NEWS_TITLE_NewsDim','STRING','REQUIRED'),
	bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
	bigquery.SchemaField('count','INTEGER','NULLABLE'),
	bigquery.SchemaField('url','STRING','NULLABLE'),
	bigquery.SchemaField('news_language','STRING','NULLABLE'),
	bigquery.SchemaField('news_id','INTEGER','NULLABLE'),
	bigquery.SchemaField('news_country','STRING','NULLABLE'),
	bigquery.SchemaField('news_companies','STRING','NULLABLE'),
	bigquery.SchemaField('news_region','STRING','NULLABLE'),
	bigquery.SchemaField('constituent','STRING','REQUIRED'),
	bigquery.SchemaField('To_date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('From_date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('entity_tags','RECORD','NULLABLE',fields=[
	bigquery.SchemaField('FACILITY','STRING','REPEATED'),
	bigquery.SchemaField('QUANTITY','STRING','REPEATED'),
	bigquery.SchemaField('EVENT','STRING','REPEATED'),
	bigquery.SchemaField('PERSON','STRING','REPEATED'),
	bigquery.SchemaField('DATE','STRING','REPEATED'),
	bigquery.SchemaField('TIME','STRING','REPEATED'),
	bigquery.SchemaField('CARDINAL','STRING','REPEATED'),
	bigquery.SchemaField('PRODUCT','STRING','REPEATED'),
	bigquery.SchemaField('LOC','STRING','REPEATED'),
	bigquery.SchemaField('WORK_OF_ART','STRING','REPEATED'),
	bigquery.SchemaField('GPE','STRING','REPEATED'),
	bigquery.SchemaField('PERCENT','STRING','REPEATED'),
	bigquery.SchemaField('FAC','STRING','REPEATED'),
	bigquery.SchemaField('ORDINAL','STRING','REPEATED'),
	bigquery.SchemaField('ORG','STRING','REPEATED'),
	bigquery.SchemaField('NORP','STRING','REPEATED'),
	bigquery.SchemaField('LANGUAGE','STRING','REPEATED'),
	bigquery.SchemaField('MONEY','STRING','REPEATED'),
	bigquery.SchemaField('LAW','STRING','REPEATED')])
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
	create_table("pecten_dataset_dev","news_all_copy1","igenie-project")
	#load_data_from_gcs("pecten_dataset_dev","news_all_copy","gs://pecten_dataset_dev/news_all.json")
	#drop_table("pecten_dataset_dev","news_all")
	#copy_table("pecten_dataset_dev","news_all_copy","news_all","igenie-project")
	#drop_table("pecten_dataset_dev","news_all_copy")