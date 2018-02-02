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
	print('Table {} copied to {}.'.format(table_id, new_table_id))
	
if __name__ == '__main__':
	copy_table("pecten_dataset_test","pecten_dataset_new","dividend","divedend_94","igenie-project")