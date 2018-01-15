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
	bigquery.SchemaField('Constituent', 'STRING','REQUIRED'),
	bigquery.SchemaField('Constituent_id', 'STRING','REQUIRED'),
	bigquery.SchemaField('Constituent_name', 'STRING','REQUIRED'),
	bigquery.SchemaField('Cumulative_return_consistency_score','INTEGER','NULLABLE'),
	bigquery.SchemaField('Date_of_analysis','STRING','NULLABLE'),
	bigquery.SchemaField('From_date','STRING','REQUIRED'),
	bigquery.SchemaField('Status','STRING','NULLABLE'),
	bigquery.SchemaField('Table','STRING','NULLABLE'),
	bigquery.SchemaField('To_date','STRING','REQUIRED'),
	bigquery.SchemaField('one_year_return','FLOAT','REQUIRED'),
	bigquery.SchemaField('six_months_return','FLOAT','REQUIRED'),
	bigquery.SchemaField('three_years_return','FLOAT','REQUIRED'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))


	
	

if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")