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
	bigquery.SchemaField('Date', 'TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('Sell', 'INTEGER','NULLABLE'),
	bigquery.SchemaField('Buy', 'INTEGER','NULLABLE'),
	bigquery.SchemaField('Hold_percentage','FLOAT','NULLABLE'),
	bigquery.SchemaField('Analyst_recommendation','STRING','NULLABLE'),
	bigquery.SchemaField('Table','STRING','NULLABLE'),
	bigquery.SchemaField('Analyst_rating','FLOAT','NULLABLE'),
	bigquery.SchemaField('Median_target_price','FLOAT','NULLABLE'),
	bigquery.SchemaField('Constituent','STRING','REQUIRED'),
	bigquery.SchemaField('Hold','INTEGER','NULLABLE'),
	bigquery.SchemaField('Highest_target_price','FLOAT','NULLABLE'),
	bigquery.SchemaField('Lowest_target_price','FLOAT','NULLABLE'),
	bigquery.SchemaField('Status','STRING','NULLABLE'),
	bigquery.SchemaField('Buy_percentage','FLOAT','REQUIRED'),
	bigquery.SchemaField('Sell_percentage','FLOAT','REQUIRED'),
	bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
	bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	

		
		
if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")