from google.cloud import bigquery

def create_table(dataset_id="pecten_dataset_dev", table_id="all_correlations", project="igenie-project"):
    """Creates a simple table in the given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
        bigquery.SchemaField('Date','TIMESTAMP','REQUIRED'),
        bigquery.SchemaField('Open','FLOAT','REQUIRED'),
        bigquery.SchemaField('High','FLOAT','REQUIRED'),
	bigquery.SchemaField('Low','FLOAT','REQUIRED'),
	bigquery.SchemaField('Close','FLOAT','REQUIRED'),
	bigquery.SchemaField('Adj_Close','FLOAT','NULLABLE'),
	bigquery.SchemaField('Volume','FLOAT','NULLABLE'),
	bigquery.SchemaField('Twitter_sent','FLOAT','REQUIRED'),
	bigquery.SchemaField('Constituent','STRING','REQUIRED'),
	bigquery.SchemaField('News_sent','FLOAT','REQUIRED'),
	bigquery.SchemaField('Constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('From_date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('To_date','TIMESTAMP','REQUIRED'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))

if __name__ == '__main__':
    create_table()