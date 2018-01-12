def create_table(dataset_id, table_id="target_prices", project=None):
    """Creates a simple table in the given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
        bigquery.SchemaField('entity_tags','STRING','NULLABLE'),
        bigquery.SchemaField('constituent','STRING','REQUIRED'),
        bigquery.SchemaField('sentiment_score','FLOAT','NULLABLE'),
		bigquery.SchemaField('from_date','TIMESTAMP','REQUIRED'),
		bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
		bigquery.SchemaField('constituent_id','STRING','NULLABLE'),
		bigquery.SchemaField('price','FLOAT','REQUIRED'),
		bigquery.SchemaField('to_date','TIMESTAMP','REQUIRED'),
		bigquery.SchemaField('tweet_date','TIMESTAMP','NULLABLE'),
		bigquery.SchemaField('date','TIMESTAMP','NULLABLE'),
		
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))