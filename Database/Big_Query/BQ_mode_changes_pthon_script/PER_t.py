def create_table(dataset_id, table_id="PER_t", project=None):
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
		bigquery.SchemaField('Current_PER','FLOAT','REQUIRED'),
		bigquery.SchemaField('Date','STRING','NULLABLE'),
		bigquery.SchemaField('PER_4_years_ago','FLOAT','NULLABLE'),
		bigquery.SchemaField('PER_last_year','FLOAT','REQUIRED'),
		bigquery.SchemaField('PER_score','INTEGER','NULLABLE'),
		bigquery.SchemaField('Status','STRING','REQUIRED'),
		bigquery.SchemaField('Table','STRING','NULLABLE'),
		bigquery.SchemaField('percentage_change_in_PER_from_4_years_ago','FLOAT','NULLABLE'),
		bigquery.SchemaField('percentage_change_in_PER_from_last_year','FLOAT','NULLABLE'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))