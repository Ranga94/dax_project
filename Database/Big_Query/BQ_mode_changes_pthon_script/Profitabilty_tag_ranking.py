def create_table(dataset_id, table_id="Profitability_tag_ranking", project=None):
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