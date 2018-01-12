from google.cloud import bigquery

def create_table(dataset_id="pecten_dataset_dev" table_id="twitter_analytics_latest_price_tweets", project="igenie-project"):
    """Creates a simple table in the given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
        bigquery.SchemaField('tweet_date','TIMESTAMP','NULLABLE'),
        bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
        bigquery.SchemaField('from_date','TIMESTAMP','REQUIRED'),
		bigquery.SchemaField('date','TIMESTAMP','NULLABLE'),
		bigquery.SchemaField('text','STRING','REQUIRED'),
		bigquery.SchemaField('entity_tags','RECORD','NULLABLE'),
		bigquery.SchemaField('entity_tags.MONEY','STRING','REPEATED'),
		bigquery.SchemaField('sentiment_score','FLOAT','REQUIRED'),
		bigquery.SchemaField('constituent','STRING','REQUIRED'),
		bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
		bigquery.SchemaField('to_date','TIMESTAMP','REQUIRED'),
		bigquery.SchemaField('followers','INTEGER','NULLABLE'),
		bigquery.SchemaField('location','STRING','NULLABLE'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))


if __name__ == '__main__':
    create_table()