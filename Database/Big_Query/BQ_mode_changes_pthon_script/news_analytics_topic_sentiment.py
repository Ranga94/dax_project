from google.cloud import bigquery

def create_table(dataset_id="pecten_dataset_dev", table_id="news_analytics_topic_sentiment", project="igenie-project"):
    """Creates a simple table in the given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
	bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('overall_sentiment','STRING','REQUIRED'),
	bigquery.SchemaField('categorised_tag','STRING','REQUIRED'),
	bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
        bigquery.SchemaField('count','INTEGER','REQUIRED'),
	bigquery.SchemaField('date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('constituent','STRING','REQUIRED'),
	bigquery.SchemaField('From_date','TIMESTAMP','NULLABLE'),
        bigquery.SchemaField('To_date','TIMESTAMP','NULLABLE'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))


if __name__ == '__main__':
    create_table()