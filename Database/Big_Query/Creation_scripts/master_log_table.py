from google.cloud import bigquery
import sys

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
	bigquery.SchemaField('Date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('Constituent_name','STRING','NULLABLE'),
	bigquery.SchemaField('Downlaoded_tweets','INTEGER','NULLABLE'),
	bigquery.SchemaField('Downloaded_ticks','INTEGER','NULLABLE'),
	bigquery.SchemaField('Downloaded_news', 'INTEGER','NULLABLE'),
	bigquery.SchemaField('Stocktwits','INTEGER','NULLABLE'),
	bigquery.SchemaField('rss_feeds', 'INTEGER','NULLABLE'),
	bigquery.SchemaField('Analyst_opinion','INTEGER','NULLABLE'),
	)
	
	table = bigquery_client.create_table(table)
	
	print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	
if __name__ == '__main__':
#System argument 1 takes Dataset ID
#System argument 2 takes table ID
	create_table(sys.argv[1],sys.argv[2],"igenie-project")
	
