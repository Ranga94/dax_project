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
	bigquery.SchemaField('count','INTEGER','REQUIRED'),
	bigquery.SchemaField('status','STRING','NULLABLE'),
	bigquery.SchemaField('avg_sentiment','FLOAT','REQUIRED'),
	bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
	bigquery.SchemaField('country_name','STRING','REQUIRED'),
	bigquery.SchemaField('constituent','STRING','REQUIRED'),
	bigquery.SchemaField('date_of_analysis','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('from_date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('to_date','TIMESTAMP','REQUIRED'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))


	
if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")