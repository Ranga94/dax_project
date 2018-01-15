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
	bigquery.SchemaField('constituent_name','STRING','NULLABLE'),
	bigquery.SchemaField('constituent_id','STRING','NULLABLE'),
	bigquery.SchemaField('datetime','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('date','STRING','NULLABLE'),
	bigquery.SchemaField('time','STRING','NULLABLE'),
	bigquery.SchemaField('open','FLOAT','NULLABLE'),
	bigquery.SchemaField('high','FLOAT','NULLABLE'),
	bigquery.SchemaField('low','FLOAT','NULLABLE'),
	bigquery.SchemaField('close','FLOAT','NULLABLE')
	bigquery.SchemaField('volume','FLOAT','NULLABLE')
		
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	#bigquery.SchemaField('','',''),		 


if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")