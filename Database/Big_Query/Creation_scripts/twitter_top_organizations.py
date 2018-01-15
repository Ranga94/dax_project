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
	bigquery.SchemaField('count','INTEGER','NULLABLE'),
	bigquery.SchemaField('constituent','STRING','NULLABLE'),
	bigquery.SchemaField('trend','STRING','NULLABLE'),
	bigquery.SchemaField('date_of_analysis','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('status','STRING','NULLABLE'),
	bigquery.SchemaField('constituent_name','STRING','NULLABLE'),
	bigquery.SchemaField('constituent_id','STRING','NULLABLE'),
	bigquery.SchemaField('To_date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('From_date','TIMESTAMP','NULLABLE')
		
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	#bigquery.SchemaField('','',''),		 


if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")