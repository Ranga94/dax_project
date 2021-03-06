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
	bigquery.SchemaField('tweet_date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('constituent_name','STRING','NULLABLE'),
	bigquery.SchemaField('from_date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('text','STRING','NULLABLE'),
	bigquery.SchemaField('CCP_Eligible','FLOAT','NULLABLE'),
	bigquery.SchemaField('sentiment_score','FLOAT','NULLABLE'),
	bigquery.SchemaField('constituent','STRING','NULLABLE'),
	bigquery.SchemaField('constituent_id','STRING','NULLABLE'),
	bigquery.SchemaField('to_date','FLOAT','NULLABLE'),
	bigquery.SchemaField('entity_tags','RECORD','NULLABLE',fields =[
	bigquery.SchemaField('MONEY','STRING','NULLABLE')])
		
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	#bigquery.SchemaField('','',''),		 


if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")