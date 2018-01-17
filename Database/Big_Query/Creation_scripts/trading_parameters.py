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
	bigquery.SchemaField('date_of_collection','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('Exchange_Symbol','FLOAT','NULLABLE'),
	bigquery.SchemaField('Alpha_30_days','FLOAT','NULLABLE'),
	bigquery.SchemaField('CCP_Eligible','FLOAT','NULLABLE'),
	bigquery.SchemaField('Instrument_Type','FLOAT','NULLABLE'),
	bigquery.SchemaField('Instrument_Subtype','FLOAT','NULLABLE'),
	bigquery.SchemaField('Instrument_Group','FLOAT','NULLABLE'),
	bigquery.SchemaField('Trading_Model','FLOAT','NULLABLE'),
	bigquery.SchemaField('Min_Tradable_Unit','FLOAT','NULLABLE'),
	bigquery.SchemaField('Max_Spread','FLOAT','NULLABLE'),
	bigquery.SchemaField('Min_Quote_Size','FLOAT','NULLABLE'),
	bigquery.SchemaField('Start_Pre_Trading','STRING','NULLABLE'),
	bigquery.SchemaField('End_Post_Trading','STRING','NULLABLE'),
	bigquery.SchemaField('Start_Intraday_Auction','STRING','NULLABLE'),
		
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
	#bigquery.SchemaField('','',''),		 


if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")