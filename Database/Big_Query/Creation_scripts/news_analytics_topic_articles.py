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
	bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
	bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
	bigquery.SchemaField('sentiment','STRING','NULLABLE'),
	bigquery.SchemaField('News_Date_NewsDim','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('constituent','STRING','REQUIRED'),
	bigquery.SchemaField('News_source_NewsDim','STRING','NULLABLE'),
	bigquery.SchemaField('To_Date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('Score','FLOAT','NULLABLE'),
	bigquery.SchemaField('Categorised_tag','STRING','REQUIRED'),
	bigquery.SchemaField('News_Title_NewsDim','STRING','REQUIRED'),
	bigquery.SchemaField('Date','TIMESTAMP','NULLABLE'),
	bigquery.SchemaField('From_Date','TIMESTAMP','REQUIRED'),
	bigquery.SchemaField('NEWS_ARTICLE_TXT_NewsDim','STRING','REQUIRED'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))


if __name__ == '__main__':
	create_table(sys.argv[1],sys.argv[2],"igenie-project")