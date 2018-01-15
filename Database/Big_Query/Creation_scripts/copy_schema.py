from google.cloud import bigquery
import sys
import os

def copy_missing_objects(args):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.google_key_path
    client = bigquery.Client()
    source_dataset_ref = client.dataset(args.source_dataset)
    source_dataset = client.get_dataset(source_dataset_ref)
    tables = export_schema(args)

    destination_dataset_ref = client.dataset(args.destination_dataset)
    destination_dataset = client.get_dataset(destination_dataset_ref)

    for table in tables:
        source_table_ref = source_dataset.table(table.table_id)
        destination_table_ref = destination_dataset.table(table.table_id)
    if not table_exists(client, destination_table_ref):
        print("Creating table {} in {}".format(table.table_id, args.destination_dataset))
        #Get schema of source table
        source_table = client.get_table(source_table_ref)
        schema = source_table.schema
        #Create empty table on the other dataset
        destination_table = bigquery.Table(destination_table_ref, schema=schema)
        destination_table = client.create_table(destination_table)

def table_exists(client, table_reference):
    """Return if a table exists.
    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        table_reference (google.cloud.bigquery.table.TableReference):
            A reference to the table to look for.
    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The connection string')
    parser.add_argument('source_dataset')
    parser.add_argument('destination_dataset')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from database.BigQuery.export_dataset_schema import export_schema
    copy_missing_objects(args)