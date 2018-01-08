#!/usr/bin/python3

from google.cloud import bigquery
import os
import sys

def rollback_object(google_key_path, object_type, dataset_name, dataset_backup_name,
                    table_name, table_backup_name):

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
    client = bigquery.Client()

    if object_type == 'table':
        #check if table exists before deleting the other one
        dataset_ref = client.dataset(dataset_name)
        dataset = client.get_dataset(dataset_ref)

        backup_table_ref = dataset.table(table_backup_name)

        if table_exists(client, backup_table_ref):
            #Delete old table
            print("Backup table {} exists".format(table_backup_name))
            print("Dropping table {}".format(table_name))
            table_ref = dataset.table(table_name)
            table = client.get_table(table_ref)
            client.delete_table(table)

            #Copy backup table to table with the old name
            print("Copying {} to {}".format(table_backup_name, table_name))
            job_config = bigquery.CopyJobConfig()
            job = client.copy_table(
                backup_table_ref, table_ref, job_config=job_config)  # API request
            job.result()  # Waits for job to complete.

            #Delete backup table
            print("Dropping table {}".format(table_backup_name))
            backup_table = client.get_table(backup_table_ref)
            client.delete_table(backup_table)


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
    parser.add_argument('dataset_name')
    parser.add_argument('table_name')
    parser.add_argument('table_backup_name')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    rollback_object(args.google_key_path,'table',args.dataset_name,None,args.table_name,args.table_backup_name)