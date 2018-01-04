#!/usr/bin/python3
from google.cloud import bigquery
import sys
import os

def main(args):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.google_key_path
    client = bigquery.Client()
    dataset_ref = client.dataset('pecten_dataset')
    dataset = client.get_dataset(dataset_ref)
    tables = list(client.list_dataset_tables(dataset))
    table_ref = dataset.table(tables[0].table_id)
    print(table_exists(client, table_ref))

    source_dataset = bigquery.DatasetReference('bigquery-public-data', 'samples')
    source_table_ref = source_dataset.table('shakespeare')
    dest_dataset = bigquery.Dataset(client.dataset(DATASET_ID))
    dest_dataset = client.create_dataset(dest_dataset)
    dest_table_ref = dest_dataset.table('destination_table')

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
    parser.add_argument('google_key_path', help='The connection string')
    parser.add_argument('google_key_path', help='The connection string')


