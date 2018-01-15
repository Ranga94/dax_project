from google.cloud import bigquery
import sys
import os

def export_schema(args):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.google_key_path
    client = bigquery.Client()
    source_dataset_ref = client.dataset(args.source_dataset)
    source_dataset = client.get_dataset(source_dataset_ref)
    return list(client.list_dataset_tables(source_dataset))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The connection string')
    parser.add_argument('source_dataset')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    export_schema(args)