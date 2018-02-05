from google.cloud import bigquery

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
bigquery_client = bigquery.Client()

    # Make an authenticated API request
datasets = list(bigquery_client.list_datasets())
print(datasets)