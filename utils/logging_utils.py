def logging(doc, dataset_name, table_name, storage_client):
    try:
        storage_client.insert_bigquery_data(dataset_name, table_name, doc)
    except Exception as e:
        print(e)