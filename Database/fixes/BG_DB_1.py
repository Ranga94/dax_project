#!/usr/bin/python3

from google.cloud import bigquery
import sys
import os
import random
#Args:
#1: google_key_path
#2: selection query
#3: delete query
#4: insert into query
#5: table name

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = sys.argv[1]
client = bigquery.Client(project='igenie-project')

#Check if contents have more than 1000 records. If not, delete them.
print("Running query {}".format(sys.argv[2]))
query_job = client.query(sys.argv[2])
iterator = query_job.result()
results = list(iterator)

if len(results) < 1000:
    print("Running query {}".format(sys.argv[3]))
    query_job = client.query(sys.argv[3])
    iterator = query_job.result()
else:
    print("Getting table {}".format(sys.argv[5]))
    dataset_ref = client.dataset('pecten_dataset_test')
    dataset = client.get_dataset(dataset_ref)
    table_ref = dataset.table(sys.argv[5])
    table = client.get_table(table_ref)
    schema = table.schema

    number = "_" + str(random.randint(1, 21) * 5)
    temp_table_name = sys.argv[5] + number
    print("Creating temporary table {}".format(temp_table_name))
    temp_table_ref = dataset.table(temp_table_name)
    temp_table = bigquery.Table(temp_table_ref, schema=schema)
    temp_table = client.create_table(temp_table)

    print("Running {}".format(sys.argv[4].format(temp_table_name)))
    query_job = client.query(sys.argv[4].format(temp_table_name))
    iterator = query_job.result()
    print(query_job.state)

    print("Deleting {}".format(sys.argv[5]))
    client.delete_table(table)
    table = None

    print("Creating empty table {}".format(sys.argv[5]))
    table_ref = dataset.table(sys.argv[5])
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

    print("Copying {} to {}".format(temp_table_name, sys.argv[5]))
    source_table_ref = temp_table_ref
    job_config = bigquery.CopyJobConfig()
    job = client.copy_table(
        temp_table_ref, table_ref, job_config=job_config)  # API request
    job.result()  # Waits for job to complete.

    print("Deleting {}".format(temp_table_name))
    client.delete_table(temp_table)



