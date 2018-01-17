from google.cloud import bigquery
import os 

def news_log_read():
	#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "E:\igenie-project-key.json"
	client = bigquery.Client()
	query_job = client.query("SELECT count(*) as unique_words FROM pecten_dataset_test.news_logs where date = '2018-01-17'")
		
	results = query_job.result()
	for row in results:
		print("{}".format(row.unique_words))
		
if __name__ == '__main__':
	news_log_read()
	
	