from google.cloud import bigquery
import datetime


def news_log_read():
	#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "E:\igenie-project-key.json"
	now = datetime.datetime.now()
	date = now.strftime("%Y-%m-%d")
	client = bigquery.Client()
	query_job = client.query("SELECT constituent_name, count(*) as unique_words FROM pecten_dataset_test.news_logs where date = '{}' GROUP BY constituent_name".format(date))
		
	results = query_job.result()
	for row in results:
		print("{} news items were inserted for {}".format(row.unique_words, row.constituent_name))
		
if __name__ == '__main__':
	news_log_read()
	
	