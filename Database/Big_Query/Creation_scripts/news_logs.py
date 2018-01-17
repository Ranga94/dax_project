from google.cloud import bigquery
import datetime as DT


def news_log_read():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	print("News collection report for the date range between {} and {}:".format(week_ago, today))
	client = bigquery.Client()
	query_job = client.query("SELECT constituent_name, count(*) as number FROM pecten_dataset_test.news_logs where date between TIMESTAMP'{}' and TIMESTAMP '{}' GROUP BY constituent_name ORDER BY number".format(week_ago, today))
		
	results = query_job.result()
	for row in results:
		print("{} news items were inserted for {}".format(row.unique_words, row.constituent_name))
		
if __name__ == '__main__':
	news_log_read()
