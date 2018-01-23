from google.cloud import bigquery
import datetime as DT
import pandas as pd

def log_table():
	today = DT.date.today()
	day_before = today - DT.timedelta(days=1)
	client = bigquery.Client()
	query_twitter = client.query("""SELECT constituent_name, sum(downloaded_tweets) as tweets
	FROM `igenie-project.pecten_dataset_test.tweet_logs`
	where date = timestamp('{}')
	GROUP BY constituent_name""".format(day_before))
	results = query_twitter.result()
	constituent_name = []
	tweets = []
	for row in results:
		#constituent_name = row.constituent_name
		#tweets = row.tweets
		print("{}:{}".format(row.constituent_name,row.tweets))
	print(constituent_name)
	print(tweets)
	collect = pd.DataFrame({'Constituent_name':[constituent_name],
							'tweets':[tweets]})
	print(collect)
	
if __name__ == '__main__':
	log_table()
	
	
		
	

