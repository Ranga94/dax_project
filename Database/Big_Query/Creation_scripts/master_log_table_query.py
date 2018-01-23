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
	tweet_results = query_twitter.result()
	constituent_name = []
	tweets = []
	for row in tweet_results:
		constituent_name.append(row.constituent_name)
		tweets.append(row.tweets)
	collect = pd.DataFrame({'Constituent_name':constituent_name,
							'tweets':tweets})
	
	query_bloomberg = client.query("""SELECT constituent_name, sum(downloaded_news) as bloomberg FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Bloomberg'
	GROUP BY constituent_name""".format(day_before))
	bloomberg_results = query_bloomberg.result()
	constituent_name1 = []
	bloomberg = []
	for row in bloomberg_results:
		constituent_name1.append(row.constituent_name)
		bloomberg.append(row.bloomberg)
	collect1 = pd.DataFrame({'Constituent_name':constituent_name1,
								'bloomberg':bloomberg})
	
	collect2 = pd.merge(collect,collect1,on ='Constituent_name')
	print(collect2)
	
if __name__ == '__main__':
	log_table()
	
	
		
	

