from google.cloud import bigquery
import datetime as DT
import pandas as pd

def log_table():
	today = DT.date.today()
	day_before = today - DT.timedelta(days=1)
	client = bigquery.Client()
	########################################Twitter logs Query################################
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
	tweets_df = pd.DataFrame({'Constituent_name':constituent_name,
							'tweets':tweets})
	######################################Bloomberg log query################################
	query_bloomberg = client.query("""SELECT constituent_name, sum(downloaded_news) as bloomberg FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Bloomberg'
	GROUP BY constituent_name""".format(day_before))
	bloomberg_results = query_bloomberg.result()
	constituent_name1 = []
	bloomberg = []
	for row in bloomberg_results:
		constituent_name1.append(row.constituent_name)
		bloomberg.append(row.bloomberg)
		date.append(row.date)
	bloomberg_df = pd.DataFrame({'date':date,'Constituent_name':constituent_name1,
								'bloomberg':bloomberg})
	print(date)
	collect = pd.merge(tweets_df,bloomberg_df,on ='Constituent_name',how='left')
	#print(collect2)
	##########################################Orbis Log Query################################
	query_orbis = client.query("""SELECT constituent_name, sum(downloaded_news) as orbis FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Orbis'
	GROUP BY constituent_name""".format(day_before))
	orbis_results = query_orbis.result()
	constituent_name2 = []
	orbis = []
	for row in orbis_results:
		constituent_name2.append(row.constituent_name)
		orbis.append(row.orbis)
	orbis_df = pd.DataFrame({'Constituent_name':constituent_name2,
								'orbis':orbis})
	collect1 = pd.merge(collect,orbis_df,on='Constituent_name',how='left')
	########################################rss_feeds######################
	query_rss = client.query("""SELECT constituent_name, sum(downloaded_news) as rss FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Yahoo Finance RSS'
	GROUP BY constituent_name""".format(day_before))
	rss_results = query_rss.result()
	constituent_name3 = []
	rss = []
	for row in rss_results:
		constituent_name3.append(row.constituent_name)
		rss.append(row.rss)
	rss_df = pd.DataFrame({'Constituent_name':constituent_name3,
								'rss':rss})
	collect2 = pd.merge(collect1,rss_df,on='Constituent_name',how='left')
	print(collect2)
	########################################Stocktwits Logs#####################
	
	
if __name__ == '__main__':
	log_table()
	
	
		
	

