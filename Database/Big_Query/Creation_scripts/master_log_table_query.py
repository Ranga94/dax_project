from google.cloud import bigquery
import datetime as DT
import pandas as pd

def log_table():
	##################################Set Date and take previous day for data agregation##############
	today = DT.date.today()
	day_before = today - DT.timedelta(days=1)
	client = bigquery.Client()  
	########################################Twitter logs Query#############################################
	query_twitter = client.query("""SELECT constituent_name, sum(downloaded_tweets) as tweets
	FROM `igenie-project.pecten_dataset_test.tweet_logs`
	where date = timestamp('{}')
	GROUP BY constituent_name""".format(day_before))
	tweet_results = query_twitter.result()
	######Initialise empty list and append results from BQ######
	constituent_name_twitter = [] 
	tweets = []
	for row in tweet_results:
		constituent_name_twitter.append(row.constituent_name)
		tweets.append(row.tweets)
	date = []
	for i in constituent_name_twitter:
		date.append(day_before)	
	##########Merge the list into pandas dataframe######
	tweets_df = pd.DataFrame({'Date':date,'Constituent_name':constituent_name_twitter,
							'tweets':tweets})
	######################################Bloomberg log query################################
	query_bloomberg = client.query("""SELECT constituent_name, sum(downloaded_news) as bloomberg FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Bloomberg'
	GROUP BY constituent_name""".format(day_before))
	bloomberg_results = query_bloomberg.result()
	######Initialise empty list and append results from BQ####
	constituent_name_bloomberg = []
	bloomberg = []
	for row in bloomberg_results:
		constituent_name_bloomberg.append(row.constituent_name)
		bloomberg.append(row.bloomberg)
	######Merge the list into pandas dataframe######
	bloomberg_df = pd.DataFrame({'Constituent_name':constituent_name_bloomberg,
								'bloomberg':bloomberg})
	######Merge the two dataframes based on key value Constituent_name#######							
	bloomberg_merge = pd.merge(tweets_df,bloomberg_df,on ='Constituent_name',how='left')
	##########################################Orbis Log Query################################
	query_orbis = client.query("""SELECT constituent_name, sum(downloaded_news) as orbis FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Orbis'
	GROUP BY constituent_name""".format(day_before))
	orbis_results = query_orbis.result()
	constituent_name_orbis = []
	orbis = []
	for row in orbis_results:
		constituent_name_orbis.append(row.constituent_name)
		orbis.append(row.orbis)
	####Merge the list into pandas dataframe######
	orbis_df = pd.DataFrame({'Constituent_name':constituent_name_orbis,
								'orbis':orbis})
	####Merge the previous dataframe with orbis_df based on Constituent_name#########
	orbis_merge = pd.merge(bloomberg_merge,orbis_df,on='Constituent_name',how='left')
	########################################rss_feeds###########################################
	query_rss = client.query("""SELECT constituent_name, sum(downloaded_news) as rss FROM `igenie-project.pecten_dataset_test.news_logs` 
	where date = TIMESTAMP('{}') and source = 'Yahoo Finance RSS'
	GROUP BY constituent_name""".format(day_before))
	rss_results = query_rss.result()
	#####Initialise empty list and append result from BQ####
	constituent_name_rss = []
	rss_feeds = []
	for row in rss_results:
		constituent_name_rss.append(row.constituent_name)
		rss_feeds.append(row.rss)
	######Merge list into pandas dataframe####
	rss_df = pd.DataFrame({'Constituent_name':constituent_name_rss,
								'rss_feeds':rss_feeds})
	######Merge rss dataframe with the previous dataframe based on constituent_name#######
	rss_merge = pd.merge(orbis_merge,rss_df,on='Constituent_name',how='left')
	#############################################stocktwits######################
	query_stocktwits = client.query("""SELECT constituent_name, count(*) as stocktwits FROM `igenie-project.pecten_dataset_test.tweets` 
	where source = 'StockTwits' and date = TIMESTAMP('{}')
	group by constituent_name""".format(day_before))
	stocktwits_results = query_stocktwits.result()
	######Initialise list with a dummy value because of the possibility of query result being nil for stocktwits######################
	stocktwits = [0]
	constituent_name_stocktwits = ['E.ON SE']
	for row in stocktwits_results:
		constituent_name_stocktwits.append(row.constituent_name)
		stocktwits.append(row.stocktwits)
	##########Check if any additional results are appended. Yes: Retain the dummy value which is zero. No: Remove dummy value#####
	if(len(stocktwits)==1):
		pass
	else:
		del stocktwits[0]
		del constituent_name_stocktwits[0]
	########Merge the list into pandas data frame########
	stocktwits_df = pd.DataFrame({'Constituent_name':constituent_name_stocktwits,
									'stocktwits':stocktwits})
	######Merge the two dataframes
	stocktwits_merge = pd.merge(rss_merge, stocktwits_df,on='Constituent_name',how='left')
	##########################################ticker log query##############################
	query_ticker = client.query("""SELECT constituent_name, sum(downloaded_ticks) as ticker FROM `igenie-project.pecten_dataset_test.ticker_logs` 
	where date = TIMESTAMP('{}')
	group by constituent_name""".format(day_before))
	ticker_results = query_ticker.result()
	##############Initialise list with a dummy value because of the possibility of query result being nil for ticker#####
	ticker = [0]
	constituent_name_ticker = ['E.ON SE']
	for row in ticker_results:
		constituent_name_ticker.append(row.constituent_name)
		ticker.append(row.ticker)
	##########Check if any additional results are appended. Yes: Retain the dummy value which is zero. No: Remove dummy value#####
	if(len(ticker)==1):
		pass
	else:
		del ticker[0]
		del constituent_name_ticker[0]
	#####Merge the lists into pandas dataframe###############
	ticker_df = pd.DataFrame({'Constituent_name':constituent_name_ticker,
								'ticker':ticker})
	#####Merge ticker dataframe with previous dataframes#########
	ticker_merge = pd.merge(stocktwits_merge, ticker_df,on='Constituent_name', how = 'left')
	#############Replace Nan in dataframes with zero#######
	df = ticker_merge.fillna(0)
	print(df)
	
	constituent = df.iloc[:,0]
	print(constituent)
	Date = df.iloc[:,1]
	Tweets = df.iloc[:,2]
	Bloomberg = df.iloc[:,3]
	Orbis = df.iloc[:,4]
	RSS_feeds = df.iloc[:,5]
	StockTwits = df.iloc[:,6]
	Ticker = df.iloc[:,7]
	######################################Insert into table###########################
	for i in constituent_name:
	
		query_insert = client.query("""INSERT INTO `igenie-project.pecten_dataset_dev.master_log_table`
		(`Date`, `Constituent_name`, `tweets`, `bloomberg`, `orbis`, `rss_feeds`,`stocktwits`,`ticker`) 
		VALUES ('{0}', '{1}', {2}, {3}, {4}, {5}, {6},{7})""".format(Date[i],constituent[i],int(Tweets[i]),int(Bloomberg[i]),int(Orbis[i]),int(RSS_feeds[i]),int(StockTwits[i]),int(Ticker[i])))
		insert_result = query_insert.result()
	
	
if __name__ == '__main__':
	log_table()
	
	
		
	

