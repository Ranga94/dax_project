from google.cloud import bigquery
import pandas as pd
import datetime as DT




constituent_name = ['ADIDAS AG','ALLIANZ SE','BASF SE','BAYER AG','BAYERISCHE MOTOREN WERKE AG','BEIERSDORF AG','COMMERZBANK AKTIENGESELLSCHAFT',
'CONTINENTAL AG','DAIMLER AG','DAX','DEUTSCHE BANK AG','DEUTSCHE BOERSE AG','DEUTSCHE LUFTHANSA AG','DEUTSCHE POST AG','DEUTSCHE TELEKOM AG','E.ON SE',
'FRESENIUS MEDICAL CARE AG & CO. KGAA','FRESENIUS SE & CO. KGAA','HEIDELBERGCEMENT AG','HENKEL AG & CO. KGAA','INFINEON TECHNOLOGIES AG','LINDE AG','MERCK KGAA',
'MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN','PROSIEBENSAT.1 MEDIA SE','RWE AG','SAP SE','SIEMENS AG','THYSSENKRUPP AG','VOLKSWAGEN AG',
'VONOVIA SE']

constituent_name_pd = pd.DataFrame({'constituent_name':constituent_name})

today = DT.date.today()
day_before = today - DT.timedelta(days=1)
client = bigquery.Client() 

query_twitter = client.query("""SELECT constituent_name, sum(downloaded_tweets) as tweets
FROM `igenie-project.pecten_dataset_test.tweet_logs`
where date = timestamp('{}')
GROUP BY constituent_name""".format(day_before))
tweet_results = query_twitter.result()

constituent_name_twitter = [] 
tweets = []
for row in tweet_results:
	constituent_name_twitter.append(row.constituent_name)
	tweets.append(row.tweets)

twitter_df =pd.DataFrame({'constituent_name':constituent_name_twitter,'tweets':tweets})
twitter_merge = pd.merge(constituent_name_pd, twitter_df, on = 'constituent_name', how = 'left')
#print(twitter_merge)
for key, value in twitter_merge.iteritems():
	print(tweet[value])

