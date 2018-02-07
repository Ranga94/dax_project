from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from google.cloud import bigquery
from tweepy import Stream
import json
 
import twitter_credentials
 
# # # # TWITTER STREAMER # # # #


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):

	def on_data(self,data):
		python_obj = json.loads(data)
		#for tweet in data:
		#	tweet._json['source'] = "Twitter"
		#	tweet._json['constituent_name'] = constituent_name
		created_at =  python_obj["created_at"]
		text = python_obj["text"]
		name= python_obj["user"]["name"]
		screen_name = python_obj["user"]["screen_name"]
		location = python_obj["user"]["location"]
		language = python_obj["lang"]
		#favourites_count = python_obj["favourites_count"]
		followers_count = python_obj["user"]["followers_count"]
		reply_count = python_obj["reply_count"]
		retweet_count = python_obj["retweet_count"]
		favourites_count = python_obj["favorite_count"]
		client = bigquery.Client()
		try: 
			query_insert =  client.query("""INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter`
			(created_at,text,name,screen_name,location, followers_count,reply_count,retweet_count) 
			VALUES ('{0}','{1}','{2}','{3}','{4}',{5},{6},{7})""".format(created_at,text,name, screen_name,location,followers_count,reply_count,retweet_count))
			insert_result = query_insert.result()
		except Exception as e:
			pass
			
		#print(screen_name)
		#print(followers_count)
		#print(language)
		#print(favourites_count)
		#print(followers_count)
		return True
		
	def on_error(self, status):
		print(status)
 
if __name__ == '__main__':
	listener = StdOutListener()
	auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
	auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
	stream = Stream(auth, listener)
	
	stream.filter(track=['ADIDAS AG','ALLIANZ SE','BREXIT','BASF SE ','BAYER AG',' BEIERSDORF AG','BAYERISCHE MOTOREN WERKE AG','COMMERZBANK AKTIENGESELLSCHAFT',
	'CONTINENTAL AG','DAIMLER AG ','DEUTSCHE BOERSE AG','DEUTSCHE BANK AG','DEUTSCHE POST AG','DEUTSCHE TELEKOM AG','E.ON SE','FRESENIUS MEDICAL CARE AG & CO. KGAA ',
	'FRESENIUS SE & CO. KGAA','HEIDELBERGCEMENT AG','HENKEL AG & CO. KGAA','INFINEON TECHNOLOGIES AG',' DEUTSCHE LUFTHANSA AG','LINDE AG','MERCK KGAA','MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN',
	'PROSIEBENSAT.1 MEDIA SE','RWE AG','SAP SE','SIEMENS AG','THYSSENKRUPP AG','VONOVIA SE','VOLKSWAGEN AG'])
	
	
    