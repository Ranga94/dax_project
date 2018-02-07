from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from google.cloud import bigquery
 
import twitter_credentials
 
# # # # TWITTER STREAMER # # # #
class StdOutListener(StreamListener):

	def on_data(self, data):
		python_obj = json.loads(data)
		created_at =  python_obj["created_at"]
		text = python_obj["text"]
		name= python_obj["user"]["name"]
		screen_name = python_obj["user"]["screen_name"]
		location = python_obj["user"]["location"]
		language = python_obj["lang"]
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
		
		return True
	
	def on_error(self,status):
		print(status)


 
if __name__ == '__main__':
 
	listener = StdOutListener()
	auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
	auth.set_access_token(twitter_credentials.ACCESS_TOKEN,twitter_credentials.ACCESS_TOKEN_SECRET)
	
	stream = Stream(auth, listener)
	stream.filter(track=['BREXIT','ADIDAS'])

