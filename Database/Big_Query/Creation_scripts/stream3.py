from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from google.cloud import bigquery
 
import twitter_credentials
 
# # # # TWITTER STREAMER # # # #
class StdOutListener(StreamListener):

	def on_status(self, status):
		python_obj = json.loads(status)
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
		if BREXIT in text:
			print("BREXIT")
		else:
			print("adidas")
		
		return True
	
	def on_error(self,status):
		print(status)


 
if __name__ == '__main__':
 
	listener = StdOutListener()
	auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
	auth.set_access_token(twitter_credentials.ACCESS_TOKEN,twitter_credentials.ACCESS_TOKEN_SECRET)
	
	stream = Stream(auth, listener)
	stream.filter(track=['BREXIT','ADIDAS'])

