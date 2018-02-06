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
		created_at =  python_obj["created_at"]
		text = python_obj["text"]
		#followers_count= python_obj["followers_count"]
		#language = python_obj["language"]
		#favourites_count = python_obj["favourites_count"]
		#followers_count = python_obj["followers_count"]
		client = bigquery.Client()
		query_insert =  client.query("""INSERT INTO `igenie-project.pecten_dataset_dev.stream_twitter`
		(`created_at`, `text`) 
		VALUES ('{}', '{}')""".format(created_at,text))
		insert_result = query_insert.result()
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
	
	stream.filter(track=['BREXIT'])
    