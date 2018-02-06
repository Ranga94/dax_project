from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import twitter_credentials

class StdOutListener(StreamListener):
	def on_data(self,data):
		python_obj = json.loads(data)
		created_at =  python_obj["created_at"]
		id = python_obj["user"]["id"]
		print(data)
		return True
		
	def on_error(self, status):
		print(status)
		
if __name__ == '__main__':
	listener = StdOutListener()
	auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
	auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
	stream = Stream(auth, listener)
	
	stream.filter(track=['ADIDAS AG','ALLIANZ SE','BREXIT','BASF SE ','BAYER AG ',' BEIERSDORF AG','BAYERISCHE MOTOREN WERKE AG','COMMERZBANK AKTIENGESELLSCHAFT',
	'CONTINENTAL AG','DAIMLER AG ','DEUTSCHE BOERSE AG','DEUTSCHE BANK AG','DEUTSCHE POST AG ','DEUTSCHE TELEKOM AG','E.ON SE'])