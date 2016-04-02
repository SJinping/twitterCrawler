import sys
sys.path.append("/home/pan/Idealab/codes/twitterKeys")
import keys
from twython import TwythonStreamer
from pymongo import MongoClient
from datetime import datetime

class TweetStreammer(TwythonStreamer):

	def tweet_filter(self,data):

		keys = "id text lang media user coordinates place entities".split()
		user_keys = "id name screen_name lang location description statuses_count".split()
		tweet = {key: value for key, value in data.items() if key in keys }
		tweet["user"] = {key: value for key, value in data["user"].items() if key in user_keys }
		tweet["created_at"] = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S +0000 %Y')
		return tweet


	def __init__(self, consumer_key, consumer_secret, oauth_token, oauth_token_secret):
		super(TweetStreammer, self).__init__(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
		self.tweets_buffer = []
		self.bulk_size = 1000
		self.collection = MongoClient("localhost", 27017)["twitter"]["geo_tweets"]

	def on_success(self, data):

		if "user" not in data:  #Skip the non-tweet request result
			return
		#print(data)
		tweet = self.tweet_filter(data)

		self.tweets_buffer.append(tweet)

		#print(tweet["coordinates"])
	
		if len(self.tweets_buffer) >= self.bulk_size:
			self.collection.insert(self.tweets_buffer)
			print("{} tweets have been inserted".format(len(self.tweets_buffer)))
			self.tweets_buffer = []


	def on_error(self, status_code, data):
		print(status_code)



consumer_key       = keys.CONSUMERKEY
consumer_secret    = keys.CONSUMERSECRET
oauth_token        = keys.OAUTHTOKEN
oauth_token_secret = keys.OAUTHTOKENSECRET




geo_stream = TweetStreammer(consumer_key, consumer_secret, oauth_token, oauth_token_secret)
while True:
	try:
		geo_stream.statuses.filter(locations = [-180,-90,180,90])  # Here indicate location of the tweet. It is also can be indicated null
	except:
		pass
